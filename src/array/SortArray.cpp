/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 *  SortArray.cpp
 *
 *  Created on: Aug 14, 2013
 *  Based on implementation of operator sort
 */

#include <array/SortArray.h>

#include <vector>
#include <list>

#include <boost/scope_exit.hpp>

#include <array/MemArray.h>
#include <array/MergeSortArray.h>
#include <array/TupleArray.h>
#include <query/PhysicalOperator.h>
#include <system/Config.h>
#include <util/PerfTimeScope.h>
#include <util/Timing.h>
#include <util/iqsort.h>

#include <log4cxx/logger.h>

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.sortarray"));
}

namespace scidb {

    using namespace std;

    /**
     * Helper class SortIterators
     */

    /**
     * Constructor -- create iterators and position them at the correct
     * chunk
     */
    SortArray::SortIterators::SortIterators(std::shared_ptr<Array>const& input,
                                            size_t shift,
                                            size_t step) :
        _arrayAttrs(input->getArrayDesc().getAttributes()),
        _arrayIters(input->getArrayDesc().getAttributes().size()),
        _shift(shift),
        _step(step)
    {
        // Create the iterators
        for (const auto& attr : _arrayAttrs)
        {
            _arrayIters[attr.getId()] = input->getConstIterator(attr);
        }

        // Position iterators at the correct chunk
        const auto& fda = _arrayAttrs.firstDataAttribute();
        for (size_t j = _shift; j != 0 && !_arrayIters[fda.getId()]->end(); --j)
        {
            for (const auto& attr : _arrayAttrs)
            {
                ++(*_arrayIters[attr.getId()]);
            }
        }
    }

    /**
     * Advance iterators by (step-1) chunks.
     * The goal is to enable SortJob::run() to read chunks: shift, shift+step, shift+2*step, ...
     * One may wonder: but to achieve the goal, shouldn't we advance (step) chunks instead of (step-1)?
     * The answer is: yes; but SortJob::run() calls TupleArray::append(), which advances 1 chunk.
     */
    void SortArray::SortIterators::advanceIterators()
    {
        const auto& fda = _arrayAttrs.firstDataAttribute();
        for (size_t j = _step-1; j != 0 && !_arrayIters[fda.getId()]->end(); --j)
        {
            for (const auto& attr : _arrayAttrs)
            {
                ++(*_arrayIters[attr.getId()]);
            }
        }
    }


    /**
     * Helper class SortJob
     */

    /**
     * The input array may not have an empty tag,
     * but the output array has an empty tag.
     */
    SortArray::SortJob::SortJob(SortArray* sorter,
                                std::shared_ptr<Query>const& query,
                                std::shared_ptr<PhysicalOperator>const& phyOp,
                                size_t id,
                                SortIterators* iters)
        : Job(query, std::string("SortArrayJob")),
          _sorter(*sorter),
          _sortIters(*iters),
          _complete(false),
          _id(id)
    {
    }

    /**
     * Here we try to partition part of the array into manageable sized chunks
     * and then sort them in-memory.  Each resulting sorted run is converted to
     * a MemArray and pushed onto the result list.  If we run out of input, or
     * we reach the limit on the size of the result list, we stop
     */
    void SortArray::SortJob::run()
    {
        LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] start ");
        try {
            arena::ScopedArenaTLS arenaTLS(_query->getArena());
            PerfTimeScope pts(PTS_JOB_RUN_SORT);    // SortArray::SortJob::run(), _query->getArena()

            // At the end of run(), we must always mark ourselves on the stopped
            // job list and signal the main thread.
            BOOST_SCOPE_EXIT ( (&_sorter) (&_id) )
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SORT_ARRAY);

                _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
                ++ _sorter._nStoppedJobs;
                _sorter._runningJobs[_id].reset();
                -- _sorter._nRunningJobs;
                _sorter._sortEvent.signal();
            } BOOST_SCOPE_EXIT_END;

            // TupleArray must handle the case that outputDesc.getAttributes().size()
            // is 1 larger than arrayIterators.size(), i.e. the case when the input array
            // does not have an empty tag (but the output array does).
            //
            size_t tupleArraySizeHint = _sorter._memLimit / _sorter._tupleSize;
            LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] _memLimit: " << _sorter._memLimit);
            LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] _tupleSize: " << _sorter._tupleSize);
            LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] tupleArraySizeHint: " << tupleArraySizeHint);
            LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] preservePositions(): "<< _sorter.preservePositions());
            std::shared_ptr<TupleArray> buffer =
                std::make_shared<TupleArray>(_sorter.getExpandedOutputSchema(),
                                        _sortIters.getIterators(),
                                        _sorter.getInputArrayDesc(),
                                        0,
                                        tupleArraySizeHint,
                                        16*MiB,
                                        _sorter._arena,
                                        _sorter.preservePositions(),
                                        _sorter._skipper);

            // Append chunks to buffer until we run out of input or reach limit
            bool limitReached = false;
            while (!_sortIters.end() && !limitReached)
            {
                buffer->append(_sorter.getInputArrayDesc(), _sortIters.getIterators(), 1, _sorter._skipper);
                size_t currentSize = buffer->getSizeInBytes();
                LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] currentSize (bytes): " << currentSize
                                      << " _memLimit " << _sorter._memLimit);
                if (currentSize > _sorter._memLimit)
                {
                    std::shared_ptr<Array> baseBuffer =
                        static_pointer_cast<TupleArray, Array> (buffer);
                    LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] sorted 1 buffer start");
                    buffer->sort(_sorter._tupleComp);
                    LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] sorted 1 buffer finish");
                    buffer->truncate();
                    {
                        std::shared_ptr<Array> ma =
                            std::make_shared<MemArray>(baseBuffer->getArrayDesc(),
                                                       getQuery());
                        ma->appendHorizontal(baseBuffer);
                        LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] appendHorizontal  1 buffer finish");
                        ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SORT_ARRAY);
                        LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] appendHorizontal  _sorter lock taken");
                        _sorter._results.push_back(ma);
                        _sorter._runsProduced++;
                        LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] At currentSize " << currentSize
                                              << " Produced sorted run # " << _sorter._runsProduced);
                        if (_sorter._results.size() > _sorter._pipelineLimit)
                        {
                            LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run]"
                                                  << " limitReached: _results.size() " << _sorter._results.size()
                                                  << " > _pipelineLimit " << _sorter._pipelineLimit);
                            limitReached = true;
                        }
                    }
                    if (limitReached) {
                        buffer.reset();
                        LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] limitReached: buffer.reset()");
                    } else {
                        buffer = std::make_shared<TupleArray>(_sorter.getExpandedOutputSchema(),
                                                    _sortIters.getIterators(),
                                                    _sorter._inputSchema,
                                                    0,
                                                    tupleArraySizeHint,
                                                    16*MiB,
                                                    _sorter._arena,
                                                    _sorter.preservePositions(),
                                                    _sorter._skipper);
                        LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] !limitReached: new buffer");
                    }
                }
                _sortIters.advanceIterators();
            }
            LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] iters ended or limitReached");

            // May have some left-overs --- only in the case where we are at the end
            if (_sortIters.end())
            {
                if (buffer && buffer->size())
                {
                    LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] handle end leftovers");
                    std::shared_ptr<Array> baseBuffer =
                        static_pointer_cast<TupleArray, Array> (buffer);
                    buffer->sort(_sorter._tupleComp);
                    buffer->truncate();
                    {
                        std::shared_ptr<Array> ma =
                            std::make_shared<MemArray>(baseBuffer->getArrayDesc(),
                                                       getQuery());
                        ma->appendHorizontal(baseBuffer);
                        ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SORT_ARRAY);
                        _sorter._results.push_back(ma);
                        _sorter._runsProduced++;
                        LOG4CXX_DEBUG(logger, "[SortArray::SortJob::run] leftovers"
                                              << " produced sorted run # " << _sorter._runsProduced);
                    }
                }
                _complete = true;
            }
        } catch (std::bad_alloc const& e) {
            throw SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "[SortArray::SortJob::run] end ");
    }


    /**
     * Helper class MergeJob
     */

    /**
     * Constructor
     */
    SortArray::MergeJob::MergeJob(SortArray* sorter,
                                  std::shared_ptr<Query>const& query,
                                  std::shared_ptr<PhysicalOperator>const& phyOp,
                                  size_t id) :
        Job(query, std::string("SortArrayJob")),
        _sorter(*sorter),
        _id(id)
    {
    }

    /**
     * Remove a group of arrays from the list, merge them using a MergeSortArray,
     * then add the result back to the end of the list.
     */
    void SortArray::MergeJob::run()
    {
        LOG4CXX_TRACE(logger, "[SortArray::MergeJob::run] start ");
        try {
            arena::ScopedArenaTLS arenaTLS(_query->getArena());
            PerfTimeScope pts(PTS_JOB_RUN_MERGE);    // SortArray::MergeJob::run()

            vector< std::shared_ptr<Array> > mergeStreams;
            std::shared_ptr<Array> merged;
            std::shared_ptr<Array> materialized;

            // At the end of run(), we must always put the result (if it exists) on the end of the
            // list, mark ourselves on the stopped job list, and signal the main thread
            BOOST_SCOPE_EXIT ( (&materialized) (&_sorter) (&_id) )
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SORT_ARRAY);

                if (materialized.get())
                {
                    _sorter._results.push_back(materialized);
                }
                _sorter._stoppedJobs[_id] = _sorter._runningJobs[_id];
                ++ _sorter._nStoppedJobs;
                _sorter._runningJobs[_id].reset();
                -- _sorter._nRunningJobs;
                _sorter._sortEvent.signal();
            } BOOST_SCOPE_EXIT_END;

            // remove the correct number of streams from the list
            {
                ScopedMutexLock sm(_sorter._sortLock, PTW_SML_SORT_ARRAY);

                size_t nSortedRuns = _sorter._results.size();
                size_t currentStreams =
                    nSortedRuns < _sorter._nStreams ? nSortedRuns : _sorter._nStreams;
                mergeStreams.resize(currentStreams);

                LOG4CXX_DEBUG(logger, "[SortArray::MergeJob::run] Found " << currentStreams << " runs to merge");

                for (size_t i = 0; i < currentStreams; i++)
                {
                    mergeStreams[i] = _sorter._results.front();
                    _sorter._results.pop_front();
                }
            }

            // merge the streams -- true means the array contains local data only
            size_t nStreams = mergeStreams.size();
            std::shared_ptr<vector<size_t> > streamSizes = std::make_shared< vector<size_t> > (nStreams);
            for (size_t i=0; i<nStreams; ++i) {
                (*streamSizes)[i] = -1;
            }
            merged = std::make_shared<MergeSortArray>(getQuery(),
                                            _sorter.getExpandedOutputSchema(),
                                            mergeStreams,
                                            _sorter._tupleComp,
                                            0,  // Do not add an offset to the cell's coordinates.
                                            streamSizes // Using -1 preserves the behavior of the original code here.
                                            );

            // false means perform a horizontal copy (copy all attributes for chunk 1,
            // all attributes for chunk 2,...)
            materialized = std::make_shared<MemArray>(merged->getArrayDesc(), getQuery());
            materialized->appendHorizontal(merged);
            LOG4CXX_DEBUG(logger, "[SortArray::MergeJob::run] merged " << mergeStreams.size());
        } catch (std::bad_alloc const& e) {
            throw SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "[SortArray::MergeJob::run] end ");
    }

    SortArray::SortArray(const ArrayDesc& inputSchema,
                         arena::ArenaPtr const& arena)
         : _inputSchema(inputSchema),
           _arena(arena),
           _sortEvent(),
           _nRunningJobs(0),
           _nStoppedJobs(0),
           _runsProduced(0),
           _partitionComplete(arena),
           _sortIterators(arena),
           _runningJobs(arena),
           _waitingJobs(arena),
           _stoppedJobs(arena),
           _skipper(NULL)
    { }

    ArrayDesc const& SortArray::getOutputSchema()
    {
        if (!_gotSchemas) {
            _computeOutputSchemas();
        }
        return _outputSchemaNormal;
    }

    ArrayDesc const& SortArray::getExpandedOutputSchema()
    {
        if (!_gotSchemas) {
            _computeOutputSchemas();
        }
        return _outputSchemaExpanded;
    }

    /**
     * Output schema is input attributes, optionally followed by "expanded" chunk_pos/cell_pos
     * attributes (_preservePositions).  An emptytag attribute is added to the schema if it doesn't
     * already exist.  ChunkSize is computed here if not already specified.
     */
    void SortArray::_computeOutputSchemas()
    {
        assert(!_gotSchemas);
        _gotSchemas = true;

        //Right now we always return an unbounded array. You can use subarray to bound it if you need to
        //(but you should not need to very often!!). TODO: if we return bounded arrays, some logic inside
        //MergeSortArray gives bad results. We should fix this some day.

        //If the user does not specify a chunk size, we'll use MIN( max_logical_size, 1 million).

        size_t inputSchemaSize = _inputSchema.getSize();
        if (_chunkSize == 0)
        {   //not set by user
            _chunkSize = Config::getInstance()->getOption<size_t>(CONFIG_TARGET_CELLS_PER_CHUNK);
            ASSERT_EXCEPTION(_chunkSize > 0, "SortArray::_computeOutputSchema,"
                                             " CONFIG_TARGET_CELLS_PER_CHUNK must be > 0");

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if (inputSchemaSize < _chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                _chunkSize = std::max<size_t>(inputSchemaSize, 1);
            }
        }

        Dimensions newDims(1,DimensionDesc(_dimName, 0, 0,
                                           CoordinateBounds::getMax(),
                                           CoordinateBounds::getMax(),
                                           _chunkSize, 0));

        Attributes attributes = _inputSchema.getAttributes(/*exclude:*/true);

        // ArrayDesc consumes the new copy, source is reused later with the assumption
        // that the empty attribute is not present in it.
        auto preservedPositionAttributes = attributes;
        _outputSchemaNormal = ArrayDesc(_inputSchema.getName(),
                                        attributes.addEmptyTagAttribute(),
                                        newDims,
                                        createDistribution(dtUndefined),
                                        _inputSchema.getResidency());

        // the expanded version.
        if (_preservePositions) {
            preservedPositionAttributes.push_back(AttributeDesc(
                            "$chunk_pos",
                            TID_INT64,
                            0, // flags
                            CompressorType::NONE  // defaultCompressionMethod
                            ));
            preservedPositionAttributes.push_back(AttributeDesc(
                            "$cell_pos",
                            TID_INT64,
                            0, // flags
                            CompressorType::NONE  // defaultCompressionMethod
                            ));
        }

        // ArrayDesc consumes the new copy, source is discarded.
        _outputSchemaExpanded = ArrayDesc(_inputSchema.getName(),
                                          preservedPositionAttributes.addEmptyTagAttribute(),
                                          newDims,
                                          createDistribution(dtUndefined),
                                          _inputSchema.getResidency());
    }


    /***
     * Sort works by first transforming the input array into a series of sorted TupleArrays.
     * Then the TupleArrays are linked under a single MergeSortArray which encodes the merge
     * logic within its iterator classes.  To complete the sort, we materialize the merge
     * Array.
     * TODO: why materialize rather than just pull on the merge array? every copy is a cost.
     */
    std::shared_ptr<MemArray> SortArray::getSortedArray(std::shared_ptr<Array> inputArray,
                                                   std::shared_ptr<Query> query,
                                                   const std::shared_ptr<PhysicalOperator>& phyOp,
                                                   std::shared_ptr<TupleComparator> tcomp,
                                                   TupleSkipper* skipper
                                                   )
    {
        assert(arena::Arena::getArenaTLS() == query->getArena());

        // Timing for Sort
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray] " << getExpandedOutputSchema().getName() << " begins");

        // Init config parameters
        size_t numJobs = inputArray->getSupportedAccess() == Array::RANDOM ?
	           Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_JOBS) : 1;
        _memLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)*MiB;
        _nStreams = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_NSTREAMS);
        _pipelineLimit = Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_PIPELINE_LIMIT);
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray]  config _pipelineLimit " << _pipelineLimit);
        // _pipelineLimit is how many sorted pieces to make before merging. default was 32
        //

        // Turn off threading, if numJobs is 1.
        if (numJobs == 1 && _arena->supports(arena::threading)) {
            // Note from Donghui Zhang: Looks to me that the arena API could be improved.
            // Here I have a seemingly simple need: turn off threading.
            // The arena API has addThreading() but not removeThreading().
            // It seems that calling newArena() with a Options object similar to that
            // of _arena but with threading=false is the only way.
            // However, there is no way to get _arena's Options.
            // So I'm calling _arena->supports() for each feature.
            // But the pagesize option value cannot be acquired. So I'm using 16MiB.
            _arena = arena::newArena(arena::Options("sort array arena", _arena).
                                     finalizing(_arena->supports(arena::finalizing)).
                                     recycling(_arena->supports(arena::recycling)).
                                     resetting(_arena->supports(arena::resetting)).
                                     debugging(_arena->supports(arena::debugging)).
                                     threading(false).
                                     pagesize(16*MiB));
            // TODO: there is no explanation of why DZ felt it necessary to disable the arena threading
            //       for the above case.
            // TODO: hardcoded 16*MiB needs to be eliminated.
        }

        // Validate config parameters
        if (_pipelineLimit <= 1)
        {
            _pipelineLimit = 2;
        }
        if (_nStreams <= 1)
        {
            _nStreams = 2;
        }
        if (_pipelineLimit < _nStreams)
        {
            _pipelineLimit = _nStreams;
        }
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray] numJobs " << numJobs);
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray] _memLimit " << _memLimit);
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray] _nStreams " << _nStreams);
        LOG4CXX_DEBUG(logger, "[SortArray::getSortedArray] final _pipelineLimit " << _pipelineLimit);

        // Init sorting state
        std::shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        _input = inputArray;
        _tupleComp = tcomp;
        _tupleSize = TupleArray::getTupleFootprint(getExpandedOutputSchema().getAttributes());
        _nRunningJobs = 0;
        _nStoppedJobs = 0;
	      _runsProduced = 0;
        _partitionComplete.resize(numJobs);
        _waitingJobs.resize(numJobs);
        _runningJobs.resize(numJobs);
        _stoppedJobs.resize(numJobs);
        _sortIterators.resize(numJobs);
        _skipper = skipper;

        // Create the iterator groups and sort jobs
        for (size_t i = 0; i < numJobs; i++)
        {
            _sortIterators[i] = std::make_shared<SortIterators>(_input, i, numJobs);
            _waitingJobs[i] = std::make_shared<SortJob>(this,
                                                        query,
                                                        phyOp,
                                                        i,
                                                        _sortIterators[i].get());
            _partitionComplete[i] = false;
        }

        // Main loop
        while (true)
        {
            Event::ErrorChecker ec;
            ScopedMutexLock sm(_sortLock, PTW_SML_SORT_ARRAY);

            // Try to spawn jobs
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_waitingJobs[i])
                {
                    _runningJobs[i] = _waitingJobs[i];
                    _waitingJobs[i].reset();
                    _nRunningJobs++;
                    queue->pushJob(_runningJobs[i]);
                }
            }

            // If nothing is running, get out
            LOG4CXX_TRACE(logger, "[SortArray::getSortedArray] _nRunningJobs " << _nRunningJobs
                                  << " _nStoppedJobs " << _nStoppedJobs);
            if (_nRunningJobs == 0)
            {
                if (_nStoppedJobs == 0) {
                    break;
                }
            } else {
                // Wait for a job to be done
                // The reason why it is in the else clause is that,
                // in case there is no running job but some job has failed,
                // there won't be any job that will signal _sortEvent any more.
                _sortEvent.wait(_sortLock, ec, PTW_JOB_SORT);
                LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]  _sortEvent occurred ");
            }

            // Reap the stopped jobs and re-schedule
            for (size_t i = 0; i < numJobs; i++)
            {
                if (_stoppedJobs[i])
                {
                    enum nextJobType { JobNone, JobMerge, JobSort };

                    nextJobType nextJob = JobNone;
                    bool jobSuccess = true;
                    std::shared_ptr<SortJob> sJob;

                    LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]  _stoppedJob["<<i<<"] waiting... ");
                    jobSuccess = _stoppedJobs[i]->wait(PTW_JOB_SORT_STOPPED);
                    LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]  _stoppedJob["<<i<<"] wait completed ");

                    if (!_failedJob && jobSuccess)
                    {
                        sJob = dynamic_pointer_cast<SortJob, Job>(_stoppedJobs[i]);
                        if (sJob.get() && sJob->complete())
                        {
                            _partitionComplete[i] = true;
                        }

                        if (_partitionComplete[i])
                        {
                            LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]"
                                                  << " _stoppedJob["<<i<<"] is a 'complete partition'");
                            // partition is complete, schedule the merge if necessary
                            if (_results.size() > _nStreams)
                            {
                                LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]"
                                                      << " _results.size() " << _results.size()
                                                      << " > _nStreams " << _nStreams << " so next Job is Merge");
                                nextJob = JobMerge;
                            }
                        }
                        else
                        {
                            LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]"
                                                  << " _stoppedJob["<<i<<"] NOT a 'complete partition'");
                            // partition is not complete, schedule the sort if we can,
                            // or the merge if we must
                            if (_results.size() < _pipelineLimit)
                            {
                                nextJob = JobSort;
                            }
                            else
                            {
                                LOG4CXX_TRACE(logger, "[SortArray::getSortedArray] num _result " << _results.size()
                                                      << " >= _pipelineLimit " << _pipelineLimit
                                                      << " so next Job is a Merge");
                                nextJob = JobMerge;
                            }
                        }

                        if (nextJob == JobSort)
                        {
                            _waitingJobs[i] = std::make_shared<SortJob>(this,
                                                              query, phyOp,
                                                              i,
                                                              _sortIterators[i].get());
                        }
                        else if (nextJob == JobMerge)
                        {
                            _waitingJobs[i] = std::make_shared<MergeJob>(this,query,phyOp,i);
                        }
                    }
                    else
                    {
                        // An error occurred.  Save this job so we can re-throw the exception
                        // when the rest of the jobs have stopped.
                        if (!jobSuccess)
                        {
                            _failedJob = _stoppedJobs[i];
                        }
                    }
                    _stoppedJobs[i].reset();
                    -- _nStoppedJobs;
                }
            }
        }
        LOG4CXX_TRACE(logger, "[SortArray::getSortedArray]  no more runnning/stopped jobs");

        // If there is a failed job, we need to just get out of here
        if (_failedJob)
        {
            _failedJob->rethrow();
        }

        // If there were no failed jobs, we still may need one last merge
        if (_results.size() > 1)
        {
            std::shared_ptr<Job> lastJob = std::make_shared<MergeJob>(this, query, phyOp, 0);
            queue->pushJob(lastJob);
            lastJob->wait(PTW_JOB_MERGE_FINAL, Job::CAN_RAISE | Job::CAN_MULTIWAIT);
        }

        // Return the result
        if (_results.empty())
        {
            _results.push_back(std::make_shared<MemArray>(getExpandedOutputSchema(), query));
        }

        std::shared_ptr<MemArray> ret = dynamic_pointer_cast<MemArray, Array> (_results.front());
        _results.clear();
        return ret;
    }

}  // namespace scidb
