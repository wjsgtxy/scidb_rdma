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
 *  SortArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef SORT_ARRAY_H
#define SORT_ARRAY_H

#include <array/ArrayDesc.h>
#include <array/ConstArrayIterator.h>  // uggh implementation in SortIterators::end()


#include <util/Arena.h>
#include <util/PointerRange.h>
#include <util/Job.h>
#include <util/Mutex.h>
#include <util/Event.h>


namespace scidb
{
class Array;
class MemArray;
class TupleComparator;
class TupleSkipper;
class Query;
class PhysicalOperator;

/**
 * Utility class for sorting arrays
 */
class SortArray
{
public:

    /**
     * Class sorts an array into 1-d output array based on keys provided.
     *
     * @param[in] inputSchema schema of array to sort
     * @param[in] arena parent mem arena for SortArray's mem usage
     */
    SortArray(const ArrayDesc& inputSchema, arena::ArenaPtr const& arena);


    /**
     * Set true if output should have chunk_pos and cell_pos attributes.
     *
     * Useful to facilitate internal chunk-wise sorting.
     * @note Must be called *before* output schema is computed.
     */
    SortArray& setPreservePositions(bool b)
    {
        _preservePositions = b;
        return *this;
    }

    /**
     * @return whether the SortArray needs to preserve the cell positions of all records.
     * @note The output array schema depends on this!
     *       In particular, to preserve positions, there output schema has two more attributes:
     *       - chunk_pos: an integer that corresponds to the chunk number.
     *                    @see ArrayCoordinatesMapper::chunkPos2pos, pos2chunkPos.
     *       - cell_pos:  an integer that corresponds to the cell position inside a chunk.
     *                    @see ArrayCoordinatesMapper::coord2pos, pos2coord.
     */
    bool preservePositions() const
    {
        return _preservePositions;
    }

    /**
     * Set output chunk size.
     * @note Must be called *before* output schema is computed.
     */
    SortArray& setChunkSize(size_t sz)
    {
        _chunkSize = sz;
        return *this;
    }

    /**
     * Set output dimension name.
     */
    SortArray& setDimName(std::string const& nm)
    {
        assert(!nm.empty());
        _dimName = nm;
        return *this;
    }

    /**
     * Sort the array
     * tcomp should be instance of TupleComparator or instance of a derived
     * class if caller wishes to override standard compare behavior.
     * @param[in] inputArray array to sort, schema must match input schema
     * @param[in] query query context
     * @param[in] phyOp the PhysicalOperator for the SG
     * @param[in] tcomp class which provides comparison operator
     * @param[in] skipper whether to skip a tuple.
     * @return sorted one-dimensional array.
     */
    std::shared_ptr<MemArray> getSortedArray(std::shared_ptr<Array> inputArray,
                                               std::shared_ptr<Query> query,
                                               std::shared_ptr<PhysicalOperator> const& phyOp,
                                               std::shared_ptr<TupleComparator> tcomp,
                                               TupleSkipper* skipper = NULL
                                               );

    /**
     * @return the array descriptor from the input array.
     */
    const ArrayDesc& getInputArrayDesc()
    {
        return _inputSchema;
    }

    /** @return output array descriptor without extra fields */
    ArrayDesc const& getOutputSchema();

    /** @return output array descriptor with the extra fields appended when _preservePositions is true */
    ArrayDesc const& getExpandedOutputSchema();

private:

    /**
     * SortIterators are the array iterators that can be used by a sort job
     * while partitioning the input into sorted runs.  An instance of SortIterator
     * may only be used by a *single* sort job at a time.
     */
    class SortIterators
    {
    public:
        SortIterators(std::shared_ptr<Array>const& input,
                      size_t shift,
                      size_t step);

        void advanceIterators();
        bool end()
            { return _arrayIters[0]->end(); }
        PointerRange<std::shared_ptr<ConstArrayIterator> const> getIterators()
            { return _arrayIters; }

    private:
        const Attributes& _arrayAttrs;

        // iterators under the control of this instance
        std::vector< std::shared_ptr<ConstArrayIterator> > _arrayIters;

        size_t const _shift;    // initial chunk offset
        size_t const _step;     // number of chunks to skip on advance
    };


    /**
     * A SortJob is a Job that partitions part of the array into a mem-sized chunk
     * and sorts it.  The result of producing this sorted run is a MemArray constructed
     * from a TupleArray (each one of which fits in memory).  Note that the input array
     * being sorted may or may not have an empty tag, but the sort job produces a
     * result with an empty tag.
     */
    class SortJob : public Job
    {
    public:
        SortJob(SortArray* sorter,
                std::shared_ptr<Query>const& query,
                std::shared_ptr<PhysicalOperator>const& phyOp,
                size_t id,
                SortIterators* iters);

        virtual void run();
        bool complete() { return _complete; };

    private:
        SortArray& _sorter;               // enclosing sort context
        SortIterators& _sortIters;        // iterators into array to partition/sort
        bool _complete;                   // true if at and of input
        size_t const _id;                 // job id, determines which chunks to partition
    };


    /**
     * A MergeJob is a Job that merges sorted runs into larger sorted runs.
     * It uses a MergeSortArray to do the merging
     */
    class MergeJob : public Job
    {
    public:
        MergeJob(SortArray* sorter,
                 std::shared_ptr<Query>const& query,
                 std::shared_ptr<PhysicalOperator>const& phyOp,
                 size_t id);

        virtual void run();

    private:
        SortArray& _sorter;     // enclosing sort context
        size_t const _id;       // job id, used to communicate with main thread
    };

    void _computeOutputSchemas();

    ArrayDesc const _inputSchema;
    arena::ArenaPtr _arena;                        // parent memory arena
    std::shared_ptr<Array> _input;                 // array to sort
    ArrayDesc _outputSchemaExpanded;               // output schema plus $chunk_pos and $cell_pos fields
    ArrayDesc _outputSchemaNormal;                 // output schema without the two extra fields.
    std::shared_ptr<TupleComparator> _tupleComp;   // comparison to use between cells

    bool _gotSchemas { false }; // true if _outputSchema* computed
    size_t _chunkSize { 0 };    // desired output chunk size
    size_t _memLimit;           // how big are the sorted runs
    size_t _nStreams;           // how many runs to merge at once
    size_t _pipelineLimit;      // how many runs to allow in the pipeline
    size_t _tupleSize;          // how big is each cell
    std::string _dimName { "n" }; // output dimension name

    // list which accumulates sorted runs
    std::list< std::shared_ptr<Array> > _results;

    // state to track sort/merge progress and jobs
    Mutex  _sortLock;
    Event  _sortEvent;
    size_t _nRunningJobs;
    size_t _nStoppedJobs;
    size_t _runsProduced;
    mgd::vector<uint8_t> _partitionComplete;
    mgd::vector< std::shared_ptr<SortIterators> > _sortIterators;
    mgd::vector< std::shared_ptr<Job> > _runningJobs;
    mgd::vector< std::shared_ptr<Job> > _waitingJobs;
    mgd::vector< std::shared_ptr<Job> > _stoppedJobs;
    std::shared_ptr<Job> _failedJob;

    /**
     * Whether the cell positions of all records need to be preserved.
     */
    bool _preservePositions { false };

    // whether to skip a tuple.
    TupleSkipper* _skipper;
};

} //namespace scidb

#endif /* SORT_ARRAY_H */
