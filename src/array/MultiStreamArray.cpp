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


#ifndef SCIDB_CLIENT

#include <array/MultiStreamArray.h>

#include <query/Query.h>
#include <system/Config.h>

using namespace std;

namespace scidb {

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.multistreamarray"));
    static log4cxx::LoggerPtr loggerVerbose(log4cxx::Logger::getLogger("scidb.qproc.multistreamarray.verbose"));
    static log4cxx::LoggerPtr loggerSleep(log4cxx::Logger::getLogger("scidb.qproc.multistreamarray.sleep"));

MultiStreamArray::DefaultChunkMerger::DefaultChunkMerger (bool isEnforceDataIntegrity)
  : _isEnforceDataIntegrity(isEnforceDataIntegrity)
  , _hasDataIntegrityIssue(false)
  , _numElems(0)
  , _chunkSizeLimitMiB(Config::getInstance()->getOption<size_t>(CONFIG_CHUNK_SIZE_LIMIT))
{ }

bool
MultiStreamArray::DefaultChunkMerger::mergePartialChunk(size_t stream,
                                                        AttributeID attId,
                                                        std::shared_ptr<MemChunk>& partialChunk,
                                                        std::shared_ptr<Query> const& query)
{
    static const char* funcName = "DefaultChunkMerger::mergePartialChunk: ";
    LOG4CXX_TRACE(logger, funcName << "@@@used ");
    assert(partialChunk);


    AttributeDesc const& attr = partialChunk->getAttributeDesc();
    SCIDB_ASSERT((attId == attr.getId()));

    const bool isEbm = isEmptyBitMap(partialChunk);

    if (!isEbm) {
        _numElems += partialChunk->count();
    }

    if (!_mergedChunk)  {
        _mergedChunk.swap(partialChunk);
        assert(!partialChunk);
        return false;
    }
    _mergedChunk->setCount(0); // unknown
    MemChunk* mergedChunk = _mergedChunk.get();
    assert(mergedChunk);
    assert(mergedChunk->getAttributeDesc().getId() == attId);
    assert(mergedChunk->getAttributeDesc().getDefaultValue() == attr.getDefaultValue());
    assert(mergedChunk->getFirstPosition(false) == partialChunk->getFirstPosition(false));

    mergedChunk->merge(*partialChunk, query);

    return true;
}

void
MultiStreamArray::DefaultChunkMerger::mergePartialChunks(AttributeID attId,
                                                         std::vector<std::shared_ptr<MemChunk>> chunksIn,
                                                         std::vector<size_t> streams,
                                                         std::shared_ptr<Query> const& query)
{
    static const char* funcName = "DefaultChunkMerger::mergePartialChunk: ";
    LOG4CXX_TRACE(logger, funcName << " start");

    SCIDB_ASSERT(chunksIn.size());
    SCIDB_ASSERT(streams.size());
    SCIDB_ASSERT(chunksIn.size() == streams.size());

    // check attId and type of chunk match across all partials
    const bool isEbm = isEmptyBitMap(chunksIn[0]);
    for(auto chunk : chunksIn) {
        SCIDB_ASSERT(attId == chunk->getAttributeDesc().getId());
        SCIDB_ASSERT(isEbm == isEmptyBitMap(chunk));
    }

    // transfer to toMerged
    typedef std::shared_ptr<MemChunk> ListElem_t;
    std::list<ListElem_t> wasMerged;
    std::list<ListElem_t> toMerge;
    for(auto chunk: chunksIn) {
        if (!isEbm) {
            _numElems += chunk->count();
        }
        toMerge.push_back(chunk);
    }

    // algorithm: (the whole point of the method accepting a list of partial chunks)
    // in a prior version, each partial chunk was merged sequentially into _mergedChunk.
    // this is necessarily O(n^2) in the number of partial chunks.
    // here, we perform a merge as in mergesort, which is O(n log n) in the number of
    // partial chunks.
    // This is a key performance aspect of redimension.  Do not change this without
    // measuring at instances > 16.
    auto curList= &toMerge;
    auto nextList=  &wasMerged;
    LOG4CXX_TRACE(logger, funcName << " " << attId << " merge loop");
    while(curList->size() > 1) {
        LOG4CXX_TRACE(logger, funcName << " " << attId << " merge loop pass");
        while(curList->size() >= 2) {
            std::shared_ptr<MemChunk> elemA= curList->front(); curList->pop_front();
            std::shared_ptr<MemChunk> elemB= curList->front(); curList->pop_front();
            elemA->merge(*elemB, query);
            nextList->push_back(elemA);
            LOG4CXX_TRACE(logger, funcName << " attId " << attId << " merged a pair");
        }
        if (curList->size()==1) {
            // transfer unpaired item to the next list
            nextList->push_back(curList->front());
            curList->pop_front();
        }
        assert(curList->size()==0);
        std::swap(curList, nextList);
    }
    LOG4CXX_TRACE(logger, funcName << " " << attId << " merge loop end");
    SCIDB_ASSERT(nextList->size() == 0);    // nothing left on nextList by mistake

    if(curList->size() > 0) {
        ASSERT_EXCEPTION(curList->size() == 1, "curList should have at most 1 entry");
        // Combine the item on the list with any data in _mergedChunk from an earlier call.
        // (NOTE: this pre-merged stuff lowers latency at the cost of additional cpu and
        //  memory loading which can hurt throughput.  So the design of maintaining
        //  _mergedChunk between calls should be revisited and compared against waiting
        // for all expected chunks to arrive prior to initiating the merging, which
        // would result in the least copying/comparison costs)

        std::shared_ptr<MemChunk> elem = curList->front();
        curList->pop_front();
        if(_mergedChunk) {
            _mergedChunk->setCount(0);      // make it re-count post-merge
            _mergedChunk->merge(*elem, query);
            LOG4CXX_TRACE(logger, funcName << " " << attId << " @@@ end @@@ new data merged ino existing _mergedChunk");
        } else {
            _mergedChunk = elem;            // preserves count
            _mergedChunk->setCount(0);      // make it re-count post-merge, since elem is not necessarily count-correct
            LOG4CXX_TRACE(logger, funcName << " " << attId << " @@@ end @@@ new data moved into null _mergedChunk");
        }
    } else {
        LOG4CXX_TRACE(logger, funcName << " " << attId << " @@@ end @@@ no new data this call");
    }
}

bool MultiStreamArray::DefaultChunkMerger::isEmptyBitMap(const std::shared_ptr<MemChunk>& chunk)
{
    AttributeDesc const* ebmAttr = chunk->getArrayDesc().getEmptyBitmapAttribute();
    return (ebmAttr != NULL && chunk->getAttributeDesc().getId() == ebmAttr->getId());
}

std::shared_ptr<MemChunk>
MultiStreamArray::DefaultChunkMerger::getMergedChunk(AttributeID attId,
                                                     const std::shared_ptr<Query>& query)
{
    static const char* funcName = "DefaultChunkMerger::getMergedChunk: ";
    SCIDB_ASSERT(_mergedChunk);
    checkChunkMagic(*_mergedChunk, funcName); // __PRETTY_FUNCTION__ is rather long due to args

    const bool isEbm = isEmptyBitMap(_mergedChunk);
    size_t mergedFootprint = _mergedChunk->getSize();
    size_t mergedNumElems = !isEbm ? _mergedChunk->count() : 0;

    assert(mergedFootprint>0);

    if (_numElems != mergedNumElems && !isEbm) {
        if (_isEnforceDataIntegrity) {
            stringstream ss;
            ss << "chunk " << CoordsToStr(_mergedChunk->getFirstPosition(false));
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_DATA_COLLISION)
            << ss.str();
        }

        if (!_hasDataIntegrityIssue) {
            LOG4CXX_WARN(logger, funcName
                         << "Data collision is detected in chunk at "
                         << CoordsToStr(_mergedChunk->getFirstPosition(false))
                         << " for attribute ID = " << _mergedChunk->getAttributeDesc().getId()
                         << ". Add log4j.logger.scidb.qproc.streamarray=TRACE to the log4cxx.properties file for more");
            _hasDataIntegrityIssue = true;
        } else {
            LOG4CXX_TRACE(logger, funcName
                          << "Data collision is detected in chunk at "
                          << CoordsToStr(_mergedChunk->getFirstPosition(false))
                          << " for attribute ID = " << _mergedChunk->getAttributeDesc().getId());
        }
    }

    if (_chunkSizeLimitMiB && mergedFootprint > _chunkSizeLimitMiB * MiB) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_TOO_LARGE)
            << mergedFootprint << _chunkSizeLimitMiB;
    }

    std::shared_ptr<MemChunk> result;
    _mergedChunk.swap(result);
    _numElems = 0;
    assert(result);
    assert(!_mergedChunk);
    return result;
}

/**
 * Multistream array constructor
 * @param n number of chunk streams
 * @param localStream local stream ID
 * @param arr array descriptor
 * @param query context
 */
MultiStreamArray::MultiStreamArray(size_t n,
                                   size_t localStream,
                                   ArrayDesc const& arr,
                                   bool enforceDataIntegrity,
                                   std::shared_ptr<Query>const& query)
: StreamArray(arr, false),
  _nStreams(n),
  _localStream(localStream),
  _enforceDataIntegrity(enforceDataIntegrity),
  _resultChunks(arr.getAttributes().size()),
  _chunkMergers(arr.getAttributes().size()),
  _readyPositions(arr.getAttributes().size()),
  _notReadyPositions(arr.getAttributes().size()),
  _currPartialStreams(arr.getAttributes().size()),
  _hasDataIntegrityIssue(false),
  _isDataframe(arr.isDataframe()),
  _currMinPos(arr.getAttributes().size())
{
    static const char *funcName = "MultiStreamArray::MultiStreamArray: " ;
    assert(query);
    _query=query;
    list<size_t> notReadyPos;
    for (size_t i=0; i < _nStreams; ++i) {
        notReadyPos.push_back(i);
    }
    for (AttributeID attId=0; attId < _notReadyPositions.size(); ++attId) {
        list<size_t>& current = _notReadyPositions[attId];
        current.insert(current.end(), notReadyPos.begin(), notReadyPos.end());
        _chunkMergers[attId] = std::make_shared<DefaultChunkMerger>(_enforceDataIntegrity);
    }

    // a delay value for nextChunk()
    // defaults to 1ms * _nStreams
    // overridden by CONFIG_MULTISTREAM_NEXT_POS_THROW_NS when >= 0
    // (in case we need to override, as we have not validated 1ms * _nStreams for more than 48 streams)

    int configNanosec = Config::getInstance()->getOption<int>(CONFIG_MULTISTREAM_NEXT_POS_THROW_NS);
    if(configNanosec >= 0) {
        _nextChunkPosThrowDelayInNanosec = configNanosec ;
        LOG4CXX_TRACE(logger, funcName << " _nextChunkPosThrowDelayInNanosec from config.ini: "
                                       << _nextChunkPosThrowDelayInNanosec);
    } else {
        // rationale for 1ms * nStreams
        // in nextChunk(), when the current position is not one assigned to this instance
        // the code calls getNextStreamPositions(), which polls the other instances
        // if an instance's work ahead queue is full, and there is still no chunk in it with a position
        // belonging to this instance, then the getNextStreamPosition() will throw.
        // In this case, asking rapidly again for the next stream position generates traffic to that instances
        // which slows service to the instances for which it does have chunks waiting in its work ahead queue.
        // so we want to back off in this condition.  The chance that the next chunk added to the workahead is
        // for us is 1/nStreams, while the chance the other instance will be delayed by this type of busy polling
        // increases with nStreams, therefore to keep the load on the other instance from rising as nInstances
        // grows, we can back off proportionally to the number of instances/streams.
        // 1 ms has be observed to work well for a few instances, and 48 milliseconds has been observed to
        // work well for 48 instances, so we will use 1 millisecond * _nStreams.
        // If a situation arises in practice where this is not a good value, it can be overridden
        // by using CONFIG_MULTISTREAM_NEXT_POS_THROW_NS (above) and we can return
        // here to improve back off mechanism in a later release.
        _nextChunkPosThrowDelayInNanosec = n * 1000 * 1000;  // n milliseconds, appropriate for pullSG,
        LOG4CXX_TRACE(logger, funcName << " _nextChunkPosThrowDelayInNanosec @ 1ms * _nStreams: "
                                       << _nextChunkPosThrowDelayInNanosec);
    }
}

ConstChunk const*
MultiStreamArray::nextChunk(const AttributeDesc& attId_in, MemChunk& chunk)
{
    auto attId = attId_in.getId();
    static const char *funcName = "MultiStreamArray::nextChunk: ";

    ASSERT_EXCEPTION( (attId < getArrayDesc().getAttributes().size()) , funcName);
    assert(attId < _resultChunks.size());
    assert(attId < _chunkMergers.size());
    assert(attId < _currMinPos.size());
    assert(attId < _notReadyPositions.size());
    assert(attId < _readyPositions.size());
    assert(attId < _currPartialStreams.size());

    list<size_t>& notReadyPos = _notReadyPositions[attId];
    PositionMap& readyPos = _readyPositions[attId];

    if (logger->isTraceEnabled()) {
        for (list<size_t>::iterator it=notReadyPos.begin(); it != notReadyPos.end(); ++it) {
            LOG4CXX_TRACE(logger, funcName << "NOT ready streams attId= " << attId
                         <<", stream="<< (*it));
        }
    }

    while (true) {

        // move chunks from notReadyPos to readyPos if now ready
        getAllStreamPositions(readyPos, notReadyPos, attId);
        LOG4CXX_TRACE(loggerVerbose, funcName << " readyPos.size() " << readyPos.size()
                                              << " notReadyPos.size() " << notReadyPos.size()
                                              << " attId " << attId);

        list<size_t>& currPartialStreams = _currPartialStreams[attId];

        if (readyPos.empty() && currPartialStreams.empty()) {
            // no more remote chunks
            LOG4CXX_TRACE(loggerVerbose, funcName << "EOF - no more chunks attId= " << attId);
            return NULL;
        }

        logReadyPositions(readyPos, attId);

        bool notMyStream = false;
        if (currPartialStreams.empty()) {
            // starting a new chunk, find all partial chunk streams

            assert(_currMinPos[attId].empty());

            PositionMap::value_type const& minElem = readyPos.top();
            _currMinPos[attId] = minElem.getCoords();
            notMyStream = (minElem.getDest() != _localStream);

            if (_resultChunks[attId] && !notMyStream) {
                scidb::CoordinatesLess comp;
                if (!comp(_resultChunks[attId]->getFirstPosition(false), _currMinPos[attId])) {
                    if (isEnforceDataIntegrity()) {
                        throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CHUNK_POSITION_OUT_OF_ORDER)
                        << CoordsToStr(_currMinPos[attId]);
                    }
                    if (!_hasDataIntegrityIssue) {
                        LOG4CXX_WARN(logger, funcName << "Data chunk at position "
                                     << CoordsToStr(_currMinPos[attId])
                                     << " for attribute ID = " << attId
                                     << " is received out of (row-major) order. Add "
                                     "log4j.logger.scidb.qproc.streamarray=TRACE to the "
                                     "log4cxx config file for more detail");
                        _hasDataIntegrityIssue = true;
                    } else {
                        LOG4CXX_TRACE(logger, funcName << "Data chunk at position "
                                      << CoordsToStr(_currMinPos[attId])
                                      << " for attribute ID = " << attId
                                      << " is received out of (row-major) order");
                    }
                }
                LOG4CXX_TRACE(logger, funcName << "clearing old chunk attId= " << attId);
                _resultChunks[attId].reset();
            }

            // move from readyPos to currPartialStreams while readyTop is not currMinPos
            while (!readyPos.empty()) {
                if (readyPos.top().getCoords() != _currMinPos[attId]) { break; }
                assert(notMyStream == (readyPos.top().getDest() != _localStream));
                currPartialStreams.push_back(readyPos.top().getSrc());
                readyPos.pop();
            }
        }

        if (logger->isTraceEnabled()) {
            for (list<size_t>::iterator it=currPartialStreams.begin();
                 it != currPartialStreams.end(); ++it) {
                LOG4CXX_TRACE(logger, funcName << "partial chunk attId= " << attId
                              <<", stream="<< *it
                              <<", isMyStream="<<!notMyStream);
            }
        }

        // many stream positions are for chunks that are not assigned to this instance
        if (notMyStream) {
            _currMinPos[attId].clear();

            // check for position updates that might define _currMinPos for us
            try {
                getNextStreamPositions(readyPos, notReadyPos, currPartialStreams, attId);
            } catch (RetryException& e) {
                LOG4CXX_TRACE(loggerSleep, funcName << "getNextStreamPositions threw"
                                           << " sleep ns " << _nextChunkPosThrowDelayInNanosec
                                           << " readyPos.size() " << readyPos.size()
                                           << " notReadyPos.size() " << notReadyPos.size()
                                           << " currPartialStreams.size() " << currPartialStreams.size());

                // NOTE: getNextStreamPositions() throws when the next chunk for us is indeterminate
                // in this case we need to throttle the rate of getting position updates to not cause
                // work-interfering load to an instance which only has work for other instances in its send queue
                // for this instance in its work-ahead.
                // See ctor for commentary about tuning _nextChunkPosThrowDelayInNanosec

                Thread::nanoSleep(_nextChunkPosThrowDelayInNanosec);
                throw;
            }
            // Not my stream and did not throw.
            // The following legacy code is left unmodified until we can determine whether
            // it has any residual purpose.
            // XXX TODO: this amounts to busy-polling of the stream generating extra network traffic etc.
            // we should probably just back-off for a few mils ...
            const uint64_t oneMilSec = 1*1000*1000;
            Thread::nanoSleep(oneMilSec);
        } else {
            break;
        }
    }

    list<size_t>& currPartialStreams = _currPartialStreams[attId];
    mergePartialStreams(readyPos, notReadyPos, currPartialStreams, attId);

    assert(currPartialStreams.empty());
    assert(_resultChunks[attId]);
    assert(_resultChunks[attId]->getFirstPosition(false) == _currMinPos[attId]);

    LOG4CXX_TRACE(logger, funcName <<"done with chunk attId= " << attId
                  <<", resultChunk=" << _resultChunks[attId].get()
                  <<", resultChunk size=" << _resultChunks[attId]->getSize());

    LOG4CXX_TRACE(logger, funcName <<"done with chunk attId=" <<attId
                  << ", minPos="<< _currMinPos[attId]);

    _currMinPos[attId].clear();
    const MemChunk* result = _resultChunks[attId].get();
    return result;
}

void
MultiStreamArray::getAllStreamPositions(PositionMap& readyPos,
                                        list<size_t>& notReadyPos,
                                        const AttributeID attId)
{
    // NOTE: very similar to getNextStreamPositions
    // TODO: consider comparing and refactoring
    static const char *funcName = "MultiStreamArray::getAllStreamPositions: ";
    Coordinates pos;
    std::shared_ptr<RetryException> err;

    // erase notReadyPositions that don't throw
    for (list<size_t>::iterator iter = notReadyPos.begin();
         iter != notReadyPos.end();) {

        const size_t stream = *iter;
        pos.clear();
        try {
            size_t destStream(_localStream);
            if (nextChunkPos(stream, attId, pos, destStream)) {
                assert(!pos.empty());
                readyPos.push(SourceAndDest(pos, stream, destStream));
                LOG4CXX_TRACE(logger, funcName << "ready stream found attId= " << attId
                              <<", src stream="<< stream
                              <<", dest stream="<< destStream
                              <<", pos="<< pos);
            } else {
                // TODO: it would be nice to add a counter and announce
                //       only for the first and last stream to handle
                //       when there are many many instances (streams)
                LOG4CXX_TRACE(loggerVerbose, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
            }
            iter = notReadyPos.erase(iter);
            continue;
        } catch (RetryException& e) {
            LOG4CXX_TRACE(loggerVerbose, funcName << "next position is NOT ready attId= " << attId
                          <<", stream="<< stream);

            if (!err) {
                err = std::dynamic_pointer_cast<RetryException>(e.clone());
            }
        }
        ++iter;
    }
    if (err) {
        // some positions are still missing
        throw *err;
    }
}

void
MultiStreamArray::mergePartialStreams(PositionMap& readyPos,
                                      list<size_t>& notReadyPos,
                                      list<size_t>& currPartialStreams,
                                      const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::mergePartialStreams: ";
    Coordinates pos;
    std::shared_ptr<RetryException> err;
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    assert(_chunkMergers[attId]);

    LOG4CXX_TRACE(logger, funcName << " entered, attId: " << attId);

    std::shared_ptr<MemChunk> mergeChunk = std::make_shared<MemChunk>(); // can't mergeChunk->count()!

    typedef std::pair<shared_ptr<MemChunk>, size_t> MemChunk_Stream_t;
    std::list<MemChunk_Stream_t> toMerge;

    // get all partial chunks
    size_t index=0;
    for (list<size_t>::iterator it=currPartialStreams.begin();
         it != currPartialStreams.end(); index++) {

        const size_t stream = *it;
        assert(stream<_nStreams);

        ConstChunk const* next = NULL;
        try {

            next = nextChunkBody(stream, attId, *mergeChunk);
            if (!next) {
                LOG4CXX_TRACE(loggerVerbose, funcName << "nextChunkBody false, throwing");
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_FETCH_CHUNK_BODY);
            }
            assert(next == mergeChunk.get());

            // record next position, request next chunk
            pos.clear();
            try {
                size_t destStream(_localStream);
                if (nextChunkPos(stream, attId, pos, destStream)) {
                    assert(!pos.empty());
                    readyPos.push(SourceAndDest(pos, stream, destStream));
                    LOG4CXX_TRACE(logger, funcName << "next position is ready attId= " << attId
                                  <<", src stream="<< stream
                                  <<", dst stream="<< destStream);
                } else {
                    // TODO would be nice to announce only fo0r first and last stream
                    LOG4CXX_TRACE(loggerVerbose, funcName << "nextChunkPos false: next position is not available"
                                  << " attId " << attId
                                  << " stream " << stream);
                }
            } catch (RetryException& e) {
                LOG4CXX_TRACE(loggerVerbose, funcName << "RetryException: next position is not ready yet "
                              << " attId " << attId
                              << " stream "<< stream);
                notReadyPos.push_back(stream);
            }
        } catch (RetryException& e) {
            LOG4CXX_TRACE(logger, funcName << "next chunk is NOT ready attId= " << attId
                          <<", stream="<< stream << " (RetryException from nextChunkBody)");

            assert(next==NULL);
            if (!err) {
                err = std::dynamic_pointer_cast<RetryException>(e.clone());
            }
            ++it;
            continue;
        }
        assert(next);

        it = currPartialStreams.erase(it);

        if (_currMinPos[attId] != next->getFirstPosition(false)) {
            ostringstream oss;
            oss << funcName << "Unexpected chunk position, attId=" << attId
                << ", _currMinPos[attId]=" << CoordsToStr(_currMinPos[attId])
                << " but next chunk's position is " << CoordsToStr(next->getFirstPosition(false));
            ASSERT_EXCEPTION_FALSE(oss.str());
        }

        LOG4CXX_TRACE(logger, funcName << "index " << index << " attId=" << attId << ", stream=" << stream
                      << ", dframe=" <<_isDataframe << ", next (chunkBody)=" << next
                      << " at " << CoordsToStr(_currMinPos[attId])
                      <<", next->getSize()=" << next->getSize() << ", err=" << static_cast<bool>(err));

        // new: for tree merging, collect them all before merging any
        toMerge.push_back(MemChunk_Stream_t(mergeChunk, stream));  // add to the list
        mergeChunk = std::make_shared<MemChunk>();  // fresh (empty) memchunk
        SCIDB_ASSERT(mergeChunk.get());
    }

    // now we have a list of partial chunks
    // convert the list to vector as required by
    // current tree-merging method
    // (TODO: consider passing the lists OR
    //        consider accumulating in vectors)
    std::vector<std::shared_ptr<MemChunk>> chunksIn;
    std::vector<size_t> streams;
    for(auto const & pair : toMerge) {
        chunksIn.push_back(pair.first);
        streams.push_back(pair.second);
    }

    if(toMerge.size()) {
        LOG4CXX_TRACE(logger, funcName << " calling mergePartialChunks (tree merging)");
        _chunkMergers[attId]->mergePartialChunks(attId, chunksIn, streams, query);
    }

    if (err) { throw *err; }    // can't throw until *after* the merging above, otherwise loss of data

    // no errs implies chunk is complete and now is published
    _resultChunks[attId] = _chunkMergers[attId]->getMergedChunk(attId, query);

    LOG4CXX_TRACE(logger, funcName << " == END == attId: " << attId << " with resultChunk= " << (void*)_resultChunks[attId].get());
}

void
MultiStreamArray::getNextStreamPositions(PositionMap& readyPos,
                                         list<size_t>& notReadyPos,
                                         list<size_t>& currPartialStreams,
                                         const AttributeID attId)
{
    // NOTE: very similar to getAllStreamPositions
    // TODO: consider comparing and refactoring
    static const char *funcName = "MultiStreamArray::getNextStreamPositions: ";
    Coordinates pos;
    std::shared_ptr<RetryException> err;
    // re-request next stream positions
    for (list<size_t>::iterator it=currPartialStreams.begin();
         it != currPartialStreams.end(); ++it) {

        const size_t stream = *it;
        assert(stream<_nStreams);

        // record next position, request next chunk
        pos.clear();
        try {
            size_t destStream(_localStream);
            if (nextChunkPos(stream, attId, pos, destStream)) {
                assert(!pos.empty());
                readyPos.push(SourceAndDest(pos, stream, destStream));
                LOG4CXX_TRACE(logger, funcName << "next position is ready attId= " << attId
                              <<", src stream="<< stream
                              <<", dst stream="<< destStream);
            } else {
                LOG4CXX_TRACE(logger, funcName << "next position is not available attId= " << attId
                              <<", stream="<< stream);
            }
        } catch (RetryException& e) {
            LOG4CXX_TRACE(logger, funcName << "next position is NOT ready attId= " << attId
                          <<", stream="<< stream);
            notReadyPos.push_back(stream);
            if (!err) {
                err = std::dynamic_pointer_cast<RetryException>(e.clone());
            }
            continue;
        }
    }
    currPartialStreams.clear();
    if (err) { throw *err; }
}


void
MultiStreamArray::logReadyPositions(PositionMap& readyPos,
                                    const AttributeID attId)
{
    static const char *funcName = "MultiStreamArray::logReadyPositions: ";
    if (!logger->isTraceEnabled()) {
        return;
    }
    // a bit ugly but only when tracing
    PositionMap tmp;
    while (!readyPos.empty()) {
        PositionMap::value_type const& top = readyPos.top();
        tmp.push(top);
        LOG4CXX_TRACE(logger, funcName << "ready streams attId= " << attId
                      <<", src stream="<< top.getSrc()
                      <<", dst stream="<< top.getDest()
                      <<", pos="<< top.getCoords());
        readyPos.pop();
    }
    while (!tmp.empty()) {
        PositionMap::value_type const& top = tmp.top();
        readyPos.push(top);
        tmp.pop();
    }
}

}  // scidb
#endif
