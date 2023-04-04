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

#include <query/AggregateChunkMerger.h>

#include <query/Aggregate.h>

namespace{
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.aggregate.AggregateChunkMerger"));
}  // namespace

namespace scidb {

AggregateChunkMerger::AggregateChunkMerger(AggregatePtr const& agg, bool isEmptyable)
    : _aggregate(agg)
    , _isEmptyable(isEmptyable)
{
    assert(_aggregate);
}

AggregateChunkMerger::~AggregateChunkMerger() {}

void  AggregateChunkMerger::clear()
{
    _mergedChunk.reset();
}

bool
AggregateChunkMerger::mergePartialChunk(size_t stream,
                                             AttributeID attId,
                                             std::shared_ptr<MemChunk>& chunk,
                                             const std::shared_ptr<Query>& query)
{
    static const char* funcName = "AggregateChunkMerger::mergePartialChunk: ";
    assert(chunk);
    static const bool withoutOverlap = false;

    if (!_mergedChunk) {
        LOG4CXX_TRACE(logger, funcName
                      << "first partial chunk pos="<< CoordsToStr(chunk->getFirstPosition(withoutOverlap))
                      << " from stream="<<stream
                      << " attId="<<attId
                      << " count=" << chunk->count());
        _mergedChunk.swap(chunk);
        assert(!chunk);
        return false;
    }
    _mergedChunk->setCount(0); // unknown

    assert(_mergedChunk->getFirstPosition(withoutOverlap) == chunk->getFirstPosition(withoutOverlap));

    if (!_isEmptyable) {
        _mergedChunk->nonEmptyableAggregateMerge(*chunk, _aggregate, query);
        LOG4CXX_TRACE(logger, funcName
                      << "next non-emptyable partial chunk pos="
                      << CoordsToStr(chunk->getFirstPosition(withoutOverlap))
                      << " from stream="<<stream
                      <<" attId="<<attId
                      << " count=" << _mergedChunk->count());
    } else {
        _mergedChunk->aggregateMerge(*chunk, _aggregate, query);
        LOG4CXX_TRACE(logger, funcName
                      << "next emptyable partial chunk pos="<< CoordsToStr(chunk->getFirstPosition(withoutOverlap))
                      << " from stream="<<stream
                      << " attId="<<attId
                      << " count=" << _mergedChunk->count());
    }
    checkChunkMagic(*_mergedChunk, __PRETTY_FUNCTION__);
    return true;
}

void  AggregateChunkMerger::mergePartialChunks(AttributeID attId,
                                               std::vector<std::shared_ptr<MemChunk>> chunksIn,
                                               std::vector<size_t> streams,
                                               std::shared_ptr<Query> const& query)
{
    LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__);
    static const char* funcName = __PRETTY_FUNCTION__;

    SCIDB_ASSERT(chunksIn.size());
    SCIDB_ASSERT(streams.size());
    SCIDB_ASSERT(chunksIn.size() == streams.size());

    // check attId and type of chunk match across all partials
    static const bool IS_WITHOUT_OVERLAP = false;
    for(auto const & chunk : chunksIn) {
        SCIDB_ASSERT(chunk.get());
        SCIDB_ASSERT(attId == chunk->getAttributeDesc().getId());
        SCIDB_ASSERT(chunksIn[0]->getFirstPosition(IS_WITHOUT_OVERLAP) == chunk->getFirstPosition(IS_WITHOUT_OVERLAP));
    }

    for(size_t iii=0; iii <chunksIn.size(); iii++) {
        if (!_mergedChunk) {
            LOG4CXX_TRACE(logger, funcName
                          << "first partial chunk pos="<< CoordsToStr(chunksIn[iii]->getFirstPosition(IS_WITHOUT_OVERLAP))
                          << " from stream="<< streams[iii]
                          << " attId="<<attId
                          << " count=" << chunksIn[iii]->count());
            _mergedChunk.swap(chunksIn[iii]);      // couldn't this make the chunk->getFirst ... below blow up?
            SCIDB_ASSERT(_mergedChunk.get());      // confirm it was moved
            SCIDB_ASSERT(!chunksIn[iii]);          // confirm it was taken

            continue;   // consumed the first element
        }

        _mergedChunk->setCount(0); // unknown

        if (!_isEmptyable) {
            _mergedChunk->nonEmptyableAggregateMerge(*chunksIn[iii], _aggregate, query);
            LOG4CXX_TRACE(logger, funcName
                          << "next non-emptyable partial chunk pos="
                          << CoordsToStr(chunksIn[iii]->getFirstPosition(IS_WITHOUT_OVERLAP))
                          << " from stream="<< streams[iii]
                          <<" attId="<<attId
                          << " count=" << _mergedChunk->count());
        } else {
            _mergedChunk->aggregateMerge(*chunksIn[iii], _aggregate, query);
            LOG4CXX_TRACE(logger, funcName
                          << "next emptyable partial chunk pos="<< CoordsToStr(chunksIn[iii]->getFirstPosition(IS_WITHOUT_OVERLAP))
                          << " from stream="<<streams[iii]
                          << " attId="<<attId
                          << " count=" << _mergedChunk->count());
        }
        checkChunkMagic(*_mergedChunk, funcName);
    }
}

std::shared_ptr<MemChunk>
AggregateChunkMerger::getMergedChunk(AttributeID attId,
                                     const std::shared_ptr<Query>& query)
{
    static const char* funcName = "AggregateChunkMerger::getMergedChunk: ";
    std::shared_ptr<MemChunk> result;
    LOG4CXX_TRACE(logger, funcName
                  << "final chunk pos="<< CoordsToStr(_mergedChunk->getFirstPosition(false))
                  <<" attId="<<attId
                  << " count=" << _mergedChunk->count());
    result.swap(_mergedChunk);
    clear();
    assert(result);
    assert(!_mergedChunk);
    return result;
}

}  // namespace scidb
