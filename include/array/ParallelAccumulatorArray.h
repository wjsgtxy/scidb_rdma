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

/**
 * @file ParallelAccumulatorArray.h
 *
 */

#ifndef PARALLEL_ACCUMULATOR_ARRAY_H_
#define PARALLEL_ACCUMULATOR_ARRAY_H_

#include <vector>
#include <memory>

#include <array/MemArray.h>
#include <util/JobQueue.h>
#include <util/Semaphore.h>
#include <util/ThreadPool.h>
#include <array/StreamArray.h>

namespace scidb
{

/**
 * An array that helps an input array prefetch result chunk and deliver to the caller.
 */
class ParallelAccumulatorArray : public StreamArray
{
public:
    ParallelAccumulatorArray(const std::shared_ptr<Array>& pipe);
    ~ParallelAccumulatorArray();
    void start(const std::shared_ptr<Query>& query);

protected:
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

private:

    /**
     * @class ChunkPrefetchJob
     *
     * @details Unlike most (all other?) Job subclasses,
     * ChunkPrefetchJob is reused for multiple run() calls.  The
     * Job::_query shared pointer is kept alive across those calls.
     *
     * @p Donghui Z. writes: "The main reason is that it keeps in it a
     * shared_ptr<ConstArrayIterator> that tracks chunkPos, and this
     * state is needed after a chunk is prefetched.  The reason [why
     * the Job::_query pointer must be kept alive] is, we need to be
     * sure the query is not destructed before the ChunkPrefetchJob
     * is, so that the destructor of ChunkPrefetchJob will destructed
     * Value objects to the query arena."
     */
    class ChunkPrefetchJob : public Job
    {
    private:
        std::weak_ptr<ParallelAccumulatorArray> _arrayLink;
        std::shared_ptr<ConstArrayIterator> _iterator;
        Coordinates _pos;
        AttributeID _attrId;
        MemChunk    _accChunk;
        ConstChunk const* _resultChunk;
        bool _isCoordinator;

    public:
        ChunkPrefetchJob(const std::shared_ptr<ParallelAccumulatorArray>& array,
                         AttributeID attr, const std::shared_ptr<Query>& query);
        virtual ~ChunkPrefetchJob();

        void setPosition(Coordinates const& coord) {
            _resultChunk = NULL;
            _pos = coord;
        }

        AttributeID getAttributeID() const {
            return _attrId;
        }

        ConstChunk const* getResult();

        virtual void run();

        void cleanup();
    };

    void doNewJob(std::shared_ptr<ChunkPrefetchJob>& job);

    std::vector< std::shared_ptr<ConstArrayIterator> > iterators;
    std::vector< std::list< std::shared_ptr<ChunkPrefetchJob> > > activeJobs;
    std::vector< std::shared_ptr<ChunkPrefetchJob> > completedJobs;
};

} // namespace
#endif
