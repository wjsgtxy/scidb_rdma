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
 * @file ParallelAccumulatorArray.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <array/ParallelAccumulatorArray.h>

#include <stdio.h>
#include <vector>
#include <log4cxx/logger.h>
#include <array/ChunkMaterializer.h>
#include "system/Exceptions.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include "query/PhysicalOperator.h"
#include <util/PerfTimeScope.h>

namespace scidb
{
    using namespace std;

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

    //
    // ParallelAccumulatorArray
    //
    ConstChunk const* ParallelAccumulatorArray::ChunkPrefetchJob::getResult()
    {
        wait(PTW_JOB_CHUNK_PREFETCH, CAN_RAISE);
        if (!_resultChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return _resultChunk;
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::cleanup()
    {
        assert(arena::Arena::getArenaTLS() == _query->getArena());
        _iterator.reset();
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::~ChunkPrefetchJob()
    {
        LOG4CXX_TRACE(logger, "ChunkPrefetchJob::~ChunkPrefetchJob "<<this);
    }

    ParallelAccumulatorArray::ChunkPrefetchJob::ChunkPrefetchJob(const std::shared_ptr<ParallelAccumulatorArray>& array,
                                                                 AttributeID attr,
                                                                 const std::shared_ptr<Query>& query)
    : Job(query, std::string("ParallelAccumulatorArrayJob")),
      _arrayLink(array),
      _iterator(array->getPipe(0)->getConstIterator(array->getArrayDesc().getAttributes().findattr(attr))),
      _attrId(attr),
      _resultChunk(NULL)
    {
        assert(query);
        assert(arena::Arena::getArenaTLS() == query->getArena());
        _isCoordinator = query->isCoordinator();
    }

    void ParallelAccumulatorArray::ChunkPrefetchJob::run()
    {
        static int pass = 0; // DEBUG ONLY
        pass++;

        SCIDB_ASSERT(_query);
        ASSERT_EXCEPTION(_query == Query::getQueryPerThread(),
                         // Base class should have arranged this!
                         "ChunkPrefetchJob query != current thread_local?!");

        arena::ScopedArenaTLS arenaTLS(_query->getArena());
        std::shared_ptr<ParallelAccumulatorArray> acc = _arrayLink.lock();
        if (!acc) {
            return;
        }

        PerfTimeScope pts(PTS_JOB_RUN_PREFETCH);    // ParallelAccumulatorArray::ChunkPrefetchJob::run()

        LOG4CXX_TRACE(logger, "ChunkPrefetchJob::run pos="
                      << CoordsToStr(_pos));

        try {
            if (_iterator->setPosition(_pos)) {
                ConstChunk const& inputChunk = _iterator->getChunk();

                if (inputChunk.isMaterialized()) {
                    _resultChunk = &inputChunk;
                } else {
                    Address addr(_attrId, inputChunk.getFirstPosition(false));

                    _accChunk.initialize(acc.get(), &acc->desc, addr, inputChunk.getCompressionMethod());
                    _accChunk.setBitmapChunk((Chunk*)&inputChunk);
                    auto srcFlags = ChunkIterator::INTENDED_TILE_MODE;
                    auto dstFlags = ChunkIterator::NO_EMPTY_CHECK |
                                    ChunkIterator::SEQUENTIAL_WRITE;
                    ChunkMaterializer materializer(inputChunk, srcFlags, acc->desc.hasOverlap(), logger, _query);
                    materializer.write(_accChunk, dstFlags, ChunkMaterializer::SHOULD_UNPIN);
                    _resultChunk = &_accChunk;
                }
            } else {
                _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
        } catch (Exception const& x) {
            _error = x.clone();
        }
    }

    ParallelAccumulatorArray::ParallelAccumulatorArray(const std::shared_ptr<Array>& array)
    : Array(array),
      StreamArray(array->getArrayDesc(), /*emptyCheck:*/false),
      iterators(array->getArrayDesc().getAttributes().size()),
      activeJobs(iterators.size()),
      completedJobs(iterators.size())
    {
        if (iterators.size() <= 0) {
            LOG4CXX_FATAL(logger, "Array descriptor arrId = " << array->getArrayDesc().getId()
                          << " name = " << array->getArrayDesc().getId()
                          << " has no attributes ");
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INCONSISTENT_ARRAY_DESC);
        }
    }

    void ParallelAccumulatorArray::start(const std::shared_ptr<Query>& query)
    {
        PhysicalOperator::getGlobalQueueForOperators();
        AttributeID nAttrs = safe_static_cast<AttributeID>(iterators.size());
        assert(nAttrs>0);
        LOG4CXX_TRACE(logger, "ParallelAccumulatorArray::start nAttrs = "
                      << nAttrs);
        const auto& attrs = getArrayDesc().getAttributes();
        for (const auto& attr : attrs) {
            iterators[attr.getId()] = getPipe(0)->getConstIterator(attr);
        }
        int nPrefetchedChunks = Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE);
        do {
            for (const auto& attr : attrs) {
                auto owner = dynamic_pointer_cast<ParallelAccumulatorArray>(shared_from_this());
                std::shared_ptr<ChunkPrefetchJob> job =
                    std::make_shared<ChunkPrefetchJob>(owner, attr.getId(), query);
                doNewJob(job);
            }
        } while ((nPrefetchedChunks -= nAttrs) > 0);
    }

    ParallelAccumulatorArray::~ParallelAccumulatorArray()
    {
        LOG4CXX_TRACE(logger, "ParallelAccumulatorArray::~ParallelAccumulatorArray "
                      << this << ", active jobs #="<<activeJobs.size());
        for (size_t i = 0; i < activeJobs.size(); i++) {
            list< std::shared_ptr<ChunkPrefetchJob> >& jobs = activeJobs[i];
            for (list< std::shared_ptr<ChunkPrefetchJob> >::iterator j = jobs.begin(); j != jobs.end(); ++j) {
                (*j)->skip();
            }
        }
    }

    void ParallelAccumulatorArray::doNewJob(std::shared_ptr<ChunkPrefetchJob>& job)
    {
        AttributeID attrId = job->getAttributeID();
        LOG4CXX_TRACE(logger, "ParallelAccumulatorArray::doNewJob " <<
                      "attrId=" << attrId);
        if (!iterators[attrId]->end()) {
            job->setPosition(iterators[attrId]->getPosition());
            PhysicalOperator::getGlobalQueueForOperators()->pushJob(job);
            activeJobs[attrId].push_back(job);
            ++(*iterators[attrId]);
        }
    }


    ConstChunk const* ParallelAccumulatorArray::nextChunk(const AttributeDesc& attId_in, MemChunk& chunk)
    {
        auto attId = attId_in.getId();
        LOG4CXX_TRACE(logger, "ParallelAccumulatorArray::nextChunk "
                      << "attrId=" << attId);

        //XXX TODO: should this method be synchronized ?
        if (completedJobs[attId]) {
            doNewJob(completedJobs[attId]);
            completedJobs[attId].reset();
        }
        if (activeJobs[attId].empty()) {
            return NULL;
        }
        completedJobs[attId] = activeJobs[attId].front();
        activeJobs[attId].pop_front();
        return completedJobs[attId]->getResult();
    }
}
