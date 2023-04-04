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
 * @file Chunk.cpp
 *
 * @brief class Chunk
 */
#include <array/Chunk.h>

#include <log4cxx/logger.h>

#include <array/CompressedBuffer.h>
#include <array/DeepChunkMerger.h>
#include <array/RLE.h>

#include <query/Aggregate.h>
#include <query/Query.h>
#include <system/Exceptions.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.Chunk"));

    void Chunk::allocateAndCopy(char const* input, size_t byteSize, size_t count,
                                const std::shared_ptr<Query>& query)
    {
        assert(getConstData()==NULL);
        assert(input!=NULL);

        allocate(byteSize);
        setCount(count);                            // one of the few times setCount may be set other than 0
                                                    // if the caller desires.
        memcpy(getWriteDataImpl(), input, byteSize);  // inside trust boundary

        write(query);
    }

    void Chunk::decompress(CompressedBuffer const& buf)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Chunk::decompress";
    }

    void Chunk::merge(ConstChunk const& with, std::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        setCount(0); // unknown
        // inside trust boundary
        auto dst = reinterpret_cast<char*>(getWriteDataImpl());

        // If dst already has data, merge; otherwise, copy.
        if ( dst != NULL )
        {
            if (isMemChunk() && with.isMemChunk()) {
                deepMerge(with, query);
            } else {
                shallowMerge(with, query);
            }
        }
        else {
            PinBuffer scope(with);
            // inside trust boundary
            auto src = reinterpret_cast<const char*>(with.getConstDataImpl());
            allocateAndCopy(src, with.getSize(), with.count(), query);
        }
    }

    void Chunk::deepMerge(ConstChunk const& with, std::shared_ptr<Query> const& query)
    {
        SCIDB_ASSERT(isMemChunk() && with.isMemChunk());  // TODO: if only allowed on MemChunk, why implemented on Chunk?

        Query::validateQueryPtr(query);

        // This is the only place in the code where the DeepChunkMerger is used, so let's
        // assume, for now, that it is within the trust boundary.
        DeepChunkMerger deepChunkMerger((MemChunk&)*this, (MemChunk const&)with, query);
        deepChunkMerger.merge();
    }

    void Chunk::shallowMerge(ConstChunk const& with, std::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        std::shared_ptr<ChunkIterator> dstIterator =
            getIterator(query,
                        ChunkIterator::APPEND_CHUNK |
                        ChunkIterator::APPEND_EMPTY_BITMAP |
                        ChunkIterator::NO_EMPTY_CHECK);
        std::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_DEFAULT_VALUES);
        if (getArrayDesc().getEmptyBitmapAttribute() != NULL) {
            while (!srcIterator->end()) {
                if (!dstIterator->setPosition(srcIterator->getPosition()))
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                Value const& value = srcIterator->getItem();
                dstIterator->writeItem(value);
                ++(*srcIterator);
            }
        } else { // ignore default values
            Value const& defaultValue = getAttributeDesc().getDefaultValue();
            while (!srcIterator->end()) {
                Value const& value = srcIterator->getItem();
                if (value != defaultValue) {
                    if (!dstIterator->setPosition(srcIterator->getPosition()))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    dstIterator->writeItem(value);
                }
                ++(*srcIterator);
            }
        }
        dstIterator->flush();
    }

    void Chunk::aggregateMerge(ConstChunk const& with,
                               AggregatePtr const& aggregate,
                               std::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        if (isReadOnly())
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);

        AttributeDesc const& attr = getAttributeDesc();

        if (aggregate->getStateType().typeId() != attr.getType())
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK);

        if (!attr.isNullable())
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE);//enforce equivalency w above merge()

        setCount(0);
        auto dst = reinterpret_cast<char*>(getWriteData());
        if (dst != NULL)
        {
            std::shared_ptr<ChunkIterator> dstIterator =
                getIterator(query,
                            ChunkIterator::APPEND_CHUNK |
                            ChunkIterator::APPEND_EMPTY_BITMAP |
                            ChunkIterator::NO_EMPTY_CHECK);
            std::shared_ptr<ConstChunkIterator> srcIterator = with.getConstIterator(ChunkIterator::IGNORE_NULL_VALUES);
            while (!srcIterator->end())
            {
                Value val = srcIterator->getItem();  // We need to make a copy here, because the mergeIfNeeded() call below may change it.

                if (!dstIterator->setPosition(srcIterator->getPosition())) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
                Value const& val2 = dstIterator->getItem();
                aggregate->mergeIfNeeded(val, val2);
                dstIterator->writeItem(val);

                ++(*srcIterator);
            }
            dstIterator->flush();
        }
        else
        {
            PinBuffer scope(with);
            auto src = reinterpret_cast<const char*>(with.getConstData());
            allocateAndCopy(src, with.getSize(), with.count(), query);
        }
    }

    void Chunk::nonEmptyableAggregateMerge(ConstChunk const& with,
                                           AggregatePtr const& aggregate,
                                           std::shared_ptr<Query> const& query)
    {
        Query::validateQueryPtr(query);

        if (isReadOnly())
        {
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);
        }

        AttributeDesc const& attr = getAttributeDesc();

        if (aggregate->getStateType().typeId() != attr.getType())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_TYPE_MISMATCH_BETWEEN_AGGREGATE_AND_CHUNK);
        }

        if (!attr.isNullable())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AGGREGATE_STATE_MUST_BE_NULLABLE);//enforce equivalency w above merge()
        }

        auto dst = reinterpret_cast<char*>(getWriteData());
        PinBuffer scope(with);
        if (dst != NULL)
        {
            std::shared_ptr<ChunkIterator>dstIterator = getIterator(query,
                                                                      ChunkIterator::APPEND_CHUNK |
                                                                      ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                      ChunkIterator::NO_EMPTY_CHECK);
            CoordinatesMapper mapper(with);
            ConstRLEPayload inputPayload(reinterpret_cast<const char*>(with.getConstData()));
            ConstRLEPayload::iterator inputIter(&inputPayload);
            Value val;
            position_t lpos;
            Coordinates cpos(mapper.getNumDims());

            while (!inputIter.end())
            {
                //Missing Reason 0 is reserved by the system meaning "group does not exist".
                //All other Missing Reasons may be used by the aggregate if needed.
                if (inputIter.isNull() && inputIter.getMissingReason() == 0)
                {
                    inputIter.toNextSegment();
                }
                else
                {
                    inputIter.getItem(val);
                    lpos = inputIter.getPPos();
                    mapper.pos2coord(lpos, cpos);
                    if (!dstIterator->setPosition(cpos))
                        throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                    Value const& val2 = dstIterator->getItem();
                    aggregate->mergeIfNeeded(val, val2);
                    dstIterator->writeItem(val);
                    ++inputIter;
                }
            }
            dstIterator->flush();
        }
        else {
            allocateAndCopy(reinterpret_cast<const char*>(with.getConstData()), with.getSize(), with.count(), query);
        }
    }

    void Chunk::setCount(size_t)
    {
    }

    void Chunk::truncate(Coordinate lastCoord)
    {
    }

}
