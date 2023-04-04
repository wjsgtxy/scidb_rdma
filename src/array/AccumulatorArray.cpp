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

#include <array/AccumulatorArray.h>

#include <array/ChunkMaterializer.h>
#include <query/Query.h>

#include <log4cxx/logger.h>


namespace scidb {
    // Logger for accumulator array.
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.accumulatorarray"));

    //
    // AccumulatorArray
    //
    AccumulatorArray::AccumulatorArray(std::shared_ptr<Array> array,
                                       std::shared_ptr<Query>const& query)
    : Array(array),
      StreamArray(array->getArrayDesc(), false),
      iterators(array->getArrayDesc().getAttributes().size())
    {
        assert(query);
        _query=query;
    }
    ConstChunk const* AccumulatorArray::nextChunk(const AttributeDesc& attId, MemChunk& chunk)
    {
        if (!iterators[attId.getId()]) {
            iterators[attId.getId()] = getPipe(0)->getConstIterator(attId);
        } else {
            ++(*iterators[attId.getId()]);
        }
        if (iterators[attId.getId()]->end()) {
            return NULL;
        }
        ConstChunk const& inputChunk = iterators[attId.getId()]->getChunk();
        if (inputChunk.isMaterialized()) {
            return &inputChunk;
        }

        Address addr(attId.getId(), inputChunk.getFirstPosition(false));

        chunk.initialize(this, &desc, addr, inputChunk.getCompressionMethod());
        chunk.setBitmapChunk((Chunk*)&inputChunk);
        auto srcFlags = ChunkIterator::INTENDED_TILE_MODE;
        auto dstFlags = ChunkIterator::NO_EMPTY_CHECK |
                        ChunkIterator::SEQUENTIAL_WRITE;
        auto query = Query::getValidQueryPtr(_query);
        ChunkMaterializer materializer(inputChunk, srcFlags, desc.hasOverlap(), logger, query);
        materializer.write(chunk, dstFlags);

        return &chunk;
    }
}  // scidb
#endif // ifndef SCIDB_CLIENT
