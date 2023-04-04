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

#ifndef CHUNK_MATERIALIZER_H_
#define CHUNK_MATERIALIZER_H_

#include <query/Query.h>

#include <functional>
#include <memory>
#include <log4cxx/logger.h>

namespace scidb {

class ConstChunk;
class MemChunk;

class ChunkMaterializer final
{
    /// references the chunk to materialize
    const ConstChunk& _srcChunk;

    /// source chunk iterator flags
    int _srcFlags;

    /// indicates chunk overlap
    bool _hasOverlap;

    /// pointer to a logger instance from the calling context
    log4cxx::LoggerPtr _logger;

    /// pointer to the query instance tied to the new materialized chunk
    std::shared_ptr<Query> _query;

  public:
    enum WriteFlags {
        /// No write flags set (default)
        NONE = 0,

        /// write call should unpin the dstChunk on return
        SHOULD_UNPIN = 1,

        /// the source iter needs to be checked for tile mode
        CHECK_TILE_MODE = 2
    };

    ChunkMaterializer() = delete;
    ChunkMaterializer(const ChunkMaterializer&) = delete;
    ChunkMaterializer(ChunkMaterializer&&) = delete;
    ~ChunkMaterializer() = default;

    /**
     * Constructor
     *
     * @param srcChunk   Reference to the source chunk to materialize.
     * @param srcFlags   Source chunk iterator flags.
     * @param hasOverlap Indicates if the source chunk has chunk overlap.
     * @param logger     Optional pointer to a logger instance from the calling context.
     * @param query      Optional pointer to a query instance tied to the new materialized chunk.
     */
    ChunkMaterializer(const ConstChunk& srcChunk,
                      int srcFlags,
                      bool hasOverlap,
                      log4cxx::LoggerPtr logger,
                      std::shared_ptr<Query> query);

    /**
     * Writes the data referenced by the source chunk passed at construction time into a new
     * in-memory materialized chunk.
     *
     * @param dstChunk   The chunk into which the materialized data will be written.
     * @param dstFlags   Destination chunk iterator flags.
     * @param writeFlags A bit field comprised of flags from the WriteFlags enumeration.
     */
    void write(MemChunk& dstChunk,
               int dstFlags,
               WriteFlags writeFlags = WriteFlags::NONE);
};

}  // namespace scidb

#endif  // CHUNK_MATERIALIZER_H_
