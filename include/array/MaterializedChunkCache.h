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

#ifndef MATERIALIZED_CHUNK_CACHE_H_
#define MATERIALIZED_CHUNK_CACHE_H_

#include <log4cxx/logger.h>

namespace scidb {

class ConstChunk;
class MemChunk;

class MaterializedChunkCache final
{
    const ConstChunk& _srcChunk;
    log4cxx::LoggerPtr _logger;
    MemChunk* _materializedChunk;

  public:
    MaterializedChunkCache() = delete;
    ~MaterializedChunkCache();

    /**
     * Constructor
     *
     * @param srcChunk the chunk to materialize
     * @param logger a pointer to a logger instance from the calling context
     */
    MaterializedChunkCache(const ConstChunk& srcChunk,
                           log4cxx::LoggerPtr logger);

    /**
     * Indicates if the cache has a materialized chunk or not.
     * @return true if the cache has a materialized chunk populated, false if not
     */
    bool hasChunk() const;

    /**
     * Returns a materialized chunk, materializing it if it has not yet
     * been materialized.
     *
     * @return pointer to the materialized chunk
     */
    MemChunk* get();

    /**
     * Forcibly clears the chunk cache by deleting the materialized chunk.
     */
    void clear();
};

}  // namespace scidb

#endif  // MATERIALIZED_CHUNK_CACHE_H_
