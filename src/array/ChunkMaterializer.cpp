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

#include <array/ChunkMaterializer.h>
#include <array/ConstChunk.h>
#include <array/MemChunk.h>

namespace scidb {

ChunkMaterializer::ChunkMaterializer(const ConstChunk& srcChunk,
                                     int srcFlags,
                                     bool hasOverlap,
                                     log4cxx::LoggerPtr logger,
                                     std::shared_ptr<Query> query)
    : _srcChunk(srcChunk)
    , _srcFlags(srcFlags)
    , _hasOverlap(hasOverlap)
    , _logger(logger)
    , _query(query)  // may be nullptr (the empty query)
{
    SCIDB_ASSERT(logger);
}

void ChunkMaterializer::write(MemChunk& dstChunk,
                              int dstFlags,
                              WriteFlags writeFlags)

{
    std::shared_ptr<ConstChunkIterator> src = _srcChunk.getConstIterator(_srcFlags);

    dstFlags = (src->getMode() & ChunkIterator::TILE_MODE) | dstFlags;
    std::shared_ptr<ChunkIterator> dst = dstChunk.getIterator(_query, dstFlags);

    auto shouldUnpin = writeFlags & SHOULD_UNPIN;
    auto checkTileMode = writeFlags & CHECK_TILE_MODE;
    size_t count = 0;
    while (!src->end()) {
        if (!dst->setPosition(src->getPosition())) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        try {
            dst->writeItem(src->getItem());
        }
        catch (const std::exception& e) {
            auto what = e.what();
            LOG4CXX_TRACE(_logger, "Caught exception on writeItem: " << what);
            if (shouldUnpin) {
                dst->unpin();
            }
            throw;
        }
        ++count;
        ++(*src);
    }

    // Checking tile mode verifies that the src iterator is not configured for tile mode, in
    // addition to checking for chunk overlap.  In the case that we don't need to check for
    // tile mode, then we care only about overlap.
    if ((checkTileMode && !(src->getMode() & ChunkIterator::TILE_MODE) && !_hasOverlap) ||
        (!checkTileMode && !_hasOverlap)) {
        dstChunk.setCount(count);
    }

    dst->flush();
    if (shouldUnpin) {
        dst->unpin();
    }
}

}  // namespace scidb
