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

#include <array/MaterializedChunkCache.h>
#include <array/ChunkMaterializer.h>
#include <array/ArrayDesc.h>
#include <array/ConstChunk.h>
#include <array/MemChunk.h>

namespace scidb {

MaterializedChunkCache::MaterializedChunkCache(const ConstChunk& srcChunk,
                                               log4cxx::LoggerPtr logger)
    : _srcChunk(srcChunk)
    , _logger(logger)
    , _materializedChunk(nullptr)
{
}

MaterializedChunkCache::~MaterializedChunkCache()
{
    clear();
}

bool MaterializedChunkCache::hasChunk() const
{
    return _materializedChunk;
}

MemChunk* MaterializedChunkCache::get()
{
    if (_materializedChunk == nullptr ||
        _materializedChunk->getFirstPosition(false) != _srcChunk.getFirstPosition(false)) {
        if (_materializedChunk == nullptr) {
            _materializedChunk = new MemChunk();
        }

        _materializedChunk->initialize(_srcChunk);
        _materializedChunk->setBitmapChunk((Chunk*)_srcChunk.getBitmapChunk());
        auto srcFlags =
            (_srcChunk.getArrayDesc().getEmptyBitmapAttribute() == nullptr ? ChunkIterator::IGNORE_DEFAULT_VALUES : 0) |
            ChunkIterator::INTENDED_TILE_MODE |
            (_materializedChunk->getArrayDesc().hasOverlap() ? 0 : ChunkIterator::IGNORE_OVERLAPS);
        auto dstFlags = ChunkIterator::ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE;
        ChunkMaterializer materializer(_srcChunk, srcFlags, _srcChunk.getArrayDesc().hasOverlap(), _logger, nullptr);
        materializer.write(*_materializedChunk, dstFlags, ChunkMaterializer::SHOULD_UNPIN);
    }
    return _materializedChunk;
}

void MaterializedChunkCache::clear()
{
    delete _materializedChunk;
    _materializedChunk = nullptr;
}

}  // namespace scidb
