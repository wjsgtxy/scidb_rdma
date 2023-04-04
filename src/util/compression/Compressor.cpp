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
 * @brief Compressors implementation
 *
 */

#include <util/compression/Compressor.h>
#include "BuiltinCompressors.h"

#include <array/Chunk.h>

namespace scidb
{
    CompressorFactory CompressorFactory::instance;

    void CompressorFactory::registerCompressor(Compressor* compressor)
    {
        compressors.push_back(compressor);
    }

    Compressor* CompressorFactory::getCompressor(CompressorType type) const
    {
        return compressors[static_cast<size_t>(type)];
    }

    CompressorFactory::CompressorFactory()
    {
        compressors.resize(static_cast<size_t>(CompressorType::MAX_DEFINED));
        compressors[static_cast<size_t>(CompressorType::NONE)] = new NoCompression();
        compressors[static_cast<size_t>(CompressorType::ZLIB)] = new ZlibCompressor();
        compressors[static_cast<size_t>(CompressorType::BZLIB)] = new BZlibCompressor();
    }

    CompressorFactory::~CompressorFactory()
    {
        for (auto  compressor : compressors) {
            delete compressor;
        }
    }

    size_t Compressor::compress(void* dst, const ConstChunk& srcChunk, size_t srcSize)
    {
        return compress(dst, srcChunk.getConstData(), srcSize);
    }

    size_t Compressor::decompress(Chunk& dstChunk, void const* src, size_t srcSize)
    {
        return decompress(dstChunk.getWriteData(),
                          dstChunk.getSize(),
                          src,
                          srcSize);
    }


} // namespace scidb
