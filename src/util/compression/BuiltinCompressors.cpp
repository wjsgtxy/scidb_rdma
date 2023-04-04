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

#include <util/compression/BuiltinCompressors.h>
#include <util/Utility.h>  // for safe_static_cast

#include <bzlib.h>
#include <zlib.h>

#include <string.h> // for memcpy

namespace scidb
{
    const char* NoCompression::getName() const
    {
        return "no compression";
    }

    CompressorType NoCompression::getType() const
    {
        return CompressorType::NONE;
    }

    size_t NoCompression::compress(void* dst, const void* src, size_t srcSize)
    {
        return srcSize;
    }

    size_t
    NoCompression::decompress(void* dst, size_t dstSize, const void* src, size_t srcSize)
    {
        memcpy(dst, src, srcSize);
        return srcSize;
    }


    int ZlibCompressor::compressionLevel = Z_DEFAULT_COMPRESSION;

    const char* ZlibCompressor::getName() const
    {
        return "zlib";
    }

    CompressorType ZlibCompressor::getType() const
    {
        return CompressorType::ZLIB;
    }

    size_t ZlibCompressor::compress(void* dst, const void* src, size_t srcSize)
    {
        static_assert(std::is_same<uLongf, size_t>::value,
                      "uLongf and size_t should be the same");
        uLongf dstSize = srcSize;
        int rc = compress2(reinterpret_cast<Bytef*>(dst),        // dest
                           &dstSize,                             // destLen
                           reinterpret_cast<const Bytef*>(src),  // source
                           srcSize,                              // sourceLen
                           compressionLevel                      // level
        );
        return rc == Z_OK ? dstSize : srcSize;
    }

    size_t
    ZlibCompressor::decompress(void* dst, size_t dstSize, const void* src, size_t srcSize)
    {
        int rc = uncompress(reinterpret_cast<Bytef*>(dst),        // dest
                            &dstSize,                             // destlen
                            reinterpret_cast<const Bytef*>(src),  // source
                            srcSize                               // sourceLen
        );
        return rc == Z_OK ? dstSize : 0;
    }


    int BZlibCompressor::blockSize100k = 4;
    int BZlibCompressor::workFactor = 9;

    const char* BZlibCompressor::getName() const
    {
        return "bzlib";
    }

    CompressorType BZlibCompressor::getType() const
    {
        return CompressorType::BZLIB;
    }

    size_t BZlibCompressor::compress(void* dst, const void* src, size_t srcSize)
    {
        unsigned int dstSize = safe_static_cast<unsigned int>(srcSize);
        int rc = BZ2_bzBuffToBuffCompress(reinterpret_cast<char*>(dst),  // dest
                                          &dstSize,                      // destLen
                                          const_cast<char*>(reinterpret_cast<const char*>(src)),  // source
                                          safe_static_cast<unsigned int>(srcSize),  // sourceLen
                                          blockSize100k,
                                          0,  // verbosity
                                          workFactor);
        return rc == BZ_OK ? dstSize : srcSize;
    }

    size_t
    BZlibCompressor::decompress(void* dst, size_t dstSize, const void* src, size_t srcSize)
    {
        unsigned int dstLen = safe_static_cast<unsigned int>(dstSize);
        int rc = BZ2_bzBuffToBuffDecompress(reinterpret_cast<char*>(dst),  // dest
                                            &dstLen,                       // destLen
                                            const_cast<char*>(reinterpret_cast<const char*>(src)),  // source
                                            safe_static_cast<uint32_t>(srcSize),  // sourceLen
                                            0,                                    // small
                                            0                                     // verbosity
        );
        return rc == BZ_OK ? dstLen : 0;
    }

}
