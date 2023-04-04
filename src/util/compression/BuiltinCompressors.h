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


/*
 * Compressor.h
 *
 *  Created on: 07.01.2010
 *      Author: knizhnik@garret.ru
 *      Description: Compressor interface
 */

#ifndef BUILTIN_COMPRESSORS_H_
#define BUILTIN_COMPRESSORS_H_

#include <util/compression/Compressor.h>

#include <cstdint>

namespace scidb
{
    /**
     * Dummy compressor: used for the chunks which do not need compression
     */
    class NoCompression : public Compressor
    {
      public:
        const char* getName() const override;
        CompressorType getType() const override;
        size_t compress(void* dst, const void* src, size_t srcSize) override;
        size_t decompress(void* dst, size_t dstSize, const void* src, size_t srcSize) override;
    };

    /**
     * Compressor using Zlib library
     */
    class ZlibCompressor : public Compressor
    {
      public:
        const char* getName() const override;
        CompressorType getType() const override;
        size_t compress(void* dst, const void* src, size_t srcSize) override;
        size_t decompress(void* dst, size_t dstSize, const void* src, size_t srcSize) override;
      private:
        static int compressionLevel;
    };

    /**
     * Compressor using BZlib library
     */
    class BZlibCompressor : public Compressor
    {
      public:
        const char* getName() const override;
        CompressorType getType() const override;
        size_t compress(void* dst, const void* src, size_t srcSize) override;
        size_t decompress(void* dst, size_t dstSize, const void* src, size_t srcSize) override;
      private:
        static int workFactor; // [0..250], default 30
        static int blockSize100k; // [1..9], default 9
    };
}

#endif
