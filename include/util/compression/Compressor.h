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

#ifndef COMPRESSOR_H_
#define COMPRESSOR_H_

#include <util/compression/CompressorType.h>

#include <vector>
#include <cstddef>

namespace scidb
{
class ConstChunk;
class Chunk;

/**
 * Compressor interface.
 */
class Compressor
{
public:
    /**
     * Compress data. If compressed size is greater ot equal than original size, then
     * no compression should be perfromed and original size should be returned
     * @param dst buffer for compressed data.
     * @param srcChunk chunk containing data to be compressed
     * @param srcSize size of data to be compressed (can be smaller than chunk.getSize())
     * @return size of compressed data (@b srcSize is returned in the case of compression failure).
     *
     * @pre dst buffer must have at least  "chunk->getSize()" allocated byte length.
     */
    size_t compress(void* dst, const ConstChunk& srcChunk, size_t srcSize);

    /**
     * Decompress data.
     * @param dstChunk destination chunk in which to put decompressed data.
     * @param src source buffer with compressed data
     * @param srcSize compressed size
     * @return decompressed size (0 should be returned in case of error)
     *
     * @pre dstChunk must have allocated space of at least chunk.getSize() for the buffer.
     *
     */
    size_t decompress(Chunk& dstChunk, void const* src, size_t srcSize);


    /**
     * Get compressor's name
     */
    virtual const char* getName() const = 0;

    /**
     * Get compressor's type
     */
    virtual CompressorType getType() const = 0;

    /**
     * Destructor
     */
    virtual ~Compressor() {}

    /**
     * Compress data.
     * @param dst destination buffer for the compressed data.
     * @param src source buffer with uncompressed data
     * @param srcSize the length (in bytes) of the compressed data buffer
     * @return compressed size (srcSize should be returned in case compression failure)
     *
     * @pre  The dst buffer must have been allocated to be at least srcSize bytes.
     */
    virtual size_t compress(void* dst, const void* src, size_t srcSize) = 0;

    /**
     * Decompress data.
     * @param dst destination buffer
     * @param dstLen the allocated length (in bytes) for the destination buffer.
     * @param src source buffer with compressed data
     * @param srcSize the length (in bytes) of the compressed data buffer
     * @return decompressed size (0 should be returned in case of error)
     *
     * @pre dst buffer must have at least dstlen bytes allocated.
     * @pre src buffer must have at least srcSize bytes allocated.
     */
    virtual size_t decompress(void* dst, size_t dstLen, const void* src, size_t srcSize) = 0;

};


/**
 * Collection of all available compressors.
 */
class CompressorFactory
{
    std::vector<Compressor*> compressors;
    static CompressorFactory instance;

public:
    CompressorFactory();

    void registerCompressor(Compressor* compressor);

    static const CompressorFactory& getInstance()
    {
        return instance;
    }

    Compressor * getCompressor(CompressorType type) const;

    const std::vector<Compressor*>& getCompressors() const
    {
        return compressors;
    }

    ~CompressorFactory();
};

} // namespace

#endif
