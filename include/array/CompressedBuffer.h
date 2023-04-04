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
 * @file CompressedBuffer.h
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef COMPRESSED_BUFFER_H_
#define COMPRESSED_BUFFER_H_

#include <array/SharedBuffer.h> // base class
#include <util/compression/CompressorType.h>

namespace scidb
{

/**
 * Buffer with compressed data.
 */
class CompressedBuffer : public SharedBuffer
{
private:
    size_t compressedSize;
    size_t decompressedSize;
    void*  data;
    CompressorType compressionMethod;

public:
    const void* getConstData() const override;
    void* getWriteData() override;
    size_t getSize() const override;
    void allocate(size_t size) override;
    void reallocate(size_t size) override;
    void free() override;
    bool pin() const override;
    void unPin() const override;

    CompressorType    getCompressionMethod() const;
    void   setCompressionMethod(CompressorType compressionMethod);

    size_t getDecompressedSize() const;
    void   setDecompressedSize(size_t size);

    CompressedBuffer(void* compressedData, CompressorType compressionMethod, size_t compressedSize, size_t decompressedSize);
    CompressedBuffer();
    ~CompressedBuffer();

    void setData(void* addr); // dz add
    void setCompressedSize(size_t size); // dz add
    void setDeCompressedSize(size_t size); // dz add
};

} // namespace
#endif /* COMPRESSED_BUFFER_H_ */
