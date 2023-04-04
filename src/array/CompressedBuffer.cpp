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
 * @file CompressedBuffer.cpp
 *
 * @brief class CompressedBuffer
 */

#include <array/CompressedBuffer.h>

#include <log4cxx/logger.h>

#include <system/Exceptions.h>
#include <util/arena/Malloc.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.CompressedBuffer"));

    const void* CompressedBuffer::getConstData() const
    {
        // PullSGContext.cpp:378, for example, assert that the pointer is allocated.
        // Don't need a writeable pointer for that, but perhaps this class should
        // provide an API to check that instead of allowing clients to infer state
        // from the pointer directly.

        return data;
    }

    void* CompressedBuffer::getWriteData()
    {
        return data;
    }

    size_t CompressedBuffer::getSize() const
    {
        return compressedSize;
    }

    void CompressedBuffer::allocate(size_t size)
    {
        data = arena::malloc(size);
        if (data == NULL) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        compressedSize = size;
    }

    void CompressedBuffer::reallocate(size_t size)
    {
        void *tmp = arena::realloc(data, size);
        if (tmp == NULL) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_CANT_ALLOCATE_MEMORY);
        }
        data = tmp;
        compressedSize = size;
    }

    void CompressedBuffer::free()
    {
        if (isDebug() && data && compressedSize) {
            memset(data, 0, compressedSize);
        }
       arena::free(data);
       data = NULL;
    }

    bool CompressedBuffer::pin() const
    {
        ASSERT_EXCEPTION(false, "CompressedBuffer::pin: ");
        return false;
    }

    void CompressedBuffer::unPin() const
    {
        ASSERT_EXCEPTION(false, "CompressedBuffer::unpin: ");
    }


    CompressorType CompressedBuffer::getCompressionMethod() const
    {
        return compressionMethod;
    }

    void CompressedBuffer::setCompressionMethod(CompressorType m)
    {
        compressionMethod = m;
    }

    size_t CompressedBuffer::getDecompressedSize() const
    {
        return decompressedSize;
    }

    void CompressedBuffer::setDecompressedSize(size_t size)
    {
        decompressedSize = size;
    }

    CompressedBuffer::CompressedBuffer(void* compressedData, CompressorType compressionMethod, size_t compressedSize, size_t decompressedSize)
    {
        data = compressedData;
        this->compressionMethod = compressionMethod;
        this->compressedSize = compressedSize;
        this->decompressedSize = decompressedSize;
    }

    CompressedBuffer::CompressedBuffer()
    {
        data = NULL;
        compressionMethod = CompressorType::NONE;
        compressedSize = 0;
        decompressedSize = 0;
    }

    CompressedBuffer::~CompressedBuffer()
    {
        free();
    }

    void CompressedBuffer::setData(void* addr)
    {
        data = addr;
    }
    void CompressedBuffer::setCompressedSize(size_t size)
    {
        compressedSize = size;
    }
    void CompressedBuffer::setDeCompressedSize(size_t size)
    {
        decompressedSize = size;
    }
}
