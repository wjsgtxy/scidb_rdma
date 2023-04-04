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
 * @file MemoryBuffer.h
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

#ifndef MEMORY_BUFFER_H_
#define MEMORY_BUFFER_H_

#include <array/SharedBuffer.h> // base class

namespace scidb
{

/**
 * An implementation of SharedBuffer using 'new char[]' to allocate memory.
 */
class MemoryBuffer : public SharedBuffer
{
private:
    char*  data;
    size_t size;
    bool   copied;

public:
    const void* getConstData() const override
    {
        return data;
    }

    void* getWriteData() override
    {
        return data;
    }

    size_t getSize() const override
    {
        return size;
    }

    void free() override
    {
        if (copied) {
            delete[] data;
        }
        data = NULL;
    }

    bool pin() const override
    {
        return false;
    }

    void unPin() const override
    { }

    ~MemoryBuffer()
    {
        free();
    }

    MemoryBuffer(const void* ptr, size_t len, bool copy = true) {
        if (copy) {
            data = new char[len];
            if (ptr != NULL) {
                memcpy(data, ptr, len);
            }
            copied = true;
        } else {
            data = (char*)ptr;
            copied = false;
        }
        size = len;
    }
};

} // namespace
#endif /* MEMORY_BUFFER_H_ */
