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
 * @file SharedBuffer.h
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

#ifndef SHARED_BUFFER_H_
#define SHARED_BUFFER_H_

#include <cstddef>
namespace scidb
{
/** \brief SharedBuffer is an abstract class for binary data holding
 *
 * It's used in network manager for holding binary data.
 * Before using object you should pin it and unpin after using.
 */
class SharedBuffer
{
public:
    virtual ~SharedBuffer() { }
    /**
     * @return pointer to binary buffer.
     * Note, data is available only during object is live.
     */
    virtual void* getWriteData() = 0;

    /**
     * @return a const pointer to binary buffer.  you may only read this data
     * Note, data is available only during object is live.
     */
    virtual void const* getConstData() const = 0;

    /**
     * @return size of buffer in bytes
     */
    virtual size_t getSize() const = 0;

    /**
     * Method allocates memory for buffer inside implementation.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void allocate(size_t size);

    /**
     * Method reallocates memory for buffer inside implementation.
     * Old buffer content is copuied to the new location.
     * Implementor will manage this buffer itself.
     * @param size is a necessary size of memory to be allocated.
     */
    virtual void reallocate(size_t size);

    /**
     * Free memory. After execution of this method getData() should return NULL.
     */
    virtual void free();

    /**
     * Tell to increase reference counter to hold buffer in memory
     * @return true if buffer is pinned (need to be unpinned) false otherwise
     */
    virtual bool pin() const = 0;

    /**
     * Tell to decrease reference counter to release buffer in memory
     * to know when it's not needed.
     */
    virtual void unPin() const = 0;
};

} // namespace
#endif /* SHARED_BUFFER_H_ */
