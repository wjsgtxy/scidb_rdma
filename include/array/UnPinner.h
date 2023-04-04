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
 * @file UnPinner.h
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

#ifndef UN_PINNER_H_
#define UN_PINNER_H_

#include <boost/noncopyable.hpp>

#include <array/Chunk.h>     // grr. inline implementation

namespace scidb
{
class Chunk;

/**
 * Constructed around a chunk pointer to automatically unpin the chunk on destruction.
 * May be initially constructed with NULL pointer, in which case the pointer may (or may not) be reset
 * to a valid chunk pointer.
 */
class UnPinner : public boost::noncopyable
{
private:
    Chunk* _buffer;

public:
    /**
     * Create an unpinner.
     * @param buffer the chunk pointer; can be NULL
     */
    UnPinner(Chunk* buffer) : _buffer(buffer)
    {}

    ~UnPinner()
    {
        if (_buffer)
        {
            _buffer->unPin();
        }
    }

    /**
     * Set or reset the unpinner pointer.
     * @param buf the chunk pointer; can be NULL
     */
    void set(Chunk* buf)
    {
        _buffer = buf;
    }

    Chunk* get()
    {
        return _buffer;
    }
};

} // namespace
#endif /* UN_PINNER_H_ */
