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
 * @file PinBuffer.h
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

#ifndef PIN_BUFFER_H_
#define PIN_BUFFER_H_

#include <array/SharedBuffer.h>        // grr. only because of implementation-in-header
#include <cassert>

namespace scidb
{
class SharedBuffer;

/**
 * A class that auto-unpins a given SharedBuffer in its destructor.
 */
class PinBuffer
{
    SharedBuffer const& _buffer;
    bool _pinned;

public:
    PinBuffer(SharedBuffer const& buf)
        : _buffer(buf)
    {
        _pinned = _buffer.pin();
    }

    bool isPinned() const
    {
        return _pinned;
    }

    void unPin()
    {
        assert(_pinned);
        _buffer.unPin();
        _pinned = false;
    }

    ~PinBuffer()
    {
        if (_pinned) {
            _buffer.unPin();
        }
    }
};

} // namespace

#endif /* ! PIN_BUFFER_H_ */
