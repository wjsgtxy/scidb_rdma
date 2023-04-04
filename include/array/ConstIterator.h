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
 * @file Array.h
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
 *
 */

#ifndef CONST_ITERATOR_H_
#define CONST_ITERATOR_H_

#include <array/Coordinate.h>       // grr unfactored typedefs

namespace scidb
{

/**
 * Common const iterator interface
 */
class ConstIterator
{
public:
    /**
     * Check if end of chunk is reached
     * @return true if iterator reaches the end of the chunk
     */
    virtual bool end() = 0;

    /**
     * Position cursor to the next element (order of traversal depends on used iteration mode)
     */
    virtual void operator ++() = 0;

    /**
     * Get coordinates of the current element in the chunk
     */
    virtual Coordinates const& getPosition() = 0;

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (belongs to the chunk and match current iteratation mode),
     * false otherwise
     */
    virtual bool setPosition(Coordinates const& pos) = 0;

    /**
     * Move iterator to the first element
     */
    virtual void restart() = 0;

    virtual ~ConstIterator();
};


} // namespace
#endif /* CONST_ITERATOR_H_ */
