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
 * @file ConstArrayIterator.h
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

#ifndef CONST_ARRAY_ITERATOR_H_
#define CONST_ARRAY_ITERATOR_H_

#include <array/ConstIterator.h>    // base

namespace scidb
{
class Array;
class ConstChunk;

/**
 * An array const iterator iterates over the chunks of the array available at the local instance.
 * Order of iteration is not specified.
 */
class ConstArrayIterator : public ConstIterator
{
public:
    ConstArrayIterator() = delete;
    ConstArrayIterator(Array const& a) : _array(a) {}

    /**
     * Select chunk which contains element with specified position in main (not overlapped) area
     * @param pos element position
     * @return true if chunk with containing specified position is present at the local instance, false otherwise
     */
    bool setPosition(Coordinates const& pos) override;

    /**
     * Restart iterations from the beginning
     */
    void restart() override;

    /**
     * Get current chunk
     */
    virtual ConstChunk const& getChunk() = 0;

    /**
     * Return associated array.
     */
    Array const& getArray() const;

private:
    Array const& _array;
};

} // namespace

#endif /* CONST_ARRAY_ITERATOR_H_ */
