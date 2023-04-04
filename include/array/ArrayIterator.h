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
 * @file ArrayIterator.h
 *
 * @brief class ArrayIterator
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

#ifndef ARRAY_ITERATOR_H_
#define ARRAY_ITERATOR_H_

#include <array/ConstArrayIterator.h> // base
#include <util/compression/CompressorType.h>

namespace scidb
{
class Chunk;
class ConstRLEEmptyBitmap;
class Query;

/**
 * The volatile iterator can also write chunks to the array
 */
class ArrayIterator : public ConstArrayIterator
{
public:
    ArrayIterator() = delete;

    ArrayIterator(Array const& a)
        : ConstArrayIterator(a)
    { }

    virtual Chunk& updateChunk();

    /**
     * Create new chunk at the local instance using default compression method for this attribute.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos) = 0;

    /**
     * Create new chunk at the local instance.
     * Only one chunk can be created and filled by iterator at each moment of time.
     * @param position of the first element in the created chunk (not including overlaps)
     */
    virtual Chunk& newChunk(Coordinates const& pos, CompressorType compressionMethod) = 0;

    /**
     * Copy chunk
     * @param srcChunk source chunk
     * @brief If you give the function a pair of coordinate pointers, the copy
     *        function will populate those coordinates with the 'start' and the
     *        'end' (first in chunk order, and last in chunk order). The idea is
     *        to be able to track meta-data about chunks without being obliged
     *        to iterate through the chunk's contents more than once.
     */
    virtual Chunk& copyChunk( ConstChunk const& srcChunk,
                              std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                              Coordinates * chunkStart = NULL,
                              Coordinates * chunkEnd   = NULL );

    virtual Chunk& copyChunk( ConstChunk const& srcChunk,
                              Coordinates * chunkStart = NULL,
                              Coordinates * chunkEnd   = NULL )
    {
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        return copyChunk(srcChunk, emptyBitmap, chunkStart, chunkEnd);
    }

    virtual void deleteChunk(Chunk& chunk);

    /// Query context for this iterator
    virtual std::shared_ptr<Query> getQuery() = 0;
};

} // namespace
#endif /* ARRAY_ITERATOR_H_ */
