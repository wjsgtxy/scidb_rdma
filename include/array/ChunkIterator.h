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
 * @file ChunkIterator.h
 *
 * @brief class ChunkIterator
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

#ifndef CHUNK_ITERATOR_H_
#define CHUNK_ITERATOR_H_

#include <array/ConstChunkIterator.h>   // base

namespace scidb
{
class Query;
class Value;


/**
 * The volatile iterator can also write items to the array.
 */
class ChunkIterator : public ConstChunkIterator
{
public:
    /**
     * Update the current element value
     */
     virtual void writeItem(const  Value& item) = 0;

    /**
     * Save all changes done in the chunk
     */
    virtual void flush() = 0;

    /**
     * Unpin the chunk referenced by this iterator
     */
    virtual void unpin() {}

    /// Query context for this iterator
    virtual std::shared_ptr<Query> getQuery() = 0;
};

} // namespace
#endif /* CHUNK_ITERATOR_H_ */
