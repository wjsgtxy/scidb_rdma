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
 * @file CachedTmpChunk.h
 *
 * @brief Non-persistent chunk managed by buffer cache
 */

#ifndef CACHED_MEM_CHUNK_H_
#define CACHED_MEM_CHUNK_H_

#include <storage/IndexMgr.h>
#include <array/MemChunk.h>
#include <array/AddressMeta.h>

namespace scidb {
class MemArray;
class MemArrayIterator;

/**
 * Chunk of MemArray managed by buffer cache
 */
class CachedTmpChunk : public MemChunk
{
    friend class MemArrayIterator;
    friend class MemArray;

    typedef DiskIndex<MemAddressMeta> MemDiskIndex;

protected:
    /* Note: CachedTmpChunk objects are created by MemArrayIterators,
       but are deleted when the last chunk iterator that uses them
       unregisters. This allows the MemArrayIterator to "forget" about
       chunks that it creates when it is asked to advance.  Otherwise,
       it would need to delete the current chunk object during the
       advance operation, even while the current chunk object was
       being accessed by an iterator.
     */
    // clang-format off
    mutable size_t                       _accessCount;
    mutable size_t                       _regCount;
    bool                                 _deleteOnLastUnregister;
    mutable MemDiskIndex::DiskIndexValue _indexValue;
    MemAddressMeta::KeyWithSpace         _key;
    // clang-format on

public:
    CachedTmpChunk();
    ~CachedTmpChunk();

    /**
     * Initialize a CachedTmpChunk with MemArray
     * Note: CachedTmpChunk can never be initialized with an array
     * that is not a MemArray.  To ensure this, the other flavors of
     * initialize in the subclass will assert.
     */
    void initialize(MemArray const* array,
                    ArrayDesc const* dess,
                    const Address& firstElem,
                    CompressorType compressionMethod);

    /**
     * Initialize a CachedTmpChunk with a generic Array
     * Note: CachedTmpChunk must be initialized with a MemArray.
     * This version of initialize will assert.
     */
    void initialize(Array const* array,
                    ArrayDesc const* desc,
                    const Address& firstElem,
                    CompressorType compressionMethod) override;

    /**
     * Initialize a CachedTmpChunk with a source chunk
     * Note: CachedTmpChunk must be initialized with a MemArray.
     * This version of initialize will assert.
     */
    void initialize(ConstChunk const& srcChunk) override;

    /**
     * Commit the chunk's data to the cache
     */
    void write(const std::shared_ptr<Query>& query) override;

    /**
     * Get read/write pointer to chunk's data
     */
    void* getWriteDataImpl() override;

    /**
     * Get read-only pointer to chunk's data
     */
    void const* getConstDataImpl() const override;

    /**
     * Allocate a new buffer to hold chunk data
     * Frees old data (if there was any)
     */
    void reallocate(size_t size) override;

    /**
     * Get a read/write chunk iterator
     */
    std::shared_ptr<ChunkIterator> getIterator(std::shared_ptr<Query> const& query,
                                               int iterationMode) override;

    /**
     * Get a read-only chunk iterator
     */
    std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const override;

    /**
     * Ensure that chunk cannot be swapped out of cache
     */
    bool pin() const override;

    /**
     * Signal that chunk may be swapped out of cache
     */
    void unPin() const override;

    /**
     * Set chunk to be deleted when the last chunk iterator
     * unregisters.
     */
    virtual void deleteOnLastUnregister();

    /**
     * Dump some debug info about the chunk (at trace level)
     */
    virtual void trace() const;

    /**
     * Increase the count of iterators that are using this chunk
     */
    bool registerIterator(ConstChunkIterator& ci) override;

    /**
     * Decrease the count of iterators that are using this chunk
     */
    void unregisterIterator(ConstChunkIterator& ci) override;
};

}  // namespace scidb
#endif  // CACHED_MEM_CHUNK_H_
