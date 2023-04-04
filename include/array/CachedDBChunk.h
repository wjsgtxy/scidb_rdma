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
 * @file CachedDBChunk.h
 *
 * @brief Chunk implementation for persistent arrays
 */

#ifndef CACHED_DB_CHUNK_H_
#define CACHED_DB_CHUNK_H_

#include <vector>
#include <map>
#include <assert.h>
#include <memory>
#include <array/ChunkAuxMeta.h>
#include <array/MemChunk.h>
#include <array/AddressMeta.h>
#include <storage/IndexMgr.h>
#include <storage/StorageMgr.h>
#include <util/compression/CompressorType.h>

namespace scidb {
class ChunkDescriptor;
class DBArray;
class DBArrayIterator;
class StorageAddress;
class Query;

/**
 * Chunk of DBArray managed by buffer cache
 */
class CachedDBChunk : public MemChunk
{
    friend class DBArrayIterator;
    friend class DBArray;

    typedef DiskIndex<DbAddressMeta> DbDiskIndex;

public:
    static void createChunk(ArrayDesc const& desc,
                            std::shared_ptr<DbDiskIndex> diskIndexPtr,
                            PersistentAddress const& addr,
                            CachedDBChunk*& chunk,
                            CachedDBChunk*& bitmapChunk,
                            bool newChunk,
                            DBArray* arrayPtr);

    /**
     * Update the chunk size and offset for a newly created chunk descriptor
     * that was ported from the legacy storage.header to rocksdb.
     */
    static void upgradeDescriptor(const CachedDBChunk& chunk,
                                  std::shared_ptr<DbDiskIndex> diskIndexPtr,
                                  const ChunkDescriptor& desc);

private:
    /* Note: CachedDBChunk objects are created by DBArrayIterators,
       but are deleted when the last chunk iterator that uses them
       unregisters. This allows the DBArrayIterator to "forget" about
       chunks that it creates when it is asked to advance.  Otherwise,
       it would need to delete the current chunk object during the
       advance operation, even while the current chunk object was
       being accessed by an iterator.
     */

    // clang-format off
    PersistentAddress                    _storageAddr;
    mutable size_t                       _accessCount;
    mutable size_t                       _regCount;
    bool                                 _deleteOnLastUnregister;
    mutable DbDiskIndex::DiskIndexValue  _indexValue;
    DbAddressMeta::KeyWithSpace          _key;
    mutable ChunkAuxMeta                 _chunkAuxMeta;
    // clang-format on

public:
    CachedDBChunk();
    ~CachedDBChunk();

    /**
     * Initialize a CachedDBChunk with from the given disk index
     */
    void initialize(std::shared_ptr<DbDiskIndex> diskIndexPtr,
                    ArrayDesc const* desc,
                    const PersistentAddress& firstElem,
                    CompressorType compressionMethod);

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
     * Return (const) reference to chunk auxiliary metadata
     */
    ChunkAuxMeta const& getChunkAuxMeta() const { return _chunkAuxMeta; }

    /**
     * Set chunk to be deleted when the last chunk iterator
     * unregisters.
     */
    void deleteOnLastUnregister();

    /**
     * Dump some debug info about the chunk (at trace level)
     */
    void trace() const;

    /**
     * Increase the count of iterators that are using this chunk
     */
    bool registerIterator(ConstChunkIterator& ci) override;

    /**
     * Decrease the count of iterators that are using this chunk
     */
    void unregisterIterator(ConstChunkIterator& ci) override;

    /**
     * Get read/write pointer to chunk's compressed data
     */
    void* getCompressionBuffer() const;

    /**
     * Set the compressed size and the compressor type of the chunk in the chunk index
     * which contains the metadata that will be persisted about the chunk.
     */
    void setCompressedSizeAndType(size_t cSize, CompressorType cType);

    /**
     * Count number of present (non-empty) elements in the chunk.
     * @return the number of non-empty elements in the chunk.
     */
    size_t count() const override;

    /**
     * Check if count of non-empty elements in the chunk is known.
     * @return true if count() will run in constant time; false otherwise.
     */
    bool isCountKnown() const override;

    /**
     * Set the count of the number of elements.
     */
    void setCount(size_t count) override;

    /**
     * @return the rocksdb key corresponding to this chunk.
     * NOTE:  This is deprecated.
     */
    const DbAddressMeta::Key* getKey() const;
};

}  // namespace scidb
#endif  // CACHED_DB_CHUNK_H_
