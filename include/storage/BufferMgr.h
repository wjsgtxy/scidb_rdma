/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file BufferMgr.h
 *
 * @author Steve Fridella, Donghui Zhang
 *
 * @brief Declaration of buffer management classes.
 */

#ifndef BUFFERMGR_H_
#define BUFFERMGR_H_

#include <util/Singleton.h>
#include <util/DataStore.h>
#include <util/Arena.h>
#include <util/PointerRange.h>
#include <util/Mutex.h>
#include <util/Condition.h>
#include <util/compression/CompressorType.h>
#include <util/ThreadPool.h>
#include <util/JobQueue.h>
#include <util/Job.h>
#include <util/InjectedError.h>
#include <queue>

namespace scidb {

/**
 * BufferMgr manages cached data.
 */
class BufferMgr
    : public Singleton<BufferMgr>
    , public InjectedErrorListener
{
    typedef size_t BlockPos;

public:
    /**
     * Key that uniquely identifies a buffer.
     * Consists of data store key, offset in ds, size
     * and allocated size
     */
    class BufferKey
    {
    public:
        DataStore::DataStoreKey getDsk() const ;
        off_t getOffset() const ;
        size_t getSize() const ;
        size_t getAllocSize() const ;

        size_t getCompressedSize() const { return _compressedSize; }
        void setCompressedSize(size_t sz) { _compressedSize = sz; }

        /* NOTE:  the size/allocated size don't really determine
           the uniqueness of a buffer.  After all, you can't have
           two different buffers at the same offset in the same
           data store!  Nevertheless, we need them, because the
           data store does not track that information for us, and
           we need easy access to it when we want to
           read (size) or delete (allocated size) a buffer.
         */
        bool operator<(const BufferKey& other) const;
        bool operator==(const BufferKey& other) const;
        BufferKey();
        BufferKey(DataStore::DataStoreKey& dsk, off_t off, size_t sz, size_t alloc);

    private:
        DataStore::DataStoreKey _dsk;
        off_t _off;
        size_t _size;
        size_t _compressedSize;
        size_t _allocSize;
    };

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_CANT_ALLOCATE_MEMORY);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(MemArrayThresholdException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_MEM_ARRAY_THRESHOLD_EXCEEDED);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(BufferMgrSlotsExceededException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_BUFFERMGR_SLOTS_EXCEEDED);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(ChunkSizeLimitException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_CHUNK_SIZE_LIMIT_EXCEEDED);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(ReadOnlyBufferException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_READ_ONLY_BUFFER);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(RetryPinException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_PIN_RETRIES_EXCEEDED);

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(CompressionFailureException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_CANT_COMPRESS_CHUNK);
    /**
     *  Handle which is used to manipulate raw memory buffers.
     *  It functions as a weak pointer.  If the buffer is pinned,
     *  getRaw*Data() can be used. After calling unpinBuffer(),
     *  all existing raw pointers are invalidated and the caller
     *  can only access the buffer after subsequent calls to pinBuffer()
     *  and getRaw*Data() succeed.
     *
     *  Default constructor returns a "NULL" handle.  "NULL" handles
     *  have no associated data.  pin/unpin are no-ops for "NULL"
     *  handles and getRaw*Data() returns NULL pointer ranges.
     */
    class BufferHandle
    {
    public:
        // static constexpr uint64_t INVALID_SLOT = std::numeric_limits<BufferMgr::BlockPos>::max();
        // a Debug build with g++ (GCC) 4.9.2 20150212 (Red Hat 4.9.2-6)
        // produces "undefined reference to `scidb::BufferMgr::BufferHandle::INVALID_SLOT'"
        // the following is a workaround
        enum dummy { INVALID_SLOT = std::numeric_limits<BufferMgr::BlockPos>::max() };
        static constexpr uint64_t NULL_GENCOUNT = 0;

        friend class BufferMgr;

        /**
         * Return the pointer to the raw data.
         * @pre Buffer MUST be pinned.
         * @post Buffer is valid ONLY UNTIL unpinning.
         * @throws ReadOnlyBufferException if buffer is not dirty.
         */
        PointerRange<char> getRawData();

        /**
         * Return the pointer to the const raw data.
         * @pre Buffer MUST be pinned.
         * @post Buffer is valid ONLY UNTIL unpinning.
         */
        PointerRange<const char> getRawConstData();

        /**
         * Pin buffer again.
         * First pin occured on allocation
         */
        void pinBuffer();

        /**
         * Unpin the buffer.
         * Invalidates any existing raw pointer to the buffer.  Future
         * access to data is ONLY possible after subsequent "pinBuffer()"
         * and "getRaw*Data()" calls.
         */
        void unpinBuffer();

        /**
         * Discard the buffer.
         * Erases the buffer from memory and disk. If the buffer is null then simply return.
         * @pre buffer must be unpinned.
         */
        void discardBuffer();

        /**
         * Is live buffer attached to handle?
         */
        bool isNull() const;

        /**
         * Return the size of the buffer
         */
        size_t size() const ;

        /**
         * Return the physical offset of the buffer on disk
         */
        off_t offset() const ;

        /**
         * Return the allocated size of the buffer on disk
         */
        size_t allocSize() const ;

        void setBufferKey(const BufferKey& k);

        /**
         * Return the pointer to the buffer used to store temporary
         * compressed data.
         * @pre Buffer (the source raw data buffer) MUST be pinned.
         * @post Resulting compressed buffer is valid only until the source raw data
         *       buffer is unpinned.
         */
        PointerRange<char> getCompressionBuffer();

        size_t compressedSize() const ;
        void setCompressedSize(size_t sz);

        CompressorType getCompressorType() const;
        void setCompressorType(CompressorType cType);

        void resetHandle();

        /**
         * Sets the slot and gencount to their respective NULL values as
         * the slot and gencount are irrelevant for the handle-at-rest on disk
         * or when its corresponding DiskIndexValue is cleaned-up.
         */
        void resetSlotGenCount();

        /**
         *  Default constructor, creates a NULL handle
         */
        BufferHandle();

        /**
         * Accessors
         * @{
         */
        uint64_t getSlot() const ;
        uint64_t getGenCount() const ;
        BufferKey const& getKey() const ;
        /** @} */

    private:
        /**
         * Constructor --- private, only called by BufferMgr
         */
        BufferHandle(BufferKey& k, uint64_t slot, uint64_t gencount, CompressorType cType);

        /* Note:  NULL handle is implemented by setting _slot and _gencount
           to 0 and key to default constructed value.
         */

        /*
         * TODO SDB-5864 if _slot and _gencount are made mutable, then
         * Entry::EntryAsSlice may take its entry argument as a const ref as
         * that modification's done just as that instance of the handle is
         * about to expire.
         */
        // clang-format off
        CompressorType _compressorType; // The compressor type actually used for the buffer
        uint64_t       _slot;           // identifies the buffer slot
        uint64_t       _gencount;       // verifies that the buffer has not been swapped
        BufferKey      _key;            // allows buffer to be retrieved after swapout
        // clang-format on
    };

    /**
     * Priority values for dirty buffers.  "Immediate" means the buffer will be
     *  written eagerly as soon as possible after it is unpinned. "Defferred"
     *  means that the buffer will be written only if memory pressure requires
     *  that cache space must be freed.
     */
    enum WritePriority { Immediate, Deferred };

    /**
     * Return a buffer handle for a newly allocated buffer.
     * @param dsk data store key for buffer
     * @param sz size of buffer to allocate
     * @param wp desired write priority
     * @returns buffer handle, buffer is returned ''dirty'' and ''pinned''.
     * @throws MemArrayThresholdExceeded if insufficient space for the chunk in the cache
     * @throws BufferMgrSlotsExceeded if all chunks in the cache are pinned
     */
    BufferHandle allocateBuffer(DataStore::DataStoreKey& dsk,
                                size_t sz,
                                BufferMgr::WritePriority wp,
                                CompressorType compressionType);

    /**
     * Flush dirty buffers for a data store
     * @param dsk data store key for the ds to flush
     * @post all buffers for data store are no longer ''pinned''
     *       or ''dirty''
     * @note: this differs from normal cache terminology where
     *        flush means to drop references.  This is really
     *        undirtyAllBuffers().  If renamed, we can then
     *        rename clearCache() to flushCache().
     */
    void flushAllBuffers(DataStore::DataStoreKey& dsk);

    /**
     * Remove a buffer from the cache (and free it on disk)
     * @param bk buffer key for target buffer
     * @pre buffer must be unpinned.
     * @post buffer is no longer in cache and marked as free in data-store
     *       (not yet stable on disk, flushMetaData must still be called)
     */
    void discardBuffer(BufferKey& bk);

    /**
     * Discard all buffers for a datastore, and remove the ds from disk
     * @param dsk data store to discard and remove
     * @post all buffers belonging to indicated datastore are discarded,
     *       data store is removed from disk
     */
    void discardAllBuffers(DataStore::DataStoreKey& dsk);

    /**
     * Flush free-list updates for data store
     * @param dsk data store key for data store to flush
     * @post all metadata for indicated data store is stable on disk
     */
    void flushMetadata(DataStore::DataStoreKey& dsk);

    /**
     * Return the buffer manager arena
     */
    arena::ArenaPtr getArena();

    /**
     * Constructor
     */
    BufferMgr();

    /**
     * empty the _blockList (vs flushAllBuffers which only makes them not dirty)
     */
    void clearCache();

    /**
     * Return the cache statistics for the BufferMgr atomically.
     */
    struct BufferStats
    {
        uint64_t cached {0};
        uint64_t dirty {0};
        uint64_t pinned {0};
        uint64_t pending {0};
        uint64_t reserveReq {0};
        uint64_t usedLimit {0};
    };

    void getByteCounts(BufferStats& bufStats);
private:
    static const BlockPos MAX_BLOCK_POS;
    static const int MAX_PIN_RETRIES;

    /* In-mem control structure for each block
     */
    struct BlockHeader
    {
        /**
         * @brief Reset the state of this instance
         *
         * @param dataStoreKey The corresponding DataStoreKey instance
         * @param blockBasePtr The memory region associated with this block
         * @param blockSize The length of the region pointed-at by blockBasePtr
         * @param compressedBlockSize The length of the region associated with the
         * compressedBlockBase, if available
         * @param blockCompressorType The compression method for the memory
         * region associated with compressedBlockBase
         */
        void reset(const DataStore::DataStoreKey& dataStoreKey,
                   char* blockBasePtr,
                   size_t blockSize,
                   size_t compressedBlockSize,
                   CompressorType blockCompressorType);

        std::string toString() const;

        /* Priority used to determine when the block should be swapped out.
           Higher number makes slot "stickier".  Lower number swapped out sooner.
           zero indicates a free slot. NOTE: this field is current only used
           to indicate a free slot.
         */
        uint64_t priority {0};

        /* Is a write scheduled for this block? */
        bool pending {false};

        /* If dirty, what is the write priority? */
        WritePriority wp {Deferred};

        /* Number of callers that have references to buffers in the block */
        uint32_t pinCount {0};

        /* Unique identifier for back-end store */
        DataStore::DataStoreKey dsk;

        /* Offset in data store (if block is persisted) */
        off_t offset {0};

        /* Size of block in memory */
        size_t size {0};

        /* Compressed Size of buffer on disk */
        size_t compressedSize {0};

        /* The compressor to use when compressing data to and decompressing data from
         * disk */
        CompressorType compressorType {CompressorType::UNKNOWN};

        /* Allocated size on disk */
        size_t allocSize {0};

        /* Generation count for slot */
        uint64_t genCount {BufferHandle::NULL_GENCOUNT};

        /* Pointer to memory region for block */
        char* blockBase {nullptr};

        /* Pointer to memory region for block of copmressed data
         * Note: lifetime of this buffer is limited to when (de)compression will occur. */
        char* compressedBlockBase {nullptr};

        // setNextXXX

        /// @param p new next slot number
        void setNextFree(BlockPos p) { _next = p; }

        /// @param p new next slot number
        void setNextClean(BlockPos p) { _next = p; }

        /// @param p new next slot number
        void setNextDirty(BlockPos p) { _next = p; }

        // setPrevXXX
        // note: no setPrevFree(), use resetPrevFree() instead

        /// @param p new prev slot number
        void setPrevClean(BlockPos p) { _prevDirtyOrClean = p; }

        /// @param p new prev slot number
        void setPrevDirty(BlockPos p) { _prevDirtyOrClean = p; }

        /// Instead of setPrevFree, since the free list
        /// does not use back links, we have resetPrevFree().
        /// Resets unused prev slot number to the initial value
        /// for clarity.
        void resetPrevFree()          { _prevDirtyOrClean = MAX_BLOCK_POS ; }

        // getNextXXX

        /// @return next slot in list
        BlockPos getNextFree()  const { return _next; }

       /// @return next slot in list
        BlockPos getNextClean() const { return _next; }

        /// @return next slot in list
        BlockPos getNextDirty() const { return _next; }

        // setPrevXXX
        // note there is no no getPrevFree

        /// @return next slot in list
        BlockPos getPrevClean() const { return _prevDirtyOrClean; }

        /// @return next slot in list
        BlockPos getPrevDirty() const { return _prevDirtyOrClean; }

        // accessors for _dirty

        /// @return whether dirty
        bool isDirty() const { return _dirty; }

        /// @return whether clean
        bool isClean() const { return !_dirty; }

        /// mark the buffer as dirty
        void markDirty() { _dirty=true; }

        /// mark the buffer as clean
        void markClean() { _dirty=false; }

    private:
        // next link, only on one list at a time
        BlockPos _next {MAX_BLOCK_POS};

        // prev link, only on one list at a time
        BlockPos _prevDirtyOrClean {MAX_BLOCK_POS};

        /* Is the block writable? */
        bool _dirty {false};
        // TODO: change to tristate dirty/clean/free ? (simplify some allocation logic)
    };

    // TODO: rename the following, vectors are not lists
    typedef std::vector<BlockHeader> BlockHeaderList;
    typedef std::vector<BlockPos> BlockPosList;
    typedef std::map<BufferKey, BlockPos> DSBufferMap;

    /* Static query reference used as a placeholder for jobs
     */
    static std::shared_ptr<Query> const& _bufferQuery;

    /* Class to handle a single background I/O operation, either a read or
     * a write.
     */
    class IoJob : public Job
    {
    public:
        enum IoType { Write,  WriteQueueBatchPoint};

        /* Constructor */
        IoJob(IoType reqType, BlockPos target, BufferMgr& mgr);

        /* Perform the requested I/O */
        void run();

    private:
        // clang-format off
        IoType     _reqType;     // what type of request?
        BlockPos   _targetSlot;  // slot for request
        BufferMgr& _buffMgr;     // owner
        // clang-format on
    };

    /* Internal parts of pinBuffer.  _blockMutex must be locked.
     */
    BufferHandle _pinBufferLocked(BufferKey& bk, CompressorType cType);
    BufferHandle _pinBufferLockedWithRetry(BufferKey& bk, CompressorType cType);

    /// @throws MemArrayThresholdExceeded if insufficient space for the chunk in the cache
    /// @throws BufferMgrSlotsExceeded if all chunks in the cache are pinned
    void _reserveSpace(size_t sz);

    // common expressions factored from reserveSpace() to ease modification
    // pre: blockMutex held for all
    bool   _slotNeeded() const;
    size_t _unCachedBytes() const;
    size_t _extraUnCachedNeeded(size_t sz) const;
    size_t _excessPending() const;
    size_t _excessPendingSlots() const;
    size_t _extraPendingNeeded(size_t sz) const;
    bool   _extraPendingSlotNeeded() const;


    void _writeSlotUnlocked(BlockPos targetSlot);

    void _writeQueueBatchPoint();

    bool _destageCache(size_t sz);

    bool _ejectCache(size_t sz);

    void _claimSpaceUsed(size_t sz);

    void _releaseSpaceUsed(size_t sz);

    void _claimDirtyBytes(size_t sz);

    void _releaseDirtyBytes(size_t sz);

    void _claimPinnedBytes(size_t sz);

    void _releasePinnedBytes(size_t sz);

    void _claimPendingBytes(size_t sz);

    void _releasePendingBytes(size_t sz);

    void _claimReserveReqBytes(size_t sz);

    void _releaseReserveReqBytes(size_t sz);

    // wait for both size bytes and slot to be satisfied
    void _waitOnReserveQueue(size_t size, bool slotNeeded);

    void _signalReserveWaiters(size_t sz);

    //
    // slot variations of some of the above byte-oriented methods
    //

    /// claims a single pending slot
    void _claimPendingSlot();

    /// releases a single pending slot
    void _releasePendingSlot();

    /// claims a single reserve-requested slot
    void _claimReserveReqSlot();

    /// releases a single reserve-requested slot
    void _releaseReserveReqSlot();

    BlockPos _allocateBlockHeader(const DataStore::DataStoreKey& dsk,
                                  size_t size,
                                  size_t compressedSize,
                                  CompressorType compressorType);

    void _releaseBlockHeader(BlockPos slot);

    BlockPos _allocateFreeSlot();

    // low-level add/removes (not moves between)
    void _addToDirtyList(BlockPos slot);
    void _removeFromDirtyList(BlockPos slot);

    void _addToCleanList(BlockPos slot);
    void _removeFromCleanList(BlockPos slot);

    // higher-level dequeue/requeue
    void _updateDirtyList(BlockPos slot);
    void _updateCleanList(BlockPos slot);

    // higher-level move from one list to another
    void _moveDirtyToCleanList(BlockPos slot);
    void _moveDirtyToFreeList(BlockPos slot);
    void _moveCleanToFreeList(BlockPos slot);
    void _moveCleanToDirtyList(BlockPos slot);
    //   _moveFreeToCleanList() -- see _allocateBlockHeader

    bool _removeBufferFromCache(DSBufferMap::iterator dbit);

    void _freeSlot(const BlockPos& slot);

    /* BufferHandle method implementations
     */
    PointerRange<char> _getRawData(BufferHandle& bh);
    PointerRange<const char> _getRawConstData(BufferHandle& bh);

    void _pinBuffer(BufferHandle& bh);
    void _unpinBuffer(BufferHandle& bh);
    void _discardBuffer(BufferHandle& bh);

    /* Lock which protects block list during allocations and frees
     */
    Mutex _blockMutex;

    /* Latches which protect state of individual block headers
     */
    std::vector<Mutex> _slotMutexes;

    /* Block data structures
     */
    // clang-format off
    BlockHeaderList    _blockList;    // blocks in slot number ordere
    BlockPos           _freeHead;     // free slots list (singly linked on _next)

                                      // clean slots (doubly linked on _next, _prevDirtyOrClean)
    BlockPos           _lruClean;     // head of clean lru (cold) list
    BlockPos           _mruClean;     // tail of clean lru (hot) list

                                      // dirty slots (doubly linked on _next, _prevDirtyOrClean)
    BlockPos           _lruDirty;     // head of dirty lru (cold) list
    BlockPos           _mruDirty;     // tail of dirty lru (hot) list

    DSBufferMap        _dsBufferMap;  // buffers indexed by datastore
    BlockPosList       _writeList;    // list of blocks to be written
    arena::ArenaPtr    _arena;        // manages memory used by storage

    /* Cache space tracking
     */
    uint64_t     _bytesCached;     // current size of cache
    uint64_t     _dirtyBytes;      // amount of cache that is dirty
    uint64_t     _pinnedBytes;     // amount of cache that is pinned
    uint64_t     _pendingBytes;    // number of bytes scheduled for write
    uint64_t     _reserveReqBytes; // number of bytes in reserve queue
    uint64_t     _pendingSlots;    // number of slot scheduled for write
    uint64_t     _reserveReqSlots; // number of slots in reserve queue
    const uint64_t  _usedMemLimit;     // max allowable cached bytes
    const uint64_t  _chunkSizeLimit;   // max chunk size

    struct SpaceRequest {
        size_t size;            // amount of space still needed
        bool   slot;            // whether a  slot is still needed
        std::atomic<bool> done; // guard against spurious wakeups
        Condition ready;        // signaled when request is fulfilled

        SpaceRequest(size_t sizeIn, bool slotIn) : size(sizeIn), slot(slotIn), done(false) { }
    };
    typedef std::queue<std::shared_ptr<SpaceRequest> > ReservationQueue;

    ReservationQueue   _reserveQueue;     // queue of waiting space reqs
    Condition          _pendingWriteCond; // signal threads waiting for individual writes
    Condition          _pendingWriteBatchCond; // signal threads waiting for a point in the queue
                                               // to be reached by the writers

    /* Thread pool and job queue for asychronous I/O
     */
    std::shared_ptr<ThreadPool> _asyncIoThreadPool;
    std::shared_ptr<JobQueue>   _jobQueue;
    // clang-format on

    PointerRange<char> _getCompressionBuffer(BufferHandle& bh);
    void _setCompressedSize(BufferHandle& bh, size_t sz);
};

std::ostream& operator<<(std::ostream& os, BufferMgr::BufferKey const& bk);
std::ostream& operator<<(std::ostream& os, BufferMgr::BufferHandle const& bh);


//
// These Accessors which are used by an external utility (indexmapper)
// must remain as inlines until linkage issues are resolved ...
//
inline size_t BufferMgr::BufferHandle::size() const { return isNull() ? 0 : _key.getSize(); }
inline size_t BufferMgr::BufferHandle::allocSize() const { return isNull() ? 0 : _key.getAllocSize(); }
inline size_t BufferMgr::BufferHandle::compressedSize() const { return isNull() ? 0 : _key.getCompressedSize(); }
inline off_t BufferMgr::BufferHandle::offset() const { return isNull() ? 0 : _key.getOffset(); }
inline uint64_t BufferMgr::BufferHandle::getSlot() const { return _slot; }
inline uint64_t BufferMgr::BufferHandle::getGenCount() const { return _gencount; }
inline BufferMgr::BufferKey const& BufferMgr::BufferHandle::getKey() const { return _key; }
inline CompressorType BufferMgr::BufferHandle::getCompressorType() const { return _compressorType; }

// and one used by the ones above
inline bool BufferMgr::BufferHandle::isNull() const {
    BufferKey nullKey;
    return _slot == INVALID_SLOT && _gencount == NULL_GENCOUNT && _key == nullKey;
}

// and two used by the one above
inline BufferMgr::BufferKey::BufferKey()
: _dsk()
, _off(0)
, _size(0)
, _compressedSize(0)
, _allocSize(0)
{}

inline bool BufferMgr::BufferKey::operator==(const BufferMgr::BufferKey& other) const
{
    return (getDsk() == other.getDsk() &&
            getOffset() == other.getOffset() &&
            getSize() == other.getSize() &&
            getAllocSize() == other.getAllocSize());
}

// and for used by the one above
inline DataStore::DataStoreKey BufferMgr::BufferKey::getDsk() const { return _dsk; }
inline off_t BufferMgr::BufferKey::getOffset() const { return _off; }
inline size_t BufferMgr::BufferKey::getSize() const { return _size; }
inline size_t BufferMgr::BufferKey::getAllocSize() const { return _allocSize; }

}  // namespace scidb

#endif /* BUFFERMGR_H_ */
