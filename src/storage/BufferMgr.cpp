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
 * @file BufferMgr.cpp
 *
 * @author Steve Fridella, Donghui Zhang
 *
 * @brief Implementation of buffer manager classes.
 */

#include <storage/BufferMgr.h>

#include <query/FunctionLibrary.h>
#include <query/Query.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/Warnings.h>
#include <util/compression/Compressor.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>
#include <util/OnScopeExit.h>
#include <util/PerfTime.h>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.storage.bufmgr"));
log4cxx::LoggerPtr loggerByteCount(log4cxx::Logger::getLogger("scidb.storage.bufmgr.bytecount"));
log4cxx::LoggerPtr loggerWait(log4cxx::Logger::getLogger("scidb.storage.bufmgr.wait"));

log4cxx::LoggerPtr loggerEject(log4cxx::Logger::getLogger("scidb.storage.bufmgr.eject"));
log4cxx::LoggerPtr loggerEjectSkipped(log4cxx::Logger::getLogger("scidb.storage.bufmgr.eject.skipped"));
log4cxx::LoggerPtr loggerDestage(log4cxx::Logger::getLogger("scidb.storage.bufmgr.destage"));
log4cxx::LoggerPtr loggerReserve(log4cxx::Logger::getLogger("scidb.storage.bufmgr.reserve"));
log4cxx::LoggerPtr loggerClaimRelBytes(log4cxx::Logger::getLogger("scidb.storage.bufmgr.claimrelbytes"));
log4cxx::LoggerPtr loggerClaimRelSlots(log4cxx::Logger::getLogger("scidb.storage.bufmgr.claimrelslots"));
log4cxx::LoggerPtr loggerClearcache(log4cxx::Logger::getLogger("scidb.storage.bufmgr.clearcache"));
}  // namespace

namespace scidb {

// This macro logs message at TRACE level using the loggerByteCount which contains the
// "standard" message followed by a json object so that the logs can more easily be parsed
// by something like python to help ascertain if/where a claim/release "ledger balance" of
// a byte count counter is incorrect. This MACRO is separate, for the time being, from the
// actual _claim/_releaseXXX() class because the __funct__ operator which makes the change
// to the byte counter. As refactoring of the code continues, this may become less important.
//
// _claimrelease_ :  "claim" or "release".
// _countertype_ : Which BufferMgr counter (cached, pinned, dirty, pending, etc) is being
//                 incremented/decremented. This is a free form text, but is was
//                 originally intended to distinguish between bytes associated with the
//                 "blockbase" and the "compressedBlockBase"
// _size_ : The size to increment(claim)/decrement(release) of the counter.
// _bhead_: the BlockHeader associated with the claim/release
//
// SEE: LOGCLAIM/LOGRELEASE. Rather than repeat the same LOG4CXX_TRACE for each of those,
// This macro maintains the same spacing in the JSON object which helps with grep
// operations of the logs. There are 10s of THOUSANDS of such messages when running large
// tests where the cache is actually ejecting/destaging buffers from the cache.
//
// clang-format off
#define LOGCLAIMRELEASE(_claimrelease_, _countertype_, _size_, _bhead_)                \
    LOG4CXX_TRACE(loggerByteCount, _claimrelease_  << " "  << _countertype_ << " -- {" \
                                   << " \"action\": \"" << _claimrelease_ << "\""      \
                                   << " , \"counter\": \"" << _countertype_ << "\""    \
                                   << " , \"sz\": " << _size_                          \
                                   << " , \"function\": " << "\"" << __func__ << "\""  \
                                   << " , \"bhead\": " << _bhead_.toString()           \
                                   << " }")

#define LOGCLAIM(_countertype_, _size_, _bhead_)             \
    LOGCLAIMRELEASE("claim", _countertype_, _size_, _bhead_)

#define LOGRELEASE(_countertype_, _size_, _bhead_)             \
    LOGCLAIMRELEASE("release", _countertype_, _size_, _bhead_)
// clang-format on

using namespace std;

const BufferMgr::BlockPos BufferMgr::MAX_BLOCK_POS =
    std::numeric_limits<BufferMgr::BlockPos>::max();

const int BufferMgr::MAX_PIN_RETRIES = 3;

//
// BufferHandle implementation
//
PointerRange<char> BufferMgr::BufferHandle::getRawData()
{
    if (isNull()) {
        return PointerRange<char>();
    }
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getRawData(*this);
}

PointerRange<const char> BufferMgr::BufferHandle::getRawConstData()
{
    if (isNull()) {
        return PointerRange<char>();
    }
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getRawConstData(*this);
}

void BufferMgr::BufferHandle::pinBuffer()
{
    // no blocking here so waiting timing is at a lower level
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_pinBuffer(*this);
}

void BufferMgr::BufferHandle::unpinBuffer()
{
    // no blocking here so waiting timing is at a lower level
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_unpinBuffer(*this);
}

void BufferMgr::BufferHandle::discardBuffer()
{
    // no blocking here so waiting timing is at a lower level
    if (isNull()) {
        return;
    }
    BufferMgr* bm = BufferMgr::getInstance();
    bm->_discardBuffer(*this);
}

// TODO: no coverage, because lack of coverage of CachedDBChunk::upgradeDescriptor
//       which is the sole use, and is not covered for the 17.9 release
void BufferMgr::BufferHandle::setBufferKey(const BufferKey& k)
{
    _key = k;
}

PointerRange<char> BufferMgr::BufferHandle::getCompressionBuffer()
{
    SCIDB_ASSERT(!isNull());
    BufferMgr* bm = BufferMgr::getInstance();
    return bm->_getCompressionBuffer(*this);
}

void BufferMgr::BufferHandle::setCompressedSize(size_t sz)
{
    _key.setCompressedSize(sz);
    BufferMgr::getInstance()->_setCompressedSize(*this, sz);
}

void BufferMgr::BufferHandle::setCompressorType(CompressorType cType)
{
    _compressorType = cType;
}

void BufferMgr::BufferHandle::resetHandle()
{
    BufferKey bk;
    _key = bk;
    _compressorType = CompressorType::UNKNOWN;
    resetSlotGenCount();
}

void BufferMgr::BufferHandle::resetSlotGenCount()
{
    _slot = INVALID_SLOT;
    _gencount = NULL_GENCOUNT;
}

BufferMgr::BufferHandle::BufferHandle()
    : _compressorType(CompressorType::UNKNOWN)
    , _slot(INVALID_SLOT)
    , _gencount(NULL_GENCOUNT)
    , _key()
{}

BufferMgr::BufferHandle::BufferHandle(BufferKey& k, uint64_t slot, uint64_t gencount, CompressorType cType)
    : _compressorType(cType)
    , _slot(slot)
    , _gencount(gencount)
    , _key(k)
{
    // BufferMgr's slot refers to a valid buffer by now, so the
    // slot's (and this handle's) gencount must be non-zero.
    assert(gencount != NULL_GENCOUNT);
}

//
// BufferKey implementation
//

bool BufferMgr::BufferKey::operator<(const BufferMgr::BufferKey& other) const
{
    if (getDsk() != other.getDsk())
        return getDsk() < other.getDsk();
    else if (getOffset() != other.getOffset())
        return getOffset() < other.getOffset();
    else if (getSize() != other.getSize())
        return getSize() < other.getSize();
    else
        return getAllocSize() < other.getAllocSize();
}

BufferMgr::BufferKey::BufferKey(DataStore::DataStoreKey& dsk, off_t off, size_t sz, size_t alloc)
    : _dsk(dsk)
    , _off(off)
    , _size(sz)
    , _compressedSize(sz)
    , _allocSize(alloc)
{}

//
// BufferMgr implementation
//

//
// wrapper for newVector<char> that adds error injection capability
//
template<InjectErrCode INJECT_ERROR_CODE>
char* allocBlockBase(arena::Arena& a, arena::count_t c, int line, const char* file)
{
    if (injectedError<INJECT_ERROR_CODE>(line, file)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MEMORY_ALLOCATION_ERROR)
            << "simulated newVector failure";
    }
    return arena::newVector<char>(a, c);
}


void BufferMgr::clearCache()
{
    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache");
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AA);

    // Attempt to make as much room in the Cache as the cache size
    // we should be able to release everything that is not pinned,
    // as well as anything pinned but pending
    // all - (pinned - pending)
    // all - pinned + pending
    // unpinnedd + pending
    size_t targetBytes = _usedMemLimit - _pinnedBytes + _pendingBytes ;
    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache start clearing targetBytes " << targetBytes);
    try {
        _reserveSpace(targetBytes);
    } catch (std::exception& ex) { 
        LOG4CXX_ERROR(loggerClearcache, "BufferMgr::clearCache: _reserveSpace("<<targetBytes<<") threw");
        throw ;
    }
    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache (post) _bytesCached " << _bytesCached);
    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache (post) _pinnedBytes " << _pinnedBytes);
    ASSERT_EXCEPTION(_bytesCached == _pinnedBytes, "BufferMgr::_bytesCached !=  _pinnedBytes ");

    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache (post) _pendingBytes " << _pendingBytes);
    ASSERT_EXCEPTION(_pendingBytes == 0, "BufferMgr::_pendingBytes is non zero after clearing");

    LOG4CXX_TRACE(loggerClearcache, "BufferMgr::clearCache: _reserveReqBytes " << _reserveReqBytes << " should be 0");
    ASSERT_EXCEPTION(_reserveReqBytes == 0, "BufferMgr::reserveReqBytes not zero");

    // for use only by the test_clear_cache operator so _INFO logging permissible
    LOG4CXX_INFO(loggerClearcache, "BufferMgr::clearCache complete " << targetBytes);
}

BufferMgr::BufferHandle BufferMgr::allocateBuffer(DataStore::DataStoreKey& dsk,
                                                  size_t sz,
                                                  BufferMgr::WritePriority wp,
                                                  CompressorType compressorType)
{
    LOG4CXX_TRACE(logger, "BufferMgr::allocateBuffer for " << dsk.toString());
    LOG4CXX_TRACE(logger, "BufferMgr::allocateBuffer sz " << sz);

    SCIDB_ASSERT(sz);
    SCIDB_ASSERT(compressorType != CompressorType::UNKNOWN);
    ScopedMutexLock sm(_blockMutex, PTW_SML_BM_ALLOC);

    // Find a slot in the block list and allocate memory for the buffer.
    BlockPos slot = _allocateBlockHeader(dsk, sz, sz, compressorType);
    // Initialize the slot values, allocate the space in the datastore.
    BlockHeader& bhead = _blockList[slot];
    _moveCleanToDirtyList(slot);
    bhead.wp = wp;
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);
    bhead.offset = ds->allocateSpace(sz, bhead.allocSize);

    SCIDB_ASSERT(sz == bhead.size);
    LOGCLAIM("SpaceUsed", bhead.size, bhead);
    LOGCLAIM("Pinned", bhead.size, bhead);
    LOGCLAIM("Dirty", bhead.size, bhead);
    _claimSpaceUsed(bhead.size);
    _claimPinnedBytes(bhead.size);
    _claimDirtyBytes(bhead.size);

    if (compressorType != CompressorType::NONE) {
        // We need twice the space if doing compression. Once for the "raw buffer" and
        // once for the "compression buffer". The compression buffer will be no larger then
        // the "raw buffer" so use that size.
        LOGCLAIM("SpaceUsed_compressed", bhead.size, bhead);
        LOGCLAIM("Pinned_compressed", bhead.size, bhead);
        LOGCLAIM("Dirty_compressed", bhead.size, bhead);
        _claimSpaceUsed(bhead.size);
        _claimPinnedBytes(bhead.size);
        _claimDirtyBytes(bhead.size);
    }

    // Enter slot into the DS Block Map
    BufferKey bk(dsk, bhead.offset, bhead.size, bhead.allocSize);
    DSBufferMap::value_type mapentry(bk, slot);
    auto result = _dsBufferMap.insert(mapentry);
    SCIDB_ASSERT(result.second);

    // Return the handle
    BufferHandle bhandle(bk, slot, bhead.genCount, compressorType);
    return bhandle;
}

void BufferMgr::flushAllBuffers(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers (dsk: " << dsk.toString() << ")");

    /* Iterate the buffer list for this dsk.  Ensure that no dirty
       buffers are pinned.  (Clean buffers may be pinned by queries
       holding RD lock that access older array versions.)  For each
       dirty buffer which is not pending io, issue a write job.  At
       end, wait for jobs, then iterate again.  Repeat till no buffers
       are dirty.
     */

    size_t numDirty= 0;
    for (size_t bufferMapPasses=0;
         bufferMapPasses==0 ||  numDirty > 0 ;
         ++bufferMapPasses) {

        BufferKey bk(dsk, 0, 0, 0);

        {  ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AC);

        // one pass over the bufferMap
        numDirty= 0;
        size_t numDirtyQueuedThisPass= 0;
        for (DSBufferMap::iterator dbit = _dsBufferMap.lower_bound(bk);
             dbit != _dsBufferMap.end() && dbit->first.getDsk() == dsk;
             ++dbit ) {
            BlockPos slot = dbit->second;
            BlockHeader& bhead = _blockList[slot];

            if (bhead.isDirty()) {
                ASSERT_EXCEPTION(!bhead.pinCount, "unpinned dirty buffer in flushAllBuffers " << dsk.toString());

                numDirty++;
                if (!bhead.pending) {
                    numDirtyQueuedThisPass++;
                    // clang-format off
                    LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers, claiming, queueing write job {"
                                          << " \"slot\": " << slot
                                          << ", \"bhead\": "<< bhead.toString()
                                          << " }");
                    // clang-format on
                    SCIDB_ASSERT(bhead.blockBase);
                    bhead.pending = true;                       // marking
                    LOGCLAIM("Pending", bhead.size, bhead);
                    _claimPendingBytes(bhead.size);             // claiming
                    if (bhead.compressedBlockBase) {
                        LOGCLAIM("Pending_compressed", bhead.size, bhead);
                        _claimPendingBytes(bhead.size);
                    }
                    _claimPendingSlot();

                    std::shared_ptr<IoJob> job =
                        make_shared<IoJob>(IoJob::IoType::Write, slot, *this);
                    _jobQueue->pushJob(job);                    // queueing
                }
            }
        } // end one pass over BufferMap
        LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers bufferMapPasses " << bufferMapPasses
                                                            << " numDirty " << numDirty
                                               << "numDirtyQueuedThisPass " << numDirtyQueuedThisPass);

        // sdb-6601 optimization: O(n*m) to O(m)
        // we used to wait for every write completion, but such checks hold the mutex
        // for the entire BufferMap traversal, slowing down writer threads from dequeing their work.
        // Together, for n writes and m BufferMap length, took O (n * m)
        // Now we enqeue a marker which is after all the currently enqueued writes, and only wake up again when
        // the marker is processed.  Waiting for this marker is not sufficient for all to have completed, because
        // the IoJobs are serviced by multiple threads, but it gets us close enough to the end that we can
        // afford to keep checking (and re-enquing markers) a finite number of times. This is O(m)
        if (numDirty) {    // if any writes in progress, wait on a batch of them
            std::shared_ptr<IoJob> job =
                make_shared<IoJob>(IoJob::IoType::WriteQueueBatchPoint, BufferHandle::INVALID_SLOT, *this);
            _jobQueue->pushJob(job);
            LOG4CXX_TRACE(loggerWait, "BufferMgr::flushAllBuffers, pwbc.wait (flushing) start");
            _pendingWriteBatchCond.wait(_blockMutex, PTW_COND_BM_PWB_FLUSHALL);
            LOG4CXX_TRACE(loggerWait, "BufferMgr::flushAllBuffers, pwbc.wait (flushing) complete");
        }
        InjectedErrorListener::throwif(__LINE__, __FILE__);

        } // end ScopedMutexLock
    } // end all passes

    LOG4CXX_TRACE(logger, "BufferMgr::flushAllBuffers, all IOs completed");

    /* Flush the data store data and metadata to ensure all writes are on
       disk.
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);

    ds->flush();
}

void BufferMgr::discardBuffer(BufferKey& bk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::discardBuffer (bk: " << bk << ")");
    ScopedMutexLock sm(_blockMutex, PTW_SML_BM_DISCARD);

    /* Look for the buffer in the cache
     */
    DSBufferMap::iterator dbit = _dsBufferMap.find(bk);
    if (dbit != _dsBufferMap.end()) {
        /* Found it, clear it out
         */
        BlockPos slot = dbit->second;
        BlockHeader& bhead = _blockList[slot];

        SCIDB_ASSERT(bhead.pinCount == 0);

        // NOTE: With gcc 4.9 this trick of using (void) to silence -Wunused-result no
        // longer works, however _removeBufferFromCache does not use
        // __attribute__((warn_unused_result));
        (void) _removeBufferFromCache(dbit);
    }

    /* Deallocate the space in the backend data store
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(bk.getDsk());

    ds->freeChunk(bk.getOffset(), bk.getAllocSize());
}

void BufferMgr::_discardBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_discardBuffer (bh: " << bh << ")");
    discardBuffer(bh._key);
}

/* Discard all buffers for the given key, and remove the datastore from
   the disk
 */
void BufferMgr::discardAllBuffers(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::discardAllBuffers (dsk: " << dsk.toString() << ")");
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AE);

    /* Remove all keys with same dsk from the cache
     */
    BufferKey bk(dsk, 0, 0, 0);
    DSBufferMap::iterator dbit = _dsBufferMap.lower_bound(bk);
    while (dbit != _dsBufferMap.end() && dbit->first.getDsk() == dsk) {
        if (_removeBufferFromCache(dbit++)) {
            // TODO: no coverage
            // Mutex was dropped, _dsBufferMap may have changed radically.
            // Refresh the iterator to start-of-dsk.
            dbit = _dsBufferMap.lower_bound(bk);
        }
    }

    /* Remove the data store from the disk
     */
    DataStores::getInstance()->closeDataStore(dsk, true /*remove from disk */);
}

void BufferMgr::flushMetadata(DataStore::DataStoreKey& dsk)
{
    LOG4CXX_TRACE(logger, "BufferMgr::flushMetadata (dsk: " << dsk.toString() << ")");
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AF);

    /* It suffices to call flush on the datastore...
     */
    std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(dsk);
    ds->flush();
}

arena::ArenaPtr BufferMgr::getArena()
{
    return _arena;
}

BufferMgr::BufferMgr()
    : InjectedErrorListener(InjectErrCode::WRITE_CHUNK)
    , _bytesCached(0)
    , _dirtyBytes(0)
    , _pinnedBytes(0)
    , _pendingBytes(0)
    , _reserveReqBytes(0)
    , _pendingSlots(0)
    , _reserveReqSlots(0)
    , _usedMemLimit(Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD) * MiB)
    , _chunkSizeLimit(Config::getInstance()->getOption<size_t>(CONFIG_CHUNK_SIZE_LIMIT) * MiB)
{
    // note: rle_strings255.test requires at least 11 slots to pass
    _blockList.resize(Config::getInstance()->getOption<int>(CONFIG_BUFFERMGR_SLOTS_MAX));
    _lruDirty = MAX_BLOCK_POS;
    _mruDirty = MAX_BLOCK_POS;
    _lruClean = MAX_BLOCK_POS;
    _mruClean = MAX_BLOCK_POS;
    _freeHead = 0;

    // TODO: factor and use _addToFreeList()
    for (size_t i = 0; i < _blockList.size(); i++) {
        /* Set the links for the freelist
         */
        _blockList[i].setNextFree(i + 1);
    }
    _blockList[_blockList.size() - 1].setNextFree(MAX_BLOCK_POS);

    /* Create the arena for buffers
     */
    _arena = arena::newArena(arena::Options("buffer-manager")
                                 .threading(true)  // thread safe
                                 .resetting(true)  // support en-masse deletion
                                 .recycling(true)  // support eager recycling of sub-allocations
    );

    /* Create the async i/o thread pool and job queue and start the
       threads
     */
    _jobQueue = std::shared_ptr<JobQueue>(new JobQueue("BufferMgr IoQueue"));
    auto numPrefetchThreads = Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_THREADS);

    _asyncIoThreadPool = std::shared_ptr<ThreadPool>(
        new ThreadPool(numPrefetchThreads,
                       _jobQueue,
                       "BufferMgr ThreadPool"));
    _asyncIoThreadPool->start();
    LOG4CXX_INFO(logger, "BufferMgr::BufferMgr: IoThreadPool started, size = " << numPrefetchThreads << ")");

    InjectedErrorListener::start();
}

void BufferMgr::getByteCounts(BufferStats& bufStats)
{
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AG);
    bufStats.cached = _bytesCached;
    bufStats.dirty = _dirtyBytes;
    bufStats.pinned = _pinnedBytes;
    bufStats.pending = _pendingBytes;
    bufStats.reserveReq = _reserveReqBytes;
    bufStats.usedLimit = _usedMemLimit;
}

//
// IoJob implementation
//
BufferMgr::IoJob::IoJob(IoType reqType, BlockPos target, BufferMgr& mgr)
    : Job(BufferMgr::_bufferQuery, "BufMgrIoJob")
    , _reqType(reqType)
    , _targetSlot(target)
    , _buffMgr(mgr)
{}

/* IoJob Implementation
 */
std::shared_ptr<Query> const& BufferMgr::_bufferQuery(NULL);

void BufferMgr::IoJob::run()
{
    switch (_reqType) {
    case IoType::Write:
        _buffMgr._writeSlotUnlocked(_targetSlot);
        break;
    case IoType::WriteQueueBatchPoint:          // a marker used to avoid waiting on individual write completions
        _buffMgr._writeQueueBatchPoint();
        break;
    default:
        ASSERT_EXCEPTION_FALSE("unreachable");
        break;
    }
}

//
// more BufferMgr implementation
// TODO: join up with the other section of these as a non-functional change
//       by moving BlockHeader and IoJob stuff before all BufferMgr stuff
//

BufferMgr::BufferHandle BufferMgr::_pinBufferLockedWithRetry(BufferKey& bk, CompressorType cType)
{
    for (int retries = 0; retries < MAX_PIN_RETRIES; ++retries) {
        try {
            return _pinBufferLocked(bk, cType);
        } catch (RetryPinException& ex) {
            // TODO: no coverage
            // No worries, try again!  Problem has already been logged.
        }
    }

    // TODO: no coverage
    // Bummer, still here.
    throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
}

/**
 * Return a buffer handle for the indicated buffer.
 * @pre The _blockMutex is held
 * @post The buffer is returned ''pinned''.
 * @throws MemArrayThresholdException if buffer is not cached and no space can be made available,
 *         BufferMgrSlotMaxException if no slot can be made available.
 *         OutOfMemoryException if cannot allocate a temporary compression buffer
 */
BufferMgr::BufferHandle BufferMgr::_pinBufferLocked(BufferKey& bk, CompressorType cType)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked for bk=" << bk);
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    /* Look for the buffer in the cache
     */
    DSBufferMap::iterator dbmit = _dsBufferMap.find(bk);
    if (dbmit != _dsBufferMap.end()) {
        // Found it
        BlockPos slot = dbmit->second;
        BlockHeader& bhead = _blockList[slot];
        BufferHandle bhand(bk, slot, bhead.genCount, cType);

        while (bhead.pinCount == 0 && bhead.pending) {
            // TODO: no coverage, bhead.pending can be forced with a flush, so flush and somehow
            //       update the chunk again ... but how?

            LOG4CXX_TRACE(loggerWait, "BufferMgr::_pinBufferLocked pwc.wait 1 (wait for pending write) start");
            _pendingWriteCond.wait(_blockMutex, PTW_COND_BM_PW__PIN_LOCKED);
            LOG4CXX_TRACE(loggerWait, "BufferMgr::_pinBufferLocked pwc.wait 1 (wait for pending write) complete");
            // NOTE: good performance at pwc.wait 1 requires others to not hold _blockMutex for a long
            //    time repeatedly. Also this will wake on every write completion, not just the one for
            //    this slot. TODO: one we know how to cover this case, time it with 20,000 writes
            //    active, we may need to use some kind of "writeBatch" approach like flushAllBuffers
        }

        //
        // check for slot contents changing during the wait in the while loop above
        //
        bool hasInjected = hasInjectedError(SLOT_GENCOUNT_CHANGE, __LINE__, __FILE__);
        if (bhead.genCount != bhand.getGenCount() || hasInjected) {
            if (bhead.genCount != bhand.getGenCount()) {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked actual slot change, slot" << slot);
                if (isDebug()) {
                    dbmit = _dsBufferMap.find(bk);
                    SCIDB_ASSERT(dbmit == _dsBufferMap.end() || dbmit->second != slot);
                }
            } else {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked injected/simulated slot change");
            }
            throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
        }

        if(bhead.isClean()) {
            _updateCleanList(slot);
        } else {
            _updateDirtyList(slot);
        }
        bhead.pinCount++;
        if (bhead.pinCount == 1) {
            LOGCLAIM("Pinned", bhead.size, bhead);
            _claimPinnedBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                _claimPinnedBytes(bhead.size);
            }
        }
        return bhand;

    } else {
        /* The buffer wasn't in the cache, find a space in the block list
         * and resize if necessary
         */
        BlockPos slot = _allocateBlockHeader(bk.getDsk(), bk.getSize(), bk.getCompressedSize(), cType);
        BlockHeader& bhead = _blockList[slot];
        LOGCLAIM("SpaceUsed", bhead.size, bhead);
        LOGCLAIM("Pinned", bhead.size, bhead);
        _claimSpaceUsed(bhead.size);
        _claimPinnedBytes(bhead.size);

        // The _reserveSpace() call, invoked above from _allocateBlockHeader(),
        // can release the _blockMutex.  We need to revalidate the dbmit
        // because someone else could have brought in the buffer we seek.
        bool hasInjected = hasInjectedError(SLOT_OTHER_THREAD_LOAD, __LINE__, __FILE__);
        dbmit = _dsBufferMap.find(bk);
        if (dbmit != _dsBufferMap.end() || hasInjected) {
            // The sought-for buffer is now resident, but almost
            // certainly in a different slot, so we need a retry.
            // to undo our work and try to pick up that result
            // The slot we just allocated is no longer needed, oh well.
            if (dbmit != _dsBufferMap.end()) {  // rare case
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked lost race to swap in block " << bk);
            } else {
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked injected SLOT_OTHER_THREAD_LOAD");
            }

            // _dsBufferMap.erase(bk);  no, we never set it

            // unpin the slot
            SCIDB_ASSERT(bhead.pinCount == 1);  // from _allocateBlockHeader()
            bhead.pinCount = 0;
            LOGRELEASE("Pinned", bhead.size, bhead);
            _releasePinnedBytes(bhead.size);

            SCIDB_ASSERT(!bhead.compressedBlockBase);  // not yet allocated

            _releaseBlockHeader(slot);
            throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException);
        }

        SCIDB_ASSERT(bk.getSize() == bhead.size);
        SCIDB_ASSERT(bk.getCompressedSize() == bhead.compressedSize);
        SCIDB_ASSERT(bk.getDsk() == bhead.dsk);

        // Update the slot values
        bhead.offset = bk.getOffset();

        // Read the data from the disk
        std::shared_ptr<DataStore> ds;

        if (cType == CompressorType::NONE) {
            // clang-format off
            LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked swap in buffer.  "
                          << "[slot=" << slot << "]"
                          << "[offset=" << bhead.offset << "]"
                          << "[dsk=" << bhead.dsk.getNsid() << "," << bhead.dsk.getDsid() << "]"
                          << "[size=" << bhead.size << "]"
                          << "[blockptr=" << (void*) bhead.blockBase << "]");
            // clang-format on
            ds = DataStores::getInstance()->getDataStore(bhead.dsk);
            bhead.allocSize = ds->readData(bhead.offset, bhead.blockBase, bhead.size);
        } else {
            SCIDB_ASSERT(bhead.compressedSize < bhead.size);
            // clang-format off
            LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked swap in compressed buffer.  "
                          << "[slot=" << slot << "]"
                          << "[offset=" << bhead.offset << "]"
                          << "[dsk=" << bhead.dsk.getNsid() << "," << bhead.dsk.getDsid() << "]"
                          << "[size=" << bhead.size << "]"
                          << "[compressedSize=" << bhead.compressedSize << "]"
                          << "[blockptr=" << (void*) bhead.blockBase << "]");
            // clang-format on

            // Create a temporary compression buffer in which to read the compressed
            // data from disk.
            try {
                bhead.compressedBlockBase =
                    allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_PBL>(*_arena,
                                                                             bhead.compressedSize,
                                                                             __LINE__,
                                                                             __FILE__);
                // For consistency, we claim these bytes. They will be released
                // immediately after read.
                LOGCLAIM("SpaceUsed_compressed", bhead.size, bhead);
                LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                _claimSpaceUsed(bhead.size);  // For the compressedBlockbase
                _claimPinnedBytes(bhead.size);

            } catch (std::exception& ex) {
                // clang-format off
                LOG4CXX_WARN(logger, "BufferMgr::_pinBufferLocked allocBlockBase failed at line " << __LINE__
                                     << " because " << ex.what());
                // clang-format on

                // Release the bytes for the blockbase which were claimed after the slot
                // was allocated.
                SCIDB_ASSERT(bhead.pinCount == 1);  // from _allocateBlockHeader()
                bhead.pinCount = 0;
                LOGRELEASE("Pinned", bhead.size, bhead);
                _releasePinnedBytes(bhead.size);

                _releaseBlockHeader(slot);

                throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
            }
            SCIDB_ASSERT(bhead.compressedBlockBase);

            ds = DataStores::getInstance()->getDataStore(bhead.dsk);
            bhead.allocSize =
                ds->readData(bhead.offset, bhead.compressedBlockBase, bhead.compressedSize);
            size_t len = CompressorFactory::getInstance()
                             .getCompressor(bhead.compressorType)
                             ->decompress(reinterpret_cast<void*>(bhead.blockBase),
                                          bhead.size,
                                          reinterpret_cast<const void*>(bhead.compressedBlockBase),
                                          bhead.compressedSize);
            arena::destroy(*_arena, bhead.compressedBlockBase);
            bhead.compressedBlockBase = nullptr;
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            _releasePinnedBytes(bhead.size);  // For the compressedBlockBase
            _releaseSpaceUsed(bhead.size);
            if (0 == len || len != bhead.size) {
                // 1. Decompression failed if the len is 0,
                // 2. If the uncompressed size (len) is not what is expected(bhead.size),
                //    then the data on disk was wrong (possibly incorrectly modified and
                //    written in another transaction, which used chunk.getData() when
                //    chunk.getConstData() was proper).
                //
                // The data on disk is corrupt. Check is performed after arena::destroy,
                // and _release..Bytes(), so that memory is returned to the Arena, and the
                // counters are set correctly.
                //
                // And clear the counters for the "raw buffer" (which were claimed after
                //  _allocateBlockHeader), as well.
                //
                SCIDB_ASSERT(bhead.pinCount == 1);  // from _allocateBlockHeader()
                bhead.pinCount = 0;
                LOGRELEASE("Pinned", bhead.size, bhead);
                _releasePinnedBytes(bhead.size);
                if (len == 0) {
                    LOG4CXX_FATAL(logger, "BufferMgr::_pinBufferLocked Compressed Data in DataStore is Corrupt"
                                          << " (Unsuccessful decompression): "
                                          << "{ \"blockHeader\": " << bhead.toString() << " }");
                } else {
                    LOG4CXX_FATAL(logger, "BufferMgr::_pinBufferLocked Compressed Data in DataStore is Corrupt"
                                          << " (Unexpected Data Written earlier): "
                                          << "{ \"len\": " << len
                                          << " , \"blockHeader\": " << bhead.toString()
                                          << " }");
                }
                _releaseBlockHeader(slot);
                SCIDB_ASSERT(false);  // Dump core for debugging purposes.
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CORRUPT_DATA_ON_DISK);
            }
        }
        LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked bhead after read: " << bhead.toString());

        // Enter slot into the DS Block Map
        DSBufferMap::value_type mapentry(bk, slot);
        auto result = _dsBufferMap.insert(mapentry);
        SCIDB_ASSERT(result.second);

        // Return the handle
        BufferHandle bhand(bk, slot, bhead.genCount, cType);
        LOG4CXX_TRACE(logger, "BufferMgr::_pinBufferLocked returning");
        return bhand;
    }
}

// helpers factored from _reserveSpace()
// all require blockMutex

// pre: blockMutex is held
bool BufferMgr::_slotNeeded() const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    return _freeHead >= _blockList.size();
}

// pre: blockMutex is held
size_t BufferMgr::_unCachedBytes() const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    SCIDB_ASSERT(_usedMemLimit >= _bytesCached);
    return _usedMemLimit - _bytesCached;
}

// pre: blockMutex is held
size_t BufferMgr::_extraUnCachedNeeded(size_t sz) const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    if(sz < _unCachedBytes()) {
        return 0;			// sufficient bytes already
    } else {
        return  sz - _unCachedBytes();	// amount beyond unCachedBytes we will need
    }
}

// pre: blockMutex is held
size_t BufferMgr::_excessPending() const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    SCIDB_ASSERT(_pendingBytes >= _reserveReqBytes);
    return _pendingBytes - _reserveReqBytes;
}

// pre: blockMutex is held
size_t BufferMgr::_extraPendingNeeded(size_t sz) const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    if(sz < _excessPending()) {
        return 0;			// sufficient bytes pending
    } else {
        return  sz - _excessPending();	// amount beyond unCachedBytes we will need
    }
}

size_t BufferMgr::_excessPendingSlots() const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    SCIDB_ASSERT(_pendingSlots >= _reserveReqSlots);
    return _pendingSlots - _reserveReqSlots;
}

bool BufferMgr::_extraPendingSlotNeeded() const
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    if(not _slotNeeded()) {
        return false;        // already have a slot
    }

    if(_excessPendingSlots()>0) { // an io is queued that will bring an extra slot
        SCIDB_ASSERT(_pendingSlots > _reserveReqSlots);
        return false;
    } else {
        return  true;	     // we need a pending slot
    }
}

/* Reserve Cache Space
   All routines which plan to put more data in the cache should
   first call "reserve(sz)".  This method keeps track of the
   remaining space, and kicks off necessary space freeing
   activities such as ejecting buffers from the LRU list and
   initiating write-back for dirty buffers.
   pre: blockMutex is held
   note: this routine may block waiting for background i/o
         to complete, releasing blockMutex for a while
   note: guarantee of free bytes upon return only lasts as
         long as blockMutex is held
 */
void BufferMgr::_reserveSpace(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace (sz: " << sz << ") start");

    static InjectedErrorListener s_injectErrReserveSpaceFail(InjectErrCode::RESERVE_SPACE_FAIL);
    if (s_injectErrReserveSpaceFail.test(__LINE__, __FILE__)) {
        LOG4CXX_ERROR(loggerReserve, "BufferMgr::_reserveSpace injected RESERVE_SPACE_FAIL, throwing");
        throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
    }

    LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace  sz " << sz << " vs _usedMemLimit " << _usedMemLimit);
    if (sz > _usedMemLimit) {
        LOG4CXX_ERROR(loggerReserve, "BufferMgr::_reserveSpace sz " <<  sz
                            << " > _usedMemLimit " << _usedMemLimit
                            << " reduce size of chunk or increase mem-array-threshold config." );
        throw SYSTEM_EXCEPTION_SUBCLASS(MemArrayThresholdException);
    }

    /* Calc the space we need to clear.  As long as it is positive, keep
       trying to clear space.
     */
    size_t remainingBytes = 0;
    unsigned int numEjects = 0;
    unsigned int numDestages = 0;
    unsigned int numWaits = 0;
    while ((remainingBytes = _extraUnCachedNeeded(sz)) > 0 || _slotNeeded()) {

        // insufficient bytes reserved
        // clang-format off
        LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace must eject clean buffers."
                      << " sz " << sz
                      << " remainingBytes " << remainingBytes
                      << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                      << " _extraUnCachedNeeded(sz) " << _extraUnCachedNeeded(sz)
                      << " _bytesCached " << _bytesCached
                      << " _usedMemLimit " << _usedMemLimit
                      << " numEjects " << numEjects
                      << " numWaits " << numWaits
                      << " numDestages " << numDestages);
        // clang-format on

        ++numEjects;
        if(_ejectCache(remainingBytes)) {
            continue;	// may be sufficient bytes and slot reserved, go check
        }

        // insufficient bytes or slot reserved
        remainingBytes = _extraUnCachedNeeded(sz);

        // clang-format off
        LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace must destage dirty buffers too."
                      << " sz " << sz
                      << " remainingBytes " << remainingBytes
                      << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                      << " _extraUnCachedNeeded(sz) " << _extraUnCachedNeeded(sz)
                      << " numEjects " << numEjects
                      << " numWaits " << numWaits
                      << " numDestages " << numDestages);
        // clang-format on

        ++numDestages;
        try{
            _destageCache(remainingBytes);
        } catch (std::exception) {
            // Ejection and destaging failed to enqueue enough cleared space/slots
            SCIDB_ASSERT(_extraUnCachedNeeded(sz) > 0 || _slotNeeded());

            // NOTE: it is not yet safe to inject an error to cause this case
            // because in Debug or Assert build, an unpin() during an interator
            // destruction (boo!) will assert and create a core

            // clang-format off
            LOG4CXX_WARN(loggerReserve, "BufferMgr::_reserveSpace _destageCached returned false."
                         << " sz " << sz
                         << " remainingBytes " << remainingBytes
                         << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                         << " _extraUnCachedNeeded(sz) " << _extraUnCachedNeeded(sz)
                         << " numEjects " << numEjects
                         << " numWaits " << numWaits
                         << " numDestages " << numDestages);
            // clang-format on
            throw ;
        }

        // bytes or slots were reserved, so a wait and cycle to top is required
        // clang-format off
        LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace destaged at least one buffer"
                      << " sz " << sz
                      << " remainingBytes " << remainingBytes
                      << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                      << " _extraUnCachedNeeded(sz) " << _extraUnCachedNeeded(sz)
                      << " numEjects " << numEjects
                      << " numWaits " << numWaits
                      << " numDestages " << numDestages);
        // clang-format on

        SCIDB_ASSERT(remainingBytes == _extraUnCachedNeeded(sz));  // no change during _destageCache
        SCIDB_ASSERT(remainingBytes || _slotNeeded());
        SCIDB_ASSERT(remainingBytes <= _pendingBytes);         // new is too much
        SCIDB_ASSERT(_reserveReqBytes <= _pendingBytes);    // old is too much
        SCIDB_ASSERT((remainingBytes + _reserveReqBytes) <= _pendingBytes); // sum is too much

        // we should be be needing space or a slot
        SCIDB_ASSERT(_extraUnCachedNeeded(sz) || _slotNeeded());
        // because of the _destage, there should be at least one byte and one slot pending to reserve
        SCIDB_ASSERT(_pendingBytes);
        SCIDB_ASSERT(_pendingSlots);

        ++numWaits;
        _waitOnReserveQueue(_extraUnCachedNeeded(sz), _slotNeeded());	// NOTE: blockMutex is released/acquired
        continue;	// may be sufficient bytes and slot reserved, go check
    }

    // clang-format off
    LOG4CXX_TRACE(loggerReserve, "BufferMgr::_reserveSpace() returning true @ end"
                  << " sz " << sz
                  << " remainingBytes " << remainingBytes
                  << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                  << " _extraUnCachedNeeded(sz) " << _extraUnCachedNeeded(sz)
                  << " numEjects " << numEjects
                  << " numWaits " << numWaits
                  << " numDestages " << numDestages);
    // clang-format on

    SCIDB_ASSERT(_extraUnCachedNeeded(sz) == 0);  // enough uncached space is available
    SCIDB_ASSERT(not _slotNeeded());              // request was satisfied
}

void BufferMgr::_writeQueueBatchPoint()
{
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AI);
    LOG4CXX_TRACE(loggerWait, "BufferMgr::_writeQueueBatchPoint() broadcasting");

    _pendingWriteBatchCond.broadcast();
}

/** Write data for requested slot to data store.
 *
 * Write data in @c target slot, and mark slot as no longer dirty when write complete.
 * Wakeup waiting thread if necessary. Called from async job.
 *
 * @pre _blockMutex not required to be held, taken internally (recursive mutex)
 */
void BufferMgr::_writeSlotUnlocked(BlockPos targetSlot)
{
    /* Get the header info under lock
     */

    BlockHeader bhcopy;
    {
        ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AH);
        BlockHeader& bhead = _blockList[targetSlot];
        bhcopy = bhead;
    }
    // clang-format off
    LOG4CXX_TRACE(logger, "bufferMgr::_writeSlotUnlocked  "
                          << "{ \"slot\": " << targetSlot
                          << " , \"bhcopy\": " << bhcopy.toString()
                          << "}");
    // clang-format on

    OnScopeExit finishAccounting([this, targetSlot, &bhcopy]() {
        /* Mark the buffer as not dirty, signal reserve waiters and
           write waiters
        */
        ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AI);
        BlockHeader& bhead = _blockList[targetSlot];
        SCIDB_ASSERT(bhead.genCount == bhcopy.genCount);
        // Confirm compressedBlockBase was updated if re-compression happened.
        SCIDB_ASSERT(bhead.compressedBlockBase == bhcopy.compressedBlockBase);

        _moveDirtyToCleanList(targetSlot);

        bhead.pending = false;
        LOGRELEASE("Dirty", bhead.size, bhead);
        _releaseDirtyBytes(bhead.size);

        if (bhead.compressedBlockBase) {
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
        }

        size_t pending = bhead.size;
        LOGRELEASE("Pending", bhead.size, bhead);
        if (bhead.compressedBlockBase) {
            LOGRELEASE("Pending_compressed", bhead.size, bhead);
            pending += bhead.size;
        }
        _signalReserveWaiters(pending);
        _releasePendingBytes(pending);
        _releasePendingSlot();
        _pendingWriteCond.broadcast();

        if (bhead.compressedBlockBase) {
            // Once the data in the compressedBlockBase are written to the DataStore, only
            // the data in the blockBase is needed for future READ operations.
            // Free the space in the cache taken by the now unnecessary compressedBlockBase.
            arena::destroy(*_arena, bhead.compressedBlockBase);
            bhead.compressedBlockBase = nullptr;
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            _releaseSpaceUsed(bhead.size);
        }

        LOG4CXX_TRACE(logger, "bufferMgr::_writeSlotUnlocked(slot "<<targetSlot<<") io complete "
                              << " { bhead: " << bhead.toString() << "}");
    });

    SCIDB_ASSERT(bhcopy.pinCount == 0);
    SCIDB_ASSERT(bhcopy.pending);
    SCIDB_ASSERT(bhcopy.blockBase);

    try {
        /* Write the data to the data store
         */
        std::shared_ptr<DataStore> ds = DataStores::getInstance()->getDataStore(bhcopy.dsk);
        if (CompressorType::NONE == bhcopy.compressorType) {
            ds->writeData(bhcopy.offset, bhcopy.blockBase, bhcopy.size, bhcopy.allocSize);
        } else {
            if (!bhcopy.compressedBlockBase) {
                // The compression should normally occur during DBArray::unpinChunk,
                // before the async write IoJob was scheduled in the chunk's unpin call.
                // However a buffer could have been incorrectly marked as "dirty" when
                // READ/WRITE access was requested (via ChunkIter::getWriteData()). In
                // that case the compressedBlockBase will be null when the finial unpin is
                // called. To prevent data loss or corruption, re-compress the blockbase
                // buffer into the compressedBlockBase.

                // clang-format off
                // DO not change the "Re-writing Compressed BlockBase" portion of this log
                // message or  the checkin/storage/badreadwrite test will fail.
                LOG4CXX_WARN(logger, "BufferMgr::_writeSlotUnlocked"
                                     << " Re-writing Compressed BlockBase. Bad getWriteData() call?: "
                                     << " { \"bhead\" " << bhcopy.toString()
                                     << " }");
                // clang-format on
                try {
                    bhcopy.compressedBlockBase =
                        allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_WSU>(*_arena,
                                                                                 bhcopy.size,
                                                                                 __LINE__,
                                                                                 __FILE__);
                } catch (std::exception& ex) {
                    // Since we didn't allocate memory for compressedBlockBase, and didn't
                    // update the accounting counters, make certain the machinery won't
                    // decrement too much. There is a precondition in OnScopeExit that
                    // the bhcopy.compressedBlockBase is the same as bhead.compressedBlockBase.
                    bhcopy.compressedBlockBase = nullptr;
                    // clang-format off
                    LOG4CXX_ERROR(logger, "BufferMgr::_writeSlotUnlocked allocBlockBase failed at line "
                                         << __LINE__
                                         << " because " << ex.what());
                    // clang-format on
                    throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
                }
                size_t compressedSize;
                compressedSize = CompressorFactory::getInstance()
                                     .getCompressor(bhcopy.compressorType)
                                     ->compress(bhcopy.compressedBlockBase,  // destination
                                                bhcopy.blockBase,            // source buffer
                                                bhcopy.size);                // source size

                if (compressedSize == bhcopy.size || compressedSize != bhcopy.compressedSize) {
                    // compress() failed (the compressedSize is the same as the source size).
                    // or The resulting size doesn't match what the IndexMgr has.
                    // The blockheader says it would succeed and would be
                    // bhcopy.compressedSize in size
                    LOG4CXX_ERROR(logger, "BufferMgr::_writeSlotUnlocked:"
                                         << " Compression failed, or resulted in unexpected size. {"
                                         << " \"compressedSize\": "<< compressedSize
                                         << " \"expected_size\": " << bhcopy.compressedSize
                                         << " \"bhcopy\": " <<  bhcopy.toString());

                    // We need to free the memory allocated in allocBlockBase, because we
                    // haven't set the original _blockList[targetSlot].compressedBlockBase to
                    // point to the allocated space yet.
                    arena::destroy(*_arena, bhcopy.compressedBlockBase);
                    bhcopy.compressedBlockBase = nullptr;

                    throw SYSTEM_EXCEPTION_SUBCLASS(CompressionFailureException);
                }

                SCIDB_ASSERT(bhcopy.compressedBlockBase);

                // Updating the original _blockList[targetSlot] and counters needs to be
                // under Mutex lock.
                {
                    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AJ);
                    BlockHeader& bhead = _blockList[targetSlot];
                    bhead.compressedBlockBase = bhcopy.compressedBlockBase;

                    SCIDB_ASSERT(bhead.genCount == bhcopy.genCount);

                    // Update the Byte Counters: the standard freeing of the slot will
                    // need to decrement counters based upon compressedBlockBase which
                    // is no longer the nullptr.

                    // Space Used,
                    LOGCLAIM("SpaceUsed_compressed", bhcopy.size, bhcopy);
                    _claimSpaceUsed(bhcopy.size);

                    // Dirty
                    LOGCLAIM("Dirty_compressed", bhcopy.size, bhcopy);
                    _claimDirtyBytes(bhcopy.size);

                    // Pinned
                    // The blockbase is not pinned so no more pinned bytes

                    // Pending
                    LOGCLAIM("Pending_compressed", bhcopy.size, bhcopy);
                    _claimPendingBytes(bhcopy.size);
                    // no _claimPendingSlot() here, it would cause a second slot to be claimed
                }
            }
            SCIDB_ASSERT(bhcopy.compressedBlockBase);
            ds->writeData(bhcopy.offset,
                          bhcopy.compressedBlockBase,
                          bhcopy.compressedSize,
                          bhcopy.allocSize);
        }
    } catch (const SystemException& e) {
        // This does not need to worry about de-allocating the compressed block base
        // that may have been allocated by allocBlockBase in this function, because
        // the blockbase will have been set as if it had come into the function
        // already set. So let the system cleans up in same the way as it would normally.

        // Since we have a failure, we need to abort all the currently running queries -- even if
        // they aren't associated with the bad buffer. The bad buffer write could have been
        // for a buffer that was opened for WRITE when it after being opened READ-only, and now
        // it's getting flushed after the fact.
        Query::freeQueriesWithWarning(SCIDB_WARNING(SCIDB_LE_CORRUPT_DATA_ON_DISK));
    } catch (...) {
        LOG4CXX_ERROR(logger, "Unexected exception in BufferMgr::_writeSlotUnlocked. Canceling queries.");
        Query::freeQueriesWithWarning(SCIDB_WARNING(SCIDB_LE_CORRUPT_DATA_ON_DISK));
    }
}

/* Find at least sz bytes worth of dirty, (not pending) buffers and issue write
   requests for them.  If sz is 0, but _pendingSlotNeeded(), then destage
   to make a slot available after its io completes.
   pre: blockMutex is held
   returns: true if (at least) szReq bytes of dirty buffers are
            made pendng and a slot is available or pending.
            false otherwise
 */
bool BufferMgr::_destageCache(const size_t bytesIn)
{
    struct DestageCacheDebug
    {
        size_t bytes_dpp; // !dirty !pinned !pending ... clean, available/free
        size_t bytes_dpP; // !dirty !pinned  pending ...        pending-without-pin-error
        size_t bytes_dPp; // !dirty  pinned !pending ... clean, software pinned
        size_t bytes_dPP; // !dirty  pinned  pending ... clean, being read/loaded
        size_t bytes_Dpp; //  dirty !pinned !pending ... dirty- must be sent to write/pending to free
        size_t bytes_DpP; //  dirty !pinned  pending ...        pending-without-pin-error
        size_t bytes_DPp; //  dirty  pinned !pending ... dirty, software pinned
        size_t bytes_DPP; //  dirty  pinned  pending ... dirty, being written/saved

        size_t slots_dpp; // !dirty !pinned !pending ... clean, available/free
        size_t slots_dpP; // !dirty !pinned  pending ...        pending-without-pin-error
        size_t slots_dPp; // !dirty  pinned !pending ... clean, software pinned
        size_t slots_dPP; // !dirty  pinned  pending ... clean, being read/loaded
        size_t slots_Dpp; //  dirty !pinned !pending ... dirty- must be sent to write/pending to free
        size_t slots_DpP; //  dirty !pinned  pending ...        pending-without-pin-error
        size_t slots_DPp; //  dirty  pinned !pending ... dirty, software pinned
        size_t slots_DPP; //  dirty  pinned  pending ... dirty, being written/saved

        DestageCacheDebug() :
            bytes_dpp(0), bytes_dpP(0), bytes_dPp(0), bytes_dPP(0),
            bytes_Dpp(0), bytes_DpP(0), bytes_DPp(0), bytes_DPP(0),
            slots_dpp(0), slots_dpP(0), slots_dPp(0), slots_dPP(0),
            slots_Dpp(0), slots_DpP(0), slots_DPp(0), slots_DPP(0)
        {}

        void categorizeBytes(const BufferMgr::BlockHeader& bhead, size_t bytes)
        { 
            if (!bhead.isDirty() && !bhead.pinCount && !bhead.pending) {
                bytes_dpp += bytes ; // !dirty !pinned !pending ... available/free
                slots_dpp++;
            }
            if (!bhead.isDirty() && !bhead.pinCount && bhead.pending) {
                bytes_dpP += bytes; // !dirty !pinned pending ...  unpinned pending error
                slots_dpP++;
            }
            if (!bhead.isDirty() && bhead.pinCount && !bhead.pending) {
                bytes_dPp += bytes; //  !dirty pinned !pending ... clean software pinned
                slots_dPp++;
            }
            if (!bhead.isDirty() && bhead.pinCount && bhead.pending) {
                bytes_dPP += bytes; //  pinned !dirty  pending ... clean pinned pending (being read)
                slots_dPP++;
            }
            if (bhead.isDirty() && !bhead.pinCount && !bhead.pending) {
                bytes_Dpp += bytes; // dirty !pinned  !pending ...  dirty to be written
                slots_Dpp++;
            }
            if (bhead.isDirty() && !bhead.pinCount && bhead.pending) {
                bytes_DpP += bytes; // dirty !pinned  pending ...  dirty pending error
                slots_DpP++;
            }
            if (bhead.isDirty() && bhead.pinCount && !bhead.pending) {
                bytes_DPp += bytes; //  pinned  dirty !pending ...  dirty software pinned
                slots_DPp++;
            }
            if (bhead.isDirty() && bhead.pinCount && bhead.pending) {
                bytes_DPP += bytes; //  pinned  dirty  pending ... dirty and being written 
                slots_DPP++;

            }
        }

        void log4(){
            LOG4CXX_DEBUG(logger, "_destageCache d p p "
                                  << bytes_dpp << " slots " << slots_dpp << " free");
            LOG4CXX_DEBUG(logger, "_destageCache d p P "
                                  << bytes_dpP << " slots " << slots_dpP << " clean, (unused?)");
            LOG4CXX_DEBUG(logger, "_destageCache d P p "
                                  << bytes_dPp << " slots " << slots_dPp << " clean, sw-pinned");
            LOG4CXX_DEBUG(logger, "_destageCache d P P "
                                  << bytes_dPP << " slots " << slots_dPP << " clean, pinned-and-reading");
            LOG4CXX_DEBUG(logger, "_destageCache D p p "
                                  << bytes_Dpp << " slots " << slots_Dpp << " dirty");
            LOG4CXX_DEBUG(logger, "_destageCache D p P "
                                  << bytes_DpP << " slots " << slots_DpP << " dirty, write in progress");
            LOG4CXX_DEBUG(logger, "_destageCache D P p "
                                  << bytes_DPp << " slots " << slots_DPp << " dirty, sw-pinned");
            LOG4CXX_DEBUG(logger, "_destageCache D P P "
                                  << bytes_DPP << " slots " << slots_DPP << " dirty, (unused?)" );
        }
    };
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    DestageCacheDebug slotsDebug;

    LOG4CXX_TRACE(loggerDestage, "BufferMgr::_destageCache(bytesIn " << bytesIn << ") start");
    size_t bytesRemaining = bytesIn ;

    // Walk the lru list queuing dirty buffers to write
    size_t numDestages=0;
    BlockPos currentSlot = _lruDirty;
    while(bytesRemaining > 0 || _extraPendingSlotNeeded()) {

        // clang-format off
        LOG4CXX_TRACE(loggerDestage, "BufferMgr::_destageCache next slot"
                      << " currentslot " << currentSlot
                      << " bytesRemaining " << bytesRemaining
                      << " _extraPendingSlotNeeded() " << _extraPendingSlotNeeded());
        // clang-format on

        static InjectedErrorListener s_injectErrBlockListEnd(InjectErrCode::BLOCK_LIST_END);
        bool injectErr = s_injectErrBlockListEnd.test(__LINE__, __FILE__);
        if (currentSlot >= _blockList.size() || injectErr) {
            if (injectErr) {
                LOG4CXX_WARN(logger, "BufferMgr::_destageCache, injected BLOCK_LIST_END");
            }
            // End of the lru list, return false
            SCIDB_ASSERT(bytesRemaining > 0 || _extraPendingSlotNeeded());

            LOG4CXX_TRACE(logger, "BufferMgr::_destageCache("<<bytesIn<<") incomplete, returning false."
                          << " bytesRemaining " << bytesRemaining
                          << " numDestages " << numDestages
                          << " _extraPendingSlotNeeded() " << _extraPendingSlotNeeded()
                          << " currentSlot " << currentSlot);
            slotsDebug.log4();
            return false;   // failure
        }

        BlockHeader& bhead = _blockList[currentSlot];
        size_t headerBytes = bhead.size;
        if (bhead.compressedBlockBase) {
            headerBytes += bhead.size;
        }

        if ((bhead.isDirty()) && (bhead.pinCount == 0) && (!bhead.pending)) {
            /* Send write request for dirty buffer
             */
            // clang-format off
            LOG4CXX_TRACE(loggerDestage, "BufferMgr::destageCache, pincount 0 on dirty buffer, "
                                         << "issuing write job: "
                                         << "{\"slot\":" << currentSlot
                                         << " , \"size\": " << bhead.size
                                         << " , \"csize\": " << bhead.compressedSize
                                         << " , \"bhead\":" << bhead.toString()
                                         << "}");
            // clang-format on
            SCIDB_ASSERT(bhead.blockBase);

            bhead.pending = true;
            LOGCLAIM("Pending", bhead.size, bhead);
            _claimPendingBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pending_compressed", bhead.size, bhead);
                _claimPendingBytes(bhead.size);
            }
            _claimPendingSlot();

            std::shared_ptr<IoJob> job =
                make_shared<IoJob>(IoJob::IoType::Write, currentSlot, *this);
            _jobQueue->pushJob(job);

            ++numDestages;
            bytesRemaining -= std::min(bytesRemaining, headerBytes);
        }

        slotsDebug.categorizeBytes(bhead, headerBytes);  // update debug accounting
        currentSlot = bhead.getNextDirty();
    }

    LOG4CXX_TRACE(loggerDestage, "BufferMgr::_destageCache(bytesIn " << bytesIn << ") returning true with"
                          << " bytesRemaining " << bytesRemaining << " asserted 0"
                          << " numDestages " << numDestages
                          << " _extraPendingSlotNeeded() " << _extraPendingSlotNeeded() << " asserted 0"
                          << " _pendingSlots " << _pendingSlots
                          << " _reserveReqSlots " << _reserveReqSlots);

    SCIDB_ASSERT(bytesRemaining == 0);
    SCIDB_ASSERT(not _extraPendingSlotNeeded());
    SCIDB_ASSERT(_pendingSlots > 0) ;   // otherwise we return true, when in fact there is nothing pending

    return true;    // success, caller must wait
}

/* Find at least sz bytes worth of unpinned clean buffers and
   eject them from the cache.
   pre: blockMutex is held
   returns: true if sz bytes were cleared, false otherwise
*/
bool BufferMgr::_ejectCache(size_t szIn)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(loggerEject, "BufferMgr::_ejectCache szIn " << szIn);
    size_t sz = szIn;

    // Walk the lru list
    size_t slotsExamined = 0;
    size_t slotsEjected = 0;
    BlockPos currentSlot = _lruClean;
    while (sz > 0 || _slotNeeded()) {
        // clang-format off
        LOG4CXX_TRACE(loggerEject, "BufferMgr::_ejectCache top of loop"
                                   << " sz " << sz
                                   << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                                   << " slot " << currentSlot);
        // clang-format on

        if (currentSlot >= _blockList.size()) { // End of the lru list, return false
            SCIDB_ASSERT(sz > 0 || _slotNeeded());
            LOG4CXX_TRACE(loggerEject, "BufferMgr::_ejectCache("<<szIn<<"):"
                                       << " still need bytes: " << sz
                                       << " _slotNeeded() " << (_slotNeeded() ? "true" : "false")
                                       << " slotsExamined " << slotsExamined
                                       << " slotsEjected " << slotsEjected);
            return false;
        }

        ++slotsExamined;
        BlockHeader& bhead = _blockList[currentSlot];

        SCIDB_ASSERT(bhead.isClean());
        if (bhead.pinCount == 0) {
            /* Eject clean, unpinned buffer
             */
            size_t amt_ejected = bhead.size;
            if (bhead.compressedBlockBase != nullptr) {
                amt_ejected += bhead.size;
                // note coverage of BUFFER_RESERVE_FAIL followed by clear_cache (BufferMgr_inject.test)
                LOG4CXX_WARN(loggerEject, "BufferMgr::_ejectCache: compressedBlockBase on unpinned, clean buffer");
            }
            sz = (amt_ejected > sz) ? 0 : sz - amt_ejected;

            BlockPos next = bhead.getNextClean();
            BufferKey bk(bhead.dsk, bhead.offset, bhead.size, bhead.allocSize);
            _dsBufferMap.erase(bk);

            _releaseBlockHeader(currentSlot);
            ++slotsEjected;

            currentSlot = next;
            LOG4CXX_TRACE(loggerEject, "BufferMgr::_ejectCache {\"ejected\": " << amt_ejected
                                       << " sz " << sz
                                       << " slotsExamined " << slotsExamined
                                       << " slotsEjected " << slotsEjected);
        } else {
            // pinned or dirty entries cannot be ejected
            LOG4CXX_TRACE(loggerEjectSkipped, "BufferMgr::_ejectCache Nothing to eject: "
                                              << " slotsExamined " << slotsExamined
                                              << " slotsEjected " << slotsEjected
                                              << "{\"bhead\": " << bhead.toString()
                                              << "}");
            currentSlot = bhead.getNextClean(); // go to the next in the LRU
        }
    }

    LOG4CXX_TRACE(loggerEject, "BufferMgr::_ejectCache("<<szIn<<"): success "
                               << " slotsExamined " << slotsExamined
                               << " slotsEjected " << slotsEjected
                               << " _blockList.size() " << _blockList.size());

    SCIDB_ASSERT(sz == 0);
    SCIDB_ASSERT(not _slotNeeded());
    return true;
}


/* Mark a certain amount of space as being used in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimSpaceUsed(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    _bytesCached += sz;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_claimSpaceUsed request: "
                                       << sz << " (post) _bytesCached " << _bytesCached
                                       << " _usedMemLimit " << _usedMemLimit);
    // clang-format on
    ASSERT_EXCEPTION(_bytesCached <= _usedMemLimit, "Cache size exceeds limit in claim.");
}

/* Mark a certain amount of space as being un-used
   pre: blockMutex is held
 */
void BufferMgr::_releaseSpaceUsed(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ASSERT_EXCEPTION(_bytesCached >= sz, "BufferMgr::_releaseSpaceUsed, more released than used");
    _bytesCached -= sz;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_releaseSpaceUsed request: " << sz
                                       << " (post) _bytesCached " << _bytesCached
                                       << " _usedMemLimit " << _usedMemLimit);
    // clang-format on
    ASSERT_EXCEPTION(_bytesCached <= _usedMemLimit, "Cache size wrap-around in release.");
}

/* Mark a certain amount of space as being dirty in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimDirtyBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    _dirtyBytes += sz;
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_claimDirtyBytes request: " << sz 
                                       << " (post) _dirtyBytes " << _dirtyBytes);
    ASSERT_EXCEPTION(_dirtyBytes <= _usedMemLimit, "Dirty bytes exceeds limit in claim.");
}

/* Mark a certain amount of space as being not dirty in the cache
   pre: blockMutex is held
*/
void BufferMgr::_releaseDirtyBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ASSERT_EXCEPTION(_dirtyBytes >= sz, "BufferMgr::_releaseDirtyBytes, more released than dirty");
    _dirtyBytes -= sz;
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_releaseDirtyBytes request: " << sz
                                       << " (post) _dirtyBytes " << _dirtyBytes);
    ASSERT_EXCEPTION(_dirtyBytes <= _usedMemLimit, "Dirty bytes wrap-around in release.");
}

/* Mark a certain amount of space as being pinned in the cache
   pre: blockMutex is held
 */
void BufferMgr::_claimPinnedBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    _pinnedBytes += sz;
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_claimPinnedBytes request: " << sz
                                       << " (post) _pinnedBytes " << _pinnedBytes);
    ASSERT_EXCEPTION(_pinnedBytes <= _usedMemLimit, "Pinned bytes exceeds limit in claim.");
}

/* Mark a certain amount of space as being unpinned in the cache
   pre: blockMutex is held
 */
void BufferMgr::_releasePinnedBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ASSERT_EXCEPTION(_pinnedBytes >= sz, "BufferMgr::_releasePinnedBytes, more released than pinned");
    _pinnedBytes -= sz;
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_releasePinnedBytes request: " << sz
                                       << " (post) _pinnedBytes " << _pinnedBytes);
    ASSERT_EXCEPTION(_pinnedBytes <= _usedMemLimit, "Pinned bytes wrap-around in release.");
}

/* Mark a certain amount of space as pending a write
   pre: blockMutex is held
 */
void BufferMgr::_claimPendingBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    _pendingBytes += sz;
    // clang-format off
        LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_claimPendingBytes request: " << sz
                      << " (post) _pendingBytes " << _pendingBytes
                      << " _reserveReqBytes " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request exceeds pending in claim.");
}

void BufferMgr::_claimPendingSlot()
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ++_pendingSlots;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelSlots, "BufferMgr::_claimPendingSlot request: "
                      << " (post) _pendingSlots " << _pendingSlots
                      << " _reserveReqSlots " << _reserveReqSlots);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqSlots <= _pendingSlots, "Reserve request exceeds pending in claim.");
}

/* Mark a certain amount of space as being no longer pending write
   pre: blockMutex is held
 */
void BufferMgr::_releasePendingBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ASSERT_EXCEPTION(_pendingBytes >= sz, "BufferMgr::_releasePendingBytes, more released than pending");
    _pendingBytes -= sz;
    // clang-format off
        LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_releasePendingBytes request: " << sz
                      << " (post) _pendingBytes " << _pendingBytes
                      << " _reserveReqBytes " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_pendingBytes <= _usedMemLimit, "Pending bytes wrap-around in release.");
}

void BufferMgr::_releasePendingSlot()
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    SCIDB_ASSERT(_pendingSlots > 0);
    --_pendingSlots;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelSlots, "BufferMgr::_releasePendingSlot request: "
                      << " (post) _pendingSlots " << _pendingSlots
                      << " _reserveReqSlots " << _reserveReqSlots);
    // clang-format on
}

/* Record an increase in the requested reserve
   pre: blockMutex is held
 */
void BufferMgr::_claimReserveReqBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    _reserveReqBytes += sz;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_claimReserveReqBytes request: " << sz
                                        << " _pendingBytes " << _pendingBytes
                                        << " (post) _reserveReqBytes " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request bytes exceeds pending in claim.");
}

void BufferMgr::_claimReserveReqSlot()
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ++_reserveReqSlots;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelSlots, "BufferMgr::_claimReserveReqSlot request: 1 "
                  << " _pendingSlots " << _pendingSlots
                  << " (post) _reserveReqSlots " << _reserveReqSlots);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqSlots <= _pendingSlots, "Reserve request slots exceeds pending in claim.");
}

/* Record a decrease in the requested reserve
   pre: blockMutex is held
*/
void BufferMgr::_releaseReserveReqBytes(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    ASSERT_EXCEPTION(_reserveReqBytes >= sz, "BufferMgr::_releaseReserveReqBytes, more released than reserved");
    _reserveReqBytes -= sz;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelBytes, "BufferMgr::_releaseReserveReqBytes request: " << sz
                                       << " _pendingBytes " << _pendingBytes
                                       << " (post) _reserveReqBytes " << _reserveReqBytes);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqBytes <= _pendingBytes, "Reserve request bytes exceeds pending in release.");
}

void BufferMgr::_releaseReserveReqSlot()
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    SCIDB_ASSERT(_reserveReqSlots >= 1);
    --_reserveReqSlots;
    // clang-format off
    LOG4CXX_TRACE(loggerClaimRelSlots, "BufferMgr::_releaseReserveReqSlot request: 1"
				        << "  _pendingSlots " << _pendingSlots
                                        << " (post) _reserveReqSlots " << _reserveReqSlots);
    // clang-format on
    ASSERT_EXCEPTION(_reserveReqSlots <= _pendingSlots, "Reserve request slots exceeds pending in release.");
}

/* Wait on the reserve queue until sz bytes have been written back
   to disk.
   pre: blockMutex is held
 */
void BufferMgr::_waitOnReserveQueue(size_t sz, bool slotNeeded)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(loggerWait, "BufferMgr::_waitOnReserveQueue start: sz " << sz
                              << " _reserveQueue.size() " << _reserveQueue.size());

    auto req = std::make_shared<SpaceRequest>(sz, slotNeeded);
    _reserveQueue.push(req);
    BlockHeader dummy;
    LOGCLAIM("ReserveReqBytes", sz, dummy);
    _claimReserveReqBytes(sz);
    if (slotNeeded) {
	    _claimReserveReqSlot();
    }

    while (!req->done) {
        LOG4CXX_TRACE(loggerWait, "BufferMgr::_waitOnReserveQueue wait start: "
                                   << " sz " << sz << " slotNeeded " << slotNeeded);
        req->ready.wait(_blockMutex, PTW_COND_BM_RESERVEQ);
        LOG4CXX_TRACE(loggerWait, "BufferMgr::_waitOnReserveQueue wait done: "
                                   << " sz " << sz << " slotNeeded " << slotNeeded);
    }
}

/* Release waiters on reserve queue when sz bytes have been cleaned
   or unpinned.
   pre: blockMutex is held
 */
void BufferMgr::_signalReserveWaiters(size_t sz)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(loggerWait, "BufferMgr::_signalReserveWaiters start: " << sz << " bytes");

    // attempt to satisfy waiters with sz bytes and 1 slot
    bool slot=true ;       // one slot to consume, implicitly on each signal

    BlockHeader dummy;
    while (_reserveQueue.size() > 0 &&
           (sz > 0 || slot)) {

        // bytes for satisfying the waiter
        size_t bytesToTake = std::min(_reserveQueue.front()->size, sz);

        _reserveQueue.front()->size -= bytesToTake;
        sz -= bytesToTake;

        LOGRELEASE("ReserveReqBytes", bytesToTake, dummy);
        _releaseReserveReqBytes(bytesToTake);
        SCIDB_ASSERT(_reserveReqBytes >= _reserveQueue.front()->size);

        // slot for satisfying the waiter
        bool slotToTake = std::min(_reserveQueue.front()->slot, true);
        _reserveQueue.front()->slot -= slotToTake;

        if(slotToTake) {
            LOGRELEASE("ReserveReqSlot", slotToTake, dummy);
            _releaseReserveReqSlot();
        }
        SCIDB_ASSERT(_reserveReqSlots >= _reserveQueue.front()->slot);

        // is the waiter completely satisfied (bytes and slot)?
        if (_reserveQueue.front()->size > 0 ||
            _reserveQueue.front()->slot) {

            break; // not yet
        }

        // yes, waiter is completely satisfied for bytes and slot
        // remove it from the queue, and Signal it.
        LOG4CXX_TRACE(loggerWait, "BufferMgr::_signalReserveWaiters signalling one waiter");
        auto req = _reserveQueue.front();
        _reserveQueue.pop();
        req->done = true;
        req->ready.signal();
    }
}

void BufferMgr::BlockHeader::reset(const DataStore::DataStoreKey& dataStoreKey,
                                   char* blockBasePtr,
                                   size_t blockSize,
                                   size_t compressedBlockSize,
                                   CompressorType blockCompressorType)
{
    pinCount = 1;
    genCount++;

    blockBase = blockBasePtr;
    size = blockSize;
    compressedSize = compressedBlockSize;
    compressorType = blockCompressorType;
    dsk = dataStoreKey;
}

/**
 * @brief Allocate and initialize a block header from the free list.
 *
 * @param dsk The DataStoreKey used for lookup
 * @param size The size of the blockBase memory region
 * @param compressedSize The size of the compressedBlockBase region
 * @param compressorType The desired compression method for the block
 * @return the slot number of the corresponding block header.
 */
BufferMgr::BlockPos BufferMgr::_allocateBlockHeader(const DataStore::DataStoreKey& dsk,
                                                    size_t size,
                                                    size_t compressedSize,
                                                    CompressorType compressorType)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_allocateBlockHeader  size " << size);

    if (_chunkSizeLimit > 0 && size > _chunkSizeLimit) {
        LOG4CXX_ERROR(logger, "BufferMgr::_allocateBlockHeader _allocateBlockHeader size " << size
                              << " exceeds chunk-size-limit " << _chunkSizeLimit);
        throw SYSTEM_EXCEPTION_SUBCLASS(ChunkSizeLimitException);
    }

    size_t reserveSpace = size;
    if (compressorType != CompressorType::NONE) {
        // Allocate space for the "tag-along" compression buffer. It will be no larger
        // than the size of the "raw buffer." Do not use compressedSize because other
        // bookkeeping areas don't necessarily know what the compressed size is. All
        // counters increment/decrement bytecounts by bhead.size for the compression
        // buffer.
        reserveSpace += size;
    }

    // Make room in the cache if necessary
    try {
        _reserveSpace(reserveSpace);
    } catch (std::exception& ex) { 
        LOG4CXX_ERROR(logger, "BufferMgr::_allocateBlockHeader _reserveSpace failed");
        throw;
    }

    char* blockBasePtr = nullptr;
    try {
        // NOTE: Do not allocate memory for the compressionBuffer here.
        // The compressionBuffer will be created elsewhere in a just-in-time manner.
        blockBasePtr = allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_ABH>(*_arena, size, __LINE__, __FILE__);
    } catch (std::exception& ex) {
        LOG4CXX_WARN(logger, "BufferMgr::_allocateBlockHeader allocBlockBase failed: " << ex.what());
        throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
    }
    SCIDB_ASSERT(blockBasePtr);

    BlockPos slot = BufferHandle::INVALID_SLOT;
    try {
        slot = _allocateFreeSlot();
        LOG4CXX_TRACE(logger, "BufferMgr::_allocateBlockHeader allocated slot " << slot);
    } catch (...) {
        arena::destroy(*_arena, blockBasePtr);
        throw;
    }
    auto& bhead = _blockList[slot];
    bhead.reset(dsk, blockBasePtr, size, compressedSize, compressorType);

    // block is allocated when bhead.priority != 0
    SCIDB_ASSERT(bhead.priority);

    LOG4CXX_TRACE(logger, "BufferMgr::_allocateBlockHeader returning slot " << slot);
    return slot;
}

/**
 * @brief De-allocate and release a block header to the free list.
 *
 * @param slot the slot of the corresponding block header.
 */
void BufferMgr::_releaseBlockHeader(BufferMgr::BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    auto& bhead = _blockList[slot];
    _freeSlot(slot);
    if(bhead.isClean()) {
        _moveCleanToFreeList(slot);
    } else {
        _moveDirtyToFreeList(slot);
    }
}

/* Allocate a free slot in the block list and return its position, resize
   the list if necessary
   pre: blockMutex is held
 */
BufferMgr::BlockPos BufferMgr::_allocateFreeSlot()
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_allocateFreeSlot()");
	ASSERT_EXCEPTION(not _slotNeeded(), "slot must be free at time of call");

    /* Remove from the freelist, reset to defaults, and mark as in-use.
     */
    BlockPos slot = _freeHead;
    BlockHeader& bhead = _blockList[slot];
    SCIDB_ASSERT(bhead.priority == 0);
    SCIDB_ASSERT(bhead.blockBase == nullptr);
    SCIDB_ASSERT(bhead.compressedBlockBase == nullptr);

    _freeHead = bhead.getNextFree();
    auto gencount = bhead.genCount;
    bhead = BlockHeader();      // reset to defaults
    bhead.priority = 1;         // mark as in-use
    bhead.genCount = gencount;

    /* Add to the lru
     */
    _addToCleanList(slot);

    LOG4CXX_TRACE(logger, "BufferMgr::_allocateFreeSlot returning slot " << slot);
    return slot;
}

/* Mark the indicated slot as free
   pre: blockMutex is held
 */
void BufferMgr::_moveCleanToFreeList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "bufferMgr::_moveCleanToFreeList  [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to mark as free");

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(bhead.isClean(), "_moveCleanToFreeList() on dirty buffer");

    /* Remove from the lru
     */
    _removeFromCleanList(slot);

    SCIDB_ASSERT(bhead.blockBase == nullptr);
    SCIDB_ASSERT(bhead.compressedBlockBase == nullptr);

    /* Mark free
     */
    SCIDB_ASSERT(bhead.pinCount == 0);
    bhead.priority = 0;
    SCIDB_ASSERT(bhead.isClean());

    /* Add to the free list
     */
    bhead.setNextFree(_freeHead);
    bhead.resetPrevFree();
    _freeHead = slot;
}

/* Mark the indicated slot as free
   pre: blockMutex is held
   note: same implementation as above with Clean changed to Dirty
   TODO: factor out Clean/Dirty from one common implementation
 */
void BufferMgr::_moveDirtyToFreeList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "bufferMgr::_moveDirtyToFreeList  [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to mark as free");

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(bhead.isDirty(), "_moveDirtyToFreeList() on dirty buffer");

    /* Remove from the lru
     */
    _removeFromDirtyList(slot);

    SCIDB_ASSERT(bhead.blockBase == nullptr);
    SCIDB_ASSERT(bhead.compressedBlockBase == nullptr);

    /* Mark free
     */
    SCIDB_ASSERT(bhead.pinCount == 0);
    bhead.priority = 0;
    bhead.markClean();

    /* Add to the free list
     */
    bhead.setNextFree(_freeHead);
    bhead.resetPrevFree();
    _freeHead = slot;
}

/* Remove slot from Clean LRU
   pre: blockMutex is held
 */
void BufferMgr::_removeFromCleanList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_removeFromCleanList [slot=" << slot << "]");

    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to remove from Clean lru");

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(bhead.isClean(), "_removeFromCleanList() on dirty block");

    /* Remove from the Clean lru
     */
    if (_lruClean == _mruClean) {
        ASSERT_EXCEPTION(slot == _lruClean, "corrupt Clean lru list in mark as free");
        // special case: last block
        _lruClean = _mruClean = MAX_BLOCK_POS;
    } else if (_lruClean == slot) {
        // special case: removing _lru
        _lruClean = bhead.getNextClean();
        _blockList[_lruClean].setPrevClean(MAX_BLOCK_POS);
    } else if (_mruClean == slot) {
        // special case: removing _mru
        _mruClean = bhead.getPrevClean();
        _blockList[_mruClean].setNextClean(MAX_BLOCK_POS);
    } else {
        // unlink from lru
        _blockList[bhead.getPrevClean()].setNextClean(bhead.getNextClean());
        _blockList[bhead.getNextClean()].setPrevClean(bhead.getPrevClean());
    }

    bhead.setNextFree(MAX_BLOCK_POS);
    bhead.resetPrevFree();
}

/* Remove slot from Dirty LRU
   pre: blockMutex is held
   note: same implementation as above with Clean changed to Dirty
   TODO: factor out Clean/Dirty from one common implementation
 */
void BufferMgr::_removeFromDirtyList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_removeFromDirtyList [slot=" << slot << "]");

    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot to remove from Dirty lru");

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(bhead.isDirty(), "_removeFromDirtyList() on dirty block");

    /* Remove from the Dirty lru
     */
    if (_lruDirty == _mruDirty) {
        ASSERT_EXCEPTION(slot == _lruDirty, "corrupt Dirty lru list in mark as free");
        // special case: last block
        _lruDirty = _mruDirty = MAX_BLOCK_POS;
    } else if (_lruDirty == slot) {
        // special case: removing _lru
        _lruDirty = bhead.getNextDirty();
        _blockList[_lruDirty].setPrevDirty(MAX_BLOCK_POS);
    } else if (_mruDirty == slot) {
        // special case: removing _mru
        _mruDirty = bhead.getPrevDirty();
        _blockList[_mruDirty].setNextDirty(MAX_BLOCK_POS);
    } else {
        // unlink from lru
        _blockList[bhead.getPrevDirty()].setNextDirty(bhead.getNextDirty());
        _blockList[bhead.getNextDirty()].setPrevDirty(bhead.getPrevDirty());
    }

    bhead.setNextFree(MAX_BLOCK_POS);
    bhead.resetPrevFree();
}

/* Move slot to the most recently used block
   pre: blockMutex is held
   pre: block is on the _lruClean list
 */
void BufferMgr::_updateCleanList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_updateCleanList [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in _updateCleanList");

    _removeFromCleanList(slot);
    _addToCleanList(slot);
}

/* Move slot to the most recently used block
   pre: blockMutex is held
   pre: block is on the lru Dirty
   note: same implementation as above with Clean changed to Dirty
   TODO: factor out Clean/Dirty from one common implementation
 */
void BufferMgr::_updateDirtyList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_updateDirtyList [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in _updateDirtyList");

    _removeFromDirtyList(slot);
    _addToDirtyList(slot);
}

/* Add slot to end of Clean LRU list (mru block)
   pre: blockMutex is held
   pre: block is not on a list
 */
void BufferMgr::_addToCleanList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_addToCleanList [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in addToClean");

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(bhead.priority, "free slot in addToClean");

    ASSERT_EXCEPTION(bhead.isClean(), "_addToCleanList() on dirty block");

    bhead.setNextClean(MAX_BLOCK_POS);
    bhead.setPrevClean(MAX_BLOCK_POS);
    bhead.setPrevClean(_mruClean);
    if (_mruClean < _blockList.size()) {
        _blockList[_mruClean].setNextClean(slot);
    } else {
        ASSERT_EXCEPTION(_lruClean == MAX_BLOCK_POS, "corrupt lru list in _addToCleanList");
        _lruClean = slot;
    }
    _mruClean = slot;
}

/* Add slot to end of Dirty LRU list (mru block)
   pre: blockMutex is held
   pre: block is not on a list
   note: same implementation as above with Clean changed to Dirty
   TODO: factor out Clean/Dirty from one common implementation
 */
void BufferMgr::_addToDirtyList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    LOG4CXX_TRACE(logger, "BufferMgr::_addToDirtyList [slot=" << slot << "]");
    ASSERT_EXCEPTION(slot < _blockList.size(), "invalid slot in addToDirty");

    BlockHeader& bhead = _blockList[slot];

    ASSERT_EXCEPTION(bhead.priority, "free slot in addToDirty");

    ASSERT_EXCEPTION(bhead.isDirty(), "_addToDirtyList() on clean block");

    bhead.setNextDirty(MAX_BLOCK_POS);
    bhead.setPrevDirty(MAX_BLOCK_POS);
    bhead.setPrevDirty(_mruDirty);
    if (_mruDirty < _blockList.size()) {
        _blockList[_mruDirty].setNextDirty(slot);
    } else {
        ASSERT_EXCEPTION(_lruDirty == MAX_BLOCK_POS, "corrupt lru list in _addToDirtyList");
        _lruDirty = slot;
    }
    _mruDirty = slot;
}

void BufferMgr::_moveCleanToDirtyList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    BlockHeader& bhead = _blockList[slot];
    SCIDB_ASSERT(bhead.isClean());
    _removeFromCleanList(slot);
    bhead.markDirty();
    _addToDirtyList(slot);
}

void BufferMgr::_moveDirtyToCleanList(BlockPos slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());
    BlockHeader& bhead = _blockList[slot];
    SCIDB_ASSERT(bhead.isDirty());
    _removeFromDirtyList(slot);
    bhead.markClean();
    _addToCleanList(slot);
}

/* Deallocate memory for a buffer, remove it from the indices, and
   mark the slot as free
   pre: blockMutex is held
   note: can drop and re-acquire blockMutex
   returns: true iff mutex was dropped
*/
bool BufferMgr::_removeBufferFromCache(DSBufferMap::iterator dbit)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    bool droppedMutex = false;
    BufferKey bk = dbit->first;
    BlockPos slot = dbit->second;

    LOG4CXX_TRACE(logger, "BufferMgr::_removeBufferFromCache [slot:" << slot << "]"
                  << ", " << _blockList[slot].toString());

    int retries = 0;
    do {
        BlockHeader& bhead = _blockList[slot];
        auto savedGenCount = bhead.genCount;

        // Wait for pending write...
        while (bhead.pending) {
            // TODO: no coverage, pending write?  store followed by clear_cache?
            // clang-format off
            LOG4CXX_TRACE(loggerWait, "BufferMgr::_removeBufferFromCache pwc 3" <<
                          "waiting for pending write, slot: " << slot);
            // clang-format on
            droppedMutex = true;
            LOG4CXX_TRACE(loggerWait, "BufferMgr::_removeBufferFromCache pwc 2 (wait for pending write) start");
            _pendingWriteCond.wait(_blockMutex, PTW_COND_BM_PW_REMOVE);
            LOG4CXX_TRACE(loggerWait, "BufferMgr::_removeBufferFromCache pwc 2 (wait for pending write) complete");
            // NOTE: good performance of pwc 2 requires waiters to not hold for a long time repeatedly
            //       see pwc 1 comment
        }


        bool hasInjected = hasInjectedError(SLOT_GENCOUNT_CHANGE_RBFC, __LINE__, __FILE__);
        if  (bhead.genCount == savedGenCount && !hasInjected) {
            // Did not wait, or if we did, desired buffer is still in this slot.
            SCIDB_ASSERT(!bhead.pending);
            break;
        }

        if(bhead.genCount != savedGenCount) {   // rare path
            LOG4CXX_WARN(logger, "BufferMgr::removeBufferFromCache(): actual slot change detected, slot" << slot);
        } else {
            SCIDB_ASSERT(hasInjected);  // simulated rare path
            LOG4CXX_WARN(logger, "BufferMgr::removeBufferFromCache(): injected/simulated slot change");
        }

        // The slot contents changed (or change simulated) while we were waiting.
        // re-obtain the buffer map iterator.
        dbit = _dsBufferMap.find(bk);
        if (dbit == _dsBufferMap.end()) {
            // Desired BufferKey is no longer in the cache, some other thread did our work for us.
            // TODO: no coverage
            LOG4CXX_WARN(logger, "BufferMgr::_removeBufferFromCache buffer already removed during wait");
            SCIDB_ASSERT(droppedMutex);
            return droppedMutex;
        }

        // Refresh slot, the BufferKey we sought has moved.
        slot = dbit->second;
        LOG4CXX_WARN(logger, "BufferMgr::_removeBufferFromCache(): retrying due to slot change");
    } while (++retries < MAX_PIN_RETRIES); // TODO: not really a pinning retry

    // We tried to wait for the buffer to quiesce MAX_PIN_RETRIES times,
    // but it moved to a different slot each time!
    //
    // "Quiesce" means we're trying to "pin-for-remove": we want the
    // target buffer in a known slot, with _blockMutex held, no pending
    // writes, and zero pinCount.  Though we're not actually pinning,
    // the error text for RetryPinException is accurate so I didn't
    // bother to rename it (LostRaceException?).
    //
    if (retries == MAX_PIN_RETRIES) {
        // TODO: no coverage
        throw SYSTEM_EXCEPTION_SUBCLASS(RetryPinException); // TODO: not really a pinning exception
    }

    BlockHeader& bhead = _blockList[slot];

    ASSERT_EXCEPTION(!bhead.pinCount, "pinned buffer in removeBufferFromCache");

    _dsBufferMap.erase(dbit);

    _releaseBlockHeader(slot);

    return droppedMutex;
}

/* Does the deallocation of the blocks allocated to the slot.
   The key should be erased from _dsBufferMap before calling this
   method (if it was ever entered... in allocation failures, that
   has typically not taken place yet).
   a. change the genCount (because we're modifying the slot)
   b. deallocate blockBase (and adjust bookkeeping)
   c. if present deallocate compressedBlockBase (and adjust bookkeeping)
   NOTE: its possible this should be merged with _moveCleanToFreeList()
*/
void BufferMgr::_freeSlot(const BlockPos& slot)
{
    SCIDB_ASSERT(_blockMutex.isLockedByThisThread());

    BlockHeader& bhead = _blockList[slot];
    ASSERT_EXCEPTION(!bhead.pinCount, "BufferMgr::_freeSlot(), buffer still pinned");

    // A. we are changing the slot, so up the gen count.
    bhead.genCount++;

    // B. deallocate bhead.blockBase
    arena::destroy(*_arena, bhead.blockBase);
    bhead.blockBase = nullptr;
    if (bhead.isDirty()) {
        LOGRELEASE("Dirty", bhead.size, bhead);
        _releaseDirtyBytes(bhead.size);
    }
    LOGRELEASE("SpaceUsed", bhead.size, bhead);
    _releaseSpaceUsed(bhead.size);

    // Destroy the compressed buffers as well  (Don't check
    // compressorType, it may have been set to NONE in unpin.)
    if (bhead.compressedBlockBase != nullptr) {
        arena::destroy(*_arena, bhead.compressedBlockBase);
        bhead.compressedBlockBase = nullptr;
        // When allocateBuffer was called, the compressedSize was unknown, so bhead.size
        // bytes were claimed. Release the same amount that was claimed.
        if (bhead.isDirty()) {
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
        }
        LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
        _releaseSpaceUsed(bhead.size);
    }
}

/* Return the pointer to the raw data. Buffer MUST be pinned.
   Throws ReadOnlyBufferException if buffer is not dirty and
   pinned by multiple callers.
   POST: Buffer is marked dirty.
*/
PointerRange<char> BufferMgr::_getRawData(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getRawData for " << bh);

    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AK);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in getRawData");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getRawData");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getRawData");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getRawData");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getRawData");

    if (bhead.isClean()) {
        if (bhead.pinCount == 1) {
            _moveCleanToDirtyList(bh._slot);

            LOGCLAIM("Dirty", bhead.size, bhead);
            _claimDirtyBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Dirty_compressed", bhead.size, bhead);
                _claimDirtyBytes(bhead.size);
            }
        } else {
            // TODO: no coverage, want an ASSERT_UNREACHED(specific exception)
            assert(false);
            throw SYSTEM_EXCEPTION_SUBCLASS(ReadOnlyBufferException);
        }
    }

    LOG4CXX_TRACE(logger, "BufferMgr::_getRawData: [slot: " << bh._slot << " ]"
                          << "[bhead: " << bhead.toString() << ']');

    SCIDB_ASSERT(bhead.blockBase);
    return PointerRange<char>(bhead.size, bhead.blockBase);
}

/**
 * Return the pointer range to the buffer to be used for compression.
 * The (uncompressed) buffer MUST be pinned and dirty.
 *
 * @note This should only be called in the "write" path, since the compressed buffer is
 * needed until replication and the asynchronous write of the chunk in the buffer is
 * complete.
 */
PointerRange<char> BufferMgr::_getCompressionBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getCompressionBuffer for" << bh);
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AL);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in _getCompressionBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    SCIDB_ASSERT(bhead.isDirty());
    SCIDB_ASSERT(bhead.pinCount);

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getCompressionBuffer");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.isDirty(), "Pristine buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.compressorType != CompressorType::NONE, "Superfluous buffer in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.compressorType != CompressorType::UNKNOWN, "Bad cType in _getCompressionBuffer");
    ASSERT_EXCEPTION(bhead.blockBase, "No raw buffer in _getCompressionBuffer");

    // See BufferMgr::allocateBuffer for how blockbase was allocated.  Create a buffer in which
    // to store the compressed buffer, which has the same size as the original raw data buffer
    // since we don't yet know how great or small the space savings will be.
    LOG4CXX_TRACE(logger, "Compression Type is " << static_cast<uint32_t>(bhead.compressorType));

    // Check if the CompressionBuffer already exists, and only allocate memory for a new one
    // when we need to. The compression buffer is created initially with the same size as the
    // raw data (uncompressed)  buffer.
    if (!bhead.compressedBlockBase) {
        SCIDB_ASSERT(bhead.size == bhead.compressedSize);
        size_t sz = bhead.size;

        try {
            bhead.compressedBlockBase =
                allocBlockBase<InjectErrCode::ALLOC_BLOCK_BASE_FAIL_GCB>(*_arena, sz, __LINE__, __FILE__);
        } catch (std::exception& ex) {
            LOG4CXX_WARN(logger, "BufferMgr::_getCompressionBuffer allocBlockBase failed at line " << __LINE__
                                 << " because " << ex.what());

            // Release bytes that were allocated for the compressionBuffer in allocateBuffer()
            LOGRELEASE("Dirty_compressed", bhead.size, bhead);
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            LOGRELEASE("SpaceUsed_compressed", bhead.size, bhead);
            _releaseDirtyBytes(bhead.size);
            _releasePinnedBytes(bhead.size);
            _releaseSpaceUsed(bhead.size);

            throw SYSTEM_EXCEPTION_SUBCLASS(OutOfMemoryException);
        }
        SCIDB_ASSERT(bhead.compressedBlockBase);
    }

    // This will be returned to NewDbArray::unpinChunk() so that the compressed
    // information can be put into this buffer.  Note that once the compression happens
    // the compressedSize -will- change (<= otherwise we have yet another issue), and so
    // must be updated in the chunk descriptor metadata value.

    return PointerRange<char>(bhead.compressedSize, bhead.compressedBlockBase);
}

void BufferMgr::_setCompressedSize(BufferMgr::BufferHandle& bh, size_t sz)
{
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AM);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in _setCompressionBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _setCompressedSize");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _setCompressedSize");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _setCompressedSize");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _setCompressedSize");

    bhead.compressedSize = sz;
}

/* Return the pointer to the const raw data.  Buffer MUST be pinned.
 */
PointerRange<const char> BufferMgr::_getRawConstData(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_getRawConstData for " << bh);
    ScopedMutexLock sm(_blockMutex, PTW_SML_STOR_AN);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in getRawConstData");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _getRawConstData");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _getRawConstData");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _getRawConstData");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _getRawConstData");

    LOG4CXX_TRACE(logger, "BufferMgr::_getRawConstData: [slot: " << bh._slot << " ]"
                          << "[bhead: " << bhead.toString() << ']');

    SCIDB_ASSERT(bhead.blockBase);
    return PointerRange<const char>(bhead.size, bhead.blockBase);
}

/* Pin buffer again.
   Initial pin happens on allocation.
 */
void BufferMgr::_pinBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_pinBuffer for " << bh);

    ScopedMutexLock sm(_blockMutex, PTW_SML_BM__PIN);

    /* Verify that the slot in the BufferHandle is still valid
       and matches the slot in the DSBufferMap.
     */
    BlockPos dsbmSlot = BufferHandle::INVALID_SLOT;
    DSBufferMap::iterator dbit = _dsBufferMap.find(bh.getKey());
    if (dbit != _dsBufferMap.end()) {
        dsbmSlot = dbit->second;
    }

    if (bh._slot < _blockList.size() && bh._slot == dsbmSlot) {
        BlockHeader& bhead = _blockList[bh._slot];

        if (bhead.genCount != BufferHandle::NULL_GENCOUNT && bhead.genCount == bh._gencount) {
            SCIDB_ASSERT(bh.getCompressorType() == bhead.compressorType);
            SCIDB_ASSERT(bhead.compressorType != CompressorType::UNKNOWN);
            SCIDB_ASSERT(bhead.blockBase);
            SCIDB_ASSERT(bhead.priority);
            while (bhead.pinCount == 0 && bhead.pending) {
                // TODO: no coverage, unpinned pending write?
                // clang-format off
                LOG4CXX_TRACE(loggerWait, "BufferMgr::_pinBuffer pwc 3 (wait for pending write) start");
                // clang-format on
                _pendingWriteCond.wait(_blockMutex, PTW_COND_BM_PW__PIN);
                LOG4CXX_TRACE(loggerWait, "BufferMgr::_pinBuffer pwc 3 (wait for for pending write) complete");
                // NOTE: good performance of pwc 3 requires waiters to not hold for a long time repeatedly
                //       see pwc 1 comment
            }

            // Waiting on _pendingWriteCond gives up the mutex, so
            // the handle may have become stale.
            if (bhead.genCount == bh._gencount) {
                // Nope, handle still good.  Pin it!
                bhead.pinCount++;
                if (bhead.pinCount == 1) {
                    LOGCLAIM("Pinned", bhead.size, bhead);
                    _claimPinnedBytes(bhead.size);
                    if (bhead.compressedBlockBase) {
                        LOGCLAIM("Pinned_compressed", bhead.size, bhead);
                        _claimPinnedBytes(bhead.size);
                    }
                }
                return;
            }

            // TODO: no coverage
            LOG4CXX_DEBUG(logger, "BufferMgr::_pinBuffer handle went stale waiting on pending write");
        }

        // Still here?  Handle was stale.
        LOG4CXX_TRACE(logger,
                      "Stale Handle. Re-pin the buffer: "
                      << "{ \"bh\": "<< bh
                      << ", \"bhead\": " << bhead.toString()
                      << "}");
    }

    /* Stale handle, re-pin the buffer
     */
    CompressorType pinCType = bh.getCompressorType();
    SCIDB_ASSERT(pinCType != CompressorType::UNKNOWN);
    bh = _pinBufferLockedWithRetry(bh._key, pinCType);
}

/* Unpin the buffer.  Cannot use raw pointer after this unless pinBuffer()
   is called.
 */
void BufferMgr::_unpinBuffer(BufferMgr::BufferHandle& bh)
{
    LOG4CXX_TRACE(logger, "BufferMgr::_unpinBuffer for " << bh);

    ScopedMutexLock sm(_blockMutex, PTW_SML_BM__UNPIN);

    /* Verify that the slot is still valid and the buffer is pinned
     */
    ASSERT_EXCEPTION(bh._slot < _blockList.size(), "Invalid buffer handle in unpinBuffer");

    BlockHeader& bhead = _blockList[bh._slot];

    ASSERT_EXCEPTION(bhead.priority, "Buffer handle slot on free list in _unpinBuffer");
    ASSERT_EXCEPTION(bhead.genCount == bh._gencount, "Stale buffer handle in _unpinBuffer");
    ASSERT_EXCEPTION(bh._gencount, "Bad buffer handle generation count in _unpinBuffer");
    ASSERT_EXCEPTION(bhead.pinCount, "Unpinned buffer in _unpinBuffer");

    bhead.pinCount--;

    if (bhead.compressorType != bh.getCompressorType()) {
        // The blockheader had the "desired compression type" for the chunk when it was
        // initialized, but it is possible that compression failed. When compression
        // fails, the bufferhandle is updated, so the blockheader needs to be updated
        // accordingly. The value in the blockheader is used when writing to the datastore
        // to determine whether to write the compressed or uncompressed buffer of the chunk.
        // (see DBArray::unpinChunk.)
        // @see SDB-5964 for a better proposal to handle this.
        LOG4CXX_TRACE(logger, "BufferMgr::_unpinBuffer updating blockheader's compressorType"
                              << " from " << static_cast<int16_t>(bhead.compressorType)
                              << " to " << static_cast<int16_t>(bh.getCompressorType())
                              << "{bhead: " << bhead.toString()
                              << "}, "
                              << "{bhand: " << bh << "}");

        bhead.compressorType = bh.getCompressorType();
    }

    if (bhead.pinCount == 0) {
        /* Pin count going to zero.  Update pinned bytes, schedule
           an i/o if required
         */
        LOGRELEASE("Pinned", bhead.size, bhead);
        _releasePinnedBytes(bhead.size);
        if (bhead.compressedBlockBase) {
            LOGRELEASE("Pinned_compressed", bhead.size, bhead);
            _releasePinnedBytes(bhead.size);
        }

        // TODO: WritePriority::Immediate might be unused and this code might be unreachable
        if (bhead.isDirty() && bhead.wp == WritePriority::Immediate && !bhead.pending) {
            // TODO: no coverage, WritePriority::Immediate
            // clang-format off
                LOG4CXX_TRACE(logger, "bufferMgr: unpin buffer, " <<
                              "pincount 0 on Immediate priority buffer," <<
                              "issuing write job" <<
                              "[slot=" << bh._slot << "]");
            // clang-format on

            bhead.pending = true;
            LOGCLAIM("Pending", bhead.size, bhead);
            _claimPendingBytes(bhead.size);
            if (bhead.compressedBlockBase) {
                LOGCLAIM("Pending_compressed", bhead.size, bhead);
                _claimPendingBytes(bhead.size);
            }
            SCIDB_ASSERT(bhead.blockBase);
            std::shared_ptr<IoJob> job =
                make_shared<IoJob>(IoJob::IoType::Write, bh._slot, *this);
            _jobQueue->pushJob(job);
        }

        /* If the buffer was not dirty (we are unpinning clean data),
           then try to signal reserve waiters, they can use this space
         */
        if (!bhead.isDirty()) {
            size_t reserveRelease = bhead.size;
            if (bhead.compressedBlockBase) {
                reserveRelease += bhead.size;
            }
            _signalReserveWaiters(reserveRelease);
        }
    }
}

}  // namespace scidb
