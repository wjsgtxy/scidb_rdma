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
 * @file DBArray.cpp
 * @brief Next-gen persistent array implementation
 */

#include <array/DBArray.h>
#include <array/SyntheticArray.h>

#include <log4cxx/logger.h>
#include <array/ArrayDistribution.h>
#include <array/DelegateArray.h>
#include <array/ReplicationMgr.h>
#include <storage/StorageMgr.h>
#include <system/Exceptions.h>
#include <util/compression/Compressor.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>
#include <util/OnScopeExit.h>

#include <set>
#include <vector>

namespace scidb {
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.DBArray"));

/*
 * DBArray
 */

DBArray::DBArray(ArrayDesc const& desc, const std::shared_ptr<Query>& query)
    : _desc(desc)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::DBArray arrayID=" << _desc.getId()
                          << ", UAID=" << _desc.getUAId()
                          << ", ps=" << _desc.getDistribution()->getDistType()
                          << ", desc=" << desc);
    // clang-format on
    _query = query;
    DataStore::DataStoreKey dsk = DBArrayMgr::getInstance()->getDsk(_desc);
    DbAddressMeta addressMeta;
    DBIndexMgr::getInstance()->getIndex(_diskIndex, dsk, addressMeta);
    SCIDB_ASSERT(_diskIndex);
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(not isUninitialized(_desc.getDistribution()->getDistType()));
    SCIDB_ASSERT(not isUndefined(_desc.getDistribution()->getDistType()));
}

void DBArray::removeDeadChunks(std::shared_ptr<Query>& query,
                                  std::set<Coordinates, CoordinatesLess> const& liveChunks)
{
    // We may not need to replicate, but if we do, the machinery has
    // to be set up outside the scope of the _mutex lock.
    ReplicationManager* repMgr = ReplicationManager::getInstance();
    std::vector<std::shared_ptr<ReplicationManager::Item>> replicasVec;
    OnScopeExit replicasCleaner([&replicasVec, repMgr] () {
            repMgr->abortReplicas(replicasVec);
        });

    { // Begin _mutex scope!
        ScopedMutexLock cs(_mutex, PTW_SML_STOR_M);

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::removeDeadChunks arrayID=" << _desc.getId());
    // clang-format on

    /* Gather a list of the "dead" chunks... those that need tombstone
       entries...
     */
    typedef std::set<Coordinates, CoordinatesLess> DeadChunks;
    DeadChunks deadChunks;
    const auto& attr = getArrayDesc().getAttributes().firstDataAttribute();
    std::shared_ptr<ConstArrayIterator> arrIter = getConstIterator(attr);

    while (!arrIter->end()) {
        Coordinates currentCoords = arrIter->getPosition();

        // clang-format off
        LOG4CXX_TRACE(logger, "DBArray::removeDeadChunks current chunk: "
                              << CoordsToStr(currentCoords));
        // clang-format on

        if (liveChunks.count(currentCoords) == 0) {
            deadChunks.insert(currentCoords);
        }
        ++(*arrIter);
    }

    /* For each address in the deadChunks set, enter a NULL buf
       handle (tombstone) in the index at the current version, for
       all attributes.
     */
    replicasVec.reserve(_desc.getDistribution()->getRedundancy() * deadChunks.size());
    for (Coordinates const& coords : deadChunks) {
        // Start replication for the tombstone to other instances.
        PersistentAddress addr(_desc.getId(), 0, coords);
        repMgr->replicate(_desc, addr, NULL, NULL, 0, 0, query, replicasVec);

        // Store the tombstone locally.
        removeLocalChunkLocked(query, coords);
    }

    } // End _mutex scope!

    // Wait for replicas to be sent (if there were any).  Replicate
    // without _mutex, to avoid inter-instance deadlock (SDB-5866).
    if (!replicasVec.empty()) {
        repMgr->waitForReplicas(replicasVec);
        replicasCleaner.cancel();
    }
}

void DBArray::removeLocalChunk(std::shared_ptr<Query> const& query, Coordinates const& coords)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_N);
    removeLocalChunkLocked(query, coords);
}

void DBArray::removeLocalChunkLocked(std::shared_ptr<Query> const& query,
                                     Coordinates const& coords)
{
    Query::validateQueryPtr(query);

    ChunkAuxMeta chunkAuxMeta;
    chunkAuxMeta.primaryInstanceId = ReplicationManager::getInstance()->getPrimaryInstanceId(_desc, coords);

    DBDiskIndex::DiskIndexValue value;

    for (const auto& attr : _desc.getAttributes()) {
        DbAddressMeta dbam;
        DbAddressMeta::KeyWithSpace key;

        key.initializeKey(dbam, coords.size());
        dbam.fillKey(key.getKey(), _diskIndex->getDsk(), attr, coords, _desc.getId());

        _diskIndex->insertRecord(key.getKey(),
                                 value,  // NULL buffer handle
                                 chunkAuxMeta,
                                 false,  // unpin the buffer after insert
                                 true);  // replace any existing value
    }
}

DBArray::~DBArray()
{
    LOG4CXX_TRACE(logger, "DBArray::~DBArray dtor, arrayID="
                  << _desc.getId());
}

void DBArray::removeEarlierVersions(std::shared_ptr<Query>& query, const ArrayID lastLiveArrId)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    SCIDB_ASSERT(lastLiveArrId > 0);

    PersistentAddress currentChunkAddr;
    DbAddressMeta dbam;
    bool currentChunkIsLive = true;

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::removeVersions arrayID=" << _desc.getId()
                          << " lastliveID=" << lastLiveArrId);
    // clang-format on

    DBArray::DBDiskIndex::Iterator current = _diskIndex->begin();

    while (!current.isEnd()) {
        PersistentAddress targetAddress;
        auto* ebmAttr = _desc.getAttributes().getEmptyBitmapAttribute();
        dbam.keyToAddress(&(current.getKey()), targetAddress, ebmAttr);

        if (!targetAddress.sameBaseAddr(currentChunkAddr)) {
            /* Move on to next coordinate
             */
            currentChunkAddr = targetAddress;
            currentChunkIsLive = true;
        }
        if (targetAddress.arrVerId > lastLiveArrId) {
            /* Chunk was added after oldest version
               so it is still live
            */
            ++current;
            continue;
        } else if (targetAddress.arrVerId == lastLiveArrId) {
            /* Chunk was added in oldest version so it is
               still live, but any older chunks are not
            */
            currentChunkIsLive = false;
            ++current;
            continue;
        } else if (targetAddress.arrVerId < lastLiveArrId) {
            /* Chunk was added prior to oldest version
             */
            if (currentChunkIsLive) {
                /* Chunk is still live, but older chunks are not
                 */
                currentChunkIsLive = false; // coverage
                ++current;
                continue;
            }
        }

        /* Chunk should be removed
         */
        // clang-format off
        LOG4CXX_TRACE(logger, "DBArray::removeVersions "
                              << "found chunk to remove: " << targetAddress.toString());
        // clang-format on
        onRemoveChunk(current.getKey());
        _diskIndex->deleteRecord(&(current.getKey()));
        ++current;
    }
}

void DBArray::removeAllVersions(std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    // clang-format off
    LOG4CXX_TRACE(logger, "Removing all chunks for array with UAID " << _desc.getUAId());
    // clang-format on

    // Remove all index map entries for the chunks and
    // remove the backing storage for the array.
    DBIndexMgr::getInstance()->closeIndex(_diskIndex, true /* remove from disk */);
}

void DBArray::removeVersions(std::shared_ptr<Query>& query, ArrayID lastLiveArrId)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_O);

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::removeVersions arrayID=" << _desc.getId()
                          << " lastliveID=" << lastLiveArrId);
    // clang-format on

    if (lastLiveArrId == 0) {
        removeAllVersions(query);
    }
    else {
        removeEarlierVersions(query, lastLiveArrId);
    }
}

void DBArray::flush()
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_P);
    _diskIndex->flushVersion(_desc.getId());
}

void DBArray::pinChunk(CachedDBChunk const& chunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::pinChunk chunk=" << reinterpret_cast<void const*>(&chunk)
                          << ", accessCount is " << chunk._accessCount
                          << ", Array=" << reinterpret_cast<void*>(this)
                          << ", name " << chunk.arrayDesc->getName()
                          << ", Addr=" << chunk.addr.toString()
                          << ", Size=" << chunk.size
                          << ", Key=" << _diskIndex->getKeyMeta().keyToString(chunk._key.getKey()));
    // clang-format on

    Query::getValidQueryPtr(_query);

    ScopedMutexLock cs(_mutex, PTW_SML_STOR_Q);
    if (chunk._accessCount++ == 0) {
        // Attempt to use the compressor type given in the bufferhandle in the chunk _index,
        // but use the compressor type specified in the chunk if the bufferhandle
        // doesn't have one set yet (and set the bufferhandle.compressorType to that value).
        //
        // @see SDB-5964 for better function call API recommendations.
        auto& bufHandle = chunk._indexValue.getBufferHandle();
        CompressorType cType = bufHandle.getCompressorType();
        if (cType == CompressorType::UNKNOWN) {
            // The buffer handle's compressionType may be UNKNOWN here if it's a new
            // handle, but the chunk is certain to have a good cType.  If we're about to
            // write a new, empty chunk, it will have the desired cType from the schema.
            // If we've read this chunk from disk (or from a peer instance) it will have
            // its actual cType.  Either way we can now update the buffer handle.
            SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
            bufHandle.setCompressorType(chunk.compressionMethod);
        }

        /* Retrieve the most recent version of the chunk for this chunk address
         * and pin the chunk.
         */
        DbAddressMeta::Key* key = chunk._key.getKey();
        DBArray::DBDiskIndex::Iterator iter = _diskIndex->leastUpper(key);
        if (coordinatesCompare(iter.getKey()._coords, key->_coords) != 0) {
            throw SYSTEM_EXCEPTION_SUBCLASS(DiskIndex<DbAddressMeta>::KeyNotExistException);
        }
        _diskIndex->pinValue(iter, chunk._indexValue);  // throws if iter is at end or guard

        chunk._chunkAuxMeta = iter.getAuxMeta();
        chunk.size = chunk._indexValue.constMemory().size();
        chunk.markClean();
    } else {
        assert(chunk.isPresent() || chunk.size == 0);
    }
}

void DBArray::unpinChunk(CachedDBChunk const& chunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::unpinChunk chunk=" << (void*)&chunk
                  << ", accessCount is " << chunk._accessCount
                  << ", Array=" << (void*)this
                  << ", name " << chunk.arrayDesc->getName()
                  << ", Addr=" << chunk.addr.toString()
                  << ", Size=" << chunk.size
                  << ", Key=" << _diskIndex->getKeyMeta().keyToString(chunk._key.getKey()));
    // clang-format on

    // We may not need to replicate, but if we do, the machinery has
    // to be set up outside the scope of the _mutex lock.
    ReplicationManager* repMgr = ReplicationManager::getInstance();
    ReplicationManager::ItemVector replicasVec;
    OnScopeExit replicasCleaner([&replicasVec, repMgr] () {
            repMgr->abortReplicas(replicasVec);
        });

    { // Begin _mutex scope!
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_R);
    assert(chunk._accessCount > 0);
    // chunk.getConstData()==NULL --> chunk.size==0
    assert(chunk.getConstData() != NULL || chunk.size == 0);

    if (--chunk._accessCount == 0) {

        /* We may be here during the abort of a transaction.  If so,
           then we should not try to write the buffer, it will get
           rolledback anyway. Just get out.
         */
        std::shared_ptr<Query> queryPtr;
        try {
            queryPtr = Query::getValidQueryPtr(_query);
        } catch (const std::exception& e) {
            /* If the value is caller pinned, the chunk destructor will
               clean it up.  But if its index pinned, we need to unpin
               the buffer here.
             */
            if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::IndexPinned) {
                chunk._indexValue.unpin();
            }
            return;
        }
        // The query is valid so this function was not called during an abort transaction.

        size_t compressedSize = chunk.size;

        if (chunk.isDirty()) {
            auto& bufHandle = chunk._indexValue.getBufferHandle();
            SCIDB_ASSERT(bufHandle.getCompressorType() == chunk.compressionMethod);
            SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
            if (CompressorType::NONE != bufHandle.getCompressorType()) {
                // Compression MUST happen before the chunk is replicated.
                compressedSize = CompressorFactory::getInstance()
                    .getCompressor(chunk.getCompressionMethod())
                    ->compress(chunk.getCompressionBuffer(),  // destination
                               chunk,        // source chunk
                               chunk.size);  // uncompressed size
                ASSERT_EXCEPTION(chunk.size >= compressedSize,
                                 "Failed to compress chunk. Compression resulted in size increase.");
                if (compressedSize == chunk.size) {
                    // The compressors return a size equal to the "uncompressed" size in the case of failure.
                    // Use no compressor for this chunk.
                    chunk.compressionMethod = CompressorType::NONE;
                }
                // Update the _indexValue.BufferHandle metadata in the Chunk with the
                // actual compressed size (and the compressionMethod if compression failed).
                //
                // TODO :
                // @see SDB-5964 for better function call API recommendations.
                const_cast<CachedDBChunk&>(chunk).setCompressedSizeAndType(compressedSize,
                                                                           chunk.compressionMethod);
                // After setting the metadata, the CompressorType in the chunk and the
                // BufferHandle should be known and equal.
                SCIDB_ASSERT(chunk.compressionMethod != CompressorType::UNKNOWN);
                SCIDB_ASSERT(chunk.compressionMethod == bufHandle.getCompressorType());

                auto chunkAuxMeta = chunk.getChunkAuxMeta();
                // Update/insert the diskIndex to record the correct size of the buffer that
                // will be added to the DataStore.
                _diskIndex->insertRecord(chunk._key.getKey(),
                                         chunk._indexValue,
                                         chunkAuxMeta,
                                         true /* keep this chunk pinned */,
                                         true /* update record if exists*/);
            }
        }

        /* If the value is dirty, replicate the data to other instances
         */
        if (chunk.isDirty()) {
            void const * data = nullptr;
            // TODO The assumption is that if the compressed size and uncompressed size
            //      are the same, then go ahead and use the "uncompressed" buffer.  The
            //      code base doesn't have any real consistency in the case where
            //      compression does not result in a size reduction.
            if (compressedSize < chunk.size &&
                CompressorType::NONE != chunk.compressionMethod){
                data = chunk.getCompressionBuffer();
            } else {
                data = chunk.getConstData();
            }

            LOG4CXX_TRACE(logger, "Attempting to replicate..."
                                  << " _desc = " << _desc.getName()
                                  << " chunk.size = " << chunk.size
                                  << " compressedSize = " << compressedSize);

            try {
                repMgr->replicate(_desc,
                                  chunk._storageAddr,
                                  &chunk,
                                  data,
                                  compressedSize,
                                  chunk.size,
                                  queryPtr,
                                  replicasVec);
                LOG4CXX_TRACE(logger, "Replication successful");
            }
            catch (const scidb::SystemException& e) {
                if (e.getShortErrorCode() == SCIDB_SE_INJECTED_ERROR &&
                    e.getLongErrorCode() == SCIDB_LE_INJECTED_ERROR) {
                    cleanupChunkRecord(chunk);
                    e.raise();
                }

                auto what = e.what();
                LOG4CXX_WARN(logger, "Replication failed on exception " << what);
            }
            catch (const std::exception& e) {
                auto what = e.what();
                LOG4CXX_WARN(logger, "Replication failed on exception " << what);
            }
            catch (...) {
                LOG4CXX_WARN(logger, "Replication failed on unknown exception");
            }
        }

        cleanupChunkRecord(chunk);
    }

    } // End _mutex scope!

    // Wait for replicas to be sent (if there were any).  Replicate
    // without _mutex, to avoid inter-instance deadlock (SDB-5866).
    if (!replicasVec.empty()) {
        repMgr->waitForReplicas(replicasVec);
        replicasCleaner.cancel();
    }
}

void DBArray::cleanupChunkRecord(CachedDBChunk const& chunk)
{
    /* If the value is owned by the caller, we need to insert/update
       the value
    */
    DbAddressMeta::Key* key = chunk._key.getKey();
    if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::CallerPinned) {
        // clang-format off
        LOG4CXX_TRACE(logger, "DBArray::unpinChunk "
                      << "insert record into index");

        // clang-format on

        // should not allocate mem if not dirty
        SCIDB_ASSERT(chunk.isDirty());

        auto chunkAuxMeta = chunk.getChunkAuxMeta();

        /* upsert into disk index and unpin dirty chunk */
        bool ok = _diskIndex->insertRecord(key,
                                           chunk._indexValue,
                                           chunkAuxMeta,
                                           /*keepPinned:*/ false,
                                           /*update:*/ true);
        SCIDB_ASSERT(ok);
    } else if (chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::IndexPinned) {
        // The value is already in the index and we simply need to
        // unpin the value...
        LOG4CXX_TRACE(logger, "DBArray::unpinChunk unpin index value");
        chunk._indexValue.unpin();
    } else {
        // e.g. when allocation fails, throwing bypassing pinning actions
        // and unpinners detect but assert to identify pin/unpin imbalances
        SCIDB_ASSERT(chunk._indexValue.state() == DBDiskIndex::DiskIndexValue::Unpinned);
        LOG4CXX_WARN(logger, "DBArray::unpinChunk chunk already UnPinned, (chunk allocation failed?)");
    }
}

ArrayDesc const& DBArray::getArrayDesc() const
{
    return _desc;
}

void DBArray::makeChunk(PersistentAddress const& addr,
                        CachedDBChunk*& chunk,
                        CachedDBChunk*& bitmapchunk,
                        bool newChunk)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    CachedDBChunk::createChunk(_desc, _diskIndex, addr, chunk, bitmapchunk, newChunk, this);
}

std::shared_ptr<ArrayIterator> DBArray::getIteratorImpl(const AttributeDesc& attId)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::getIterator Getting arrayID=" << _desc.getId()
                          << ", attrID=" << attId.getId());
    // clang-format on
    std::shared_ptr<DBArray> owner;
    owner = std::dynamic_pointer_cast<DBArray>(shared_from_this());
    return std::shared_ptr<ArrayIterator>(new DBArrayIterator(owner, attId.getId()));
}

std::shared_ptr<ConstArrayIterator> DBArray::getConstIteratorImpl(const AttributeDesc& attId) const
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArray::getConstIterator "
                          << "Getting const DB iterator for arrayID=" << _desc.getId()
                          << ", attrID=" << attId.getId());
    // clang-format on
    std::shared_ptr<ConstArrayIterator> iter;
    iter = (const_cast<DBArray*>(this))->getIterator(attId);
    if (attId.isEmptyIndicator()) {
        // Never synthesize values for the empty bitmap attribute.
        return iter;
    }

    // We may need to substitute a SyntheticArrayIterator to produce null or default values if attId
    // was newly added by the add_attributes() operator.  We know to do this IFF the ordinary
    // DBArrayIterator (currently in 'iter') is at end(), because:
    //
    // 1. Correct use of the Array API dictates that an ArrayIterator::end() check must be done
    //    before any call to ...::getChunk(); AND
    //
    // 2. That end() check will likely already have be done on some non-synthesized attribute, in
    //    which case end() here means "nothing for attId in the _diskIndex", so attId *must* be
    //    newly added and so should be synthesized; OR
    //
    // 3. Someone is about to do an end() check on *this* newly added attribute.  But
    //    SyntheticArrayIterator::end() can handle that: it will do the _diskIndex lookup for the
    //    empty bitmap attribute and return the correct end() value.
    //
    // So if iter->end(), we're justified in returning a SyntheticArrayIterator here in all cases.
    // If we're really at end-of-data, it'll reflect that; otherwise it'll produce null or default
    // values for the newly added attribute as needed.

    if (iter->end()) {
        auto owner = std::dynamic_pointer_cast<DBArray>(const_cast<DBArray*>(this)->shared_from_this());
        std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        iter = std::make_shared<SyntheticArrayIterator>(owner, attId, query);
    }

    return iter;
}

void DBArray::rollbackVersion(VersionID lastVersion, ArrayID baseArrayId, ArrayID newArrayId)
{
    typedef DiskIndex<DbAddressMeta> DBDiskIndex;
    typedef IndexMgr<DbAddressMeta> DBIndexMgr;

    LOG4CXX_TRACE(logger, "DBArray::rollbackVersion()"
                  << " lastVer=" << lastVersion
                  << " uaid=" << baseArrayId
                  << " vaid=" << newArrayId << ")");

    /* Find the disk index associated with the baseArrayId, then
       initiate rollback.
     */
    DBArrayMgr* dbMgr = DBArrayMgr::getInstance();
    std::shared_ptr<DBDiskIndex> diskIndex;
    DataStore::DataStoreKey dsk = dbMgr->getDsk(baseArrayId);
    DbAddressMeta addressMeta;

    DBIndexMgr::getInstance()->getIndex(diskIndex, dsk, addressMeta);
    diskIndex->rollbackVersion(newArrayId, lastVersion);
}

/* DBArrayIterator
 */

DBArrayIterator::DBArrayIterator(std::shared_ptr<DBArray> arr, AttributeID attId)
    : ArrayIterator(*arr)
    , _array(arr)
    , _currChunk(NULL)
    , _currBitmapChunk(NULL)
{
    _addr.arrVerId = _array->_desc.getId();
    _addr.attId = attId;
    _addr.coords.insert(_addr.coords.begin(), _array->_desc.getDimensions().size(), 0);
    resetAddrToMin();
    _positioned = false;
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::DBArrayIterator()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
}

DBArrayIterator::~DBArrayIterator()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::~DBArrayIterator()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    if (_currChunk) {
        _currChunk->deleteOnLastUnregister();
    }
    if (_currBitmapChunk) {
        _currBitmapChunk->deleteOnLastUnregister();
    }
}

void DBArrayIterator::resetAddrToMin()
{
    for (size_t i = 0; i < _array->_desc.getDimensions().size(); ++i) {
        _addr.coords[i] = _array->_desc.getDimensions()[i].getStartMin();
    }
    _addr.arrVerId = _array->_desc.getId();
}

void DBArrayIterator::resetChunkRefs()
{
    _positioned = false;
    if (_currChunk) {
        _currChunk->deleteOnLastUnregister();
    }
    if (_currBitmapChunk) {
        _currBitmapChunk->deleteOnLastUnregister();
    }
    _currChunk = NULL;
    _currBitmapChunk = NULL;
}

ConstChunk const& DBArrayIterator::getChunk()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::getChunk()"
                          << " This=" << (void*)this
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    position();
    if (!_currChunk || hasInjectedError(CANNOT_GET_CHUNK, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return *_currChunk;
}

bool DBArrayIterator::end()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::end()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    position();

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::end() _currChunk = " << (void*)_currChunk);
    // clang-format on

    return _currChunk == NULL;
}

bool DBArrayIterator::advanceIters(PersistentAddress& nextAddr,
                                   PersistentAddress const& oldAddr)
{
    ++_curr;
    if (_array->_desc.getEmptyBitmapAttribute()) {
        ++_currBitmap;
    }
    if (!_curr.isEnd()) {
        auto ebmAttr = _array->_desc.getAttributes().getEmptyBitmapAttribute();
        _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(),
                                                      nextAddr,
                                                      ebmAttr);
    }
    if (_curr.isEnd() || nextAddr.attId != oldAddr.attId) {
        /* We have reached the end of the iteration (off the end of
           the list or reached the next attribute)
         */
        return true;
    } else {
        /* Not at the end of the iteration
         */
        return false;
    }
}

void DBArrayIterator::findNextLogicalChunk(PersistentAddress& currAddr_io,  // io: 'in out'
                                           const ArrayID& targetVersion,
                                           std::shared_ptr<Query> query)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "findNextLogicalChunk after addr=" << currAddr_io.toString());
    // clang-format on

    PersistentAddress nextAddr;
    ReplicationManager* repMgr = ReplicationManager::getInstance();

    bool done = false;
    do {
        for( done=advanceIters(nextAddr, currAddr_io);  // modifies nextAddr, _curr
             !done && (nextAddr.coords == currAddr_io.coords ||     // same coordinate
                       nextAddr.arrVerId > targetVersion);          // wrong version
             done=advanceIters(nextAddr, currAddr_io)); // modifies nextAddr, _curr
        if (done) { break; }

        // _curr & nextAddr good via advanceIters()
        currAddr_io = nextAddr;    // make currAddr match

    } while (_curr.getBufHandle().isNull() ||
             not repMgr->isResponsibleFor(_array->_desc, currAddr_io, _curr.getAuxMeta().primaryInstanceId,
                                          query));

    setCurrent();
}

void DBArrayIterator::operator++()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::operator++()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex, PTW_SML_DB_ARRAY_ITER);
    std::shared_ptr<Query> query = getQuery();
    position();

    findNextLogicalChunk(_addr, _array->_desc.getId(), query);
}

Coordinates const& DBArrayIterator::getPosition()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::getPosition()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    position();
    if (!_currChunk ||
        hasInjectedError(NO_CURRENT_CHUNK, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    return _currChunk->getFirstPosition(false);
}

bool DBArrayIterator::setPosition(Coordinates const& pos)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::setPosition()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned
                          << " pos=" << pos);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex, PTW_SML_STOR_S);
    std::shared_ptr<Query> query = getQuery();
    DbAddressMeta::KeyWithSpace key;
    key.initializeKey(_array->_diskIndex->getKeyMeta(), pos.size());

    /* Search the disk index for the requested position.
     */
    PersistentAddress target;

    target.coords = pos;
    _array->_desc.getChunkPositionFor(target.coords);
    const auto& attrKey = _array->_desc.getAttributes().findattr(_addr.attId);
    target.attId = _addr.attId;
    target.arrVerId = _array->_desc.getId();

    _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                             _array->_diskIndex->getDsk(),
                                             attrKey,
                                             target.coords,
                                             target.arrVerId);
    _curr = _array->_diskIndex->leastUpper(key.getKey());

    if (_array->_desc.getEmptyBitmapAttribute()) {
        auto ebmAttrId = _array->_desc.getEmptyBitmapAttribute();

        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 *ebmAttrId,
                                                 target.coords,
                                                 target.arrVerId);
        _currBitmap = _array->_diskIndex->leastUpper(key.getKey());
    }

    /* If the requested position exists, set the current chunk refs.
       If not, mark this iterator as "not positioned"
     */
    PersistentAddress nextAddr;
    ReplicationManager* repMgr = ReplicationManager::getInstance();
    bool sameCoords = true;
    InstanceID currIid = _curr.getAuxMeta().primaryInstanceId;

    auto ebmAttr = _array->_desc.getAttributes().getEmptyBitmapAttribute();
    _array->_diskIndex->getKeyMeta().keyToAddress(&(_curr.getKey()), nextAddr, ebmAttr);
    sameCoords = nextAddr.coords.size()
        ? (coordinatesCompare(nextAddr.coords, target.coords) == 0)
        : false;

    /* Position "exists" iff it is not a tombstone, not a replica chunk,
       and has the correct coordinates.
     */
    if (!_curr.getBufHandle().isNull() && sameCoords &&
        repMgr->isResponsibleFor(_array->_desc, nextAddr, currIid, query)) {
        setCurrent();
    } else {
        resetChunkRefs();
    }

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::setPosition() returning=" << (_currChunk != NULL));
    // clang-format on

    return _currChunk != NULL;
}

void DBArrayIterator::setCurrent()
{
    SCIDB_ASSERT(_array->_mutex.isLockedByThisThread());

    resetChunkRefs();
    _positioned = true;

    /* If we are not off the end, update the chunk refs.
     */
    if (!_curr.isEnd()) {
        auto ebmAttr = _array->_desc.getAttributes().getEmptyBitmapAttribute();
        auto currAttId = _curr.getKey().decodeAttributeID(ebmAttr);
        if (currAttId == _addr.attId) {
            _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(), _addr, ebmAttr);
            _array->makeChunk(_addr, _currChunk, _currBitmapChunk, false);
            if (_currChunk) {
                _currChunk->size = _curr.valueSize();
                _currChunk->_chunkAuxMeta = _curr.getAuxMeta();
            }
            if (_currBitmapChunk) {
                _currBitmapChunk->size = _currBitmap.valueSize();
                _currBitmapChunk->_chunkAuxMeta = _currBitmap.getAuxMeta();
            }
            // clang-format off
            LOG4CXX_TRACE(logger, "DBArrayIterator::setCurrent()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned
                          << " CurrChunk=" << _currChunk
                          << " CurrBitmapChunk=" << _currBitmapChunk);
            // clang-format on
        }
        else {
            // clang-format off
            LOG4CXX_TRACE(logger, "DBArrayIterator::setCurrent"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned
                          << " Requested position doesn't exist");
            // clang-format on
        }
    } else {
        // clang-format off
        LOG4CXX_TRACE(logger, "DBArrayIterator::setCurrent"
                              << " Array=" << (void*)_array.get()
                              << " Addr=" << _addr.toString()
                              << " Positioned=" << _positioned
                              << " Requested position doesn't exist");
        // clang-format on
    }
}

void DBArrayIterator::restart()
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::restart()"
                          << " This=" << (void*)this
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    ScopedMutexLock cs(_array->_mutex, PTW_SML_STOR_T);

    DbAddressMeta::KeyWithSpace key;
    std::shared_ptr<Query> query = getQuery();

    resetAddrToMin();
    key.initializeKey(_array->_diskIndex->getKeyMeta(), _addr.coords.size());
    const auto& attrKey = _array->_desc.getAttributes().findattr(_addr.attId);
    _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                             _array->_diskIndex->getDsk(),
                                             attrKey,
                                             _addr.coords,
                                             _addr.arrVerId);
    _curr = _array->_diskIndex->leastUpper(key.getKey());

    if (_array->_desc.getEmptyBitmapAttribute()) {
        auto ebmAttrId = _array->_desc.getEmptyBitmapAttribute();

        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 *ebmAttrId,
                                                 _addr.coords,
                                                 _addr.arrVerId);
        _currBitmap = _array->_diskIndex->leastUpper(key.getKey());
    }

    /* If we are off the end, we should call setCurrent and be done.
     */
    if (_curr.isEnd()) {
        setCurrent();
        return;
    }

    auto ebmAttr = _array->_desc.getAttributes().getEmptyBitmapAttribute();
    auto currAttId = _curr.getKey().decodeAttributeID(ebmAttr);

    if (currAttId != _addr.attId) {
        setCurrent();
        return;
    }

    /* We have found the first entry in the map for this attribute.
       make sure we are viewing the correct version (check the version id)
     */
    PersistentAddress foundAddr;

    _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(), foundAddr, ebmAttr);
    if (foundAddr.arrVerId > _array->_desc.getId()) {
        findNextLogicalChunk(_addr, _array->_desc.getId(), query);
        return;
    }

    /* We are viewing the correct version, but it still may not
       be the one we want for two reasons:
       1) it may be a replica (check if we are not responsible)
       2) it may be a tombstone (check for null value)
     */
    InstanceID foundIid;
    ReplicationManager* repMgr = ReplicationManager::getInstance();
    foundIid = _curr.getAuxMeta().primaryInstanceId;

    if (!repMgr->isResponsibleFor(_array->_desc, foundAddr, foundIid, query) ||
        _curr.getBufHandle().isNull()) {
        findNextLogicalChunk(foundAddr, _array->_desc.getId(), query);
        return;
    }

    /* This must be the one we want
     */
    setCurrent();
}

void DBArrayIterator::deleteChunk(Chunk& aChunk)
{
    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::deleteChunk()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on
    CachedDBChunk& chunk = dynamic_cast<CachedDBChunk&>(aChunk);
    ScopedMutexLock cs(_array->_mutex, PTW_SML_STOR_U);
    chunk._accessCount = 0;

    DbAddressMeta::Key* key = chunk._key.getKey();
    try {
        _array->_diskIndex->deleteRecord(key);
    }
    catch (const std::exception& e) {
        LOG4CXX_DEBUG(logger,
                      "Key " << key
                      << " not found in disk index when deleting chunk, "
                      << e.what());
    }
    catch (...) {
        LOG4CXX_WARN(logger, "Unexpected exception when deleting chunk");
    }
}

Chunk& DBArrayIterator::newChunk(Coordinates const& pos)
{
    if (!_array->_desc.contains(pos) ||
        hasInjectedError(CHUNK_OUT_OF_BOUNDS, __LINE__, __FILE__)) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
            << CoordsToStr(pos) << _array->_desc.getDimensions();
    }

    ScopedMutexLock cs(_array->_mutex, PTW_SML_STOR_V);

    _addr.coords = pos;
    _addr.arrVerId = _array->_desc.getId();
    _array->_desc.getChunkPositionFor(_addr.coords);

    // clang-format off
    LOG4CXX_TRACE(logger, "DBArrayIterator::newChunk()"
                          << " Array=" << (void*)_array.get()
                          << " Addr=" << _addr.toString()
                          << " Positioned=" << _positioned);
    // clang-format on

    resetChunkRefs();

    /* TODO: previously we checked for existence of chunk at this point.
       Is this necessary?  Can we check when chunk is written (unpin)?
     */
    _array->makeChunk(_addr, _currChunk, _currBitmapChunk, true /* chunk pinned */);
    return *(_currChunk);
}

Chunk& DBArrayIterator::newChunk(Coordinates const& pos, CompressorType compressionMethod)   // coverage
{
    SCIDB_ASSERT(compressionMethod != CompressorType::UNKNOWN);
    Chunk& chunk = newChunk(pos);
    ((MemChunk&)chunk).compressionMethod = compressionMethod;
    return chunk;
}

}  // namespace scidb
