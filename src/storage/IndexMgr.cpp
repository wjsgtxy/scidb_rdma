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
 * @file IndexMgr.cpp
 * @author Steve Fridella, Donghui Zhang
 *
 * This file provides the implementation of functions defined in IndexMgr.h.
 */

#include <storage/IndexMgr.h>

#include <array/ArrayDesc.h>
#include <array/ChunkAuxMeta.h>
#include <storage/PersistentIndexMap.h>
#include <array/AddressMeta.h>
#include <system/Config.h>
#include <system/SystemCatalog.h>
#include <log4cxx/logger.h>

namespace scidb {

static log4cxx::LoggerPtr
    logger(log4cxx::Logger::getLogger("scidb.storage.diskindex"));

bool supportingEncodedEBM()
{
    static bool encode = false;
    if (!encode) {
        auto tmp = Config::getInstance()->getOption<int>(CONFIG_DISKINDEX_RENUMBER_EBM);
        encode = tmp == 1;
    }
    return encode;
}

AttributeID DiskIndexKeyMetaBase::KeyBase::_encodeAttributeID(const AttributeDesc& attr) const
{
    /**
     * When the EBM is stored on disk, its attribute ID
     * is changed from whatever it was in-flight to the
     * special EBM value.  New attributes that would be
     * added to the array come after existing attributes
     * yet on disk are guaranteed always to come before
     * the empty bitmap attribute, avoiding any re-keying
     * or re-writing on chunks at-rest on disk.
     */
    return supportingEncodedEBM() && attr.isEmptyIndicator() ? EBM_ATTRIBUTE_ID : attr.getId();
}

AttributeID DiskIndexKeyMetaBase::KeyBase::_decodeAttributeID(AttributeID attrId,
                                                              const AttributeDesc* ebm) const
{
    /**
     * Reverse the encoding scheme above, where if the
     * attrId passed to us is the at-rest, on disk, EBM
     * value, translate it to whatever the in-flight EBM
     * value is as specified by passed attribute.
     */
    if (attrId == EBM_ATTRIBUTE_ID) {
        SCIDB_ASSERT(ebm);
        auto ebmId = ebm->getId();
        SCIDB_ASSERT(ebmId != INVALID_ATTRIBUTE_ID);
        SCIDB_ASSERT(ebmId != EBM_ATTRIBUTE_ID);
        return ebmId;
    }

    return attrId;
}


template<class KeyMeta>
typename DiskIndex<KeyMeta>::Iterator& DiskIndex<KeyMeta>::Iterator::operator++()
{
    ScopedMutexLock scm(_index->_mutex, PTW_SML_DISK_INDEX);
    ++(*_itp);
    return *this;
}

template<class KeyMeta>
bool DiskIndex<KeyMeta>::Iterator::isEnd()
{
    ScopedMutexLock scm(_index->_mutex, PTW_SML_DISK_INDEX);
    return _itp->isEnd();
}

template<class KeyMeta>
typename KeyMeta::Key const& DiskIndex<KeyMeta>::Iterator::getKey()
{
    return *(_itp->getKey());
}

template<class KeyMeta>
size_t DiskIndex<KeyMeta>::Iterator::valueSize()
{
    return _itp->valueSize();
}

template<class KeyMeta>
const ChunkAuxMeta& DiskIndex<KeyMeta>::Iterator::getAuxMeta()
{
    return _itp->getAuxMeta();
}

template<class KeyMeta>
const BufferMgr::BufferHandle& DiskIndex<KeyMeta>::Iterator::getBufHandle() const
{
    return _itp->getBufHandle();
}

template<class KeyMeta>
DiskIndex<KeyMeta>::DiskIndex(DataStore::DataStoreKey const& dsk, KeyMeta const& keyMeta)
    : _dsk(dsk)
    , _keyMeta(keyMeta)
    , _arena(BufferMgr::getInstance()->getArena())
    , _deleteDataOnDestroy(false)
{
    /* Create the appropriate chunkmap instance
     */
    _map = constructIndexMap(dsk, keyMeta, _arena);
}

template<class KeyMeta>
DiskIndex<KeyMeta>::~DiskIndex()
{
    LOG4CXX_TRACE(logger, "~DiskIndex<KM>() dtor, dsk = " << _dsk.toString());

    if (_deleteDataOnDestroy) {
        _map->clear();
        BufferMgr::getInstance()->discardAllBuffers(_dsk);
    }
    arena::destroy(*_arena, _map);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::allocateMemory(size_t numBytes,
                                        DiskIndexValue& value,
                                        CompressorType compressionType)
{
    _cleanupDiskIndexValue(value);
    value._bufHandle = BufferMgr::getInstance()->allocateBuffer(_dsk,
                                                                numBytes,
                                                                BufferMgr::Deferred,
                                                                compressionType);
    value._state = DiskIndexValue::CallerPinned;
    value._diskIndex = this->shared_from_this();
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::_cleanupDiskIndexValue(DiskIndexValue& value)
{
    switch (value._state) {
    case DiskIndexValue::CallerPinned:
        value._bufHandle.unpinBuffer();
        value._bufHandle.discardBuffer();
        value._bufHandle.resetHandle();
        value._state = DiskIndexValue::Unpinned;
        break;
    case DiskIndexValue::IndexPinned:
        value._bufHandle.unpinBuffer();
        value._bufHandle.resetHandle();
        value._state = DiskIndexValue::Unpinned;
        break;
    default: // (i.e  DiskIndexValue::Unpinned:)
        break;
    }
}

template<class KeyMeta>
typename DiskIndex<KeyMeta>::Iterator DiskIndex<KeyMeta>::begin()
{
    ScopedMutexLock cs(_mutex, PTW_SML_DISK_INDEX);
    Iterator result(_map->begin(), this->shared_from_this());
    return result;
}

template<class KeyMeta>
typename DiskIndex<KeyMeta>::Iterator DiskIndex<KeyMeta>::find(typename KeyMeta::Key const* key)
{
    ScopedMutexLock cs(_mutex, PTW_SML_DISK_INDEX);
    Iterator result(_map->find(key), this->shared_from_this());
    return result;
}

template<class KeyMeta>
typename DiskIndex<KeyMeta>::Iterator
DiskIndex<KeyMeta>::leastUpper(typename KeyMeta::Key const* key)
{
    ScopedMutexLock cs(_mutex, PTW_SML_DISK_INDEX);
    Iterator result(_map->leastUpper(key), this->shared_from_this());
    return result;
}

template<class KeyMeta>
bool DiskIndex<KeyMeta>::insertRecord(typename KeyMeta::Key const* key,
                                      DiskIndexValue& value,
                                      const ChunkAuxMeta& auxMeta,
                                      bool keepPinned,
                                      bool update)
{
    ScopedMutexLock cs(_mutex, PTW_SML_DISK_INDEX);
    bool insertRes = true;
    if (update) {
        /* Update always succeeds.  Discard the old buffer if there was
           one.
         */
        BufferMgr::BufferHandle old = _map->update(key, value._bufHandle, auxMeta);
        old.discardBuffer();
    } else {
        /* Insert may fail if key exists.
         */
        insertRes = _map->insert(key, value._bufHandle, auxMeta);
    }

    if (insertRes) {
        /* Insert/update succeeded, unpin the buffer if required
           and update the value state.
         */
        if (keepPinned) {
            value._state = DiskIndexValue::IndexPinned;
            value._diskIndex = this->shared_from_this();
        } else {
            value._state = DiskIndexValue::Unpinned;
            value._bufHandle.unpinBuffer();
        }
    }
    return insertRes;
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::deleteRecord(Iterator& iter)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_D);
    IMIteratorPtr itp = iter._itp;
    deleteRecordInternal(itp);
    iter._itp.reset();
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::deleteRecord(typename KeyMeta::Key const* key)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_D);
    IMIteratorPtr itp = _map->find(key);
    deleteRecordInternal(itp);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::pinValue(typename KeyMeta::Key const* key, DiskIndexValue& value)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_E);

    _cleanupDiskIndexValue(value);

    /* Try to find the entry for the indicated key
     */
    Iterator target(_map->find(key), this->shared_from_this());
    pinValueInternal(target, value);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::pinValue(Iterator& iter, DiskIndexValue& value)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_E);

    _cleanupDiskIndexValue(value);

    pinValueInternal(iter, value);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::flushVersion(ArrayID vaid)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_F);
    _map->flush();
    BufferMgr::getInstance()->flushAllBuffers(_dsk);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::rollbackVersion(ArrayID vaid, VersionID lastVersion)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_G);

    if (lastVersion == 0) {
        // Rolling back initial store()/insert(), delete all buffers
        // *and* the DataStore.
        BufferMgr::getInstance()->discardAllBuffers(_dsk);
    } else {
        IMIteratorPtr itp = _map->begin();
        while (!itp->isEnd()) {
            if (_keyMeta.keyVersion(itp->getKey()) == vaid) {
                itp->getBufHandle().discardBuffer();
            }
            ++(*itp);
        }
    }

    _map->rollbackVersion(vaid);
    _map->flush();
    if (lastVersion != 0) {
        BufferMgr::getInstance()->flushMetadata(_dsk);
    }
    // ...else initial store()/insert(), don't recreate just-removed DataStore.
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::visitDiskIndex(const DiskIndexVisitor& visit) const
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_H);

    IMIteratorPtr itp = _map->begin();
    while (!itp->isEnd()) {
        visit(itp->getKey(), itp->getBufHandle(), itp->getAuxMeta());
        ++(*itp);
    }
}

template<class KeyMeta>
IndexMap<KeyMeta>* DiskIndex<KeyMeta>::constructIndexMap(DataStore::DataStoreKey dsk,
                                                         KeyMeta const& keyMeta,
                                                         arena::ArenaPtr arena)
{
    DataStores* ds = DataStores::getInstance();
    if (dsk.getNsid() == ds->openNamespace("tmp")) {
        static const int swapMd =
            Config::getInstance()->getOption<int>(CONFIG_SWAP_TEMP_METADATA);

        if (swapMd) {
            return arena::newScalar<RocksIndexMap<KeyMeta>>(*arena, dsk, keyMeta, arena);
        } else {
            return arena::newScalar<BasicIndexMap<KeyMeta>>(*arena, keyMeta, arena);
        }
    } else if (dsk.getNsid() == ds->openNamespace("persistent")) {
        return arena::newScalar<RocksIndexMap<KeyMeta>>(*arena, dsk, keyMeta, arena);
    } else if (dsk.getNsid() == ds->openNamespace("testbasic")) {
        return arena::newScalar<BasicIndexMap<KeyMeta>>(*arena, keyMeta, arena);
    } else if (dsk.getNsid() == ds->openNamespace("testrocksdb")) {
        return arena::newScalar<RocksIndexMap<KeyMeta>>(*arena, dsk, keyMeta, arena);
    } else {
        return arena::newScalar<BasicIndexMap<KeyMeta>>(*arena, keyMeta, arena);
    }
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::deleteRecordInternal(IMIteratorPtr itp)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    if (!itp || itp->isEnd() ||
        hasInjectedError(DI_FAIL_DELETE_RECORD, __LINE__, __FILE__)) {
        throw SYSTEM_EXCEPTION_SUBCLASS(KeyNotExistException);
    }
    itp->getBufHandle().discardBuffer();
    _map->erase(*itp);
}

template<class KeyMeta>
void DiskIndex<KeyMeta>::pinValueInternal(Iterator& iter, DiskIndexValue& value)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    IMIteratorPtr itp = iter._itp;
    if (itp->isEnd() ||
        hasInjectedError(DI_FAIL_PIN_VALUE, __LINE__, __FILE__)) {
        throw SYSTEM_EXCEPTION_SUBCLASS(KeyNotExistException);
    }
    itp->getBufHandle().pinBuffer();
    value._state = DiskIndexValue::IndexPinned;
    value._diskIndex = this->shared_from_this();
    value._bufHandle = itp->getBufHandle();
}

//------------------------------
// IndexMgr
//------------------------------

template<class KeyMeta>
IndexMgr<KeyMeta>::IndexMgr()
{}

template<class KeyMeta>
void IndexMgr<KeyMeta>::openIndex(std::shared_ptr<DiskIndex<KeyMeta>>& index,
                                  DataStore::DataStoreKey dsk,
                                  KeyMeta const& keyMeta)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_I);
    auto iter = _indexMap.find(dsk);
    // Either a new key(dsk)/value(index) pair is created in the map or update
    // the value if the index was a nullptr.
    ASSERT_EXCEPTION(iter == _indexMap.end() || !(iter->second),
                     "In IndexMgr::openIndex(), the index was already open.");
    index = std::make_shared<DiskIndex<KeyMeta>>(dsk, keyMeta);
    _indexMap[index->getDsk()] = index;
}

template<class KeyMeta>
bool IndexMgr<KeyMeta>::findIndex(std::shared_ptr<DiskIndex<KeyMeta>>& index,
                                  DataStore::DataStoreKey dsk)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_J);
    typename IndexMap::iterator it = _indexMap.find(dsk);
    if (it != _indexMap.end() && it->second) {
        index = it->second;
        return true;
    }
    return false;
}

template<class KeyMeta>
typename IndexMgr<KeyMeta>::IndexMap::iterator
IndexMgr<KeyMeta>::closeIndex(std::shared_ptr<DiskIndex<KeyMeta>> const& index, bool destroy)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_K);
    typename IndexMap::iterator it = _indexMap.find(index->getDsk());

    ASSERT_EXCEPTION(it != _indexMap.end(), "In IndexMgr::closeIndex(), the index did not exist.");
    if (destroy) {
        it->second->deleteDataOnDestroy();
    }
    return _indexMap.erase(it);
}

template<class KeyMeta>
void IndexMgr<KeyMeta>::addMissingDataStoreKeys()
{
    // The KeyMeta=MemAddressMeta only has information about temp arrays as a part of
    // running, so there are no missing datastore keys to get from the system catalog.
    // Make this the general case as well.
}

template<>
void IndexMgr<DbAddressMeta>::addMissingDataStoreKeys()
{
    DataStores* ds = DataStores::getInstance();
    std::vector<ArrayDesc> arrayDescs;
    DataStore::NsId nsid = ds->openNamespace("persistent");
    SystemCatalog::getInstance()->getArrays(/*nsName*/ std::string(),
                                            arrayDescs,
                                            /*ignoreOrphanAttributes*/ true,
                                            /*ignoreVersions*/ true,
                                            /*orderByName*/ false);
    for (ArrayDesc const& arrayDesc : arrayDescs) {
        if (arrayDesc.isTransient()) {
            // Just to save some work. Transient/Temp Arrays are not in
            // the PersistentIndexMap (IndexMap<DbAddressMeta>) anyway.
            continue;
        }
        DataStore::DataStoreKey dsk(nsid, arrayDesc.getUAId());
        std::shared_ptr<DiskIndex<DbAddressMeta>> index;
        auto result = _indexMap.insert(std::make_pair(dsk, index));
        if (result.second) {
            LOG4CXX_TRACE(logger, "Adding key dsk = " << result.first->first.toString());
        }
    }
}

template<class KeyMeta>
void IndexMgr<KeyMeta>::visitDiskIndexes(const typename DiskIndex<KeyMeta>::DiskIndexVisitor& visit,
                                         bool residentOnly)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_L);
    if (!residentOnly) {
        addMissingDataStoreKeys();
    }
    typename IndexMap::iterator it = _indexMap.begin();

    while (it != _indexMap.end()) {
        bool present = it->second.get();
        auto index = it->second;

        if (!present) {
            KeyMeta const km;
            openIndex(index, it->first, km);
        }
        index->visitDiskIndex(visit);
        if (!present) {
            it = closeIndex(index);
        } else {
            ++it;
        }
    }
}

template class DiskIndex<MemAddressMeta>;
template class IndexMgr<MemAddressMeta>;
template class DiskIndex<DbAddressMeta>;
template class IndexMgr<DbAddressMeta>;
}  // namespace scidb
