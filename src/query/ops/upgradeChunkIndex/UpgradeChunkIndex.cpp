/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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

#include "io/InternalStorage.h"
#include "io/Storage.h"
#include "UpgradeChunkIndex.h"

#include <array/CachedDBChunk.h>
#include <array/DBArray.h>
#include <array/Metadata.h>
#include <system/Config.h>
#include <system/Constants.h>

#include <string>


namespace scidb
{

void UpgradeStorage::convertToPersistentAddress(
    const StorageAddress& addr,
    PersistentAddress& persistentAddress)
{
    // Both StorageAddress and PersistentAddress have Address as
    // a parent and Address implements an assignment operator.
    // PersistentAddress does not implement an assignment operator
    // with StorageAddress as the rvalue.  So I get references to
    // the parents here, perform the assignment through the Address
    // assignment operator, then assign the remaining field.  Since
    // this location in the code is the only place where this
    // assignment is needed, I encapsulated it here rather than
    // implement it on any of the other classes.
    Address& paParent = persistentAddress;
    const Address& saParent = addr;
    paParent = saParent;
    persistentAddress.arrVerId = addr.arrId;
}

std::shared_ptr<DiskIndex<DbAddressMeta>>
UpgradeStorage::getDiskIndex(const ArrayDesc& adesc)
{
    // Fetch the Disk Index by first retrieving the Data Store Key
    // from the Array Descriptor and passing that to the Index Manager
    // which can fetch the Disk Index directly.
    DataStore::DataStoreKey dsk = DBArrayMgr::getInstance()->getDsk(adesc);
    DbAddressMeta addressMeta;
    std::shared_ptr<DiskIndex<DbAddressMeta>> pdiskIndex;
    IndexMgr<DbAddressMeta>::getInstance()->getIndex(pdiskIndex, dsk, addressMeta);
    return pdiskIndex;
}


void
UpgradeStorage::onCreateChunk(const ArrayDesc& adesc,
                              const ChunkDescriptor& desc,
                              const StorageAddress& addr)
{
    // This hook is called from CachedStorage::initChunkMap
    CachedDBChunk* cachedDBChunk = nullptr;
    CachedDBChunk* bitmapDBChunk = nullptr;
    auto pdiskIndex = getDiskIndex(adesc);
    PersistentAddress persistentAddress;
    convertToPersistentAddress(addr, persistentAddress);
    CachedDBChunk::createChunk(adesc,
                               pdiskIndex,
                               persistentAddress,
                               cachedDBChunk,
                               bitmapDBChunk,
                               true,
                               nullptr);
    CachedDBChunk::upgradeDescriptor(*cachedDBChunk, pdiskIndex, desc);
}

std::string
getLegacyConfigurationFile(const std::string& storageConfigPath)
{
    // the legacy configuration file is named storage.cfg, take the config path (which
    // is set by arguments from scidbctl.py anyhow) and return a copy with the location
    // of the pre-17.x storage.cfg file.
    auto basenameOffset = storageConfigPath.find_last_of('/') + 1;
    auto newStorageConfigFilePath = storageConfigPath.substr(0, basenameOffset);
    return newStorageConfigFilePath + "storage.cfg";
}

void
upgradeChunkIndex()
{
    UpgradeStorage legacyStorage;
    Config *cfg = Config::getInstance();
    auto storageConfigPath = cfg->getOption<std::string>(CONFIG_STORAGE);
    storageConfigPath = getLegacyConfigurationFile(storageConfigPath);

    // UpgradeStorage extends CachedStorage, so this calls open on CachedStorage.
    // CachedStorage::open leads to a call to CachedStorage::initChunkMap which will
    // call the onCreateChunk hook implemented on UpgradeStorage.  That causes the
    // new chunk descriptor to be created in rocksdb.
    legacyStorage.open(storageConfigPath, cfg->getOption<int>(CONFIG_SMGR_CACHE_SIZE)*MiB);
}

}  // namespace scidb
