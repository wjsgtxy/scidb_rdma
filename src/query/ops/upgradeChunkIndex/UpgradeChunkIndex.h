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

namespace scidb
{
class ArrayDesc;
class ChunkDescriptor;
class PersistentAddress;
class StorageAddress;

class UpgradeStorage : public CachedStorage
{
    /// Convert a StorageAddress to a PersistentAddress
    /// @param addr The StorageAddress from which to convert
    /// @param persistentAddress The target PersistentAddress into which to convert
    void convertToPersistentAddress(const StorageAddress& addr,
                                    PersistentAddress& persistentAddress);

    /// Retrieve the DiskIndex implementation depending on the ArrayDesc
    /// @param desc The ArrayDesc as the key to fetch the disk index
    /// @return A shared pointer to the DiskIndex
    std::shared_ptr<DiskIndex<DbAddressMeta>> getDiskIndex(const ArrayDesc& desc);

    /// Implement the hook from CachedStorage driving conversion of chunk
    /// descriptors from the legacy storage.header into rocksdb
    /// @param adesc The ArrayDesc for the chunk descriptor from the legacy storage.header
    /// @param desck The chunk descriptor from the legacy storage.header
    /// @param addr The StorageAddress of the chunk from the legacy storage.header
    virtual void onCreateChunk(const ArrayDesc& adesc,
                               const ChunkDescriptor& desc,
                               const StorageAddress& addr) override;
};


/// Entry point to chunk conversion logic, called from the upgradeChunkIndex operator
void upgradeChunkIndex();

}  // namespace scidb
