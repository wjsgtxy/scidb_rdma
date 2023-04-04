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
 * @file ChunkAuxMeta.h
 *
 * @brief ChunkAuxMeta implementation
 */

#ifndef CHUNK_AUX_META_H_
#define CHUNK_AUX_META_H_

#include <storage/StorageMgr.h>
#include <query/InstanceID.h>

namespace scidb {

/**
 * Chunk metadata which is not part of the index key
 *
 * Must be a POD struct and asserted as such, so we can store it in
 * a flat RocksDB slice.  ChunkAuxMeta is persisted to disk so the
 * size needs to be fixed.
 */
struct ChunkAuxMeta
{
    ChunkAuxMeta()
      : primaryInstanceId(static_cast<uint64_t>(INVALID_INSTANCE))
      , nElements(0)
      , storageVersion(StorageMgr::getInstance()->SCIDB_STORAGE_FORMAT_VERSION)
    {}

    // instance that responds with the chunk under normal circumstances,
    // for the replicated distribution, this is set to the local instanceID on all instances
    // (i.e. they are all "primary")
    // we need to know the Id, rather than just "is it mine" so that non-primary instances
    // can start serving the data when the primary has gone offline.
    // NOTE: in theory could be replaced by bool "isMine", if ReplicationMgr::isResponsibleFor()
    //       can be modified to suit.  See the comments in isResponsibleFor()
    InstanceID primaryInstanceId;
    uint64_t nElements;
    uint32_t storageVersion;
};

// Unfortunately, gcc 4.9.2 doesn't supply std::is_trivially_copyable even though
// it is part of the C++11 standard.
// static_assert(std::is_trivially_copyable<ChunkAuxMeta>::value,
//               "ChunkAuxMetadata must have POD members only");

}  // namespace scidb

#endif  // CHUNK_AUX_META_H_
