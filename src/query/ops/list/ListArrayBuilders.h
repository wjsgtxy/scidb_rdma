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

#ifndef QUERY_LIST_ARRAY_BUILDER_H_
#define QUERY_LIST_ARRAY_BUILDER_H_

/****************************************************************************/

#include <array/ListArrayBuilder.h>
#include <query/Aggregate.h>
#include <rbac/Rbac.h>
#include <util/PluginManager.h>
#include <util/DataStore.h>
#include <util/Counter.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

class ChunkAuxMeta;

/**
 * Some short definitions shared by LogicalList and PhysicalList.
 * @{
 */
constexpr char const * const KW_NS = "namespace";
constexpr char const * const KW_NS_ABBREV = "ns";
constexpr char const * const KW_BACKUP = "_backup";
/** @} */

/**
 *  A ListArrayBuilder for listing PersistentChunk objects.
 */
/**
 *  A ListArrayBuilder for listing entries in disk indexes but displaying
 *  them using the old list('chunk map') format.
 */

struct ListChunkMapArrayBuilder : ListArrayBuilder
{
    enum
    {
        STORAGE_VERSION,
        INSTANCE_ID,
        DATASTORE_DSID,
        DISK_OFFSET,
        U_ARRAY_ID,
        V_ARRAY_ID,
        ATTRIBUTE_ID,
        COORDINATES,
        COMPRESSION,
        FLAGS,
        NUM_ELEMENTS,
        COMPRESSED_SIZE,
        UNCOMPRESSED_SIZE,
        ALLOCATED_SIZE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void list(typename DbAddressMeta::Key const* key,
              BufferMgr::BufferHandle& buf,
              const ChunkAuxMeta& auxMeta);
    Attributes getAttributes() const;
};

struct ListBufferStatsArrayBuilder : ListArrayBuilder
{
    enum
    {
        CACHED_BYTES,
        DIRTY_BYTES,
        PINNED_BYTES,
        PENDING_BYTES,
        RESERVE_REQ_BYTES,
        USED_MEM_LIMIT,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void list();
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing entries in disk indexes
 */
struct ListDiskIndexArrayBuilder : ListArrayBuilder
{
    enum
    {
        DATASTORE_DSID,
        DATASTORE_NSID,
        U_ARRAY_ID,
        V_ARRAY_ID,
        ATTRIBUTE_ID,
        COORDINATES,
        BUFFER_OFF,
        BUFFER_SIZE,
        BUF_COMPRESS_SIZE,
        ALLOCATED_SIZE,
        NUM_ELEMENTS,
        STORAGE_VERSION,
        INSTANCE_ID,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void list(typename DbAddressMeta::Key const* key,
              BufferMgr::BufferHandle& buf,
              const ChunkAuxMeta& stats);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing loaded library information.
 */
struct ListLibrariesArrayBuilder : ListArrayBuilder
{
    enum
    {
        PLUGIN_NAME,
        MAJOR,
        MINOR,
        PATCH,
        BUILD,
        BUILD_TYPE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const PluginManager::Plugin&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing aggregates information.
 */
struct ListAggregatesArrayBuilder : ListArrayBuilder
{
    size_t const _cellCount;
    bool const   _showSys;

    void       list(const std::string&,const TypeId&,const std::string&);
    Attributes getAttributes()                                const;
    Dimensions getDimensions(std::shared_ptr<Query> const&) const;

    ListAggregatesArrayBuilder(size_t cellCount, bool showSys);
};

/**
 *  A ListArrayBuilder for listing datastore information.
 */
struct ListDataStoresArrayBuilder : ListArrayBuilder
{
    DataStore::NsId const PERSISTENT_NSID;

    enum
    {
        DSID,
        FILE_BYTES,
        FILE_FREE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    ListDataStoresArrayBuilder();
    void       list(DataStore&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing Query objects.
 */
class ListQueriesArrayBuilder : public ListArrayBuilder
{
    bool _hasOperatorPrivs { false };
    rbac::ID _myUserId { rbac::NOBODY };

public:
    enum
    {
        QUERY_ID,
        COORDINATOR,
        QUERY_STR,
        CREATION_TIME,
        ERROR_CODE,
        ERROR,
        IDLE,
        USER_ID,
        USER_NAME,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    // Constructor used by logical operator.
    ListQueriesArrayBuilder() = default;

    // Constructor used by physical operator, sets _hasOperatorPrivs flag.
    explicit   ListQueriesArrayBuilder(std::shared_ptr<Query> const&);

    void       list(const std::shared_ptr<Query>&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing counter values.
 */
struct ListCounterArrayBuilder : ListArrayBuilder
{
    enum
    {
        NAME,
        TOTAL,
        TOTAL_MSECS,
        AVG_MSECS,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const CounterState::Entry&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing array information.
 */
struct ListArraysArrayBuilder : ListArrayBuilder
{
    enum
    {
        ARRAY_NAME,
        ARRAY_UAID,
        ARRAY_ID,
        ARRAY_SCHEMA,
        ARRAY_IS_AVAILABLE,
        ARRAY_IS_TRANSIENT,
        ARRAY_NAMESPACE,
        ARRAY_DISTRIBUTION,
        ARRAY_EMPTY_TAG_COMPRESSION,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const ArrayDesc&, bool forBackup);
    Attributes getAttributes() const;
    Dimensions getDimensions(std::shared_ptr<Query> const&) const;
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
