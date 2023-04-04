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

/****************************************************************************/
#include "ListArrayBuilders.h"

#include <array/ChunkAuxMeta.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Session.h>

#include <iostream>


using namespace std;
using NsComm = scidb::namespaces::Communicator;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

/**
 * Cast unsigned array id to signed for easy redimensioning.
 *
 * @details
 * Although ArrayID is unsigned, we treat array id attributes as
 * signed here so that they can be converted to dimensions by a
 * redimension().  That's useful for a variety of reporting.
 *
 * @p Note that a datastore id (dsid) is really an unversioned array
 * id by another name.
 */
inline int64_t array_id_cast(ArrayID aid)
{
    return safe_static_cast<int64_t>(aid);
}

/****************************************************************************/

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("svrsn",TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("instn",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("dsid", TID_INT64, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("doffs",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("uaid", TID_INT64, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("aid",  TID_INT64, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("attid",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("coord",TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("comp", TID_INT8,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("flags",TID_UINT8, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("nelem",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("csize",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("usize",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("asize",TID_UINT64,0,CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

/* For back-compatibility with the old flags enum
 */
const uint32_t CHUNK_HEADER_TOMBSTONE_FLAG = 8;

void ListChunkMapArrayBuilder::list(typename DbAddressMeta::Key const* key,
                                    BufferMgr::BufferHandle& buf,
                                    const ChunkAuxMeta& chunkAuxMeta)
{
    Coordinates coords;
    std::ostringstream s;

    for (uint i = 0; i < key->_nDims; ++i)
    {
        coords.push_back(key->_coords[i]);
    }

    beginElement();
    write(STORAGE_VERSION,  chunkAuxMeta.storageVersion);
    write(INSTANCE_ID,      chunkAuxMeta.primaryInstanceId);
    write(DATASTORE_DSID,   array_id_cast(key->dsk().getDsid()));
    write(DISK_OFFSET,      buf.offset());
    write(U_ARRAY_ID,       array_id_cast(key->dsk().getDsid()));
    write(V_ARRAY_ID,       array_id_cast(key->_arrVerId));
    write(ATTRIBUTE_ID,     key->_attId);
    s << coords;
    write(COORDINATES,      s.str());
    write(COMPRESSION,      static_cast<uint8_t>(buf.getCompressorType()));
    write(FLAGS,
          buf.isNull() ?
          uint8_t(CHUNK_HEADER_TOMBSTONE_FLAG) :
          uint8_t(0));
    write(NUM_ELEMENTS,     chunkAuxMeta.nElements);
    write(COMPRESSED_SIZE,  buf.compressedSize());
    write(UNCOMPRESSED_SIZE, buf.size());
    write(ALLOCATED_SIZE,   buf.allocSize());
    endElement();
}

Attributes ListBufferStatsArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("cache", TID_UINT64, 0, CompressorType::NONE));
    atts.push_back(AttributeDesc("dirty", TID_UINT64, 0, CompressorType::NONE));
    atts.push_back(AttributeDesc("pinned", TID_UINT64, 0, CompressorType::NONE));
    atts.push_back(AttributeDesc("pending", TID_UINT64, 0, CompressorType::NONE));
    atts.push_back(AttributeDesc("reserve", TID_UINT64, 0, CompressorType::NONE));
    atts.push_back(AttributeDesc("limit", TID_UINT64, 0, CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

void ListBufferStatsArrayBuilder::list()
{
    BufferMgr::BufferStats bufStats;
    BufferMgr::getInstance()->getByteCounts(bufStats);

    beginElement();
    write(CACHED_BYTES, bufStats.cached);
    write(DIRTY_BYTES,  bufStats.dirty);
    write(PINNED_BYTES, bufStats.pinned);
    write(PENDING_BYTES, bufStats.pending);
    write(RESERVE_REQ_BYTES, bufStats.reserveReq);
    write(USED_MEM_LIMIT, bufStats.usedLimit);
    endElement();
}

/****************************************************************************/

Attributes ListDiskIndexArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("dsid",TID_INT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("nsid",TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("uaid",TID_INT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("aid",TID_INT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("attid",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("coord",TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("boffs",TID_INT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("bsize",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("csize",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("asize",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("nelem",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("svrsn",TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("iid",TID_UINT64,0,CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

void ListDiskIndexArrayBuilder::list(typename DbAddressMeta::Key const* key,
                                     BufferMgr::BufferHandle& bufHandle,
                                     const ChunkAuxMeta& chunkAuxMeta)
{
    Coordinates coords;
    std::ostringstream s;

    for (uint i = 0; i < key->_nDims; ++i)
    {
        coords.push_back(key->_coords[i]);
    }

    beginElement();

    write(DATASTORE_DSID,   array_id_cast(key->dsk().getDsid()));
    write(DATASTORE_NSID,   key->dsk().getNsid());
    write(U_ARRAY_ID,       array_id_cast(key->dsk().getDsid()));
    write(V_ARRAY_ID,       array_id_cast(key->_arrVerId));
    write(ATTRIBUTE_ID,     key->_attId);
    s << coords;
    write(COORDINATES,      s.str());
    write(BUFFER_OFF,       bufHandle.offset());
    write(BUFFER_SIZE,      bufHandle.size());
    write(BUF_COMPRESS_SIZE, bufHandle.compressedSize());
    write(ALLOCATED_SIZE,   bufHandle.allocSize());
    write(NUM_ELEMENTS,     chunkAuxMeta.nElements);
    write(STORAGE_VERSION,  chunkAuxMeta.storageVersion);
    write(INSTANCE_ID,      chunkAuxMeta.primaryInstanceId);
    endElement();
}

/****************************************************************************/

Attributes ListLibrariesArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("name",      TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("major",     TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("minor",     TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("patch",     TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("build",     TID_UINT32,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("build_type",TID_STRING,AttributeDesc::IS_NULLABLE, CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

void ListLibrariesArrayBuilder::list(const PluginManager::Plugin& e)
{
    beginElement();
    write(PLUGIN_NAME,e._name);
    write(MAJOR,      e._major);
    write(MINOR,      e._minor);
    write(BUILD,      e._build);
    write(PATCH,      e._patch);
    if (e._buildType.empty())
        write(BUILD_TYPE,Value());
    else
        write(BUILD_TYPE,e._buildType);
    endElement();
}

/****************************************************************************/

ListAggregatesArrayBuilder::ListAggregatesArrayBuilder(size_t cellCount, bool showSys)
 : ListArrayBuilder("aggregates")
 , _cellCount(cellCount)
 , _showSys(showSys)
{}

Dimensions ListAggregatesArrayBuilder::getDimensions(std::shared_ptr<Query> const& query) const
{
    size_t cellCount = std::max(_cellCount,1UL);

    return Dimensions(1,DimensionDesc("No",0,0,cellCount-1,cellCount-1,cellCount,0));
}

Attributes ListAggregatesArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("name",   TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("typeid", TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("library",TID_STRING,0,CompressorType::NONE));
    if (_showSys) {
        atts.push_back(AttributeDesc("internal", TID_BOOL,0,CompressorType::NONE));
    }
    atts.addEmptyTagAttribute();
    return atts;
}

void ListAggregatesArrayBuilder::list(const std::string& name,const TypeId& type,const std::string& lib)
{
    bool isSys = (name.find('_') == 0);
    if (!isSys || _showSys) {
        beginElement();
        write(0, name);
        write(1, type);
        write(2, lib);
        if (_showSys) {
            write(3, isSys);
        }
        endElement();
    }
}

/****************************************************************************/

ListDataStoresArrayBuilder::ListDataStoresArrayBuilder()
    : PERSISTENT_NSID(DataStores::getInstance()->openNamespace("persistent"))
{ }


Attributes ListDataStoresArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("uaid",           TID_INT64,0,CompressorType::NONE));

    // the next two values are passed as signed values.  file_free_bytes can return -1
    atts.push_back(AttributeDesc("file_bytes",     TID_INT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("file_free_bytes",TID_INT64,0,CompressorType::NONE));

    atts.addEmptyTagAttribute();
    return atts;
}

void ListDataStoresArrayBuilder::list(DataStore& item)
{
    off_t    filebytes = 0;
    off_t    filefree = 0;

    // list only the persistent datastores
    if (item.getDsk().getNsid() != PERSISTENT_NSID)
    {
        return;
    }

    item.getSizes(filebytes, filefree);

    beginElement();
    write(DSID,           array_id_cast(item.getDsk().getDsid()));
    write(FILE_BYTES,     filebytes);
    write(FILE_FREE ,     filefree); // can be -1 e.g. when onfig-punch-holes disabled
    endElement();
}

/****************************************************************************/

ListQueriesArrayBuilder::ListQueriesArrayBuilder(std::shared_ptr<Query> const& query)
    : ListArrayBuilder()
{
    _myUserId = query->getSession()->getUser().getId();

    // Find out if we have operator privilege.  If not, user can only
    // list their own queries.

    rbac::RightsMap rights;
    rights.upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
    try {
        NsComm::checkAccess(query->getSession().get(), &rights);
        _hasOperatorPrivs = true;
    }
    catch (Exception&) {
        _hasOperatorPrivs = false;
    }
}

Attributes ListQueriesArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("query_id",     TID_STRING,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("coordinator",  TID_UINT64,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("query_string", TID_STRING,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("creation_time",TID_DATETIME,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("error_code",   TID_INT32,   0,CompressorType::NONE));
    atts.push_back(AttributeDesc("error",        TID_STRING,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("idle",         TID_BOOL,    0,CompressorType::NONE));
    atts.push_back(AttributeDesc("user_id",      TID_UINT64,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("user",         TID_STRING,  0,CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

void ListQueriesArrayBuilder::list(std::shared_ptr<Query> const& query)
{
    std::shared_ptr<Session> session(query->getSession());

    // Skip this query if it's not ours and we don't have operator privilege.
    if (!_hasOperatorPrivs && session->getUser().getId() != _myUserId) {
        return;
    }

    const bool resolveLocalInstanceID = true;
    std::stringstream qs;
    beginElement();
    qs << query->getQueryID();

    write(QUERY_STR,    query->queryString);
    write(QUERY_ID,     qs.str());
    write(COORDINATOR,  query->getPhysicalCoordinatorID(resolveLocalInstanceID));
    write(CREATION_TIME,query->getCreationTime());

    std::shared_ptr<Exception> error(query->getError());

    write(ERROR_CODE,   error ? error->getLongErrorCode() : 0);
    write(ERROR,        error ? error->getErrorMessage() : "");
    write(IDLE,         query->idle());
    write(USER_ID,      session ? session->getUser().getId() : rbac::NOBODY);
    write(USER_NAME,    session ? session->getUser().getName() : rbac::NOBODY_USER);
    endElement();
}

/****************************************************************************/

Attributes ListCounterArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("name",       TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("total",      TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("total_msecs",TID_UINT64,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("avg_msecs",  TID_FLOAT, 0,CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

void ListCounterArrayBuilder::list(const CounterState::Entry& item)
{
    float avg_msecs =
      item._num ?
      static_cast<float>(item._msecs) / static_cast<float>(item._num) :
      0;

    beginElement();
    write(NAME,       CounterState::getInstance()->getName(item._id));
    write(TOTAL,      item._num);
    write(TOTAL_MSECS,item._msecs);
    write(AVG_MSECS,  avg_msecs);
    endElement();
}

/****************************************************************************/

Dimensions ListArraysArrayBuilder::getDimensions(const std::shared_ptr<Query>& query) const
{
    return Dimensions(
        1,DimensionDesc("No",0,0,
        CoordinateBounds::getMax(),
        CoordinateBounds::getMax(),
        LIST_CHUNK_SIZE,0));
}

Attributes ListArraysArrayBuilder::getAttributes() const
{
    Attributes atts;
    atts.push_back(AttributeDesc("name",        TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("uaid",        TID_INT64, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("aid",         TID_INT64, 0,CompressorType::NONE));
    atts.push_back(AttributeDesc("schema",      TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("availability",TID_BOOL,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("temporary",   TID_BOOL,  0,CompressorType::NONE));
    atts.push_back(AttributeDesc("namespace",   TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("distribution", TID_STRING,0,CompressorType::NONE));
    atts.push_back(AttributeDesc("etcomp",
                                 TID_STRING,
                                 AttributeDesc::IS_NULLABLE,
                                 CompressorType::NONE));
    atts.addEmptyTagAttribute();
    return atts;
}

Value encodeEmptyTagCompression(const AttributeDesc* emptyTag)
{
    Value v;

    if (emptyTag) {
        auto emptyTagCompression =
            compressorTypeToString(emptyTag->getDefaultCompressionMethod());
        v.setString(emptyTagCompression);
    }

    // Otherwise, the empty tag doesn't exist because the array doesn't
    // have one:  the attribute will be null for this position in the output.

    return v;
}

void ListArraysArrayBuilder::list(const ArrayDesc& desc, bool forBackup)
{
    stringstream s;
    printSchema(s, desc, !desc.isDataframe() || forBackup);

    beginElement();
    write(ARRAY_NAME,           desc.getName());
    write(ARRAY_UAID,           array_id_cast(desc.getUAId()));
    write(ARRAY_ID,             array_id_cast(desc.getId()));
    write(ARRAY_SCHEMA,         s.str());
    write(ARRAY_IS_AVAILABLE,   !desc.isDead() && desc.isComplete());
    write(ARRAY_IS_TRANSIENT,   desc.isTransient());
    write(ARRAY_NAMESPACE,      desc.getNamespaceName());
    write(ARRAY_DISTRIBUTION,   distTypeToStr(desc.getDistribution()->getDistType()));
    write(ARRAY_EMPTY_TAG_COMPRESSION,
          encodeEmptyTagCompression(desc.getEmptyBitmapAttribute()));
    endElement();
}

/****************************************************************************/
}
/****************************************************************************/
