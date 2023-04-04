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
 * @file
 *
 * @brief Storage implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 * @author sfridella@paradigm4.com
 */

// stdc++lib
#include <limits>
#include <map>
#include <memory>
#include <unordered_set>

// linux and packages
#include <sys/time.h>
#include <inttypes.h>
#include <log4cxx/logger.h>

// scidb
#include <array/ArrayDistribution.h>
#include <array/CompressedBuffer.h>
#include <array/TileIteratorAdaptors.h>

#include <monitor/InstanceStats.h>
#include <monitor/MonitorConfig.h>
#include <network/MessageDesc.h>
#include <network/MessageUtils.h>
#include <query/PhysicalOperator.h>

#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

#include <util/FileIO.h>
#include <util/OnScopeExit.h>
#include <util/PerfTime.h>
#include <util/Platform.h>

#include "InternalStorage.h"

namespace scidb
{

using namespace std;

///////////////////////////////////////////////////////////////////
/// Constants and #defines
///////////////////////////////////////////////////////////////////

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr"));
static log4cxx::LoggerPtr chunkLogger(log4cxx::Logger::getLogger("scidb.smgr.chunk"));

const size_t DEFAULT_TRANS_LOG_LIMIT = 1024; // default limit of transaction log file (in mebibytes)
const size_t MAX_CFG_LINE_LENGTH = 1*KiB;
const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances

///////////////////////////////////////////////////////////////////
/// Static helper functions
///////////////////////////////////////////////////////////////////

inline static char* strtrim(char* buf)
{
    char* p = buf;
    char ch;
    while ((unsigned char) (ch = *p) <= ' ' && ch != '\0')
    {
        p += 1;
    }
    char* q = p + strlen(p);
    while (q > p && (unsigned char) q[-1] <= ' ')
    {
        q -= 1;
    }
    *q = '\0';
    return p;
}

inline static string relativePath(const string& dir, const string& file)
{
    return file[0] == '/' ? file : dir + file;
}

inline static double getTimeSecs()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (((double) tv.tv_sec) * 1000000 + ((double) tv.tv_usec)) / 1000000;
}

///////////////////////////////////////////////////////////////////
/// ChunkInitializer
///////////////////////////////////////////////////////////////////

CachedStorage::ChunkInitializer::~ChunkInitializer()
{
}

///////////////////////////////////////////////////////////////////
/// CachedStorage class
///////////////////////////////////////////////////////////////////

/* Constructor
 */
CachedStorage::CachedStorage()
:
    InjectedErrorListener(InjectErrCode::WRITE_CHUNK),
    _loadEvent(),
    _cacheOverflowEvent()
{}

/* Initialize/read the Storage Description file on startup
 */
void
CachedStorage::initStorageDescriptionFile(const std::string& storageDescriptorFilePath)
{
    InjectedErrorListener::start();
    char buf[MAX_CFG_LINE_LENGTH];
    char const* descPath = storageDescriptorFilePath.c_str();
    size_t pathEnd = storageDescriptorFilePath.find_last_of('/');
    _databasePath = "";
    if (pathEnd != string::npos)
    {
        _databasePath = storageDescriptorFilePath.substr(0, pathEnd + 1);
    }
    FILE* f = scidb::fopen(descPath, "r");
    if (f == NULL)
    {
        f = scidb::fopen(descPath, "w");
        if (!f)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) << descPath << ferror(f);
        size_t fileNameBeg = (pathEnd == string::npos) ? 0 : pathEnd + 1;
        size_t fileNameEnd = storageDescriptorFilePath.find_last_of('.');
        if (fileNameEnd == string::npos || fileNameEnd < fileNameBeg)
        {
            fileNameEnd = storageDescriptorFilePath.size();
        }
        string databaseName = storageDescriptorFilePath.substr(fileNameBeg, fileNameEnd - fileNameBeg);
        _databaseHeader = _databasePath + databaseName + ".header";
        _databaseLog = _databasePath + databaseName + ".log";
        scidb::fprintf(f, "%s.header\n", databaseName.c_str());
        scidb::fprintf(f, "%ld %s.log\n", (long) DEFAULT_TRANS_LOG_LIMIT, databaseName.c_str());
        _logSizeLimit = (uint64_t) DEFAULT_TRANS_LOG_LIMIT * MiB;
    }
    else
    {
        int pos;
        long sizeMb;
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseHeader = relativePath(_databasePath, strtrim(buf));
        if (!fgets(buf, sizeof buf, f))
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        if (sscanf(buf, "%ld%n", &sizeMb, &pos) != 1)
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        _databaseLog = relativePath(_databasePath, strtrim(buf + pos));
        _logSizeLimit = (uint64_t) sizeMb * MiB;
    }
    scidb::fclose(f);
}

/* Record an extent in the extent map
 */
void
CachedStorage::recordExtent(Extents& extents,
                            std::shared_ptr<PersistentChunk>& chunk)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    ChunkExtent ext;
    const ChunkHeader& hdr = chunk->getHeader();

    ext = std::make_tuple(hdr.pos.dsid,
                          hdr.pos.offs,
                          hdr.allocatedSize,
                          hdr.pos.hdrPos);
    if (!extents.insert(ext).second)
    {
        assert(false);
    }
}

/* Erase an extent from the extent map
 */
void
CachedStorage::eraseExtent(Extents& extents,
                           std::shared_ptr<PersistentChunk>& chunk)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    ChunkExtent ext;
    const ChunkHeader& hdr = chunk->getHeader();

    ext = std::make_tuple(hdr.pos.dsid,
                          hdr.pos.offs,
                          hdr.allocatedSize,
                          hdr.pos.hdrPos);
    extents.erase(ext);
}

/* Check extent map for overlaps on disk.  If in "recovery mode"
   replace overlapping chunks with tombstones.  If not in "recovery
   mode" throw exception.  Delete extent map when done.
 */
void
CachedStorage::checkExtentsForOverlaps(Extents& extents)
{
    if (_skipChunkmapIntegrityCheck)
    {
        return;
    }

    Extents::iterator ext_it = extents.begin();
    bool hasCurrent = false;
    ChunkExtent currExt;
    set<uint64_t> overlaps;

    /* Process the extents and check for overlaps
     */
    while (ext_it != extents.end())
    {
        if (!hasCurrent)
        {
            /* Starting a new extent
             */
            currExt = *ext_it;
            hasCurrent = true;
            extents.erase(ext_it);
            ext_it = extents.begin();
        }
        else
        {
            /* Still working on an extent
             */
            DataStore::DsId currDsid = std::get<0>(currExt);
            DataStore::DsId extDsid = std::get<0>(*ext_it);
            off_t currOff = std::get<1>(currExt);
            off_t extOff = std::get<1>(*ext_it);
            size_t currLen = std::get<2>(currExt);
            size_t extLen = std::get<2>(*ext_it);

            if (currDsid != extDsid ||
                extOff >= currOff + (off_t)currLen)
            {
                /* Found a new extent
                 */
                hasCurrent = false;
            }
            else
            {
                /* Found an overlap
                 */
                overlaps.insert(std::get<3>(currExt));
                overlaps.insert(std::get<3>(*ext_it));
                if (currOff + currLen < extOff + extLen)
                {
                    currExt = *ext_it;
                }
                extents.erase(ext_it);
                ext_it = extents.begin();
            }
        }
    }

    /* If overlaps were present log them and decide what to do
     */
    if (overlaps.size())
    {
        LOG4CXX_ERROR(logger, "smgr open:  found overlapping chunks in chunkmap: ");

        set<uint64_t>::iterator over_it;
        for (over_it = overlaps.begin();
             over_it != overlaps.end();
             ++over_it)
        {
            ChunkDescriptor desc;
            std::stringstream ss;

            size_t rc = _hd->read(&desc, sizeof(ChunkDescriptor), *over_it);
            SCIDB_ASSERT(rc == sizeof(ChunkDescriptor));

            ss<< "    [dsguid=" << desc.hdr.pos.dsid <<
                "] [offset=" << desc.hdr.pos.offs <<
                "] [hdrpos=" << desc.hdr.pos.hdrPos <<
                "] [len=" << desc.hdr.allocatedSize <<
                "] [arrayid=" << desc.hdr.arrId <<
                "] [attrid=" << desc.hdr.attId <<
                "] [coords=";
            for (uint16_t i=0;
                 (i < desc.hdr.nCoordinates) &&
                 (i < MAX_NUM_DIMS_SUPPORTED);
                 ++i)
            {
                ss << desc.coords[i] << " ";
            }
            ss << "]";
            LOG4CXX_ERROR(logger, ss.str());

            if (_enableChunkmapRecovery)
            {
                /* In reovery mode, mark all attribute chunks at this position
                   as a tombstone
                 */
                LOG4CXX_ERROR(logger,
                    "    marking position for overlapping chunk as tombstone.");

                ChunkMap::iterator cmiter = _chunkMap.find(desc.hdr.pos.dsid);
                ASSERT_EXCEPTION((cmiter != _chunkMap.end()),
                                 "Attempt to create tombstone for unkown array");
                std::shared_ptr<InnerChunkMap> inner = cmiter->second;
                InnerChunkMap::iterator mapiter;
                StorageAddress addr;

                desc.getAddress(addr);

                for (addr.attId = 0;
                     (mapiter = inner->find(addr)) != inner->end();
                     addr.attId++)
                {
                    mapiter->second.getChunk().reset();
                    mapiter->second.setTombstonePos(InnerChunkMapEntry::INVALID,
                                                    desc.hdr.pos.hdrPos);
                }
            }
        }

        if (!_enableChunkmapRecovery)
        {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_DATABASE_HEADER_CORRUPTED);
        }
    }
}

/* Initialize the chunk map from on-disk store
 */
void
CachedStorage::initChunkMap()
{
    LOG4CXX_TRACE(logger, "smgr open:  reading chunk map, nchunks " << _hdr.nChunks);

    _enableChunkmapRecovery =
        Config::getInstance()->getOption<bool> (CONFIG_ENABLE_CHUNKMAP_RECOVERY);
    _skipChunkmapIntegrityCheck =
        Config::getInstance()->getOption<bool> (CONFIG_SKIP_CHUNKMAP_INTEGRITY_CHECK);

    ChunkDescriptor desc;
    uint64_t chunkPos = HEADER_SIZE;
    StorageAddress addr;
    set<ArrayID> removedArrays;
    typedef map<ArrayID, ArrayID> ArrayMap;
    ArrayMap oldestVersions;
    typedef map<ArrayID, std::shared_ptr<ArrayDesc> > ArrayDescCache;
    ArrayDescCache existentArrays;
    Extents extents;

    for (size_t i = 0; i < _hdr.nChunks; i++, chunkPos += sizeof(ChunkDescriptor))
    {
        size_t rc = _hd->read(&desc, sizeof(ChunkDescriptor), chunkPos);
        if (rc != sizeof(ChunkDescriptor))
        {
            LOG4CXX_ERROR(logger, "Inconsistency in storage header: rc="
                          << rc << ", chunkPos="
                          << chunkPos << ", i="
                          << i << ", hdr.nChunks="
                          << _hdr.nChunks << ", hdr.currPos="
                          << _hdr.currPos);
            _hdr.currPos = chunkPos;
            _hdr.nChunks = i;
            break;
        }
        if (desc.hdr.pos.hdrPos != chunkPos)
        {
            LOG4CXX_ERROR(logger, "Invalid chunk header " << i << " at position " << chunkPos
                          << " desc.hdr.pos.hdrPos=" << desc.hdr.pos.hdrPos
                          << " arrayID=" << desc.hdr.arrId
                          << " hdr.nChunks=" << _hdr.nChunks);
            _freeHeaders.insert(chunkPos);
        }
        else
        {
            assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);

            if (desc.hdr.arrId != 0)
            {
                /* Check if unversioned array exists
                 */
                ArrayDescCache::iterator it = existentArrays.find(desc.hdr.pos.dsid);
                if (it == existentArrays.end())
                {
                    if (removedArrays.count(desc.hdr.pos.dsid) == 0)
                    {
                        try
                        {
                            std::shared_ptr<ArrayDesc> ad =
                                SystemCatalog::getInstance()->getArrayDescForDsId(desc.hdr.pos.dsid);
                            it = existentArrays.insert(
                                ArrayDescCache::value_type(desc.hdr.pos.dsid, ad)
                                ).first;
                        }
                        catch (SystemException const& x)
                        {
                            if (x.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST)
                            {
                                removedArrays.insert(desc.hdr.pos.dsid);
                            }
                            else
                            {
                                throw x;
                            }
                        }
                    }
                }

                /* If the unversioned array does not exist... wipe the chunk
                 */
                if (it == existentArrays.end())
                {
                    desc.hdr.arrId = 0;
                    LOG4CXX_TRACE(chunkLogger,
                                  "chunkl: initchunkmap: remove chunk desc "
                                  << "for non-existant array at position "
                                  << chunkPos);
                    _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                    assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                    _freeHeaders.insert(chunkPos);
                    continue;
                }

                /* Else add chunk to map (if it is live)
                 */
                else
                {
                    /* Init array descriptor
                     */
                    ArrayDesc& adesc = *it->second;
                    assert(adesc.getUAId() == desc.hdr.pos.dsid);

                    /* Find/init the inner chunk map
                     */
                    ChunkMap::iterator iter = _chunkMap.find(adesc.getUAId());
                    if (iter == _chunkMap.end())
                    {
                        iter = _chunkMap.insert(make_pair(adesc.getUAId(),
                                                          std::make_shared <InnerChunkMap> ())).first;
                    }
                    std::shared_ptr<InnerChunkMap>& innerMap = iter->second;

                    /* Find the oldest version of array, and the storage address
                       of the chunk currently in use by this version
                    */
                    ArrayMap::iterator oldest_it = oldestVersions.find(adesc.getUAId());
                    if (oldest_it == oldestVersions.end())
                    {
                        oldestVersions[adesc.getUAId()] =
                            SystemCatalog::getInstance()->getOldestArrayVersion(adesc.getUAId());
                    }
                    desc.getAddress(addr);
                    StorageAddress oldestVersionAddr = addr;
                    oldestVersionAddr.arrId = oldestVersions[adesc.getUAId()];
                    StorageAddress oldestLiveChunkAddr;
                    InnerChunkMap::iterator oldestLiveChunk =
                        innerMap->lower_bound(oldestVersionAddr);
                    if (oldestLiveChunk == innerMap->end() ||
                        oldestLiveChunk->first.coords != oldestVersionAddr.coords ||
                        oldestLiveChunk->first.attId != oldestVersionAddr.attId)
                    {
                        oldestLiveChunkAddr = oldestVersionAddr;
                        oldestLiveChunkAddr.arrId = 0;
                    }
                    else
                    {
                        oldestLiveChunkAddr = oldestLiveChunk->first;
                    }

                    /* Chunk is live if and only if arrayID of chunk is > arrayID of chunk
                       currently pointed to by oldest version
                    */
                    if (desc.hdr.arrId > oldestLiveChunkAddr.arrId)
                    {
                        /* Chunk is live, put it in the map
                         */
                        std::shared_ptr<PersistentChunk>& chunk =(*innerMap)[addr].getChunk();
                        ASSERT_EXCEPTION((!chunk), "smgr open: NOT unique chunk");
                        if (!desc.hdr.is<ChunkHeader::TOMBSTONE>())
                        {
                            onCreateChunk(adesc, desc, addr);
                            chunk.reset(new PersistentChunk());
                            chunk->setAddress(adesc, desc);
                            recordExtent(extents, chunk);
                        }
                        else
                        {
                            (*innerMap)[addr].setTombstonePos(
                                    InnerChunkMapEntry::TOMBSTONE,
                                    desc.hdr.pos.hdrPos);
                        }

                        /* Now check if by inserting this chunk we made the previous one dead...
                         */
                        if (oldestLiveChunkAddr.arrId &&
                            desc.hdr.arrId <= oldestVersionAddr.arrId)
                        {
                            /* The oldestLiveChunk is now dead... wipe it out
                             */
                            DataStore::DataStoreKey dsk(_dsnsid,
                                                        desc.hdr.pos.dsid);
                            std::shared_ptr<DataStore> ds =
                                _datastores->getDataStore(dsk);
                            if (!oldestLiveChunk->second.isTombstone())
                            {
                                eraseExtent(extents,
                                            oldestLiveChunk->second.getChunk());
                            }
                            markChunkAsFree(oldestLiveChunk->second, ds);
                            innerMap->erase(oldestLiveChunk);
                        }
                    }
                    else
                    {
                        /* Chunk is dead, wipe it out
                         */
                        DataStore::DataStoreKey dsk(_dsnsid,
                                                    desc.hdr.pos.dsid);
                        std::shared_ptr<DataStore> ds =
                            _datastores->getDataStore(dsk);
                        desc.hdr.arrId = 0;
                        LOG4CXX_TRACE(chunkLogger, "chunkl: initchunkmap: "
                                      << "remove dead chunk desc for non-existent "
                                      << "array version at position " << chunkPos);
                        _hd->writeAll(&desc.hdr, sizeof(ChunkHeader), chunkPos);
                        assert(desc.hdr.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
                        _freeHeaders.insert(chunkPos);
                        ds->freeChunk(desc.hdr.pos.offs, desc.hdr.allocatedSize);
                    }
                }
            }
            else
            {
                _freeHeaders.insert(chunkPos);
            }
        }
    }

    /* Perform some simple validation for storage header
     */
    if (chunkPos != _hdr.currPos)
    {
        LOG4CXX_ERROR(logger, "Storage header is not consistent: " << chunkPos << " vs. " << _hdr.currPos);
        _hdr.currPos = chunkPos;
    }

    /* Run through removed arrays and try to remove the datastores (if they
       exist)
     */
    set<ArrayID>::iterator remit = removedArrays.begin();
    while (remit != removedArrays.end())
    {
        DataStore::DataStoreKey dsk(_dsnsid, *remit);
        _datastores->closeDataStore(dsk,
                                    true /* remove from disk */);
        ++remit;
    }

    /* Check chunkmap for overlaps...
     */
    checkExtentsForOverlaps(extents);
}

/* Read the storage description file to find path for chunk map file.
   Iterate the chunk map file and build the chunk map in memory.
 */
void
CachedStorage::open(const string& storageDescriptorFilePath, size_t cacheSizeBytes)
{
    /* read/create the storage description file
     */
    initStorageDescriptionFile(storageDescriptorFilePath);

    /* init cache
     */
    _cacheSize = cacheSizeBytes;
    _cacheUsed = 0;
    _cacheOverflowFlag = false;
    _timestamp = 1;
    _lru.prune();

    /* Open metadata (chunk map) file and transcation log file
     */
    int flags = O_LARGEFILE | O_RDWR | O_CREAT;
    _hd = FileManager::getInstance()->openFileObj(_databaseHeader.c_str(), flags);
    if (!_hd) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE) <<
            _databaseHeader << ::strerror(errno) << errno;
    }

    struct flock flc;
    flc.l_type = F_WRLCK;
    flc.l_whence = SEEK_SET;
    flc.l_start = 0;
    flc.l_len = 1;

    if (_hd->fsetlock(&flc))
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE);

    _logSize = 0;
    _currLog = 0;

    /* Initialize the data stores
     */
    _datastores = DataStores::getInstance();
    string dataStoresBase = _databasePath + "/datastores";
    _datastores->initDataStores(dataStoresBase.c_str());
    _dsnsid = _datastores->openNamespace("persistent");

    /* Read/initialize metadata header
     */
    size_t rc = _hd->read(&_hdr, sizeof(_hdr), 0);
    if (rc != 0 && rc != sizeof(_hdr)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "read" << ::strerror(errno) << errno;
    }

    if (rc == 0 || (_hdr.magic == SCIDB_STORAGE_HEADER_MAGIC && _hdr.currPos < HEADER_SIZE))
    {
        LOG4CXX_TRACE(logger, "smgr open:  initializing storage header");

        /* Database is not initialized
         */
        ::memset(&_hdr, 0, sizeof(_hdr));
        _hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
        _hdr.versionLowerBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.versionUpperBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.currPos = HEADER_SIZE;
        _hdr.instanceId = INVALID_INSTANCE;
        _hdr.nChunks = 0;
    }
    else
    {
        LOG4CXX_TRACE(logger, "smgr open:  opening storage header");

        /* Check for corrupted metadata file
         */
        if (_hdr.magic != SCIDB_STORAGE_HEADER_MAGIC)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);
        }

        /* At the moment, both upper and lower bound versions in the file must equal to the
           current version in the code.
         */
        if (_hdr.versionLowerBound != SCIDB_STORAGE_FORMAT_VERSION ||
            _hdr.versionUpperBound != SCIDB_STORAGE_FORMAT_VERSION)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION)
                  << _hdr.versionLowerBound
                  << _hdr.versionUpperBound
                  << SCIDB_STORAGE_FORMAT_VERSION;
        }

        /* Database is initialized: read information about all locally available chunks in map
         */
        initChunkMap();

        /* Flush the datastores to capture freelist changes
         */
        _datastores->flushAllDataStores();
    }
}


/* Cleanup and close smgr
 */
void
CachedStorage::close()
{
    InjectedErrorListener::stop();

    for (ChunkMap::iterator i = _chunkMap.begin(); i != _chunkMap.end(); ++i)
    {
        std::shared_ptr<InnerChunkMap> & innerMap = i->second;
        for (InnerChunkMap::iterator j = innerMap->begin(); j != innerMap->end(); ++j)
        {
            if (j->second.getChunk() && j->second.getChunk()->_accessCount != 0)
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_PIN_UNPIN_DISBALANCE);
        }
    }
    _chunkMap.clear();

    _hd.reset();
}

void CachedStorage::freeChunk(PersistentChunk* victim)
{
    ScopedMutexLock cs(_mutex, PTW_SML_STOR_H);
    internalFreeChunk(*victim);
}

void CachedStorage::internalFreeChunk(PersistentChunk& victim)
{
    if (victim._data != NULL && victim._hdr.pos.hdrPos != 0)
    {
        LOG4CXX_TRACE(logger, "CachedStorage::internalFreeChunk chunk=" << &victim
                      << ", size = "<< victim.getSize() << ", accessCount = "<<victim._accessCount
                      << ", cacheUsed="<<_cacheUsed);

        _cacheUsed -= victim.getSize();
        if (_cacheOverflowFlag)
        {
            _cacheOverflowFlag = false;
            _cacheOverflowEvent.signal();
        }
    }
    if (victim._next != NULL)
    {
        victim.unlink();
    }
    victim.free();
}

typedef std::pair<size_t,size_t> IndexRange;

/// Assert that the ranges are ordered, not overlapping, and dont cover maxSize elements
void validateRangesInDebug(const IndexRange* ranges,
                             const size_t nRanges, const size_t maxSize)
{
    if (isDebug()) {
        size_t used=0;
        for (size_t i=0; i < nRanges; ++i)  {
            SCIDB_ASSERT(ranges[i].second >= ranges[i].first);
            if (i>0) {
                SCIDB_ASSERT(ranges[i].first > ranges[i-1].second);
            }
            used += ranges[i].second-ranges[i].first+1;
        }
        SCIDB_ASSERT(nRanges < 1 || ranges[nRanges-1].second < maxSize);
        SCIDB_ASSERT(used < maxSize);
    }
}

/// Make sure the range @ curIndex is inserted in ascending order
void insertLastInOrder(IndexRange* ranges, size_t curIndex)
{
    SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
    while (curIndex > 0) {
        if (ranges[curIndex].first < ranges[curIndex-1].first) {
            ranges[curIndex].swap(ranges[curIndex-1]);
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            --curIndex;
        } else {
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            break;
        }
    }
}

/* Mark a chunk as free in the on-disk and in-memory chunk map.  Also mark it as free
   in the datastore.
 */
void CachedStorage::markChunkAsFree(InnerChunkMapEntry& entry, std::shared_ptr<DataStore>& ds)
{
    ChunkHeader header;
    std::shared_ptr<PersistentChunk>& chunk = entry.getChunk();

    if (!chunk)
    {
        /* Handle tombstone chunks
         */
        size_t rc = _hd->read(&header, sizeof(ChunkHeader), entry.getTombstonePos());
        if (rc != 0 && rc != sizeof(ChunkHeader)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
                << "read" << ::strerror(errno) << errno;
        }
    }
    else
    {
        /* Handle live chunks
         */
        memcpy(&header, &(chunk->_hdr), sizeof(ChunkHeader));
        if (ds)
            ds->freeChunk(chunk->_hdr.pos.offs, chunk->_hdr.allocatedSize);
    }

    /* Update header as free and write back to storage header file
     */
    header.arrId = 0;
    LOG4CXX_TRACE(chunkLogger,
                  "chunkl: markchunkasfree: free chunk descriptor at position "
                  << header.pos.hdrPos);
    _hd->writeAll(&header, sizeof(ChunkHeader), header.pos.hdrPos);
    assert(header.nCoordinates < MAX_NUM_DIMS_SUPPORTED);
    _freeHeaders.insert(header.pos.hdrPos);
}

/* Flush all changes to the physical device(s) for the indicated array.
   (optionally flush data for all arrays, if uaId == INVALID_ARRAY_ID).
*/
void
CachedStorage::flush(ArrayUAID uaId)
{
    int rc;

    /* flush the chunk map file
     */
    rc = _hd->fsync();
    if (rc != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "fsync" << ::strerror(errno) << errno;
    }

    /* flush the data store for the indicated array (or flush all datastores)
     */
    if (uaId != INVALID_ARRAY_ID)
    {
        DataStore::DataStoreKey dsk(_dsnsid, uaId);
        std::shared_ptr<DataStore> ds = _datastores->getDataStore(dsk);
        ds->flush();
    }
    else
    {
        _datastores->flushAllDataStores();
    }
}

}
