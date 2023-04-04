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

/*
 * InternalStorage.h
 *
 *  Created on: 06.01.2010
 *      Author: knizhnik@garret.ru
 *              sfridella@paradigm4.com
 *      Description: Internal storage manager interface
 */

#ifndef INTERNAL_STORAGE_H_
#define INTERNAL_STORAGE_H_

#include <dirent.h>
#include <map>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>

#include "Storage.h"
#include "PersistentChunk.h"
#include <array/MemArray.h>
#include <util/DataStore.h>
#include <util/Event.h>
#include <util/RWLock.h>
#include <util/ThreadPool.h>
#include <query/Query.h>
#include <util/InjectedError.h>
#include <system/Constants.h>
#include <util/compression/Compressor.h>

namespace scidb
{
    /**
     * Transaction log record
     */
    struct TransLogRecordHeader {
        ArrayUAID      arrayUAID;
        ArrayID        arrayId;
        VersionID      version;
        uint32_t       oldSize;
        uint64_t       newHdrPos;
        ChunkHeader hdr;

        TransLogRecordHeader() :
        arrayUAID(0),
        arrayId(0),
        version(0),
        oldSize(0),
        newHdrPos(0),
        hdr() {}
    };

    struct TransLogRecord : TransLogRecordHeader {
        uint32_t    hdrCRC;
        uint32_t    bodyCRC;

        TransLogRecord()
            : hdrCRC(0),
              bodyCRC(0) {}
    };

    /**
     * Storage with LRU in-memory cache of chunks
     */
    class CachedStorage : public Storage, InjectedErrorListener
    {
    //Inner Structures
    private:
        struct ChunkInitializer
        {
            CachedStorage& storage;
            PersistentChunk& chunk;

            ChunkInitializer(CachedStorage* sto, PersistentChunk& chn) : storage(*sto), chunk(chn) {}
            ~ChunkInitializer();
        };

        /**
         * Entry in the inner chunkmap.  It is either a) a shared pointer to a persistent chunk, or
         * b) a tombstone.  If it is a tombstone, the chunk pointer will be NULL and the position
         * of the tombstone descriptor will be stored.
         */
        class InnerChunkMapEntry
        {
        public:
            enum Status {
                NORMAL = 2,
                INVALID = 4,
                TOMBSTONE = 8
            };

            InnerChunkMapEntry() :
                _status(NORMAL),
                _hdrpos(0)
                {}

            /**
             * Return pointer to chunk
             */
            std::shared_ptr<PersistentChunk>& getChunk()
                {
                    assert(_status == NORMAL || _chunk == NULL);
                    return _chunk;
                }

            const std::shared_ptr<PersistentChunk>& getChunk() const
                {
                    assert(_status == NORMAL || _chunk == NULL);
                    return _chunk;
                }

            /**
             * Is this a tombstone?
             */
            bool isTombstone() const
                { return (_status != NORMAL); }

            /**
             * Set the tombstone position
             * @param pos new tombstone position
             */
            void setTombstonePos(Status st, uint64_t pos)
                {
                    assert(st != NORMAL);
                    _status = st;
                    _hdrpos = pos;
                }

            /**
             * Return position of tombstone
             */
            uint64_t getTombstonePos() const
                {
                    assert(_status != NORMAL);
                    return _hdrpos;
                }

            /**
             * Is entry valid?
             */
            bool isValid() const
                { return (_status != INVALID); }

        private:

            /* An entry is either:
               NORMAL (pointer to persistent chunk)
               INVALID (treated as tomstone in memory, but considered corrupt
                        on disk)
               TOMBSTONE (no chunk present at this location, but the position
                          of descriptor is stored)
             */
            Status                           _status;
            uint64_t                         _hdrpos;
            std::shared_ptr<PersistentChunk> _chunk;
        };

    private:

        // Data members

        union
        {
            StorageHeader _hdr;
            char          _filler[HEADER_SIZE];
        };

        DataStore::NsId _dsnsid;  // namespace id for persistent arrays
        DataStores* _datastores;  // ref to manager of data store objects

        typedef std::map<StorageAddress, InnerChunkMapEntry> InnerChunkMap;
        typedef std::unordered_map<ArrayUAID, std::shared_ptr< InnerChunkMap > > ChunkMap;
        typedef std::tuple<DataStore::DsId, off_t, size_t, off_t> ChunkExtent;
        typedef std::set<ChunkExtent> Extents;

        ChunkMap _chunkMap;  // The root of the chunk map

        size_t _cacheSize;    // maximal size of memory used by cached chunks
        size_t _cacheUsed;    // current size of memory used by cached chunks
                              // (it can be larger than cacheSize if all chunks are pinned)
        Mutex mutable _mutex; // mutex used to synchronize access to the storage
        Event _loadEvent;     // event to notify threads waiting for completion of chunk load
        PersistentChunk _lru; // header of LRU L2-list
        uint64_t _timestamp;

        bool _strictCacheLimit;
        bool _cacheOverflowFlag;
        Event _cacheOverflowEvent;

        int32_t _writeLogThreshold;

        std::string _databasePath;   // path to db directory
        std::string _databaseHeader; // path of chunk header file
        std::string _databaseLog;    // path of log file (prefix)
        File::FilePtr _hd;           // storage header file descriptor
        File::FilePtr _log[2];       // _transaction logs
        uint64_t _logSizeLimit;      // transaciton log size limit
        uint64_t _logSize;
        int _currLog;
        bool _redundancyEnabled;
        bool _enableDeltaEncoding;
        bool _enableChunkmapRecovery;
        bool _skipChunkmapIntegrityCheck;

        RWLock _latches[N_LATCHES];  //XXX TODO: figure out if latches are necessary after removal of clone logic
        std::set<uint64_t> _freeHeaders;

        // Methods

        /**
         * Initialize/read the Storage Description file on startup
         */
        void initStorageDescriptionFile(const std::string& storageDescriptorFilePath);

        /**
         * Initialize the chunk map from on-disk store
         */
        void initChunkMap();

        /**
         * Record an extent in the extent map
         */
        void recordExtent(Extents& extents,
                          std::shared_ptr<PersistentChunk>& chunk);

        /**
         * Erase an extent from the extent map
         */
        void eraseExtent(Extents& extents,
                         std::shared_ptr<PersistentChunk>& chunk);

        /**
         * Check extent map for overlaps on disk.  If in "recovery mode"
         * replace overlapping chunks with tombstones.  If not in "recovery
         * mode" throw exception.  Delete extent map when done.
         */
        void checkExtentsForOverlaps(Extents& extents);

        /**
         * Mark a chunk as free in the on-disk and in-memory chunk map.  Also mark it as free
         * in the datastore ds if provided.
         */
        void markChunkAsFree(InnerChunkMapEntry& entry, std::shared_ptr<DataStore>& ds);

        void internalFreeChunk(PersistentChunk& chunk);

    public:
        /**
         * Constructor
         */
        CachedStorage();

        /**
         * Cleanup and close smgr
         * @see Storage::close
         */
        void close();

        /**
         * @see Storage::rollback
         */
        void rollback(RollbackMap const& undoUpdates);

        /**
         * Read the storage description file to find path for chunk map file.
         * Iterate the chunk map file and build the chunk map in memory.  TODO:
         * We would like to be able to initialize the chunk map without iterating
         * the file.  In general the entire chunk map should not be required to
         * fit entirely in memory.  Chage this in the future.
         * @see Storage::open
         */
        void open(const std::string& storageDescriptorFilePath, size_t cacheSize);

        /**
         * Flush all changes to the physical device(s) for the indicated array.
         * (optionally flush data for all arrays, if uaId == INVALID_ARRAY_ID).
         * @see Storage::flush
         */
        void flush(ArrayUAID uaId = INVALID_ARRAY_ID);

        /**
         * @see Storage::removeDeadChunks
         */
        void freeChunk(PersistentChunk* chunk);

        static CachedStorage instance;
    };

}

#endif
