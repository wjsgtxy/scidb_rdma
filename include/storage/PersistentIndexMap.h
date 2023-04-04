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

/*
 * PersistentIndexMap.h
 *
 * @author Steve Fridella
 *
 *
 */

#ifndef PERSISTENT_INDEXMAP_H_
#define PERSISTENT_INDEXMAP_H_

#include <array/ChunkAuxMeta.h>
#include <system/Utils.h>
#include <storage/IndexMap.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/compaction_filter.h>
#include <util/Singleton.h>
#include <util/DataStore.h>
#include <util/Mutex.h>
#include <util/Arena.h>

namespace scidb {

/**
 * RocksIndexMap is an implementation of IndexMap which stores the map
 * in a Rocksdb database.
 *
 * Template parameters:
 *   - class KeyMeta: class which provides the type definition for the record
 *     key, as well as functions to compare, hash, and get the sizes and versions
 *     of keys.
 */
template<class KeyMeta>
class RocksIndexMap : public IndexMap<KeyMeta>
{
public:
    using BaseIterator = typename IndexMap<KeyMeta>::Iterator;
    using Key = typename KeyMeta::Key;

    /**
     * Class used by rocksdb to compare keys in the map
     */
    class KeyComp : public rocksdb::Comparator
    {
    public:
        /**
         * View a slice as a Key
         */
        static Key const& SliceAsKey(const rocksdb::Slice& slice)
        {
            return *(reinterpret_cast<Key const*>(slice.data()));
        }

        /**
         * View a Key as a slice
         */
        static void KeyAsSlice(Key const& key, rocksdb::Slice& slice)
        {
            KeyMeta km;
            rocksdb::Slice newSlice(reinterpret_cast<char const*>(&key), km.keySize(&key));
            slice = newSlice;
        }

        /** Three-way comparison.  Returns value:
         *   < 0 iff "a" < "b",
         *  == 0 iff "a" == "b",
         *  > 0 iff "a" > "b"
         */
        int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const
        {
            Key const& keya = SliceAsKey(a);
            Key const& keyb = SliceAsKey(b);
            KeyMeta km;
            return km.keyLess(&keya, &keyb) ? -1 : km.keyLess(&keyb, &keya) ? 1 : 0;
        }

        const char* Name() const { return "KeyMetaComparator"; }
        void FindShortestSeparator(std::string*, const rocksdb::Slice&) const {}
        void FindShortSuccessor(std::string*) const {}
    };

    /**
     * Value in the rocksdb map
     */
    class Entry
    {
    public:
        /**
         * Allocate a new value for the rocksdb map, copying
         * the provided contents
         */
        static Entry* makeEntry(arena::ArenaPtr arena,
                                BufferMgr::BufferHandle const& value,
                                const ChunkAuxMeta& auxMeta);
        /**
         * View an Entry as a slice
         */
        static void EntryAsSlice(Entry& entry, rocksdb::Slice& slice);

        BufferMgr::BufferHandle const& getBufHandle() const { return _value; }

        const ChunkAuxMeta& getAuxMeta() const { return _auxMetaData; }

        size_t size() const { return sizeof(Entry) + sizeof(ChunkAuxMeta); }

    private:
        // clang-format off
        BufferMgr::BufferHandle _value;          // buffer handle
        ChunkAuxMeta            _auxMetaData;    // opaque chunk auxiliary metadata buffer
        // clang-format on
    };

    /**
     * Class used to iterate elements of the map
     */
    class RocksIterator : public BaseIterator
    {
    public:
        /**
         * Access elements
         */
        Key const* getKey() override;
        BufferMgr::BufferHandle& getBufHandle() override;
        const ChunkAuxMeta& getAuxMeta() override;

        /**
         * Advance to next element
         */
        BaseIterator& operator++() override;

        /**
         * Compare elements
         */
        bool isEnd() const override;

        /**
         * Query value properties
         */
        size_t valueSize() const override;

        /**
         * Constructor
         */
        RocksIterator(rocksdb::Iterator* iter, RocksIndexMap* map)
            : _map(map)
            , _isBegin(false)
            , _iter(iter)
        {}

        /**
         * Destructor
         */
        ~RocksIterator() { delete _iter; }

    private:
        RocksIndexMap* _map;       // reference to parent map
        bool _isBegin;             // is iterator at start?
        rocksdb::Iterator* _iter;  // ref to underlying rocksdb iterator

        friend RocksIndexMap;
    };

public:
    /**
     * Constructor/Destructor
     */
    RocksIndexMap(DataStore::DataStoreKey const& dsk,
                  KeyMeta const& keyMeta,
                  arena::ArenaPtr const& arena);
    ~RocksIndexMap();

    /**
     * Erase map
     */
    void clear() override;

    /**
     * Find an element in the map
     */
    std::shared_ptr<BaseIterator> find(Key const* key) override;

    /**
     * Find least upper bound of an element in the map
     */
    std::shared_ptr<BaseIterator> leastUpper(Key const* key) override;

    /**
     * Return first element in the map
     */
    std::shared_ptr<BaseIterator> begin() override;

    /**
     * Insert the entry in the proper place in the map
     * If the key exists, return false and do nothing
     */
    bool insert(Key const* key,
                BufferMgr::BufferHandle& value,
                const ChunkAuxMeta& auxMeta) override;

    /**
     * Update the entry in the proper place in the map
     *   and return the previous value.
     * If the key does not exist, insert the entry as new
     *   and return a null value.
     */
    BufferMgr::BufferHandle update(Key const* key,
                                   BufferMgr::BufferHandle& value,
                                   const ChunkAuxMeta& auxMeta) override;

    /**
     * Erase the indicated entry
     */
    void erase(BaseIterator& it) override;

    /**
     * Erase the record with matching key
     */
    void erase(Key const* key) override;

    /**
     * Ensure all current entries are stable on disk
     */
    void flush() override;

    /**
     * Remove all entries with the indicated version #
     * @pre version # must be the latest version
     */
    void rollbackVersion(size_t version) override;

    /**
     * Ask RocksDB to schedule a full compaction.
     */
    static void runFullCompaction();

private:
    /**
     * Forward declaration.  See below.
     */
    class RocksDbManager;

    /**
     * Class which manages compaction filters and tracks the
     * recently deleted arrays.
     */
    class ScidbCompactFactory : public rocksdb::CompactionFilterFactory
    {
    public:
        /**
         * Construction/destruction
         */
        ScidbCompactFactory(RocksDbManager* dbm, arena::ArenaPtr& arena);
        ~ScidbCompactFactory();

        /**
         * Create a ScidbCompact object when requested by the compaction
         * thread.
         */
        std::unique_ptr<rocksdb::CompactionFilter>
        CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context);

        /**
         * Add the dsk to the set of deleted indexes
         */
        void addDeletedIndex(DataStore::DataStoreKey& dsk);

        /**
         * Return true if dsk is in deleted set
         */
        bool isDeletedIndex(DataStore::DataStoreKey& dsk);

        /**
         * Check whether each index in the deleted set is completely deleted
         */
        void checkDeletedSet();

        /**
         * Return label identifying this factory
         */
        const char* Name() const { return "ScidbCompactFactory"; }

    private:
        // a set of recently deleted indexes
        std::set<DataStore::DataStoreKey> _deletedIndexes;

        // clang-format off
        Mutex             _deleteMutex;   // protects the deleted index set
        RocksDbManager*   _dbm;           // ref to parent manager
        KeyMeta           _keyMeta;       // used to find ranges of indexes
        arena::ArenaPtr   _arena;         // needed to manage memory
        // clang-format on
    };

    /**
     * Class used to clean up entries from the db after a disk
     * index is cleared.
     */
    class ScidbCompact : public rocksdb::CompactionFilter
    {
    public:
        /**
         * Virtual function that we override to provide our own
         * compaction behavior.  We will check if the key belongs
         * to a deleted disk index, and if so, drop it.
         */
        bool Filter(int level,
                    const rocksdb::Slice& key,
                    const rocksdb::Slice& existing_value,
                    std::string* new_value,
                    bool* value_changed) const override;

        /**
         * Constructor/destructor
         */
        ScidbCompact(ScidbCompactFactory* fac)
            : _nKeyExamined(0)
            , _nKeyFiltered(0)
            , _savedRes(false)
            , _fac(fac)
        {}

        ~ScidbCompact();

        const char* Name() const
        {
            // method unused, to be removed under child of SDB-5924
            SCIDB_ASSERT(false);
            return "ScidbCompact";
        }

    private:
        // clang-format off
        mutable int                     _nKeyExamined;
        mutable int                     _nKeyFiltered;
        mutable DataStore::DataStoreKey _savedDsk; // dsk of previous key
        mutable bool                    _savedRes; // result of previous filter

        ScidbCompactFactory*    _fac;      // ref to parent factory
        // clang-format on
    };

    /**
     * Private class used to manage rocks dbs and connections
     */
    class RocksDbManager : public Singleton<RocksDbManager>
    {
    public:
        /**
         * Constructor
         */
        RocksDbManager();

        /**
         * Destructor
         */
        ~RocksDbManager();

        /**
         * Return a connection to the db
         */
        rocksdb::DB* getDbConnection();

        /**
         * Return a reference to the compaction filter factory for this
         * dsk
         */
        ScidbCompactFactory& getCompactFac(const DataStore::DataStoreKey& _dsk);

    private:
        // clang-format off
        KeyComp                    _kc;        // comparator object for the db
        rocksdb::DB*               _dbConn;    // rocksdb connection

        // used to clean up old entries
        std::shared_ptr<ScidbCompactFactory> _dbCompact;
        // clang-format on
    };

    // clang-format off
    DataStore::DataStoreKey  _dsk;         // id of datastore for this index
    KeyMeta                  _keyMeta;     // container of key comp functions
    arena::ArenaPtr          _arena;       // arena managing index memory
    typename KeyMeta::Key*   _keyMax;      // max possible key for index
    typename KeyMeta::Key*   _keyMaxPred;  // max key for predecessor index
    rocksdb::DB*             _dbConn;      // connection to md database
    // clang-format on
};
}  // namespace scidb
#endif /* PERSISTENT_INDEXMAP_H_ */
