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
 * @file IndexMgr.h
 *
 * @author Steve Fridella, Donghui Zhang
 *
 * This class declares the interface of IndexMgr and DiskIndex.
 */

#ifndef INDEXMGR_H_
#define INDEXMGR_H_

#include <memory>
#include <unordered_map>
#include <stdlib.h>

#include <array/ArrayID.h>
#include <array/AttributeDesc.h>
#include <array/VersionID.h>
#include <storage/BufferMgr.h>
#include <storage/IndexMap.h>
#include <util/Mutex.h>
#include <util/PointerRange.h>
#include <util/compression/CompressorType.h>
#include <system/Exceptions.h>

namespace scidb {

class ChunkAuxMeta;

/**
 * An index backed by a DataStore, managing raw chunk data for both the DBArray
 * and the MemArray.
 *
 * The three components of a record are:
 *   - key: what's identifying a chunk; e.g. for MemArray it is attributeID, and
 *     chunkCoords.
 *   - value: the raw chunk data.
 *   - auxMeta: additional metadata about data in a chunk, such as compressionType, home instance, etc.
 *
 * Template parameters:
 *   - class KeyMeta: class which provides the type definition for the record
 *     key, as well as functions to compare, hash, and get the sizes and versions
 *     of keys.
 *
 * KeyMeta is a template paramater to the DiskIndex template.  It encapsulates
 * the interface that DiskIndex will use to manipulate keys. KeyMeta must
 * inherit from DiskIndexKeyMetaBase.  KeyMeta must provide a
 * type "KeyMeta::Key" which defines the structure of the Key.  KeyMeta::Key
 * must inherit from DiskIndexKeyMetaBase::KeyBase.
 *
 * KeyMeta must provide the following function objects:
 *
 * - keyLess:     A function object for two pointers to keys defining "less-than"
 *                Note:  keyLessBase(k1, k2) -> keyLess(k1, k2)
 * - keyEqual:    A function object for two pointers to keys defining "equality"
 *                Note:  !keyEqualBase(k1, k2) -> !keyEqual(k1, k2)
 * - keyVersion:  A function object for one pointer to a key returning the key version
 * - keySize:     A function object for one pointer to a key returning the key size
 *                (including the size of the KeyBase, returned by keySizeBase)
 * - keyToString: A function object for one pointer to a key returning a string
 * - keyMax:      A function object which returns a reference to a newly allocated
 *                copy of the maximum possible key for a given index, using the
 *                memory arena provided.
 */
template<class KeyMeta>
class DiskIndex : public std::enable_shared_from_this<DiskIndex<KeyMeta>>
{
public:
    /* Exception classes
     */
    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(KeyExistsException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_DISK_INDEX_KEY_EXISTS);
    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(KeyNotExistException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_DISK_INDEX_KEY_NOT_EXIST);
    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(BufferTooSmallException,
                                      SCIDB_SE_STORAGE,
                                      SCIDB_LE_DISK_INDEX_BUFFER_TOO_SMALL);

protected:
    typedef std::shared_ptr<typename IndexMap<KeyMeta>::Iterator> IMIteratorPtr;
    typedef std::shared_ptr<DiskIndex> DiskIndexPtr;

    /// the DataStore that manage the underlying disk file.
    DataStore::DataStoreKey _dsk;

    /// a KeyMeta object that is used to manipulate keys.
    KeyMeta _keyMeta;

    /// A map from key to chunk meta data information.
    IndexMap<KeyMeta>* _map;

    /// Mutex which protects the map (could be a read/write lock in future)
    mutable Mutex _mutex;

    /// The arena to allocate data from.
    arena::ArenaPtr _arena;

    /// Boolean indicating whether the data should be cleared from the
    /// disk upon destruction.
    bool _deleteDataOnDestroy;

public:
    /**
     * @return a pointer to the SciDB-facing interface to RocksDB.
     * NOTE:  This is deprecated.
     */
    IndexMap<KeyMeta>* getRocks()
    {
        return _map;
    }

    /**
     * An iterator that can be used to scan through values/auxMeta in the index.
     */
    class Iterator
    {
    public:
        /**
         * Move to the next key in the index
         */
        Iterator& operator++();

        /**
         * Does iterator point to end of index?
         */
        bool isEnd();

        /**
         * Return const reference to key
         */
        typename KeyMeta::Key const& getKey();

        /**
         * Return size of the associated value
         */
        size_t valueSize();

        /**
         * Return reference to chunk auxiliary meta record
         */
        const ChunkAuxMeta& getAuxMeta();

        /**
         * Return a reference to the buffer handle
         */
        const BufferMgr::BufferHandle& getBufHandle() const;

        /**
         * Default constructor creates empty iterator
         */
        Iterator()
            : _itp(NULL)
            , _index(NULL)
        {}

    private:
        /**
         * Constructor
         */
        Iterator(IMIteratorPtr itp, DiskIndexPtr index)
            : _itp(itp)
            , _index(index)
        {}

        IMIteratorPtr _itp;
        DiskIndexPtr _index;
        friend class DiskIndex<KeyMeta>;
    };

    /**
     * Wrapper structure for DiskIndex value.  Values tracked in a DiskIndex are
     * simply buffers.  Values can be in one of three states:
     *
     * 1) CallerPinned --- Buffer is pinned, but not entered in the index.
     *                     It won't be swapped and it is safe to access.
     *                     Must be unpinned/discarded on error.
     *
     * 2) IndexPinned --- Buffer is pinned and entered in the index.
     *                    It won't be swapped and it is safe to access.
     *                    Must be unpinned on error.
     *
     * 3) Unpinned --- Buffer is unpinned and may be swapped at any time,
     *                 or it may be a NULL buffer. It is not safe to access.
     *                 No special handling is necessary on error.
     *
     * The DiskIndexValue tracks the state and its destructor performs the
     * appropriate actions during cleanup.  Note: it is NOT POSSIBLE to
     * have an unpinned buffer that is not in the index.
     */
    class DiskIndexValue
    {
        friend class DiskIndex;

    public:
        enum State {
            CallerPinned,  ///< New entry pinned but not yet owned by IndexMap
            IndexPinned,   ///< Entry is pinned and belongs to the IndexMap
            Unpinned,      ///< Entry is unpinned and cannot be accessed
        };

        /**
         * Destructor, cleans up buffer if necessary
         */
        ~DiskIndexValue()
        {
            if (_state != Unpinned) {
                _diskIndex->_cleanupDiskIndexValue(*this);
            }
        }

        /**
         * Access the underlying data.
         * If buffer is not pinned, NULL pointer range returned.
         */
        PointerRange<char> const memory()
        {
            return (_state == Unpinned) ? PointerRange<char>() : _bufHandle.getRawData();
        }

        /**
         * Access the underlying data (for read only).
         * If buffer is not pinned, NULL pointer range returned.
         */
        PointerRange<const char> const constMemory()
        {
            return (_state == Unpinned) ? PointerRange<const char>()
                                        : _bufHandle.getRawConstData();
        }

        /**
         * Access the underlying compressed data .
         * If buffer is not pinned, NULL pointer range returned.
         */
        PointerRange<char> const compressionBuffer()
        {
            return (_state == Unpinned) ? PointerRange<char>()
                                        : _bufHandle.getCompressionBuffer();
        }

        /**
         * Return the current value state:  CallerPinned, IndexPinned, or
         * Unpinned.
         */
        State state() const { return _state; }

        /**
         * Unpin the underlying buffer.
         * @pre value must be in state "IndexPinned".
         */
        void unpin()
        {
            SCIDB_ASSERT(_state == IndexPinned);
            _bufHandle.unpinBuffer();
            _state = Unpinned;
        }

        /**
         * Return the size of the underlying buffer
         */
        size_t valueSize()
        {
            // method unused, to be removed under child of SDB-5924
            SCIDB_ASSERT(false);
            return _bufHandle.size();
        }

        /**
         * Return true if the value has a live buffer
         */
        bool isActive()
        {
            // SDB-6622, this method is still in use at this time
            // removing the SCIDB_ASSERT(false) that was here
            return !_bufHandle.isNull();
        }

        /**
         * Default Constructor --- creates unpinned empty value
         */
        DiskIndexValue(State st = Unpinned)
            : _state(st)
            , _diskIndex()
            , _bufHandle()
        {}

        BufferMgr::BufferHandle& getBufferHandle() { return _bufHandle; }


        void setCompressedSize(size_t newCompSize)
        {
            _bufHandle.setCompressedSize(newCompSize);
        }

    private:
        State _state;
        std::shared_ptr<DiskIndex> _diskIndex;
        BufferMgr::BufferHandle _bufHandle;
    };

    /**
     * The same constructor will be used both to open an existing array
     * and to create a new array.
     * @param[in] dsk        a unique identifier for the back-end file.
     * @param[in] keyMeta    a KeyMeta object to help manipulating keys.
     *
     * @note It is the caller's responsibility to provide KeyMeta object
     *       with the same properties, when creating the index and when
     *       opening the index at a later time.
     */
    DiskIndex(DataStore::DataStoreKey const& dsk, KeyMeta const& keyMeta);

    virtual ~DiskIndex();

    /**
     * @return the data store key of this index.
     */
    DataStore::DataStoreKey const& getDsk() const { return _dsk; }

    /**
     * @return a reference to the _keyMeta object.
     */
    KeyMeta const& getKeyMeta() const { return _keyMeta; }

    /**
     * @return the buffer manager arena
     */
    arena::ArenaPtr getArena() { return _arena; }

    /**
     * @param[in]  numBytes  how many bytes to allocate.
     * @param[out] value     a diskIndexValue object in "new" state.
     * @throws BuffMgr::{MemArrayThresholdExceeded, BufferMgrSlotsExceeded,ChunkSizeLimit}Exception
     */
    virtual void allocateMemory(size_t numBytes,
                                DiskIndexValue& value,
                                CompressorType compressionType);

    /**
     * Insert a new record.
     * @param[in]    key          the key of the record.
     * @param[in]    value        the value of the record.
     * @param[in]    auxMeta      the auxiliary chunk metadata of the record.
     * @param[in]    keepPinned   should the new value remain pinned after the call?
     * @param[in]    update       should record replace an existing value?
     * @returns if update is false, and key is already present in index,
     *          false.  Otherwise, returns true.
     * @note If update is true, then the value will replace any existing value
     *       for the key, and be inserted if no value exists for the key.
     */
    virtual bool insertRecord(typename KeyMeta::Key const* key,
                              DiskIndexValue& value,
                              const ChunkAuxMeta& auxMeta,
                              bool keepPinned,
                              bool update);

    /**
     * Delete an existing record.
     * @param[in]    iter iterator pointing to record to delete
     * @throw KeyNotExist if iter is invalid or points to end of index
     * @post record is no longer in index and iterator is invalid
     */
    virtual void deleteRecord(Iterator& iter);

    /**
     * Delete an existing record.
     * @param[in]    key  pointer to key matching record to delete
     * @post record is no longer in index, iterators that previously
     *       pointed to it are no longer valid
     */
    virtual void deleteRecord(typename KeyMeta::Key const* key);

    /**
     * Return an iterator for the first record in the index
     * @return an iterator pointing to the first record
     * @note inserting/deleting a record pointed to by an iterator
     *       invalidates the iterator
     */
    virtual Iterator begin();

    /**
     * Return an iterator for the requested key
     * @param[in]  key  the key of the record to find
     * @return an iterator pointing to the record with the requested key,
     *         or pointing to the 'end' of the index if the key does not exist
     */
    virtual Iterator find(typename KeyMeta::Key const* key);

    /**
     * Return an iterator for the smallest element of the index >= to key
     * @param[in]  key  the key of the target record
     * @return an iterator pointing to the record which has the smallest
     *         key in the index that is >= to k, or 'end' if k is greater
     *         than all records in the index.
     */
    virtual Iterator leastUpper(typename KeyMeta::Key const* key);

    /**
     * Pin the value of a record in memory.
     * @param[in]    key        key of the record to pin.
     * @param[out]   value      a DiskIndexValue object in the "pinned" state.
     * @throw KeyNotExist if no entry for "key" exists
     * @note the value of entry for "key" will be returned and pinned.
     */
    virtual void pinValue(typename KeyMeta::Key const* key,
                          DiskIndexValue& value);

    /**
     * Pin the value of a record in memory.
     * @param[in]    iter       iterator pointing to the desired record.
     * @param[out]   value      a DiskIndexValue object in the "pinned" state.
     * @note the value of entry will be returned and pinned.
     */
    virtual void pinValue(Iterator& iter, DiskIndexValue& value);

    /**
     * Ensure all data and metadata associated with an array version are stable
     * @param vaid  versioned array id to flush
     */
    virtual void flushVersion(ArrayID vaid);

    /**
     * Remove all records associated with a particular version
     * @param vaid      versioned array id to rollback
     * @param lastv     previous VersionID of array to rollback ('n' in A@n)
     */
    virtual void rollbackVersion(ArrayID vaid, VersionID lastv);

    /**
     * Set the index to delete all associated buffers when the destructor
     * fires
     */
    virtual void deleteDataOnDestroy() { _deleteDataOnDestroy = true; }

    /**
     * Infrastructure to list entries in the disk index
     */
    using DiskIndexVisitor =
        std::function<void(typename KeyMeta::Key const*, BufferMgr::BufferHandle&, const ChunkAuxMeta&)>;

    virtual void visitDiskIndex(const DiskIndexVisitor& visit) const;

protected:  // Internal helper routines that don't use the mutex, assuming caller already acquired necessary locks.
    friend class DiskIndexValue;

    /**
     * @param[in]  value  a DiskIndexValue object that needs to be cleaned up
     */
    virtual void _cleanupDiskIndexValue(DiskIndexValue& value);

    /**
     * Create the appropriate chunkmap instance for the disk index
     * @param[in]  dsk      dataStoreKey for the index
     * @param[in]  keyMeta  instance of KeyMeta for the index
     * @param[in]  arena    arena used for the index
     */
    virtual IndexMap<KeyMeta>* constructIndexMap(DataStore::DataStoreKey dsk,
                                                 KeyMeta const& keyMeta,
                                                 arena::ArenaPtr arena);

private:
    void deleteRecordInternal(IMIteratorPtr iteratorPtr);
    void pinValueInternal(Iterator& iter, DiskIndexValue& value);
};

/**
 * The base class used by all callers for the KeyMeta template
 * parameter of DiskIndex.
 */
class DiskIndexKeyMetaBase
{
public:
    /**
     * A structure that describes the base shared by all keys
     */
    class KeyBase
    {
    public:
        KeyBase(DataStore::DataStoreKey const& dsk)
            : _dsk(dsk)
        {}

        DataStore::DataStoreKey const& dsk() const { return _dsk; }

        void makePredecessor() { _dsk = _dsk.predecessor(); }

    private:
        DataStore::DataStoreKey _dsk;  // used to identify
        // disk index for this key

    protected:
        AttributeID _encodeAttributeID(const AttributeDesc& attr) const;
        AttributeID _decodeAttributeID(AttributeID attrId,
                                       const AttributeDesc* ebm) const;
    };

    /**
     * A less-than function object for two KeyBases (given pointers to them).
     */
    struct KeyLessBase
    {
    public:
        bool operator()(KeyBase const* key1, KeyBase const* key2) const
        {
            return key1->dsk() < key2->dsk();
        }
    };

    /**
     * A equal-to function object for two KeyBases (given pointers to them).
     */
    struct KeyEqualBase
    {
    public:
        bool operator()(KeyBase const* key1, KeyBase const* key2) const
        {
            return key1->dsk() == key2->dsk();
        }
    };

    /**
     * A function object returning a string serialization of the keybase
     */
    struct KeyToStringBase
    {
    public:
        std::string operator()(KeyBase const* key) const
        {
            return key->dsk().toString();
        }
    };

public:  // key-manipulation function objects.
    // clang-format off
    KeyLessBase     const keyLessBase = KeyLessBase();
    KeyEqualBase    const keyEqualBase = KeyEqualBase();
    KeyToStringBase const keyToStringBase = KeyToStringBase();
    // clang-format on
};

/**
 * IndexMgr manages multiple indexes.
 *
 * The index manager class maintains a map of DiskIndex objects for a
 * particular instantiation of DiskIndex.  Each different instantiation
 * of DiskIndex results in a corresponding Singleton IndexMgr object
 * which manages all the DiskIndexes for that particular KeyMeta type.
 *
 * Note however, that the manager may not have all existing on-disk
 * indexes at once, since a DiskIndex is only entered into the
 * map when it is created or opened from disk (and it previously had
 * no entry).  It does not try to find all existing DiskIndexes upon
 * startup, because there is currently no general way to query which
 * elements of the dsk space are currently in use.  Thus older disk
 * indexes that have not been accessed during the current instantiation
 * of the server process will not appear in the map.
 *
 * TODO: in the future metadata about all DiskIndexes that are
 * currently on-disk can be saved in a format that the manager can
 * use to query upon startup and load any or all of them.
 */
template<class KeyMeta>
class IndexMgr : public Singleton<IndexMgr<KeyMeta>>
{
public:
    using IndexMap =
        std::unordered_map<DataStore::DataStoreKey, std::shared_ptr<DiskIndex<KeyMeta>>, DataStore::DataStoreKeyHash>;

protected:
    IndexMap _indexMap;
    mutable Mutex _mutex;

public:
    IndexMgr();

    /**
     * Get a DiskIndex, open it if necessary
     * @param[out] index      a new DiskIndex.
     * @param[in]  dsk        the data store key of an existing file.
     * @param[in]  keyMeta    a KeyMeta object to help manipulating keys.
     */
    void getIndex(std::shared_ptr<DiskIndex<KeyMeta>>& index,
                  DataStore::DataStoreKey dsk,
                  KeyMeta const& keyMeta)
    {
        if (!findIndex(index, dsk)) {
            openIndex(index, dsk, keyMeta);
        }
    }

    /**
     * Open a DiskIndex.
     * @param[out] index      a new DiskIndex.
     * @param[in]  dsk        the data store key of an existing file.
     * @param[in]  keyMeta    a KeyMeta object to help manipulating keys.
     * @pre  index must not be already opened
     */
    void openIndex(std::shared_ptr<DiskIndex<KeyMeta>>& index,
                   DataStore::DataStoreKey dsk,
                   KeyMeta const& keyMeta);

    /**
     * Find an open DiskIndex.
     * @param[out] index the located index.
     * @param[in]  dsid  the dsid of the index to search for.
     * @return whether the index is found.
     */
    bool findIndex(std::shared_ptr<DiskIndex<KeyMeta>>& index, DataStore::DataStoreKey dsk);

    /**
     * Close an index and remove the entry from the indexmap.
     * @param[in]  index     the index to close.
     * @param[in]  destroy   if true, destroy the index (destroyed
     *                       index may not be opened later).
     * @return the iterator in the indexMap to the element after the index being closed.
     * @pre index must be previously opened
     */
    typename IndexMap::iterator closeIndex(std::shared_ptr<DiskIndex<KeyMeta>> const& index, bool destroy = false);

    /**
     * Visit each disk index in the manager
     * @param visit The visitor function to call on each index
     * @param residentOnly Only visit the disk indexes already resident in the index map
     */
    void visitDiskIndexes(const typename DiskIndex<KeyMeta>::DiskIndexVisitor& visit,
                          bool residentOnly);

private:
    /**
     * Add All DataStore Keys (arrayIDs) to the _indexMap. Key/Value pairs already in the
     * index map are not modified, but keys which have not yet been loaded will be added.
     */
    void addMissingDataStoreKeys();
};

}  // namespace scidb

#endif /* INDEXMGR_H_ */
