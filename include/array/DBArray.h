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
 * DBArray.h
 *
 *  Created on: 2.28.2017
 *      Author: sfridella@paradigm4.com
 *      Description: New persistent array implementation
 */

#ifndef DBARRAY_H_
#define DBARRAY_H_

#include <vector>
#include <query/Query.h>
#include <array/AddressMeta.h>
#include <array/ArrayIterator.h>
#include <storage/IndexMgr.h>
#include <array/Array.h>
#include <array/ArrayDesc.h>

namespace scidb {

class CachedDBChunk;

/**
 * Persistent array.
 */
class DBArray
    : public Array
{
    friend class CachedDBChunk;
    friend class DBArrayIterator;

    typedef DiskIndex<DbAddressMeta> DBDiskIndex;
    typedef IndexMgr<DbAddressMeta> DBIndexMgr;

    /* The disk index that manages the raw-chunk data for the array.
     */
    std::shared_ptr<DBDiskIndex> _diskIndex;

public:
    virtual ArrayDesc const& getArrayDesc() const;

    std::shared_ptr<ArrayIterator> getIteratorImpl(const AttributeDesc& attId) override;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attId) const override;

    /**
     * @see Array::isMaterialized()
     */
    bool isMaterialized() const override { return true; }

    /**
     * @see Array::removeDeadChunks
     */
    void removeDeadChunks(std::shared_ptr<Query>& query,
                          std::set<Coordinates, CoordinatesLess> const& liveChunks)
        override;

    /**
     * @see Array::removeLocalChunk
     */
    void removeLocalChunk(std::shared_ptr<Query> const& query,
                          Coordinates const& coords)
        override;

    /**
     * @see Array::removeVersions
     */
    void removeVersions(std::shared_ptr<Query>& query, ArrayID lastLiveArrId)
        override;

    /**
     * @see Array::flush
     */
    void flush() override;

    /**
     * Destructor
     */
    virtual ~DBArray();

    /**
     * Create a new DBArray instance
     */
    static std::shared_ptr<DBArray> createDBArray(ArrayDesc const& desc,
                                                  const std::shared_ptr<Query>& query)
    {
        return std::shared_ptr<DBArray>(new DBArray(desc, query));
    }

    /**
     * Rollback the indicated version for the array specified
     * by the arrayId.  If the last remaining version is zero,
     * delete the array from disk.
     * @param lastVersion highest version left in array after rollback
     * @param baseArrayId unversioned array id of target array
     * @param newArrayId  versioned array id of version to rollback
     */
    static void rollbackVersion(VersionID lastVersion, ArrayID baseArrayId, ArrayID newArrayId);

protected:
    DBArray(ArrayDesc const& desc, const std::shared_ptr<Query>& query);
    DBArray();
    DBArray(const DBArray& other);
    DBArray& operator=(const DBArray& other);

    /**
     * Callback to be implemented by test code primarily, invoked by
     * removeEarlierVersions just before a chunk is removed from the
     * disk index.
     *
     * @param dbckey The key pertaining to the chunk about to be removed.
     */
    virtual void onRemoveChunk(const DbAddressMeta::Key& dbckey)
    {
        // Intentionally do nothing, by default.
    }

private:
    void makeChunk(PersistentAddress const& addr,
                   CachedDBChunk*& chunk,
                   CachedDBChunk*& bitmapchunk,
                   bool newChunk);
    DBDiskIndex& diskIndex() const { return *_diskIndex; }
    void pinChunk(CachedDBChunk const& chunk);
    void unpinChunk(CachedDBChunk const& chunk);
    void cleanupChunkRecord(CachedDBChunk const& chunk);
    void removeLocalChunkLocked(std::shared_ptr<Query> const& query, Coordinates const& coords);

    /**
     * Remove all versions prior to lastLiveArrId from the array.
     *
     * @param query shared pointer to query context
     * @param lastLiveArrId the Versioned Array ID of last version to preserve
     */
    void removeEarlierVersions(std::shared_ptr<Query>& query, const ArrayID lastLiveArrId);

    /**
     * Removes all versions of the array.
     *
     * @param query shared pointer to query context
     */
    void removeAllVersions(std::shared_ptr<Query>& query);

private:
    ArrayDesc _desc;
    Mutex _mutex;
};

/**
 * Class which tracks unique ids for persistent arrays
 */
class DBArrayMgr : public Singleton<DBArrayMgr>
{
public:
    /**
     * Constructor
     */
    DBArrayMgr()
        : _nsid(0)
    {
        _nsid = DataStores::getInstance()->openNamespace("persistent");
    }

    /**
     * Return a unique data store key to use for next DBArray
     */
    DataStore::DataStoreKey getDsk(ArrayID uaId)
    {
        DataStore::DataStoreKey dsk(_nsid, uaId);
        return dsk;
    }
    DataStore::DataStoreKey getDsk(ArrayDesc const& desc)
    {
        return getDsk(desc.getUAId());
    }

private:
    // clang-format off
    DataStore::NsId       _nsid;       // name space id for datastores
    // clang-format on
};

/**
 * Persistent array iterator
 */
class DBArrayIterator : public ArrayIterator
{
private:
    // clang-format off
    DBArray::DBDiskIndex::Iterator _curr;
    DBArray::DBDiskIndex::Iterator _currBitmap;
    std::shared_ptr<DBArray>       _array;
    PersistentAddress                 _addr;
    CachedDBChunk*                    _currChunk;
    CachedDBChunk*                    _currBitmapChunk;
    bool                              _positioned;
    // clang-format on

    /* Utility functions - not part of public interface
     */

    /* Resets the iteration if no position is defined
     */
    void position()
    {
        if (!_positioned) {
            restart();
        }
    }

    /* Reset _addr to first possible element
     */
    void resetAddrToMin();

    /* Clear out the chunk references and unset "positioned"
     */
    void resetChunkRefs();

    /* Advance chunk and bitmap iters one step in the map and update
       curAddr.
     */
    bool advanceIters(PersistentAddress& curAddr, PersistentAddress const& oldAddr);

    /* Find the chunk at the next logical address which differs
       from addr and with the requested version
     */
    void findNextLogicalChunk(PersistentAddress& addr,
                              const ArrayID& targetVersion,
                              std::shared_ptr<Query> query);

    /* Make the chunk objects for the current chunk iterators
     */
    void setCurrent();

public:
    /**
     * Public interface -- see ArrayIterator
     */
    DBArrayIterator(std::shared_ptr<DBArray> arr, AttributeID attId);
    ~DBArrayIterator();
    ConstChunk const& getChunk() override;
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    Chunk& newChunk(Coordinates const& pos) override;
    Chunk& newChunk(Coordinates const& pos, CompressorType compressionMethod) override;
    void deleteChunk(Chunk& chunk);
    virtual std::shared_ptr<Query> getQuery()
    {
        return Query::getValidQueryPtr(_array->_query);
    }
};

}  // namespace scidb
#endif
