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
 * @file MemArray.h
 *
 * @brief In-Memory (temporary) array implementation
 */

#ifndef MEM_ARRAY_H_
#define MEM_ARRAY_H_

#include <array/ArrayDesc.h>
#include <array/CachedTmpChunk.h>
#include <array/AddressMeta.h>
#include <vector>
#include <map>
#include <atomic>

#include <array/Array.h>
#include <array/ArrayIterator.h>
#include <array/Tile.h>

#include <monitor/MonitorConfig.h>
#include <query/Query.h>
#include <storage/IndexMgr.h>
#include <util/CoordinatesMapper.h>
#include <util/DataStore.h>
#include <util/Singleton.h>

namespace scidb
{
    /**
     * An array that is similar to DBArray in that data may spill to disk,
     * but different from DBArray in that its life time is only within a query.
     * @see the note at the beginning of the file (that this is a server-only concept).
     */
    class MemArray : public Array
    {
        friend class CachedTmpChunk;
        friend class MemArrayIterator;

        typedef DiskIndex<MemAddressMeta> MemDiskIndex;
        typedef IndexMgr<MemAddressMeta> MemIndexMgr;

        /// The disk index that manages the raw-chunk data for the MemArray.
        std::shared_ptr<MemDiskIndex> _diskIndex;

      public:
        //
        // Sparse chunk iterator
        //
        virtual std::string const& getName() const;
        virtual ArrayID getHandle() const;

        virtual ArrayDesc const& getArrayDesc() const;

        virtual std::shared_ptr<ArrayIterator> getIteratorImpl(const AttributeDesc& attId) override;
        virtual std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attId) const override;

        MemArray(ArrayDesc const& arrayDesc, std::shared_ptr<Query> const& query);

        ~MemArray();

        /**
         * Retrieve the query associated with this memArray
         */
        virtual std::shared_ptr<Query> getQuery() const
        {
            return Query::getValidQueryPtr(_query);
        }

        /**
         * @see Array::isMaterialized()
         */
        virtual bool isMaterialized() const
        {
            return true;
        }
      protected:
        ArrayDesc desc;
      private:
        void makeChunk(Address const& addr,
                       CachedTmpChunk*& chunk,
                       CachedTmpChunk*& bitmapchunk,
                       bool newChunk);
        void pinChunk(CachedTmpChunk const& chunk);
        void unpinChunk(CachedTmpChunk const& chunk);
        MemDiskIndex& diskIndex() const { return *_diskIndex; }
        Mutex _mutex;
    };

    /**
     * Class which tracks unique ids for mem arrays
     */
    class MemArrayMgr : public Singleton<MemArrayMgr>
    {
    public:
        /* Constructor
         */
        MemArrayMgr()
            : _genCount(0),
              _nsid(0)
            {
                _nsid = DataStores::getInstance()->openNamespace("tmp");
            }

        /* Return a unique data store key to use for next MemArray
         */
        DataStore::DataStoreKey getNextDsk()
            {
                DataStore::DataStoreKey dsk(_nsid, _genCount.fetch_add(1));
                return dsk;
            }

    protected:
        std::atomic<uint64_t> _genCount;   // unique id for next MemArray
        DataStore::NsId       _nsid;       // name space id for datastores
    };

    /**
     * Temporary (in-memory) array iterator
     */
    class MemArrayIterator : public ArrayIterator
    {
      private:
        MemArray::MemDiskIndex::Iterator _curr;
        MemArray::MemDiskIndex::Iterator _currBitmap;
        std::shared_ptr<MemArray>        _array;
        Address                          _addr;
        CachedTmpChunk*                  _currChunk;
        CachedTmpChunk*                  _currBitmapChunk;
        bool                             _positioned;

        void position()
        {
            if (!_positioned) restart();
        }

        void resetAddrToMin();
        void resetChunkRefs();

      public:
        MemArrayIterator(std::shared_ptr<MemArray> arr, AttributeID attId);
        ~MemArrayIterator();
        ConstChunk const& getChunk() override;
        bool end() override;
        void operator ++() override;
        Coordinates const& getPosition() override;
        bool setPosition(Coordinates const& pos) override;
        void setCurrent();
        void restart() override;
        Chunk& newChunk(Coordinates const& pos) override;
        Chunk& newChunk(Coordinates const& pos, CompressorType compressionMethod) override;
        void deleteChunk(Chunk& chunk) override;
        virtual std::shared_ptr<Query> getQuery()
        {
            return Query::getValidQueryPtr(_array->_query);
        }
    };

} // scidb namespace
#endif
