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
 * @file MemArray.cpp
 *
 * @brief Temporary (in-memory) array implementation
 *
 * @author poliocough@gmail.com
 * @author Donghui Zhang
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <array/MemArray.h>
#include <array/SyntheticArray.h>

#include <exception>
#include <log4cxx/logger.h>

#include <util/Platform.h>
#include <util/FileIO.h>
#include <system/Exceptions.h>
#include <system/Config.h>
#include <util/compression/Compressor.h>
#include <system/SciDBConfigOptions.h>
#include <system/Utils.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <monitor/InstanceStats.h>
#include <monitor/MonitorConfig.h>
#include <util/PerfTime.h>

namespace scidb
{
    using namespace std;

    const size_t MAX_SPARSE_CHUNK_INIT_SIZE = 1*MiB;

    const bool _sDebug = false;

    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.memarray"));

    //
    // MemArray
    //

    MemArray::MemArray(ArrayDesc const& arr, std::shared_ptr<Query> const& query)
    : desc(arr)
    {
        DataStore::DataStoreKey dsk = MemArrayMgr::getInstance()->getNextDsk();
        _query=query;
        MemAddressMeta addressMeta;
        MemIndexMgr::getInstance()->getIndex(_diskIndex,
                                             dsk,
                                             addressMeta);
        SCIDB_ASSERT(_diskIndex);
    }

    MemArray::~MemArray()
    {
        MemIndexMgr::getInstance()->closeIndex(_diskIndex, true /*destroy */);
    }

    void MemArray::pinChunk(CachedTmpChunk const& chunk)
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "PIN: chunk="<<(void*)&chunk
                          << ",  accessCount is " << chunk._accessCount
                          << ", Array=" << (void*)this
                          << ", name " << chunk.arrayDesc->getName()
                          << ", Addr=" << chunk.addr.toString()
                          << ", Size=" << chunk.size
                          << ", Key="
                          << _diskIndex->getKeyMeta().keyToString(
                              chunk._key.getKey()));
        }
        Query::getValidQueryPtr(_query);

        ScopedMutexLock cs(_mutex, PTW_SML_MEM_ARRAY_PIN);
        if (chunk._accessCount++ == 0) {
            /* Retrieve the value stored in the map for this chunk address
             */
            MemAddressMeta::Key* key = chunk._key.getKey();
            _diskIndex->pinValue(key, chunk._indexValue);
            chunk.size = chunk._indexValue.constMemory().size();
            chunk.markClean();
        }
        else {
            assert(chunk.isPresent() || chunk.size == 0);
        }
    }

    void MemArray::unpinChunk(CachedTmpChunk const& chunk)
    {
        if (_sDebug) {
            LOG4CXX_TRACE(logger, "UNPIN: chunk=" << (void*)&chunk
                          << ",  accessCount is " << chunk._accessCount
                          << ", Array=" << (void*)this << ", name " << chunk.arrayDesc->getName()
                          << ", Addr=" << chunk.addr.toString() << ", Size=" << chunk.size
                          << ", Key=" << _diskIndex->getKeyMeta().keyToString(chunk._key.getKey()));
        }

        ScopedMutexLock cs(_mutex, PTW_SML_MEM_ARRAY_UNPIN);
        assert(chunk._accessCount > 0);
        assert(chunk.isPresent() || chunk.size==0); // chunk.getConstData()==NULL --> chunk.size==0

        if (--chunk._accessCount == 0) {

            MemAddressMeta::Key* key = chunk._key.getKey();

            /* If the value is owned by the caller, we need to insert/update the value
             */
            if (chunk._indexValue.state() == MemDiskIndex::DiskIndexValue::CallerPinned)
            {
                SCIDB_ASSERT(chunk.isDirty());  // should not allocate mem if not dirty

                ChunkAuxMeta emptyStats;
                _diskIndex->insertRecord(key,
                                         chunk._indexValue,
                                         emptyStats,
                                         false,  // unpin the buffer after insert
                                         true);  // replace any existing value
            }
            /* If the value is already in the index then we simply need to unpin the value...
             */
            else if (chunk._indexValue.state() == MemDiskIndex::DiskIndexValue::IndexPinned)
            {
                chunk._indexValue.unpin();
            }
            /* It is possible that the value is not pinned at all... but this can only happen
               if the buffer is empty.  In this case, we don't need to do anything.
             */
            else
            {
                SCIDB_ASSERT(!chunk._indexValue.isActive());
            }
        }
    }

    string const& MemArray::getName() const
    {
        return desc.getName();
    }

    ArrayID MemArray::getHandle() const
    {
        return desc.getId();
    }

    ArrayDesc const& MemArray::getArrayDesc() const
    {
        return desc;
    }

    void MemArray::makeChunk(Address const& addr,
                             CachedTmpChunk*& chunk,
                             CachedTmpChunk*& bitmapchunk,
                             bool newChunk)
    {
        SCIDB_ASSERT(_mutex.isLockedByThisThread());

        /* Allocate new cached mem chunk object
         */
        arena::ArenaPtr arena = _diskIndex->getArena();
        chunk = createCachedChunk<CachedTmpChunk>(*arena);

        chunk->array = this;

        SCIDB_ASSERT(!chunk->isInitialized());

        /* Initialize bitmap chunk
         */
        AttributeDesc const* bitmapAttr = desc.getEmptyBitmapAttribute();
        if (bitmapAttr != NULL && bitmapAttr->getId() != addr.attId) {

            Address bitmapAddr(bitmapAttr->getId(), addr.coords);

            bitmapchunk = createCachedChunk<CachedTmpChunk>(*arena);
            bitmapchunk->initialize(this,
                                    &desc,
                                    bitmapAddr,
                                    bitmapAttr->getDefaultCompressionMethod());
            bitmapchunk->setBitmapChunk(NULL);
        }

        /* Initialize chunk and set bitmap
         */
        SCIDB_ASSERT(desc.getAttributes().hasAttribute(addr.attId));
        auto targetAttr = desc.getAttributes().find(addr.attId);
        chunk->initialize(this,
                          &desc,
                          addr,
                          targetAttr->getDefaultCompressionMethod());
        chunk->setBitmapChunk(bitmapchunk);

        /* If this is a new chunk, we will return it pinned
         */
        if (newChunk)
        {
            ChunkAuxMeta emptyStats;

            /* We don't know if the bitmap chunk has already been entered
               into the index map.  Try to enter it anyway, if it fails,
               that is ok.
             */
            if (bitmapchunk)
            {
                MemAddressMeta::Key* bitmapkey = bitmapchunk->_key.getKey();
                _diskIndex->insertRecord(bitmapkey,
                                         bitmapchunk->_indexValue,
                                         emptyStats,
                                         false,   // unpin buffer after insert
                                         false);  // do not replace existing value
            }

            /* Enter the new chunk into the index map, unless its already there.
               If there is already an entry for this position, that's ok, the
               current value will be replaced by this chunk's value when this
               chunk is flushed.  Until then, we need to keep the old value in
               place because someone may read it (as happens in "insert").
             */
            MemAddressMeta::Key* key = chunk->_key.getKey();
            _diskIndex->insertRecord(key,
                                     chunk->_indexValue,
                                     emptyStats,
                                     true,    // keep buffer pinned after insert
                                     false);  // do not replace existing value

            /* The previous call leaves the buffer pinned. Increase the access count
             */
            ++chunk->_accessCount;
        }
    }

    std::shared_ptr<ArrayIterator> MemArray::getIteratorImpl(const AttributeDesc& attId)
    {
        auto owner = dynamic_pointer_cast<MemArray>(shared_from_this());
        return std::shared_ptr<ArrayIterator>(new MemArrayIterator(owner, attId.getId()));
    }

    std::shared_ptr<ConstArrayIterator> MemArray::getConstIteratorImpl(const AttributeDesc& attr) const
    {
        std::shared_ptr<ConstArrayIterator> iter;
        iter = (const_cast<MemArray*>(this))->getIterator(attr);
        if (attr.isEmptyIndicator()) {
            // Never synthesize values for the empty bitmap attribute.
            return iter;
        }
        if (!desc.isTransient()) {
            // No need to synthesize for a MemArray unless it was
            // created by "create temp array ..."  (and is therefore
            // marked "transient").
            return iter;
        }

        // See comment in DBArray::getConstIteratorImpl() to understand
        // why iter->end() implies we should synthesize this attribute.
        // Briefly, it may be newly added by add_attributes(), and if so
        // we must synthesize; if not, end() is end() regardless of
        // subtype of ConstArrayIterator.
        if (iter->end()) {
            auto owner = std::dynamic_pointer_cast<MemArray>(const_cast<MemArray*>(this)->shared_from_this());
            std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
            iter = std::make_shared<SyntheticArrayIterator>(owner, attr, query);
        }

        return iter;
    }


    //
    // Temporary (in-memory) array iterator
    //
    MemArrayIterator::MemArrayIterator(std::shared_ptr<MemArray> arr, AttributeID attId)
        : ArrayIterator(*arr),
          _array(arr),
          _currChunk(NULL),
          _currBitmapChunk(NULL)
    {
        _addr.attId = attId;
        _addr.coords.insert(_addr.coords.begin(),
                            _array->desc.getDimensions().size(),
                            0);
        resetAddrToMin();
        _positioned = false;
        LOG4CXX_TRACE(logger, "MemArrayIterator::MemArrayIterator()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
    }

    MemArrayIterator::~MemArrayIterator()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::~MemArrayIterator()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
        if (_currChunk)
        {
            _currChunk->deleteOnLastUnregister();
        }
        if (_currBitmapChunk)
        {
            _currBitmapChunk->deleteOnLastUnregister();
        }
    }

    void MemArrayIterator::resetAddrToMin()
    {
        for (size_t i = 0; i < _array->desc.getDimensions().size(); ++i)
        {
            _addr.coords[i] = _array->desc.getDimensions()[i].getStartMin();
        }
    }

    void MemArrayIterator::resetChunkRefs()
    {
        if (_currChunk)
        {
            _currChunk->deleteOnLastUnregister();
        }
        if (_currBitmapChunk)
        {
            _currBitmapChunk->deleteOnLastUnregister();
        }
        _currChunk = NULL;
        _currBitmapChunk = NULL;
    }

    ConstChunk const& MemArrayIterator::getChunk()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::getChunk()" <<
                      " This="<<(void*)this <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
        position();
        if (!_currChunk) {
            assert(false);
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return *_currChunk;
    }

    bool MemArrayIterator::end()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::end()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
        position();
        return _currChunk == NULL;
    }

    void MemArrayIterator::operator ++()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::operator++()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);

        ScopedMutexLock cs(_array->_mutex, PTW_SML_MEM_ARRAY_ITER);
        position();
        ++_curr;
        if (_array->desc.getEmptyBitmapAttribute())
        {
            ++_currBitmap;
        }
        setCurrent();
    }

    Coordinates const& MemArrayIterator::getPosition()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::getPosition()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
        position();
        if (!_currChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return _currChunk->getFirstPosition(false);
    }

    bool MemArrayIterator::setPosition(Coordinates const& pos)
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::setPosition()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);

        ScopedMutexLock cs(_array->_mutex, PTW_SML_MEM_ARRAY_ITER);
        MemAddressMeta::KeyWithSpace key;
        key.initializeKey(_array->_diskIndex->getKeyMeta(),
                          pos.size());

        _addr.coords = pos;
        _array->desc.getChunkPositionFor(_addr.coords);

        const auto& attrKey = _array->desc.getAttributes().findattr(_addr.attId);
        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 attrKey,
                                                 _addr.coords);
        _curr = _array->_diskIndex->find(key.getKey());

        if (_array->desc.getEmptyBitmapAttribute())
        {
            auto ebmAttrId = _array->desc.getEmptyBitmapAttribute();

            _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                     _array->_diskIndex->getDsk(),
                                                     *ebmAttrId,
                                                     _addr.coords);
            _currBitmap = _array->_diskIndex->find(key.getKey());
        }

        _positioned = true;
        setCurrent();
        if (_currChunk) {
            return true;
        } else {
            return false;
        }
    }

    void MemArrayIterator::setCurrent()
    {
        SCIDB_ASSERT(_array->_mutex.isLockedByThisThread());

        resetChunkRefs();
        if (!_curr.isEnd()) {
            auto ebmAttr = _array->desc.getAttributes().getEmptyBitmapAttribute();
            auto currAttId = _curr.getKey().decodeAttributeID(ebmAttr);
            if (currAttId == _addr.attId) {
                _array->_diskIndex->getKeyMeta().keyToAddress(&_curr.getKey(),
                                                              _addr,
                                                              ebmAttr);
                _array->makeChunk(_addr, _currChunk, _currBitmapChunk, false);
                if (_currChunk)
                    {
                        _currChunk->size = _curr.valueSize();
                    }
                if (_currBitmapChunk)
                    {
                        _currBitmapChunk->size = _currBitmap.valueSize();
                    }
                LOG4CXX_TRACE(logger, "MemArrayIterator::setCurrent()" <<
                              " Array="<<(void*)_array.get() <<
                              " Addr="<<_addr.toString() <<
                              " Positioned="<< _positioned <<
                              " CurrChunk="<< _currChunk <<
                              " CurrBitmapChunk="<< _currBitmapChunk);
            }
            else {
                LOG4CXX_TRACE(logger, "MemArrayIterator::setCurrent" <<
                              " Array="<<(void*)_array.get() <<
                              " Addr="<<_addr.toString() <<
                              " Positioned="<< _positioned <<
                              " No more chunks");
            }
        }
        else
        {
            LOG4CXX_TRACE(logger, "MemArrayIterator::setCurrent" <<
                          " Array="<<(void*)_array.get() <<
                          " Addr="<<_addr.toString() <<
                          " Positioned="<< _positioned <<
                          " No more chunks");
        }
    }

    void MemArrayIterator::restart()
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::restart()" <<
                      " This="<<(void*)this <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);

        ScopedMutexLock cs(_array->_mutex, PTW_SML_MEM_ARRAY_ITER);
        MemAddressMeta::KeyWithSpace key;

        resetAddrToMin();
        key.initializeKey(_array->_diskIndex->getKeyMeta(),
                          _addr.coords.size());
        const auto& attrKey = _array->desc.getAttributes().findattr(_addr.attId);
        _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                 _array->_diskIndex->getDsk(),
                                                 attrKey,
                                                 _addr.coords);
        _curr = _array->_diskIndex->leastUpper(key.getKey());

        if (_array->desc.getEmptyBitmapAttribute())
        {
            auto ebmAttrId = _array->desc.getEmptyBitmapAttribute();

            _array->_diskIndex->getKeyMeta().fillKey(key.getKey(),
                                                     _array->_diskIndex->getDsk(),
                                                     *ebmAttrId,
                                                     _addr.coords);
            _currBitmap = _array->_diskIndex->leastUpper(key.getKey());
        }

        _positioned = true;
        setCurrent();
    }

    ///
    /// @warning It looks dangerous to use this function.
    ///          Even though the accessing of _array._chunks is protected by _array._mutex,
    ///          deleting a chunk may invalidate other MemArrayIterator's curr data member.
    ///          So if any code uses a read and write MemArrayIterator at the same time
    ///          for the same attribute, it should fail.
    ///
    void MemArrayIterator::deleteChunk(Chunk& aChunk)
    {
        LOG4CXX_TRACE(logger, "MemArrayIterator::deleteChunk()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);
        CachedTmpChunk& chunk = dynamic_cast<CachedTmpChunk&>(aChunk);
        ScopedMutexLock cs(_array->_mutex, PTW_SML_MEM_ARRAY_ITER);
        chunk._accessCount = 0;

        MemAddressMeta::Key* key = chunk._key.getKey();
        MemArray::MemDiskIndex::Iterator iter = _array->_diskIndex->find(key);
        if (!(iter.isEnd()))
        {
            _array->_diskIndex->deleteRecord(iter);
        }
    }

    Chunk& MemArrayIterator::newChunk(Coordinates const& pos)
    {
        if (!_array->desc.contains(pos)) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                << CoordsToStr(pos) << _array->desc.getDimensions();
        }

        ScopedMutexLock cs(_array->_mutex, PTW_SML_MEM_ARRAY_ITER);

        _addr.coords = pos;
        _array->desc.getChunkPositionFor(_addr.coords);

        LOG4CXX_TRACE(logger, "MemArrayIterator::newChunk()" <<
                      " Array="<<(void*)_array.get() <<
                      " Addr="<<_addr.toString() <<
                      " Positioned="<< _positioned);

        resetChunkRefs();

        /* TODO: previously we checked for existence of chunk at this point.
           Is this necessary?  Can we check when chunk is written (unpin)?
         */
        _array->makeChunk(_addr, _currChunk, _currBitmapChunk, true /* chunk pinned */);
        return *(_currChunk);
    }

    Chunk& MemArrayIterator::newChunk(Coordinates const& pos, CompressorType compressionMethod)
    {
        SCIDB_ASSERT(compressionMethod != CompressorType::UNKNOWN);
        Chunk& chunk = newChunk(pos);
        ((MemChunk&)chunk).compressionMethod = compressionMethod;
        return chunk;
    }

} // scidb namespace
