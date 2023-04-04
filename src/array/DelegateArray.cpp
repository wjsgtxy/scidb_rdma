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
 * @file DelegateArray.cpp
 *
 * @brief Delegate array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <array/ChunkMaterializer.h>
#include <array/DelegateArray.h>

#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>
#include <util/RegionCoordinatesIterator.h>


#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

//#define NO_MATERIALIZE_CACHE 1

namespace scidb
{
    using namespace std;

    // Logger for delegate array. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.delegatearray"));

    //
    // Delegate chunk methods
    //
    const ArrayDesc& DelegateChunk::getArrayDesc() const
    {
        return array.getArrayDesc();
    }

    void DelegateChunk::overrideTileMode(bool enabled) {
        if (chunk != NULL) {
            ((Chunk*)chunk)->overrideTileMode(enabled);
        }
        tileMode = enabled;
    }

    Array const& DelegateChunk::getArray() const
    {
        return array;
    }

    const AttributeDesc& DelegateChunk::getAttributeDesc() const
    {
        auto attrIter = std::find_if(array.getArrayDesc().getAttributes().begin(),
                                     array.getArrayDesc().getAttributes().end(),
                                     [this] (const auto& elem) {
                                         return elem.getId() == attrID;
                                     });
        assert(attrIter != array.getArrayDesc().getAttributes().end());
        return *attrIter;
    }

    CompressorType DelegateChunk::getCompressionMethod() const
    {
        return chunk->getCompressionMethod();
    }

    Coordinates const& DelegateChunk::getFirstPosition(bool withOverlap) const
    {
        return chunk->getFirstPosition(withOverlap);
    }

    Coordinates const& DelegateChunk::getLastPosition(bool withOverlap) const
    {
        return chunk->getLastPosition(withOverlap);
    }

    std::shared_ptr<ConstChunkIterator> DelegateChunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(array.createChunkIterator(this, iterationMode));
    }

    void DelegateChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        chunk = &inputChunk;
    }

    ConstChunk const& DelegateChunk::getInputChunk() const
    {
        return *chunk;
    }

    DelegateArrayIterator const& DelegateChunk::getArrayIterator() const
    {
        return iterator;
    }

    size_t DelegateChunk::count() const
    {
        return isClone ? chunk->count() : ConstChunk::count();
    }

    bool DelegateChunk::isCountKnown() const
    {
        return isClone ? chunk->isCountKnown() : ConstChunk::isCountKnown();
    }

    bool DelegateChunk::isMaterialized() const
    {
        return isClone && chunk->isMaterialized();
    }

    bool DelegateChunk::isDirectMapping() const
    {
        return isClone;
    }

    bool DelegateChunk::pin() const
    {
        return isClone && chunk->pin();
    }

    void DelegateChunk::unPin() const
    {
        if (isClone) {
            chunk->unPin();
        }
    }

    void* DelegateChunk::getWriteDataImpl()
    {
        ASSERT_EXCEPTION_FALSE("getWriteData() not permitted on DelegateChunk");
    }

    void const* DelegateChunk::getConstDataImpl() const
    {
        // Inside trust boundary, so can use ...Impl().  -DG
        return isClone ? chunk->getConstDataImpl() : ConstChunk::getConstDataImpl();
    }

    size_t DelegateChunk::getSize() const
    {
        return isClone ? chunk->getSize() : ConstChunk::getSize();
    }

    void DelegateChunk::compress(CompressedBuffer& buf,
                                 std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                                 bool forceUncompressed) const
    {
        if (isClone) {
            chunk->compress(buf, emptyBitmap, forceUncompressed);
        } else {
            ConstChunk::compress(buf, emptyBitmap, forceUncompressed);
        }
    }

    DelegateChunk::DelegateChunk(DelegateArray const& arr,
                                 DelegateArrayIterator const& iter,
                                 AttributeID attr,
                                 bool clone)
    : array(arr),
      iterator(iter),
      attrID(attr),
      chunk(NULL),
      isClone(clone),
      tileMode(false)
    {
    }

    //
    // Delegate chunk iterator methods
    //
    int DelegateChunkIterator::getMode() const
    {
        return inputIterator->getMode();
    }

     Value const& DelegateChunkIterator::getItem()
    {
        return inputIterator->getItem();
    }

    bool DelegateChunkIterator::isEmpty() const
    {
        return inputIterator->isEmpty();
    }

    bool DelegateChunkIterator::end()
    {
        return inputIterator->end();
    }

    void DelegateChunkIterator::operator ++()
    {
        ++(*inputIterator);
    }

    Coordinates const& DelegateChunkIterator::getPosition()
    {
        return inputIterator->getPosition();
    }

    bool DelegateChunkIterator::setPosition(Coordinates const& pos)
    {
        return inputIterator->setPosition(pos);
    }

    void DelegateChunkIterator::restart()
    {
        inputIterator->restart();
    }

    ConstChunk const& DelegateChunkIterator::getChunk()
    {
        return *chunk;
    }

    DelegateChunkIterator::DelegateChunkIterator(DelegateChunk const* aChunk, int iterationMode)
    : chunk(aChunk), inputIterator(aChunk->getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE))
    {
    }

    //
    // Delegate array iterator methods
    //

    DelegateArrayIterator::DelegateArrayIterator(DelegateArray const& delegate,
                                                 const AttributeDesc& aDesc,
                                                 std::shared_ptr<ConstArrayIterator> input)
        : ConstArrayIterator(delegate)
        , array(delegate)
        , attr(aDesc)
        , inputIterator(input)
        , chunkInitialized(false)
        , _chunk(nullptr)
    { }

    std::shared_ptr<DelegateChunk>& DelegateArrayIterator::_chunkPtr()
    {
        if (!_chunk) {
            _chunk.reset(array.createChunk(this, attr.getId()));
        }
        return _chunk;
    }

    std::shared_ptr<ConstArrayIterator> DelegateArrayIterator::getInputIterator() const
    {
        return inputIterator;
    }

    ConstChunk const& DelegateArrayIterator::getChunk()
    {
        _chunkPtr()->setInputChunk(inputIterator->getChunk());
        return *_chunkPtr();
    }

    bool DelegateArrayIterator::end()
    {
        return inputIterator->end();
    }

    void DelegateArrayIterator::operator ++()
    {
        LOG4CXX_TRACE(logger, "DelegateArray::operator++()");
        chunkInitialized = false;
        ++(*inputIterator);
    }

    Coordinates const& DelegateArrayIterator::getPosition()
    {
        return inputIterator->getPosition();
    }

    bool DelegateArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        return inputIterator->setPosition(pos);
    }

    void DelegateArrayIterator::restart()
    {
        chunkInitialized = false;
        inputIterator->restart();
    }

    //
    // Delegate array methods
    //

    DelegateArray::DelegateArray(ArrayDesc const& arrayDesc,
                                 std::shared_ptr<Array> input,
                                 bool clone)
        : Array(input)
        , desc(arrayDesc)
        , isClone(clone)
    { }

    string const& DelegateArray::getName() const
    {
        return desc.getName();
    }

    ArrayID DelegateArray::getHandle() const
    {
        return desc.getId();
    }

    const ArrayDesc& DelegateArray::getArrayDesc() const
    {
        return desc;
    }

    std::shared_ptr<ConstArrayIterator> DelegateArray::getConstIteratorImpl(const AttributeDesc& id) const
    {
        return std::shared_ptr<ConstArrayIterator>(createArrayIterator(id));
    }

    DelegateChunk* DelegateArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        return new DelegateChunk(*this, *iterator, id, isClone);
    }

    DelegateChunkIterator* DelegateArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new DelegateChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* DelegateArray::createArrayIterator(const AttributeDesc& id) const
    {
        return new DelegateArrayIterator(*this, id, getPipe(0)->getConstIterator(id));
    }

    //
    // NonEmptyable array
    //

    NonEmptyableArray::NonEmptyableArray(const std::shared_ptr<Array>& input)
    : DelegateArray(input->getArrayDesc(), input, true)
    {
        Attributes newAttrs = desc.getAttributes();
        newAttrs.addEmptyTagAttribute();
        assert(newAttrs.hasEmptyIndicator());
        emptyTagID = newAttrs.getEmptyBitmapAttribute()->getId();
        desc = ArrayDesc(desc.getName(),
                         newAttrs,
                         desc.getDimensions(),
                         desc.getDistribution(),
                         desc.getResidency());
    }

    DelegateArrayIterator* NonEmptyableArray::createArrayIterator(const AttributeDesc& id) const
    {
        const auto& fda = getArrayDesc().getAttributes().firstDataAttribute();
        if (id.getId() == emptyTagID) {
            return new DummyBitmapArrayIterator(*this, id,
                                                getPipe(0)->getConstIterator(fda));
        }
        return new DelegateArrayIterator(*this, id,
                                         getPipe(0)->getConstIterator(id.getId() == emptyTagID ?
                                                                      fda : id));
    }

    DelegateChunkIterator* NonEmptyableArray::createChunkIterator(DelegateChunk const* chunk,
                                                                  int iterationMode) const
    {
        AttributeDesc const& attr = chunk->getAttributeDesc();
        return attr.isEmptyIndicator()
            ? (DelegateChunkIterator*)new DummyBitmapChunkIterator(chunk, iterationMode)
            : (DelegateChunkIterator*)new DelegateChunkIterator(chunk, iterationMode);
    }

    DelegateChunk* NonEmptyableArray::createChunk(DelegateArrayIterator const* iterator,
                                                  AttributeID id) const
    {
        return new DelegateChunk(*this, *iterator, id, id != emptyTagID);
    }

    Value const& NonEmptyableArray::DummyBitmapChunkIterator::getItem()
    {
        return _true;
    }

    bool NonEmptyableArray::DummyBitmapChunkIterator::isEmpty() const
    {
        return false;
    }

    NonEmptyableArray::DummyBitmapChunkIterator::DummyBitmapChunkIterator(DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      _true(TypeLibrary::getType(TID_BOOL))
    {
        _true.setBool(true);
    }

    ConstChunk const& NonEmptyableArray::DummyBitmapArrayIterator::getChunk()
    {
        ConstChunk const& inputChunk = inputIterator->getChunk();
        if (!shapeChunk.isInitialized() ||
            shapeChunk.getFirstPosition(false) != inputChunk.getFirstPosition(false)) {
            ArrayDesc const& arrayDesc = array.getArrayDesc();
            Address addr(attr.getId(), inputChunk.getFirstPosition(false));
            shapeChunk.initialize(&array, &arrayDesc, addr,
                                  inputChunk.getCompressionMethod());
            fillRLEBitmap(shapeChunk);
        }
        return shapeChunk;
    }

    NonEmptyableArray::DummyBitmapArrayIterator::DummyBitmapArrayIterator(
        DelegateArray const& delegate,
        const AttributeDesc& attrID,
        std::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(delegate, attrID, inputIterator)
    {
    }

    void NonEmptyableArray::DummyBitmapArrayIterator::fillRLEBitmap(MemChunk& chunk) const
    {
        // Set 'chunk' to be a filled (that is, non-empty and dense) EBM chunk.
        CoordinatesMapper mapper(chunk);
        auto emptyBitmap = std::make_shared<RLEEmptyBitmap>(mapper.getLogicalChunkSize());
        chunk.allocate(emptyBitmap->packedSize());
        emptyBitmap->pack(reinterpret_cast<char*>(chunk.getWriteData()));
    }

    //
    // Split array
    //

    SplitArray::SplitArray(ArrayDesc const& desc,
                           const boost::shared_array<char>& src,
                           Coordinates const& from,
                           Coordinates const& till,
                           std::shared_ptr<Query>const& query)
    : DelegateArray(desc, std::shared_ptr<Array>(), true),
      _startingChunk(from),
      _from(from),
      _till(till),
      _size(from.size()),
      _src(src),
      _empty(false)
    {
        assert(query);
        _query = query;
        desc.getChunkPositionFor(_startingChunk);
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            _size[i] = _till[i] - _from[i] + 1;
            if (_size[i] == 0) {
                _empty = true;
            }
            if (_till[i] > dims[i].getEndMax()) {
                _till[i] = dims[i].getEndMax();
            }
        }
    }

    SplitArray::~SplitArray()
    {
    }

    DelegateArrayIterator* SplitArray::createArrayIterator(const AttributeDesc& _id) const
    {
        return new SplitArray::ArrayIterator(*this, _id);
    }

    ConstChunk const& SplitArray::ArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (chunkInitialized)
        {
            return chunk;
        }
        else
        {
            chunk.initialize(&array, &array.getArrayDesc(), addr, CompressorType::NONE);
            const size_t nDims = dims.size();
            Coordinates const& firstChunkPosition = chunk.getFirstPosition(false);
            Coordinates const& firstArrayPosition = array.from();
            Coordinates const& lastChunkPosition = chunk.getLastPosition(false);
            Coordinates const& lastArrayPosition = array.till();
            Coordinates first(nDims);
            Coordinates last(nDims);
            for(size_t i = 0; i < nDims; ++i)
            {
                first[i] = max(firstChunkPosition[i], firstArrayPosition[i]);
                last[i] = min(lastChunkPosition[i], lastArrayPosition[i]);
            }
            Value value;

            // duration of getChunk() short enough
            const std::shared_ptr<scidb::Query>
                localQueryPtr(Query::getValidQueryPtr(array._query));

            std::shared_ptr<ChunkIterator> chunkIter =
                chunk.getIterator(localQueryPtr, nDims <= 2 ?
                                  ChunkIterator::SEQUENTIAL_WRITE : 0);
            double* src = reinterpret_cast<double*>(array._src.get());
            CoordinatesMapper bufMapper( array.from(), array.till());

            // Per the THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
            // here is why it is ok to iterate over the logical space.
            //
            // [from Alex Poliakov:]
            // The input array is passed in as a C++ array.
            // It is safe to assume this input _src is dense or near dense. It is also safe to assume _src is small enough to fit in memory.
            // Iterating over the logical space is therefore a perfectly reasonable solution.
            // This should be self evident in the code: for each position, there is one value in _src that we take and insert.
            // There is a writeItem() call for each position. None of the values are skipped. So clearly the data is dense.
            //
            RegionCoordinatesIterator coordinatesIter(first, last);
            while(!coordinatesIter.end())
            {
                Coordinates const& coord = coordinatesIter.getPosition();
                chunkIter->setPosition(coord);
                position_t pos = bufMapper.coord2pos(coord);
                if (!chunk.getAttributeDesc().isEmptyIndicator()) {
                    value.setDouble(src[pos]);
                }
                else {
                    value.setBool(true);
                }
                chunkIter->writeItem(value);
                ++coordinatesIter;
            }
            chunkIter->flush();
            chunkInitialized = true;
            return chunk;
        }
    }

    bool SplitArray::ArrayIterator::end()
    {
        return !hasCurrent;
    }

    void SplitArray::ArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        size_t i = dims.size()-1;
        while ((addr.coords[i] += dims[i].getChunkInterval()) > array._till[i]) {
            if (i == 0) {
                hasCurrent = false;
                return;
            }
            addr.coords[i] = array.startingChunk()[i];
            i -= 1;
        }
        chunkInitialized = false;
    }

    Coordinates const& SplitArray::ArrayIterator::getPosition()
    {
        return addr.coords;
    }

    bool SplitArray::ArrayIterator::setPosition(Coordinates const& pos)
    {
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (pos[i] < array.startingChunk()[i] || pos[i] > array._till[i]) {
                return false;
            }
        }
        addr.coords = pos;
        array.getArrayDesc().getChunkPositionFor(addr.coords);
        chunkInitialized = false;
        return hasCurrent = true;
    }

    void SplitArray::ArrayIterator::restart()
    {
        addr.coords = array.startingChunk();
        chunkInitialized = false;
        hasCurrent = !array._empty;
    }

    SplitArray::ArrayIterator::ArrayIterator(SplitArray const& arr, const AttributeDesc& attrID)
    : DelegateArrayIterator(arr, attrID, std::shared_ptr<ConstArrayIterator>()),
      dims(arr.getArrayDesc().getDimensions()),
      array(arr),
      attrBitSize(TypeLibrary::getType(attrID.getType()).bitSize())
    {
        //You can add support for non-doubles, but then you have to deal with some uglier math in ArrayIterator::getChunk
        //no one uses this for non-doubles at the moment.
        if(!attrID.isEmptyIndicator() && attrID.getType() != TID_DOUBLE)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "SplitArray does not support non-double attributes.";
        }
        addr.attId = attrID.getId();
        restart();
    }

    //
    // Materialized array
    //
    MaterializedArray::ArrayIterator::ArrayIterator(MaterializedArray& arr,
                                                    const AttributeDesc& attrID,
                                                    std::shared_ptr<ConstArrayIterator> input,
                                                    MaterializeFormat chunkFormat)
    : DelegateArrayIterator(arr, attrID, input),
      _array(arr),
      _chunkToReturn(0)
    {
    }

    ConstChunk const& MaterializedArray::ArrayIterator::getChunk()
    {
        if(_chunkToReturn)
        {
            return *_chunkToReturn;
        }

        ConstChunk const& chunk = inputIterator->getChunk();
        MaterializeFormat format = _array._format;
        if (chunk.isMaterialized()
            && (format == PreserveFormat
                || (format == RLEFormat)))
        {
            ((ConstChunk&)chunk).overrideTileMode(false);
            _chunkToReturn = &chunk;
            return *_chunkToReturn;
        }
#ifdef NO_MATERIALIZE_CACHE
        if (!_materializedChunk) {
            _materializedChunk = std::shared_ptr<MemChunk>(new MemChunk());
        }
        std::shared_ptr<Query> query(Query::getValidQueryPtr(_array._query));
        MaterializedArray::materialize(query, *_materializedChunk, chunk, _format);
#else
        _materializedChunk = _array.getMaterializedChunk(chunk);
#endif
        _chunkToReturn = _materializedChunk.get();
        return *_chunkToReturn;
    }

    void MaterializedArray::ArrayIterator::operator ++()
    {
        _chunkToReturn = 0;
        DelegateArrayIterator::operator ++();
    }

    bool MaterializedArray::ArrayIterator::setPosition(Coordinates const& pos)
    {
        _chunkToReturn = 0;
        return DelegateArrayIterator::setPosition(pos);
    }

    void MaterializedArray::ArrayIterator::restart()
    {
        _chunkToReturn = 0;;
        DelegateArrayIterator::restart();
    }

    std::shared_ptr<MemChunk> MaterializedArray::getMaterializedChunk(ConstChunk const& inputChunk)
    {
        bool newChunk = false;
        std::shared_ptr<MemChunk> chunk;
        std::shared_ptr<ConstRLEEmptyBitmap> bitmap;
        Coordinates const& pos = inputChunk.getFirstPosition(false);
        AttributeID attr = inputChunk.getAttributeDesc().getId();
        {
            ScopedMutexLock cs(_mutex, PTW_SML_MATERIALIZED_ARRAY);
            chunk = _chunkCache[attr][pos];
            if (!chunk) {
                chunk.reset(new MemChunk());
                bitmap = _bitmapCache[pos];
                newChunk = true;
            }
        }
        if (newChunk) {
            std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            materialize(query, *chunk, inputChunk, _format);
            if (!bitmap) {
                bitmap = chunk->getEmptyBitmap();
            }
            chunk->setEmptyBitmap(bitmap);
            {
                ScopedMutexLock cs(_mutex, PTW_SML_MATERIALIZED_ARRAY);
                if (_chunkCache[attr].size() >= _cacheSize) {
                    _chunkCache[attr].erase(_chunkCache[attr].begin());
                }
                _chunkCache[attr][pos] = chunk;
                if (_bitmapCache.size() >= _cacheSize) {
                    _bitmapCache.erase(_bitmapCache.begin());
                }
                _bitmapCache[pos] = bitmap;
            }
        }
        return chunk;
    }

MaterializedArray::MaterializedArray(std::shared_ptr<Array> input,
                                     std::shared_ptr<Query>const& query,
                                     MaterializeFormat chunkFormat)
    : DelegateArray(input->getArrayDesc(), input, true),
      _format(chunkFormat),
      _chunkCache(desc.getAttributes().size())
    {
        assert(query);
        _query = query;
#ifndef SCIDB_CLIENT
        _cacheSize = Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE);
#else
        _cacheSize = 1;
#endif
    }

    size_t nMaterializedChunks = 0;

void MaterializedArray::materialize(const std::shared_ptr<Query>& query,
                                    MemChunk& materializedChunk,
                                    ConstChunk const& chunk,
                                    MaterializeFormat format)
    {
        nMaterializedChunks += 1;

        materializedChunk.initialize(chunk);
        materializedChunk.setBitmapChunk((Chunk*)chunk.getBitmapChunk());
        int srcFlags = ChunkIterator::IGNORE_DEFAULT_VALUES |
                       (chunk.isSolid() ? ChunkIterator::INTENDED_TILE_MODE : 0);
        int dstFlags = ChunkIterator::ChunkIterator::NO_EMPTY_CHECK |
                       ChunkIterator::SEQUENTIAL_WRITE;
        ChunkMaterializer materializer(chunk, srcFlags, chunk.getArrayDesc().hasOverlap(), logger, query);
        materializer.write(materializedChunk, dstFlags, ChunkMaterializer::CHECK_TILE_MODE);
    }

    DelegateArrayIterator* MaterializedArray::createArrayIterator(const AttributeDesc& id) const
    {
        return new MaterializedArray::ArrayIterator(*(MaterializedArray*)this, id,
                                                    getPipe(0)->getConstIterator(id), _format);
    }

}
