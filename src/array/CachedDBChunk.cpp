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
 * @file CachedDBChunk.cpp
 *
 * @brief Persistent chunk managed by buffer cache implementation
 *
 */

#include <array/CachedDBChunk.h>

#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <array/DBArray.h>
#include <array/ReplicationMgr.h>
#include <system/Exceptions.h>
#include <storage/BufferMgr.h>
#include <storage/IndexMgr.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Utils.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>

// TODO: refactor what's needed from PersistentChunk.h out
// so as not to include something from an operator into the engine.
// Needed for ChunkDescriptor definition.
#include <query/ops/upgradeChunkIndex/io/PersistentChunk.h>

namespace scidb {
using namespace std;
using namespace arena;

// Logger. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.CachedDBChunk"));

void CachedDBChunk::createChunk(ArrayDesc const& desc,
                                std::shared_ptr<DbDiskIndex> diskIndexPtr,
                                PersistentAddress const& addr,
                                CachedDBChunk*& chunk,
                                CachedDBChunk*& bitmapChunk,
                                bool newChunk,
                                DBArray* arrayPtr)
{
    static InjectedErrorListener s_injectErrChunkExists(InjectErrCode::CHUNK_EXISTS);

    /* Allocate new cached db chunk object
     */
    arena::ArenaPtr arena = diskIndexPtr->getArena();
    chunk = createCachedChunk<CachedDBChunk>(*arena);

    SCIDB_ASSERT(!chunk->isInitialized());

    /* Initialize bitmap chunk, only if this is not bitmap attribute
       AND this is not a new chunk
     */
    AttributeDesc const* bitmapAttr = desc.getEmptyBitmapAttribute();
    if (bitmapAttr != nullptr && bitmapAttr->getId() != addr.attId && !newChunk) {

        PersistentAddress bitmapAddr(addr.arrVerId, bitmapAttr->getId(), addr.coords);

        bitmapChunk = createCachedChunk<CachedDBChunk>(*arena);
        auto attrIter = std::find_if(desc.getAttributes().begin(),
                                     desc.getAttributes().end(),
                                     [bitmapAddr] (const auto& elem) {
                                         return elem.getId() == bitmapAddr.attId;
                                     });
        assert(attrIter != desc.getAttributes().end());
        const auto& compressionType = attrIter->getDefaultCompressionMethod();
        bitmapChunk->initialize(diskIndexPtr, &desc, bitmapAddr, compressionType);
        bitmapChunk->setBitmapChunk(nullptr);
        bitmapChunk->array = arrayPtr;
    }

    /* Initialize chunk and set bitmap
     */
    auto attrIter = std::find_if(desc.getAttributes().begin(),
                                 desc.getAttributes().end(),
                                 [addr] (const auto& elem) {
                                     return elem.getId() == addr.attId;
                                 });
    const auto& compressionType = attrIter->getDefaultCompressionMethod();
    chunk->initialize(diskIndexPtr, &desc, addr, compressionType);
    chunk->setBitmapChunk(bitmapChunk);
    chunk->array = arrayPtr;

    /* If this is a new chunk, we will return it IndexPinned
     */
    if (newChunk) {
        auto chunkAuxMeta = chunk->getChunkAuxMeta();

        /* Enter the new chunk into the index map, unless it's already there.
           It is NOT okay for the chunk to already exist because
           we have to protect from multiple unmerged writes to the same
           chunk -- those would result in loss of data.
         */
        DbAddressMeta::Key* key = chunk->_key.getKey();
        bool inserted = diskIndexPtr->insertRecord(key,
                                                   chunk->_indexValue,
                                                   chunkAuxMeta,
                                                   /*keepPinned:*/ true,
                                                   /*update:*/ false);

        if (!inserted ) {
            // TODO: test coverage
            //     however adding s_injectErrChunkExists.test(__LINE__, __FILE__)
            //     causes an abort in Debug build, so there is work to be done before that can be enabled
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CHUNK_ALREADY_EXISTS)
                << CoordsToStr(addr.coords);
        }

        /* The previous call leaves the buffer pinned. Increase the access
           count
         */
        ++chunk->_accessCount;
    }
}

// Re-evaluate the test coverage after the checkin test has been re-integrated
// TODO: no coverage (in reports),
//       because test has to be disabled for cdash (SDB-5922)
//       causes no coverage in BufferHandle::setBufferKey
//       coverage has been validated manually by the test listed in SDB-5922
void CachedDBChunk::upgradeDescriptor(const CachedDBChunk& chunk,
                                      std::shared_ptr<DbDiskIndex> diskIndexPtr,
                                      const ChunkDescriptor& desc)
{
    // Pull-out constituents from the legacy Chunk Descriptor
    auto dsk = diskIndexPtr->getDsk();
    const auto& chdr = desc.hdr;
    const auto& diskPos = chdr.pos;

    // Construct the new Buffer Key from the legacy Chunk Descriptor constituents
    BufferMgr::BufferKey bufferKey(dsk, diskPos.offs, chdr.size, chdr.allocatedSize);
    bufferKey.setCompressedSize(chdr.compressedSize);

    // Populate the Buffer Handle with the new Buffer Key and the compression method if applicable
    auto& bufferHandle = chunk._indexValue.getBufferHandle();
    bufferHandle.setBufferKey(bufferKey);
    bufferHandle.setCompressorType(static_cast<CompressorType>(chdr.compressionMethod));

    // TODO: implement tests for compressed chunks, further changes for such chunks may be required as
    // current checkin tests uses uncompressed chunks only.

    // Update the newly created index record accordingly
    auto chunkAuxMeta = chunk.getChunkAuxMeta();
    diskIndexPtr->insertRecord(chunk._key.getKey(),
                               chunk._indexValue,
                               chunkAuxMeta,
                               /*keepPinned:*/ true,
                               /*update:*/ true);
}

//
// CachedDBChunk
//
CachedDBChunk::CachedDBChunk()
    : MemChunk()
    , _accessCount(0)
    , _regCount(0)
    , _deleteOnLastUnregister(false)
    , _chunkAuxMeta()
{}

void CachedDBChunk::initialize(std::shared_ptr<DbDiskIndex> diskIndexPtr,
                               ArrayDesc const* desc,
                               const PersistentAddress& firstElem,
                               CompressorType compMethod)
{
    assert(diskIndexPtr);
    MemChunk::initialize(nullptr, desc, firstElem, compMethod);
    _storageAddr = firstElem;
    _key.initializeKey(diskIndexPtr->getKeyMeta(), desc->getDimensions().size());
    auto const& km = diskIndexPtr->getKeyMeta();
    km.fillKey(_key.getKey(),
               diskIndexPtr->getDsk(),
               getAttributeDesc(),
               getFirstPosition(false),
               firstElem.arrVerId);

    _chunkAuxMeta.primaryInstanceId = ReplicationManager::getInstance()->getPrimaryInstanceId(*desc, firstElem);
    LOG4CXX_TRACE(logger, "CachedDBChunk::init() for key=" << km.keyToString(_key.getKey())
                  << ", stgAddr=" << _storageAddr.toString()
                  << ", cType=" << static_cast<int16_t>(compMethod));
}

void* CachedDBChunk::getWriteDataImpl()
{
    return _indexValue.memory().begin();
}

void const* CachedDBChunk::getConstDataImpl() const
{
    LOG4CXX_TRACE(logger, "CachedDBChunk::getConstData for " << array->getName()
                  << "[attrid: " << _storageAddr.attId << "]");

    return _indexValue.constMemory().begin();
}

void* CachedDBChunk::getCompressionBuffer() const
{
    return _indexValue.compressionBuffer().begin();
}

void CachedDBChunk::reallocate(size_t newSize)
{
    SCIDB_ASSERT(newSize > 0);

    if (newSize > size) {
        /* Allocate a new buffer.  Note:  this frees the old buffer (if
           there was one)
         */
        DBArray const* myArray = static_cast<DBArray const*>(array);
        DBArray::DBDiskIndex& diskIndex = myArray->diskIndex();
        diskIndex.allocateMemory(newSize, _indexValue, compressionMethod);
    }
    size = newSize;
}

CachedDBChunk::~CachedDBChunk()
{
    SCIDB_ASSERT(data == nullptr);
}

std::shared_ptr<ChunkIterator> CachedDBChunk::getIterator(std::shared_ptr<Query> const& query,
                                                          int iterationMode)
{
    static InjectedErrorListener s_injectErrInvalidQuery1(InjectErrCode::INVALID_QUERY1);
    if (Query::getValidQueryPtr(static_cast<const DBArray*>(array)->_query) != query
            || s_injectErrInvalidQuery1.test(__LINE__, __FILE__)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query";
    }
    return MemChunk::getIterator(query, iterationMode);
}

std::shared_ptr<ConstChunkIterator> CachedDBChunk::getConstIterator(int iterationMode) const
{
    std::shared_ptr<Query> query(
        Query::getValidQueryPtr(static_cast<const DBArray*>(array)->_query));
    return MemChunk::getConstIterator(query, iterationMode);
}

bool CachedDBChunk::pin() const
{
    // wait timing done in DBArray layer, there is no blocking here
    DBArray* ary = dynamic_cast<DBArray*>(const_cast<Array*>(array));
    assert(ary);
    ary->pinChunk(*this);
    return true;
}

void CachedDBChunk::unPin() const
{
    // wait timing done in DBArray layer, there is no blocking here
    DBArray* ary = dynamic_cast<DBArray*>(const_cast<Array*>(array));
    assert(ary);
    ary->unpinChunk(*this);
}

void CachedDBChunk::write(const std::shared_ptr<Query>& query)
{
    static InjectedErrorListener s_injectErrInvalidQuery2(InjectErrCode::INVALID_QUERY2);
    if (Query::getValidQueryPtr(static_cast<const DBArray*>(array)->_query) != query
            || s_injectErrInvalidQuery2.test(__LINE__, __FILE__)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query";
    }
    dirty = true;

    /* Update the element count in the chunk auxiliary metadata
     */
    if (getAttributeDesc().isEmptyIndicator()) {
        ConstRLEEmptyBitmap bitmap(reinterpret_cast<char const*>(getConstData()));
        _chunkAuxMeta.nElements = bitmap.count();
    } else {
        ConstRLEPayload payload(reinterpret_cast<char const*>(getConstData()));
        _chunkAuxMeta.nElements = payload.count();
    }

    unPin();
}

void CachedDBChunk::deleteOnLastUnregister()
{
    _deleteOnLastUnregister = true;
    if (_regCount == 0) {
        DBArray const* myArray = static_cast<DBArray const*>(array);
        if (myArray) {
            ArenaPtr a = myArray->diskIndex().getArena();
            destroyCachedChunk<CachedDBChunk>(*a, this);
        }
    }
}

bool CachedDBChunk::registerIterator(ConstChunkIterator& ci)
{
    ++_regCount;
    return true;
}

void CachedDBChunk::unregisterIterator(ConstChunkIterator& ci)
{
    --_regCount;
    if (_deleteOnLastUnregister && _regCount == 0) {
        DBArray const* myArray = static_cast<DBArray const*>(array);
        if (myArray) {
            ArenaPtr a = myArray->diskIndex().getArena();
            destroyCachedChunk<CachedDBChunk>(*a, this);
        }
    }
}

void CachedDBChunk::setCompressedSizeAndType(size_t cSize, CompressorType cType)
{
    _indexValue.getBufferHandle().setCompressorType(cType);
    _indexValue.setCompressedSize(cSize);
}

size_t CachedDBChunk::count() const
{
    if (getArrayDesc().hasOverlap()) {
        // In the case of chunk overlap, the number of elements must be computed
        // by (potentially) materializing the chunk, as performed by ConstChunk.
        return ConstChunk::count();
    }
    return _chunkAuxMeta.nElements;
}

bool CachedDBChunk::isCountKnown() const
{
    if (getArrayDesc().hasOverlap()) {
        // In the case of chunk overlap, the presence of the element count is
        // determined by (potentially) materializing the chunk, as performed
        // by ConstChunk.
        return ConstChunk::isCountKnown();
    }
    return _chunkAuxMeta.nElements > 0;
}

void CachedDBChunk::setCount(size_t count)
{
    _chunkAuxMeta.nElements = count;
}

// NOTE:  This is deprecated.
const DbAddressMeta::Key* CachedDBChunk::getKey() const
{
    return _key.getKey();
}

}  // namespace scidb
