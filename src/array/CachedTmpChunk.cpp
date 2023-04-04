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
 * @file CachedTmpChunk.cpp
 *
 * @brief Non-persistent chunk managed by buffer cache implementation
 *
 */

#include <array/CachedTmpChunk.h>

#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <array/MemArray.h>
#include <system/Exceptions.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/Utils.h>

namespace scidb {
using namespace std;
using namespace arena;

// Logger. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.memchunk"));

//
// CachedTmpChunk
//

CachedTmpChunk::CachedTmpChunk()
    : MemChunk()
    , _accessCount(0)
    , _regCount(0)
    , _deleteOnLastUnregister(false)
{}

void CachedTmpChunk::initialize(ConstChunk const& srcChunk)
{
    SCIDB_ASSERT(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "CachedTmpChunk::initialize";
}

void CachedTmpChunk::initialize(MemArray const* arr,
                                ArrayDesc const* desc,
                                const Address& firstElem,
                                CompressorType compMethod)
{
    assert(arr);
    MemChunk::initialize(arr, desc, firstElem, compMethod);
    _key.initializeKey(arr->_diskIndex->getKeyMeta(), desc->getDimensions().size());
    arr->_diskIndex->getKeyMeta().fillKey(_key.getKey(),
                                          arr->_diskIndex->getDsk(),
                                          getAttributeDesc(),
                                          getFirstPosition(false));
}

void CachedTmpChunk::initialize(Array const* arr,
                                ArrayDesc const* desc,
                                const Address& firstElem,
                                CompressorType compMethod)
{
    SCIDB_ASSERT(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "CachedTmpChunk::initialize";
}

void* CachedTmpChunk::getWriteDataImpl()
{
    return _indexValue.memory().begin();
}

void const* CachedTmpChunk::getConstDataImpl() const
{
    return _indexValue.constMemory().begin();
}

void CachedTmpChunk::reallocate(size_t newSize)
{
    SCIDB_ASSERT(newSize > 0);

    if (newSize > size) {
        /* Allocate a new buffer.  Note:  this frees the old buffer (if
           there was one)
         */
        MemArray const* myArray = static_cast<MemArray const*>(array);
        MemArray::MemDiskIndex& diskIndex = myArray->diskIndex();
        // CachedTmpChunk is always uncompressed (CompressorType::NONE) when being used,
        // even if the Chunk's (target) compressionMethod for the Array is something else.
        diskIndex.allocateMemory(newSize, _indexValue, CompressorType::NONE);

    }
    size = newSize;
}

CachedTmpChunk::~CachedTmpChunk()
{
    SCIDB_ASSERT(data == NULL);
}

std::shared_ptr<ChunkIterator> CachedTmpChunk::getIterator(std::shared_ptr<Query> const& query,
                                                           int iterationMode)
{
    if (Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query) != query) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query";
    }
    return MemChunk::getIterator(query, iterationMode);
}

std::shared_ptr<ConstChunkIterator> CachedTmpChunk::getConstIterator(int iterationMode) const
{
    std::shared_ptr<Query> query(
        Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query));
    return MemChunk::getConstIterator(query, iterationMode);
}

bool CachedTmpChunk::pin() const
{
    ((MemArray*)array)->pinChunk(*this);
    return true;
}

void CachedTmpChunk::unPin() const
{
    ((MemArray*)array)->unpinChunk(*this);
}

void CachedTmpChunk::write(const std::shared_ptr<Query>& query)
{
    if (Query::getValidQueryPtr(static_cast<const MemArray*>(array)->_query) != query) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT) << "invalid query";
    }
    dirty = true;
    unPin();
}

void CachedTmpChunk::deleteOnLastUnregister()
{
    _deleteOnLastUnregister = true;
    if (_regCount == 0) {
        MemArray const* myArray = static_cast<MemArray const*>(array);
        ArenaPtr a = myArray->diskIndex().getArena();
        destroyCachedChunk<CachedTmpChunk>(*a, this);
    }
}

void CachedTmpChunk::trace() const
{
    // clang-format off
        LOG4CXX_TRACE(logger, "chunk trace [this=" << this << "] [accesscount=" <<
                      _accessCount << "]");
    // clang-format on
}

bool CachedTmpChunk::registerIterator(ConstChunkIterator& ci)
{
    ++_regCount;
    return true;
}

void CachedTmpChunk::unregisterIterator(ConstChunkIterator& ci)
{
    --_regCount;
    if (_deleteOnLastUnregister && _regCount == 0) {
        MemArray const* myArray = static_cast<MemArray const*>(array);
        ArenaPtr a = myArray->diskIndex().getArena();
        destroyCachedChunk<CachedTmpChunk>(*a, this);
    }
}
}  // namespace scidb
