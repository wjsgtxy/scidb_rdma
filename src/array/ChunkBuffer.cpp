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

#include <array/ChunkBuffer.h>

#include <array/MemChunk.h>
#include <log4cxx/logger.h>

#include <cassert>
#include <sstream>

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.chunkbuffer"));
const bool enforce = true;  // set to true to turn on read/write enforcement
const bool gatherTraces = false;  // set to true to collect stack traces

}

namespace scidb
{
    ChunkBuffer::ChunkBuffer()
        : _hasBeenRead(false)
        , _lastReadFrom()
        , _lastWrittenFrom()
    {
    }

    ChunkBuffer::ChunkBuffer(const ChunkBuffer& other)
        : _hasBeenRead(other._hasBeenRead)
        , _lastReadFrom(other._lastReadFrom)
        , _lastWrittenFrom(other._lastWrittenFrom)
    {
    }

    void* ChunkBuffer::getWriteData()
    {
        if (enforce) {
            if (_hasBeenRead) {
                // force a coredump, inspect the call stacks
                // in with GDB from there
                assert(false);
            }
            else if (gatherTraces) {
                _lastWrittenFrom = fetchBacktrace();
            }
        }

        return getWriteDataImpl();
    }

    void const* ChunkBuffer::getConstData() const
    {
        if (enforce) {
            _hasBeenRead = true;
            if (gatherTraces) {
                _lastReadFrom = fetchBacktrace();
            }
        }

        return getConstDataImpl();
    }

    bool ChunkBuffer::isPresent() const
    {
        auto mutable_this = const_cast<ChunkBuffer*>(this);
        // it may not be safe to call getWriteDataImpl from here because
        // of the case
        //  RLEBitmapChunkIterator::RLEBitmapChunkIterator
        //   ChunkBuffer::isPresent
        //    CachedDBChunk::getWriteDataImpl
        //     DiskIndex<scidb::DbAddressMeta>::DiskIndexValue::memory
        //      BufferHandle::getRawData
        //       BufferMgr::_getRawData
        //        BufferMgr::_getRawData
        //         assert(false) because pinCount is zero (read-only buffer)
        return getConstDataImpl() != nullptr ||
               mutable_this->getWriteDataImpl() != nullptr;
    }

    void ChunkBuffer::cloneConstData(void* destinationPtr, size_t bytes) const
    {
        assert(destinationPtr);
        bytes = (bytes == 0 ? getSize() : bytes);
        assert(bytes > 0);
        memcpy(destinationPtr, getConstDataImpl(), bytes);
    }

    std::shared_ptr<ConstRLEEmptyBitmap> ChunkBuffer::createEmptyBitmap(size_t offset) const
    {
        auto source = reinterpret_cast<const char*>(getConstDataImpl()) + offset;
        return std::make_shared<ConstRLEEmptyBitmap>(source);
    }

    void ChunkBuffer::clearReadBit()
    {
        _hasBeenRead = false;
    }
}
