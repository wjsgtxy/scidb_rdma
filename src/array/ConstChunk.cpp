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
 * @file ConstChunk.cpp
 *
 * @brief class ConstChunk
 */

#include <array/ConstChunk.h>

#include <vector>
#include <string.h>

#include <log4cxx/logger.h>

#include <array/MemChunk.h>
#include <array/PinBuffer.h>
#include <array/ArrayDesc.h>
#include <array/AttributeDesc.h>
#include <array/RLE.h>
#include <array/Metadata.h> // For getChunkNumberOfElements
#include <util/OnScopeExit.h>
#include <query/Query.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstChunk"));

    const uint64_t* ConstChunk::getChunkMagic() const
    {
        return reinterpret_cast<const uint64_t*>(getConstDataImpl());
    }

    size_t ConstChunk::getBitmapSize() const
    {
        if (isMaterialized() && !getAttributeDesc().isEmptyIndicator()) {
            PinBuffer scope(*this);
            // inside trust boundary
            ConstRLEPayload payload(reinterpret_cast<const char*>(getConstDataImpl()));
            return getSize() - payload.packedSize();
        }
        return 0;
    }

    ConstChunk const* ConstChunk::getBitmapChunk() const
    {
        // You might ask, how do we know this is a bitmap chunk?  We don't.
        // Apparently the meaning of setBitmapChunk(x) is not to set the
        // bitmap chunk to x, but to set it to whatever bitmap chunk x is
        // using.  Which may be x itself.  Hence return 'this' here, the
        // induction step in a sketchy chain of reasoning.

        return this;
    }

    void ConstChunk::makeClosure(Chunk& closure, std::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const
    {
        PinBuffer scope(*this);
        closure.allocate(getSize() + emptyBitmap->packedSize());
        cloneConstData(closure.getWriteData());
        emptyBitmap->pack(reinterpret_cast<char*>(closure.getWriteData()) + getSize());
    }

    ConstChunk* ConstChunk::materialize() const
    {
        return _cache.get();
    }

    void ConstChunk::compress(CompressedBuffer& buf,
                              std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                              bool forceUncompressed) const
    {
        materialize()->compress(buf, emptyBitmap, forceUncompressed);
    }

    void* ConstChunk::getWriteDataImpl()
    {
        ASSERT_EXCEPTION_FALSE("getWriteData() on ConstChunk not permitted");
    }

    const void* ConstChunk::getConstDataImpl() const
    {
        // inside trust boundary
        return materialize()->getConstDataImpl();
    }

    size_t ConstChunk::getSize() const
    {
        return materialize()->getSize();
    }

    bool ConstChunk::pin() const
    {
        return false;
    }
    void ConstChunk::unPin() const
    {
        assert(typeid(*this) != typeid(ConstChunk));
    }

    bool ConstChunk::contains(Coordinates const& pos, bool withOverlap) const
    {
        Coordinates const& first = getFirstPosition(withOverlap);
        Coordinates const& last = getLastPosition(withOverlap);
        for (size_t i = 0, n = first.size(); i < n; i++) {
            if (pos[i] < first[i] || pos[i] > last[i]) {
                return false;
            }
        }
        return true;
    }

    bool ConstChunk::isCountKnown() const
    {
        return getArrayDesc().getEmptyBitmapAttribute() == NULL
            || (isMaterializedChunkPresent() && materialize()->isCountKnown());
    }

    size_t ConstChunk::count() const
    {
        if (getArrayDesc().getEmptyBitmapAttribute() == NULL) {
            return getNumberOfElements(false);
        }
        if (isMaterializedChunkPresent()) {
            // We have a materialized chunk but its first position may not match
            // that of *this, call materialize() to check and re-materialize
            // the chunk if necessary.
            return materialize()->count();
        }
        std::shared_ptr<ConstChunkIterator> i = getConstIterator();
        size_t n = 0;
        while (!i->end()) {
            ++(*i);
            n += 1;
        }
        return n;
    }

    size_t ConstChunk::getNumberOfElements(bool withOverlap) const
    {
        Coordinates low = getFirstPosition(withOverlap);
        Coordinates high = getLastPosition(withOverlap);
        return getChunkNumberOfElements(low, high);
    }

    bool ConstChunk::isSolid() const
    {
        Dimensions const& dims = getArrayDesc().getDimensions();
        Coordinates const& first = getFirstPosition(false);
        Coordinates const& last = getLastPosition(false);
        for (size_t i = 0, n = dims.size(); i < n; i++) {
            if (dims[i].getChunkOverlap() != 0 || (last[i] - first[i] + 1) != dims[i].getChunkInterval()) {
                return false;
            }
        }
        return !getAttributeDesc().isNullable()
            && !TypeLibrary::getType(getAttributeDesc().getType()).variableSize()
            && getArrayDesc().getEmptyBitmapAttribute() == NULL;
    }

    bool ConstChunk::isReadOnly() const
    {
        return true;
    }

    bool ConstChunk::isMaterialized() const
    {
        return false;
    }

    void ConstChunk::cloneRLEPayload(RLEPayload& destination) const
    {
        auto dataPtr = reinterpret_cast<const char*>(getConstDataImpl());
        ConstRLEPayload constPayload(dataPtr);

        // RLEPayload assignment operator from ConstRLEPayload will
        // deep copy the data from the ConstRLEPayload object,
        // accomplishing a clone of the chunk data into the destination
        // RLEPayload instance.
        destination = constPayload;
    }

    std::shared_ptr<ConstRLEEmptyBitmap> ConstChunk::getEmptyBitmap() const
    {
        if (getAttributeDesc().isEmptyIndicator()/* && isMaterialized()*/) {
            PinBuffer scope(*this);
            // inside trust boundary
            auto constDataPtr = reinterpret_cast<const char*>(getConstDataImpl());
            auto ebmPtr = new RLEEmptyBitmap(ConstRLEEmptyBitmap(constDataPtr));
            return std::shared_ptr<ConstRLEEmptyBitmap>(ebmPtr);
        }
        AttributeDesc const* emptyAttr = getArrayDesc().getEmptyBitmapAttribute();
        if (emptyAttr != NULL) {
            if (!emptyIterator) {
                ((ConstChunk*)this)->emptyIterator = getArray().getConstIterator(*emptyAttr);
            }
            Array const& arr = emptyIterator->getArray();
            if (arr.getSupportedAccess() == Array::RANDOM) {
                if (!emptyIterator->setPosition(getFirstPosition(false))) {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            } else {
                LOG4CXX_DEBUG(logger, __func__ << ": Array " << demangle_cpp(typeid(arr).name())
                              << " only supports access " << arr.getSupportedAccess()
                              << ", skipping setPosition(firstPosition) call");
            }
            ConstChunk const& bitmapChunk = emptyIterator->getChunk();
            PinBuffer scope(bitmapChunk);
            return std::make_shared<RLEEmptyBitmap>(*bitmapChunk.createEmptyBitmap());
        }
        return std::shared_ptr<ConstRLEEmptyBitmap>();
    }


    ConstChunk::ConstChunk()
        : _cache(*this, logger)
    {
    }

    ConstChunk::~ConstChunk()
    {
    }

    bool ConstChunk::isMaterializedChunkPresent() const
    {
        return _cache.hasChunk();
    }

    void ConstChunk::releaseMaterializedChunk()
    {
        _cache.clear();
    }
}
