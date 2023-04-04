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
 * @file ConstChunk.h
 *
 * @brief class ConstChunk
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h ChunkBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef CONST_CHUNK_H_
#define CONST_CHUNK_H_

#include <log4cxx/logger.h>

#include <array/ConstArrayIterator.h>   // member
#include <array/ConstChunkIterator.h>   // (grr) unfactored default parameter
#include <array/ChunkBuffer.h>          // base class
#include <array/MaterializedChunkCache.h>
#include <util/compression/CompressorType.h>


namespace scidb
{
class Array;
class ArrayDesc;
class AttributeDesc;
class Chunk;
class CompressedBuffer;
class ConstRLEEmptyBitmap;
class RLEPayload;

/**
 * A read only chunk interface provides information on whether the chunk is:
 *   readonly - isReadOnly()
 *   positions:
 *      getFirstPosition(withOverlap) - provides the smallest position in stride-major order
 *      getLastPosition(withOverlap) - provides the largest position in stride-major order
 *      positions can be computed with or without taking overlap items into account
 *  Also the chunk can be:
 *  An iterator can be requested to access the items in the chunk:
 *      getConstIterator() - returns a read-only iterator to the items
 *      getIterator() - returns a volatile iterator to the items (the chunk cannot be read-only)
 */
class ConstChunk : public ChunkBuffer          // TODO: probably should be has-a (not is-a)
{
public:
    const uint64_t* getChunkMagic() const;

    /**
     * Check if this is MemChunk.
     */
    virtual bool isMemChunk() const
    {
        return false;
    }

   virtual bool isReadOnly() const;

   /**
    * Check if chunk data is stored somewhere (in memory or on disk).
    */
   virtual bool isMaterialized() const;

    /**
     * If chunk is a "closure", return size of the appended bitmap, else zero.
     */
    size_t getBitmapSize() const;

   /**
    * Get array descriptor
    */
   virtual const ArrayDesc& getArrayDesc() const = 0;

   /**
    * Get chunk attribute descriptor
    */
   virtual const AttributeDesc& getAttributeDesc() const = 0;

   /**
    * Count number of present (non-empty) elements in the chunk.
    * @return the number of non-empty elements in the chunk.
    * @note Materialized subclasses that do not use the field materializedChunk might want to
    *       provide their own implementation.
    */
   virtual size_t count() const;

   /**
    * Check if count of non-empty elements in the chunk is known.
    * @return true if count() will run in constant time; false otherwise.
    * @note Materialized subclasses that do not use the field materializedChunk might want to
    *       provide their own implementation.
    */
   virtual bool isCountKnown() const;

   /**
    * Get number of logical elements in the chunk.
    * @return the product of the chunk sizes in all dimensions.
    */
   size_t getNumberOfElements(bool withOverlap) const;

    /**
     * If chunk contains no gaps in its data: has no overlaps and fully belongs to non-emptyable array.
     */
   bool isSolid() const;

   virtual Coordinates const& getFirstPosition(bool withOverlap) const = 0;
   virtual Coordinates const& getLastPosition(bool withOverlap) const = 0;

   bool contains(Coordinates const& pos, bool withOverlap) const;

   virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS) const = 0;

   ConstChunkIterator* getConstIteratorPtr(int iterationMode = ConstChunkIterator::IGNORE_OVERLAPS)
   {
      return getConstIterator(iterationMode).operator->();
   }

   virtual CompressorType getCompressionMethod() const = 0;

   /**
    * Compress chunk data info the specified buffer.
    * @param buf buffer where compressed data will be placed
    * @details Buffer is intended to be initialized using default constructor and will be filled by
    * this method.
    */
    virtual void compress(CompressedBuffer& buf,
                          std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                          bool forceUncompressed) const;

    void* getWriteDataImpl() override;
    const void* getConstDataImpl() const override;
    size_t getSize() const override;
    bool pin() const override;
    void unPin() const override;

    virtual Array const& getArray() const = 0;

    void makeClosure(Chunk& closure, std::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const;

    /**
     * Clone the chunk data into a new RLEPayload instance.
     * @param destination the RLEPayload instance to populate.
     */
    void cloneRLEPayload(RLEPayload& destination) const;

    virtual std::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const;
    virtual ConstChunk const* getBitmapChunk() const;

    /**
     * Compute and place the chunk data in memory (if needed) and return a pointer to it.
     * @return a pointer to a chunk object that is materialized; may be a pointer to this.
     */
    virtual ConstChunk* materialize() const;

    virtual void overrideTileMode(bool) {}

    /**
     * @return true if the chunk has no cells
     * @param withOverlap true if the overlap region(s) should be included (default)
     */
    virtual bool isEmpty(bool withOverlap=true) const
    {
        int iterationMode = ConstChunkIterator::DEFAULT;
        if (!withOverlap) {
            iterationMode |= ConstChunkIterator::IGNORE_OVERLAPS;
        }
        std::shared_ptr<ConstChunkIterator> ci = getConstIterator(iterationMode);
        assert(ci);
        return (ci->end());
    }

    /**
     * Register the presence of a chunk iterator using this chunk
     * Return true if unregister is necessary.
     */
    virtual bool registerIterator(ConstChunkIterator& ci)
    {
        return false;
    }

    /**
     * Unregister the presence of a chunk iterator using this chunk
     */
    virtual void unregisterIterator(ConstChunkIterator& ci)
    {}

protected:
    ConstChunk();
    virtual ~ConstChunk();

    bool isMaterializedChunkPresent() const;
    void releaseMaterializedChunk();

private:
    mutable MaterializedChunkCache _cache;
    std::shared_ptr<ConstArrayIterator> emptyIterator;
};

} // namespace
#endif /* CONST_CHUNK_H_ */
