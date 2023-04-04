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
 * @file Chunk.h
 *
 * @brief class Chunk
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef CHUNK_H_
#define CHUNK_H_

#include <array/ChunkIterator.h>        // (grr) for a default
#include <array/ConstChunk.h>           // base class

namespace scidb
{
class Aggregate;                        // consider whether its truly appropriate for chunks to know about aggregates, which are part of query currently

/**
 * chunk implementation
 */
class Chunk : public ConstChunk
{
   double expectedDensity;

protected:
   Chunk() {
      expectedDensity = 0;
   }

public:

   /**
    * Allocate and memcpy from a raw byte array.
    */
   virtual void allocateAndCopy(char const* input, size_t byteSize, size_t count,
                                const std::shared_ptr<Query>& query);

   virtual bool isReadOnly() const {
      return false;
   }

   /**
    * Set expected sparse chunk density
    */
   void setExpectedDensity(double density) {
      expectedDensity = density;
   }

   /**
    * Get expected sparse chunk density
    */
   double getExpectedDensity() const {
      return expectedDensity;
   }

   /**
    * Decompress chunk from the specified buffer.
    * @param buf buffer containing compressed data.
    */
   virtual void decompress(const CompressedBuffer& buf);

   virtual std::shared_ptr<ChunkIterator> getIterator(std::shared_ptr<Query> const& query,
                                                        int iterationMode = ChunkIterator::NO_EMPTY_CHECK) = 0;

   virtual void merge(ConstChunk const& with, std::shared_ptr<Query> const& query);

   /**
    * This function merges at the cell level. SLOW!
    * @param[in] with   the source chunk
    * @param[in] query
    *
    * @note The caller should call merge(), instead of directly calling this.
    */
   virtual void shallowMerge(ConstChunk const& with, std::shared_ptr<Query> const& query);

   /**
    * This function tries to merge at the segment level. FAST!
    * Segment-level merging is performed if both chunks have empty-bitmap attached to the end.
    * Otherwise, shallowMerge is called.
    *
    * @param[in] with   the source chunk
    * @param[in] query
    *
    * @note The caller should call merge(), instead of directly calling this.
    * @pre The chunks must be MemChunks.
    * @pre The chunks must be in RLE format.
    */
   virtual void deepMerge(ConstChunk const& with, std::shared_ptr<Query> const& query);

   /**
    * Perform a generic aggregate-merge of this with another chunk.
    * This is an older algorithm. Currently only used by aggregating redimension.
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */
   virtual void aggregateMerge(ConstChunk const& with,
                               std::shared_ptr<Aggregate> const& aggregate,
                               std::shared_ptr<Query> const& query);

   /**
    * Perform an aggregate-merge of this with another chunk.
    * This function is optimized for current group-by aggregates, which
    * are liable to produce sparse chunks with many nulls. This method does NOT work
    * if the intermediate aggregating array is emptyable (which is what redimension uses).
    * @param[in] with chunk to merge with. Must be filled out by an aggregating op.
    * @param[in] aggregate the aggregate to use
    * @param[in] query the query context
    */
   virtual void nonEmptyableAggregateMerge(ConstChunk const& with,
                                           std::shared_ptr<Aggregate> const& aggregate,
                                           std::shared_ptr<Query> const& query);

   virtual void write(const std::shared_ptr<Query>& query) = 0;
   virtual void truncate(Coordinate lastCoord);
   virtual void setCount(size_t count);

   /**
    * @brief Indicates if this chunk is dirty from a write.  The default
    * implementation always returns false (i.e., chunk is clean)
    *
    * @return true if dirty, false if not.
    */
   virtual bool isDirty() const
   {
       return false;
   }
};

} // namespace
#endif /* CHUNK_H_ */
