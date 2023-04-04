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
 * @file ShiftArray.cpp
 *
 * @brief Shift array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#ifndef SHIFT_ARRAY_H
#define SHIFT_ARRAY_H

#include <array/DelegateArray.h>
#include <array/MemArray.h>

namespace scidb {


class ShiftArray;
class ShiftArrayIterator;
class ShiftChunk;
class ShiftChunkIterator;

class ShiftChunkIterator : public DelegateChunkIterator
{
    ShiftArray const& array;
    Coordinates outPos;
    Coordinates inPos;
  public:
    virtual bool   setPosition(Coordinates const& pos);
    virtual Coordinates const& getPosition();

    ShiftChunkIterator(ShiftArray const& array, DelegateChunk const* chunk, int iterationMode);
};

class ShiftChunk : public DelegateChunk
{
    ShiftArray const& array;
    Coordinates firstPos;
    Coordinates lastPos;
  public:
    void setInputChunk(ConstChunk const& inputChunk);
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;

    ShiftChunk(ShiftArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class ShiftArrayIterator : public DelegateArrayIterator
{
    ShiftArray const& array;
    Coordinates inPos;
    Coordinates outPos;

  public:
    virtual Coordinates const& getPosition();
    virtual bool setPosition(Coordinates const& pos);

	ShiftArrayIterator(ShiftArray const& array,
                       const AttributeDesc& attrID,
                       std::shared_ptr<ConstArrayIterator> inputIterator);
};

class ShiftArray : public DelegateArray
{
    friend class ShiftChunk;
    friend class ShiftChunkIterator;
    friend class ShiftArrayIterator;

    size_t const _N_DIMS;
    Coordinates _in2outOffset;
    bool _byOffset { false };

    void in2out(Coordinates const& inPos, Coordinates& outPos) const;
    void out2in(Coordinates const& outPos, Coordinates& inPos) const;

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    /**
     * @brief Shift input array by origin.
     *
     * @details The input and output array dimension descriptors have
     * the same chunk lengths but different startMin values.  We want
     * to shift the input chunks by the difference in startMin values,
     * so that they keep their relative positions, but are now based
     * off the startMin origin of the output array descriptor.  For
     * example:
     *
     * - Input has dimensions [x=0:180; y=0:360].
     * - Output has dimensions [lat=-90:90; lon=-180:180].
     * - Input cell at (p, q) becomes output cell (p - 90, q - 180).
     */
    ShiftArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array);

    /**
     * @brief Shift input array by chunk-wise offset.
     *
     * @details We want to shift input chunks to a different position
     * via geometric translation using an explicit offset coordinate
     * vector.  There are some caveats, however:
     *
     * - The per-dimension offsets must be multiples of the respective
     *   chunk lengths, otherwise chunk rewriting is needed (and since
     *   ShiftArray doesn't do that, you have some code to write!).
     *
     * - The shifted chunks must lie within the boundaries of the
     *   output dimensions.  If not, an exception will be thrown when
     *   the bounds are exceeded.
     */
    ShiftArray(ArrayDesc const& desc,
               std::shared_ptr<Array> const& array,
               Coordinates const& offset);
};

}

#endif
