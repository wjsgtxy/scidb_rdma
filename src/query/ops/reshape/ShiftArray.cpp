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

#include "ShiftArray.h"

using namespace std;

namespace scidb
{
    //
    // Shift chunk iterator methods
    //
    bool ShiftChunkIterator::setPosition(Coordinates const& newPos)
    {
        array.out2in(newPos, inPos);
        return inputIterator->setPosition(inPos);
    }

    Coordinates const& ShiftChunkIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }

    ShiftChunkIterator::ShiftChunkIterator(ShiftArray const& arr, DelegateChunk const* chunk, int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      array(arr),
      outPos(arr._N_DIMS),
      inPos(arr._N_DIMS)
    {
    }

    //
    // Shift chunk methods
    //

    Coordinates const& ShiftChunk::getFirstPosition(bool withOverlap) const
    {
        return firstPos;
    }

    Coordinates const& ShiftChunk::getLastPosition(bool withOverlap) const
    {
        return lastPos;
    }

    void ShiftChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        isClone = true;
        array.in2out(inputChunk.getFirstPosition(false), firstPos);
        array.in2out(inputChunk.getLastPosition(false), lastPos);
    }

    ShiftChunk::ShiftChunk(ShiftArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr),
      firstPos(arr._N_DIMS),
      lastPos(arr._N_DIMS)
    {
    }

    //
    // Shift array iterator
    //
    Coordinates const& ShiftArrayIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }

    bool ShiftArrayIterator::setPosition(Coordinates const& newPos)
    {
        chunkInitialized = false;
        array.out2in(newPos, inPos);
        return inputIterator->setPosition(inPos);
    }

    ShiftArrayIterator::ShiftArrayIterator(ShiftArray const& arr,
                                           const AttributeDesc& attrID,
                                           std::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr._N_DIMS),
      outPos(arr._N_DIMS)
    {
        try {
            restart();
        }
        catch (Exception const& ex) {
            if (ex.getLongErrorCode() != SCIDB_LE_NOT_IMPLEMENTED) {
                ex.raise();
            }
            // ...else input array doesn't support it.  Oh well, we tried.
        }
    }

    //
    // Shift array methods
    //

    void ShiftArray::in2out(Coordinates const& inPos, Coordinates& outPos)  const
    {
        for (size_t i = 0; i < _N_DIMS; ++i) {
            outPos[i] = inPos[i] + _in2outOffset[i];
        }

        if (_byOffset) {
            // Explicitly provided offset must not position chunks
            // outside of the destination dimension space!
            Dimensions const& outDims = desc.getDimensions();
            for (size_t i = 0; i < _N_DIMS; ++i) {
                if (outPos[i] < outDims[i].getStartMin() || outPos[i] > outDims[i].getEndMax()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                        << CoordsToStr(outPos) << outDims;
                }
            }
        }
    }

    void ShiftArray::out2in(Coordinates const& outPos, Coordinates& inPos)  const
    {
        for (size_t i = 0; i < _N_DIMS; ++i) {
            inPos[i] = outPos[i] - _in2outOffset[i];
        }
    }

    DelegateChunkIterator* ShiftArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        return new ShiftChunkIterator(*this, chunk, iterationMode);
    }

    DelegateChunk* ShiftArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new ShiftChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* ShiftArray::createArrayIterator(const AttributeDesc& id) const
    {
        return new ShiftArrayIterator(*this, id, getPipe(0)->getConstIterator(id));
    }

    ShiftArray::ShiftArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array)
        : DelegateArray(desc, array)
        , _N_DIMS(desc.getDimensions().size())
        , _in2outOffset(_N_DIMS)
    {
        Dimensions const& inDims = array->getArrayDesc().getDimensions();
        Dimensions const& outDims = desc.getDimensions();

        SCIDB_ASSERT(inDims.size() == outDims.size());
        for (size_t i = 0; i < inDims.size(); ++i) {
            _in2outOffset[i] = outDims[i].getStartMin() - inDims[i].getStartMin();
        }
    }

    ShiftArray::ShiftArray(ArrayDesc const& desc,
                           std::shared_ptr<Array> const& array,
                           Coordinates const& offset)
        : DelegateArray(desc, array)
        , _N_DIMS(offset.size())
        , _in2outOffset(offset)
        , _byOffset(true)
    {
        Dimensions const& inDims = array->getArrayDesc().getDimensions();
        Dimensions const& outDims = desc.getDimensions();

        SCIDB_ASSERT(inDims.size() == outDims.size());
        SCIDB_ASSERT(_N_DIMS == inDims.size());
        for (size_t i = 0; i < _N_DIMS; ++i) {
            int64_t ci = inDims[i].getChunkInterval();
            SCIDB_ASSERT(ci > 0);
            ASSERT_EXCEPTION((_in2outOffset[i] % ci) == 0, __func__
                             << ": Offset " << CoordsToStr(_in2outOffset) << '['
                             << i << "] not a multiple of input chunk interval " << ci);
        }
    }

} // namespace scidb
