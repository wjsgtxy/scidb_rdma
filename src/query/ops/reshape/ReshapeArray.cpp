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
 * @file ReshapeArray.cpp
 *
 * @brief Reshape array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <query/PhysicalOperator.h>
#include <system/Exceptions.h>
#include "ReshapeArray.h"

using namespace std;

namespace scidb
{
    //
    // Reshape chunk iterator methods
    //
    int ReshapeChunkIterator::getMode() const
    {
        return mode;
    }

    void ReshapeChunkIterator::restart()
    {
        outPos = first;
        outPos[outPos.size()-1] -= 1;
        hasCurrent = true;
        ++(*this);
    }

    void ReshapeChunkIterator::operator++()
    {
        size_t nDims = outPos.size();
        while (true) {
            size_t i = nDims-1;
            while (++outPos[i] > last[i]) {
                if (i == 0) {
                    hasCurrent = false;
                    return;
                }
                outPos[i] = first[i];
                i -= 1;
            }
            array.out2in(outPos, inPos);
            if (!inputIterator || !inputIterator->getChunk().contains(inPos, !(mode & IGNORE_OVERLAPS))) {
                inputIterator.reset();
                if (arrayIterator->setPosition(inPos)) {
                    ConstChunk const& inputChunk = arrayIterator->getChunk();
                    inputIterator = inputChunk.getConstIterator(mode);
                    if (inputIterator->setPosition(inPos)) {
                        hasCurrent = true;
                        return;
                    }
                }
            } else if (inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool ReshapeChunkIterator::setPosition(Coordinates const& newPos)
    {
        outPos = newPos;
        array.out2in(newPos, inPos);
        inputIterator.reset();
        if (arrayIterator->setPosition(inPos)) {
            ConstChunk const& inputChunk = arrayIterator->getChunk();
            inputIterator = inputChunk.getConstIterator(mode);
            if (inputIterator->setPosition(inPos)) {
                return hasCurrent = true;
            }
        }
        return hasCurrent = false;
    }

    Coordinates const& ReshapeChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    Value const& ReshapeChunkIterator::getItem()
    {
         if (!hasCurrent)
             throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
         return inputIterator->getItem();
    }

    bool ReshapeChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool ReshapeChunkIterator::isEmpty() const
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    ConstChunk const& ReshapeChunkIterator::getChunk()
    {
        return chunk;
    }

    ReshapeChunkIterator::ReshapeChunkIterator(ReshapeArray const& arr, ReshapeChunk& chk, int iterationMode)
    : array(arr),
      chunk(chk),
      outPos(arr.outDims.size()),
      inPos(arr.inDims.size()),
      first(chunk.getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      last(chunk.getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      arrayIterator(chunk.getArrayIterator().getInputIterator()),
      mode(iterationMode & ~INTENDED_TILE_MODE)
    {
        restart();
    }

    //
    // Reshape chunk methods
    //
    std::shared_ptr<ConstChunkIterator> ReshapeChunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(new ReshapeChunkIterator(array, *(ReshapeChunk*)this, iterationMode));
    }

    void ReshapeChunk::initialize(Coordinates const& pos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes().findattr(attrID).getDefaultCompressionMethod());
        setInputChunk(chunk);
    }

    ReshapeChunk::ReshapeChunk(ReshapeArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {
    }

    //
    // Reshape array iterator
    //
    ConstChunk const& ReshapeArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        if (!chunkInitialized) {
            ((ReshapeChunk&)*_chunkPtr()).initialize(outPos);
            chunkInitialized = true;
        }
        return *_chunkPtr();
    }

    bool ReshapeArrayIterator::end()
    {
        return !hasCurrent;
    }

    void ReshapeArrayIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        while (true) {
            size_t i = dims.size()-1;
            while ((outPos[i] += dims[i].getChunkInterval()) > dims[i].getEndMax()) {
                if (i == 0) {
                    hasCurrent = false;
                    return;
                }
                outPos[i] = dims[i].getStartMin();
                i -= 1;
            }
            array.out2in(outPos, inPos);
            inputIterator->setPosition(inPos);
            chunkInitialized = false;
            if (!getChunk().getConstIterator(ConstChunkIterator::DEFAULT)->end()) {
                return;
            }
        }
    }

    void ReshapeArrayIterator::restart()
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        size_t nDims = dims.size();
        for (size_t i = 0; i < nDims; i++) {
            outPos[i] = dims[i].getStartMin();
        }
        outPos[nDims-1] -= dims[nDims-1].getChunkInterval();
        hasCurrent = true;
        chunkInitialized = false;
        ++(*this);
    }


    Coordinates const& ReshapeArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    bool ReshapeArrayIterator::setPosition(Coordinates const& newPos)
    {
        Dimensions const& dims = array.getArrayDesc().getDimensions();
        for (size_t i = 0, nDims = dims.size(); i < nDims; i++) {
            if (newPos[i] < dims[i].getStartMin() || newPos[i] > dims[i].getEndMax()) {
                return hasCurrent = false;
            }
        }
        outPos = newPos;
        chunkInitialized = false;
        array.getArrayDesc().getChunkPositionFor(outPos);
        array.out2in(outPos, inPos);
        inputIterator->setPosition(inPos);
        return hasCurrent = true;
    }

    ReshapeArrayIterator::ReshapeArrayIterator(ReshapeArray const& arr,
                                               const AttributeDesc& attrID,
                                               std::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.inDims.size()),
      outPos(arr.outDims.size())
    {
        restart();
    }

    //
    // Reshape array methods
    //

    inline void convertCoordinates(Coordinates const& srcPos, Dimensions const& srcDims,
                                   Coordinates& dstPos, Dimensions const& dstDims)
    {
        dstPos = PhysicalBoundaries::reshapeCoordinates(srcPos, srcDims, dstDims, /*throwOnOverflow:*/true);
    }

    void ReshapeArray::in2out(Coordinates const& inPos, Coordinates& outPos)  const
    {
        convertCoordinates(inPos, inDims, outPos, outDims);
    }

    void ReshapeArray::out2in(Coordinates const& outPos, Coordinates& inPos)  const
    {
        convertCoordinates(outPos, outDims, inPos, inDims);
    }

    DelegateChunk* ReshapeArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new ReshapeChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* ReshapeArray::createArrayIterator(const AttributeDesc& id) const
    {
        return new ReshapeArrayIterator(*this, id, getPipe(0)->getConstIterator(id));
    }

    ReshapeArray::ReshapeArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array)
    : DelegateArray(desc, array),
      inDims(array->getArrayDesc().getDimensions()),
      outDims(desc.getDimensions())
    {
    }
}
