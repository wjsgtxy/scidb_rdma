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
 * @file XgridArray.cpp
 *
 * @brief Xgrid array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <system/Exceptions.h>
#include "XgridArray.h"

using namespace std;

namespace scidb
{
    //
    // Xgrid chunk iterator methods
    //
    int XgridChunkIterator::getMode() const
    {
        return mode;
    }

    void XgridChunkIterator::restart()
    {
        outPos = first;
        outPos[outPos.size()-1] -= 1;
        hasCurrent = true;
        ++(*this);
    }

    void XgridChunkIterator::operator++()
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
            if (inputIterator->setPosition(inPos)) {
                hasCurrent = true;
                return;
            }
        }
    }

    bool XgridChunkIterator::setPosition(Coordinates const& newPos)
    {
        array.out2in(newPos, inPos);
        outPos = newPos;
        if (inputIterator->setPosition(inPos)) {
            return hasCurrent = true;
        }
        return hasCurrent = false;
    }

    Coordinates const& XgridChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return outPos;
    }

    Value const& XgridChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->getItem();
    }

    bool XgridChunkIterator::end()
    {
        return !hasCurrent;
    }

    bool XgridChunkIterator::isEmpty() const
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return inputIterator->isEmpty();
    }

    ConstChunk const& XgridChunkIterator::getChunk()
    {
        return chunk;
    }

    XgridChunkIterator::XgridChunkIterator(XgridArray const& arr, XgridChunk const& chk, int iterationMode)
    : array(arr),
      chunk(chk),
      outPos(arr.getArrayDesc().getDimensions().size()),
      inPos(outPos.size()),
      first(chunk.getFirstPosition(!(iterationMode & IGNORE_OVERLAPS))),
      last(chunk.getLastPosition(!(iterationMode & IGNORE_OVERLAPS))),
      inputIterator(chunk.getArrayIterator().getInputIterator()->getChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      mode(iterationMode)
    {
        restart();
    }

    //
    // Xgrid chunk methods
    //
    std::shared_ptr<ConstChunkIterator> XgridChunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(new XgridChunkIterator(array, *this, iterationMode));
    }

    void XgridChunk::initialize(Coordinates const& pos)
    {
        ArrayDesc const& desc = array.getArrayDesc();
        Address addr(attrID, pos);
        chunk.initialize(&array, &desc, addr, desc.getAttributes().findattr(attrID).getDefaultCompressionMethod());
        setInputChunk(chunk);
    }

    XgridChunk::XgridChunk(XgridArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr)
    {
    }

    //
    // Xgrid array iterator
    //
    ConstChunk const& XgridArrayIterator::getChunk()
    {
        if (!chunkInitialized) {
            ((XgridChunk&)*_chunkPtr()).initialize(getPosition());
            chunkInitialized = true;
        }
        return *_chunkPtr();
    }

    Coordinates const& XgridArrayIterator::getPosition()
    {
        array.in2out(inputIterator->getPosition(), outPos);
        return outPos;
    }

    bool XgridArrayIterator::setPosition(Coordinates const& newPos)
    {
        chunkInitialized = false;
        outPos = newPos;
        array.getArrayDesc().getChunkPositionFor(outPos);
        array.out2in(outPos, inPos);
        return inputIterator->setPosition(inPos);
    }

    XgridArrayIterator::XgridArrayIterator(XgridArray const& arr,
                                           const AttributeDesc& attrID,
                                           std::shared_ptr<ConstArrayIterator> inputIterator)
    : DelegateArrayIterator(arr, attrID, inputIterator),
      array(arr),
      inPos(arr.getArrayDesc().getDimensions().size()),
      outPos(inPos.size())
    {
    }

    //
    // Xgrid array methods
    //

    void XgridArray::out2in(Coordinates const& outPos, Coordinates& inPos)  const
    {
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = outPos.size(); i < n; i++) {
            inPos[i] = dims[i].getStartMin() + (outPos[i] - dims[i].getStartMin()) / scale[i];
        }
    }

    void XgridArray::in2out(Coordinates const& inPos, Coordinates& outPos)  const
    {
        Dimensions const& dims = desc.getDimensions();
        for (size_t i = 0, n = inPos.size(); i < n; i++) {
            outPos[i] = dims[i].getStartMin() + (inPos[i] - dims[i].getStartMin()) * scale[i];
        }
    }

    DelegateChunk* XgridArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
       return new XgridChunk(*this, *iterator, id);
    }

    DelegateArrayIterator* XgridArray::createArrayIterator(const AttributeDesc& id) const
    {
        return new XgridArrayIterator(*this, id, getPipe(0)->getConstIterator(id));
    }

    XgridArray::XgridArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array)
    : DelegateArray(desc, array),
      scale(desc.getDimensions().size())
    {
        Dimensions const& oldDims = array->getArrayDesc().getDimensions();
        Dimensions const& newDims = desc.getDimensions();
        for (size_t i = 0, n = newDims.size(); i < n; i++) {
            scale[i] = newDims[i].getLength() / oldDims[i].getLength();
        }
    }

    std::shared_ptr<Array> XgridArray::create(ArrayDesc const& partlyScaledDesc,
                                              std::vector<int32_t> const& scaleFactors,
                                              std::shared_ptr<Array>& inputArray)
    {
        // Easy case: no late scaling to perform.
        if (scaleFactors.empty()) {
            return std::shared_ptr<Array>(new XgridArray(partlyScaledDesc, inputArray));
        }

        // Need to build a new set of scaled dimensions.  We rely on scaleFactors being all 1 for
        // already-scaled dimensions.
        //
        Dimensions const& psDims = partlyScaledDesc.getDimensions();
        Dimensions const& inputDims = inputArray->getArrayDesc().getDimensions();
        size_t const nDims = psDims.size();
        Dimensions newDims(nDims);
        int64_t chunkInterval;
        for (size_t i = 0; i < nDims; ++i) {
            newDims[i] = psDims[i];
            if (scaleFactors[i] == 1) {
                // This is one of the already-scaled dimensions.
                chunkInterval = psDims[i].getChunkInterval();
            } else {
                // Late scaling needed for this autochunked dimension.
                SCIDB_ASSERT(psDims[i].isAutochunked());
                chunkInterval = inputDims[i].getChunkInterval() * scaleFactors[i];
            }
            newDims[i].setChunkInterval(chunkInterval);
        }

        return std::shared_ptr<Array>(new XgridArray(ArrayDesc(
                                                         partlyScaledDesc.getName(),
                                                         partlyScaledDesc.getAttributes(),
                                                         newDims,
                                                         partlyScaledDesc.getDistribution(),
                                                         partlyScaledDesc.getResidency()),
                                                     inputArray));
    }

} // namespace
