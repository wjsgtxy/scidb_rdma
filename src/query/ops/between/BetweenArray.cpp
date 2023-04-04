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
 * @file BetweenArray.cpp
 *
 * @brief Between array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "BetweenArray.h"
#include <system/Exceptions.h>
#include <util/SpatialType.h>
#include <system/Utils.h>

namespace scidb
{
    //
    // Between chunk methods
    //
    std::shared_ptr<ConstChunkIterator> BetweenChunk::getConstIterator(int iterationMode) const
    {
        AttributeDesc const& attr = getAttributeDesc();
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;
        return std::shared_ptr<ConstChunkIterator>(
            attr.isEmptyIndicator()
            ? (attrID >= array.getPipe(0)->getArrayDesc().getAttributes().size())  // TODO EBM problematic
               ? fullyInside
                  ? (ConstChunkIterator*)new EmptyBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                  : (ConstChunkIterator*)new NewBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
               : fullyInside
                  ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                  : (ConstChunkIterator*)new ExistedBitmapBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
            : fullyInside
                ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode)
                : (ConstChunkIterator*)new BetweenChunkIterator(*this, iterationMode));
    }

    BetweenChunk::BetweenChunk(BetweenArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
    : DelegateChunk(arr, iterator, attrID, false),
      array(arr),
      myRange(arr.getArrayDesc().getDimensions().size()),
      fullyInside(false),
      fullyOutside(false)
    {
        tileMode = false;
    }

    void BetweenChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        myRange._low = inputChunk.getFirstPosition(true);
        myRange._high = inputChunk.getLastPosition(true);

        // TO-DO: the fullyInside computation is simple but not optimal.
        // It is possible that the current chunk is fully inside the union of the specified ranges,
        // although not fully contained in any of them.
        size_t dummy = 0;
        fullyInside  =  array._spatialRangesPtr->findOneThatContains(myRange, dummy);
        fullyOutside = !array._spatialRangesPtr->findOneThatIntersects(myRange, dummy);

        isClone = fullyInside && attrID < array.getPipe(0)->getArrayDesc().getAttributes().size();
        if (emptyBitmapIterator) {
            if (!emptyBitmapIterator->setPosition(inputChunk.getFirstPosition(false)))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    Value const& BetweenChunkIterator::getItem()
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return inputIterator->getItem();
    }

    bool BetweenChunkIterator::isEmpty() const
    {
        if (!hasCurrent) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return inputIterator->isEmpty() ||
            !array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges);
    }

    bool BetweenChunkIterator::end()
    {
        return !hasCurrent;
    }

    void BetweenChunkIterator::operator ++()
    {
        while (true) {
            ++(*inputIterator);
            if (!inputIterator->end()) {
                Coordinates const& pos = inputIterator->getPosition();

                if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                    currPos = pos;
                    hasCurrent = true;
                    return;
                }
            } else {
                break;
            }
        }
        hasCurrent = false;
    }

    Coordinates const& BetweenChunkIterator::getPosition()
    {
        return currPos;
    }

    bool BetweenChunkIterator::setPosition(Coordinates const& pos)
    {
        if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
            hasCurrent = inputIterator->setPosition(pos);
            if (hasCurrent) {
                currPos = pos;
            }
            return hasCurrent;
        }
        hasCurrent = false;
        return false;
    }

    void BetweenChunkIterator::restart()
    {
        inputIterator->restart();
        if (!inputIterator->end()) {
            Coordinates const& pos = inputIterator->getPosition();
            if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                currPos = pos;
                hasCurrent = true;
                return;
            }
            else {
                ++(*this);  // The operator++() will skip the cells outside all the requested ranges.
            }
        }
        else {
            hasCurrent = false;
        }
    }

    ConstChunk const& BetweenChunkIterator::getChunk()
    {
        return chunk;
    }

    BetweenChunkIterator::BetweenChunkIterator(BetweenChunk const& aChunk, int iterationMode)
    : CoordinatesMapper(aChunk),
      array(aChunk.array),
      chunk(aChunk),
      inputIterator(aChunk.getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      currPos(array.getArrayDesc().getDimensions().size()),
      _mode(iterationMode & ~INTENDED_TILE_MODE & ~TILE_MODE),
      type(chunk.getAttributeDesc().getType()),
      _hintForSpatialRanges(0)
    {
        restart();
    }

    //
    // Exited bitmap chunk iterator methods
    //
    Value const& ExistedBitmapBetweenChunkIterator::getItem()
    {
        _value.setBool(
                inputIterator->getItem().getBool() &&
                array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges));
        return _value;
    }

    ExistedBitmapBetweenChunkIterator::ExistedBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode)
    : BetweenChunkIterator(chunk, iterationMode),
      _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // New bitmap chunk iterator methods
    //
    Value const& NewBitmapBetweenChunkIterator::getItem()
    {
        _value.setBool(array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges));
        return _value;
    }

    NewBitmapBetweenChunkIterator::NewBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode)
    : BetweenChunkIterator(chunk, iterationMode),
      _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // Empty bitmap chunk iterator methods
    //
    Value const& EmptyBitmapBetweenChunkIterator::getItem()
    {
        return _value;
    }

    bool EmptyBitmapBetweenChunkIterator::isEmpty() const
    {
        return false;
    }

    EmptyBitmapBetweenChunkIterator::EmptyBitmapBetweenChunkIterator(BetweenChunk const& chunk, int iterationMode)
    : NewBitmapBetweenChunkIterator(chunk, iterationMode)
    {
        _value.setBool(true);
    }

    //
    // Between array iterator methods
    //
    BetweenArrayIterator::BetweenArrayIterator(BetweenArray const& arr,
                                               const AttributeDesc& attrID,
                                               const AttributeDesc& inputAttrID)
    : DelegateArrayIterator(arr, attrID, arr.getPipe(0)->getConstIterator(inputAttrID)),
      array(arr),
      pos(arr.getArrayDesc().getDimensions().size()),
      _hintForSpatialRanges(0)
    {
        _spatialRangesChunkPosIteratorPtr = std::shared_ptr<SpatialRangesChunkPosIterator>(
                new SpatialRangesChunkPosIterator(array._spatialRangesPtr, array.getArrayDesc()));
        restart();
    }

    bool BetweenArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& BetweenArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return pos;
    }

    bool BetweenArrayIterator::setPosition(Coordinates const& newPos)
    {
        Coordinates newChunkPos = newPos;
        array.getArrayDesc().getChunkPositionFor(newChunkPos);

        if (hasCurrent && pos == newChunkPos) {
            return true;
        }

        // If cannot set position in the inputIterator, fail.
        if (! inputIterator->setPosition(newChunkPos)) {
            hasCurrent = false;
            return false;
        }

        // If the position does not correspond to a chunk intersecting some query range, fail.
        if (!array._extendedSpatialRangesPtr->findOneThatContains(newChunkPos, _hintForSpatialRanges)) {
            hasCurrent = false;
            return false;
        }

        // Set position there.
        hasCurrent = true;
        chunkInitialized = false;
        pos = newChunkPos;
        if (_spatialRangesChunkPosIteratorPtr->end() || _spatialRangesChunkPosIteratorPtr->getPosition() > pos) {
            _spatialRangesChunkPosIteratorPtr->restart();
        }
        _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == pos);

        return true;
    }

    void BetweenArrayIterator::advanceToNextChunkInRange()
    {
        assert(!inputIterator->end() && !_spatialRangesChunkPosIteratorPtr->end());

        hasCurrent = false;
        chunkInitialized = false;

        while (!inputIterator->end())
        {
            // Increment inputIterator.
            ++(*inputIterator);
            if (inputIterator->end()) {
                assert(hasCurrent == false);
                return;
            }
            pos = inputIterator->getPosition();
            if (array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                hasCurrent = true;
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
                assert(_spatialRangesChunkPosIteratorPtr->getPosition() == pos);
                return;
            }

            // Incrementing inputIterator led to a position outside the spatial ranges.
            // We could keep incrementing inputIterator till we find a chunkPos inside a query range, but that
            // can be too slow.
            // So let's try to increment spatialRangesChunkPosIterator also, in every iteration.
            // Whenever one of them (inputIterator or spatialRangesChunkPosIterator) gets there first
            // (i.e. finds a position the other one "like"), the algorithm declares victory.
            //
            // Another note:
            // If advancePositionToAtLeast(pos) advances to a position > pos, we cannot increment spatialRangesChunkPosIterator.
            // The reason is that this new position has not been checked against inputIterator for validity yet, and it will
            // be a mistake to blindly skip it.
            //
            bool advanced = _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
            if (_spatialRangesChunkPosIteratorPtr->end()) {
                assert(hasCurrent == false);
                return;
            }
            if (! (advanced && _spatialRangesChunkPosIteratorPtr->getPosition() > pos)) {
                ++(*_spatialRangesChunkPosIteratorPtr);
                if (_spatialRangesChunkPosIteratorPtr->end()) {
                    assert(hasCurrent == false);
                    return;
                }
            }
            Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
            if (inputIterator->setPosition(myPos)) {
                // The position suggested by _spatialRangesChunkPosIterator exists in inputIterator.
                // Declare victory!
                pos = myPos;
                hasCurrent = true;
                return;
            }
            else {
                // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
                // Restore to its previous valid state (even though not in any query range).
                bool restored = inputIterator->setPosition(pos);
                SCIDB_ASSERT(restored);
            }
        }
    }

    void BetweenArrayIterator::operator ++()
    {
        assert(!end());
        assert(!inputIterator->end() && hasCurrent && !_spatialRangesChunkPosIteratorPtr->end());
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == inputIterator->getPosition());

        advanceToNextChunkInRange();
    }

    void BetweenArrayIterator::restart()
    {
        chunkInitialized = false;
        inputIterator->restart();
        _spatialRangesChunkPosIteratorPtr->restart();

        // If any of the two iterators is invalid, fail.
        if (inputIterator->end() || _spatialRangesChunkPosIteratorPtr->end())
        {
            hasCurrent = false;
            return;
        }

        // Is inputIterator pointing to a position intersecting some query range?
        pos = inputIterator->getPosition();
        hasCurrent = array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges);
        if (hasCurrent) {
            assert(pos >= _spatialRangesChunkPosIteratorPtr->getPosition());
            if (pos > _spatialRangesChunkPosIteratorPtr->getPosition()) {
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
                assert(!_spatialRangesChunkPosIteratorPtr->end() && pos == _spatialRangesChunkPosIteratorPtr->getPosition());
            }
            return;
        }

        // Is spatialRangesChunkPosIterator pointing to a position that has data?
        Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
        if (inputIterator->setPosition(myPos)) {
            // The position suggested by _spatialRangesChunkPosIterator exists in inputIterator.
            // Declare victory!
            pos = myPos;
            hasCurrent = true;
            return;
        }
        else {
            // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
            // Restore to its previous valid state (even though not in any query range).
            bool restored = inputIterator->setPosition(pos);
            SCIDB_ASSERT(restored);
        }

        advanceToNextChunkInRange();
    }

    //
    // Between array methods
    //
    BetweenArray::BetweenArray(ArrayDesc const& array, SpatialRangesPtr const& spatialRangesPtr, std::shared_ptr<Array> const& input)
    : DelegateArray(array, input),
      _spatialRangesPtr(spatialRangesPtr)
    {
        // Copy _spatialRangesPtr to extendedSpatialRangesPtr, but reducing low by (interval-1) to cover chunkPos.
        _extendedSpatialRangesPtr = std::make_shared<SpatialRanges>(_spatialRangesPtr->numDims());
        auto const& ranges = _spatialRangesPtr->ranges();
        for (size_t i=0; i < ranges.size(); ++i) {
            Coordinates newLow = ranges[i]._low;
            array.getChunkPositionFor(newLow);
            _extendedSpatialRangesPtr->insert(SpatialRange(newLow, ranges[i]._high));
        }
        _extendedSpatialRangesPtr->buildIndex();
    }

    DelegateArrayIterator* BetweenArray::createArrayIterator(const AttributeDesc& attrID) const
    {
        AttributeDesc inputAttrID = attrID;
        const auto& attrs = getPipe(0)->getArrayDesc().getAttributes();
        if (attrID.getId() >= attrs.size()) {
            inputAttrID = attrs.firstDataAttribute();
        }
        return new BetweenArrayIterator(*this, attrID, inputAttrID);
    }

    DelegateChunk* BetweenArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new BetweenChunk(*this, *iterator, attrID);
    }
}
