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
 * @file CrossJoinArray.cpp
 *
 * @brief CrossJoin array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#include <array/MemArray.h>
#include <system/Exceptions.h>
#include "CrossJoinArray.h"

using namespace std;

namespace scidb
{
    //
    // CrossJoin chunk methods
    //
    const ArrayDesc& CrossJoinChunk::getArrayDesc() const
    {
        return array.desc;
    }

    Array const& CrossJoinChunk::getArray() const
    {
        return array;
    }

    const AttributeDesc& CrossJoinChunk::getAttributeDesc() const
    {
        return array.desc.getAttributes().findattr(attr);
    }

    CompressorType CrossJoinChunk::getCompressionMethod() const
    {
        return leftChunk->getCompressionMethod();
    }

    Coordinates const& CrossJoinChunk::getFirstPosition(bool withOverlap) const
    {
        return withOverlap ? firstPosWithOverlap : firstPos;
    }

    Coordinates const& CrossJoinChunk::getLastPosition(bool withOverlap) const
    {
       return withOverlap ? lastPosWithOverlap : lastPos;
     }

    std::shared_ptr<ConstChunkIterator> CrossJoinChunk::getConstIterator(int iterationMode) const
    {
        return std::shared_ptr<ConstChunkIterator>(new CrossJoinChunkIterator(*this, iterationMode));
    }

    CrossJoinChunk::CrossJoinChunk(CrossJoinArray const& cross, AttributeID attrID, bool isLeftAttr)
    : array(cross), attr(attrID), isLeftAttribute(isLeftAttr)
    {
        isEmptyIndicatorAttribute = getAttributeDesc().isEmptyIndicator();
    }

    void CrossJoinChunk::setInputChunk(ConstChunk const* left, ConstChunk const* right)
    {
        leftChunk  = left;
        rightChunk = right;

        firstPos = array.getPosition(left->getFirstPosition(false), right->getFirstPosition(false));
        firstPosWithOverlap = array.getPosition(left->getFirstPosition(true), right->getFirstPosition(true));
        lastPos  = array.getPosition(left->getLastPosition(false), right->getLastPosition(false));
        lastPosWithOverlap  = array.getPosition(left->getLastPosition(true), right->getLastPosition(true));
    }

    bool CrossJoinChunk::isMaterialized() const
    {
        return false;
    }

    //
    // CrossJoin chunk iterator methods
    //
    int CrossJoinChunkIterator::getMode() const
    {
        return leftIterator->getMode();
    }

    Value const& CrossJoinChunkIterator::getItem()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if (chunk.isEmptyIndicatorAttribute)
        {
            boolValue.setBool(!isEmpty());
            return boolValue;
        }

        return chunk.isLeftAttribute
            ? leftIterator->getItem()
            : (*currentBucket)[currentIndex].second;
    }

    bool CrossJoinChunkIterator::isEmpty() const
    {
        return false;
    }

    bool CrossJoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    void CrossJoinChunkIterator::operator ++()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        if( ++currentIndex >= (ssize_t) currentBucket->size())
        {
            Coordinates joinKey(array.nJoinDims);
            ++(*leftIterator);
            while (!leftIterator->end())
            {
                array.decomposeLeftCoordinates(leftIterator->getPosition(), joinKey);

                ChunkHash::const_iterator it = rightHash.find(joinKey);
                if (it!=rightHash.end())
                {
                    currentBucket = &(it->second);
                    currentIndex = 0;
                    return;
                }
                ++(*leftIterator);
            }
            hasCurrent = false;
        }
    }

    Coordinates const& CrossJoinChunkIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        array.composeOutCoordinates(leftIterator->getPosition(), (*currentBucket)[currentIndex].first, currentPos);
        return currentPos;
    }

    bool CrossJoinChunkIterator::setPosition(Coordinates const& pos)
    {
        Coordinates left(array.nLeftDims);
        Coordinates joinKey(array.nJoinDims);
        Coordinates rightLeftover(array.nRightDims - array.nJoinDims);
        array.decomposeOutCoordinates(pos, left, joinKey, rightLeftover);

        if(!leftIterator->setPosition(left))
        {
            return hasCurrent = false;
        }

        ChunkHash::const_iterator it = rightHash.find(joinKey);
        if (it==rightHash.end())
        {
            return hasCurrent = false;
        }

        currentBucket = &(it->second);
        currentIndex = findValueInBucket(currentBucket, rightLeftover);

        return hasCurrent = (currentIndex!=-1);
    }

    void CrossJoinChunkIterator::restart()
    {
        hasCurrent = false;
        leftIterator->restart();
        Coordinates joinKey(array.nJoinDims);
        while (!leftIterator->end())
        {
            array.decomposeLeftCoordinates(leftIterator->getPosition(), joinKey);
            ChunkHash::const_iterator it = rightHash.find(joinKey);
            if (it!=rightHash.end())
            {
                currentBucket = &(it->second);
                currentIndex = 0;
                hasCurrent = true;
                return;
            }

            ++(*leftIterator);
        }
    }

    ConstChunk const& CrossJoinChunkIterator::getChunk()
    {
        return chunk;
    }

    CrossJoinChunkIterator::CrossJoinChunkIterator(CrossJoinChunk const& aChunk, int iterationMode)
    : array(aChunk.array),
      chunk(aChunk),
      leftIterator(aChunk.leftChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      currentPos(aChunk.array.desc.getDimensions().size()),
      currentBucket(0),
      currentIndex(-1)
    {
        rightHash.clear();
        std::shared_ptr<ConstChunkIterator> iter = aChunk.rightChunk->getConstIterator(iterationMode & ~INTENDED_TILE_MODE);
        Coordinates joinKey(array.nJoinDims);
        Coordinates rightLeftover(array.nRightDims - array.nJoinDims);
        while(!iter->end())
        {
            array.decomposeRightCoordinates(iter->getPosition(), joinKey, rightLeftover);
            rightHash[joinKey].push_back( make_pair(rightLeftover, iter->getItem()));
            ++(*iter);
        }

        restart();
    }

    ssize_t CrossJoinChunkIterator::findValueInBucket(HashBucket const* bucket, Coordinates const& coords) const
    {
        CoordinatesLess cless;
        size_t l = 0, r = bucket->size();
        while (l < r) {
            size_t m = (l + r) >> 1;
            if ( cless((*bucket)[m].first, coords))
            {
                l = m + 1;
            }
            else
            {
                r = m;
            }
        }

        if (r < bucket->size() && (*bucket)[r].first == coords)
        {
            return r;
        }

        return -1;
    }

    //
    // CrossJoin array iterator methods
    //
    CrossJoinArrayIterator::CrossJoinArrayIterator(CrossJoinArray const& cross, AttributeID attrID,
                                                   std::shared_ptr<ConstArrayIterator> left,
                                                   std::shared_ptr<ConstArrayIterator> right,
                                                   std::shared_ptr<ConstArrayIterator> input)
    : ConstArrayIterator(cross),
      array(cross),
      attr(attrID),
      leftIterator(left),
      rightIterator(right),
      inputIterator(input),
      chunk(cross, attrID, input == left)
    {
        restart();
    }

    ConstChunk const& CrossJoinArrayIterator::getChunk()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        if (!chunkInitialized) {
            chunk.setInputChunk(&leftIterator->getChunk(), &rightIterator->getChunk());
            chunkInitialized = true;
        }
        return chunk;
    }

    bool CrossJoinArrayIterator::end()
    {
        return !hasCurrent;
    }

    void CrossJoinArrayIterator::operator ++()
    {
        if (hasCurrent) {
            chunkInitialized = false;
            ++(*rightIterator);
            do {
                while (!rightIterator->end()) {
                    if (array.matchPosition(leftIterator->getPosition(), rightIterator->getPosition())) {
                        return;
                    }
                    ++(*rightIterator);
                }
                rightIterator->restart();
                ++(*leftIterator);
            } while (!leftIterator->end());

            hasCurrent = false;
        }
    }

    Coordinates const& CrossJoinArrayIterator::getPosition()
    {
        currentPos = array.getPosition(leftIterator->getPosition(), rightIterator->getPosition());
        return currentPos;
    }

    bool CrossJoinArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        return hasCurrent = leftIterator->setPosition(array.getLeftPosition(pos))
            && rightIterator->setPosition(array.getRightPosition(pos));
    }

    void CrossJoinArrayIterator::restart()
    {
        chunkInitialized = false;
        hasCurrent = false;
        leftIterator->restart();
        while (!leftIterator->end())  {
            rightIterator->restart();
            while (!rightIterator->end()) {
                if (array.matchPosition(leftIterator->getPosition(), rightIterator->getPosition())) {
                    hasCurrent = true;
                    return;
                }
                ++(*rightIterator);
            }
            ++(*leftIterator);
        }
    }

    //
    // CrossJoin array methods
    //
    CrossJoinArray::CrossJoinArray(const ArrayDesc& d,
                                   const std::shared_ptr<Array>& leftArray,
                                   const std::shared_ptr<Array>& rightArray,
                                   vector<int> const& ljd,
                                   vector<int> const& rjd)
    : Array(leftArray, rightArray),
      desc(d),
      leftDesc(leftArray->getArrayDesc()),
      rightDesc(rightArray->getArrayDesc()),
      nLeftDims(safe_static_cast<AttributeID>(leftDesc.getDimensions().size())),
      nRightDims(safe_static_cast<AttributeID>(rightDesc.getDimensions().size())),
      nLeftAttrs(safe_static_cast<AttributeID>(leftDesc.getAttributes().size())),
      nRightAttrs(safe_static_cast<AttributeID>(rightDesc.getAttributes().size())),
      leftJoinDims(ljd),
      rightJoinDims(rjd),
      nJoinDims(0)
    {
        for(size_t i=0; i<leftJoinDims.size(); i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                nJoinDims++;
            }
        }

        leftEmptyTagPosition = leftDesc.getEmptyBitmapAttribute();
        rightEmptyTagPosition = rightDesc.getEmptyBitmapAttribute();
    }

    bool CrossJoinArray::matchPosition(Coordinates const& left, Coordinates const& right) const
    {
        for (size_t r = 0; r < nRightDims; r++) {
            int l = rightJoinDims[r];
            if (l >= 0) {
                if (left[l] != right[r]) {
                    return false;
                }
            }
        }
        return true;
    }

    void CrossJoinArray::decomposeRightCoordinates(Coordinates const& right, Coordinates& hashKey, Coordinates &rightLeftover) const
    {
        assert(hashKey.size() == nJoinDims);
        assert(rightLeftover.size() == nRightDims - nJoinDims);
        assert(right.size() == nRightDims);

        size_t k=0;
        size_t j=0;
        for(size_t i=0; i<nRightDims; i++)
        {
            if(rightJoinDims[i]!=-1)
            {
                hashKey[k++]=right[i];
            }
            else
            {
                rightLeftover[j++]=right[i];
            }
        }
    }

    void CrossJoinArray::decomposeOutCoordinates(Coordinates const& out, Coordinates& left, Coordinates& hashKey, Coordinates& rightLeftover) const
    {
        assert(out.size() == desc.getDimensions().size());
        assert(left.size() == nLeftDims);
        assert(rightLeftover.size() == nRightDims-nJoinDims);
        assert(hashKey.size() == nJoinDims);

        left.assign(out.begin(), out.begin()+nLeftDims);
        rightLeftover.assign(out.begin()+nLeftDims, out.end());

        for (size_t i =0; i<nLeftDims; i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                hashKey[leftJoinDims[i]] = out[i];
            }
        }
    }

    void CrossJoinArray::decomposeLeftCoordinates(Coordinates const& left, Coordinates& hashKey) const
    {
        assert(left.size() == nLeftDims);
        assert(hashKey.size() == nJoinDims);

        for (size_t i =0; i<nLeftDims; i++)
        {
            if(leftJoinDims[i]!=-1)
            {
                hashKey[leftJoinDims[i]] = left[i];
            }
        }
    }

    void CrossJoinArray::composeOutCoordinates(Coordinates const &left, Coordinates const& rightLeftover, Coordinates& out) const
    {
        assert(left.size() == nLeftDims);
        assert(rightLeftover.size() == nRightDims - nJoinDims);
        assert(out.size() == desc.getDimensions().size());

        memcpy(&out[0], &left[0], nLeftDims*sizeof(Coordinate));
        memcpy(&out[left.size()], &rightLeftover[0], (nRightDims-nJoinDims)*sizeof(Coordinate));
    }

    Coordinates CrossJoinArray::getLeftPosition(Coordinates const& pos) const
    {
        return Coordinates(pos.begin(), pos.begin() + nLeftDims);
    }

    Coordinates CrossJoinArray::getRightPosition(Coordinates const& pos) const
    {
        Coordinates rightPos(nRightDims);
        for (size_t r = 0, i = nLeftDims; r < nRightDims; r++) {
            int l = rightJoinDims[r];
            rightPos[r] = (l >= 0) ? pos[l] : pos[i++];
        }
        return rightPos;
    }


    Coordinates CrossJoinArray::getPosition(Coordinates const& left, Coordinates const& right) const
    {
        Coordinates pos(desc.getDimensions().size());
        for (size_t l = 0; l < nLeftDims; l++) {
            pos[l] = left[l];
        }
         for (size_t r = 0, i = nLeftDims; r < nRightDims; r++) {
            if (rightJoinDims[r] < 0) {
                pos[i++] = right[r];
            }
        }
        return pos;
    }

    const ArrayDesc& CrossJoinArray::getArrayDesc() const
    {
        return desc;
    }

    std::shared_ptr<ConstArrayIterator> CrossJoinArray::getConstIteratorImpl(const AttributeDesc& attrID) const
	{
        std::shared_ptr<ConstArrayIterator> leftIterator;
        std::shared_ptr<ConstArrayIterator> rightIterator;
        std::shared_ptr<ConstArrayIterator> inputIterator;
        AttributeID inputAttrID = attrID.getId();

        if (leftEmptyTagPosition) { // left array is emptyable
            if (inputAttrID >= leftEmptyTagPosition->getId()) {
                ++inputAttrID;
            }
            if (rightEmptyTagPosition) { // right array is also emptyable: ignore left empty-tag attribute
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left()->getConstIterator(*leftEmptyTagPosition);
                    const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                    auto targetAttrIter = rightAttrs.find(inputAttrID - nLeftAttrs);
                    SCIDB_ASSERT(targetAttrIter != rightAttrs.end());
                    inputIterator = rightIterator = right()->getConstIterator(*targetAttrIter);
                } else {
                    inputIterator = leftIterator = left()->getConstIterator(attrID);
                    rightIterator = right()->getConstIterator(*rightEmptyTagPosition);
                }
            } else { // emptyable array only from left side
                if (inputAttrID >= nLeftAttrs) {
                    leftIterator = left()->getConstIterator(*leftEmptyTagPosition);
                    const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                    const auto& fda = rightAttrs.firstDataAttribute();
                    auto targetAttrIter =
                        rightAttrs.find(inputAttrID == nLeftAttrs + nRightAttrs ? fda.getId() : inputAttrID - nLeftAttrs);
                    SCIDB_ASSERT(targetAttrIter != rightAttrs.end());
                    inputIterator = rightIterator = right()->getConstIterator(*targetAttrIter);
                } else {
                    inputIterator = leftIterator = left()->getConstIterator(attrID);
                    const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                    const auto& fda = rightAttrs.firstDataAttribute();
                    rightIterator = right()->getConstIterator(fda);
                }
            }
        } else if (rightEmptyTagPosition) { // only right array is emptyable
            if (inputAttrID >= nLeftAttrs) {
                const auto& leftAttrs = left()->getArrayDesc().getAttributes();
                const auto& fda = leftAttrs.firstDataAttribute();
                leftIterator = left()->getConstIterator(fda);
                const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                auto targetAttrIter = rightAttrs.find(inputAttrID - nLeftAttrs);
                SCIDB_ASSERT(targetAttrIter != rightAttrs.end());
                inputIterator = rightIterator = right()->getConstIterator(*targetAttrIter);
            } else {
                inputIterator = leftIterator = left()->getConstIterator(attrID);
                rightIterator = right()->getConstIterator(*rightEmptyTagPosition);
            }
        } else { // both input arrays are non-emptyable
            if (inputAttrID >= nLeftAttrs) {
                const auto& leftAttrs = left()->getArrayDesc().getAttributes();
                const auto& fda = leftAttrs.firstDataAttribute();
                leftIterator = left()->getConstIterator(fda);
                if (inputAttrID == nLeftAttrs + nRightAttrs) {
                    const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                    const auto& fda = rightAttrs.firstDataAttribute();
                    rightIterator = right()->getConstIterator(fda);
                } else {
                    const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                    auto targetAttrIter = rightAttrs.find(inputAttrID - nLeftAttrs);
                    SCIDB_ASSERT(targetAttrIter != rightAttrs.end());
                    inputIterator = rightIterator = right()->getConstIterator(*targetAttrIter);
                }
            } else {
                inputIterator = leftIterator = left()->getConstIterator(attrID);
                const auto& rightAttrs = right()->getArrayDesc().getAttributes();
                const auto& fda = rightAttrs.firstDataAttribute();
                rightIterator = right()->getConstIterator(fda);
            }
        }
        return std::shared_ptr<CrossJoinArrayIterator>(new CrossJoinArrayIterator(*this, attrID.getId(), leftIterator, rightIterator, inputIterator));
    }
}
