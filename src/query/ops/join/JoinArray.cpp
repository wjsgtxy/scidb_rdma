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

/*
 * JoinArray.h
 *
 *  Created on: Oct 22, 2010
 *      Author: Knizhnik
 */


#include "array/Array.h"
#include "JoinArray.h"

using namespace std;

namespace scidb
{
    //
    // Chunk iterator
    //
    inline bool JoinChunkIterator::join()
    {
        return joinIterator->setPosition(inputIterator->getPosition());
    }

    bool JoinChunkIterator::end()
    {
        return !hasCurrent;
    }

    void JoinChunkIterator::alignIterators()
    {
        while (!inputIterator->end()) {
            if (join()) {
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    void JoinChunkIterator::restart()
    {
        inputIterator->restart();
        joinIterator->restart();
        alignIterators();
    }

    bool JoinChunkIterator::setPosition(Coordinates const& pos)
    {
        if (inputIterator->setPosition(pos)) {
            return hasCurrent = join();
        }
        return hasCurrent = false;
    }

    void JoinChunkIterator::operator ++()
    {
        ++(*inputIterator);
        alignIterators();
    }

    JoinChunkIterator::JoinChunkIterator(JoinEmptyableArrayIterator const& arrayIterator,
                                         DelegateChunk const* chunk,
                                         int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      joinIterator(arrayIterator._joinIterator->getChunk().getConstIterator(iterationMode)),
      mode(iterationMode)
    {
        alignIterators();
    }

     Value& JoinBitmapChunkIterator::getItem()
    {
        value.setBool(inputIterator->getItem().getBool() && joinIterator->getItem().getBool());
        return value;
    }

    JoinBitmapChunkIterator::JoinBitmapChunkIterator(JoinEmptyableArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : JoinChunkIterator(arrayIterator, chunk, iterationMode),
      value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // Array iterator
    //
    bool JoinEmptyableArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        _hasCurrent = inputIterator->setPosition(pos) && _joinIterator->setPosition(pos);
        return _hasCurrent;
    }

    void JoinEmptyableArrayIterator::restart()
    {
        inputIterator->restart();
        _joinIterator->restart();
        alignIterators();
    }

    void JoinEmptyableArrayIterator::operator ++()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        ++(*inputIterator);
        alignIterators();
    }

    bool JoinEmptyableArrayIterator::end()
    {
        return !_hasCurrent;
    }

    void JoinEmptyableArrayIterator::alignIterators()
    {
        _hasCurrent = false;
        chunkInitialized = false;
        while (!inputIterator->end()) {
            if (_joinIterator->setPosition(inputIterator->getPosition())) {
                _hasCurrent = true;
                break;
            }
            ++(*inputIterator);
        }
    }

    ConstChunk const& JoinEmptyableArrayIterator::getChunk()
    {
        _chunkPtr()->overrideClone(!_chunkLevelJoin);
        return DelegateArrayIterator::getChunk();
    }

    JoinEmptyableArrayIterator::JoinEmptyableArrayIterator(JoinEmptyableArray const& array,
                                                           const AttributeDesc& attrID,
                                                           std::shared_ptr<ConstArrayIterator> input,
                                                           std::shared_ptr<ConstArrayIterator> join,
                                                           bool chunkLevelJoin)
    : DelegateArrayIterator(array, attrID, input),
      _joinIterator(join),
      _chunkLevelJoin(chunkLevelJoin)
    {
        alignIterators();
    }


    DelegateChunkIterator* JoinEmptyableArray::createChunkIterator(DelegateChunk const* chunk,
                                                                   int iterationMode) const
    {
        const auto& arrayIterator =
            dynamic_cast<const JoinEmptyableArrayIterator&>(chunk->getArrayIterator());
        AttributeDesc const& attr = chunk->getAttributeDesc();
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;

        if (!arrayIterator._chunkLevelJoin)
        {
            return new DelegateChunkIterator(chunk, iterationMode);
        }
        else if (attr.isEmptyIndicator())
        {
            return (DelegateChunkIterator*)new JoinBitmapChunkIterator(arrayIterator, chunk, iterationMode);
        }
        else
        {
            return (DelegateChunkIterator*)new JoinChunkIterator(arrayIterator, chunk, iterationMode);
        }
    }

    DelegateArrayIterator* JoinEmptyableArray::createArrayIterator(const AttributeDesc& attrID) const
    {
        std::shared_ptr<ConstArrayIterator> inputIterator;
        std::shared_ptr<ConstArrayIterator> joinIterator;
        bool chunkLevelJoin = true;
        AttributeID inputAttrID = attrID.getId();

        /*
         * There are two 'levels' of join.
         * First we must ensure that each chunk in LEFT has a matching chunk in RIGHT and vice-versa;
         * otherwise we must exclude non-matching chunk from output. We ALWAYS perform these array-level
         * joins of chunks - regardless of whether the two arrays are emptyable or not.
         *
         * Once you have two matching chunks, you need to make sure that each value in LEFT has a matching
         * value in RIGHT. This is what we call "chunkLevelJoin". At this point, there are cases such as
         * reading an attribute from LEFT and RIGHT is not EMPTYABLE, where we do NOT need to perform a
         * chunk-level join. We can return the entire chunk from LEFT directly.
         */

        const auto& leftAttributes = _left->getArrayDesc().getAttributes();
        const auto& rightAttributes = _right->getArrayDesc().getAttributes();

        if (_leftEmptyTagPosition)
        {   // left array is emptyable
            if (inputAttrID >= _leftEmptyTagPosition->getId())
            {
                inputAttrID += 1;
            }
            if (_rightEmptyTagPosition)
            {    // right array is also emptyable: ignore left empty-tag attribute
                if (inputAttrID >= _nLeftAttributes)
                {
                    const auto& target = rightAttributes.findattr(inputAttrID - _nLeftAttributes);
                    inputIterator = _right->getConstIterator(target);
                    joinIterator = _left->getConstIterator(*_leftEmptyTagPosition);
                }
                else
                {
                    const auto& target = leftAttributes.findattr(inputAttrID);
                    inputIterator = _left->getConstIterator(target);
                    joinIterator = _right->getConstIterator(*_rightEmptyTagPosition);
                }
            }
            else
            {   // emptyable array only from left side
                if (attrID.getId() == _emptyTagPosition->getId())
                {
                    inputIterator = _left->getConstIterator(*_leftEmptyTagPosition);
                    joinIterator = _right->getConstIterator(rightAttributes.firstDataAttribute());
                    chunkLevelJoin = false;
                }
                else if (inputAttrID >= _nLeftAttributes)
                {
                    const auto& target = rightAttributes.findattr(inputAttrID - _nLeftAttributes);
                    inputIterator = _right->getConstIterator(target);
                    joinIterator = _left->getConstIterator(*_leftEmptyTagPosition);
                }
                else
                {
                    const auto& target = leftAttributes.findattr(inputAttrID);
                    inputIterator = _left->getConstIterator(target);
                    joinIterator = _right->getConstIterator(rightAttributes.firstDataAttribute());
                    chunkLevelJoin = false;
                }
            }
        }
        else
        {   // only right array is emptyable
            if (inputAttrID >= _nLeftAttributes)
            {
                SCIDB_ASSERT(rightAttributes.hasAttribute(inputAttrID - _nLeftAttributes));
                const auto& target = rightAttributes.findattr(inputAttrID - _nLeftAttributes);
                inputIterator = _right->getConstIterator(target);
                joinIterator = _left->getConstIterator(leftAttributes.firstDataAttribute());
                chunkLevelJoin = false;
            }
            else
            {
                SCIDB_ASSERT(_rightEmptyTagPosition);
                const auto& target = leftAttributes.findattr(inputAttrID);
                inputIterator = _left->getConstIterator(target);
                joinIterator = _right->getConstIterator(*_rightEmptyTagPosition);
            }
        }

        return new JoinEmptyableArrayIterator(*this, attrID, inputIterator, joinIterator, chunkLevelJoin);
    }

    JoinEmptyableArray::JoinEmptyableArray(ArrayDesc const& desc,
                                           std::shared_ptr<Array> leftArr,
                                           std::shared_ptr<Array> rightArr)
        : DelegateArray(desc, leftArr)
        , _left(leftArr)
        , _right(rightArr)
        , _nLeftAttributes(0)
    {
        // empty tag position
        _emptyTagPosition = desc.getEmptyBitmapAttribute();

        // left attrs
        const auto& leftAttrs = _left->getArrayDesc().getAttributes();
        _nLeftAttributes = safe_static_cast<AttributeID>(leftAttrs.size());
        _leftEmptyTagPosition = leftAttrs.getEmptyBitmapAttribute();

        // right attrs
        const auto& rightAttrs = _right->getArrayDesc().getAttributes();
        _rightEmptyTagPosition = rightAttrs.getEmptyBitmapAttribute();
    }
}
