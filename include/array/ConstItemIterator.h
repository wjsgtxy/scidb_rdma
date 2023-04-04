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
 * @file ConstItemIterator.h
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

#ifndef CONST_ITEM_ITERATOR_H_
#define CONST_ITEM_ITERATOR_H_

#include <array/AttributeDesc.h>
#include <array/ConstChunkIterator.h>   // base class

namespace scidb
{
class Array;
class ConstChunk;
class ConstArrayIterator;

/**
 * Iterator through all array elements. This iterator combines array and chunk iterators.
 * Please notice that using random positioning in array can cause very significant degradation of performance.
 */
class ConstItemIterator : public ConstChunkIterator
{
public:
    int getMode() const override;
    Value const& getItem() override;
    bool isEmpty() const override;
    ConstChunk const& getChunk()  override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

    ConstItemIterator(Array const& array, const AttributeDesc& attrID, int iterationMode);

private:
    std::shared_ptr<ConstArrayIterator> arrayIterator;
    std::shared_ptr<ConstChunkIterator> chunkIterator;
    int iterationMode;
};


} // namespace
#endif /* CONST_ITEM_ITERATOR_H_ */
