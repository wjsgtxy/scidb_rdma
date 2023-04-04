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

#ifndef RESHAPE_ARRAY_H
#define RESHAPE_ARRAY_H

#include <array/DelegateArray.h>
#include <array/MemArray.h>

namespace scidb {


class ReshapeArray;
class ReshapeArrayIterator;
class ReshapeChunk;
class ReshapeChunkIterator;

class ReshapeChunkIterator : public ConstChunkIterator
{
    ReshapeArray const& array;
    ReshapeChunk& chunk;
    Coordinates outPos;
    Coordinates inPos;
    Coordinates first;
    Coordinates last;
    std::shared_ptr<ConstChunkIterator> inputIterator;
    std::shared_ptr<ConstArrayIterator> arrayIterator;
    int mode;
    bool hasCurrent;

  public:
    int    getMode() const override;
    bool   setPosition(Coordinates const& pos) override;
    Coordinates const& getPosition() override;
    void   operator++() override;
    void   restart() override;
    Value const& getItem() override;
    bool   isEmpty() const override;
    bool   end() override;
    ConstChunk const& getChunk() override;


    ReshapeChunkIterator(ReshapeArray const& array, ReshapeChunk& chunk, int iterationMode);
};

class ReshapeChunk : public DelegateChunk
{
    friend class ReshapeChunkIterator;

    ReshapeArray const& array;
    MemChunk chunk;

  public:
    virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
    void initialize(Coordinates const& pos);

    ReshapeChunk(ReshapeArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class ReshapeArrayIterator : public DelegateArrayIterator
{
    ReshapeArray const& array;
    Coordinates inPos;
    Coordinates outPos;
    bool hasCurrent;

  public:
    ConstChunk const& getChunk() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    bool end() override;
    void operator ++() override;
    void restart() override;

    ReshapeArrayIterator(ReshapeArray const& array,
                         const AttributeDesc& attrID,
                         std::shared_ptr<ConstArrayIterator> inputIterator);
};

class ReshapeArray : public DelegateArray
{
    friend class ReshapeChunk;
    friend class ReshapeChunkIterator;
    friend class ReshapeArrayIterator;

    Dimensions inDims;
    Dimensions outDims;

    void in2out(Coordinates const& inPos, Coordinates& outPos) const;
    void out2in(Coordinates const& outPos, Coordinates& inPos) const;

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    ReshapeArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array);
};

}

#endif
