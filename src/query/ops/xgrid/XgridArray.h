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

#ifndef XGRID_ARRAY_H
#define XGRID_ARRAY_H

#include "LogicalXgrid.h"       // for ScaleFactors
#include <array/DelegateArray.h>
#include <array/MemArray.h>

namespace scidb {


class XgridArray;
class XgridArrayIterator;
class XgridChunk;
class XgridChunkIterator;

class XgridChunkIterator : public ConstChunkIterator
{
    XgridArray const& array;
    XgridChunk const& chunk;
    Coordinates outPos;
    Coordinates inPos;
    Coordinates first;
    Coordinates last;
    std::shared_ptr<ConstChunkIterator> inputIterator;
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


    XgridChunkIterator(XgridArray const& array, XgridChunk const& chunk, int iterationMode);
};

class XgridChunk : public DelegateChunk
{
    friend class XgridChunkIterator;
    friend class XgridArrayIterator;

    XgridArray const& array;
    MemChunk chunk;

  public:
    virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void initialize(Coordinates const& pos);

    XgridChunk(XgridArray const& array, DelegateArrayIterator const& iterator, AttributeID attrID);
};

class XgridArrayIterator : public DelegateArrayIterator
{
    XgridArray const& array;
    Coordinates inPos;
    Coordinates outPos;

  public:
    ConstChunk const& getChunk() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;

    XgridArrayIterator(XgridArray const& array,
                       const AttributeDesc& attrID,
                       std::shared_ptr<ConstArrayIterator> inputIterator);
};

class XgridArray : public DelegateArray
{
    friend class XgridChunk;
    friend class XgridChunkIterator;
    friend class XgridArrayIterator;

    Coordinates scale;

    void out2in(Coordinates const& outPos, Coordinates& inPos) const;
    void in2out(Coordinates const& inPos, Coordinates& outPos) const;

    XgridArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array);

  public:

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    // Factory method performs execute()-time scaling on autochunked dimensions.
    static std::shared_ptr<Array> create(ArrayDesc const& partlyScaledSchema,
                                         std::vector<int32_t> const& scaleFactors,
                                         std::shared_ptr<Array>& inputArray);
};

} // namespace

#endif
