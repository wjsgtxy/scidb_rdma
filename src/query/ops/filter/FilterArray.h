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
 * @file FilterArray.h
 *
 * @brief The implementation of the array iterator for the filter operator
 *
 */

#ifndef FILTER_ARRAY_H_
#define FILTER_ARRAY_H_

#include <string>
#include <vector>

#include "array/DelegateArray.h"
#include "query/LogicalExpression.h"
#include "query/Expression.h"

namespace scidb
{


class FilterArray;
class FilterArrayIterator;
class FilterChunkIterator;


class FilterChunkIterator : public DelegateChunkIterator
{
  protected:
    Value& evaluate();
    Value& buildBitmap();
    bool filter();
    void moveNext();
    void nextVisible();

  public:
    Value const& getItem() override;
    void operator ++() override;
    void restart() override;
    bool end() override;
    bool setPosition(Coordinates const& pos) override;
    virtual std::shared_ptr<Query> getQuery() { return _query; }
    FilterChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);

  protected:
    MemChunk _shapeChunk;
    FilterArray const& _array;
    std::vector< std::shared_ptr<ConstChunkIterator> > _iterators;
    std::shared_ptr<ConstChunkIterator> _emptyBitmapIterator;
    ExpressionContext _params;
    bool _hasCurrent;
    int _mode;
    Value _tileValue;
    TypeId _type;
 private:
    std::shared_ptr<Query> _query;
};


class ExistedBitmapChunkIterator : public FilterChunkIterator
{
public:
    Value& getItem() override;

    ExistedBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);

private:
     Value _value;
};


class NewBitmapChunkIterator : public FilterChunkIterator
{
public:
    Value& getItem() override;

    NewBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode);
};


class FilterArrayIterator : public DelegateArrayIterator
{
    friend class FilterChunkIterator;
  public:
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    void operator ++() override;
    ConstChunk const& getChunk() override;
    FilterArrayIterator(FilterArray const& array,
                        const AttributeDesc& attrID,
                        const AttributeDesc& inputAttrID);

  private:
    std::vector< std::shared_ptr<ConstArrayIterator> > iterators;
    std::shared_ptr<ConstArrayIterator> emptyBitmapIterator;
    AttributeID inputAttrID;
};

class FilterArray : public DelegateArray
{
    friend class FilterArrayIterator;
    friend class FilterChunkIterator;
    friend class NewBitmapChunkIterator;
  public:
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    FilterArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array,
                std::shared_ptr< Expression> expr, std::shared_ptr<Query>& query,
                bool tileMode);

  private:
    std::map<Coordinates, std::shared_ptr<DelegateChunk>, CoordinatesLess > cache;
    Mutex mutex;
    std::shared_ptr<Expression> expression;
    std::vector<BindInfo> bindings;
    bool _tileMode;
    size_t cacheSize;
    AttributeID emptyAttrID;

};

}

#endif
