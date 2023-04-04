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

#ifndef SYNTHETIC_ARRAY_H_
#define SYNTHETIC_ARRAY_H_

/**
 * @file SyntheticArray.h
 * @author Dave Gosselin
 *
 * @brief Iterators to synthesize nulls or default values for newly added attributes.
 *
 * @details The add_attributes() operator adds attributes (!) to an
 * array, but does not populate any BufferMgr disk indeces.  When
 * scanning such an array, these iterators essentially wrap the empty
 * bitmap for positioning purposes, but when called upon for actual
 * scidb::Value objects, null (or the appropriate default) is returned
 * instead.  These iterators are interposed as necessary by DBArray
 * and MemArray.
 */

#include <array/ArrayDesc.h>
#include <array/ConstArrayIterator.h>
#include <array/ConstChunk.h>
#include <query/Value.h>

namespace scidb {

class Array;
class Query;
class SyntheticChunk;
class RLEBitmapChunkIterator;

class SyntheticChunkIterator : public ConstChunkIterator
{
public:
    SyntheticChunkIterator(std::shared_ptr<Array> parray,
                           const AttributeDesc& attr,
                           const SyntheticChunk& chunk,
                           int iterationMode,
                           std::shared_ptr<ConstChunkIterator> ebmChunkIter);
    virtual ~SyntheticChunkIterator();

    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

    int getMode() const override;
    Value const& getItem() override;
    bool isEmpty() const override;
    ConstChunk const& getChunk() override;

private:
    std::shared_ptr<Array> _parray;
    AttributeDesc _attr;
    const SyntheticChunk& _chunk;
    const size_t _cellCount;
    int _iterationMode;
    Coordinates _pos;
    std::shared_ptr<ConstChunkIterator> _ebmChunkIter;
    RLEBitmapChunkIterator* _bitmapIter {nullptr};
    Value _fullTile;
    Value _partialTile;

    static constexpr char const * const _cls = "SyntheticChunkIterator::";
};

class SyntheticChunk : public ConstChunk
{
public:
    SyntheticChunk(std::shared_ptr<Array> parray,
                   const AttributeDesc& attr);
    virtual ~SyntheticChunk();

    const ArrayDesc& getArrayDesc() const override;
    const AttributeDesc& getAttributeDesc() const override;

    // This group of methods just forwards to the backing EBM chunk.
    Coordinates const& getFirstPosition(bool withOverlap) const override;
    Coordinates const& getLastPosition(bool withOverlap) const override;
    bool isCountKnown() const override;
    size_t count() const override;

    std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const override;
    CompressorType getCompressionMethod() const override;
    Array const& getArray() const override;

    void setEbmChunk(const ConstChunk* pebmChunk);

private:
    std::shared_ptr<Array> _parray;
    AttributeDesc _attr;
    const ConstChunk* _ebmChunk;
    static constexpr char const * const _cls = "SyntheticChunk::";
};

class SyntheticArrayIterator : public ConstArrayIterator
{
public:
    SyntheticArrayIterator(std::shared_ptr<Array> parray,
                      const AttributeDesc& attr,
                      std::shared_ptr<Query> query);
    virtual ~SyntheticArrayIterator();

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

private:
    std::shared_ptr<Array> _parray;
    SyntheticChunk _chunk;
    std::shared_ptr<ConstArrayIterator> _ebmIter;
    static constexpr char const * const _cls = "SyntheticArrayIterator::";
};

}  // namespace scidb

#endif  // SYNTHETIC_ARRAY_H_
