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
 * @file RemapperArray.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef REMAPPER_ARRAY_H
#define REMAPPER_ARRAY_H

#include <array/DelegateArray.h>
#include <system/ErrorCodes.h>

namespace scidb {

class ChunkIdMap;
class Remapper;
class RemapperArrayIterator;

/**
 *  Remap provisional (pos,id) pairs to final (pos,id) pairs on the fly.
 *
 *  @description Wrap the 1-D redimensionArray and translate (chunkPos,chunkId) pairs from the
 *  provisional chunking scheme to the final chunking scheme.  Attributes MUST be traversed in
 *  horizontal order, with the exception of the empty bitmap attribute which can be accessed at any
 *  time.
 */
class RemapperArray : public DelegateArray, boost::noncopyable
{
    typedef DelegateArray inherited;
    friend class RemapperArrayIterator;
    friend class WrappedRemapperArrayIterator;

public:

    RemapperArray(std::shared_ptr<Array> inputArray,
                  std::shared_ptr<Remapper>& remap,
                  std::shared_ptr<Query> const& query);
    virtual ~RemapperArray() {}

    /// Provide DelegateArray framework with our array iterators.
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    // XXX We *should* be able to support random access by using some kind of chunk cache and
    // allowing "buddy" _POS_ATTR and _ID_ATTR array iterators to coordinate via that cache.
    // That would allow the downstream SortArray to use multiple prefetch threads.
    Access getSupportedAccess() const override
    {
        return SINGLE_PASS;
    }

private:

    std::shared_ptr<Remapper>       _remap;
    std::shared_ptr<Query> const&   _query;

    // Attribute order-of-access enforcement.
    size_t const            _N_ATTRS;
    AttributeID const       _ID_ATTR;
    AttributeID const       _POS_ATTR;
    AttributeID             _expectedAttrId;

    // Input array iterators for attributes of interest.
    std::shared_ptr<ConstArrayIterator> _posArrayIter;
    std::shared_ptr<ConstArrayIterator> _idArrayIter;

    // Doing the safe thing for now: smart pointers to single-use MemChunks.  XXX It might be
    // possible to embed actual MemChunks here or in array iterators and re-use them (but only worth
    // doing *if* there is a measurable speed improvement).
    //
    std::shared_ptr<MemChunk> _posChunkPtr;
    std::shared_ptr<MemChunk> _idChunkPtr;
    void    computeRemappedChunks();
};


/**
 *  Array iterator for RemapperArray.
 *
 *  @description
 *  Both subclasses enforce the horizontal-chunk-access-only requirement.
 *    - PassThruRemapperArrayIterator\n
 *      An ordinary DelegateArrayIterator that enforces the horizontal access requirement.
 *    - WrappedRemapperArrayIterator\n
 *      If _POS_ATTR, precompute both _POS_ATTR and _ID_ATTR chunks.  Whether _POS_ATTR or _ID_ATTR,
 *      yield appropriate chunk.  (_ID_ATTR assumes its chunk was just precomputed, which is why
 *      horizontal access order is so important.)
 */
class RemapperArrayIterator : public DelegateArrayIterator
{
public:
    RemapperArrayIterator(RemapperArray& array,
                          const AttributeDesc& aid,
                          std::shared_ptr<ConstArrayIterator> inputIter)
        : DelegateArrayIterator(array, aid, inputIter)
        , _needOrderCheck(true)
        , _array(array)
    { }

    bool setPosition(Coordinates const&) override
    {
        // Random access not supported.
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ILLEGAL_OPERATION)
            << "RemapperArrayIterator::setPosition";
    }

protected:
    bool                _needOrderCheck;
    RemapperArray&      _array;
    void                checkAccessOrder();
    void                blessWrappedIterator(AttributeID, std::shared_ptr<ConstArrayIterator>);
    bool                isPosAttr() const { return attr.getId() == _array._POS_ATTR; }
    bool                haveIdChunk() const { return _array._idChunkPtr.get(); }

    // I hate using member data that doesn't follow the leading underscore convention.
    AttributeID         getAttrId() const { return attr.getId(); }
    DelegateArray const& getArray() const { return array; }
};


class PassThruRemapperArrayIterator : public RemapperArrayIterator
{
    typedef RemapperArrayIterator inherited;

public:
    PassThruRemapperArrayIterator(RemapperArray& array,
                                  const AttributeDesc& aid,
                                  std::shared_ptr<ConstArrayIterator> inputIter)
        : RemapperArrayIterator(array, aid, inputIter)
    { }

    ConstChunk const& getChunk() override
    {
        if (_needOrderCheck) {
            checkAccessOrder();
        }
        return inherited::getChunk();
    }

    void operator++() override
    {
        inherited::operator++();
        _needOrderCheck = true;
    }
};

class WrappedRemapperArrayIterator : public RemapperArrayIterator
{
    typedef RemapperArrayIterator inherited;

public:
    WrappedRemapperArrayIterator(RemapperArray& array,
                                 const AttributeDesc& aid,
                                 std::shared_ptr<ConstArrayIterator> inputIter)
        : RemapperArrayIterator(array, aid, inputIter)
        , _wantChunk(0L)
        , _gotChunk(-1L)
        , _currPos(1)       // We know we are wrapping a zero-based 1-D array.
        , _INTERVAL(array.getArrayDesc().getDimensions()[0].getChunkInterval())
    { }
    ~WrappedRemapperArrayIterator();

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override { return _currPos; }

private:
    ssize_t _wantChunk;
    ssize_t _gotChunk;
    Coordinates _currPos;
    int64_t const _INTERVAL;
    std::shared_ptr<MemChunk> _chunkPtr;
    std::shared_ptr<MemChunk> getNextChunkPtr();
};

} // namespace scidb

#endif /* ! REMAPPER_ARRAY_H */
