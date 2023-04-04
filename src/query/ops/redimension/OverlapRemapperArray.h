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
 * @file OverlapRemapperArray.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef OVERLAP_REMAPPER_ARRAY_H
#define OVERLAP_REMAPPER_ARRAY_H

#include <array/DelegateArray.h>
#include <system/ErrorCodes.h>

namespace scidb {

class ChunkIdMap;
class Remapper;
class OverlapRemapperArrayIterator;

/**
 *  Generate overlap region cells while also remapping provisional (pos,id) pairs to final (pos,id) pairs.
 *
 *  @description Wrap the 1-D redimensionArray and translate (chunkPos,chunkId) pairs from the
 *  provisional chunking scheme to the final chunking scheme, while also generating cells for the
 *  overlap region(s).
 *
 *  @p As with RemapperArray, attributes MUST be traversed in horizontal order, with the exception
 *  of the empty bitmap attribute which can be accessed at any time.
 */
class OverlapRemapperArray : public DelegateArray, boost::noncopyable
{
    typedef DelegateArray inherited;
    friend class OverlapRemapperArrayIterator;

public:

    OverlapRemapperArray(std::shared_ptr<Array> inputArray,
                         std::shared_ptr<Remapper>& remap,
                         std::shared_ptr<Query> const& query);
    virtual ~OverlapRemapperArray() {}

    /// Provide DelegateArray framework with our array iterators.
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;

    // Unlike ordinary RemapperArray, there is no hope at all of implementing RANDOM or MULTI_PASS
    // access when overlaps are involved, since output cells/chunks are not 1-to-1 with input
    // cells/chunks.  At best it's a trade-off: SINGLE_PASS here, or else materilize it first and
    // you can do random access on the MemArray.
    Access getSupportedAccess() const override { return SINGLE_PASS; }

private:

    // Keep hold of our c'tor args.
    const Attributes&           _inputArrayAttrs;
    std::shared_ptr<Remapper>   _remap;

    // Constants everyone likes to know.
    size_t const            _N_ATTRS;
    AttributeID const       _EBM_ATTR;
    AttributeID const       _ID_ATTR;
    AttributeID const       _POS_ATTR;
    int64_t const           _INTERVAL;

    // Bookkeeping.
    size_t                  _chunkEpoch;
    ssize_t                 _freshChunks;
    bool                    _sawEof;
    bool                    _firstRow;
    bool                    _firstChunks;

    // Input array and chunk iterators, input values.
    std::vector< std::shared_ptr<ConstArrayIterator> > _inputArrayIters;
    std::vector< std::shared_ptr<ConstChunkIterator> > _inputChunkIters;
    std::vector<Value>      _inputValues; // XXX Should use an arena, but... query's or phys op's?
    bool                    getNextInputRow();

    // Queue of (chunkPos,chunkId) pairs generated for overlap regions.
    typedef std::pair<position_t, position_t>   PosIdPair;
    std::deque<PosIdPair>   _clones; // https://www.youtube.com/watch?v=rYnXPZ_DLbQ

    // Output chunks produced by makeMoreChunks().
    std::vector< std::shared_ptr<MemChunk> >      _chunks;
    std::vector< std::shared_ptr<ChunkIterator> > _writeIters;
    bool                    makeMoreChunks();
};


/**
 *  Array iterator for OverlapRemapperArray.
 */
class OverlapRemapperArrayIterator : public DelegateArrayIterator
{
public:
    OverlapRemapperArrayIterator(OverlapRemapperArray& array,
                                 const AttributeDesc& aid,
                                 std::shared_ptr<ConstArrayIterator> inputIter)
        : DelegateArrayIterator(array, aid, inputIter)
        , _array(array)
        , _attrId(aid)
        , _wantChunk(0UL)
        , _gotChunk(0UL)
        , _currPos(1)
    { }

    bool setPosition(Coordinates const&) override
    {
        // Random access not supported.
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ILLEGAL_OPERATION)
            << "OverlapRemapperArrayIterator::setPosition";
    }

    ConstChunk const& getChunk() override;
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override { return _currPos; }

private:
    OverlapRemapperArray&       _array;
    AttributeDesc               _attrId;
    size_t                      _wantChunk;
    size_t                      _gotChunk;
    Coordinates                 _currPos;
    std::shared_ptr<MemChunk>   _chunkPtr;

    bool        isPosAttr() const { return _attrId.getId() == _array._POS_ATTR; }
    bool        isIdAttr() const  { return _attrId.getId() == _array._ID_ATTR; }
};

} // namespace scidb

#endif /* ! OVERLAP_REMAPPER_ARRAY_H */
