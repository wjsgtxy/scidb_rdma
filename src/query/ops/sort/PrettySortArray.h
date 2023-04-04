/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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
 * @file PrettySortArray.h
 * @author Mike Leibensperger
 */

#ifndef PRETTY_SORT_ARRAY_H
#define PRETTY_SORT_ARRAY_H

#include <array/SinglePassArray.h>
#include <util/ArrayCoordinatesMapper.h>

namespace scidb
{

class MemArray;
class Query;

/** Set true to make PrettySortArray the default sort result. */
constexpr bool const SORT_DEFAULT_WANT_COORDS = false;

/**
 * @brief Transform a SortArray with "expanded" schema into an array
 * decorated with original input coordinates as attributes.
 *
 * @details The sorting machinery appends two additional int64
 * attributes, $chunk_pos and $cell_pos, to the sort tuples in order
 * to provide a stable sort (among other reasons).  These values are
 * the row-major-order (RMO) chunk number and cell position within
 * that chunk.  (All array coordinates can be condensed into these two
 * values; for example, see RedimensionCommon.cpp .)
 *
 * @p For the final sort output, we want to project away these
 * internal position values, but first we want to use them to decorate
 * the output with the original cell coordinates.  PrettySortArray
 * does this.
 *
 * @p For example, suppose the original array given to SortArray had
 * schema
 * @code
 * <a:int32, b:double>[x;y;z]
 * @endcode
 * From that, SortArray with @c preservePositions option would produce
 * @code
 * <a:int32, b:double, $chunk_pos:int64, $cell_pos:int64>[$n]
 * @endcode
 * And from @i that, PrettySortArray produces
 * @code
 * <x:int64, y:int64, z:int64, a:int32, b:double>[$n]
 * @endcode
 * the sorted @c <a,b> tuples decorated with their original @c x,y,z
 * input coordinates.
 *
 * @see wiki:DEV/.../DistributedSortDesign
 */
class PrettySortArray : public SinglePassArray
{
public:
    /**
     * Constructor
     *
     * @param schema         result schema with coordinates-as-attributes prefix
     * @param sortedArray    result of SortArray w/ $chunk_pos/$cell_pos suffix
     * @param originalDims   dimensions of original unsorted input array
     * @param query          active query pointer
     */
    PrettySortArray(ArrayDesc const& schema,
                    std::shared_ptr<MemArray>& sortedArray,
                    Dimensions const& originalDims,
                    std::shared_ptr<Query> const& query);

    virtual ~PrettySortArray();

    PrettySortArray(PrettySortArray const&) = delete;
    PrettySortArray& operator=(PrettySortArray const&) = delete;

    // SinglePassArray interface
    size_t getCurrentRowIndex() const override { return _currRowIndex; }
    bool moveNext(size_t rowIdx) override;
    ConstChunk const& getChunk(AttributeID, size_t rowIdx) override;

    /**
     * Compose final pretty output schema from SortArray result and input.
     *
     * Starting with the sortedSchema, prefix one TID_INT64 attribute
     * for each inputSchema dimension, and name it accordingly.
     */
    static ArrayDesc makeOutputSchema(ArrayDesc const& inputSchema,
                                      std::string const& dimName,
                                      size_t chunkSize);

private:
    // Constants computed in constructor init list.
    size_t const _N_OUT_ATTRS;          // Result attribute count including EBM
    AttributeID const _OUT_EBM_ATTR;    // Result array's empty bitmap attribute
    size_t const _PREFIX_COUNT;         // Result array's # of coords-as-attrs
    size_t const _N_IN_ATTRS;           // Input attribute count including EBM
    AttributeID const _IN_EBM_ATTR;     // Input array's empty bitmap attribute
    AttributeID const _CELLPOS_ATTR;    // Input array's $cell_pos attribute
    AttributeID const _CHUNKPOS_ATTR;   // Input array's $chunk_pos attribute

    void _makePrefixChunks();
    void _makePassThruChunks();
    std::shared_ptr<MemArray> _sortedArray();

    size_t _currRowIndex { 0 };
    ArrayCoordinatesMapper _mapper;
    std::vector<std::shared_ptr<ConstArrayIterator> > _arrayIters;

    // Change _HISTORY_SIZE to 2 if you want to keep a deeper
    // "history".  Unlike other SinglePassArray-derived clases, here
    // we don't seem to actually need the history.  Coded to allow for
    // it just in case, but it's "cargo cult programming"... might be
    // related to Donghui's comment for StreamArray::nextChunk()...?
    //
    static constexpr size_t _HISTORY_SIZE = 1;

    // Use _chunkRef() to index into the _chunks vector modulo the
    // _HISTORY_SIZE.
    std::vector<MemChunk> _chunks;
    MemChunk& _chunkRef(AttributeID attId, size_t row);
};

} // namespace

#endif /* ! PRETTY_SORT_ARRAY_H */
