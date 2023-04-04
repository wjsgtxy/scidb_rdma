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
 * @file FastDfArray.h
 * @author Mike Leibensperger
 * @brief "Fast path" array-to-dataframe converter.
 */

#ifndef FAST_DF_ARRAY_H
#define FAST_DF_ARRAY_H

#include <array/DelegateArray.h>
#include <util/ArrayCoordinatesMapper.h>

namespace scidb
{

/**
 * "Fast path" array-to-dataframe converter.
 *
 * @details Here, the desired dataframe chunk size matches the chunk
 * volume of the input array.  Because input and output chunks are the
 * same size, proxied chunk payloads need not (in theory) be
 * rewritten.
 *
 * Enforces horizontal-by-chunk iteration, because additional chunks
 * must be generated for the original input dimension values.
 */
class FastDfArray : public DelegateArray
{
    friend class FastDfChunk;
    friend class FastDfChunkIterator;
    friend class FastDfArrayIterator;

public:
    // 'structors
    FastDfArray(ArrayDesc const& outSchema,
                std::shared_ptr<Array> inArray,
                std::shared_ptr<Query> const& query);

    virtual ~FastDfArray() = default;

    // Non-copyable.
    FastDfArray(FastDfArray const&) = delete;
    FastDfArray& operator=(FastDfArray const&) = delete;

    // Multi-pass because (a) we have chunks to generate on the fly
    // and so must stay row-wise "in step", and (b) setPosition()
    // makes no sense on a dataframe, which is (conceptually) a bag
    // that has no coordinate system.
    Access getSupportedAccess() const override { return MULTI_PASS; }

    // Customization points.
    DelegateArrayIterator* createArrayIterator(AttributeDesc const& aDesc) const override;
    DelegateChunk* createChunk(DelegateArrayIterator const* dai, AttributeID id) const override;
    DelegateChunkIterator* createChunkIterator(DelegateChunk const* dc, int mode) const override;

private:
    size_t const _N_IN_DIMS;
    size_t const _N_IN_ATTRS;   // includes empty bitmap
    AttributeID const _OUT_EBM_ATTR;

    InstanceID _localInstance;

    bool _atEnd { false };
    size_t _currRow { 0 };
    Coordinates _currPos;
    Coordinates _prevPos;       // Intentionally not initialized in ctor!
    mgd::vector<MemChunk> _chunks;
    std::shared_ptr<ConstArrayIterator> _inputEbmIter;
    ArrayCoordinatesMapper _mapper;  // position_t <--> input schema coords

    /**
     * Create the non-pass-through chunks for the row.
     *
     * We generate chunks for prefixed coordinate attributes.  Called
     * by the first FastDfArrayIterator to arrive at row and request a
     * chunk.
     *
     * @returns true if more chunks were made, i.e. we're not at end()
     *
     * @note Because the PhysicalStore::copyChunks() loop increments
     * each array iterator before subsequent ones request their
     * chunks, we must delay calling this until a chunk is actually
     * requested from FastDfArrayIterator::getChunk().
     */
    bool _makeChunks(size_t row);

    // Called by first FastDfArrayIterator to restart.  Maybe we can
    // be truly MULTI_PASS one day?  (But not if inputArray is
    // SINGLE_PASS of course.)
    void _restart();

    // Non-clone array iterators ask us about whether the end() is nigh.
    bool _end();

    // Report array iterator was incremented and will want 'row' soon.
    void _bumped(AttributeDesc const& who, size_t row);

    // Is attrId one of the coordinate-as-attribute prefix attributes?
    bool _isPrefixAttr(size_t attrId) const;

    // Is attrId one of the pass-thru attributes from the input array?
    // XXX Maybe "_isPassThruAttr()" is a better name because we still
    // need to wrap chunks with FastDfChunk, else we won't get the
    // right EBM?
    bool _isCloneAttr(size_t attrId) const;
};

class FastDfArrayIterator : public DelegateArrayIterator
{
    using super = DelegateArrayIterator;

public:

    /**
     * Construct array iterator that
     * - enforces MULTI_PASS
     * - might be a delegate/proxy, function-shipping to the inIter
     * - if not (generated empty bitmap or coordinate-as-attribute),
     *   redirect to proper entry in _array._chunks[] vector
     */
    FastDfArrayIterator(FastDfArray& array,
                        AttributeDesc const& outAttr,
                        std::shared_ptr<ConstArrayIterator>);

    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    ConstChunk const& getChunk() override;
    bool end() override;
    Coordinates const& getPosition() override;
    void operator ++() override;

private:
    FastDfArray& _array;
    size_t _myRow { 1 };        // ...to trigger initial _makeChunks() call.
    bool _isClone { false };
    bool _isEmptyBitmap { false };
};


/**
 * FastDfChunk has same payload and EBM chunk as its referent, but
 * answers to a different coordinate system.
 *
 * The physical positions of values within the payload do not change,
 * what changes is that the cell is now in the dataframe schema's
 * coordinate system rather than the input array's.
 */
class FastDfChunk : public DelegateChunk
{
    friend class FastDfChunkIterator;
    using super = DelegateChunk;

public:
    FastDfChunk(FastDfArray const&,
                FastDfArrayIterator const&,
                AttributeID outAttrId);

    // Overriding these lets my iterators use a correct dataframe
    // CoordinatesMapper instead of the input chunk's.
    // Dataframes do not support overlap, so the bool parameter is ignored.
    Coordinates const& getFirstPosition(bool withOverlap) const override;
    Coordinates const& getLastPosition(bool withOverlap) const override;

    // Route these calls to the right EBM chunk.
    std::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const override;
    ConstChunk const* getBitmapChunk() const override;

private:
    FastDfArray const& _array;
    AttributeID _attrId;
    size_t _myRow;
    Coordinates _firstPos;
    Coordinates _lastPos;

    void _refreshBounds();
};

/**
 * FastDfChunkIterator must report positions in dataframe coordinates,
 * not input array coordinates.
 */
class FastDfChunkIterator : public DelegateChunkIterator
{
    using super = DelegateChunkIterator;

public:
    FastDfChunkIterator(FastDfChunk const& chunk, int mode);

    Coordinates const& getPosition() override;
    Coordinates const& getFirstPosition() override;
    Coordinates const& getLastPosition() override;
    operator const CoordinatesMapper* () const override { return &_mapper; }

private:
    FastDfChunk const& _chunk;
    CoordinatesMapper _mapper;
    Coordinates _coords;
    bool _canGetLogicalPos { true };
};

} // namespace

#endif /* ! FAST_DF_ARRAY_H */
