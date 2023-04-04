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

#ifndef __FOLD_ARRAY__
#define __FOLD_ARRAY__

#include <array/Array.h>
#include <array/ConstArrayIterator.h>
#include <array/ConstChunk.h>
#include <array/ConstChunkIterator.h>
#include <query/Query.h>

namespace scidb {

class FoldArray;
class FoldArrayIterator;
class FoldChunk;

class FoldChunkIterator : public ConstChunkIterator
{
public:
    // 'structors
    FoldChunkIterator(FoldChunk& foldChunk,
                      Array const& inputArray,
                      std::shared_ptr<ConstArrayIterator> inputAttrIter,
                      int iterationMode);

    virtual ~FoldChunkIterator();

    // Overrides from ConstChunkIterator.
    int getMode() const override;
    Value const& getItem() override;
    bool isEmpty() const override;
    ConstChunk const& getChunk() override;

    // Overrides from ConstIterator.
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

private:
    /**
     * Move to the next logical position on the chunk.
     *
     * @return true if on the chunk, false if at the end.
     */
    bool _next();

    /**
     * Get the attribute ID for the attribute in the output
     * array for which this iterates.
     *
     * @return the attribute ID
     */
    AttributeID _getTargetAttribute() const;

    /**
     * Set the position on the input chunk iterator.
     *
     * @param pos The position in the input array's coordinate space.
     */
    void _setInputPosition(Coordinates const& pos);

    FoldChunk& _foldChunk;
    Array const& _inputArray;
    std::shared_ptr<ConstArrayIterator> _inputAttrIter;
    std::shared_ptr<ConstChunkIterator> _inputChunkIter;
    int _iterationMode;
    Coordinates _position;  // satisfies getPositions() const ref return.
    Coordinates _currPos;   // Tracks the current position in the logical chunk space.
    Value _nullValue;       // Holds null value returned when a cell location doesn't exist.
    Value _trueValue;       // Holds true value returned when constructing the bitmap.
    bool _hasCurrent;       // True when this iterator's current position is on the chunk.
    size_t _inputChunkRow;
};

class FoldChunk : public ConstChunk
{
public:
    // 'structors
    FoldChunk(FoldArrayIterator& foldArrayIterator,
              Array const& inputArray);

    virtual ~FoldChunk();

    // Overrides from ConstChunk.
    const ArrayDesc& getArrayDesc() const override;
    const AttributeDesc& getAttributeDesc() const override;
    Coordinates const& getFirstPosition(bool withOverlap) const override;
    Coordinates const& getLastPosition(bool withOverlap) const override;
    std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const override;
    CompressorType getCompressionMethod() const override;
    Array const& getArray() const override;

    /**
     * @return a reference to the FoldArrayIterator owning *this.
     */
    FoldArrayIterator& getArrayIter();

    /**
     * Reinitialize *this to frame the chunk pointed at by the input array iterator.
     *
     * @param inputAttrIter An array iterator from the source array.
     */
    void updateInputIter(std::shared_ptr<ConstArrayIterator> inputAttrIter);

private:
    FoldArrayIterator& _foldArrayIter;
    Array const& _inputArray;
    std::shared_ptr<ConstArrayIterator> _inputAttrIter;
    mutable Coordinates _firstPosition;
    mutable Coordinates _lastPosition;
};

class FoldArrayIterator : public ConstArrayIterator
{
public:
    // 'structors
    FoldArrayIterator(Array const& inputArray,
                      FoldArray& foldArray,
                      const AttributeDesc& foldArrayAttr);
    virtual ~FoldArrayIterator();

    // Overrides from ConstIterator.
    bool end() override;
    void operator++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

    // Overrides from ConstArrayIterator.
    ConstChunk const& getChunk() override;

    /**
     * @return the AttributeDesc instance for the attribute over which
     * *this iterates.
     */
    const AttributeDesc& getAttributeDesc() const;

    /**
     * @return a reference to the FoldArray from which this iterator
     * was created.
     */
    FoldArray& getArray();

    /**
     * @return a reference to the input array over which operator
     * fold() acts.
     */
    Array const& getInputArray();

private:
    /**
     * Indicates if the current chunk referred to by this iterator
     * contains the position corresponding to the attribute in the
     * output array.
     *
     * @return True if the current chunk has the position required
     * for the output attribute; false if not.
     */
    bool _isTargetForAttribute(AttributeID targetId) const;

    /**
     * Get the attribute ID for the attribute in the output
     * array for which this iterates.
     *
     * @return the attribute ID
     */
    AttributeID _getTargetAttribute() const;

    Array const& _inputArray;
    FoldArray& _foldArray;
    const AttributeDesc _foldArrayAttr;
    std::shared_ptr<ConstArrayIterator> _inputAttrIter;
    FoldChunk _foldChunk;
    Coordinates _position;
    bool _hasCurrent;
};

class FoldArray : public Array
{
public:
    // 'structors
    FoldArray(std::shared_ptr<Query> query,
              const ArrayDesc& desc,
              std::shared_ptr<Array> inputArray);

    virtual ~FoldArray();

    // Overrides from Array.
    ArrayDesc const& getArrayDesc() const override;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const override;

    /**
     * @return A valid pointer to the active query if
     * it still exists.
     */
    std::shared_ptr<Query> getQuery();

private:
    const ArrayDesc _desc;
};

}  // namespace scidb

#endif  //  __FOLD_ARRAY__
