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
 * @file CrossArray.h
 *
 * @brief The implementation of the array iterator for the cross operator
 *
 * The array iterator for the cross maps incoming getChunks calls into the
 * appropriate getChunks calls for its input array. Then, if the requested chunk
 * fits in the cross range, the entire chunk is returned as-is. Otherwise,
 * the appropriate piece of the chunk is carved out.
 *
 * NOTE: In the current implementation if the cross window stretches beyond the
 * limits of the input array, the behavior of the operator is undefined.
 *
 * The top-level array object simply serves as a factory for the iterators.
 */

#ifndef CROSS_JOIN_ARRAY_H_
#define CROSS_JOIN_ARRAY_H_

#include <array/Array.h>            // base class of CrossJoinArray

#include <array/ConstChunk.h>       // base class of CrossJoinChunk
#include <array/ArrayDesc.h>        // member
#include <query/Value.h>            // member


#include <string>
#include <unordered_map>


namespace scidb
{

typedef std::vector<std::pair<Coordinates, Value> > HashBucket;
typedef std::unordered_map<Coordinates, HashBucket, CoordinatesHash> ChunkHash;

class CrossJoinArray;
class CrossJoinArrayIterator;
class CrossJoinChunkIterator;

class CrossJoinChunk : public ConstChunk
{
    friend class CrossJoinChunkIterator;
    friend class CrossJoinArray;
  public:
    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    CompressorType getCompressionMethod() const;
    Coordinates const& getFirstPosition(bool withOverlap) const;
    Coordinates const& getLastPosition(bool withOverlap) const;
    std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;

    void setInputChunk(ConstChunk const* leftChunk, ConstChunk const* rightChunk);

    bool isMaterialized() const;
    virtual Array const& getArray() const;

    CrossJoinChunk(CrossJoinArray const& array, AttributeID attrID, bool isLeftAttr);


  private:
    CrossJoinArray const& array;
    AttributeID attr;
    ConstChunk const* leftChunk;
    ConstChunk const* rightChunk;
    Coordinates firstPos;
    Coordinates firstPosWithOverlap;
    Coordinates lastPos;
    Coordinates lastPosWithOverlap;
    bool isEmptyIndicatorAttribute;
    bool isLeftAttribute;
};

class CrossJoinChunkIterator : public ConstChunkIterator
{
  public:
    int getMode() const override;
    Value const& getItem() override;
    bool isEmpty() const override;
    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    ConstChunk const& getChunk() override;

    CrossJoinChunkIterator(CrossJoinChunk const& chunk, int iterationMode);

  private:
    ssize_t findValueInBucket(HashBucket const* bucket, Coordinates const& coords) const;

    CrossJoinArray const& array;
    CrossJoinChunk const& chunk;
    std::shared_ptr<ConstChunkIterator> leftIterator;
    Coordinates currentPos;
    bool hasCurrent;
    Value boolValue;

    HashBucket const* currentBucket;
    ssize_t currentIndex;
    ChunkHash rightHash;
};

/***
 * The iterator of the cross implements all the calls of the array iterator API though setPosition
 * calls.
 * NOTE: This looks like a candidate for an intermediate abstract class: PositionConstArrayIterator.
 */
class CrossJoinArrayIterator : public ConstArrayIterator
{
  public:
	/***
	 * Constructor for the cross iterator
	 * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
	 * attribute in the input array.
	 */
	CrossJoinArrayIterator(CrossJoinArray const& cross, AttributeID attrID,
                           std::shared_ptr<ConstArrayIterator> leftIterator,
                           std::shared_ptr<ConstArrayIterator> rightIterator,
                           std::shared_ptr<ConstArrayIterator> inputIterator);

	/***
	 * Get chunk method retrieves the chunk at current position from the input iterator and either
	 * passes it up intact (if the chunk is completely within the cross window), or carves out
	 * a piece of the input chunk into a fresh chunk and sends that up.
	 *
	 * It performs a mapping from the cross coordinates to the coordinates of the input array
	 * It does not check any bounds, since the setPosition call will not set an invalid position.
	 *
	 * NOTE: Currently we don't check where the cross window streches beyond the limits of the
	 * input array. What is the behavior then - padd with NULLs?
	 */
	ConstChunk const& getChunk() override;

	/***
	 * The end call checks whether we're operating with the last chunk of the cross
	 * window.
	 */
	bool end() override;

	/***
	 * The ++ operator advances the current position to the next chunk of the cross
	 * window.
	 */
	void operator ++() override;

	/***
	 * Simply returns the current position
	 * Initial position is a vector of zeros of appropriate dimensionality
	 */
	Coordinates const& getPosition() override;

	/***
	 * Here we only need to check that we're not moving beyond the bounds of the cross window
	 */
	bool setPosition(Coordinates const& pos) override;

	/***
	 * Restart simply changes the current position to all zeros
	 */
	void restart() override;

  private:
    CrossJoinArray const& array;
	AttributeID attr;
    std::shared_ptr<ConstArrayIterator> leftIterator;
    std::shared_ptr<ConstArrayIterator> rightIterator;
    std::shared_ptr<ConstArrayIterator> inputIterator;
    CrossJoinChunk chunk;
	Coordinates currentPos;
    bool hasCurrent;
    bool chunkInitialized;
};

class CrossJoinArray : public Array
{
    friend class CrossJoinChunk;
    friend class CrossJoinChunkIterator;
    friend class CrossJoinArrayIterator;

  public:
    CrossJoinArray(const ArrayDesc& desc,
                   const std::shared_ptr<Array>& left,
                   const std::shared_ptr<Array>& right,
                   std::vector<int> const& leftJoinDims,
                   std::vector<int> const& rightJoinDims);

    virtual const ArrayDesc& getArrayDesc() const;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& id) const override;

    bool matchPosition(Coordinates const& left, Coordinates const& right) const;
    Coordinates getLeftPosition(Coordinates const& pos) const;
    Coordinates getRightPosition(Coordinates const& pos) const;
    Coordinates getPosition(Coordinates const& left, Coordinates const& right) const;

    void decomposeRightCoordinates(Coordinates const& right, Coordinates& hashKey, Coordinates &rightLeftover) const;
    void decomposeOutCoordinates(Coordinates const& out, Coordinates& left, Coordinates& hashKey, Coordinates& rightLeftover) const;
    void decomposeLeftCoordinates(Coordinates const& left, Coordinates& hashKey) const;
    void composeOutCoordinates(Coordinates const &left, Coordinates const& rightLeftover, Coordinates& out) const;

  private:
    ArrayDesc desc;
    ArrayDesc leftDesc;
    ArrayDesc rightDesc;
    std::shared_ptr<Array> left() const { return getPipe(0); }
    std::shared_ptr<Array> right() const { return getPipe(1); }

    const AttributeID nLeftDims;
    const AttributeID nRightDims;
    const AttributeID nLeftAttrs;
    const AttributeID nRightAttrs;

    //Hash key has right coordinates in the order they appear in right array

    //for each left coordinate - either 0-based index into hash key OR -1
    std::vector<int> leftJoinDims;
    std::vector<int> rightJoinDims;

    size_t nJoinDims;

    const AttributeDesc* leftEmptyTagPosition;
    const AttributeDesc* rightEmptyTagPosition;
};


} //namespace

#endif /* CROSS_ARRAY_H_ */
