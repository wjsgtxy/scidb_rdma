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
 * @file UnfoldArray.h
 *
 * @brief Contains the implementations of the array, iterator, and chunk
 *        types for the unfold operator.
 */

#include <array/DelegateArray.h>
#include <array/TileIteratorAdaptors.h>
#include <vector>

namespace scidb
{
  void copyCoordinates(Coordinates& dst, const Coordinates& src);

  class UnfoldChunkIter;
  class UnfoldArrayIter : public DelegateArrayIterator
  {
    friend class UnfoldChunkIter;
  public:
    UnfoldArrayIter(DelegateArray const& delegate,
		    const AttributeDesc& attrID,
		    const std::shared_ptr<Array>& inputArray);

    virtual ~UnfoldArrayIter();

    /**
     * @return true if no more chunks; false otherwise
     */
      bool end() override;

    /**
     * Go to the next chunk.
     */
    void operator ++() override;

    /**
     * @return the current position as a Coordinates object
     */
    Coordinates const& getPosition() override;

    /**
     * Select chunk which contains element with specified position in main
     * (not overlapped) area.
     * @param pos element position
     * @return true if chunk with containing specified position is present at
     * the local instance, false otherwise
     */
    bool setPosition(Coordinates const& pos) override;

    /**
     * Move to the first element
     */
    void restart() override;

  private:
    // Don't allow the compiler to automatically
    // generate the code for these constructors.
    UnfoldArrayIter();
    UnfoldArrayIter(const UnfoldArrayIter&);
    UnfoldArrayIter& operator=(const UnfoldArrayIter&);

    std::vector<std::shared_ptr<ConstArrayIterator> > _inputArrayIterators;
    Coordinates _position;
  };

  class UnfoldArray : public DelegateArray
  {
  public:
    UnfoldArray(ArrayDesc const& schema,
		const std::shared_ptr<Array>& pinputArray,
		const std::shared_ptr<Query>& pquery);

    virtual ~UnfoldArray();

  private:
    // Don't allow the compiler to automatically
    // generate the code for these constructors.
    UnfoldArray();
    UnfoldArray(const UnfoldArray&);
    UnfoldArray& operator=(const UnfoldArray&);

    // Factory method implementations from DelegateArray.
    virtual DelegateChunkIterator*
      createChunkIterator(DelegateChunk const* chunk,
			  int iterationMode) const;
      DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator,
				       AttributeID id) const;
  };

  class UnfoldChunk : public DelegateChunk
  {
  public:
    UnfoldChunk(DelegateArray const& array,
		DelegateArrayIterator const& iterator,
		AttributeID attrID,
		bool isClone);

    /**
     * Get first position in the iterated chunk according to the iteration mode
     * @param withOverlap consider overlap in the result
     * @return the coordinates
     */
    virtual Coordinates const& getFirstPosition(bool withOverlap) const;

    /**
     * Get last position in the iterated chunk according to the iteration mode
     * @param withOverlap consider overlap in the result
     * @return the coordinates
     */
    virtual Coordinates const& getLastPosition(bool withOverlap) const;

  private:
    // Don't allow the compiler to automatically
    // generate the code for these constructors.
    UnfoldChunk();
    UnfoldChunk(const UnfoldChunk&);
    UnfoldChunk& operator=(const UnfoldChunk&);

    mutable Coordinates _firstPosition;
    mutable Coordinates _lastPosition;
    size_t _unfoldedDimensionUpperBound;
  };

  class UnfoldChunkIter : public DelegateChunkIterator
  {
  public:
    UnfoldChunkIter(const DelegateChunk* chunk,
		    int iterationMode);

    // Disallow these constructors.
    UnfoldChunkIter() = delete;
    UnfoldChunkIter(const UnfoldChunkIter&) = delete;
    UnfoldChunkIter& operator=(const UnfoldChunkIter&) = delete;

    virtual ~UnfoldChunkIter();

    Value const& getItem() override;

    bool isEmpty() const override;

    bool end() override;

    void operator ++() override;

    Coordinates const& getPosition() override;

    bool setPosition(Coordinates const& pos) override;

    void restart() override;

  private:
    std::vector<std::shared_ptr<ConstChunkIterator> > _inputChunkIterators;
    AttributeID _visitingAttribute;
    Coordinates _currentPosition;
  };

  class UnfoldBitmapChunkIter : public DelegateChunkIterator
  {
  public:
    UnfoldBitmapChunkIter(const DelegateChunk* chunk,
		       int iterationMode,
		       AttributeID attrId);

    // Disallow these constructors.
    UnfoldBitmapChunkIter() = delete;
    UnfoldBitmapChunkIter(const UnfoldBitmapChunkIter&) = delete;
    UnfoldBitmapChunkIter& operator=(const UnfoldBitmapChunkIter&) = delete;

    virtual ~UnfoldBitmapChunkIter();

    Value& getItem() override;

    void operator ++() override;

    Coordinates const& getPosition() override;

    bool setPosition(Coordinates const& pos) override;

    void restart() override;

  private:
    Value _value;
    AttributeID _nAttrs;
    AttributeID _visitingAttribute;
    Coordinates _currentPosition;
  };
}
