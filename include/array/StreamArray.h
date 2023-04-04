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
 * @file StreamArray.h
 *
 * @brief Array receiving chunks from abstract stream
 */

#ifndef STREAM_ARRAY_H_
#define STREAM_ARRAY_H_

#include <vector>

#include <array/Array.h>              // base
#include <array/ConstArrayIterator.h> // base
#include <array/MemChunk.h>           // member
#include <array/ArrayDesc.h>

namespace scidb
{
class StreamArrayIterator;

/**
 * @brief Abstract stream array.
 * @note Uses virtual inheritance because some (all?) derived classes are also SynchableArrays.
 */
class StreamArray: public virtual Array
{
    friend class StreamArrayIterator;
  public:
    std::string const& getName() const override;

    ArrayID getHandle() const override;

    ArrayDesc const& getArrayDesc() const override;

    /**
     * Get the least restrictive access mode that the array supports.
     * @return SINGLE_PASS
     */
    Access getSupportedAccess() const override
    {
        return SINGLE_PASS;
    }

    std::shared_ptr<ArrayIterator> getIteratorImpl(const AttributeDesc& attId) override;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attId) const override;

    /**
     * Constructor
     * @param arr array schema
     * @param emptyCheck if true, StreamArrayIterator will automatically fetch the emptyBitmap chunk
     *        and set it on other attribute chunks
     * @note XXX WARNING: if emptyCheck==true, the iteration MUST be horizontal across all attributes
     *       from 0 to n(-1)
     */
    StreamArray(ArrayDesc const& arr, bool emptyCheck = true);

    /// Destructor
    virtual ~StreamArray() {}

    // Non-copyable, non-assignable.
    StreamArray(const StreamArray& other) = delete;
    StreamArray& operator=(const StreamArray& other) = delete;

    /**
     * Exception indicating that an attempt to get the next chunk should be re-tried
     * because it is not yet ready.
     * The common reasons are: the data have not arrived from remote instance(s) or
     * a SINGLE_PASS array is not being consumed horizontally
     * (when the entire "row" of attributes is consumed, the re-try should succeed)
     */
    DECLARE_SYSTEM_EXC_SUBCLASS_W_ARGS(RetryException,
                                       SCIDB_SE_INTERNAL,
                                       SCIDB_LE_RESOURCE_BUSY,
                                       "StreamArray::RetryException");

protected:
    /**
     * Implemented by subclasses for obtaining the next stream chunk
     * @param attId chunk attribute ID
     * @param chunk which can be used to store the next chunk
     * @return the next chunk
     * @note XXX WARNING: the returned chunk pointer does NOT
     *       necessarily point to the chunk supplied as the second
     *       argument, in which case the second argument chunk remains
     *       unused. Also, it is the responsibility of the subclass to
     *       make sure the returned chunk remains live until the next
     *       call to nextChunk() ... uuuuugly!!!  -- Donghui Z.
     */
    virtual ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) = 0;

    ArrayDesc desc;
    bool emptyCheck;
    std::vector< std::shared_ptr<ConstArrayIterator> > _iterators;
    ConstChunk const* currentBitmapChunk;
};

/**
 * Stream array iterator
 * @note NOT thread-safe i.e. behavior is undefined if multiple StreamArrayIterators execute concurrently
 */
class StreamArrayIterator : public ConstArrayIterator
{
private:
    StreamArray& _array;
    AttributeDesc _attId;
    ConstChunk const* _currentChunk;
    MemChunk _dataChunk;
    MemChunk _bitmapChunk;

    void moveNext();

  public:
    StreamArrayIterator(StreamArray& arr, AttributeID attId);
    ConstChunk const& getChunk() override;
    bool end() override;
    /**
     * @note XXX WARNING Because StreamArray/ClientArray advances the emptybitmap iterator behind the scenes
     * increment of all attributes has to happen simulteneously
     */
    void operator ++() override;
    Coordinates const& getPosition() override;
};

} // namespace
#endif
