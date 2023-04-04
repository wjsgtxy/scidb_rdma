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
 * @file DelegateArray.h
 *
 * @brief The implementation of the array delegating all functionality to some other array
 */

#ifndef DELEGATE_ARRAY_H_
#define DELEGATE_ARRAY_H_

#include <string>
#include <boost/shared_array.hpp>
#include <array/MemArray.h>

namespace scidb
{

class DelegateArray;
class DelegateChunkIterator;
class DelegateArrayIterator;

class DelegateChunk : public ConstChunk
{
    friend class DelegateChunkIterator;
public:
    const ArrayDesc& getArrayDesc() const;
    const AttributeDesc& getAttributeDesc() const;
    CompressorType getCompressionMethod() const override;
    Coordinates const& getFirstPosition(bool withOverlap) const override;
    Coordinates const& getLastPosition(bool withOverlap) const override;
    std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const override;

    // Virtual so subclasses can pre-compute stuff (e.g. BetweenChunk).
    virtual void setInputChunk(ConstChunk const& inputChunk);
    ConstChunk const& getInputChunk() const;

    DelegateArrayIterator const& getArrayIterator() const;
    bool isDirectMapping() const;

    size_t count() const override;
    bool isCountKnown() const override;
    bool isMaterialized() const override;
    void* getWriteDataImpl() override;
    void const* getConstDataImpl() const override;
    size_t getSize() const override;
    bool pin() const override;
    void unPin() const override;
    void compress(CompressedBuffer& buf,
                  std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                  bool forceUncompressed) const override;
    Array const& getArray() const;

    void overrideClone(bool clone = true)
    {
        isClone = clone;
    }

    void overrideTileMode(bool enabled) override;

    bool inTileMode() const
    {
        return tileMode;
    }

    DelegateChunk(DelegateArray const& array,
                  DelegateArrayIterator const& iterator,
                  AttributeID attrID, bool isClone);

    DelegateArray const& getDelegateArray() const
    {
        return array;
    }

protected:
    DelegateArray const& array;
    DelegateArrayIterator const& iterator;
    AttributeID attrID;
    ConstChunk const* chunk;
    bool isClone;
    bool tileMode;
};

class DelegateChunkIterator : public ConstChunkIterator
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

    DelegateChunkIterator(DelegateChunk const* chunk, int iterationMode);
    virtual ~DelegateChunkIterator() {}

protected:
    DelegateChunk const* chunk;
    std::shared_ptr<ConstChunkIterator> inputIterator;
};

class DelegateArrayIterator : public ConstArrayIterator
{
public:
	DelegateArrayIterator(DelegateArray const& delegate,
                              const AttributeDesc& attrID,
                              std::shared_ptr<ConstArrayIterator> inputIterator);

    ConstChunk const& getChunk() override;
    std::shared_ptr<ConstArrayIterator> getInputIterator() const;

    bool end() override;
    void operator ++() override;
    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos) override;
    void restart() override;

protected:
    DelegateArray const& array;
    AttributeDesc attr;
    std::shared_ptr<ConstArrayIterator> inputIterator;
    bool chunkInitialized;
    std::shared_ptr<DelegateChunk>& _chunkPtr();

private:
    // Lazy init by _chunkPtr(), no direct access from derived
    // classes.  Otherwise array.createChunk() call gets a pointer
    // to a not-yet-initialized DelegateArrayIterator object.
    std::shared_ptr<DelegateChunk> _chunk;
};

class DelegateArray : public Array
{
public:

    DelegateArray(ArrayDesc const& desc, std::shared_ptr<Array> input, bool isClone = false);
    virtual ~DelegateArray() = default;

    /**
     * Get the least restrictive access mode that the array supports.
     * @return the least restrictive access mode supported by the input array.
     */
    virtual Access getSupportedAccess() const
    {
        return getPipe(0)->getSupportedAccess();
    }

    std::string const& getName() const override;
    ArrayID getHandle() const override;
    ArrayDesc const& getArrayDesc() const override;
    std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& id) const
        override;

    // XXX Can anyone explain why these methods should be const?  It seems overly restrictive.
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const;

protected:
    ArrayDesc desc;
    bool isClone;
};

/**
 * Array with dummy empty-tag attribute - used to perform operations with
 * emptyable and non-emptyable arrays
 */
class NonEmptyableArray : public DelegateArray
{
    class DummyBitmapChunkIterator : public DelegateChunkIterator
    {
    public:
        virtual Value const& getItem();
        virtual bool isEmpty() const;

        DummyBitmapChunkIterator(DelegateChunk const* chunk, int iterationMode);

    private:
        Value _true;
    };

    class DummyBitmapArrayIterator : public DelegateArrayIterator
    {
    public:
        ConstChunk const& getChunk();
        DummyBitmapArrayIterator(DelegateArray const& delegate,
                                 const AttributeDesc& attrID,
                                  std::shared_ptr<ConstArrayIterator> inputIterator);

    private:
        void fillRLEBitmap(MemChunk& chunk) const;

        MemChunk shapeChunk;
    };

public:
	NonEmptyableArray(const std::shared_ptr<Array>& input);

    virtual DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const;

private:
    AttributeID emptyTagID;
};

/**
 * Array splitting C++ array into chunks
 */
class SplitArray : public DelegateArray
{
protected:
    class ArrayIterator : public DelegateArrayIterator
    {
    public:
        ConstChunk const& getChunk() override;
        bool end() override;
        void operator ++() override;
        Coordinates const& getPosition() override;
        bool setPosition(Coordinates const& pos) override;
        void restart() override;

        ArrayIterator(SplitArray const& array, const AttributeDesc& attrID);

    protected:
        MemChunk chunk;
        Address addr;
        Dimensions const& dims;
        SplitArray const& array;
        bool hasCurrent;
        bool chunkInitialized;
    private:
        size_t attrBitSize;
    };

public:
    SplitArray(ArrayDesc const& desc,
               const boost::shared_array<char>& src,
               Coordinates const& from,
               Coordinates const& till,
               std::shared_ptr<Query>const& query);
    virtual ~SplitArray();

    /**
     * Get the least restrictive access mode that the array supports.
     * @return Array::RANDOM
     */
    virtual Access getSupportedAccess() const
    {
        return Array::RANDOM;
    }

    virtual DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const;
    const Coordinates& from() const { return _from; }
    const Coordinates& till() const { return _till; }
    const Coordinates& size() const { return _size; }
    const Coordinates& startingChunk() const { return _startingChunk; }

private:
    Coordinates _startingChunk;
    Coordinates _from;
    Coordinates _till;
    Coordinates _size;
    boost::shared_array<char> _src;
    bool        _empty;
};

/**
 * Array materializing chunks
 */
class MaterializedArray : public DelegateArray
{
public:
    enum MaterializeFormat
    {
        PreserveFormat,
        RLEFormat,
        DenseFormat
    };
    MaterializeFormat _format;
    std::vector< std::map<Coordinates, std::shared_ptr<MemChunk>, CoordinatesLess > > _chunkCache;
    std::map<Coordinates, std::shared_ptr<ConstRLEEmptyBitmap>, CoordinatesLess > _bitmapCache;
    Mutex _mutex;
    size_t _cacheSize;

    static void materialize(const std::shared_ptr<Query>& query, MemChunk& materializedChunk, ConstChunk const& chunk, MaterializeFormat format);

    std::shared_ptr<MemChunk> getMaterializedChunk(ConstChunk const& inputChunk);

    class ArrayIterator : public DelegateArrayIterator
    {
        MaterializedArray& _array;
        ConstChunk const* _chunkToReturn;
        std::shared_ptr<MemChunk> _materializedChunk;

    public:
        ConstChunk const& getChunk() override;
        void operator ++() override;
        bool setPosition(Coordinates const& pos) override;
        void restart() override;

        ArrayIterator(MaterializedArray& arr,
                      const AttributeDesc& attrID,
                      std::shared_ptr<ConstArrayIterator> input,
                      MaterializeFormat chunkFormat);
    };

    MaterializedArray(std::shared_ptr<Array> input, std::shared_ptr<Query>const& query, MaterializeFormat chunkFormat = PreserveFormat);

    virtual DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const;
};

} //namespace
#endif /* DELEGATE_ARRAY_H_ */
