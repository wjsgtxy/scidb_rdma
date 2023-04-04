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
 * @file MemChunk.h
 *
 * @brief In-Memory (temporary) chunk implementation
 */

#ifndef MEM_CHUNK_H_
#define MEM_CHUNK_H_

#include <vector>
#include <map>
#include <assert.h>
#include <memory>
#include <array/Array.h>
#include <array/Chunk.h>            // base
#include <array/Tile.h>             // base ( CoordinatesMapperProvider (yes, unexpected location))
#include <array/Address.h>

namespace scidb
{
    class Chunk;
    class Query;
    class MemArray;
#ifndef SCIDB_CLIENT
    class MemArrayIterator;
#endif

    /**
     * Chunk of temporary (in-memory) array
     */
    class MemChunk : public Chunk
    {
#ifndef SCIDB_CLIENT
        friend class MemArray;
        friend class MemArrayIterator;
        friend class DBArray;
        friend class DBArrayIterator;
#endif
    protected:
        Address addr; // address of first chunk element
        mutable void* data; // uncompressed data (may be NULL if swapped out)
        mutable bool dirty; // true if dirty data might be present in buffer
        mutable size_t size;
        size_t  nElems;
        mutable CompressorType compressionMethod; // compress() may change this
        Coordinates firstPos;
        Coordinates firstPosWithOverlaps;
        Coordinates lastPos;
        Coordinates lastPosWithOverlaps;
        ArrayDesc const* arrayDesc;
        Chunk* bitmapChunk;
        Array const* array;
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        std::shared_ptr<ConstChunkIterator>
            getConstIterator(std::shared_ptr<Query> const& query, int iterationMode) const;

      public:
        MemChunk();
        ~MemChunk();

        std::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const override;
        void setEmptyBitmap(std::shared_ptr<ConstRLEEmptyBitmap> const& bitmap);

        Address const& getAddress() const
        {
            return addr;
        }

        /**
         * @see ConstChunk::isMemChunk
         */
        bool isMemChunk() const override
        {
            return true;
        }

        size_t count() const override;
        bool isCountKnown() const override;
        void setCount(size_t count) override;

        /**
         * @see ConstChunk::isMaterialized
         */
        bool isMaterialized() const override;

        /**
         * @see ConstChunk::materialize
         */
        ConstChunk* materialize() const override
        {
            assert(!isMaterializedChunkPresent());
            return const_cast<MemChunk*> (this);
        }

        /**
         * @see Chunk::write
         */
        void write(const std::shared_ptr<Query>& query) override;

        virtual void initialize(Array const* array, ArrayDesc const* desc,
                                const Address& firstElem, CompressorType compressionMethod);

        virtual void initialize(ConstChunk const& srcChunk);

        void setBitmapChunk(Chunk* bitmapChunk);

        virtual bool isInitialized() const
        {
            return arrayDesc != NULL;
        }

        ConstChunk const* getBitmapChunk() const override;

        Array const& getArray() const override;
        const ArrayDesc& getArrayDesc() const override;
        void setArrayDesc(const ArrayDesc* desc) { arrayDesc = desc; assert(desc); }
        const AttributeDesc& getAttributeDesc() const override;
        CompressorType getCompressionMethod() const  override { return compressionMethod; }
        void* getWriteDataImpl() override { dirty = true; return data; }
        void const* getConstDataImpl() const override{ return data; }
        bool isDirty() const override { return dirty; }
        void markClean() const { dirty = false; }
        /**
         * returns the amount of memory, in bytes, backing the chunk
         * @return size_t containing the number of bytes in memory
         */
        size_t getSize() const override { return size; }
        void allocate(size_t size) override;
        void reallocate(size_t size) override;
        void free() override;
        Coordinates const& getFirstPosition(bool withOverlap) const override;
        Coordinates const& getLastPosition(bool withOverlap) const override;
        std::shared_ptr<ChunkIterator> getIterator(std::shared_ptr<Query> const& query,
                                                   int iterationMode) override;
        std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const override;
        bool pin() const override;
        void unPin() const override;

        void compress(CompressedBuffer& buf,
                      std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                      bool forceUncompressed) const override;
        void decompress(const CompressedBuffer& buf) override;
        void setPayload(SharedBuffer const* buf);

        static size_t getFootprint(size_t ndims)
        { return sizeof(MemChunk) + (4 * (ndims * sizeof(Coordinate))); }
    };

    class BaseChunkIterator: public ChunkIterator, protected CoordinatesMapper
    {
      protected:
        ArrayDesc const& array;
        AttributeID attrID;
        AttributeDesc const& attr;
        Chunk* dataChunk;
        bool   dataChunkPinned;
        bool   dataChunkRegistered;
        bool   hasCurrent;
        bool   hasOverlap;
        bool   isEmptyable;
        int    mode;
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        ConstRLEEmptyBitmap::iterator emptyBitmapIterator;

        Coordinates currPos;
        TypeId typeId;
        Type   type;
        Value const& defaultValue;
        uint64_t tilePos;       // Position in # of cells, always a multiple of tileSize
        uint64_t tileSize;      // Cells in a full tile, determined by Config and chunk size
        bool isEmptyIndicator;
        std::weak_ptr<Query> _query;

        BaseChunkIterator(ArrayDesc const& desc,
                          AttributeID attr,
                          Chunk* data,
                          int iterationMode,
                          std::shared_ptr<Query> const& query);
        ~BaseChunkIterator();

      public:
        int  getMode() const override;
        bool isEmpty() const override;
        bool end() override;
        bool setPosition(Coordinates const& pos) override;
        void operator ++() override;
        ConstChunk const& getChunk() override;
        void restart() override;
        void writeItem(const Value& item) override;
        void flush() override;
        Coordinates const& getPosition() override;
        std::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap();

        std::shared_ptr<Query> getQuery() override
        {
            return _query.lock();
        }

        uint64_t getTileSize() const
        {
            return tileSize;
        }

        uint64_t getTilePos() const
        {
            return tilePos;
        }
    };

    /**
     * Base for duck-typed chunk iterator types that treat their referent
     * chunks as read-only.  This will pin the passed chunk on creation
     * and unpin it on destruction.  Assumes that the passed chunk is not
     * dirty at destruction time, enforcing that the extending chunk
     * iterator type treats its corresponding chunk as read-only.
     */
    struct PinnedChunkIterator : BaseChunkIterator
    {
        PinnedChunkIterator(ArrayDesc const& desc,
                            AttributeID attr,
                            Chunk* data,
                            int iterationMode,
                            std::shared_ptr<Query> const& query);

        ~PinnedChunkIterator();
    };

    class RLEConstChunkIterator : public PinnedChunkIterator
    {
      public:
        RLEConstChunkIterator(ArrayDesc const& desc,
                              AttributeID attr,
                              Chunk* data,
                              Chunk* bitmap,
                              int iterationMode,
                              std::shared_ptr<Query> const& query);

        Value const& getItem() override;
        using ConstChunkIterator::setPosition;
        bool setPosition(Coordinates const& pos) override;
        void operator ++() override;
        void restart() override;

      private:
        ConstRLEPayload payload;
        ConstRLEPayload::iterator payloadIterator;

        Value value;
    };

    /**
     * RLEBitmapChunkIterator treats its referent chunks as read-only while not
     * following the 'const' naming convention of other iterators in the system.
     */
    class RLEBitmapChunkIterator : public PinnedChunkIterator
    {
      public:
        Value const& getItem();

        RLEBitmapChunkIterator(ArrayDesc const& desc, AttributeID attr,
                               Chunk* data,
                               Chunk* bitmap,
                               int iterationMode,
                               std::shared_ptr<Query> const& query);

      private:
        Value trueValue;
        Value value;
    };

    class RLEChunkIterator : public BaseChunkIterator
    {
      public:
        bool isEmpty() const;
        Value const& getItem();
        void writeItem(const Value& item);
        void flush();
        bool setPosition(Coordinates const& pos);

        RLEChunkIterator(ArrayDesc const& desc, AttributeID attr,
                         Chunk* data,
                         Chunk* bitmap,
                         int iterationMode,
                         std::shared_ptr<Query> const& query);
        virtual ~RLEChunkIterator();

        void unpin() override;

      private:
        position_t getPos() const {
            return isEmptyable ? emptyBitmapIterator.getLPos() : emptyBitmapIterator.getPPos();
        }
        arena::ArenaPtr const _arena;
        ValueMap  _values;
        size_t    _valuesFootprint;  // current memory footprint of elements in _values
        size_t    _initialFootprint; // memory footprint of original data payload
        Value     trueValue;
        Value     falseValue;
        Value     tmpValue;
        Value     tileValue;
        std::shared_ptr<ChunkIterator> emptyChunkIterator;
        RLEPayload payload;
        Chunk* bitmapChunk;
        RLEPayload::append_iterator appender;
        position_t prevPos;
        size_t    _sizeLimit;

        /**
         * Exception-safety control flag. This is checked by in the destructor of RLEChunkIterator,
         * to make sure unPin() is called upon destruction, unless flush() already executed.
         */
        bool _needsFlush;
    };

    /**
     * Abstract base chunk iterator providing functionality which keeps track of the iterator logical position within a chunk.
     * @note It uses an RLE empty bitmap extracted from a different materialized chunk (aka the empty bitmap chunk)
     */
    class BaseTileChunkIterator: public ConstChunkIterator, protected CoordinatesMapper
    {
      protected:
        ArrayDesc const& _array;
        AttributeID      _attrID;
        AttributeDesc const& _attr;
        Chunk* _dataChunk;
        bool   _dataChunkRegistered;
        bool   _hasCurrent;
        bool   _hasOverlap;
        int    _mode;
        std::shared_ptr<ConstRLEEmptyBitmap> _emptyBitmap;
        ConstRLEEmptyBitmap::iterator _emptyBitmapIterator;
        Coordinates _currPos;
        std::weak_ptr<Query> _query;

        BaseTileChunkIterator(ArrayDesc const& desc,
                              AttributeID attr,
                              Chunk* data,
                              int iterationMode,
                              std::shared_ptr<Query> const& query);

        virtual ~BaseTileChunkIterator();

      public:
        int  getMode() const override;
        bool isEmpty() const override;
        bool end() override;
        position_t getLogicalPosition() override;
      protected:
        bool setPosition(position_t pos) override;
        bool setPosition(Coordinates const& pos) override;
        void operator ++() override;
        void restart() override;
        Coordinates const& getPosition() override;
    public:
        ConstChunk const& getChunk() override;
        std::shared_ptr<Query> getQuery()
        {
            return _query.lock();
        }
    };

    class BaseTile;

    /**
     * Concrete chunk iterator class providing a tile-at-a-time access to chunk's data via getData()
     * as well as a value-at-a-time access via getItem().
     */
    class RLETileConstChunkIterator : public BaseTileChunkIterator
    {
    public:
        RLETileConstChunkIterator(ArrayDesc const& desc,
                                  AttributeID attr,
                                  Chunk* data,
                                  Chunk* bitmap,
                                  int iterationMode,
                                  std::shared_ptr<Query> const& query);
        ~RLETileConstChunkIterator();
        Value const& getItem() override;
        Coordinates const& getPosition() override;
        bool setPosition(Coordinates const& pos) override;
        bool setPosition(position_t pos) override;
        void operator ++() override;
        void restart() override;

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           std::shared_ptr<BaseTile>& tileData,
                                           std::shared_ptr<BaseTile>& tileCoords) override;

        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                           size_t maxValues,
                           std::shared_ptr<BaseTile>& tileData,
                           std::shared_ptr<BaseTile>& tileCoords) override;

        /// @see ConstChunkIterator
        virtual const Coordinates& getData(scidb::Coordinates& logicalStart /*IN/OUT*/,
                                           size_t maxValues,
                                           std::shared_ptr<BaseTile>& tileData);

        /// @see ConstChunkIterator
        virtual position_t getData(position_t logicalStart,
                                   size_t maxValues,
                                   std::shared_ptr<BaseTile>& tileData) override;

        /// @see ConstChunkIterator
        operator const CoordinatesMapper* () const override { return this; }
    private:
        /// Helper to pin the chunk and construct a payload iterator
        void prepare();
        void unPrepare();

        /// Helper to make sure that the payload iterator corresponds to the EBM iterator
        void alignIterators();

        /**
         * Return a tile of at most maxValues starting at logicalStart.
         * The logical position is advanced by the size of the returned tile.
         * @parm logicalStart - logical position (in row-major order) within a chunk of the first data element
         * @param maxValues   - max number of values in a tile
         * @param tileData    - output data tile
         * @param tileCoords  - output tile of logical position_t's, one for each element in the data tile
         * @param coordTileType - "scidb::Coordinates"
         * @param coordCtx      - any context necessary for constructing the tile of coordinates
         * @return positon_t(-1) if no data is found at the logicalStart position
         *         (either at the end of chunk or at a logical "whole" in the serialized data);
         *         otherwise, the next position where data exist in row-major order.
         *         If positon_t(-1) is returned, the output variables are not modified.
         */
        position_t
        getDataInternal(position_t logicalStart,
                        size_t maxValues,
                        std::shared_ptr<BaseTile>& tileData,
                        std::shared_ptr<BaseTile>& tileCoords,
                        const scidb::TypeId& coordTileType,
                        const BaseTile::Context* coordCtx);
        /**
         * Return a tile of at most maxValues starting at logicalStart.
         * The logical position is advanced by the size of the returned tile.
         * @parm logicalStart - logical position (in row-major order) within a chunk of the first data element
         * @param maxValues   - max number of values in a tile
         * @param tileData    - output data tile
         * @return positon_t(-1) if no data is found at the logicalStart position
         *         (either at the end of chunk or at a logical "whole" in the serialized data);
         *         otherwise, the next position where data exist in row-major order.
         *         If positon_t(-1) is returned, the output variables are not modified.
         */
        position_t
        getDataInternal(position_t logicalOffset,
                        size_t maxValues,
                        std::shared_ptr<BaseTile>& tileData);

        class CoordinatesMapperWrapper : public CoordinatesMapperProvider
        {
        private:
            CoordinatesMapper* _mapper;
        public:
            CoordinatesMapperWrapper(CoordinatesMapper* mapper) : _mapper(mapper)
            { assert(_mapper); }
            virtual ~CoordinatesMapperWrapper() {}
            virtual operator const CoordinatesMapper* () const { return _mapper; }
        };

        class RLEPayloadDesc : public rle::RLEPayloadProvider
        {
        public:
            RLEPayloadDesc(ConstRLEPayload* rlePayload, position_t offset, size_t numElem);
            virtual const ConstRLEPayload* getPayload() const  { return _rlePayload; }
            virtual position_t getOffset() const { return _offset; }
            virtual size_t getNumElements() const { return _numElem; }
        private:
            const ConstRLEPayload* _rlePayload;
            const position_t _offset;
            const size_t _numElem;
        };

    private:
        /// data chunk RLE payload
        ConstRLEPayload _payload;
        ConstRLEPayload::iterator _payloadIterator;

        /// Current logical positon within a chunk,
        /// not needed as long as EBM is never unpinned/not in memory
        position_t _lPosition;

        /// cached singleton pointer
        TileFactory* _tileFactory;
        //// The data is non-bool, fixed size, and ebm is aligned with data
        mutable bool _fastTileInitialize;
        /// Whether the payload is always pinned
        bool  _isDataChunkPinned;
    protected:
        Value _value;
    };

    /**
     *  Locking-aware support for arena allocation of a MemChunk-derived chunk.
     *
     *  @param[in] a    heap arena from which to allocate an object
     *
     *  @details Use @i this routine rather than arena::newScalar()
     *  when allocating chunks from an arena.  Using newScalar
     *  introduces a deadlock, because the destructor will be run as
     *  an arena finalizer with the arena mutex held.  But destruction
     *  involves calls into BufferMgr, which grabs its _blockMutex and
     *  then performs further arena operations.  If another thread is
     *  already holding the _blockMutex and making arena calls, the
     *  instance will deadlock.
     *
     *  @see SDB-5988
     */
    template <class CHUNK>
    CHUNK* createCachedChunk(arena::Arena& a)
    {
        // Allocate raw bytes from the arena and then construct the chunk
        // in-place.  This avoids setting up an arena finalizer for the
        // object, which can cause a nasty deadlock.

        char *p = arena::newVector<char>(a, sizeof(CHUNK));
        try {
            return new (p) CHUNK();
        }
        catch (...) {
            arena::destroy(a, p);
            throw;
        }
        SCIDB_UNREACHABLE();
    }

    /**
     *  Locking-aware support for arena free of a MemChunk-derived chunk.
     *
     *  @param[in] a    heap arena into which to release object memory
     *  @param[in] p    pointer to object to destroy and deallocate
     *  @see SDB-5988
     */
    template <class CHUNK>
    void destroyCachedChunk(arena::Arena& a, CHUNK* chunk)
    {
        chunk->~CHUNK();    // in-place dtor
        arena::destroy(a, reinterpret_cast<char*>(chunk));
    }

} // scidb namespace

#endif // MEM_CHUNK_H_
