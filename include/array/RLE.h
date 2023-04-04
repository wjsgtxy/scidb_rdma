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

#ifndef RLE_H
#define RLE_H

#include <array/Coordinate.h>
#include <array/RLESegment.h>
#include <query/Value.h>
#include <system/Exceptions.h>
#include <util/arena/Map.h>

#include <map>
#include <vector>
#include <memory>
#include <boost/serialization/access.hpp>
#include <boost/serialization/split_member.hpp>
#include <boost/serialization/vector.hpp>
#include <limits>

namespace scidb
{
const uint64_t RLE_EMPTY_BITMAP_MAGIC = 0xEEEEAAAA00EEBAACLL;
const uint64_t RLE_PAYLOAD_MAGIC      = 0xDDDDAAAA000EAAACLL;

class ConstChunk;
class RLEPayload;
class ArrayDesc;

typedef mgd::map<position_t, Value> ValueMap;

/**
 * Type for offsets into the variable length part ("VarPart") of a payload.
 * A change to this type will require a new on-disk storage version.
 */
typedef uint32_t varpart_offset_t;
const size_t RLE_MAX_VARPART_OFFSET = std::numeric_limits<varpart_offset_t>::max();

extern void checkChunkMagic(ConstChunk const& chunk, const char* where);

class RLEEmptyBitmap;
class ConstRLEEmptyBitmap
{
    friend class RLEEmptyBitmap;
public:
    struct Segment {
        position_t _lPosition;   // start position of sequence of set bits
        position_t _length;  // number of set bits
        position_t _pPosition; // index of value in payload

        Segment()
            : _lPosition(-1),
              _length(-1),
              _pPosition(-1) {}
    };

    // This structure must use platform independent data types with fixed size.
    struct Header {
        uint64_t _magic;
        uint64_t _nSegs;
        uint64_t _nNonEmptyElements;
    };

  protected:
    // Pointers are to memory not owned by this object.
    size_t _nSegs;
    Segment const* _seg;
    uint64_t _nNonEmptyElements;
    ConstChunk const* _chunk;
    bool _chunkPinned;

    /**
     * Default constructor
     */
    ConstRLEEmptyBitmap()
        : _nSegs(0)
        , _seg(NULL)
        , _nNonEmptyElements(0)
        , _chunk(NULL)
        , _chunkPinned(false)
    {}

  public:

    size_t getValueIndex(position_t pos) const {
        size_t r = findSegment(pos);
        return (r < _nSegs && _seg[r]._lPosition <= pos)
            ? _seg[r]._pPosition + pos - _seg[r]._lPosition
            : size_t(-1);
    }

    /**
     * Check if element at specified position is empty
     */
    bool isEmpty(position_t pos) const {
        size_t r = findSegment(pos);
        return r == _nSegs || _seg[r]._lPosition > pos;
    }

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const {
        return _nSegs;
    }

    /**
     * Get next i-th segment corresponding to non-empty elements
     */
    Segment const& getSegment(size_t i) const {
        assert(i < _nSegs);
        return _seg[i];
    }

    /**
     * Return the last logical position covered by this bitmap.
     */
    position_t getLastLogicalPosition() const {
        assert(nSegments() > 0);
        Segment const& sg = getSegment(nSegments()-1);
        return sg._lPosition + sg._length - 1;
    }

    /**
     * Find segment of non-empty elements with position greater or equal than specified.
     */
    size_t findSegment(position_t pos) const {
        size_t l = 0, r = _nSegs;
        while (l < r) {
            size_t m = (l + r) >> 1;
            if (_seg[m]._lPosition + _seg[m]._length <= pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    /**
     * Method to be called to save bitmap in chunk body
     */
    void pack(char* dst) const;

    /**
     * Get size needed to pack bitmap (used to determine size of chunk)
     */
    size_t packedSize() const
    {
        return sizeof(Header) + _nSegs*sizeof(Segment);
    }

    /**
     * Constructor for initializing Bitmap with raw chunk data
     */
    ConstRLEEmptyBitmap(char const* src);

    ConstRLEEmptyBitmap(ConstChunk const& chunk);

    virtual ~ConstRLEEmptyBitmap();

    std::ostream& getInfo(std::ostream& stream, bool verbose=false) const;

    /// Iterate over the non-empty cells in a ConstRLEEmptyBitmap.
    class iterator
    {
      private:
        ConstRLEEmptyBitmap const* _bm;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currLPos;

        bool const_end() const { return _currSeg >= _bm->nSegments(); }

      public:
        iterator(ConstRLEEmptyBitmap const* bm):
        _bm(bm), _currSeg(0), _cs(NULL), _currLPos(-1)
        {
            restart();
        }

        iterator():
        _bm(NULL), _currSeg(0), _cs(NULL), _currLPos(-1)
        {}

        void restart()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_bm->getSegment(_currSeg);
                _currLPos = _cs->_lPosition;
            }
        }

        bool end()
        {
            return const_end();
        }

        position_t const& getLPos() const
        {
            assert(!const_end());
            return _currLPos;
        }

        position_t getPPos() const
        {
            assert(!const_end());
            return _cs->_pPosition + _currLPos - _cs->_lPosition;
        }

        bool setPosition(position_t lPos)
        {
            _currSeg = _bm->findSegment(lPos);
            if (end() || _bm->getSegment(_currSeg)._lPosition > lPos)
            {
                _currSeg = _bm->nSegments();
                return false;
            }
            _cs = &_bm->getSegment(_currSeg);
            _currLPos = lPos;
            return true;
        }

        bool skip(size_t n);

        void operator ++()
        {
            assert(!end());
            if (_currLPos + 1 < _cs->_lPosition + _cs->_length)
            {
                _currLPos ++;
            }
            else
            {
                _currSeg++;
                if (!end())
                {
                    _cs = &_bm->getSegment(_currSeg);
                    _currLPos = _cs->_lPosition;
                }
            }
        }
    };

    /**
     * An iterator that iterates through the segments, rather than the individual non-empty cells.
     * It also helps remember an 'offset' within a segment, enabling the caller to take a partial
     * segment out and treat the remains of the segment as another virtual segment.
     */
    class SegmentIterator
    {
    private:
        ConstRLEEmptyBitmap const* _bm;
        size_t _currSeg;
        position_t _offset;  // offset of _lPosition

    public:
        /**
         * Constructor.
         */
        SegmentIterator(ConstRLEEmptyBitmap const* bm): _bm(bm)
        {
            restart();
        }

        /**
         * Move to the beginning of the first segment.
         */
        void restart()
        {
            _currSeg = 0;
            _offset = 0;
        }

        /**
         * Whether there is no more segment.
         */
        bool end() const
        {
            return _currSeg >= _bm->nSegments();
        }

        /**
         * Get lPosition, pPosition, and length, for the current virtual segment (could be in the middle of an actual segment).
         * @param[out] ret     the segment to be returned
         */
        void getVirtualSegment(ConstRLEEmptyBitmap::Segment& ret) const
        {
            assert(!end());
            ConstRLEEmptyBitmap::Segment const& segment = _bm->getSegment(_currSeg);
            ret._lPosition = segment._lPosition + _offset;
            ret._pPosition = segment._pPosition + _offset;
            ret._length = segment._length - _offset;
        }

        /**
         * Advance the position, within the same segment.
         * @param stepSize  how many positions to skip
         * @pre stepSize must be LESS than the remaining length of the virtual segment; to advance to the end of the segment you should use operator++()
         */
        void advanceWithinSegment(position_t stepSize)
        {
            assert(stepSize > 0);
            assert(!end());
            assert(_offset + stepSize < _bm->getSegment(_currSeg)._length);
            _offset += stepSize;
        }

        /**
         * Advance to the next segment.
         */
        void operator++()
        {
            assert(!end());
            ++ _currSeg;
            _offset = 0;
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }

    uint64_t count() const
    {
        return _nNonEmptyElements;
    }

    /**
     * Extract subregion from bitmap.
     *
     * @param lowerOrigin lower coordinates of original array.
     * @param upperOrigin upper coordinates of original array.
     * @param lowerResult lower coordinates of subarray.
     * @param lowreResult lower coordinates of subarray.
     *
     * @return bitmap with same shape and zeros in (Original MINUS Subarray) areas.
     */
    std::shared_ptr<RLEEmptyBitmap> cut(
            Coordinates const& lowerOrigin,
            Coordinates const& upperOrigin,
            Coordinates const& lowerResult,
            Coordinates const& upperResult) const;
};

std::ostream& operator<<(std::ostream& stream, ConstRLEEmptyBitmap const& map);

class RLEEmptyBitmap : public ConstRLEEmptyBitmap
{
  private:
    std::vector<Segment> _container;

    position_t addRange(position_t lpos, position_t ppos, uint64_t sliceSize, size_t level,
                        Coordinates const& chunkSize,
                        Coordinates const& origin,
                        Coordinates const& first,
                        Coordinates const& last);

  public:
    void reserve(size_t size) {
        _container.reserve(size);
    }

    void clear()
    {
        _container.clear();
        _seg = NULL;
        _nSegs = 0;
        _nNonEmptyElements = 0;
    }

    void addSegment(Segment const& segm)
    {
        if (_nSegs > 0)
        {
            assert(segm._lPosition >= _container[_nSegs-1]._lPosition + _container[_nSegs-1]._length &&
                   segm._pPosition >= _container[_nSegs-1]._pPosition + _container[_nSegs-1]._length);
        }

        _container.push_back(segm);
        _seg = &_container[0];
        _nNonEmptyElements += segm._length;
        _nSegs++;
    }

    void addPositionPair(position_t const& lPosition, position_t const& pPosition)
    {
        _nNonEmptyElements += 1;
        if (_nSegs > 0 &&
            _container[_nSegs-1]._lPosition + _container[_nSegs-1]._length == lPosition &&
            _container[_nSegs-1]._pPosition + _container[_nSegs-1]._length == pPosition)
        {
            _container[_nSegs-1]._length++;
        }
        else
        {
            Segment ns;
            ns._lPosition=lPosition;
            ns._pPosition=pPosition;
            ns._length=1;
            addSegment(ns);
        }
    }

    RLEEmptyBitmap& operator=(ConstRLEEmptyBitmap const& other)
    {
        if (static_cast<ConstRLEEmptyBitmap*>(this) != &other) {
            _nSegs = other.nSegments();
            _nNonEmptyElements = other._nNonEmptyElements;
            _container.resize(_nSegs);
            if (_container.empty()) {
                _seg = NULL;
            } else {
                memcpy(&_container[0], other._seg, _nSegs*sizeof(Segment));
                _seg = &_container[0];
            }
        }
        return *this;
    }

    RLEEmptyBitmap(ConstRLEEmptyBitmap const& other):
        ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    RLEEmptyBitmap& operator=(RLEEmptyBitmap const& other)
    {
        if (this != &other) {
            _nSegs = other._nSegs;
            _nNonEmptyElements = other._nNonEmptyElements;
            _container = other._container;
            _seg = _container.empty() ? NULL : &_container[0];
        }
        return *this;
    }

    RLEEmptyBitmap(RLEPayload& payload);

    RLEEmptyBitmap(RLEEmptyBitmap const& other):
            ConstRLEEmptyBitmap()
    {
        *this = other;
    }

    /**
     * Default constructor
     */
    RLEEmptyBitmap(): ConstRLEEmptyBitmap()
    {}

    /*
     * Create fully dense bitmask of nBits bits
     */
    RLEEmptyBitmap(position_t nBits): ConstRLEEmptyBitmap(), _container(1)
    {
        _container[0]._lPosition = 0;
        _container[0]._length = nBits;
        _container[0]._pPosition = 0;
        _nSegs=1;
        _nNonEmptyElements = nBits;
        _seg = &_container[0];
    }

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     */
    RLEEmptyBitmap(ValueMap& vm, bool all = false);

    /**
     * Constructor of RLE bitmap from dense bit vector
     */
    RLEEmptyBitmap(char* data, size_t numBits);

    /**
     * Constructor for initializing Bitmap from specified chunk
     */
    RLEEmptyBitmap(ConstChunk const& chunk);
};

class RLEPayload;

/**
  * This class stores values in a stride-major-ordered array with RLE-packing of data.
  *
  * @description
  * We have the payload array where we store values.
  * The payload array is split into separated parts called segments.
  * Each segment has description (rle::Segment).  All Segments stored within a container.
  * Every segment has the following fields:
  *  - pPosition:  physical position (stride-major-order) of first value from segment
  *  - valueIndex:  byte number inside the payload array where the data for this segment is
  *      located, or a value for missingReason if the segment is absent (nulls, empty, etc).
  *  - same:  true if all values in the segment are equal
  *  - null:  bit describing valueIndex
  *
  * @note This class does NOT take ownership of passed-in payloads.
  * @note Cannot add values.
  */
class ConstRLEPayload
{
friend class boost::serialization::access;
friend class RLEPayload;
public:

    typedef rle::Segment Segment;

    // This structure has platform independent data types and explicit padding because it is
    // implicitly part of the 'opaque' file format.
    struct Header {
        uint64_t  _magic;
        uint64_t  _nSegs;
        uint64_t  _elemSize;
        uint64_t  _dataSize;
        uint64_t  _varOffs;
        uint8_t   _isBoolean;
        uint8_t   _pad[7];
    } __attribute__((packed));

  protected:
    uint64_t _nSegs;
    uint64_t _elemSize;
    uint64_t _dataSize;
    uint64_t _varOffs;
    bool   _isBoolean;

    Segment* _seg;
    // case 1:
    // 1,1,1,2,2,3,0,0,0,0,0,5,5,5
    // seg = {0,0,true}, {3,1,true}, {5,2,true}, {6,3,true}, {11 ,4,true}, {14}
    // case 2:
    // 1,2,3,4,5,0,0,0,0
    // seg = {0,0,false}, {5,5,true}, {10}
    char* _payload;

    ConstRLEPayload()
        : _nSegs(0), _elemSize(0), _dataSize(0), _varOffs(0), _isBoolean(false), _seg(NULL), _payload(NULL)
    {}

    static varpart_offset_t* idxToVoffAddr(const char* payload, uint32_t index)
    {
        return ((varpart_offset_t*)payload) + index;
    }

    static varpart_offset_t idxToVoff(const char* payload, uint32_t index)
    {
        return *idxToVoffAddr(payload, index);
    }

  public:

    size_t count() const {
        return _nSegs == 0 ? 0 : _seg[_nSegs].pPosition();
    }

    bool isBool() const
    {
        return _isBoolean;
    }

    /**
     * Given the beginning byte address of a var-part datum in an RLE payload, get the size of the datum including header size.
     * @param[in]  address    the byte address of the var-part datum
     * @param[out] sizeHeader the size of the header, that stores the size of the actual datum; either 1 or 5
     * @param[out] sizeDatum  the size of the datum (not including the header)
     * @note If the size is less than 256, one byte is used to store the datum length.
     *       Otherwise, five bytes are used to store the length. In particular, the first byte is 0, and the next four bytes stores the length.
     */
    inline static void getSizeOfVarPartForOneDatum(char* const address, size_t& sizeHeader, size_t& sizeDatum);

    /**
     * Given an offset into the var part (the value that is stored in the fixed part), tell how many bytes the var part of the datum has.
     * @pre Must be var-size type.
     * @pre The offset must be within the range of the var part of the payload.
     *
     * @param[in] offset    the offset of the data in the var part
     * @return    #bytes of the var-part of the datum
     */
    inline size_t getSizeOfVarPartForOneDatum(size_t offset);

    /**
     * Given an existing varPart of some RLEPayload, append a var-size value to the end of it.
     * @param[inout] varPart  an existing var part
     * @param[in]    datumInRLEPayload a var-type value, from another RLEPayload, to be appended
     * @note varPart will be resized to include both the (1-or-5-byte) header and the actual value
     */
    inline static void appendValueToTheEndOfVarPart(std::vector<char>& varPart, char* const datumInRLEPayload);

    /**
     * Given an existing varPart of some RLEPayload, append a var-size value to the end of it.
     * @param[inout] varPart  an existing var part
     * @param[in]    value    a new value to be appended
     * @pre the value must be var-size type
     * @note varPart will be resized to include both the (1-or-5-byte) header and the actual value
     */
    inline static void appendValueToTheEndOfVarPart(std::vector<char>& varPart, Value const& value);

    /**
     * Get value data by the given index
     * @param placeholder for extracted value
     * @param index of value obtained through Segment::valueIndex
     */
    void getValueByIndex(Value& value, size_t index) const;

    /**
     * Get pointer to raw value data for the given position.
     * @param placeholder for exracted value
     * @param pos element position
     * @return true if values exists in payload, false otherwise
     */
    bool getValueByPosition(Value& value, position_t pos) const;

    /**
     * Return pointer for raw data for non-nullable types
     */
    char* getRawValue(size_t index) const {
        return _payload + index*(_elemSize == 0 ? sizeof(varpart_offset_t) : _elemSize);
    }

    /**
     * Return pointer for raw data of variable size types
     */
    char* getRawVarValue(size_t index, size_t& size) const;

    /**
     * Get number of RLE segments
     */
    size_t nSegments() const {
        return _nSegs;
    }

    /**
     * Get element size (0 for varying size types)
     */
    size_t elementSize() const {
        return _elemSize;
    }

    /**
     * Get payload size in bytes
     */
    size_t payloadSize() const {
        return _dataSize;
    }

    /**
     * Get number of items in payload
     */
    size_t payloadCount() const {
        return _dataSize / (_elemSize == 0 ? sizeof(varpart_offset_t) : _elemSize);
    }

    /**
     * Get next i-th segment
     */
    Segment const& getSegment(size_t i) const {
        assert(i <= _nSegs); // allow _nSegs, used to cut new Tile's
        assert(_seg);
        return _seg[i];
    }

    /**
     * Get next i-th segment and also return its length.
     *
     * Faster than two method calls when both are needed.
     * @see getSegLength
     */
    Segment const& getSegment(size_t i, size_t& length) const {
        assert(i < _nSegs);     // _nSegs disallowed: its length cannot be known
        assert(_seg);
        length = _seg[i+1].pPosition() - _seg[i].pPosition();
        return _seg[i];
    }

    /**
     * Get length of i-th segment.
     *
     * @note Only this container, and not the segment itself, can know the length of a segment:
     * it depends on values in the "next" segment, and this container ensures that a next
     * segment exists.  Hence rle::Segment does not itself have a length() method.
     */
    size_t getSegLength(size_t i) const {
        assert(i < _nSegs);
        assert(_seg);
        return _seg[i+1].pPosition() - _seg[i].pPosition();
    }

    /**
     * Find segment containing elements with position greater or equal than specified
     */
    size_t findSegment(position_t pos) const {
        size_t l = 0, r = _nSegs;
        while (l < r) {
            size_t m = (l + r) / 2;
            position_t mpos =_seg[m+1].pPosition();
            if (mpos == pos) {
                return (m+1);
            } else if (mpos < pos) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return r;
    }

    /**
     * Method to be called to save payload in chunk body
     */
    void pack(char* dst) const;

    /**
     * Get size needed to pack payload (used to determine size of chunk)
     */
    size_t packedSize() const
    {
        return sizeof(Header) + (_nSegs+1)*sizeof(Segment) + _dataSize;
    }

    /**
     * Constructor for initializing payload with raw chunk data
     */
    ConstRLEPayload(char const* src);

    void getCoordinates(ArrayDesc const& array,
                        size_t dim,
                        Coordinates const& chunkPos,
                        Coordinates const& tilePos,
                        Value& dst,
                        bool withOverlap) const;

    bool checkBit(size_t bit) const {
        return (_payload[bit >> 3] & (1 << (bit & 7))) != 0;
    }

    char* getFixData() const {
        return _payload;
    }

    char* getVarData() const {
        return _payload + _varOffs;
    }

    virtual ~ConstRLEPayload()
    {}

    class iterator
    {
        ConstRLEPayload const* _referent;
        size_t _currSeg;
        Segment const* _cs;
        position_t _currPpos;

    public:
        //defined in .cpp because of value constructor
        iterator(ConstRLEPayload const* payload);
        iterator() : _referent(NULL), _currSeg(0), _cs(0), _currPpos(-1) {}

        size_t getCurrSeg()
        {
            return _currSeg;
        }

        void restart()
        {
            _currSeg = 0;
            if(!end())
            {
                _cs = &_referent->getSegment(_currSeg);
                _currPpos = _cs->pPosition();
            }
        }

        bool end() const
        {
            return _currSeg >= _referent->nSegments();
        }

        unsigned getMissingReason() const
        {
            assert(!end());
            return _cs->valueIndex();
        }

        bool isNull() const
        {
            assert(!end());
            return _cs->null();
        }

        bool isSame() const
        {
            assert(!end());
            return _cs->same();
        }

        position_t const& getPPos() const
        {
            assert(!end());
            return _currPpos;
        }

        size_t getValueIndex() const
        {
            assert(!end());
            return (_cs->same() || _cs->null())
                ? _cs->valueIndex()
                : _cs->valueIndex() + _currPpos - _cs->pPosition();
        }

        uint64_t getSegLength() const
        {
            assert(!end());
            return _cs[1].pPosition() - _cs->pPosition();
        }

        uint64_t getRepeatCount() const
        {
            assert(!end());
            return _cs->same() ? getSegLength() - _currPpos + _cs->pPosition() : 1;
        }

        uint64_t available() const
        {
            assert(!end());
            return getSegLength() - _currPpos + _cs->pPosition();
        }

       bool checkBit() const
        {
            assert(_referent->_isBoolean);
            return _referent->checkBit(_cs->valueIndex() + (_cs->same() ? 0 : _currPpos - _cs->pPosition()));
        }

        void toNextSegment()
        {
            assert(!end());
            _currSeg ++;
            if (!end())
            {
                _cs = &_referent->getSegment(_currSeg);
                _currPpos = _cs->pPosition();
            }
        }

        char* getRawValue(size_t& valSize)
        {
            size_t index = _cs->same() ? _cs->valueIndex() : _cs->valueIndex() + _currPpos - _cs->pPosition();
            return _referent->getRawVarValue(index, valSize);
        }

        char* getFixedValues()
        {
            size_t index = _cs->same() ? _cs->valueIndex() : _cs->valueIndex() + _currPpos - _cs->pPosition();
            return _referent->_payload + index*_referent->_elemSize;
        }

        bool isDefaultValue(Value const& defaultValue);

        //defined in .cpp because of value methods
        void getItem(Value &item);

        void operator ++()
        {
            assert(!end());
            if (_currPpos + 1 < position_t(_cs->pPosition() + getSegLength()))
            {
                _currPpos ++;
            }
            else
            {
                _currSeg ++;
                if(!end())
                {
                    _cs = &_referent->getSegment(_currSeg);
                    _currPpos = _cs->pPosition();
                }
            }
        }

        bool setPosition(position_t pPos)
        {
            _currSeg = _referent->findSegment(pPos);
            if (end())
            {
                return false;
            }

            assert (_referent->getSegment(_currSeg).pPosition() <= pPos);

            _cs = &_referent->getSegment(_currSeg);
            _currPpos = pPos;
            return true;
        }

        /**
         * Move ahead @c count physical positions in a tile.
         *
         * @param count number of physical positions to move ahead
         * @return if tile is a bitmap, count of 1 bits seen while moving, otherwise 0
         *
         * @description A data tile just stores values without knowledge
         * about physical positions inside the tile.  The corresponding
         * bitmap tile (that is, @c *this ) helps to understand where
         * each data value is actually positioned.  Hence if you call
         *
         *      dataReader += bitmapReader.skip(physicalPositionsCount);
         *
         * you will have consistent positioning in both dataReader and
         * bitmapReader.
         *
         * @note The return value of this method is ONLY useful if
         * called on a bitmap tile iterator, that is, on iterators for
         * boolean ConstRLEPayloads.
         */
        uint64_t skip(uint64_t count)
        {
            uint64_t setBits = 0;
            bool countBits = _referent->_isBoolean;
            while (!end()) {
                if (_currPpos + count >= _cs->pPosition() + getSegLength()) {
                    uint64_t tail = getSegLength() - _currPpos + _cs->pPosition();
                    count -= tail;
                    if (countBits) {
                        if (_cs->same())  {
                            setBits += _referent->checkBit(_cs->valueIndex()) ? tail : 0;
                        }  else {
                            position_t beg = _cs->valueIndex() + _currPpos - _cs->pPosition();
                            position_t end = _cs->valueIndex() + getSegLength();
                            while (beg < end) {
                                setBits += _referent->checkBit(beg++);
                            }
                        }
                    }
                    toNextSegment();
                } else {
                    if (countBits) {
                        if (_cs->same())  {
                            setBits += _referent->checkBit(_cs->valueIndex()) ? count : 0;
                        } else {
                            position_t beg = _cs->valueIndex() + _currPpos - _cs->pPosition();
                            position_t end = beg + count;
                            while (beg < end) {
                                setBits += _referent->checkBit(beg++);
                            }
                        }
                    }
                    _currPpos += count;
                    break;
                }
            }
            return setBits;
        }

        void operator +=(uint64_t count)
        {
            assert(!end());
            _currPpos += count;
            if (_currPpos - _cs->pPosition() >= static_cast<position_t>(getSegLength())) {
                if (++_currSeg < _referent->nSegments()) {
                    _cs = &_referent->getSegment(_currSeg);
                    if (_currPpos - _cs->pPosition() < static_cast<position_t>(getSegLength())) {
                        return;
                    }
                }
                setPosition(_currPpos);
            }
        }
    };

    /**
     * A structure that contains more complete info of a virtual segment, than Segment does.
     */
    class SegmentWithLength: public Segment
    {
    public:
        position_t _length;         // the length of this segment
    };

    /**
     * An iterator that iterates through the segments, rather than the individual non-empty cells.
     * It is a wrapper over the 'iterator' class.
     */
    class SegmentIterator
    {
    private:
        ConstRLEPayload::iterator _it;

    public:
        /**
         * Constructor.
         */
        SegmentIterator(ConstRLEPayload const* payload): _it(payload)
        {
            restart();
        }

        /**
         * Get the segment number of the current segment.
         */
        size_t getCurrSeg()
        {
            return _it.getCurrSeg();
        }

        /**
         * Move to the beginning of the first segment.
         */
        void restart()
        {
            _it.restart();
        }

        /**
         * Whether there is no more segment.
         */
        bool end() const
        {
            return _it.end();
        }

        /**
         * Get information for SegmentWithLength of the current virtual
         * segment (could be in the middle of an actual segment).
         *
         * @param[out] ret     the segment to be returned
         */
        void getVirtualSegment(ConstRLEPayload::SegmentWithLength& ret) const
        {
            assert(!end());

            ret.setPPosition(_it.getPPos());
            ret._length = _it.available();
            ret.setSame(_it.isSame());
            ret.setNull(_it.isNull());
            ret.setValueIndex(_it.getValueIndex());
        }

        /**
         * Advance the position, within the same segment.
         * @param stepSize  how many positions to skip
         * @pre stepSize must be LESS than the remaining length of the
         *      virtual segment; to advance to the end of the segment you
         *      should use operator++()
         */
        void advanceWithinSegment(uint64_t stepSize)
        {
            assert(!end());
            assert(_it.available() > stepSize);

            _it.skip(stepSize);
        }

        /**
         * Advance to the next segment.
         */
        void operator++()
        {
            assert(!end());
            _it.toNextSegment();
        }

        /**
         * Advance either to the beginning of the next segment or to a position within the same segment.
         */
        void advanceBy(uint64_t stepSize)
        {
            assert(!end());
            assert(stepSize <= _it.available());
            if (stepSize == _it.available()) {
                _it.toNextSegment();
            } else {
                advanceWithinSegment(stepSize);
            }
        }
    };

    iterator getIterator() const
    {
        return iterator(this);
    }
};

std::ostream& operator<<(std::ostream& stream, ConstRLEPayload const& payload);

/**
 * A class for building a ConstRLEPayload object.
 *
 * @note
 * Some remarks on the implementation...
 *
 * - For a variable-size type, until setVarPart() is called, _dataSize
 *   is the byte size of the fixed-size part (and _payload only
 *   contains the fixed-size part).  In the meantime, variable-sized
 *   values accumulate in an external vector.  See appendValue() .
 *
 * - After setVarPart() is called (passing in the accumulated
 *   variable-sized values), _dataSize is the total byte size of the
 *   fixed-size part and the variable-size part (and the var part is
 *   copied into _payload).
 *
 * - For a fixed-size type, there is no var part. So _dataSize is both
 *   the size of the fixed-size part and the size of all the data.
 *
 * - For a boolean type, _dataSize is _valuesCount divided by NBBY
 *   (bits per byte, i.e. 8) -- just enough to hold all the boolean
 *   values.
 *
 * - Each call to appendValue() takes as input a vector of bytes (as the
 *   var part of the data), to receive the var part of the new value.
 *
 * - When constructing an RLEPayload, don't forget the last segment
 *   (which helps tell the length of the previous segment). You may
 *   call flush() to accomplish this.
 *
 * @note
 * Donghui believes the class should be rewritten in several ways:
 *
 * - It is better to store the variable part of the data inside the
 *   RLEPayload object, not somewhere else.
 *
 * - getValuesCount() returns _dataSize/elementSize, which is simply
 *   wrong after setVarPart() is called.
 *
 * - It is not clear when _valuesCount can be trusted. Seems that while
 *   data are being appended, _valuesCount is trustworthy only if the
 *   data type is boolean.  [DJG: While this is true, you never access
 *   _valuesCount directly.  The getValuesCount accessor returns the
 *   correct number of elements regardless of whether or not
 *   _isBoolean is true.  _valuesCount is an optimization for the
 *   _isBoolean = true case only.]
 *
 * - addBoolValues() updates _dataSize and _valuesCount, but
 *   addRawValues() and addRawVarValues() do NOT update any of
 *   them. This is very inconsistent.
 */
class RLEPayload : public ConstRLEPayload
{
  private:
    std::vector<Segment> _container;
    std::vector<char> _data;
    uint64_t _valuesCount;      // Used only for _isBoolean case.

  public:

    /**
     * Append a single Value to either the fixed-size part of the array, or to @c varPart.
     *
     * @param[in,out] varPart   append val here iff payload is for variable-length attributes
     * @param[in] val           value to append (or if attribute is Boolean, whether to set/clear)
     * @param[in] valueIndex    for Boolean attributes only, set or clear this bit position
     */
    void appendValue(std::vector<char>& varPart, Value const& val, size_t valueIndex);

    /**
     * Append a (partial) segment of values from another RLEPayload.
     *
     * @param[in]    dstSegmentToAppend   a segment to be appended
     * @param[in,out] varPart             the variable-size part of the data
     * @param[in]    srcPayload           the source ConstRLEPayload to copy data from
     * @param[in]    valueIndexInSrc      the valueIndex in the src, corresponding to the first value to append
     * @param[in]    realLength           the number of values to append; 1 if dstSegmentToAppend._same==true;
     *                                    0 if dstSegmentToAppend._null==true
     *
     * @note _valuesCount needs to be accurate before and after the call.
     * @note _dataSize needs to be the byte size of the fixed data before and after the call.
     * @note It is the caller's responsibility to call setVarPart() and flush() at the end.
     */
    void appendAPartialSegmentOfValues(Segment const& dstSegmentToAppend, std::vector<char>& varPart,
            ConstRLEPayload& srcPayload, uint32_t valueIndexInSrc, position_t realLength);

    void setVarPart(char const* data, size_t size);
    void setVarPart(std::vector<char>& varPart);

    /** Append the contents of another RLEPayload object. */
    void append(const RLEPayload& payload);

    /**
     * Add raw fixed data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addRawValues(size_t n = 1) {
        assert(_elemSize != 0);
        const size_t ret = _dataSize / _elemSize;
        _data.resize(_dataSize += _elemSize * n);
        _payload = &_data[0];
        return ret;
    }

    /**
     * Add raw var data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     * @note We are resizing the fixed-size portion of the payload only.
     */
    size_t addRawVarValues(size_t n = 1) {
        assert(_elemSize == 0);
        const size_t fixedSize = sizeof(varpart_offset_t);
        const size_t ret = _dataSize / fixedSize;
        _data.resize(_dataSize += fixedSize * n);
        _payload = &_data[0];
        return ret;
    }

    /**
     * Add raw bool data for non-nullable types
     * @param n a number of new items
     * @return index of the first new item
     */
    size_t addBoolValues(size_t n = 1) {
        assert(_elemSize == 1 && _isBoolean);
        size_t ret = _valuesCount;
        _valuesCount += n;
        _dataSize = (_valuesCount >> 3) + 1;
        _data.resize(_dataSize);
        _payload = &_data[0];
        return ret;
    }

    /**
     * @return number of elements
     */
    size_t getValuesCount() const  {
        if (_isBoolean)
            return _valuesCount;
        const size_t fixedSize = _elemSize == 0 ? sizeof(varpart_offset_t) : _elemSize;
        return _dataSize / fixedSize;
    }

    /**
     * Add new segment
     */
    void addSegment(const Segment& segment) {
        assert(_container.empty() || _container[_container.size() - 1].pPosition() < segment.pPosition());
        _container.push_back(segment);
        _seg = &_container[0];
        _nSegs = _container.size() - 1;
    }

    /**
     * Assign segments pointer from other payload.
     * Sometimes it's safe to just copy pointer but for conversion
     * constant inplace it's impossible for example.
     * That's why copy param is true by default.
     */
    void assignSegments(const ConstRLEPayload& payload, bool copy = true)
    {
        if (copy) {
            _nSegs = payload.nSegments();
            _container.resize(_nSegs + 1);
            memcpy(&_container[0], payload._seg, (_nSegs + 1) * sizeof(Segment));
            _seg = &_container[0];
        } else {
            // XXX This code looks suspect to me: what about
            // _container[] ???  I don't believe that "it's safe to
            // just copy the pointer".  Fortunately it doesn't seem to
            // be getting called anywhere at the moment.  -mjl
            SCIDB_UNREACHABLE();
            _seg = payload._seg;
            _nSegs = payload._nSegs;
        }
    }

    /**
     * Assignment operator: deep copy from const payload into non-const
     */
    RLEPayload& operator=(ConstRLEPayload const& other)
    {
        if (dynamic_cast<ConstRLEPayload*>(this) != &other) {
            _nSegs = other.nSegments();
            _elemSize = other._elemSize;
            _dataSize = other._dataSize;
            _varOffs = other._varOffs;
            _isBoolean = other._isBoolean;
            _container.resize(_nSegs+1);
            memcpy(&_container[0], other._seg, (_nSegs+1)*sizeof(Segment));
            _seg = &_container[0];
            _data.resize(_dataSize);
            memcpy(&_data[0], other._payload, _dataSize);
            _payload = &_data[0];
        }
        return *this;
    }

    RLEPayload(ConstRLEPayload const& other):
        ConstRLEPayload()
    {
        *this = other;
    }

    RLEPayload& operator=(RLEPayload const& other)
    {
        if (this != &other) {
            _nSegs = other._nSegs;
            _elemSize = other._elemSize;
            _dataSize = other._dataSize;
            _varOffs = other._varOffs;
            _isBoolean = other._isBoolean;
            _container = other._container;
            _seg = &_container[0];
            _data = other._data;
            _payload = &_data[0];
            _valuesCount = other._valuesCount;
        }
        return *this;
    }

    /**
     * Copy constructor: deep copy
     */
    RLEPayload(RLEPayload const& other):
        ConstRLEPayload()
    {
        *this = other;
    }

    RLEPayload();

    /**
     * Constructor of bitmap from ValueMap (which is used to be filled by ChunkIterator)
     * @param vm ValueMap of inserted {position,value} pairs
     * @param nElems number of elements present in the chunk
     * @param elemSize fixed size of element (in bytes), 0 for varying size types
     * @param defaultValue default value used to fill holes (elements not specified in ValueMap)
     * @param subsequent all elements in ValueMap are assumed to be subsequent
     */
    RLEPayload(ValueMap const& vm, size_t nElems, size_t elemSize,
               Value const& defaultVal, bool isBoolean, bool subsequent);

    /**
     * Constructor which is used to fill a non-emptyable RLE chunk with default values.
     * @param[in]  defaultVal  the default value of the attribute
     * @param[in]  logicalSize the number of logical cells in the chunk
     * @param[in]  elemSize    fixed size of element (in bytes), 0 for varying size types
     * @param[in]  isBoolean   whether the element is of boolean type
     */
    RLEPayload(Value const& defaultVal, size_t logicalSize, size_t elemSize, bool isBoolean);

    /**
     * Constructor of RLE bitmap from dense non-nullable data
     */
    RLEPayload(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);
    void unpackRawData(char* rawData, size_t rawSize, size_t varOffs, size_t elemSize, size_t nElems, bool isBoolean);

    RLEPayload(const class Type& type);

    RLEPayload(size_t bitSize);

    //
    // Yet another appender: correct handling of boolean and varying size types
    //
    class append_iterator : boost::noncopyable
    {
        RLEPayload* _result;
        std::vector<char> _varPart;
        RLEPayload::Segment _segm;
        Value _prevVal;
        size_t _valueIndex;
        size_t _segLength;

      public:
        RLEPayload* getPayload() {
            return _result;
        }

        explicit append_iterator(RLEPayload* dstPayload);
        void flush();
        void add(Value const& v, uint64_t count = 1);

        /**
         * @name Compatibility aliases.
         * Backward compatibility methods to make replacing the obsolete
         * RLEPayloadAppender class easier.
         */
        /**@{*/
        void finalize() { flush(); }
        void append(Value const& v, uint64_t count = 1) { add(v,count); }
        /**@}*/

        /**
         * add not more than @param limit values from @param inputIterator
         * Flag @param setupPrevVal just a workaround for bug with mixed
         * add(iterator&, limit) and add(Value const&) calls - after that payload
         * can be broken.
         * I (Oleg) did not fix bug directly according to potential performance regression
         *
         * @return count of added value from @param inputIterator
         * please note - this method add just single segment from @param inputIterator.
         * if your @param limit can be larger than segmentLength from @param inputIterator
         * you should compare @return and @param limit, and repeat call with
         * (@param limit = @param limit - @return)
         * (if @param inputIterator still has values, of course).
         */
        uint64_t add(iterator& inputIterator, uint64_t limit, bool setupPrevVal = false);
    };

    /**
     * Clear all data
     */
    void clear();

    /**
     * Use this method to copy payload data according to an empty bitmask and start and stop
     * positions.
     * @param [in] payload an input payload
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEPayload& payload,
                    const ConstRLEEmptyBitmap& emptyMap,
                    position_t vStart,
                    position_t vEnd);

    /**
     * Use this method to copy empty bitmask to payload
     * positions.
     * @param [in] emptyMap an input empty bitmap mask according to which data should be extracted
     * @param [in] vStart a logical position of start from data should be copied
     * @param [in] vEnd a logical position of stop where data should be copied
     */
    void unPackTile(const ConstRLEEmptyBitmap& emptyMap, position_t vStart, position_t vEnd);

     /**
     * Complete adding segments to the chunk
     */
    void flush(position_t chunkSize);

    void trim(position_t lastPos);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _nSegs;
        ar & _elemSize;
        ar & _dataSize;
        ar & _varOffs;
        ar & _container;
        ar & _data;
        ar & _isBoolean;
        if (Archive::is_loading::value) {
            _seg = &_container[0];
            _payload = &_data[0];
        }
    }
};

}

#endif
