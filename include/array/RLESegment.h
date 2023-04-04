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

#ifndef RLE_SEGMENT_H
#define RLE_SEGMENT_H

#include <query/Value.h>
#include <system/UserException.h>
#include <util/Utility.h>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/split_member.hpp>
#include <inttypes.h>

/**
 *  @file RLESegment.h
 *
 *  @brief The One True Segment Definition for RLE encodings.
 *
 *  @description Largely based on Tigor's Tile.h implementation.  The
 *  main purpose here is to reduce the repeated definition, over and
 *  over, of what an RLE segment is.  Please!!?
 *
 *  Also, we need to catch overflows of dataIndex (30 bits), and this
 *  is only practical with a fully encapsulated class object instead
 *  of a plain old data struct.  (Ticket #4603)
 *
 *  Boost serialization support added, used by ConstRLEPayload.
 *
 *  Now that this definition is shared, it needs to be in its own file
 *  (rather than Tile.h) to avoid circular #include dependencies.
 */

namespace scidb { namespace rle {

/**
 * Common representation of an RLE payload segment (*not* a bitmap segment!).
 *
 * An ordered (by _startPosition) list of segments in conjunction with a list of data elements ("payload")
 * is used to represent RLE encoded data.
 * The _dataIndex field points to the first data element inside the payload, which is the first value of the segment.
 * Each segment can either be a "run" or a "literal".
 * In a run segment all values are the same and are represented by a single payload value @ payload [ _dataIndex ].
 * In a literal segment the values may or may not be the same and each value in the segment
 * is represented by a separate value in the payload.
 * In the literal segment Si, the first value is payload [ Si._dataIndex ],
 * the last value is payload [ Si+1._startPosition - Si._startPosition - 1 ].
 * The terminating segment exists strictly for computing the length of the last data segment as in
 * Sterm._startPosition - Slast._startPosition.
 * To accomodate SciDB NULLs with missing codes, there are NULL segments.
 * They dont index into the payload and cannot be literals.
 * They represent runs (possibly of length 1) of NULLs with missing codes recorded in the _dataIndex field.
 */
class Segment {

private:

    // Data members have platform independent data types because Segment objects are implicitly part
    // of the 'opaque' file format.  The static assertion in the default constructor guards against
    // accidental incompatibility.

    uint64_t _startPosition;          // physical position of first cell in segment
    struct bits {
        uint32_t     _dataIndex : 30; // index of element into payload (but see _isNull)
        uint32_t     _isRun : 1;      // true iff segment is run of same value
        uint32_t     _isNull : 1;     // if true, _dataIndex is missingReason code
    } __attribute__ ((packed));

    union {
        struct bits _bits;
        uint32_t _allBits;
    };

public:

    static const uint32_t MAX_DATA_INDEX = 0x3FFFFFFF; // 30bits

    Segment ()
        : _startPosition(0), _allBits(0)
    {
        static_assert(sizeof(Segment) == 12, "Opaque file format is broken: rle::Segment");
        assert(sizeof(_bits) <= sizeof(_allBits));
    }

    Segment ( const uint64_t position, const uint32_t dataIndex,
              const bool isRun, const bool isNull )
        : _startPosition(position)
    {
        if (dataIndex > MAX_DATA_INDEX) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_TOO_POPULATED)
                << dataIndex << MAX_DATA_INDEX;
        }
        _bits._dataIndex = dataIndex & MAX_DATA_INDEX;
        _bits._isRun = isRun;
        _bits._isNull = isNull;
    }
    bool isLiteral() const
    {
        return !_bits._isRun;
    }
    bool isRun() const
    {
        return _bits._isRun;
    }
    bool isNull() const
    {
        return _bits._isNull;
    }
    int8_t getMissingCode() const
    {
        assert(isRun());
        assert(isNull());
        return static_cast<int8_t>(_bits._dataIndex & 0xFF);
    }
    uint32_t getDataIndex() const
    {
        return _bits._dataIndex;
    }
    uint64_t getStartPosition() const
    {
        return _startPosition;
    }
    void setStartPosition(uint64_t pos)
    {
        _startPosition = pos;
    }
    void setMissingCode(int32_t code)
    {
        // The missing code is not stored in payload _data but rather
        // directly in _bits._dataIndex.  Valid values must actually fit
        // in an int8_t... which is what this check is about.
        if (code < 0 || !Value::isValidMissingReason(code)) {
            // This should have been caught by now.
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_BAD_MISSING_REASON) << code;
        }
        setNull(true);
        setRun(true);
        setDataIndex(code);
    }
    void setDataIndex(uint32_t i)
    {
        if (i > MAX_DATA_INDEX) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_TOO_POPULATED)
                << i << MAX_DATA_INDEX;
        }
        // 'i' fits in _dataIndex per above, but compiler insists it be masked.
        _bits._dataIndex = i & MAX_DATA_INDEX;
    }
    void setRun(bool b)  { _bits._isRun = b; }
    void setNull(bool b) { _bits._isNull = b; }

    /**
     * Aliases for setters/getters used by other Segment
     * implementations being replaced by this one.
     * @{
     */
    // Getters
    position_t  pPosition() const { return getStartPosition(); }
    uint32_t    valueIndex() const { return getDataIndex(); }
    bool        same() const { return isRun(); }
    bool        null() const { return isNull(); }

    // Setters
    void    setPPosition(position_t pPos) { setStartPosition(pPos); }
    void    addToPPosition(position_t incr) { _startPosition += incr; }
    void    setSame(bool b) { setRun(b); }
    void    setValueIndex(size_t vi) { setDataIndex(safe_static_cast<uint32_t>(vi)); }
    void    addToValueIndex(size_t incr)
    {
        if (getDataIndex() + incr > MAX_DATA_INDEX) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_TOO_POPULATED)
                << (getDataIndex() + incr) << MAX_DATA_INDEX;
        }
        _bits._dataIndex = MAX_DATA_INDEX & static_cast<uint32_t>(getDataIndex() + incr);
    }
    /**@}*/

    template<class Archive>
    void save(Archive & ar, const unsigned int version) const
    {
        uint64_t startPos__ = _startPosition;
        uint32_t dataIndex__ = _bits._dataIndex;
        uint8_t same__ = _bits._isRun;
        uint8_t null__ = _bits._isNull;
        ar & startPos__;
        ar & dataIndex__;
        ar & same__;
        ar & null__;
    }

    template<class Archive>
    void load(Archive & ar, const unsigned int version)
    {
        uint64_t startPos__;
        uint32_t dataIndex__;
        uint8_t same__;
        uint8_t null__;
        ar & startPos__;
        ar & dataIndex__;
        ar & same__;
        ar & null__;
        _startPosition = startPos__;
        _bits._dataIndex = dataIndex__ & MAX_DATA_INDEX;
        _bits._isRun = static_cast<bool>(same__);
        _bits._isNull = static_cast<bool>(null__);
    }

    BOOST_SERIALIZATION_SPLIT_MEMBER()

} __attribute__((packed));


} } // namespaces

#endif /* ! RLE_SEGMENT_H */
