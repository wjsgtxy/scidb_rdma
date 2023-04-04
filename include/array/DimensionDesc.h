#ifndef DIMENSIONDESC_H_
#define DIMENSIONDESC_H_
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
#include <system/Exceptions.h>  // For ASSERT_EXCEPTION
#include <system/Utils.h>       // For SCIDB_ASSERT

#include <array/ObjectNames.h>

#include <array/Coordinate.h>

#include <boost/operators.hpp>
#include <boost/serialization/base_object.hpp>

#include <string>

namespace scidb {

class ArrayDesc;

/**
 * Descriptor of dimension
 */
class DimensionDesc final  // value class; no more subclassing
    : public ObjectNames
    , private boost::equality_comparable<DimensionDesc>
{
public:
    /**
     * Special (non-positive) chunk interval values.
     */
    enum SpecialIntervals {
        UNINITIALIZED = 0,      ///< chunkInterval value not yet initialized.
        AUTOCHUNKED   = -1,     ///< chunkInterval will (eventually) be automatically computed.
        PASSTHRU      = -2,     ///< redimension uses chunkInterval from another corresponding input dimension
        LAST_SPECIAL  = -3,     ///< All enums must be greater than this. This MUST be the last.
    };

    /**
     * Construct empty dimension descriptor (for receiving metadata)
     */
    DimensionDesc();

    ~DimensionDesc() = default;

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     *
     * @param baseName name of dimension derived from catalog
     * @param names dimension names and/ aliases collected during query compilation
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param startMin dimension minimum start
     * @param currSart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param baseName dimension name derived from catalog
     * @param name dimension names and/ aliases collected during query compilation
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currEnd dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    bool operator == (DimensionDesc const&) const;

    /**
     * @brief replace the values, excluding the name, with those from another descriptor
     */
    void replaceValues(DimensionDesc const &other)
    {
        setStartMin(other.getStartMin());
        setEndMax(other.getEndMax());

        setRawChunkInterval(other.getRawChunkInterval());
        setChunkOverlap(other.getChunkOverlap());

        setCurrStart(other.getCurrStart());
        setCurrEnd(other.getCurrEnd());
    }

    /**
     * @return minimum start coordinate.
     * @note This is reliable. The value is independent of the data in the array.
     */
    Coordinate getStartMin() const
    {
        return _startMin;
    }

    /**
     * @return current start coordinate.
     * @note In an array with no data, getCurrStart()=CoordinateBounds::getMax() and getCurrEnd()=CoordinateBounds::getMin().
     * @note This is NOT reliable.
     *       The only time the value can be trusted is right after the array schema is generated by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinate getCurrStart() const
    {
        return _currStart;
    }

    /**
     * @return current end coordinate.
     * @note This is NOT reliable. @see getCurrStart().
     */
    Coordinate getCurrEnd() const
    {
        return _currEnd;
    }

    /**
     * @return maximum end coordinate.
     * @note This is reliable. @see getStartMin().
     */
    Coordinate getEndMax() const
    {
        return _endMax;
    }

    /**
     * @return dimension length
     * @note This is reliable. @see getStartMin().
     */
    uint64_t getLength() const
    {
        return _endMax - _startMin + 1;
    }

    /**
     * @brief Determine if the max value matches '*'
     * @return true={max=='*'}, false otherwise
     */
    bool isMaxStar() const
    {
        return CoordinateBounds::isMaxStar(getEndMax());
    }

    /**
     * @return current dimension length.
     * @note This is NOT reliable. @see getCurrStart().
     * @note This may read from the system catalog.
     */
    uint64_t getCurrLength() const;

    /**
     * Get the chunk interval (during query execution)
     *
     * @return the chunk interval in this dimension, not including overlap.
     * @throws ASSERT_EXCEPTION if interval is unspecified (aka autochunked)
     * @see getRawChunkInterval
     * @see https://trac.scidb.net/wiki/Development/components/Rearrange_Ops/RedimWithAutoChunkingFS
     *
     * @description
     * Ordinarily #getChunkInterval() would be a simple accessor, but due to autochunking, chunk
     * intervals may not be knowable until actual query execution.  Specifically, operators must be
     * prepared to handle unknown chunk intervals right up until the inputArrays[] are received by
     * their PhysicalFoo::execute() method.  The input arrays are guaranteed to have fully resolved
     * chunk intervals, so it is safe to #getChunkInterval() in execute() methods and in the Array
     * subclass objects that they build.
     *
     * @p At all other times (parsing, schema inference, optimization, maybe elsewhere), code most
     * be prepared to Do The Right Thing(tm) when it encounters a DimensionDesc::AUTOCHUNKED chunk
     * interval.  Specifically, code must either call #getRawChunkInterval() and take appropriate
     * action if the return value is AUTOCHUNKED, or else guard calls to #getChunkInterval() by
     * first testing the #DimensionDesc::isAutochunked() predicate.  What a particular operator does
     * for autochunked dimensions depends on the operator.  Many operators merely need to propragate
     * the autochunked interval up the inferSchema() tree using getRawChunkInterval(), but other
     * code may require more thought.
     *
     * @p There was no #getRawChunkInterval() before autochunking, so by placing an ASSERT_EXCEPTION
     * in this "legacy" #getChunkInterval() method we intend to catch all the places in the code
     * that need to change (hopefully during development rather than during QA).
     *
     * @note Only redimension() and repart() may have autochunked *schema parameters*, all other
     *       operators must disallow that---either by using #getRawChunkInterval() or by testing
     *       with isAutochunked(), and possibly throwing SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED.
     *       Autochunked input schemas (as opposed to parameter schemas) are permitted everywhere in
     *       the logical plan, and their intervals should be propagated up the query tree with
     *       #getRawChunkInterval().  If an operator has particular constraints on input chunk
     *       intervals (as gemm() and svd() do), it should @em try to make the checks at logical
     *       inferSchema() time, but if it cannot because one or more intervals are unspecified, it
     *       @b must do the checks at physical execute() time, when all unknown autochunked
     *       intervals will have been resolved.  During query execution it's safe to call
     *       #getChunkInterval() without any checking.
     */
    int64_t getChunkInterval() const
    {
        ASSERT_EXCEPTION(isIntervalResolved(), "Caller not yet modified for autochunking.");
        return _chunkInterval;
    }

    /**
     * Get the possibly-not-yet-specified chunk interval (during query planning)
     *
     * @return the raw chunk interval in this dimension (not including overlap), or AUTOCHUNKED.
     * @see getChunkInterval
     * @note Callers that are autochunk-aware should call this version rather
     *       than getChunkInterval().  Other callers would presumably not be
     *       prepared for a return value of AUTOCHUNKED.
     */
    int64_t getRawChunkInterval() const
    {
        return _chunkInterval;
    }

    /**
     * @return the chunk interval in this dimension, or useMe if the interval is autochunked.
     */
    int64_t getChunkIntervalIfAutoUse(int64_t useMe) const
    {
        return _chunkInterval == AUTOCHUNKED ? useMe : _chunkInterval;
    }

    /**
     * @return true iff interval in this dimension is autochunked.
     */
    bool isAutochunked() const
    {
        return _chunkInterval == AUTOCHUNKED;
    }

    bool isIntervalResolved() const
    {
        SCIDB_ASSERT(_chunkInterval > LAST_SPECIAL);
        // UNINITIALIZED counts as "resolved" because there is no other value
        // that is going to 'magically' replace it, whereas, these two will be
        // replaced at execution time.
        return _chunkInterval != AUTOCHUNKED && _chunkInterval != PASSTHRU;
    }

    /**
     * @return chunk overlap in this dimension.
     * @note Given base coordinate Xi, a chunk stores data with coordinates in
     *       [Xi-getChunkOverlap(), Xi+getChunkInterval()+getChunkOverlap()].
     */
    int64_t getChunkOverlap() const
    {
        return _chunkOverlap;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] os output stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostream&, int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<ObjectNames>(*this);
        ar & _startMin;
        ar & _currStart;
        ar & _currEnd;
        ar & _endMax;
        ar & _chunkInterval;
        ar & _chunkOverlap;
    }

    void setCurrStart(Coordinate currStart)
    {
        _currStart = currStart;
    }

    void setCurrEnd(Coordinate currEnd)
    {
        _currEnd = currEnd;
    }

    void setStartMin(Coordinate startMin)
    {
        _startMin = startMin;
    }

    void setEndMax(Coordinate endMax)
    {
        _endMax = endMax;
    }

    void setChunkInterval(int64_t i)
    {
        assert(i > 0);
        _chunkInterval = i;
    }

    void setRawChunkInterval(int64_t i)
    {
        assert(i > 0 || i == AUTOCHUNKED || i == PASSTHRU);
        _chunkInterval = i;
    }

    void setChunkOverlap(int64_t i)
    {
        assert(i >= 0);
        _chunkOverlap = i;
    }

    // @return true if the target is a reserved dimension name.
    static bool isReservedName(const std::string& target);

private:
    void validate() const;

private:
    friend class ArrayDesc;

    Coordinate _startMin;
    Coordinate _currStart;

    Coordinate _currEnd;
    Coordinate _endMax;

    /**
     * The length of the chunk along this dimension, excluding overlap.
     *
     * Chunk Interval is often used as part of coordinate math and coordinates are signed int64. To
     * make life easier for everyone, chunk interval is also signed for the moment. Same with
     * position_t in RLE.h.
     */
    int64_t _chunkInterval;

    /**
     * The length of just the chunk overlap along this dimension.
     * Signed to make coordinate math easier.
     */
    int64_t _chunkOverlap;

    ArrayDesc* _array;
};

void printSchema(std::ostream&,const DimensionDesc&,bool verbose=false);
std::ostream& operator<<(std::ostream&,const DimensionDesc&);


}  // namespace scidb
#endif
