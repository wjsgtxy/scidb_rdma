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

/*
 * SpatialType.h
 *  This file defines some spatial types.
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */
#ifndef SPATIALTYPE_H_
#define SPATIALTYPE_H_

#include <cassert>
#include <memory>  // unique_ptr
#include <vector>

#include <array/RLE.h>

namespace scidb
{
/**
 * Point A dominates point B, if in all dimensions A has a larger or equal coordinate.
 * The dominance relationship is strict, if in at least one dimension the coordinates are not equal.
 *
 * Comparing two multi-dimensional points (with the same dimensionality), the following cases are possible:
 *   - They are exactly equal in all dimensions.
 *   - A strictly dominates B.
 *   - A is strictly dominated by B.
 *   - Neither of them dominates the other.
 */
enum DominanceRelationship
{
    EQUALS,
    STRICTLY_DOMINATES,
    IS_STRICTLY_DOMINATED_BY,
    HAS_NO_DOMINANCE_WITH
};

/**
 * Given two points, tell their dominance relationship.
 * @param left  one point.
 * @param right another point.
 * @return a DominanceRelationship value.
 * @assert the points must have the same dimensionality, which must be at least one.
 */
DominanceRelationship calculateDominance(Coordinates const& left, Coordinates const& right);

/**
 * @return whether left is dominated by right.
 * @left  a point.
 * @right another point.
 */
bool isDominatedBy(Coordinates const& left, Coordinates const& right);

/**
 * A spatial range is composed of a low Coordinates and a high Coordinates.
 */
class SpatialRange
{
public:
    /**
     * The low point.
     */
    Coordinates _low;

    /**
     * The high point.
     */
    Coordinates _high;

    /**
     * This version of the constructor only allocates space for _low and _high.
     * it is the caller's responsibility to assign values to the Coordinates.
     * @param numDims   the number of dimensions. Must be positive.
     */
    SpatialRange(size_t numDims = 0)
    : _low(numDims), _high(numDims)
    {
    }

    /**
     * This version of the constructor assigns the values to the low & high coordinates.
     * @param low  the low coordinates.
     * @param high the high coordinates.
     * @assert low must be dominated by high.
     */
    SpatialRange(Coordinates const& low, Coordinates const& high)
    : _low(low), _high(high)
    {
        assert(valid());
    }

    /**
     * A range is valid, if _low is dominated by _high.
     * @assert _low and _high have the same positive size.
     * @return whether the range is valid.
     */
    bool valid() const
    {
        return isDominatedBy(_low, _high);
    }

    /**
     * @return whether I intersect with the other range.
     * @param other  the other range.
     * @note Two ranges intersect, iff the low point of each range is dominated by the high point of the other range.
     * @assert: the other range and I must have the same dimensionality and must both be valid.
     */
    bool intersects(SpatialRange const& other) const
    {
        assert(valid() && other.valid() && _low.size()==other._low.size());
        return isDominatedBy(_low, other._high) && isDominatedBy(other._low, _high);
    }

    /**
     * @return whether I spatially contain a given point.
     * @param point  a point.
     */
    bool contains(Coordinates const& point) const
    {
        assert(valid() && _low.size()==point.size());
        return isDominatedBy(_low, point) && isDominatedBy(point, _high);
    }

    /**
     * @return whether I spatially contain a given range.
     * @note I fully contain a range, if I contain both its low point and its high point.
     * @param other    a range
     * @assert: the other ranges and I must have the same dimensionality and must both be valid.
     */
    bool contains(SpatialRange const& other) const
    {
        assert(valid() && other.valid() && _low.size()==other._low.size());
        return contains(other._low) && contains(other._high);
    }
};

/**
  * Output a human-readable string description of bounds onto stream
  * @param stream where to write
  * @param bounds the boundaries to record
  * @return the stream with the boundaries data appended to the end
  */
std::ostream& operator<<(std::ostream& stream, const SpatialRange& bounds);

/**
 * The class SpatialRanges is essentially a vector of SpatialRange objects,
 * with some additional capabilities.
 *
 * @note Must call buildIndex() before calling any of the findXXX functions.
 */
class SpatialRanges
{
public:
    /**
     * Every newly added SpatialRange object will have numDims dimensions.
     */
    explicit SpatialRanges(const size_t numDims);

    ~SpatialRanges();

    size_t numDims() const;

    std::vector<SpatialRange> const& ranges() const;

    /**
     * Append a new SpatialRange object to the end of the vector.
     */
    void insert(SpatialRange&&);

    /**
     * @brief Caller must call this before any of the findXXX() functions may be called.
     */
    void buildIndex();

    /**
     * @return whether at least one stored range intersects the query range.
     * @param queryRange  the query range.
     * @param[inout] hint  the index to look first; will be changed to the index in ranges (successful search), or -1.
     */
    bool findOneThatIntersects(SpatialRange const& queryRange, size_t& hint) const;

    /**
     * @return whether at least one stored range contains a query point.
     * @param queryCoords  the query coordinates.
     * @param[inout] hint  the index to look first; will be changed to the index in ranges (successful search), or -1.
     */
    bool findOneThatContains(Coordinates const& queryCoords, size_t& hint) const;

    /**
     * @return whether at least one stored range contains a query range.
     * @param queryRange  the query range.
     * @param[inout] hint  the index to look first; will be changed to the index in ranges (successful search), or -1.
     */
    bool findOneThatContains(SpatialRange const& queryRange, size_t& hint) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

using SpatialRangesPtr = std::shared_ptr<SpatialRanges>;

}  // namespace scidb

#endif
