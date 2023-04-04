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
 * This file defines an in-memory R-tree that stores SpatialRange objects.
 *
 * @author Donghui Zhang
 * @date 2017-2-3
 */

#ifndef RTREE_OF_SPATIAL_RANGE_H_
#define RTREE_OF_SPATIAL_RANGE_H_

#include <cstdint>  // size_t
#include <memory>  // unique_ptr

#include <array/Coordinate.h>  // Coordinates
#include <util/SpatialType.h>  // SpatialRange

namespace scidb
{

class SpatialRange;

/**
 * @brief An in-memory R-tree of SpatialRange objects.
 * @note A tree is valid() only if numDims is in [1..kMaxNumDims], and
 *       the config parameter filter-use-rtree is true.
 */
class RtreeOfSpatialRange
{
public:  // constants
    static constexpr size_t kMaxNumDims = 10;

public:
    explicit RtreeOfSpatialRange(const size_t numDims);
    ~RtreeOfSpatialRange();

    bool valid() const;

    /**
     * @brief Insert an object.
     */
    void insert(const SpatialRange&);

    /**
     * @brief Removes all objects.
     */
    void clear();

    /**
     * @brief Return whether at least one stored range intersects the query range.
     * @param queryRange  the query range.
     */
    bool findOneThatIntersects(SpatialRange const& queryRange) const;

    /**
     * @brief Return whether at least one stored range contains a query point.
     * @param queryCoords  the query coordinates.
     */
    bool findOneThatContains(Coordinates const& queryCoords) const;

    /**
     * @brief Return whether at least one stored range contains a query range.
     * @param queryRange  the query range.
     */
    bool findOneThatContains(SpatialRange const& queryRange) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};  // RtreeOfSpatialRange

}  // namespace scidb

#endif  // RTREE_OF_SPATIAL_RANGE_H_
