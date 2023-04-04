/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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
 * SpatialType.cpp
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */

#include <util/SpatialType.h>

#include <system/Utils.h>  // SCIDB_ASSERT
#include <util/RtreeOfSpatialRange.h>  // RtreeOfSpatialRange

namespace scidb
{

//--------------------------------------------------
// Declarations
//--------------------------------------------------

struct SpatialRanges::Impl
{
public:  // data
    /// @brief Number of dimensions.
    const size_t _numDims;

    /// @brief A vector of SpatialRange objects.
    std::vector<SpatialRange> _ranges;

    /// @brief Whether an index has been built using the inserted objects.
    bool _isIndexBuilt;

    /// @brief An Rtree index built from _ranges.
    RtreeOfSpatialRange _rtree;

public:  // methods
    explicit Impl(const size_t numDims)
      : _numDims(numDims), _ranges(), _isIndexBuilt(true), _rtree(numDims)
    {}
};

//--------------------------------------------------
// Free functions
//--------------------------------------------------

DominanceRelationship calculateDominance(Coordinates const& left, Coordinates const& right)
{
    assert(left.size() == right.size() && !left.empty());
    bool isDominating = true;
    bool isDominatedBy = true;
    for (size_t i=0, n=left.size(); i<n; ++i) {
        if (left[i] > right[i]) {
            isDominatedBy = false;
        }
        else if (left[i] < right[i]) {
            isDominating = false;
        }
    }
    if (isDominating && isDominatedBy) {
        return EQUALS;
    }
    else if (!isDominating && !isDominatedBy) {
        return HAS_NO_DOMINANCE_WITH;
    }
    return isDominating ? STRICTLY_DOMINATES : IS_STRICTLY_DOMINATED_BY;
}

std::ostream& operator<< (std::ostream& stream, const SpatialRange& range)
{
    stream<<"SpatialRange[ start "<<range._low<<" end "<<range._high<<" ]";
    return stream;
}


bool isDominatedBy(Coordinates const& left, Coordinates const& right)
{
    DominanceRelationship dr = calculateDominance(left, right);
    return dr==EQUALS || dr==IS_STRICTLY_DOMINATED_BY;
}

//--------------------------------------------------
// SpatialRanges
//--------------------------------------------------

SpatialRanges::SpatialRanges(const size_t numDims)
  : _impl(std::make_unique<SpatialRanges::Impl>(numDims))
{
}

SpatialRanges::~SpatialRanges() = default;

size_t SpatialRanges::numDims() const
{
    return _impl->_numDims;
}

std::vector<SpatialRange> const& SpatialRanges::ranges() const
{
    return _impl->_ranges;
}

void SpatialRanges::insert(SpatialRange&& range)
{
    _impl->_isIndexBuilt = false;
    _impl->_ranges.push_back(range);
}

void SpatialRanges::buildIndex()
{
    _impl->_isIndexBuilt = true;
    if (!_impl->_rtree.valid()) return;
    _impl->_rtree.clear();
    for (auto const& range: _impl->_ranges) {
        _impl->_rtree.insert(range);
    }
}

bool SpatialRanges::findOneThatIntersects(SpatialRange const& queryRange, size_t& hint) const
{
    SCIDB_ASSERT(_impl->_isIndexBuilt);
    if (_impl->_rtree.valid()) {
        return _impl->_rtree.findOneThatIntersects(queryRange);
    }

    if (hint>0 && hint<_impl->_ranges.size()) {
        if (_impl->_ranges[hint].intersects(queryRange)) {
            return true;
        }
    }
    for (size_t i=0, n=_impl->_ranges.size(); i<n; ++i) {
        if (_impl->_ranges[i].intersects(queryRange)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

bool SpatialRanges::findOneThatContains(Coordinates const& queryCoords, size_t& hint) const
{
    SCIDB_ASSERT(_impl->_isIndexBuilt);
    if (_impl->_rtree.valid()) {
        return _impl->_rtree.findOneThatContains(queryCoords);
    }

    if (hint>0 && hint<_impl->_ranges.size()) {
        if (_impl->_ranges[hint].contains(queryCoords)) {
            return true;
        }
    }
    for (size_t i=0, n=_impl->_ranges.size(); i<n; ++i) {
        if (_impl->_ranges[i].contains(queryCoords)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

bool SpatialRanges::findOneThatContains(SpatialRange const& queryRange, size_t& hint) const
{
    SCIDB_ASSERT(_impl->_isIndexBuilt);
    if (_impl->_rtree.valid()) {
        return _impl->_rtree.findOneThatContains(queryRange);
    }

    if (hint>0 && hint<_impl->_ranges.size()) {
        if (_impl->_ranges[hint].contains(queryRange)) {
            return true;
        }
    }
    for (size_t i=0, n=_impl->_ranges.size(); i<n; ++i) {
        if (_impl->_ranges[i].contains(queryRange)) {
            hint = i;
            return true;
        }
    }
    hint = -1;
    return false;
}

}  // namespace scidb
