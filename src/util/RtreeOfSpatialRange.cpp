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
 * This file implements RtreeOfSpatialRange.h.
 *
 * @author Donghui Zhang
 * @date 2017-2-3
 */

#include <util/RtreeOfSpatialRange.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>

#include <system/Config.h>  // Config
#include <system/Utils.h>  // SCIDB_ASSERT
#include <util/FlexiblePoint.h>  // bg::model::FlexiblePoint

namespace bg = boost::geometry;

namespace scidb
{

//-------------------------------------------------
// Declarations
//-------------------------------------------------

/**
 * @brief The interface for RtreeFixedDim with any kNumDims.
 * @note The findXXX functions have the same signature as in SpatialRanges.
 */
class RtreeInterface
{
public:
   virtual ~RtreeInterface() {}
   virtual void insert(SpatialRange const&) = 0;
   virtual void clear() = 0;
   virtual bool findOneThatIntersects(SpatialRange const& queryRange) = 0;
   virtual bool findOneThatContains(Coordinates const& queryCoords) = 0;
   virtual bool findOneThatContains(SpatialRange const& queryRange) = 0;
};

/**
 * @brief A collection of types and utilities for supporting RtreeFixedDim.
 */
template<size_t kNumDims>
class FixedDim
{
public:  // constants and types
    static constexpr size_t kFanout = 8;
    using Point = bg::model::FlexiblePoint<Coordinate, kNumDims, bg::cs::cartesian>;
    using Box = bg::model::box<Point>;
    using Tree = bg::index::rtree<Box, bg::index::quadratic<kFanout>>;

public:  // utility methods that translates Point to/from Coordinates, and Box to/from SpatialRange.
    static void coordsToPoint(const Coordinates& coords, Point& p);
    static Point coordsToPoint(const Coordinates& coords);
    static void pointToCoords(const Point& p, Coordinates& coords);
    static Coordinates pointToCoords(const Point& p);
    static void spatialRangeToBox(const SpatialRange& range, Box& box);
    static Box spatialRangeToBox(const SpatialRange& range);
    static void boxToSpatialRange(const Box& box, SpatialRange& range);
    static SpatialRange boxToSpatialRange(const Box& box);
};  // FixedDim

/**
 * @brief An Rtree with a particular kNumDims.
 */
template<size_t kNumDims>
class RtreeFixedDim: public RtreeInterface
{
public:
    void insert(SpatialRange const&) override;
    void clear() override;
    bool findOneThatIntersects(SpatialRange const& queryRange) override;
    bool findOneThatContains(Coordinates const& queryCoords) override;
    bool findOneThatContains(SpatialRange const& queryRange) override;

private:
    typename FixedDim<kNumDims>::Tree _tree;
};

/**
 * @brief Implementation for RtreeOfSpatialRange.
 */
struct RtreeOfSpatialRange::Impl
{
public:  // data
    std::shared_ptr<RtreeInterface> _tree;
    size_t _numDims;

public:  // methods
    explicit Impl(const size_t numDims);
};

//-------------------------------------------------
// FixedDim
//-------------------------------------------------

template<size_t kNumDims>
inline void FixedDim<kNumDims>::coordsToPoint(const Coordinates& coords, Point& p)
{
    for (size_t i=0; i<kNumDims; ++i) {
        p.set(i, coords[i]);
    }
}

template<size_t kNumDims>
inline typename FixedDim<kNumDims>::Point FixedDim<kNumDims>::coordsToPoint(const Coordinates& coords)
{
    Point p;
    coordsToPoint(coords, p);
    return p;
}

template<size_t kNumDims>
inline void FixedDim<kNumDims>::pointToCoords(const Point& p, Coordinates& coords)
{
    for (size_t i=0; i<kNumDims; ++i) {
        coords[i] = p.get(i);
    }
}

template<size_t kNumDims>
inline Coordinates FixedDim<kNumDims>::pointToCoords(const Point& p)
{
    Coordinates coords(kNumDims);
    pointToCoords(p, coords);
    return coords;
}

template<size_t kNumDims>
inline void FixedDim<kNumDims>::spatialRangeToBox(const SpatialRange& range, Box& box)
{
    coordsToPoint(range._low, box.min_corner());
    coordsToPoint(range._high, box.max_corner());
}

template<size_t kNumDims>
inline typename FixedDim<kNumDims>::Box FixedDim<kNumDims>::spatialRangeToBox(const SpatialRange& range)
{
    Box box;
    spatialRangeToBox(range, box);
    return box;
}

template<size_t kNumDims>
inline void FixedDim<kNumDims>::boxToSpatialRange(const Box& box, SpatialRange& range)
{
    pointToCoords(box.min_corner(), range._low);
    pointToCoords(box.max_corner(), range._high);
}

template<size_t kNumDims>
inline SpatialRange FixedDim<kNumDims>::boxToSpatialRange(const Box& box)
{
    SpatialRange range(kNumDims);
    boxToSpatialRange(box, range);
    return range;
}

//-------------------------------------------------
// RtreeFixedDim
//-------------------------------------------------

template<size_t kNumDims>
inline void RtreeFixedDim<kNumDims>::insert(const SpatialRange& range)
{
    auto box = FixedDim<kNumDims>::spatialRangeToBox(range);
    _tree.insert(box);
}

template<size_t kNumDims>
inline void RtreeFixedDim<kNumDims>::clear()
{
    _tree.clear();
}

template<size_t kNumDims>
inline bool RtreeFixedDim<kNumDims>::findOneThatIntersects(SpatialRange const& queryRange)
{
    auto queryBox = FixedDim<kNumDims>::spatialRangeToBox(queryRange);
    std::vector<typename FixedDim<kNumDims>::Box> result;
    _tree.query(bg::index::intersects(queryBox), std::back_inserter(result));
    return !result.empty();
}

template<size_t kNumDims>
inline bool RtreeFixedDim<kNumDims>::findOneThatContains(Coordinates const& queryCoords)
{
    auto queryPoint = FixedDim<kNumDims>::coordsToPoint(queryCoords);

    // queryCoords is inside a box stored in the Rtree, as long as they intersect.
    std::vector<typename FixedDim<kNumDims>::Box> result;
    _tree.query(bg::index::intersects(queryPoint), std::back_inserter(result));
    return !result.empty();
}

template<size_t kNumDims>
inline bool RtreeFixedDim<kNumDims>::findOneThatContains(SpatialRange const& queryRange)
{
    auto queryBox = FixedDim<kNumDims>::spatialRangeToBox(queryRange);

    // For every box stored in the Rtree that intersects the queryRange, check if it contains the queryRange.
    std::vector<typename FixedDim<kNumDims>::Box> result;
    _tree.query(bg::index::intersects(queryBox), std::back_inserter(result));
    for (auto const& b : result) {
        if (bg::within(queryBox, b)) {
            return true;
        }
    }
    return false;
}

//-------------------------------------------------
// RtreeOfSpatialRange::Impl
//-------------------------------------------------

RtreeOfSpatialRange::Impl::Impl(const size_t numDims)
  : _numDims(numDims)
{
    const bool useRtree = Config::getInstance()->getOption<bool>(CONFIG_FILTER_USE_RTREE);
    if (!useRtree) {
        return;
    }

    static_assert(kMaxNumDims == 10, "The code here only covers up to 10 dims.");

    switch (numDims) {
      case 1:
          _tree = std::make_shared<RtreeFixedDim<1>>();
          break;
      case 2:
          _tree = std::make_shared<RtreeFixedDim<2>>();
          break;
      case 3:
          _tree = std::make_shared<RtreeFixedDim<3>>();
          break;
      case 4:
          _tree = std::make_shared<RtreeFixedDim<4>>();
          break;
      case 5:
          _tree = std::make_shared<RtreeFixedDim<5>>();
          break;
      case 6:
          _tree = std::make_shared<RtreeFixedDim<6>>();
          break;
      case 7:
          _tree = std::make_shared<RtreeFixedDim<7>>();
          break;
      case 8:
          _tree = std::make_shared<RtreeFixedDim<8>>();
          break;
      case 9:
          _tree = std::make_shared<RtreeFixedDim<9>>();
          break;
      case 10:
          _tree = std::make_shared<RtreeFixedDim<10>>();
          break;
      default:
          break;
    }
}

//-------------------------------------------------
// RtreeOfSpatialRange
//-------------------------------------------------

RtreeOfSpatialRange::RtreeOfSpatialRange(const size_t numDims)
  : _impl(std::make_unique<Impl>(numDims))
{
}

RtreeOfSpatialRange::~RtreeOfSpatialRange() = default;

bool RtreeOfSpatialRange::valid() const
{
    return static_cast<bool>(_impl->_tree);
}

void RtreeOfSpatialRange::insert(const SpatialRange& range)
{
    SCIDB_ASSERT(valid());
    return _impl->_tree->insert(range);
}

void RtreeOfSpatialRange::clear()
{
    SCIDB_ASSERT(valid());
    return _impl->_tree->clear();
}

bool RtreeOfSpatialRange::findOneThatIntersects(SpatialRange const& queryRange) const
{
    SCIDB_ASSERT(valid());
    return _impl->_tree->findOneThatIntersects(queryRange);
}

bool RtreeOfSpatialRange::findOneThatContains(Coordinates const& queryCoords) const
{
    SCIDB_ASSERT(valid());
    return _impl->_tree->findOneThatContains(queryCoords);
}

bool RtreeOfSpatialRange::findOneThatContains(SpatialRange const& queryRange) const
{
    SCIDB_ASSERT(valid());
    return _impl->_tree->findOneThatContains(queryRange);
}

}  // namespace scidb
