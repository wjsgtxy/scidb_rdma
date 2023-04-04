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

///
/// @file PhysicalBoundaries.h
///

#ifndef PHYSICAL_BOUNDARIES_H
#define PHYSICAL_BOUNDARIES_H

#include <array/Attributes.h>
#include <array/Coordinate.h>
#include <array/Dimensions.h>
#include <system/Exceptions.h>

namespace scidb
{

class Array;
class ArrayDesc;
class ConstChunk;
class SharedBuffer;
class SpatialRange;

/**
 * A class used to loosely represent a rectilinear box that contains data, allows for computations
 * like reshaping, intersections, and data size estimation. Used by the optimizer to reason about
 * the size of the results returned by queries.
 */
class PhysicalBoundaries
{
private:
    Coordinates _startCoords;
    Coordinates _endCoords;
    double _density;

public:

    /**
     * Create a new set of boundaries assuming that the given schema is completely full of cells
     * (fully dense array).
     *
     * @param schema desired array shape
     * @return boundaries with coordinates at edges of schema
     */
    static PhysicalBoundaries createFromFullSchema(ArrayDesc const& schema );

    /**
     * Create a new set of boundaries for an array using a list of chunk coordinates present in the array
     * @param inputArray for which the boundaries to be computed; must support Array::RANDOM access
     * @param chunkCoordinates a set of array chunk coordinates
     * @return boundaries with coordinates at edges of inputArray
     */
    static PhysicalBoundaries createFromChunkList(std::shared_ptr<Array>& inputArray,
                                                  const std::set<Coordinates, CoordinatesLess>& chunkCoordinates);


    /**
     * Create a new set of boundaries that span numDimensions dimensions but contain 0 cells (fully sparse array).
     * @param numDimensions desired number of dimensions
     * @return boundaries with numDimensions nonintersecting coordinates.
     */
    static PhysicalBoundaries createEmpty(size_t numDimensions);

    /**
     * Given a set of array attributes, compute the estimated size of a single cell of this array. Uses
     * CONFIG_STRING_SIZE_ESTIMATION for variable-length types.
     * @param attrs a list of array attributes
     * @return the sum of the attribute sizes
     */
    static uint32_t getCellSizeBytes(const Attributes& attrs);

    /**
     * Compute the number of logical cells that are enclosed in the bounding box between start and end.
     * @return the product of (end[i] - start[i] + 1) for all i from 0 to end.size(), not to exceed
     *         CoordinateBounds::getMaxLength()
     */
    static uint64_t getNumCells (Coordinates const& start, Coordinates const& end);

    /**
     * Given a position in the space given by currentDims, wrap the coordinates around a new space given by newDims
     * and return the position of the corresponding cell.
     * @param inCoords a set of coordinates
     * @param currentDims a list of array dimensions, must match inCoords
     * @param newDims another list of array dimensions
     * @param throwOnOverflow if inCoords won't fit into the new space, throw an exception
     *                        if true, otherwise return a set of CoordinateBounds::getMax() values
     * @return The coordinates of the cell that corresponds to inCoords in newDims, if
     *         possible. The result may be incorrect if the volume of the region specified by
     *         newDims is lower than the volume of currentDims. The result may be a set of
     *         CoordinateBounds::getMax() values if currentDims are unbounded.
     */
    static Coordinates reshapeCoordinates (Coordinates const& inCoords,
                                           Dimensions const& currentDims,
                                           Dimensions const& newDims,
                                           bool throwOnOverflow);

private:
    /**
     * Within the space given by dims, compute the row-major-order number of the cell at coords.
     * @param coords the position of a cell
     * @param dims a set of dimensions
     * @return the row-major-order position of coords, not to exceed CoordinateBounds::getMaxLength()
     */
    static uint64_t getCellNumber (Coordinates const& coords, Dimensions const& dims);

    /**
     * Within the space given by dims, compute the coordinate of the cellNum-th cell.
     * @param cellNum the number of a cell in row-major order
     * @param dims a set of dimensions
     * @param throwOnOverflow when cellNum exceeds the volume provided by dims, throw an exception
     *                        if true, otherwise return a set of CoordinateBounds::getMax() values
     * @return the coordinates obtained by wrapping cellNum around the space of dims, or a set
     *         of CoordinateBounds::getMax() positions if cellNum does not fit and !throwOnOverflow
     */
    static Coordinates getCoordinates(uint64_t cellNum, Dimensions const& dims, bool throwOnOverflow = true);

public:
    /**
     * Noop. Required to satisfy the PhysicalQueryPlanNode default ctor.
     */
    PhysicalBoundaries()
        : _density(0.0)
    { }

    /**
     * Create a new bounding box.
     * @param start the upper-left coordinates
     * @param end the lower-right coordinates
     * @param density the percentage of the box that is occupied with data (between 0 and 1)
     */
    PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density = 1.0);

    ~PhysicalBoundaries() = default;

    /**
     * @return the upper-left coordinates of the box
     */
    const Coordinates& getStartCoords() const
    {
        return _startCoords;
    }

    /**
     * @return the lower-right coordinates of the box
     */
    const Coordinates& getEndCoords() const
    {
        return _endCoords;
    }

    /**
     * @return the density of the data in the box
     */
    double getDensity() const
    {
        return _density;
    }

    /**
     * @return true if the box is volume-less
     */
    bool isEmpty() const;

    /**
     * Determine if a coordinate along a particular dimension is inside the box.
     * @param in a coordinate value
     * @param dimensionNum the dimension along which to check
     * @return true if getStartCoords()[dimensionNum] <= in <= getEndCoords()[dimensionNum]; false otherwise.
     */
    bool isInsideBox (Coordinate const& in, size_t const& dimensionNum) const;

    /**
     * Determine if a Coordinates vector is inside the PhysicalBoundaries box.
     * @param Coordinates reference
     * @return true iff isInsideBox(in[i], i) for all coordinates.
     */
    bool isInsideBox (Coordinates const& in) const;

    /**
     * Determine if another PhysicalBoundaries box is inside this box.
     * @param other a bounding box, must have the same number of dimensions
     * @return true iff isInsideBox(Coordinates const&) for start and end
     * @note Intentionally NOT another isInsideBox() overload, because
     *       the "directionality" of who's-inside-who would differ
     *       from all the other overloads.
     */
    bool contains(PhysicalBoundaries const& other) const;

    /**
     * Compute the number of logical cells that are enclosed in the bounding box between getStartCoords() and
     * getEndCoords()
     * @return the volume of the bounding box, not to exceed CoordinateBounds::getMaxLength()
     */
    uint64_t getNumCells() const;

    /**
     * Given a set of dimensions, compute the maximum number of chunks that may reside inside this bounding
     * box.
     * @param dims the dimensions used for array start, end and chunk interval
     * @return the number of chunks inside this
     * @throws UnknownChunkIntervalException if any dimension is autochunked
     */
    uint64_t getNumChunks(Dimensions const& dims) const;

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(UnknownChunkIntervalException,
                                      SCIDB_SE_INTERNAL,
                                      SCIDB_LE_UNKNOWN_ERROR);
    /**
     * Given a schema, estimate the total size, in bytes that an array would occupy in this bounding box.
     * @param schema an array shape
     * @return the estimate size of this bounding box area.
     */
    double getSizeEstimateBytes(const ArrayDesc& schema) const;

    /**
     * Intersect this with another bounding box, in place.
     * @param other a bounding box, must have the same number of dimensions
     * @return *this (the bounding box intersection, with maximum possible density)
     * @see scidb::intersectWith()
     */
    PhysicalBoundaries& intersectWith (PhysicalBoundaries const& other);

    /**
     * Merge another bounding box into this, in place.
     * @param other a bounding box, must have the same number of dimensions
     * @return *this (the bounding box union, with maximum possible density)
     * @see scidb::unionWith()
     */
    PhysicalBoundaries& unionWith(PhysicalBoundaries const& other);

    /**
     * Compute the cartesian product of this and other, in place.
     * @param other a different bounding box.
     * @return *this (the bounding box cartesian product, with the product of the densities)
     * @see scidb::crossWith()
     */
    PhysicalBoundaries& crossWith (PhysicalBoundaries const& other);

    /**
     * Add offset coordinates to each boundary corner, in place.
     * @params offset Coordinates vector to add to each corner
     * @return *this (the bounding box, with shifted boundaries)
     * @see scidb::shiftBy()
     */
    PhysicalBoundaries& shiftBy(Coordinates const& offset);

    /**
     * Wrap this bounding box around a new set of dimensions, return the result as a new object.
     * @param oldDims the current dimensions - must match the number of dimensions in this
     * @param newDims the new dimensions
     * @throws scidb::SystemException SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES
     * @return the reshaped bounding box
     * @note If the current (old) dimensions are all bounded, reshape throws if they won't fit
     *       inside the new dimensions.  If any of the current dimensions are @em unbounded ('*'),
     *       they will certainly not fit inside the new dimensions, but instead of throwing the
     *       returned PhyiscalBoundaries object will be unbounded ('*') in all dimensions.
     */
    PhysicalBoundaries reshape(Dimensions const& oldDims, Dimensions const& newDims) const;

    /**
     * Write this into a buffer
     * @return the serialized form
     */
    std::shared_ptr<SharedBuffer> serialize() const;

    /**
     * Construct a PhysicalBoundaries from a buffer
     * @param buf the result of a previous PhysicalBoundaries::serialize call
     */
    static PhysicalBoundaries deSerialize(std::shared_ptr<SharedBuffer> const& buf);

    /**
     * Expand the boundaries to include data from the given chunk. By default, has a
     * side effect of materializing the chunk. All known callers invoke the routine
     * before needing to materialize the chunk anyway, thus no work is wasted. After
     * materialization, the chunk is examined and the boundaries are expanded using only
     * the non-empty cells in the chunk. If the second argument is set, no materialization
     * takes place and the boundaries are simply updated with chunk start and end positions.
     * @param chunk the chunk to use
     * @param chunkShapeOnly if set to true, only update the bounds from the chunk start and end
     *                       positions
     */
    void updateFromChunk(ConstChunk const* chunk, bool chunkShapeOnly = false);

    /** Expand the boundaries to include a SpatialRange. */
    void updateFromRange(SpatialRange const&);

    /**
     * Create a new set of boundaries based on the this, trimmed down to the max and min
     * coordinate set by dims.
     * @param dims the dimensions to reduce to; must match the number of coordinates
     * @return the new boundaries that do not overlap min and end coordinates of dims
     */
    PhysicalBoundaries trimToDims(Dimensions const& dims) const;

    /**
     * Output a human-readable string description of bounds onto stream
     * @param stream where to write
     * @param bounds the boundaries to record
     * @return the stream with the boundaries data appended to the end
     */
    friend std::ostream& operator<<(std::ostream& stream, const PhysicalBoundaries& bounds);
};


/**
 * Intersect two bounding boxes and return the result as a new object.
 * @param left  a  bounding box
 * @param right a different bounding box, must have the same number of dimensions
 * @return the bounding box intersection, with maximum possible density
 */
PhysicalBoundaries intersectWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right);

/**
 * Merge two bounding boxes and return the result as a new object.
 * @param left  a  bounding box
 * @param right a different bounding box, must have the same number of dimensions
 * @return the bounding box union, with maximum possible density
 */
PhysicalBoundaries unionWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right);

/**
 * Compute the cartesian product of two bounding boxes and return the result as a new object.
 * @param left  a  bounding box
 * @param right a different bounding box, must have the same number of dimensions
 * @return the bounding box cartesian product, with the product of the densities
 */
PhysicalBoundaries crossWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right);

/**
 * Add offset coordinates to each boundary corner and return the result as a new object.
 * @param bounds a bounding box
 * @params offset vector to add to each corner
 * @return shifted bounding box
 */
PhysicalBoundaries shiftBy(PhysicalBoundaries const& bounds, Coordinates const& offset);

} // namespace

#endif /* ! PHYSICAL_BOUNDARIES_H */
