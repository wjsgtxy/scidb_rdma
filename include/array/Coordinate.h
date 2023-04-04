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

#ifndef COORDINATE_H_
#define COORDINATE_H_

/****************************************************************************/

#include <vector>                                        // For vector
#include <inttypes.h>                                    // For int64_t etc

#include <util/PointerRange.h>                           // For PointerRange
#include <util/Hashing.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

typedef int64_t                         position_t;
typedef int64_t                         Coordinate;
typedef std::vector <Coordinate>        Coordinates;
typedef PointerRange<Coordinate>        CoordinateRange;
typedef PointerRange<Coordinate const>  CoordinateCRange;

/**
 * A functor to hash a Coordinates object.
 * E.g. you may define:
 * std::unordered_map<Coordinates, Value, CoordinatesHash> coordsToValue;
 */
typedef VectorHash<Coordinate>          CoordinatesHash;

/****************************************************************************/

/* Email from Alex (2015_02_15):
 * Philosophically:
 *  -  '*' and MAX_COORDINATE (aka 2^62 -1) are merely syntactic synonyms.
 *     They can be used interchangeably in the queries
 *
 * -   MAX_COORDINATE is obviously the largest bound an array can have.
 *     Arrays with a bound greater than this are not allowed. Analogous
 *     for the MIN_COORDINATE
 *
 * -   the system makes NO distinction between arrays that end with '*'
 *     or MAX_COORDINATE, versus arrays that end with a smaller number,
 *     say 10,000. They are just arrays of different logical size.
 *
 * -   in other words, there is no such thing as an "unbounded" array
 *     really. It makes no sense to draw this distinction
 *
 * -  operators "do the right thing" whenever possible
*/
class CoordinateBounds
{
public:
    /// MAX_COORDINATE replacement.
    static inline int64_t getMax()
    {
        // This is 2**62-1 (rather than 2**63-1) to allow the largest
        // possible dimension length (getMax() - getMin() + 1) to fit
        // in an int64_t without overflow, simplifying typecasting.
        return (1ULL << 62) - 1;
    }

    /// MIN_COORDINATE replacement.
    static inline int64_t getMin()
    {
        return - static_cast<int64_t>(getMax());
    }

    static inline size_t getMaxLength()
    {
        return getMax() - getMin() + 1;
    }

    static inline bool isValidLength(size_t length)
    {
        return getMaxLength() >= length;
    }

    /**
     * @param value - the value to compare against '*'
     * @brief Determine if the parameter value matches '*'
     * @return true={value=='*'}, false otherwise
     */
    static inline bool isMaxStar(int64_t value)
    {
        return value == getMax();
    }
};

/**
 * @param Coodinates - vector of dimension
 * @brief Set the values of each dimension to the minimum value
 */
void setCoordsMin ( Coordinates& coord );

/**
 * @param Coodinates - vector of dimension
 * @brief Set the values of each dimension to the maximum value
 */
void setCoordsMax ( Coordinates& coord );

/**
 * @param Coordinates - coordinates to update, and coordinates to update from
 * @brief If a dimension's index value in the other is smaller than the
 *        corresponding value in the coord, reduce the index value in the
 *        coord
 */
void resetCoordMin ( Coordinates& coord, const Coordinates& other );

/**
 * @param Coordinates - coordinates to update, and coordinates to update from
 * @brief If a dimension's index value in the other is larger than the
 *        corresponding value in the coord, reduce the index value in the
 *        coord
 */
void resetCoordMax ( Coordinates& coord, const Coordinates& other );

/****************************************************************************/

/**
 * @param coord - vector of dimension
 * @brief Set the index values of each dimension to the minimum possible value
 */
void setCoordsMin ( Coordinates& coord );

/**
 * @param coord - vector of dimension
 * @brief Set the index values of each dimension to the maximum possible value
 */
void setCoordsMax ( Coordinates& coord );

/**
 * @param coord - coordinates to update
 * @param other - coordinates to update from
 * @brief If a dimension's index value in the 'other' is SMALLER than the
 *        corresponding value in the 'coord', reduce the index value in the
 *        'coord' to the value in the 'other'. Used to track upper corner
 *        of MBR.
 */
void resetCoordMin ( Coordinates& coord, const Coordinates& other );

/**
 * @param coord - coordinates to update
 * @param other - coordinates to update from
 * @brief If a dimension's index value in the 'other' is LARGER than the
 *        corresponding value in the 'coord', reduce the index value in the
 *        'coord' to the value in the 'other'. Used to track the lower corner
 *        of MBR.
 */
void resetCoordMax ( Coordinates& coord, const Coordinates& other );

/**
 *  Compare two points in row-major order and return a number indicating how
 *  they differ.
 *
 *  @param  a   the left side of the comparison
 *  @param  b   the right side of the comparison
 *  @return negative if a is less than b;
 *          positive if b is less than a,
 *          0 if both are equal.
 */
inline int64_t coordinatesCompare(CoordinateCRange a, CoordinateCRange b)
{
    assert(a.size() == b.size());

    for (size_t i=0, n=a.size(); i != n; ++i)
    {
        if (int64_t d = a[i] - b[i])
        {
            return d;
        }
    }

    return 0;
}

/**
 *  Compare two points in colum-major order and return a number indicating how
 *  they differ.
 *
 *  @param  a   the left side of the comparison
 *  @param  b   the right side of the comparison
 *  @return negative if a is less than b;
 *          positive if b is less than a,
 *          0 if both are equal.
 */
inline int64_t coordinatesCompareCMO(CoordinateCRange a, CoordinateCRange b)
{
    assert(a.size() == b.size());

    for (size_t i = a.size(); i-- > 0; )
    {
        if (int64_t d = a[i] - b[i])
        {
            return d;
        }
    }

    return 0;
}

/**
 * Compare two points in row-major order.
 */
struct CoordinatesLess
{
    bool operator()(CoordinateCRange a, CoordinateCRange b) const
    {
        return coordinatesCompare(a,b) < 0;
    }
};

/**
 *  Compare two points in column-major order.
 */
struct CoordinatesLessCMO
{
    bool operator()(CoordinateCRange a,CoordinateCRange b) const
    {
        return coordinatesCompareCMO(a,b) < 0;
    }
};

/**
 * Compare two points in row-major order.
 */
struct CoordinatePtrLess
{
    CoordinatePtrLess(size_t n)
     : rank(n)
    {}

    bool operator()(const Coordinate* p,const Coordinate* q) const
    {
        return coordinatesCompare(pointerRange(rank,p), pointerRange(rank,q)) < 0;
    }

    size_t const rank;
};

/**
 * Compare two points in column-major order.
 */
struct CoordinatePtrLessCMO
{
    CoordinatePtrLessCMO(size_t n)
     : rank(n)
    {}

    bool operator()(const Coordinate* p,const Coordinate* q) const
    {
        return coordinatesCompareCMO(pointerRange(rank,p), pointerRange(rank,q)) < 0;
    }

    size_t const rank;
};

/****************************************************************************/

std::ostream& operator<<(std::ostream&,CoordinateCRange);

/****************************************************************************/

typedef CoordinateCRange   CoordsToStr;                  // Obsolete name
typedef CoordinatesLess    CoordinatesComparator;        // Obsolete name
typedef CoordinatesLessCMO CoordinatesComparatorCMO;     // Obsolete name


/****************************************************************************/
} // namespace
/****************************************************************************/
#endif
/****************************************************************************/
