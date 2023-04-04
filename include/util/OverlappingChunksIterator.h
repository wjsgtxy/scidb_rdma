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
 * OverlappingChunksIterator.h
 *
 *  Created on: Aug 15, 2012
 *      Author: dzhang
 *  The iterator can be used to enumerate the positions of all the chunks, starting with the 'home' chunk, that should store an item.
 */

#ifndef OVERLAPPINGCHUNKSITERATOR_H_
#define OVERLAPPINGCHUNKSITERATOR_H_

#include <assert.h>
#include <array/Array.h>
#include <system/Constants.h>
#include <bitset>
#include <util/RegionCoordinatesIterator.h>

namespace scidb
{
/**
 * An iterator that iterates over the positions of all the chunks that should store an item.
 * Normally, an item is stored in a single chunk.
 * However, with overlaps, an item may also need to be stored in adjacent chunks.
 * All we need to do is construct a RegionCoordinatesIterator with proper arguments, it does the rest (quite well).
 *
 * @note Use with caution! This class iterates over the logical space.
 * @see THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION in RegionCoordinatesIterator.h.
 *
 */
class OverlappingChunksIterator : public ConstIterator
{
public:
    /**
     * @param dims the array dimensions used for chunk size and overlap
     * @param itemPos the position of a cell in the array
     */
    OverlappingChunksIterator(Dimensions const& dims, Coordinates const& itemPos)
        : _low(getLowCoordinates(dims, itemPos)),
          _high(getHighCoordinates(dims, itemPos)),
          _intervals(getIntervals(dims)),
          _regionIter(_low, _high, _intervals)
    {}

    /**
     * Check if end is reached
     * @return true if iterator reaches the end of the region
     */
    bool end() override
    {
        return _regionIter.end();
    }

    /**
     * Position cursor to the next chunk.
     */
    void operator ++() override
    {
        ++_regionIter;
    }

    /**
     * Advance to the smallest position >= a given newPos.
     * @param newPos  the position to reach or exceed.
     * @return whether any advancement is made.
     */
    bool advanceToAtLeast(Coordinates const& newPos)
    {
        return _regionIter.advanceToAtLeast(newPos);
    }

    /**
     * Get coordinates of the current element.
     */
    Coordinates const& getPosition() override
    {
        return _regionIter.getPosition();
    }

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (in the region),
     * false otherwise
     * @note the pos MUST be a chunk start.
     */
    bool setPosition(Coordinates const& pos) override
    {
        return _regionIter.setPosition(pos);
    }

    /**
     * Reset iterator to the first coordinates.
     */
    void restart() override
    {
        _regionIter.restart();
    }

private:
    //helpers so we can construct the superclass in the initializer list
    static Coordinates getLowCoordinates(Dimensions const& dims, Coordinates const& itemPos)
    {
        size_t const nDims = itemPos.size();
        Coordinates low (nDims);
        for (size_t i= 0; i<nDims; ++i)
        {
            low[i] =  std::max<int64_t> (itemPos[i] - dims[i].getChunkOverlap(), dims[i].getStartMin());
            low[i] -= (low[i] - dims[i].getStartMin()) % dims[i].getChunkInterval();
        }
        return low;
    }

    static Coordinates getHighCoordinates(Dimensions const& dims, Coordinates const& itemPos)
    {
        size_t const nDims = itemPos.size();
        Coordinates high (nDims);
        for (size_t i= 0; i<nDims; ++i)
        {
            high[i] =  std::min<int64_t> (itemPos[i] + dims[i].getChunkOverlap(), dims[i].getEndMax());
        }
        return high;
    }

    static std::vector<size_t> getIntervals(Dimensions const& dims)
    {
        size_t const nDims = dims.size();
        std::vector<size_t> res (nDims);
        for (size_t i =0; i<nDims; ++i)
        {
            res[i] = dims[i].getChunkInterval();
        }
        return res;
    }

    Coordinates _low;
    Coordinates _high;
    std::vector<size_t> _intervals;
    RegionCoordinatesIterator _regionIter;
};

}
#endif /* OVERLAPPINGCHUNKSITERATOR_H_ */
