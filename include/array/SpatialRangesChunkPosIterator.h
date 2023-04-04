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
 * SpatialRangesChunkPosIterator.h
 *
 *  Created on: Aug 15, 2014
 *      Author: Donghui Zhang
 */

#ifndef SPATIALRANGESCHUNKPOSITERATOR_H_
#define SPATIALRANGESCHUNKPOSITERATOR_H_

#include <util/SpatialType.h>
#include <util/MultiConstIterators.h>
#include <util/RegionCoordinatesIterator.h>

#include <array/ArrayDesc.h>

#include <memory>

namespace scidb
{
/**
 * A class that enumerates the positions of the chunks that intersect at least one of the stored ranges.
 *
 * @note Use with caution! This class iterates over the logical space.
 * @see THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION in RegionCoordinatesIterator.h.
 */
class SpatialRangesChunkPosIterator: public ConstIterator
{
public:
    /**
     * @param spatialRanges  a SpatialRanges object.
     * @param schema  the array schema.
     */
    SpatialRangesChunkPosIterator(std::shared_ptr<SpatialRanges> const& spatialRanges, ArrayDesc const& schema);

    /**
     * @return true if done with enumeration.
     */
    bool end() override;

    /**
     * Advance to the next chunkPos (that intersect with at least one of the ranges).
     */
    void operator ++() override;

    /**
     * Get coordinates of the current element in the chunk
     */
    Coordinates const& getPosition() override;

    /**
     * Not supported.
     */
    bool setPosition(Coordinates const& pos) override;

    /**
     * Restart builds the rawIterators from low and high positions, and generate wrapperIterator..
     */
    void restart() override;

    /**
     * Make minimal advancement of the iterator, provided that its position >= the specified newPos.
     * @param newPos  the position to reach or exceed.
     * @return whether any advancement is made.
     * @note it is possible that as result of the advancement, end() is reached.
     *
     */
    bool advancePositionToAtLeast(Coordinates const& newPos);

private:
    /**
     * How many spatial ranges are there?
     */
    const size_t _numRanges;

    /**
     * A spatialRanges object.
     */
    std::shared_ptr<SpatialRanges> _spatialRanges;

    /**
     * Array schema.
     */
    ArrayDesc _schema;

    /**
     * A vector of RegionCoordinatesIterator objects.
     */
    std::vector<std::shared_ptr<ConstIterator> > _rawIterators;

    /**
     * A MultiConstIterators object wrapping _rawIterators, for synchronized advancement.
     */
    std::shared_ptr<MultiConstIterators> _wrapperIterator;

    /**
     * Each Coordinates in the vector will be used to initialize the 'low' for one of _rawIterators.
     * This is set in the constructor and never change.
     */
    std::vector<Coordinates> _lowPositionsForRawIterators;

    /**
     * Each Coordinates in the vector will be used to initialize the 'high' for one of _rawIterators.
     * This is set in the constructor and never changes.
     */
    std::vector<Coordinates> _highPositionsForRawIterators;

    /**
     * Chunk intervals.
     */
    std::vector<size_t> _intervals;
};

} // namespace
#endif /* SPATIALRANGESCHUNKPOSITERATOR_H_ */
