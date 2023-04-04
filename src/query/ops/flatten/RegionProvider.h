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

/**
 * @file RegionProvider.h
 * @author Mike Leibensperger
 * @brief Wrap the Array API to provide row-of-chunks sequential access.
 */

#ifndef REGION_PROVIDER_H
#define REGION_PROVIDER_H

#include <array/Coordinate.h>
#include <memory>

namespace scidb
{

class Array;
class ConstArrayIterator;
class ConstChunk;

using Region = std::vector<ConstChunk const*>;

/**
 * Provide region-at-a-time access to an Array.
 *
 * @details A "region" is a row of chunks, that is, all the cell data
 * for a logical region of the array.  (A chunk is all the values for
 * a single attribute in the region.)
 *
 * @note Focusing on sequential access for the immediate use case
 * (SlowDfArray), so setPosition() is not yet supported.
 *
 * @see CellProvider
 * @note Eventually, TileProvider too.
 */
class RegionProvider
{
public:
    // 'structors
    explicit RegionProvider(std::shared_ptr<Array>& ary);

    RegionProvider() = delete;
    RegionProvider(RegionProvider const&) = delete;
    RegionProvider& operator=(RegionProvider const&) = delete;

    /** @return true iff no more regions */
    bool end() const;

    /**
     * Get next sequential region and its position.
     *
     * @return true if region is valid, false if no more data
     *
     * @note Input parameters are cleared if end() encountered, since
     * the Region chunk memory is owned the input iterators, which
     * have now moved to the end() state, invalidating the memory.  No
     * dangling pointers!
     */
    bool next(Region& r, Coordinates& where);

    /** Reposition to beginning of input array. */
    void restart();

private:
    using Iters = std::vector<std::shared_ptr<ConstArrayIterator>>;

    std::shared_ptr<Array> _array;
    Iters _inIters;
    bool _first { true };

    Iters& _getInIters();
};

} // namespace

#endif /* ! REGION_PROVIDER_H */
