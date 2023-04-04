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
 * @file CellProvider.h
 * @author Mike Leibensperger
 * @brief Extract individual cell values from a Region.
 */

#ifndef CELL_PROVIDER_H
#define CELL_PROVIDER_H

#include "RegionProvider.h"

#include <array/Coordinate.h>
#include <memory>

namespace scidb
{

class ConstChunkIterator;
class Value;

using Cell = std::vector<scidb::Value>;

/**
 * Provide cell-at-a-time access to an Array.
 *
 * @detail The intent is to take the pain out of using chunk
 * iterators.
 *
 * @note Focusing on sequential access for the immediate use case
 * (SlowDfArray), so setPosition() is not yet supported.
 */
class CellProvider
{
public:
    // 'structors
    CellProvider(RegionProvider&);
    CellProvider(RegionProvider&, int iterationMode);

    CellProvider() = delete;
    CellProvider(CellProvider const&) = delete;
    CellProvider& operator=(CellProvider const&) = delete;

    /** @return true iff no more cells */
    bool end() const;

    /**
     * Get next sequential cell and its position.
     *
     * @return false iff end() encountered
     * @note Input parameters are left as-is if end() encountered.
     */
    bool next(Cell& c, Coordinates& where);

    /**
     * Reposition to beginning of input array.
     * @note Repositions backend RegionProvider too.
     */
    void restart();

private:
    using Iters = std::vector<std::shared_ptr<ConstChunkIterator>>;
    Iters _inIters;
    Iters& _getInIters();
    void _makeIters();
    bool _end();

    RegionProvider& _source;
    bool _first { true };
    int _mode;

    Region _region;
    Coordinates _regPos;
};

} // namespace

#endif /* ! CELL_PROVIDER_H */
