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

/**
 * @file Metadata.cpp
 *
 * @brief Free Functions for working with Array/Attribute/Dimension Descs.
 *
 */

#include <array/Metadata.h>

#include <array/ArrayDesc.h>
#include <query/TypeSystem.h>
#include <system/Exceptions.h>

namespace scidb
{

size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high)
{
    size_t M = size_t(-1);
    size_t ret = 1;
    assert(low.size()==high.size());
    for (size_t i=0; i<low.size(); ++i) {
        assert(high[i] >= low[i]);
        size_t interval = high[i] - low[i] + 1;
        if (M/ret < interval) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        ret *= interval;
    }
    return ret;
}

Coordinates computeFirstChunkPosition(Coordinates const& chunkPos,
                                             Dimensions const& dims,
                                             bool withOverlap)
{
    assert(chunkPos.size() == dims.size());
    if (!withOverlap) {
        return chunkPos;
    }

    Coordinates firstPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        firstPos[i] -= dims[i].getChunkOverlap();
        if (firstPos[i] < dims[i].getStartMin()) {
            firstPos[i] = dims[i].getStartMin();
        }
    }
    return firstPos;
}

Coordinates computeLastChunkPosition(Coordinates const& chunkPos,
                                            Dimensions const& dims,
                                            bool withOverlap)
{
    assert(chunkPos.size() == dims.size());

    Coordinates lastPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        lastPos[i] += dims[i].getChunkInterval()-1;
        if (withOverlap) {
            lastPos[i] += dims[i].getChunkOverlap();
        }
        if (lastPos[i] > dims[i].getEndMax()) {
            lastPos[i] = dims[i].getEndMax();
        }
    }
    return lastPos;
}

size_t getChunkNumberOfElements(Coordinates const& chunkPos,
                                       Dimensions const& dims,
                                       bool withOverlap)
{
    Coordinates lo(computeFirstChunkPosition(chunkPos,dims,withOverlap));
    Coordinates hi(computeLastChunkPosition (chunkPos,dims,withOverlap));
    return getChunkNumberOfElements(lo,hi);
}

int64_t getChunkVolume(Dimensions const& dims)
{
    int64_t result = 1;
    int64_t last = 1;
    for (auto const& d : dims) {
        int64_t ci = d.getRawChunkInterval();
        assert(ci != DimensionDesc::UNINITIALIZED);
        if (ci < 0) {
            // AUTOCHUNKED, PASSTHRU, or otherwise inscrutable.
            return ci;
        }
        result *= ci;
        if (result < last) {
            // Overflow.
            return -1;
        }
        last = result;
    }
    return result;
}

} // namespace
