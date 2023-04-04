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
 * @file Remapper.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef REMAPPER_H
#define REMAPPER_H

#include <util/Arena.h>
#include <util/ArrayCoordinatesMapper.h>

namespace scidb {

class ChunkIdMap;

/**
 * A functor for mapping provisional (pos,id) pairs to final (pos,id) pairs.
 *
 * @description
 * Here we bundle up the various ChunkIdMaps and ArrayCoordinatesMappers required to translate
 * between autochunking redimension()'s provisional and final chunking schemes.  The @c operator()
 * method does the entire calculation, as used by RemapperArray.  However, the calculation is broken
 * up into pieces for the benefit of OverlapRemapperArray, which supplies its own beginning-of-chunk
 * coordinates.
 */
class Remapper
{
public:
    Remapper(ArrayCoordinatesMapper& provisionalCoordMapper,
             std::shared_ptr<ChunkIdMap> provisionalChunkIdMap,
             Dimensions const& finalDims,
             arena::ArenaPtr& arena)
        : _provCoordMapper(provisionalCoordMapper)
        , _provChunkIdMap(provisionalChunkIdMap)
        , _finalDims(finalDims)
        , _finalCoordMapper(finalDims)
        , _finalChunkIdMap(createChunkIdMap(finalDims, arena))
        , _cellCoords(finalDims.size())
        , _finalChunkCoords(finalDims.size())
    {
        _provChunkIdMap->reverse();
    }

    /**
     * Partial translation: provisional (pos, id) to full cell coordinates.
     */
    Coordinates& provPosIdToCoords(Value const& posIn, Value const& idIn)
    {
        // Provisional chunk id to chunk coordinates.
        _provChunkCRange = _provChunkIdMap->mapIdToChunkPos(idIn.getInt64());

        // (Chunk coords, provisional cell position) to cell coordinates.
        _provCoordMapper.pos2coord(_provChunkCRange, posIn.getInt64(), _cellCoords);

        return _cellCoords;
    }

    /**
     * Full translation: provisional (pos, id) pair to final (pos, id) pair.
     */
    void operator()(Value const& posIn, Value const& idIn, Value& posOut, Value& idOut)
    {
        provPosIdToCoords(posIn, idIn); // Sets _cellCoords.

        // Cell's chunk coordinates will be different between final and provisional schemas.
        _finalChunkCoords = _cellCoords;
        ArrayDesc::getChunkPositionFor(_finalDims, _finalChunkCoords);

        chunkAndCellToPosId(_finalChunkCoords, _cellCoords, posOut, idOut);
    }

    /**
     * Partial translation: (chunkCoord, cellCoord) to final (pos, id) pair.
     */
    void chunkAndCellToPosId(Coordinates const& chunkCoords,
                             Coordinates const& cellCoords,
                             Value& posOut, Value& idOut)
    {
        // Chunk coordinates to final chunk id!
        idOut.setInt64(_finalChunkIdMap->mapChunkPosToId(chunkCoords));

        // Original cell coordinates to final cell position!!!
        posOut.setInt64(_finalCoordMapper.coord2pos(chunkCoords, cellCoords));
    }

    /// Get cell coordinates from last translation.
    Coordinates const& getCellCoordinates() const { return _cellCoords; }

    /// Get final dimensions we are remapping to.
    Dimensions const& getFinalDimensions() const { return _finalDims; }

    /// Extract ChunkIdMap built during the translation process.
    std::shared_ptr<ChunkIdMap> getFinalChunkIdMap() { return _finalChunkIdMap; }

private:
    ArrayCoordinatesMapper&     _provCoordMapper;
    std::shared_ptr<ChunkIdMap> _provChunkIdMap;
    Dimensions const&           _finalDims;
    ArrayCoordinatesMapper      _finalCoordMapper;
    std::shared_ptr<ChunkIdMap> _finalChunkIdMap; 
    CoordinateCRange            _provChunkCRange;
    Coordinates                 _cellCoords;
    Coordinates                 _finalChunkCoords;
};

} // namespace scidb

#endif /* ! REMAPPER_H */
