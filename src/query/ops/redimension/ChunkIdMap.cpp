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

#include "ChunkIdMap.h"

#include <boost/tuple/tuple.hpp>
#include <array/MemChunk.h>
#include <system/Config.h>
#include <util/OverlappingChunksIterator.h>

using namespace std;

namespace scidb
{
using namespace arena;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.RedimensionCommon.ChunkIdMap"));

IndirectChunkIdMap::IndirectChunkIdMap(
    const ArenaPtr& parentArena,
    size_t rank) :
    ChunkIdMap(rank),
    _rbtree(newArena(Options("ChunkIdMap.rbtree").scoped(parentArena).threading(0))),
    _coords(newArena(Options("ChunkIdMap.coords").scoped(parentArena).threading(0))),
    _chunkOverhead(MemChunk::getFootprint(rank) + sizeof(Address)),
    _chunkOverheadLimit(Config::getInstance()->getOption<size_t>(CONFIG_REDIM_CHUNK_OVERHEAD_LIMIT)),
    _posToId(_rbtree,CoordinatePtrLess(rank)),
    _idToPos(parentArena)
{
    assert(!_chunkOverheadLimit || _chunkOverhead<_chunkOverheadLimit*MiB);
}

/* No need to search the map twice.  Try to insert the initial element.
   If it is already there, insert will tell us and provide an iterator
   to the existing element. The reverse mapping will be set up later with an
   explicit call to reverse().
 */
size_t
IndirectChunkIdMap::mapChunkPosToId(CoordinateCRange pos)
{
    assert(getDirection() == PosToId);                   // Not yet 'reversed'
    assert(pos.size() == rank());                        // Validate argument

    PosToIdMap::iterator i;                              // Insertion pointer
    bool                 b;                              // Insert succeeded?

 /* Try adding the pair <pos,size()> to the first map...*/

    boost::tie(i,b) = _posToId.insert(make_pair(pos.begin(),_posToId.size()));

    if (b)                                               // Inserted new entry?
    {
        if (_chunkOverheadLimit!=0 && size()*_chunkOverhead > _chunkOverheadLimit*MiB)
        {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR,SCIDB_LE_TOO_MANY_CHUNKS_IN_REDIMENSION)
                << size() << _chunkOverhead << _chunkOverheadLimit;
        }

     /* Copy 'pos' onto the coordinate arena...*/

        Coordinate* const p = newVector<Coordinate>(*_coords,rank(),manual);

        std::copy(pos.begin(),pos.end(),p);

     /* Update the map key to point at this new local copy...*/

        const_cast<coords&>(i->first) = p;
    }

    return i->second;                                    // The chunk number
}

CoordinateCRange
IndirectChunkIdMap::mapIdToChunkPos(size_t i)
{
    assert(getDirection() == IdToPos);                   // Running in reverse

    return pointerRange(rank(),_idToPos[i]);              // Consult the vector
}

/**
 *  Now that all of the chunk positions have been seen and recorded, construct
 *  the reverse mapping id=>pos in _idToPos in one single go, then discard the
 *  contents of the forward mapping pos=>id, which are no longer needed.  This
 *  is slightly more efficient than updating the reverse mapping as we go...
 */
void
IndirectChunkIdMap::reverse()
{
    assert(getDirection() == PosToId);                   // Not yet reversed
    assert(_idToPos.empty());                            // Not yet populated

    _idToPos.resize(_posToId.size());                    // Resize the vector

    for (PosToIdMap::value_type& kv : _posToId)          // For each <pos,id>
    {
        _idToPos[kv.second] = kv.first;                  // ...update vector
    }

    _posToId.clear();                                    // No longer needed
    _rbtree->reset();                                    // Free node memory

    ChunkIdMap::reverse();                              // Switch direction
}

/**
 *  Clear both maps (though only actually has anything in it) and flush their
 *  arenas.
 */
void
IndirectChunkIdMap::clear()
{
    _posToId.clear();
    _idToPos.clear();
    _rbtree->reset();
    _coords->reset();
}

/****************************************************************************/

/* Factory function which returns a heap-allocated ChunkIdMap object
   of the appropriate subtype for the given schema.
 */
std::shared_ptr<ChunkIdMap>
createChunkIdMap(Dimensions const& dimensions, ArenaPtr& arenaPtr)
{
    std::shared_ptr<ChunkIdMap> result;
    bool useDirect = true;

    /* For each dimension, determine whether a min and max value are
       provided.  If not, we need an IndirectChunkIdMap.
    */
    PointerRange<DimensionDesc const> dims = dimensions;

    for (size_t i = 0; i < dims.size(); ++i)
    {
        if (dims[i].isMaxStar())
        {
            useDirect = false;
            break;
        }
    }

    /* Using the chunk sizes of each dimension, determine the total
       number of chunks in the logical coordinate space.  If it is
       greater than 64 bits, we need an IndirectChunkIdMap.
     */
    size_t totalChunks = 1;
    for (size_t i = 0; i < dims.size(); ++i)
    {
        assert(dims[i].getLength());
        size_t chunksInDim =
            ((dims[i].getLength() - 1) / dims[i].getChunkInterval()) + 1;
        size_t newTotalChunks = totalChunks * chunksInDim;
        if (newTotalChunks / totalChunks != chunksInDim)
        {
            useDirect = false;
            break;
        }
        totalChunks = newTotalChunks;
    }

    /* Return the appropriate sub-type of ChunkIdMap
     */
    if (useDirect)
    {
        LOG4CXX_DEBUG(logger, "Creating direct chunk-id-map.");
        result = std::make_shared<DirectChunkIdMap>(arenaPtr, dims);
    }
    else
    {
        LOG4CXX_DEBUG(logger, "Creating indirect chunk-id-map.");
        result = std::make_shared<IndirectChunkIdMap>(arenaPtr, dims.size());
    }

    return result;
}

} // namespace scidb
