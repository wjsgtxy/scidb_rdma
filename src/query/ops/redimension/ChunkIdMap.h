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
 * @file ChunkIdMap.h
 * @see https://trac.scidb.net/wiki/Development/components/Rearrange_Ops/RedimensionChunkPosToId
 */

#ifndef CHUNK_ID_MAP_H
#define CHUNK_ID_MAP_H

#include <util/Arena.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>

#include <array/Coordinate.h>
#include <array/Dimensions.h>
#include <array/DimensionDesc.h>
#include <boost/noncopyable.hpp>

namespace scidb {

class ArrayDesc;

/**
 *  Implements a bijection between chunk position (represented as a
 *  coordinate vector of the given rank) and its chunk identifier (a zero
 *  based index).  This is an abstract base class.
 */
class ChunkIdMap : boost::noncopyable
{
public:
    enum direction {PosToId, IdToPos};

public:  // Construction
            ChunkIdMap(size_t rank) :
                _rank(rank),
                _direction(PosToId)
                {}
    virtual ~ChunkIdMap()
                {}

public:  // Operations
            size_t            rank() const
                                  {return _rank;}
            direction         getDirection() const
                                  {return _direction;}
    virtual void              reverse()
                                  {_direction = IdToPos;}
    virtual void              clear()
                                  {}

    virtual size_t            mapChunkPosToId(CoordinateCRange) = 0;
    virtual CoordinateCRange  mapIdToChunkPos(size_t) = 0;
    virtual size_t            getUnusedId() const = 0;

private: // Representation
    size_t    const _rank;                  // Length of chunk pos
    direction       _direction;             // Mapping direction
};


/**
 * Implements the mapping with a simple stateless calculation based
 * on the chunk's position in row-major order in the logcial chunk space.
 * Due to the nature of the api, mapping from id back to pos returns
 * a const pointer range which points to an internally allocated
 * coords structure.  Each subsequent call to mapIdToChunkPos
 * clobbers the result of the previous call.
 */
class DirectChunkIdMap : public ChunkIdMap
{
public:  // Construction
    DirectChunkIdMap(const arena::ArenaPtr& a,
                      PointerRange<DimensionDesc const> dims) :
        ChunkIdMap(dims.size()),
        _lows(a, dims.size(), 0),
        _highs(a, dims.size(), 0),
        _chunksz(a, dims.size(), 0),
        _intervals(a, dims.size(), 0),
        _mapres(a, dims.size(), 0)
        {
            for (size_t i = 0; i < dims.size(); ++i)
            {
                _lows[i] = dims[i].getStartMin();
                _highs[i] = dims[i].getEndMax();
                _chunksz[i] = dims[i].getChunkInterval();
                _intervals[i] = ((_highs[i] - _lows[i]) / _chunksz[i]) + 1;
            }
        }

public:  // Operations
    size_t mapChunkPosToId(CoordinateCRange cpos)
        {
            size_t id = 0;
            size_t chunkOffset = 0;
            for (size_t i = 0; i < rank(); ++i)
            {
                chunkOffset = (cpos[i] - _lows[i]) / _chunksz[i];
                id *= _intervals[i];
                id += chunkOffset;
            }
            return id;
        }

    /* Note:  each call to this method clobbers the result of the previous
       call to this method.
     */
    CoordinateCRange mapIdToChunkPos(size_t id)
        {
            for (size_t i = rank(); i > 0;)
            {
                --i;
                _mapres[i] = (id % _intervals[i]) * _chunksz[i] +
                    _lows[i];
                id /= _intervals[i];
            }
            return _mapres;
        }

    size_t getUnusedId() const
        {
            return std::numeric_limits<size_t>::max();
        }

private: // Representation

    mgd::vector<Coordinate> _lows;      // lower bound for each dimension
    mgd::vector<Coordinate> _highs;     // upper bound for each dimension
    mgd::vector<Coordinate> _chunksz;   // chunk size for each dimension
    mgd::vector<Coordinate> _intervals; // chunk interval in each dimension
    mgd::vector<Coordinate> _mapres;    // stores result of reverse mapping
};


/**
 *  Implements chunk-id mapping using a standard map for the forward direction
 *  and a vector for the backward direction.
 *
 *  Observe that:
 *
 *  a) items are not removed until the entire bijection is discarded,
 *
 *  b) all entries therefore have the same lifetime,
 *
 *  c) all chunk position coordinate vectors have the same length (i.e. rank),
 *
 *  d) all chunk positions are seen and recorded before any are found by id,
 *
 *  e) once we begin searching for chunk positions by their id, we no longer
 *  need to look up their id's up by chunk position.
 *
 *  Thus:
 *
 *  b) justifies the use of a scoped arena,
 *
 *  c) justifies the use of raw coordinate arrays rather than (more expensive)
 *  Coordinates objects,
 *
 *  d + e) allow us to operate in a modal 'direction', only consuming resources
 *  for one mapping at a time.
 */
class IndirectChunkIdMap : public ChunkIdMap
{
 public:  // Construction
    IndirectChunkIdMap(const arena::ArenaPtr&,size_t rank);
    ~IndirectChunkIdMap()
        {clear();}

 public:               // Operations
    size_t            size()               const
        {return getDirection()==PosToId ?
                _posToId.size() : _idToPos.size();}
    size_t            getUnusedId()        const
        {return size();}
    CoordinateCRange  mapIdToChunkPos(size_t);
    size_t            mapChunkPosToId(CoordinateCRange);
    void              reverse();
    void              clear();

 private:               // Representation
    typedef Coordinate const*                           coords;
    typedef mgd::map<coords,size_t,CoordinatePtrLess>   PosToIdMap;
    typedef mgd::vector<coords>                         IdToPosMap;

 private:               // Representation
    arena::ArenaPtr    const _rbtree;                   // Tree node allocator
    arena::ArenaPtr    const _coords;                   // Coordinate allocator
    size_t      const _chunkOverhead;            //
    size_t      const _chunkOverheadLimit;       //
    PosToIdMap        _posToId;                  // Maps coords to ids
    IdToPosMap        _idToPos;                  // Maps ids to coords
};

/* Factory function which returns a heap-allocated ChunkIdMap object
   of the appropriate subtype for the given schema dimensions.
 */
std::shared_ptr<ChunkIdMap> createChunkIdMap(Dimensions const&, arena::ArenaPtr&);

} //namespace scidb

#endif /* CHUNK_ID_MAP_H */
