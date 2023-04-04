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
 * SyntheticDimChunkMerger.h
 *
 *  This file contains helper routines for merging partial chunks with a synthetic dimension.
 */

#ifndef SYNTHETIC_DIM_CHUNK_MERGER_H_
#define SYNTHETIC_DIM_CHUNK_MERGER_H_

#include <vector>
#include <unordered_map>

#include <array/MemChunk.h>
#include <array/MultiStreamArray.h>
#include <array/StreamArray.h>

namespace scidb
{
/**
 * A partial chunk merger which adjusts the synthetic dimension coordinate for incoming partial chunks
 * Each new cell gets the synthetic dimension coordiante equal to the current count of cells
 * with the same non-synthetic dimension coordinates.
 * The partial chunks are merged in the order of instance of origin IDs.
 */
class SyntheticDimChunkMerger : public MultiStreamArray::PartialChunkMerger
{
private:

    /// Helper class to keep track of the current count of cells with the same non-synthetic coordinates
    class SyntheticDimAdjuster
    {
    private:
        typedef std::unordered_map<Coordinates, size_t, CoordinatesHash> MapCoordToCount;

        /// Maps "collapsed" coordinates to count of collisions
        /// Collapsed coordinates have the synthetic dimension set to the same value, the dimension start
        MapCoordToCount _coord2Count;

        /**
         * Which dimension is the synthetic one.
         */
        size_t _dimSynthetic;

        /**
         * What is the dimStart of the synthetic dim.
         */
        Coordinate _dimStartSynthetic;

        Coordinates _collapsed;

    public:
        void clear()
        {
            _coord2Count.clear();
        }

        SyntheticDimAdjuster(size_t dimSynthetic, Coordinate dimStartSynthetic);
        ~SyntheticDimAdjuster() {}

        /**
         * Get a coordinates where the synthetic dim is 'collapsed', i.e. uses _dimStartSynthetic.
         */
        void useStartForSyntheticDim(Coordinates& coord)
        {
            coord[_dimSynthetic] = _dimStartSynthetic;
        }

        /**
         * Increase the synthetic dim's coordinate by an offset.
         */
        void increaseSyntheticDim(Coordinates& coord, size_t offset)
        {
            coord[_dimSynthetic] += offset;
        }

        /**
         * Update the count in a MapCoordToCount from a chunk
         * @param chunk from which the count should be aquired
         * @param chunkIter an optional cached iterator for the chunk
         */
        void updateMapCoordToCount(MemChunk const* chunk,
                                   ConstChunkIterator* chunkIter=NULL);

        /// Update the current count for a given positon
        void updateCount(const Coordinates& coords);

        /**
         * Calculate a new coordinates, by offseting what the map stores.
         * @param coord [in/out] the coordinates in which the synthetic dim should be increased
         */
        void calcNewCoord(Coordinates& coord);
    };

    SyntheticDimAdjuster _syntheticDimHelper;
    std::vector<std::shared_ptr<MemChunk> > _partialChunks;
    Coordinates _currChunkPos;
    Coordinates _coord;

public:

    struct RedimInfo {
        /**
         * Whether there is a synthetic dimension.
         */
        bool _hasSynthetic;

        /**
         * Which dimension is the synthetic one.
         */
        AttributeID _dimSynthetic;

        /**
         * A copy of the synthetic dim description
         */
        DimensionDesc _dim;

        RedimInfo(bool hasSynthetic, AttributeID dimSynthetic, DimensionDesc const& dim):
        _hasSynthetic(hasSynthetic), _dimSynthetic(dimSynthetic), _dim(dim)
        {}
    };

    /**
     * Constructor
     * @param redimInfo information about the synthetic dimension
     * @param numInstances number of instance
     */
    SyntheticDimChunkMerger(const RedimInfo* redimInfo, size_t numInstances);

    /// Destructor
    virtual ~SyntheticDimChunkMerger() {}

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
    bool
    mergePartialChunk(size_t stream,
                      AttributeID attId,
                      std::shared_ptr<MemChunk>& chunk,
                      const std::shared_ptr<Query>& query);

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunks
    void mergePartialChunks(AttributeID attId,
                            std::vector<std::shared_ptr<MemChunk>> partialChunks,
                            std::vector<size_t> streams,
                            std::shared_ptr<Query> const& query);

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    std::shared_ptr<MemChunk>
    getMergedChunk(AttributeID attId,
                   const std::shared_ptr<Query>& query);

private:
    void
    mergeChunks(std::shared_ptr<ChunkIterator>& dstIterator,
                std::shared_ptr<MemChunk>& src);
    void clear();
};

}
#endif
