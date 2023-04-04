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
 * @file Metadata.h
 *
 * @brief Structures for fetching and updating metadata of cluster.
 *
 */

#ifndef METADATA_H_
#define METADATA_H_

#include <array/Attributes.h>
#include <array/Coordinate.h>
#include <array/Dimensions.h>

namespace scidb
{

class ArrayDesc;

extern Coordinates computeFirstChunkPosition(Coordinates const& chunkPos,
                                             Dimensions const& dims,
                                             bool withOverlap = true);


/**
 * Compute the last position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the last chunk position
 */
extern Coordinates computeLastChunkPosition(Coordinates const& chunkPos,
                                            Dimensions const& dims,
                                            bool withOverlap = true);

/**
 * Get the logical space size of a chunk.
 * @param[in]  low   the low position of the chunk
 * @param[in]  high  the high position of the chunk
 * @return     #cells in the space that the chunk covers
 * @throw      SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE)
 */
size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high);

/**
 * Get the logical space size of a chunk.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return     #cells in the space the cell covers
 */
extern size_t getChunkNumberOfElements(Coordinates const& chunkPos,
                                       Dimensions const& dims,
                                       bool withOverlap = true);

/**
 * Compute product of the chunk sizes of each dimension.
 *
 * @param[in] dims   The dimensions.
 * @return volume of chunk in # of cells, or -1
 *
 * @details Return the product of the chunk intervals of the
 * dimensions, or -1 on overflow or on encountering an autochunked
 * dimension.  To distinguish between overflow and autochunking, call
 * isAutochunked().  This should compute the same value as a call to
 * ConstChunk::getNumberOfElements(NO_OVERLAP), but doesn't need a
 * chunk.
 *
 * @p (Why does getChunkNumberOfElements() above need a chunkPos
 * parameter?  I don't get it.)
 *
 * @note Does not account for overlap (but could be made to do so).
 */
int64_t getChunkVolume(Dimensions const& dims);

} // namespace

#endif /* METADATA_H_ */
