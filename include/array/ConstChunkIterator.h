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
 * @file ConstChunkIterator.h
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef CONST_CHUNK_ITERATOR_H_
#define CONST_CHUNK_ITERATOR_H_


#include <array/Coordinate.h>           // grr unfactored typedefs
#include <array/ConstIterator.h>        // base

#include <system/Exceptions.h>          // grr only because of inline implementation
#include <system/ErrorCodes.h>          // grr only because of inline implementation

namespace scidb
{
class BaseTile;
class ConstChunk;
class CoordinatesMapper;
class Value;

/**
 * Iterator over items in the chunk. The chunk consists of a number of Value entries with
 * positions in the coordinate space, as well as flags:
 *      NULL - the value is unknown
 *      core - the value is a core value managed by the current instance
 *      overlap - the value is an overlap value, it can only be used for computation, but
 *              its managed by some other site
 */
class ConstChunkIterator : public ConstIterator
{
public:
    /**
     * Constants used to specify iteration mode mask
     */
    enum IterationMode {
        /**
         * Default iteration mode (no flags set).
         */
        DEFAULT = 0,
        /**
         * Ignore components having null value
         */
        IGNORE_NULL_VALUES  = 1,
        /**
         * Unused mode
         */
        UNUSED_IGNORE_EMPTY_CELLS = 2,
        /**
         * Ignore overlaps
         */
        IGNORE_OVERLAPS = 4,
        /**
         * Do not check for empty cells event if there is empty attribute in array
         */
        NO_EMPTY_CHECK = 8,
        /**
         * When writing append empty bitmap to payload
         */
        APPEND_EMPTY_BITMAP = 16,
        /**
         * Append to the existed chunk
         */
        APPEND_CHUNK = 32,
        /**
         * Ignore default value in sparse array
         */
        IGNORE_DEFAULT_VALUES = 64,
        /**
         * Unused mode
         */
        UNUSED_VECTOR_MODE = 128,
        /**
         * Tile mode
         */
        TILE_MODE = 256,
        /**
         * Data is written in stride-major order
         */
        SEQUENTIAL_WRITE = 512,
        /**
         * Intended tile mode
         */
        INTENDED_TILE_MODE = 1024
    };

    /**
     * Get current iteration mode
     */
    virtual int getMode() const = 0;

    /**
     * Get current element value
     */
    virtual Value const& getItem() = 0;

    /**
     * Check if current array cell is empty (if iteration mode allows visiting of empty cells)
     */
    virtual bool isEmpty() const = 0;

#define COORD(i) ((uint64_t)1 << (i))     // TODO: seriously, a macro?
    /**
     * Move forward in the specified direction
     * @param direction bitmask of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) increments I coordinate, moveNext(COORD(1)) increments J coordinate and
     * moveNext(COORD(0)|COORD(1)) increments both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool forward(uint64_t direction = COORD(0));

    /**
     * Move backward in the specified direction
     * @param direction bitmask of of coordinates in which direction movement is performed,
     * for example in case of two dimensional matrix [I=1:10, J=1:100]
     * moveNext(COORD(0)) decrements I coordinate, moveNext(COORD(1)) decrements J coordinate and
     * moveNext(COORD(0)|COORD(1)) decrements both coordinates
     * @return false if movement in the specified direction is not possible
     */
    virtual bool backward(uint64_t direction = COORD(0));

    /**
     * Get iterated chunk
     */
    virtual ConstChunk const& getChunk() = 0;

    /**
     * Get first position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getFirstPosition();

    /**
     * Get last position in the iterated chunk according to the iteration mode
     */
    virtual Coordinates const& getLastPosition();

    /**
     * Return a tile of at most maxValues starting at the logicalStart coordinates.
     * The logical position is advanced by the size of the returned tile.
     * @parm offset - [IN] array coordinates of the first data element
     *                      [OUT] Coordinates object to which the return value refers
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @param tileCoords  - output tile of array coordinates, one for each element in the data tile
     * @return scidb::Coordinates() if no data are found at the logicalStart coordinates
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next set of array coordinates where data exist in row-major order.
     *         If the next position is at the end of the chunk, scidb::Coordinates() is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            std::shared_ptr<BaseTile>& tileData,
            std::shared_ptr<BaseTile>& tileCoords)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(const Coordinates)";
    }

    /**
     * Return a tile of at most maxValues starting at logicalStart.
     * The logical position is advanced by the size of the returned tile.
     * @parm logicalOffset - logical position (in row-major order) within a chunk of the first data element
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @param tileCoords  - output tile of logical position_t's, one for each element in the data tile
     * @return positon_t(-1) if no data is found at the logicalStart position
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next position where data exist in row-major order.
     *         If the next position is at the end of the chunk, positon_t(-1) is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            std::shared_ptr<BaseTile>& tileData,
            std::shared_ptr<BaseTile>& tileCoords)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(positon_t)";
    }

    /**
     * Return a tile of at most maxValues starting at the logicalStart coordinates.
     * The logical position is advanced by the size of the returned tile.
     * @parm offset - [IN] array coordinates of the first data element
     *                [OUT] Coordinates object to which the return value refers
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @return scidb::Coordinates() if no data are found at the logicalStart coordinates
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next set of array coordinates where data exist in row-major order.
     *         If the next position is at the end of the chunk, scidb::Coordinates() is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual const Coordinates&
    getData(scidb::Coordinates& offset,
            size_t maxValues,
            std::shared_ptr<BaseTile>& tileData)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(const Coordinates, data)";
    }

    /**
     * Return a tile of at most maxValues starting at logicalStart.
     * The logical position is advanced by the size of the returned tile.
     * @parm logicalOffset - logical position (in row-major order) within a chunk of the first data element
     * @param maxValues   - max number of values in a tile
     * @param tileData    - output data tile
     * @return positon_t(-1) if no data is found at the logicalStart position
     *         (either at the end of chunk or at a logical "whole" in the serialized data);
     *         otherwise, the next position where data exist in row-major order.
     *         If the next position is at the end of the chunk, positon_t(-1) is returned and
     *         the output variables may contain data/coordinates.
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t
    getData(position_t logicalOffset,
            size_t maxValues,
            std::shared_ptr<BaseTile>& tileData)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getData(positon_t,data)";
    }

    /**
     * @return a mapper capable of converting logical positions to/from array coordinates
     *         assuming row-major serialization order
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     * @example usage of this operator:
     *   std::shared_ptr<ConstChunkIterator> ci = ...;
     *   const CoordinatesMapper* cm = (*ci);
     */
    virtual operator const CoordinatesMapper* () const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::operator const CoordinatesMapper* () const";
    }

    /**
     * @return the iterator current logical position within a chunk
     *         assuming row-major serialization order
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    virtual position_t getLogicalPosition()
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::getLogicalPosition";
    }

    /**
     * Set the current iterator position corresponding to the logical position within a chunk
     *         assuming row-major serialization order
     * @return true if a cell with such position exist and its position is successfully recorded
     * @note a reference implementation is provided to avoid breaking (or rather fixing) all existing child implementations
     */
    using ConstIterator::setPosition;
    virtual bool setPosition(position_t pos)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << "ConstChunkIterator::setPosition";
    }
};

} // namespace
#endif /* CONST_CHUNK_ITERATOR_H_ */
