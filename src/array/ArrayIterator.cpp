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
 * @file ArrayIterator.cpp
 *
 * @brief class ArrayIterator
 */

#include <array/ArrayIterator.h>

#include <log4cxx/logger.h>

#include <array/Chunk.h>
#include <array/PinBuffer.h>
#include <array/ArrayDesc.h>

#include <query/Query.h>
#include <system/Exceptions.h>
#include <util/SpatialType.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ArrayIterator"));

    void ArrayIterator::deleteChunk(Chunk& chunk)
    {
        assert(false);
    }
    Chunk& ArrayIterator::updateChunk()
    {
        ConstChunk const& constChunk = getChunk();
        if (constChunk.isReadOnly()) {
            throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_CANT_UPDATE_READ_ONLY_CHUNK);
        }
        Chunk& chunk = const_cast<Chunk&>(dynamic_cast<const Chunk&>(constChunk));
        chunk.pin();
        return chunk;
    }

    Chunk& ArrayIterator::copyChunk(ConstChunk const& chunk,
                                    std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                                    Coordinates* chunkStart,
                                    Coordinates* chunkEnd)
    {
        const Coordinates& firstPos = chunk.getFirstPosition(false/*no overlap*/);
        const Coordinates& lastPos  = chunk.getLastPosition(false/*no overlap*/);
        Chunk& outChunk    = newChunk(firstPos);
        SpatialRange chunkDataBounds( firstPos, lastPos );
        bool hasOverlap    = chunk.getArrayDesc().hasOverlap();
        bool collectBounds = ( nullptr != chunkStart && nullptr != chunkEnd );

        //verify that the declared chunk intervals match. Otherwise the copy - could still work - but would either be an implicit reshape or outright dangerous
        SCIDB_ASSERT(chunk.getArrayDesc().getDimensions().size() == outChunk.getArrayDesc().getDimensions().size());

        for(size_t i = 0, n = chunk.getArrayDesc().getDimensions().size(); i < n; i++)
        {
            SCIDB_ASSERT(chunk.getArrayDesc().getDimensions()[i].getChunkInterval() == outChunk.getArrayDesc().getDimensions()[i].getChunkInterval());
        }

        std::shared_ptr<ChunkIterator> dst;
        try {
            std::shared_ptr<Query> query(getQuery());

            //
            // If copying from an emptyable array to an non-emptyable array, we need to fill in the default values.
            // TODO: Purge SciDB of references to non-emptyable arrays.
            size_t nAttrsChunk    = chunk.getArrayDesc().getAttributes().size();
            size_t nAttrsOutChunk = outChunk.getArrayDesc().getAttributes().size();

            SCIDB_ASSERT( nAttrsChunk >= nAttrsOutChunk );
            SCIDB_ASSERT( nAttrsOutChunk+1 >= nAttrsChunk );

            bool emptyableToNonEmptyable = (nAttrsOutChunk+1 == nAttrsChunk);
            if (chunk.isMaterialized()
                && chunk.getArrayDesc().hasOverlap() == outChunk.getArrayDesc().hasOverlap()
                && chunk.getAttributeDesc().isNullable() == outChunk.getAttributeDesc().isNullable()
                // if emptyableToNonEmptyable, we cannot use memcpy because we need to insert defaultvalues
                && !emptyableToNonEmptyable
                && chunk.getNumberOfElements(true) == outChunk.getNumberOfElements(true)
                //
                // Can't use the memcpy(...) if an additional goal of the
                // copyChunk is to collect information about the range of
                // cells in the chunk.
                && !collectBounds
               )
            {
                PinBuffer scope(chunk);
                if (emptyBitmap && chunk.getBitmapSize() == 0) {
                    size_t size = chunk.getSize() + emptyBitmap->packedSize();
                    outChunk.allocate(size);
                    chunk.cloneConstData(outChunk.getWriteData());
                    emptyBitmap->pack((char*)outChunk.getWriteData() + chunk.getSize());
                } else {
                    size_t size = emptyBitmap ? chunk.getSize() : chunk.getSize() - chunk.getBitmapSize();
                    outChunk.allocate(size);
                    chunk.cloneConstData(outChunk.getWriteData(), size);
                }
                outChunk.setCount(chunk.isCountKnown() ? chunk.count() : 0);
                outChunk.write(query);
            } else {
                //
                //  If we want to track information about the range of cells
                //  in the chunk, then we need to initialize the variables
                //  passed-by-reference appropriately.
                if (collectBounds) {
                    setCoordsMax ( *chunkStart );
                    setCoordsMin ( *chunkEnd );
                }

                if ( emptyBitmap && !collectBounds )
                {
                    //
                    // If this chunk is part of a coordinates list, and we're
                    // not being asked to figure out the MBR, then do the copy
                    // with a single step ...
                    chunk.makeClosure(outChunk, emptyBitmap);
                    outChunk.write(query);
                } else {
                    //
                    // ... otherwise, we need to step through the chunk's
                    // contents, one cell at a time.
                    std::shared_ptr<ConstChunkIterator> src =
                        chunk.getConstIterator(ChunkIterator::INTENDED_TILE_MODE
                                               | (outChunk.getArrayDesc().hasOverlap()
                                                  ? 0
                                                  : ChunkIterator::IGNORE_OVERLAPS));
                    dst = outChunk.getIterator(query,
                                               ((src->getMode() & ChunkIterator::TILE_MODE)
                                                | ChunkIterator::NO_EMPTY_CHECK
                                                | ChunkIterator::SEQUENTIAL_WRITE));
                    size_t count = 0;
                    while (!src->end()) {
                        if (!emptyableToNonEmptyable) {
                            count += 1;
                        }
                        Coordinates const& srcCoords = src->getPosition();

                        //
                        //   The note that we don't want to evaluate the
                        //  chunkDataBounds.contains() if we can afford it,
                        //  and we need to only if the input chunk has an
                        //  overlap.
                        if (collectBounds &&
                            (!hasOverlap || chunkDataBounds.contains(srcCoords))) {
                            //
                            // Track the size of the MBR enclosing the chunk's cells.
                            resetCoordMin ( (*chunkStart), srcCoords );
                            resetCoordMax ( (*chunkEnd),   srcCoords );
                        }
                        dst->setPosition(srcCoords);
                        dst->writeItem(src->getItem());
                        ++(*src);
                    }
                    if (!(src->getMode() & ChunkIterator::TILE_MODE) &&
                        !chunk.getArrayDesc().hasOverlap()) {
                        if (emptyableToNonEmptyable) {
                            count = outChunk.getNumberOfElements(false); // false = no overlap
                        }
                        outChunk.setCount(count);
                    }
                    dst->flush();
                    dst->unpin();
                }
            }
        } catch (...) {
            if (dst) {
                dst->unpin();
            }
            deleteChunk(outChunk);
            throw;
        }
        return outChunk;
    }

}
