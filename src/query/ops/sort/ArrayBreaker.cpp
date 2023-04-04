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

/*
 * ArrayBreaker.cpp
 *
 *  Created on: Nov 25, 2014
 *      Author: Donghui Zhang
 */

#include "ArrayBreaker.h"

#include <array/ArrayIterator.h>
#include <array/Chunk.h>

#include <util/SchemaUtils.h>
#include <array/Metadata.h> // For computeLastChunkPosition

using namespace std;

namespace scidb
{
typedef vector<std::shared_ptr<ArrayIterator> > ArrayIterators;
typedef vector<std::shared_ptr<ChunkIterator> > ChunkIterators;

void breakOneArrayIntoMultiple(
        std::shared_ptr<Array> const& inputArray,
        std::vector<std::shared_ptr<Array> >& outputArrays,
        std::shared_ptr<Query>& query,
        BreakerOnCoordinates breaker,
        bool isBreakerConsecutive,
        void* additionalInfo)
{
    SchemaUtils schemaUtils(inputArray);
    const auto& iaAttrs = schemaUtils._attrsWithET;
    size_t nOutputArrays = outputArrays.size();

    // Get nAttrs input array iterators.
    vector<std::shared_ptr<ConstArrayIterator> > inputArrayIterators(iaAttrs.size());
    for (const auto& attr : iaAttrs) {
        inputArrayIterators[attr.getId()] = inputArray->getConstIterator(attr);
    }

    // For each output array, get nAttrs output array iterators.
    vector<ArrayIterators> outputArrayIterators(nOutputArrays);
    for (size_t i=0; i<nOutputArrays; ++i) {
        outputArrayIterators[i].resize(schemaUtils._nAttrsWithET);
        for (const auto& attr : iaAttrs)
        {
            outputArrayIterators[i][attr.getId()] = outputArrays[i]->getIterator(attr);
        }
    }

    // This is the result of the call to the breaker() function, telling which instance a cell should be sent to.
    // The value is sent back to the next call to the breaker() function, as a hint where to start searching.
    size_t which = 0;

    // Iterate through the chunks in the input array, using attribute 0.
    const auto& fda = iaAttrs.firstDataAttribute();
    while (!inputArrayIterators[fda.getId()]->end())
    {
        // The chunk position.
        Coordinates const& chunkPos = inputArrayIterators[fda.getId()]->getPosition();

        // Shortcut: if the whole chunk belongs to one instance, copy the chunk.
        const bool withOverlap = false;
        Coordinates lastPosInChunk = computeLastChunkPosition(chunkPos, inputArray->getArrayDesc().getDimensions(), withOverlap);
        const size_t instForFirstCell = breaker(chunkPos, which, query, schemaUtils._dims, additionalInfo);
        const size_t instForLastCell = breaker(lastPosInChunk, instForFirstCell, query, schemaUtils._dims, additionalInfo);

        if (isBreakerConsecutive && instForFirstCell == instForLastCell) {
            which = instForLastCell;
            for (const auto& attr : iaAttrs)
            {
                std::shared_ptr<ArrayIterator> dst = outputArrayIterators[which][attr.getId()];
                std::shared_ptr<ConstArrayIterator> src = inputArrayIterators[attr.getId()];
                dst->copyChunk(src->getChunk());
                ++(*src);
            }
            continue;
        }

        // Input chunk iterators.
        vector<std::shared_ptr<ConstChunkIterator> > inputChunkIterators(iaAttrs.size());
        for (const auto& attr : iaAttrs) {
            inputChunkIterators[attr.getId()] =
                inputArrayIterators[attr.getId()]->getChunk().getConstIterator();
        }

        // Declare output chunk iterators, but don't assign values yet. They will be assigned lazily.
        vector<ChunkIterators> outputChunkIterators(nOutputArrays);
        for (size_t i=0; i<nOutputArrays; ++i) {
            outputChunkIterators[i].resize(iaAttrs.size());
        }

        // Iterate through the cell positions in the chunk.
        while (! inputChunkIterators[fda.getId()]->end()) {
            Coordinates const& cellPos = inputChunkIterators[fda.getId()]->getPosition();
            which = breaker(cellPos, which, query, schemaUtils._dims, additionalInfo);
            assert(which < nOutputArrays);

            // Make sure the output array's chunk iterators exist. If not, create new chunks first!

            if (! outputChunkIterators[which][fda.getId()]) {
                for (const auto& attr : iaAttrs) {
                    outputChunkIterators[which][attr.getId()] =
                        outputArrayIterators[which][attr.getId()]->newChunk(chunkPos)
                            .getIterator(query, ConstChunkIterator::SEQUENTIAL_WRITE|ConstChunkIterator::NO_EMPTY_CHECK);
                }
            }

            // Copy the items.
            for (const auto& attr : iaAttrs) {
                outputChunkIterators[which][attr.getId()]->setPosition(cellPos);
                outputChunkIterators[which][attr.getId()]->writeItem(inputChunkIterators[attr.getId()]->getItem());
            }

            // Advance to the next cell.
            for (const auto& attr : iaAttrs) {
                ++(*inputChunkIterators[attr.getId()]);
            }
        }

        // Flush all chunks.
        for (size_t i=0; i<nOutputArrays; ++i) {
            if (!outputChunkIterators[i][fda.getId()]) {
                continue;
            }

            for (const auto& attr : iaAttrs) {
                outputChunkIterators[i][attr.getId()]->flush();
                outputChunkIterators[i][attr.getId()].reset();
            }
        }

        // Advance to the next chunk in the input array.
        for (const auto& attr : iaAttrs) {
            ++(*inputArrayIterators[attr.getId()]);
        }
    } // while (! inputArrayIterators[0]->end())
}

} // namespace scidb
