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
 * PhysicalBadReadWrite.cpp
 */

#include <array/Array.h>
#include <array/PinBuffer.h>
#include <array/RLE.h>
#include <array/TileIteratorAdaptors.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <util/MultiConstIterators.h>

#include <log4cxx/logger.h>

using namespace std;

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.test_badreadwrite"));
}

namespace scidb {

class PhysicalBadReadWrite : public PhysicalOperator
{
public:
    PhysicalBadReadWrite(const string& logicalName,
                         const string& physicalName,
                         const Parameters& parameters,
                         const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    shared_ptr<Array> execute(vector<shared_ptr<Array>>& inputArrays, shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() <= 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        try {
            BufferMgr::getInstance()->clearCache();
        } catch (const SystemException& e) {
            LOG4CXX_INFO(logger, "Unable to clear cache. Proceeding anyway.");
            // We should be able to clear the items we need.
            // If something is still pinned, then it shouldn't affect what we
            // need to test.
        }

        /* This parameter determines the width of the vertical slice that we use to scan
           Default is 1.
        */
        uint64_t attrStrideSize = (_parameters.size() == 1)
            ? ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])
                  ->getExpression()
                  ->evaluate()
                  .getUint64()
            : 1;

        shared_ptr<Array>& array = inputArrays[0];

        /* Scan through the array in vertical slices of width "attrStrideSize"
         */
        const auto& attrs = _schema.getAttributes(true);
        const size_t numRealAttrs = attrs.size();
        auto baseAttr = attrs.begin();

        attrStrideSize = std::max(attrStrideSize, uint64_t(1));
        attrStrideSize = std::min(attrStrideSize, numRealAttrs);

        while (baseAttr < attrs.end()) {
            /* Get iterators for this vertical slice
             */
            size_t sliceSize = std::min(numRealAttrs - baseAttr->getId(), attrStrideSize);
            vector<shared_ptr<ConstIterator>> arrayIters(sliceSize);

            auto sliceEnd = attrs.begin() + sliceSize;
            for (auto i = baseAttr; i < sliceEnd; ++i) {
                auto res = attrs.begin() + (i - baseAttr);
                arrayIters[res->getId()] =
                    array->getConstIterator(*i);
            }

            /* Scan each attribute one chunk at a time, use MultiConstIterator
               to handle possible gaps at each chunk position
            */
            MultiConstIterators multiIters(arrayIters);
            while (!multiIters.end()) {
                vector<size_t> minIds;

                /* Get list of all iters where current chunk position is not empty
                 */
                multiIters.getIDsAtMinPosition(minIds);
                for (size_t i = 0; i < minIds.size(); i++) {

                    shared_ptr<ConstArrayIterator> currentIter =
                        static_pointer_cast<ConstArrayIterator, ConstIterator>(arrayIters[minIds[i]]);
                    if (isDebug()) {
                        currentIter->getChunk();  // to catch some bugs like #3656
                    }
                    const ConstChunk& chunk = currentIter->getChunk();

                    {
                        // Open the Chunk for Write (after having opened it for READONLY above)
                        PinBuffer scope(chunk);
                        ConstRLEPayload payload(static_cast<const char*>(
                            const_cast<ConstChunk&>(chunk).getWriteData()));
                        SCIDB_ASSERT(payload.packedSize() > 0);
                    }

                    int configTileSize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
                    int iterMode = ConstChunkIterator::INTENDED_TILE_MODE;
                    shared_ptr<ConstChunkIterator> chunkIter =
                        chunk.getConstIterator(iterMode);
                    iterMode = chunkIter->getMode();

                    if ((iterMode & ConstChunkIterator::TILE_MODE) == 0) {
                        // new tile mode
                        if (chunkIter->end()) {
                            continue;
                        }
                        chunkIter =
                            make_shared<TileConstChunkIterator<shared_ptr<ConstChunkIterator>>>(chunkIter,
                                                                                                query);
                        assert(configTileSize > 0);
                        badreadwriteTiledChunk(chunkIter, size_t(configTileSize));
                        continue;
                    }

                    while (!chunkIter->end()) {
                        Value const& v = chunkIter->getItem();
                        ConstRLEPayload* tile = v.getTile();
                        if (tile) {
                            // old tile mode
                            ConstRLEPayload::iterator piter = tile->getIterator();
                            while (!piter.end()) {
                                Value tv;
                                piter.getItem(tv);
                                ++(piter);
                            }
                        } else {
                            chunkIter->getPosition();
                        }
                        // chunkIter->
                        ++(*chunkIter);
                    }
                }

                /* Advance to next chunk position
                 */
                ++multiIters;
            }

            /* Progress to the next vertical slice
             */
            baseAttr += sliceSize;
        }
        try {
            BufferMgr::getInstance()->clearCache();
        } catch (const SystemException& e) {
            LOG4CXX_INFO(logger, "Unable to clear cache. Proceeding anyway.");
            // We should be able to clear the items we need.
            // If something is still pinned, then it shouldn't affect what we
            // need to test.
        }
        return shared_ptr<Array>();
    }
    void badreadwriteTiledChunk(shared_ptr<ConstChunkIterator>& chunkIter, size_t tileSize)
    {
        ASSERT_EXCEPTION(!chunkIter->end(),
                         "badreadwriteTiledChunk must be called with a valid chunkIter");
        Value v;
        scidb::Coordinates coords;
        position_t nextPosition = chunkIter->getLogicalPosition();
        assert(nextPosition >= 0);
        while (nextPosition >= 0) {
            shared_ptr<BaseTile> tile;
            shared_ptr<BaseTile> cTile;
            nextPosition = chunkIter->getData(nextPosition, tileSize, tile, cTile);
            if (tile) {
                assert(cTile);
                Tile<Coordinates, ArrayEncoding>* coordsTyped =
                    safe_dynamic_cast<Tile<Coordinates, ArrayEncoding>*>(cTile.get());

                for (size_t i = 0, n = tile->size(); i < n; ++i) {
                    tile->at(i, v);
                    coordsTyped->at(i, coords);
                }
            }
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBadReadWrite, "test_badreadwrite", "PhysicalBadReadWrite");

}  // namespace scidb
