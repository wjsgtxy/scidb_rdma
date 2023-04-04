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
 * PhysicalConsume.cpp
 *
 *  Created on: Aug 6, 2013
 *      Author: Tigor, sfridella
 */

#include <array/Array.h>
#include <array/RLE.h>
#include <array/TileIteratorAdaptors.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <util/MultiConstIterators.h>

using namespace std;

namespace scidb
{

class PhysicalConsume: public PhysicalOperator
{
public:
    PhysicalConsume(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() <= 1);

        /* This parameter determines the width of the vertical slice that we use to scan
           Default is 1.
        */
        uint64_t attrStrideSize =
            (_parameters.size() == 1)
            ? ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint64()
            : 1;

        std::shared_ptr<Array>& array = inputArrays[0];
        const auto& inputSchema = array->getArrayDesc();

        /* Scan through the array in vertical slices of width "attrStrideSize"
         */
        size_t numRealAttrs = inputSchema.getAttributes(true).size();
        size_t baseAttr = 0;

        attrStrideSize = std::max(attrStrideSize,uint64_t(1));
        attrStrideSize = std::min(attrStrideSize,numRealAttrs);
        const auto& attrs = array->getArrayDesc().getAttributes();

        while (baseAttr < numRealAttrs)
        {
            /* Get iterators for this vertical slice
             */
            size_t sliceSize = std::min(numRealAttrs - baseAttr,attrStrideSize);
            std::vector<std::shared_ptr<ConstIterator> > arrayIters(sliceSize);

            for (size_t i = baseAttr;
                 i < (baseAttr + sliceSize);
                 ++i)
            {
                const auto& attr = attrs.findattr(safe_static_cast<AttributeID>(i));
                arrayIters[i - baseAttr] = array->getConstIterator(attr);
            }

            /* Scan each attribute one chunk at a time, use MultiConstIterator
               to handle possible gaps at each chunk position
            */
            MultiConstIterators multiIters(arrayIters);
            while (!multiIters.end())
            {
                std::vector<size_t> minIds;

                /* Get list of all iters where current chunk position is not empty
                 */
                multiIters.getIDsAtMinPosition(minIds);
                for (size_t i = 0; i < minIds.size(); i++)  {

                    std::shared_ptr<ConstArrayIterator> currentIter =
                       static_pointer_cast<ConstArrayIterator, ConstIterator > (arrayIters[ minIds[i] ] );
                    if (isDebug()) {
                        currentIter->getChunk(); // to catch some bugs like #3656
                    }
                    const ConstChunk& chunk = currentIter->getChunk();

                    int configTileSize = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE);
                    int iterMode = configTileSize > 0 ? ConstChunkIterator::INTENDED_TILE_MODE : 0;

                    std::shared_ptr<ConstChunkIterator> chunkIter = chunk.getConstIterator(iterMode);
                    iterMode = chunkIter->getMode();

                    if ((iterMode & ConstChunkIterator::TILE_MODE)==0 &&
                        configTileSize > 0) {
                        // new tile mode
                        if (chunkIter->end()) {
                            continue;
                        }
                        chunkIter = std::make_shared<
                           TileConstChunkIterator<
                              std::shared_ptr<ConstChunkIterator> > >(chunkIter, query);
                        assert(configTileSize>0);
                        consumeTiledChunk(chunkIter, size_t(configTileSize));
                        continue;
                    }

                    while (!chunkIter->end()) {
                        Value const& v = chunkIter->getItem();

                        ConstRLEPayload* tile = nullptr;
                        if (configTileSize > 0 &&
                            (tile = v.getTile())) {
                            // old tile mode
                            ConstRLEPayload::iterator piter = tile->getIterator();
                            while (!piter.end()) {
                                Value tv;
                                piter.getItem(tv);
                                ++(piter);
                            }
                        } else {
                            chunkIter->getPosition();
                            v.isNull(); // suppress compiler warning
                        }
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
        return std::shared_ptr<Array>();
    }
    void consumeTiledChunk(std::shared_ptr<ConstChunkIterator>& chunkIter, size_t tileSize)
    {
        ASSERT_EXCEPTION( ! chunkIter->end(), "consumeTiledChunk must be called with a valid chunkIter" );
        Value v;
        scidb::Coordinates coords;
        position_t nextPosition = chunkIter->getLogicalPosition();
        assert(nextPosition >= 0);
        while(nextPosition >= 0) {
            std::shared_ptr<BaseTile> tile;
            std::shared_ptr<BaseTile> cTile;
            nextPosition = chunkIter->getData(nextPosition, tileSize, tile, cTile);
            if (tile) {
                assert(cTile);
                Tile<Coordinates, ArrayEncoding >* coordsTyped =
                    safe_dynamic_cast< Tile<Coordinates, ArrayEncoding >* >(cTile.get());

                for (size_t i = 0, n = tile->size(); i < n ; ++i) {
                    tile->at(i, v);
                    coordsTyped->at(i,coords);
                }
            }
        }
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalConsume, "consume", "PhysicalConsume")

}  // namespace scidb
