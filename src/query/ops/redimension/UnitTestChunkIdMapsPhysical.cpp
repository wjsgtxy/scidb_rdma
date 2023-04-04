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
 * UnitTestChunkIdMap.cpp
 *
 *  Created on: 2/6/15
 *  @author sfridella
 */

#include "ChunkIdMap.h"
#include "RedimensionCommon.h"

#include <array/MemArray.h>
#include <query/Query.h>

#include <log4cxx/logger.h>

#include <boost/tuple/tuple.hpp>

namespace scidb
{

using namespace std;

/**
 * UnitTestChunkIdMap operator
 */
class UnitTestChunkIdMapPhysical : public RedimensionCommon
{
public:
    /**
     * @param logicalName the name of operator "test_chunk_id_maps"
     * @param physicalName the name of the physical counterpart
     * @param parameters the operator parameters
     * @param schema the result of LogicalRedimension::inferSchema
     */
    UnitTestChunkIdMapPhysical(const string& logicalName,
                                const string& physicalName,
                                const Parameters& parameters,
                                const ArrayDesc& schema)
        : RedimensionCommon(logicalName, physicalName, parameters, schema)
        {}



    virtual RedistributeContext
    getOutputDistribution(std::vector<RedistributeContext>const&,
                          std::vector<ArrayDesc>const&) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * @see PhysicalOperator::execute
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                              std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        std::shared_ptr<Array>& srcArray = inputArrays[0];
        ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();

        /* Create a ChunkIdMap object and our own map to check the
           correctness of the ChunkIdMap
         */
        map<size_t, Coordinates> checkingMap;
        map<size_t, Coordinates>::iterator checkingMapIt;
        bool checkingMapInserted;
        ArenaPtr ap = query->getArena();
        std::shared_ptr<ChunkIdMap> cidMap = createChunkIdMap(srcArrayDesc.getDimensions(), ap);

        /* Loop through each chunk, mapping the chunk pos to an id, and
           recording the mapping
         */
        const auto& attrs = srcArray->getArrayDesc().getAttributes();
        std::shared_ptr<ConstArrayIterator> srcArrayIterator =
            srcArray->getConstIterator(attrs.firstDataAttribute());

        while (!srcArrayIterator->end())
        {
            std::shared_ptr<ConstChunkIterator> srcChunkIterator =
                srcArrayIterator->getChunk().getConstIterator();

            Coordinates const& chunkPos = srcChunkIterator->getPosition();

            LOG4CXX_DEBUG(logger, "Mapping for position " << CoordsToStr(chunkPos));

            size_t newid = cidMap->mapChunkPosToId(chunkPos);

            boost::tie(checkingMapIt, checkingMapInserted) =
                checkingMap.insert(make_pair(newid, chunkPos));

            if (!checkingMapInserted)
            {
                stringstream ss;
                ss << "non-unique id returned by chunk id map for "
                   << CoordsToStr(chunkPos) << " and "
                   << CoordsToStr(checkingMapIt->second);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestChunkIdMap" << ss.str();
            }

            ++(*srcArrayIterator);
        }

        cidMap->reverse();

        /* Run through the mapping and verify that the reverse mapping call returns
           the correct value
         */
        for (checkingMapIt = checkingMap.begin();
             checkingMapIt != checkingMap.end();
             ++checkingMapIt)
        {
            LOG4CXX_DEBUG(logger, "Reverse mapping for chunk id " << checkingMapIt->first);

            if (coordinatesCompare(checkingMapIt->second,
                                   cidMap->mapIdToChunkPos(checkingMapIt->first)))
            {
                stringstream ss;
                ss << "reverse mapping not consistent for id "
                   << checkingMapIt->first << " expected: "
                   << CoordsToStr(checkingMapIt->second) << "found: "
                   << CoordsToStr(cidMap->mapIdToChunkPos(checkingMapIt->first));
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestChunkIdMap" << ss.str();
            }
        }

        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestChunkIdMapPhysical, "test_chunk_id_map", "UnitTestChunkIdMapPhysical");

}  // namespace ops
