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
#include "RankCommon.h"

#include <fstream>
#include <iostream>
#include <iomanip>

namespace scidb
{

/**
 * We not only need to increment inputIterator, but also need to increment the iterator
 * in the RowCollection.
 */
void GroupbyRankChunkIterator::operator++() {
    ++(*_rcIterator);
    ++(*inputIterator);
    assert(doIteratorsMatch());
}

/**
 * setPosition
 *
 * @param pos a position in the space of the input array (e.g. it could have many dimensions)
 * @see ConstChunkIterator::setPosition()
 *
 * The function essentially changes the _locInRow variable stored in RowIterator _rcIterator.
 * The _locInRow variable is a sequence number of pos, if the input chunk is scanned from beginning until pos.
 * To support finding this sequence number, the first time setPosition() is called, we scan the input chunk
 * and build a map from pos to sequence number.
 *
 */
bool GroupbyRankChunkIterator::setPosition(const Coordinates& pos) {
    assert(doIteratorsMatch());

    // Did some one call setPosition at the current position?
    if (!end() && coordinatesCompare(pos, getPosition()) == 0) {
        return true;
    }

    // The first time setPosition is called, build a map that supports subsequent calls to setPosition.
    if (!_validPosToLocInRow) {
        _validPosToLocInRow = true;

        // store a copy of the inputIterator's current pos
        bool isEnd = inputIterator->end();
        Coordinates posInInput;
        if (!isEnd) {
            posInInput = inputIterator->getPosition();
        }

        // Scan inputIterator and build the map _posToLocInRow
        inputIterator->restart();
        size_t locInRow = 0;
        while (!inputIterator->end()) {
            _posToLocInRow.insert(std::make_pair(inputIterator->getPosition(), locInRow));
            ++locInRow;
            ++(*inputIterator);
        }

        // restore inputIterator's current pos
        if (!isEnd) {
            inputIterator->setPosition(posInInput);
        }
        assert(doIteratorsMatch());
    }

    // call RowIterator::setPosition()
    std::unordered_map<Coordinates, size_t, CoordinatesHash>::const_iterator it = _posToLocInRow.find(pos);
    if ( it == _posToLocInRow.end()) {
        assert(doIteratorsMatch());
        return false;
    }

    const size_t COLUMN = 1; // All cells in a groupby-rank chunk have the same row, but different columns.
    _locInRow2D[COLUMN] = it->second;
    bool ret1 = _rcIterator->setPosition(_locInRow2D);

    // call inputIterator::setPosition()
    bool ret2 = inputIterator->setPosition(pos);

    ASSERT_EXCEPTION(ret1 == ret2, "The two iterators in GroupbyRankChunkIterator::setPosition() do not match.");
    assert(doIteratorsMatch());
    return ret1;
}

std::shared_ptr<SharedBuffer> rMapToBuffer( CountsMap const& input, size_t nCoords)
{
    size_t totalSize = input.size() * (nCoords * sizeof(Coordinate) + sizeof(uint64_t));
    std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, totalSize));
    auto dst = reinterpret_cast<Coordinate*>(buf->getWriteData());
    for (auto const &bucket : input)
    {
        Coordinates const& coords = bucket.first;

        for (size_t i=0; i< nCoords; i++)
        {
            *dst = coords[i];
            ++dst;
        }

        auto dMaxRank = reinterpret_cast<uint64_t*>(dst);
        *dMaxRank = bucket.second;
        ++dMaxRank;
        dst = reinterpret_cast<Coordinate*>(dMaxRank);
    }
    return buf;
}

void updateRmap(CountsMap& input, std::shared_ptr<SharedBuffer> buf, size_t nCoords)
{
    if (buf.get() == 0)
    {
        return;
    }

    auto src = reinterpret_cast<const Coordinate*>(buf->getConstData());
    while (reinterpret_cast<const char*>(src) != reinterpret_cast<const char*>(buf->getConstData()) + buf->getSize())
    {
        Coordinates coords(nCoords);
        for (size_t i =0; i<nCoords; i++)
        {
            coords[i] = *src;
            ++src;
        }

        auto dMaxRank = reinterpret_cast<const uint64_t*>(src);
        if (input.count(coords) == 0)
        {
            input[coords] = *dMaxRank;
        }
        else if (input[coords] < *dMaxRank)
        {
            input[coords] = *dMaxRank;
        }

        ++dMaxRank;
        src = reinterpret_cast<const Coordinate*>(dMaxRank);
    }
}

ArrayDesc getRankingSchema(ArrayDesc const& inputSchema,
                           std::shared_ptr<Query> const& query,
                           const AttributeDesc& rankedAttribute,
                           bool dualRank)
{
    Attributes outputAttrs;
    outputAttrs.push_back(AttributeDesc(rankedAttribute.getName(),
                                        rankedAttribute.getType(),
                                        rankedAttribute.getFlags(),
                                        rankedAttribute.getDefaultCompressionMethod()));
    outputAttrs.push_back(AttributeDesc(rankedAttribute.getName() + "_rank",
                                        TID_DOUBLE,
                                        AttributeDesc::IS_NULLABLE,
                                        CompressorType::NONE));

    if (dualRank)
    {
        outputAttrs.push_back(AttributeDesc(rankedAttribute.getName() + "_hrank",
                                            TID_DOUBLE,
                                            AttributeDesc::IS_NULLABLE,
                                            CompressorType::NONE));
    }

    AttributeDesc const* emptyTag = inputSchema.getEmptyBitmapAttribute();
    if (emptyTag)
    {
        outputAttrs.push_back(AttributeDesc(emptyTag->getName(),
                                            emptyTag->getType(),
                                            emptyTag->getFlags(),
                                            emptyTag->getDefaultCompressionMethod()));
    }

    //no overlap. otherwise quantile gets a count that's too large
    Dimensions const& dims = inputSchema.getDimensions();
    Dimensions outDims(dims.size());
    for (size_t i = 0, n = dims.size(); i < n; i++)
    {
        DimensionDesc const& srcDim = dims[i];
        outDims[i] = DimensionDesc(srcDim.getBaseName(),
                                   srcDim.getNamesAndAliases(),
                                   srcDim.getStartMin(),
                                   srcDim.getCurrStart(),
                                   srcDim.getCurrEnd(),
                                   srcDim.getEndMax(),
                                   srcDim.getRawChunkInterval(),
                                   0);
    }
    return ArrayDesc(inputSchema.getName(), outputAttrs, outDims,
                     createDistribution(dtUndefined),
                     query->getDefaultArrayResidency());
}


static std::shared_ptr<PreSortMap>
makePreSortMap(std::shared_ptr<Array>& ary, const AttributeDesc& aId, Dimensions const& dims)
{
    TypeEnum type = typeId2TypeEnum(aId.getType(),
                                    true/*noThrow*/);

    std::shared_ptr<PreSortMap> preSortMap;
    switch (type) {
    case TE_DOUBLE:
        preSortMap.reset(new PrimitivePreSortMap<double>(ary, aId, dims));
        break;
    case TE_FLOAT:
        preSortMap.reset(new PrimitivePreSortMap<float>(ary, aId, dims));
        break;
    case TE_INT64:
        preSortMap.reset(new PrimitivePreSortMap<int64_t>(ary, aId, dims));
        break;
    case TE_UINT64:
        preSortMap.reset(new PrimitivePreSortMap<uint64_t>(ary, aId, dims));
        break;
    case TE_INT32:
        preSortMap.reset(new PrimitivePreSortMap<int32_t>(ary, aId, dims));
        break;
    case TE_UINT32:
        preSortMap.reset(new PrimitivePreSortMap<uint32_t>(ary, aId, dims));
        break;
    case TE_INT16:
        preSortMap.reset(new PrimitivePreSortMap<int16_t>(ary, aId, dims));
        break;
    case TE_UINT16:
        preSortMap.reset(new PrimitivePreSortMap<uint16_t>(ary, aId, dims));
        break;
    case TE_INT8:
        preSortMap.reset(new PrimitivePreSortMap<int8_t>(ary, aId, dims));
        break;
    case TE_UINT8:
        preSortMap.reset(new PrimitivePreSortMap<uint8_t>(ary, aId, dims));
        break;
    case TE_CHAR:
        preSortMap.reset(new PrimitivePreSortMap<char>(ary, aId, dims));
        break;
    case TE_BOOL:
        preSortMap.reset(new PrimitivePreSortMap<bool>(ary, aId, dims));
        break;
    case TE_DATETIME:
        preSortMap.reset(new PrimitivePreSortMap<time_t>(ary, aId, dims));
        break;
    default:
        preSortMap.reset(new ValuePreSortMap(ary, aId, dims));
        break;
    }

    return preSortMap;
}

//inputArray must be distributed round-robin
std::shared_ptr<Array> buildRankArray(std::shared_ptr<Array>& inputArray,
                                      const AttributeDesc& rankedAttributeID,
                                      Dimensions const& groupedDimensions,
                                      std::shared_ptr<Query>& query,
                                      const std::shared_ptr<PhysicalOperator>& phyOp,
                                      std::shared_ptr<RankingStats> rstats)
{
    std::shared_ptr<PreSortMap> preSortMap =
        makePreSortMap(inputArray, rankedAttributeID, groupedDimensions);

    const ArrayDesc& input = inputArray->getArrayDesc();
    ArrayDesc outputSchema = getRankingSchema(input, query, rankedAttributeID);

    if (isDebug()) {
        SCIDB_ASSERT(input.getResidency()->isEqual(query->getDefaultArrayResidency()));
        SCIDB_ASSERT(input.getDistribution()->checkCompatibility(createDistribution(dtHashPartitioned)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtDataframe)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtByRow)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtByCol)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtRowCyclic)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtColCyclic)));

        SCIDB_ASSERT(outputSchema.getResidency()->isEqual(query->getDefaultArrayResidency()));
        SCIDB_ASSERT(outputSchema.getDistribution()->checkCompatibility(createDistribution(dtUndefined)));
    }

    std::shared_ptr<Array> runningRank(new RankArray(outputSchema,
                                                     inputArray,
                                                     preSortMap,
                                                     rankedAttributeID,
                                                     false,
                                                     rstats));

    const size_t nInstances = query->getInstancesCount();
    for (size_t i =1; i<nInstances; i++)
    {
        LOG4CXX_DEBUG(logger, "Performing rotation "<<i);

        ArrayDistPtr psHashShiftedDist = ArrayDistributionFactory::getInstance()->construct(input.getDistribution()->getDistType(),
                                                                                            DEFAULT_REDUNDANCY,
                                                                                            std::string(),
                                                                                            i);
        runningRank = redistributeToRandomAccess(runningRank,
                                                 psHashShiftedDist,
                                                 ArrayResPtr(), // default query residency
                                                 query,
                                                 phyOp);

        const auto& fda = outputSchema.getAttributes().firstDataAttribute();
        runningRank = std::shared_ptr<Array>(new RankArray(outputSchema, runningRank,
                                                           preSortMap, fda, true, rstats));
    }

    return runningRank;
}

//inputArray must be distributed round-robin
std::shared_ptr<Array> buildDualRankArray(std::shared_ptr<Array>& inputArray,
                                     const AttributeDesc& rankedAttributeID,
                                     Dimensions const& groupedDimensions,
                                     std::shared_ptr<Query>& query,
                                     const std::shared_ptr<PhysicalOperator>& phyOp,
                                     std::shared_ptr<RankingStats> rstats)
{
    std::shared_ptr<PreSortMap> preSortMap =
        makePreSortMap(inputArray, rankedAttributeID, groupedDimensions);

    const ArrayDesc& input = inputArray->getArrayDesc();
    ArrayDesc dualRankSchema = getRankingSchema(input, query, rankedAttributeID, true);

    if (isDebug()) {
        SCIDB_ASSERT(input.getResidency()->isEqual(query->getDefaultArrayResidency()));
        SCIDB_ASSERT(input.getDistribution()->checkCompatibility(createDistribution(dtHashPartitioned)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtDataframe)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtByRow)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtByCol)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtRowCyclic)) ||
                     input.getDistribution()->checkCompatibility(createDistribution(dtColCyclic)));

        SCIDB_ASSERT(dualRankSchema.getResidency()->isEqual(query->getDefaultArrayResidency()));
        SCIDB_ASSERT(dualRankSchema.getDistribution()->checkCompatibility(createDistribution(dtUndefined)));
    }

    std::shared_ptr<Array> runningRank(new DualRankArray(dualRankSchema,
                                                         inputArray,
                                                         preSortMap,
                                                         rankedAttributeID,
                                                         false,
                                                         rstats));

    const size_t nInstances = query->getInstancesCount();
    for (size_t i =1; i<nInstances; i++)
    {
        LOG4CXX_DEBUG(logger, "Performing rotation "<<i);

        ArrayDistPtr psHashShiftedDist = ArrayDistributionFactory::getInstance()->construct(input.getDistribution()->getDistType(),
                                                                                            DEFAULT_REDUNDANCY,
                                                                                            std::string(),
                                                                                            i);
        runningRank = redistributeToRandomAccess(runningRank,
                                                 psHashShiftedDist,
                                                 ArrayResPtr(), // default query residency
                                                 query,
                                                 phyOp);

        const auto& fda = dualRankSchema.getAttributes().firstDataAttribute();
        runningRank = std::shared_ptr<Array>(new DualRankArray(dualRankSchema,
                                                               runningRank,
                                                               preSortMap, fda, true, rstats));
    }

    ArrayDesc outputSchema = getRankingSchema(input,query, rankedAttributeID);
    return std::shared_ptr<Array> (new AvgRankArray(outputSchema, runningRank));
}

}
