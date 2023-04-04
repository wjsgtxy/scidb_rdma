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
 * PhysicalCrossBetween.cpp
 *
 *  Created on: August 15, 2014
 *  Author: Donghui Zhang
 */

#include <query/PhysicalOperator.h>

#include <array/Array.h>
#include "BetweenArray.h"
#include <util/SchemaUtils.h>

namespace scidb {

using std::vector;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("CrossBetween"));

/// @note cross_between is a form of between where the rhs array defines the between parameters
///       it does not peform any cross_join, so the use of "cross" in the name is somewhat misleading.
class PhysicalCrossBetween: public  PhysicalOperator
{
public:
    PhysicalCrossBetween(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
         PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    // required to allow replicated input
    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        vector<uint8_t> result(numChildren, false);
        SCIDB_ASSERT(numChildren==2);
        result[1] = true;   // permitted on the right-hand input
        return result;
    }

    /// @see OperatorDist::inferSynthesizedDistType()
    ///       it produces the same distribution as its left-hand-side
    ///       This is handled correctly by OperatorDist

    /// @see PhysicalOperator::getOutputDistribution
    /// @note cross_between is a form of between where the rhs array defines the between parameters
    ///       This is already handled correctly by PhysicalOperator

    /// see OperatorDist
    void checkInputDistAgreement(std::vector<DistType> const& inDist, size_t /*depth*/) const override
    {
        SCIDB_ASSERT(inDist.size() == 2);
        // inDist[0] can be arbitrary
        // inDist[1] unrelated to inDist[0]
        //    it might be required to be dtReplication in the future ...
        //    if and when redistribution of input[1] is always done by separate SG node
        //    rather than internally as is done now. TBD
    }

    /// @see PhysicalOperator
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const override
    {
       return inputBoundaries[0];
    }

    /***
     * CrossBetween is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkIterator method.
     */
    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays,
                                    std::shared_ptr<Query> query) override
    {
        assert(inputArrays.size() == 2);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        // Ensure inputArray supports random access, and rangesArray is replicated.
        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        SCIDB_ASSERT(inputArrays[0]->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));
        SCIDB_ASSERT(inputArray->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));

        // ranges might or might not be replicated, already.
        bool isRangesReplicated = inputArrays[1]->getArrayDesc().getDistribution()
                                  ->checkCompatibility(createDistribution(dtReplication));

        std::shared_ptr<Array> rangesArray;
        if (!isRangesReplicated) {
            LOG4CXX_TRACE(logger, "PhysicalCrossBetween::execute redistributing ranges");
            rangesArray = redistributeToRandomAccess(inputArrays[1],
                                                     createDistribution(dtReplication),
                                                     inputArray->getArrayDesc().getResidency(),
                                                     query,
                                                     shared_from_this());
        } else {
            // already replicated, ensure RandomAccess
            LOG4CXX_TRACE(logger, "PhysicalCrossBetween::execute ranges already replicated, ensuring random");
            rangesArray = PhysicalOperator::ensureRandomAccess(inputArrays[1], query);
        }

        // Some variables.
        SchemaUtils schemaUtilsInputArray(inputArray);
        SchemaUtils schemaUtilsRangesArray(rangesArray);
        size_t nDims = schemaUtilsInputArray._dims.size();
        const auto nAttrs = schemaUtilsRangesArray._attrsWithoutET.size();
        assert(nDims*2 == nAttrs);

        // Scan all attributes of the rangesArray simultaneously, and fill in spatialRanges.
        // Set up a MultiConstIterators to process the array iterators simultaneously.
        SpatialRangesPtr spatialRangesPtr = std::make_shared<SpatialRanges>(nDims);

        vector<std::shared_ptr<ConstIterator> > rangesArrayIters(nAttrs);
        for (const auto& attr : schemaUtilsRangesArray._attrsWithoutET) {
            rangesArrayIters[attr.getId()] = rangesArray->getConstIterator(attr);
        }
        MultiConstIterators multiItersRangesArray(rangesArrayIters);
        while (!multiItersRangesArray.end()) {
            // Set up a MultiConstIterators to process the chunk iterators simultaneously.
            vector<std::shared_ptr<ConstIterator> > rangesChunkIters;
            for (const auto& attr : schemaUtilsRangesArray._attrsWithoutET) {
                rangesChunkIters.push_back(
                    std::dynamic_pointer_cast<ConstArrayIterator>(rangesArrayIters[attr.getId()])->getChunk().getConstIterator());
            }
            MultiConstIterators multiItersRangesChunk(rangesChunkIters);
            while (!multiItersRangesChunk.end()) {
                SpatialRange spatialRange(nDims);
                for (size_t i=0; i<nDims; ++i) {
                    const Value& v = std::dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._low[i] = v.getInt64();
                }
                for (size_t i=nDims; i<nDims*2; ++i) {
                    const Value& v = std::dynamic_pointer_cast<ConstChunkIterator>(rangesChunkIters[i])->getItem();
                    spatialRange._high[i-nDims] = v.getInt64();
                }
                if (spatialRange.valid()) {
                    spatialRangesPtr->insert(std::move(spatialRange));
                }
                ++ multiItersRangesChunk;
            }
            ++ multiItersRangesArray;
        }

        // Return a CrossBetweenArray.
        spatialRangesPtr->buildIndex();
        return std::make_shared<BetweenArray>(_schema, spatialRangesPtr, inputArray);
   }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCrossBetween, "cross_between", "physicalCrossBetween")

}  // namespace scidb
