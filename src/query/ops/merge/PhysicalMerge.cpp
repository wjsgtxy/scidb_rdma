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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "MergeArray.h"

#include <query/PhysicalOperator.h>
#include <log4cxx/logger.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.merge"));

class PhysicalMerge: public PhysicalOperator
{
public:
    PhysicalMerge(const string& logicalName, const string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return unionWith(inputBoundaries[0], inputBoundaries[1]);
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                                      std::vector<ArrayDesc> const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);
        assertConsistency(inputSchemas[1], inputDistributions[1]);

        RedistributeContext distro = PhysicalOperator::getOutputDistribution(inputDistributions,
                                                                             inputSchemas);
        LOG4CXX_TRACE(logger, "merge() output distro: "<< distro);
        return distro;
    }

    /**
     * Ensure startMin, endMax, chunkOverlap, and chunkInterval match
     * for each of the input arrays.  Note:  the only one this routine
     * is allowed to change is endMax.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        const size_t N = inputSchemas.size();
        SCIDB_ASSERT(N > 1);
        SCIDB_ASSERT(N == modifiedPtrs.size());

        ArrayDesc const&     mergedSchema      = _schema;
        _redimRepartSchemas.clear();
        bool foundNonAutochunked = false;
        for (size_t nSchema = 0; nSchema < inputSchemas.size(); nSchema++)
        {
            ArrayDesc const &currentSchema  = inputSchemas[nSchema];
            if (!currentSchema.isAutochunked()) {
                foundNonAutochunked = true;
            }
            if(mergedSchema.sameShape(currentSchema)) {
                // Indicate no modification is necessary.
                modifiedPtrs[nSchema] = nullptr;
            }
            else {
                ArrayDesc newSchema  = currentSchema;

                // Replace the current schema's dimension values with
                // those from the merged schema.
                newSchema.replaceDimensionValues(mergedSchema);
                // Create a modifification indicator.
                _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(
                                                  newSchema.getName(),
                                                  newSchema.getAttributes(),
                                                  newSchema.getDimensions(),
                                                  newSchema.getDistribution(),
                                                  newSchema.getResidency() ));
                // Indicate modification is necessary.
                modifiedPtrs[nSchema] = _redimRepartSchemas.back().get();
            }
        }  // for (size_t nSchema = 0; nSchema < ...) { ... }

        if (!foundNonAutochunked) {
            // All the input schemas are autochunked, which is not allowed.
            // This should have been caught in LogicalMerge::inferSchema when
            // the exemplar schema was being determined.
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
                << getPhysicalName();
        }

        if (_redimRepartSchemas.empty())
        {
            // Assertions elsewhere hate an all-NULLs vector here.
            modifiedPtrs.clear();
        }
    }

    /**
     * Merge is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() >= 2);
        checkOrUpdateIntervals(_schema, inputArrays[0]); // The mergedDimensions[] intervals come from left array.
                                                         // See LogicalMerge::inferSchema().

        const std::shared_ptr<Array>& left = inputArrays[0];
        const std::shared_ptr<Array>& right = inputArrays[1];
        if (isDebug()) {
            SCIDB_ASSERT(left->getArrayDesc().getResidency()->isEqual(right->getArrayDesc().getResidency()));
            SCIDB_ASSERT(left->getArrayDesc().getDistribution()->checkCompatibility(right->getArrayDesc().getDistribution()));

            SCIDB_ASSERT(left->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));
            SCIDB_ASSERT(left->getArrayDesc().getDistribution()->checkCompatibility(_schema.getDistribution()));
        }
        return std::shared_ptr<Array>(new MergeArray(_schema, inputArrays));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMerge, "merge", "physicalMerge")

}  // namespace scidb
