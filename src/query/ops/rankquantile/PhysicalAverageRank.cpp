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
 * PhysicalAverageRank.cpp
 *  Created on: May 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <query/PhysicalOperator.h>

#include <array/DelegateArray.h>
#include <array/MergeSortArray.h>
#include "RankCommon.h"
#include <sys/time.h>

using namespace std;

namespace scidb
{

class PhysicalAverageRank: public PhysicalOperator
{
  public:
    PhysicalAverageRank(const std::string& logicalName,
                        const std::string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    //We require that input is distributed round-robin so that our parallel trick works
    virtual DistributionRequirement getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
    {
        vector<RedistributeContext> requiredDistribution;
        requiredDistribution.push_back(RedistributeContext(createDistribution(defaultDistType()),
                                                           _schema.getResidency()));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext
        getOutputDistribution(const std::vector<RedistributeContext> & /*inputDistrib*/,
                              const std::vector< ArrayDesc> & /*inputSchema*/) const override
    {
        SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
        SCIDB_ASSERT(_schema.getResidency()->isEqual(Query::getValidQueryPtr(_query)->getDefaultArrayResidency()));
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        std::shared_ptr<Array> inputArray = inputArrays[0];
        checkOrUpdateIntervals(_schema, inputArray);

        if (inputArray->getSupportedAccess() == Array::SINGLE_PASS)
        {   //if input supports MULTI_PASS, don't bother converting it
            inputArray = ensureRandomAccess(inputArray, query);
        }

        const ArrayDesc& input = inputArray->getArrayDesc();
        string attName = _parameters.size() > 0 ? ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName() :
                                                input.getAttributes(true).firstDataAttribute().getName();

        const AttributeDesc* rankedAttributeID = nullptr;
        for (const auto& attr : input.getAttributes())
        {
            if (attr.getName() == attName)
            {
                rankedAttributeID = &attr;
                break;
            }
        }
        assert(rankedAttributeID);

        Dimensions const& dims = inputArray->getArrayDesc().getDimensions();
        Dimensions groupBy;
        if (_parameters.size() > 1)
        {
            size_t i, j;
            for (i = 0; i < _parameters.size()-1; i++) {
               const string& dimName = ((std::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getObjectName();
               const string& dimAlias = ((std::shared_ptr<OperatorParamReference>&)_parameters[i + 1])->getArrayName();
               for (j = 0; j < dims.size(); j++) {
                   if (dims[j].hasNameAndAlias(dimName, dimAlias)) {
                       groupBy.push_back(dims[j]);
                       break;
                   }
               }
               assert(j < dims.size());
            }
        }

        return buildDualRankArray(inputArray, *rankedAttributeID, groupBy, query, shared_from_this());
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAverageRank, "avg_rank", "physicalAverageRank")

}
