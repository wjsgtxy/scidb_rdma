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
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include "MatchArray.h"

#include <query/Expression.h>
#include <query/PhysicalOperator.h>

using namespace std;

namespace scidb {

class PhysicalMatch: public PhysicalOperator
{
  public:
    PhysicalMatch(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    //TODO: when this operator works as expected and is debugged - add a correct getOutputBoundaries implementation here

    /***
     * Match is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        int64_t error = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();

        std::shared_ptr<Array> left = inputArrays[0];
        std::shared_ptr<Array> right = inputArrays[1];
        if (isDebug()) {
            SCIDB_ASSERT(left->getArrayDesc().getResidency()->isEqual(right->getArrayDesc().getResidency()));
            SCIDB_ASSERT(left->getArrayDesc().getDistribution()->checkCompatibility(right->getArrayDesc().getDistribution()));
            SCIDB_ASSERT(left->getArrayDesc().getResidency()->isEqual(query->getDefaultArrayResidency()));
            SCIDB_ASSERT(left->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));
            SCIDB_ASSERT(left->getArrayDesc().getDistribution()->checkCompatibility(_schema.getDistribution()));
        }
        return std::shared_ptr<Array>(new MatchArray(_schema, inputArrays[0], inputArrays[1], error));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalMatch, "match", "physicalMatch");

}  // namespace scidb
