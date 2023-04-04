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
 * @file PhysicalRepart.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author apoliakov@paradigm4.com
 */

#include "PhysicalRepart.h"
#include <array/DelegateArray.h>

using namespace std;

namespace scidb {

std::vector<DistType> PhysicalRepart::inferChildInheritances(DistType inherited, size_t numChildren) const
{
    LOG4CXX_TRACE(logger, "PhysicalRepart::inferChildInheritances: operator " << getOperatorName());
    LOG4CXX_TRACE(logger, "PhysicalRepart::inferChildInheritances: inherited " << inherited);
    LOG4CXX_TRACE(logger, "PhysicalRepart::inferChildInheritances: numChildren " << numChildren);
    SCIDB_ASSERT(numChildren==1);  // bug is fixed

    std::vector<DistType> result(numChildren);
    if (not isReplicated(inherited)) {
        result[0] = inherited;
    } else {
        result[0] = defaultDistType();    // ask for something that is not replicated to
                                                // prevent insertion of SG to replication
                                                // due to inheritance
    }
    LOG4CXX_TRACE(logger, "PhysicalRepart::inferChildInheritances: [0] " << result[0]);

    return result;
}

//True if this is a no-op (just a metadata change, doesn't change chunk sizes or overlap) --AP
bool PhysicalRepart::_isNoop(ArrayDesc const& inputSchema) const
{
    if (!_schema.samePartitioning(inputSchema)) {
        return false;
    }
    if (findKeyword(RedimSettings::KW_OFFSET)) {
        return false;
    }
    return true;
}

PhysicalBoundaries
PhysicalRepart::getOutputBoundaries(std::vector<PhysicalBoundaries>const& sourceBoundaries,
                                    std::vector<ArrayDesc>const& sourceSchema) const
{
    return sourceBoundaries[0];
}

/// @see OperatorDist
/// @note This is an operator that needs the inputSchemas argument because it uses it to
///       determine what the output DistType should be.  Would need further investigation
///       to determine whether that is important to proper functioning.
DistType
PhysicalRepart::inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/,
                                         std::vector<ArrayDesc> const& inSchema, size_t depth) const
{
    SCIDB_ASSERT(inSchema.size() >= 1); // for inSchema[0]

    if (_isNoop(inSchema[0])) {
        _schema.setDistribution(createDistribution(inSchema[0].getDistribution()->getDistType()));
    }

    auto result = _schema.getDistribution()->getDistType();
    LOG4CXX_TRACE(logger, "PhysicalRepart::inferSynthesizedDistType() returning " << result);
    return result;
}

RedistributeContext
PhysicalRepart::getOutputDistribution(std::vector<RedistributeContext>const& inputDistributions,
                                      std::vector<ArrayDesc>const& inputSchemas) const
{
    assertConsistency(inputSchemas[0], inputDistributions[0]);

    _schema.setResidency(inputDistributions[0].getArrayResidency());
    if (_isNoop(inputSchemas[0])) {
        _schema.setDistribution(inputDistributions[0].getArrayDistribution());
    }
    return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
}

bool PhysicalRepart::outputFullChunks(std::vector<ArrayDesc>const& inputSchemas) const
{
    return _isNoop(inputSchemas[0]);
}

std::shared_ptr<Array>
PhysicalRepart::execute(vector< std::shared_ptr<Array> >& sourceArray, std::shared_ptr<Query> query)
{
    std::shared_ptr<Array> input = sourceArray[0];
    if (_isNoop(input->getArrayDesc()))
    {
        return std::shared_ptr<Array> (new DelegateArray(_schema, input, true) );
    }

    Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.
    Dimensions const& destDims  = _schema.getDimensions();

    vector<AggregatePtr> aggregates (destAttrs.size());
    vector<size_t>       attrMapping(destAttrs.size());
    vector<size_t>       dimMapping (destDims.size());

    setupMappingsByOrder(destAttrs, destDims);

    ElapsedMilliSeconds timing;
    std::shared_ptr<Array> res = redimensionArray(input, query, shared_from_this(), timing, AUTO);

    SCIDB_ASSERT(isUndefined(res->getArrayDesc().getDistribution()->getDistType()));
    return res;
}

// Note that the name "physicalRepart" is known in QueryPlan.h.
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRepart, "repart", "physicalRepart")

}  // namespace scidb
