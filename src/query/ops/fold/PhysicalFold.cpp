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

#include "FoldArray.h"

#include <query/PhysicalOperator.h>

namespace scidb {

struct PhysicalFold : PhysicalOperator
{
    PhysicalFold(std::string const& logicalName,
                 std::string const& physicalName,
                 Parameters const& parameters,
                 ArrayDesc const& schema);
    virtual ~PhysicalFold();

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query) override;

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/,
                                      size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(
        std::vector<RedistributeContext> const& inputDistributions,
        std::vector<ArrayDesc> const& inputSchemas) const override
    {
        assertConsistency(inputSchemas.front(), inputDistributions.front());

        SCIDB_ASSERT(_schema.getDistribution()->getDistType()==dtUndefined);
        _schema.setResidency(inputDistributions.front().getArrayResidency());

        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }
};

PhysicalFold::PhysicalFold(std::string const& logicalName,
                           std::string const& physicalName,
                           Parameters const& parameters,
                           ArrayDesc const& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
{
}

PhysicalFold::~PhysicalFold()
{
}

std::shared_ptr<Array> PhysicalFold::execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                             std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(inputArrays.size() == 1);
    SCIDB_ASSERT(_parameters.empty());

    return std::make_shared<FoldArray>(query,
                                       getSchema(),
                                       inputArrays.front());
}

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFold,
                                  "_fold",
                                  "PhysicalFold")

}  // namespace scidb
