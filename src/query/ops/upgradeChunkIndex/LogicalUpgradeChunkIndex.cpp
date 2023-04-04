/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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
#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/AutochunkFixer.h>
#include <query/Query.h>

using namespace std;

namespace scidb
{

class LogicalUpgradeChunkIndex : public LogicalOperator
{
public:
    LogicalUpgradeChunkIndex(const string& logicalName,
                             const string& alias)
        : LogicalOperator(logicalName, alias) {}

    ArrayDesc inferSchema(vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query)
    {
        ArrayDesc arrDesc;
        arrDesc.setDistribution(createDistribution(getSynthesizedDistType()));
        arrDesc.setResidency(query->getDefaultArrayResidency());

        return arrDesc;
    }
};

// This macro registers the operator with the system. The second argument is
// the SciDB user-visible operator name that is used to invoke it.
REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalUpgradeChunkIndex,
                                  "upgradeChunkIndex");

}  // namespace scidb
