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
 * \file PhysicalCancel.cpp
 *
 * \author roman.simakov@gmail.com
 */

#include <SciDBAPI.h>

#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <query/executor/SciDBExecutor.h>

#include <iostream>

namespace scidb {

class PhysicalCancel : public PhysicalOperator
{
public:
    PhysicalCancel(const std::string& logicalName,
                   const std::string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        std::stringstream queryIdS (paramToString(_parameters[0]));
        QueryID queryID;
        queryIdS >> queryID;
        auto targetQueryPtr = Query::getQueryByID(queryID, /*raise*/ true);
        if (targetQueryPtr->isCoordinator()) {
            // Cancel on the coordinator, broadcasting the abort request to all
            // worker instances.  Instances behind a network partition will have
            // cancelled locally, missing nothing.
            scidb::SciDBServer& scidb = getSciDBExecutor();
            scidb.cancelQuery(queryID);
        }

        return std::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCancel, "cancel", "cancel_impl")

}  // namespace ops
