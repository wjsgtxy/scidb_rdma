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

#include "TraceArray.h"

#include <array/Array.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>

#include <sys/stat.h>

namespace scidb
{

class PhysicalTrace : public PhysicalOperator
{
public:
    PhysicalTrace(const std::string& logicalName,
                  const std::string& physicalName,
                  const Parameters& parameters,
                  const ArrayDesc& schema)
            : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);
        SCIDB_ASSERT(_parameters.size() == 1);  // filename

        auto outputFilePath = paramToString(_parameters[0]);
        auto inputArray = inputArrays[0];
        checkOrUpdateIntervals(_schema, inputArray);
        return std::make_shared<TraceArray>(inputArray,
                                            outputFilePath,
                                            query->getInstanceID());
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalTrace, "_trace", "PhysicalTrace")

}  // namespace scidb
