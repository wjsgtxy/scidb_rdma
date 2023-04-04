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
 * @file PhysicalProject.cpp
 *
 * @date Apr 20, 2010
 * @author Knizhnik
 */

#include "Projection.h"

#include <query/PhysicalOperator.h>

#include <array/ProjectArray.h>

#include <log4cxx/logger.h>

namespace scidb {

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.project"));

class PhysicalProject: public  PhysicalOperator
{
public:
    PhysicalProject(const string& logicalName,
                    const string& physicalName,
                    const Parameters& parameters,
                    const ArrayDesc& schema)
        :  PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    PhysicalBoundaries getOutputBoundaries(const vector<PhysicalBoundaries> & inputBoundaries,
                                           const vector<ArrayDesc> & inputSchemas) const override
    {
        return inputBoundaries[0];
    }

    /***
     * Project is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query) override
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        // Decode projected attributes from control cookie.
        Projection projectedAttrs = Projection::fromString(getControlCookie());
        SCIDB_ASSERT(!projectedAttrs.empty());

        return std::shared_ptr<Array>(new ProjectArray(_schema,
                                                       inputArrays[0],
                                                       projectedAttrs));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalProject, "project", "physicalProject")

}  // namespace scidb
