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
#include <query/LogicalExpression.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;
using namespace scidb;

namespace scidb
{

/**
 * @brief The operator: _blasflopstest().
 *
 * @par Synopsis:
 *   _blasflopstest()
 *
 * @par Summary:
 *   This is a unit test operator only.
 *   It runs a dgemm_() locally on each instance to
 *   show that a plugin is provided linkage to
 *   BLAS functions.  The GEMM flops rate
 *   are currently printed in the scidb log.
 *
 * @par Input:
 *   - None
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   blasflopstest()
 *
 * @par Errors:
 *   Any runtime error should result in a generic exception.
 *
 * @par Notes:
 *   A unit test operator only, for validating BLAS
 *   linkage.  It is supported for no other purpose.
 *
 */
class LogicalBLASFlopsTest: public LogicalOperator
{
public:
    LogicalBLASFlopsTest(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
         // cells have a single attribute representing FLOPS/s
        Attributes attributes(1);
        attributes.push_back(AttributeDesc("flops__s",  TID_DOUBLE, 0, CompressorType::NONE));
        // rows are instances, columns are the operator that was timed
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("instance"),
                                      Coordinate(0), Coordinate(query->getInstancesCount()-1),
                                      uint32_t(1), uint32_t(0));
        // only timing one operation, dgemm, at the moment
        // [This extra dimension would allow for future backward compatible expansion to more operations]
        // dimensions[1] = DimensionDesc(string("operation"), Coordinate(0), Coordinate(0),
        //                             uint32_t(1), uint32_t(0), TID_STRING);
        return ArrayDesc("blas_flops_test", attributes, dimensions,
                         createDistribution(dtRowCyclic), query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalBLASFlopsTest, "_blasflopstest");

}  // namespace scidb
