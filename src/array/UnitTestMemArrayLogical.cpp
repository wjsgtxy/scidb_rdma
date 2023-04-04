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
 * @file UnitTestMemArrayLogical.cpp
 *
 * @brief The logical operator interface for testing deep-chunk merge.
 */

#include <query/Query.h>
#include <array/Array.h>
#include <query/LogicalOperator.h>

namespace scidb
{
using namespace std;

/**
 * @brief The operator: test_memarray().
 *
 * @par Synopsis:
 *   test_memarray()
 *
 * @par Summary:
 *   This operator performs unit tests for memarray. It returns an empty string. Upon failures exceptions are thrown.
 *
 * @par Input:
 *   n/a
 *
 * @par Output array:
 *        <
 *   <br>   dummy_attribute: string
 *   <br> >
 *   <br> [
 *   <br>   dummy_dimension: start=end=chunk_interval=0.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class UnitTestMemArrayLogical: public LogicalOperator
{
public:
    UnitTestMemArrayLogical(const string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        Attributes attributes;
        attributes.push_back(AttributeDesc(
            "dummy_attribute", TID_STRING, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), Coordinate(0), Coordinate(0), uint32_t(0), uint32_t(0));
        return ArrayDesc("dummy_array", attributes, dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(UnitTestMemArrayLogical, "test_memarray");
}  // namespace scidb
