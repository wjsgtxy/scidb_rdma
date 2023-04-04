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
 * @file OrderedBcastTestLogical.cpp
 *
 * @brief The logical operator interface for the operator testing  ordered global broadcast.
 */

#include <query/Query.h>
#include <array/Array.h>
#include <query/LogicalOperator.h>

namespace scidb
{
using namespace std;

/**
 * @brief The operator: _obcast_test().
 *
 * @par Synopsis:
 *   _obcast_test()
 *
 * @par Summary:
 *
 *
 *
 * @par Input:
 *   n/a
 *
 * @par Output array:
 *        <
 *   <br>   obcast_test_attribute: string
 *   <br> >
 *   <br> [
 *   <br>   obcast_test_dimension: start=end=chunk interval=0.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 *
 */
class OrderedBcastTestLogical: public LogicalOperator
{
    public:
    OrderedBcastTestLogical(const string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr< Query> query) override
    {
        Attributes attributes;
        attributes.push_back(AttributeDesc(
            "obcast_test_attribute",  TID_STRING, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("obcast_test_dimension"), Coordinate(0), Coordinate(0), uint32_t(0), uint32_t(0));
        return ArrayDesc("obcast_test_array",
                         attributes,
                         dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(OrderedBcastTestLogical, "_obcast_test");

}  // namespace scidb
