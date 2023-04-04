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
 * UnitTestChunkIdMaps.cpp
 *
 *  Created on: 2/5/15
 *      Author: sfridella
 */

#include <query/LogicalOperator.h>

#include <query/Query.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: test_chunk_id_map().
 *
 * @par Synopsis:
 *   test_chunk_id_map( srcArray )
 *
 * @par Summary:
 *   Creates a chunk-id-map for the array.  Iterates each chunk and maps the
 *   chunk position and saves it.  Ensures that all chunks map to a unique
 *   id.  Checks that reverse mapping is correct.  If the test fails the
 *   operator throws an exception.  If it succeeds it returns an empty array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
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
 */

class UnitTestChunkIdMapLogical: public  LogicalOperator
{
public:
    UnitTestChunkIdMapLogical(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_INPUT))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        Attributes attributes;
        attributes.push_back(AttributeDesc("dummy_attribute", TID_STRING,
                                           0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), Coordinate(0), Coordinate(0), uint32_t(0), uint32_t(0));
        return ArrayDesc("dummy_array", attributes, dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(UnitTestChunkIdMapLogical, "test_chunk_id_map");

}  // namespace scidb
