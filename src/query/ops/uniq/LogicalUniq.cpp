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

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Expression.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logical_scan"));

/**
 * An operator that removes duplicates from a sorted one-dimensional array. In other words, it works just like
 * the Unix "uniq" command.
 * <br>
 *
 * @brief The operator: uniq()
 *
 * @par Synopsis: uniq (input_array [, chunk_size: CHUNK_SIZE ] )
 *
 * @par Examples:
 *   <br> uniq (sorted_array)
 *   <br> store ( uniq ( sort ( project (big_array, string_attribute) ), chunk_size: 100000), string_attribute_index )
 *
 * @par Summary:
 *   <br>
 *   The input array must have a single attribute of any type and a single dimension. The data in the input array must
 *   be sorted and dense. The operator is built to accept the output produced by sort() with a single attribute.
 *   The output array shall have the same attribute with the dimension i starting at 0 and chunk size of 1 million.
 *   An optional chunk_size parameter may be used to set a different output chunk size.
 *   <br>
 *   Data is compared using a simple bitwise comparison of underlying memory. Null values are discarded from the output.
 *
 * @par Input:
 *   <br> array <single_attribute: INPUT_ATTRIBUTE_TYPE> [single_dimension= *]
 *
 * @par Output:
 *   <br> array
 *   <br> <
 *   <br>   single_attribute: INPUT_ATTRIBUTE_TYPE
 *   <br> >
 *   <br> [
 *   <br>   i = 0:*:0:CHUNK_SIZE
 *   <br> ]
 *   <br>
 *
 * @see PhysicalUniq.cpp for a description of the algorithm.
 */
class LogicalUniq : public LogicalOperator
{
    static const int64_t DEFAULT_CHUNK_SIZE = 1000000;

public:
    LogicalUniq(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        // Order of dataframe cells is non-deterministic, so
        // eliminating runs of duplicates would also be
        // non-deterministic (and is therefore disallowed).
        _properties.dataframe = false;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_INPUT))
            },
            { "chunk_size", RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        if (inputSchema.getAttributes(true).size() != 1)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNIQ_SINGLE_ATTRIBUTE_ONLY);
        }
        if (inputSchema.getDimensions().size() != 1 || inputSchema.getDimensions()[0].getStartMin() != 0)
        {
            //See PhysicalUniq::getOutputBoundaries
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNIQ_SINGLE_DIMENSION_AT_ZERO);
        }
        AttributeDesc const& inputAttribute = inputSchema.getAttributes().firstDataAttribute();

        int64_t chunkSize = DEFAULT_CHUNK_SIZE;
        Parameter p = findKeyword("chunk_size");
        if (p) {
            chunkSize = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)p)->getExpression(),
                                 TID_INT64).getInt64();
            if (chunkSize < 1) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
            }
        }

        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(inputAttribute.getName(),
                                                  inputAttribute.getType(),
                                                  0, //no longer nullable
                                                  inputAttribute.getDefaultCompressionMethod())); //Note:
        /* The compression feature (beyond RLE) is rarely used but here we carry the value over from the input.
         * It only has an effect if it is not set to NONE (default) and the array is stored later in the query.
         */
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, CoordinateBounds::getMax(), chunkSize, 0));

        return ArrayDesc(inputSchema.getName(),
                         outputAttributes.addEmptyTagAttribute(),
                         outputDimensions,
                         createDistribution(dtUndefined),
                         inputSchema.getResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUniq, "uniq")

} //namespace scidb
