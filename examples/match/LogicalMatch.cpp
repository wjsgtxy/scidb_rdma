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
 * LogicalMatch.cpp
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

class LogicalMatch: public LogicalOperator
{
  public:
    LogicalMatch(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        ArrayDesc  const& patternDesc = schemas[0];
        ArrayDesc  const& catalogDesc = schemas[1];
        Attributes const& catalogAttributes = catalogDesc.getAttributes(true);
        Dimensions const& catalogDimensions = catalogDesc.getDimensions();
        Attributes const& patternAttributes = patternDesc.getAttributes(true);
        Dimensions resultDimensions = patternDesc.getDimensions();
        Attributes matchAttributes;

        if (catalogDimensions.size() != resultDimensions.size())
        {
            stringstream left, right;
            printDimNames(left, resultDimensions);
            printDimNames(right, catalogDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "match" << left.str() << right.str();
        }
        for (size_t i = 0, n = catalogDimensions.size(); i < n; i++) {

            // XXX To do: No hope of autochunk support until requiresRedimensionOrRepartition() is
            // implemented (see below), since we'd need to make an unspecified (autochunked)
            // interval match some specified one.
            ASSERT_EXCEPTION(!catalogDimensions[i].isAutochunked(), "Operator does not support autochunked input");
            ASSERT_EXCEPTION(!resultDimensions[i].isAutochunked(), "Operator does not support autochunked input");

            if (!(catalogDimensions[i].getStartMin() == resultDimensions[i].getStartMin()
                  && catalogDimensions[i].getChunkInterval() == resultDimensions[i].getChunkInterval()
                  && catalogDimensions[i].getChunkOverlap() == resultDimensions[i].getChunkOverlap()))
            {
                // XXX To do: implement requiresRedimensionOrRepartition() method, remove
                // interval/overlap checks above, use SCIDB_LE_START_INDEX_MISMATCH here.
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Dimensions do not match";
            }
        }

        for (const auto& attr : patternAttributes) {
            matchAttributes.push_back(AttributeDesc(attr.getName(), attr.getType(), attr.getFlags(),
                                               attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                               attr.getDefaultValueExpr()));
        }
        for (const auto& attr : catalogAttributes) {
            matchAttributes.push_back(AttributeDesc("match_" + attr.getName(), attr.getType(), attr.getFlags(),
                                               attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                                               attr.getDefaultValueExpr()));
        }
        for (size_t i = 0, n = catalogDimensions.size(); i < n; ++i) {
            matchAttributes.push_back(AttributeDesc(
                "match_" + catalogDimensions[i].getBaseName(),
                TID_INT64, 0, CompressorType::NONE));
        }
        matchAttributes.addEmptyTagAttribute();

        int64_t maxCollisions = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
            TID_INT64).getInt64();
        if (maxCollisions <= 0 || (int32_t)maxCollisions != maxCollisions)  {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENT2) << "positive";
        }
        resultDimensions.push_back(DimensionDesc("collision", 0, 0, maxCollisions-1, maxCollisions-1, (uint32_t)maxCollisions, 0));

        ArrayDistPtr undefDist = ArrayDistributionFactory::getInstance()->construct(dtUndefined,
                                                                                    DEFAULT_REDUNDANCY);
        return ArrayDesc("match", matchAttributes, resultDimensions,
                         undefDist, // not known until getOutputDistribution() time
                         query->getDefaultArrayResidency() );
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalMatch, "match");


} //namespace
