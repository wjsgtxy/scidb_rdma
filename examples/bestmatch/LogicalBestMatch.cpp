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
 * LogicalBestMatch.cpp
 *
 *  Created on: Apr 04, 2012
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

class LogicalBestMatch: public LogicalOperator
{
  public:
    LogicalBestMatch(const string& logicalName, const std::string& alias):
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
        Dimensions const& patternDimensions = patternDesc.getDimensions();
        Attributes matchAttributes;

        if (catalogDimensions.size() != patternDimensions.size())
        {
            ostringstream left, right;
            printDimNames(left, patternDimensions);
            printDimNames(right, catalogDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "bestmatch" << left.str() << right.str();
        }
        for (size_t i = 0, n = catalogDimensions.size(); i < n; i++) {

            // XXX To do: No hope of autochunk support until requiresRedimensionOrRepartition() is
            // implemented (see below), since we'd need to make an unspecified (autochunked)
            // interval match some specified one.
            ASSERT_EXCEPTION(!catalogDimensions[i].isAutochunked(), "Operator does not support autochunked input");
            ASSERT_EXCEPTION(!patternDimensions[i].isAutochunked(), "Operator does not support autochunked input");

            if (!(catalogDimensions[i].getStartMin() == patternDimensions[i].getStartMin()
                  && catalogDimensions[i].getChunkInterval() == patternDimensions[i].getChunkInterval()
                  && catalogDimensions[i].getChunkOverlap() == patternDimensions[i].getChunkOverlap()))
            {
                // XXX To do: implement requiresRedimensionOrRepartition() method, remove
                // interval/overlap checks above, use SCIDB_LE_START_INDEX_MISMATCH here.
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Dimensions do not match";
            }
        }

        assert((patternAttributes.size() +
                catalogAttributes.size() +
                catalogDimensions.size()) < std::numeric_limits<AttributeID>::max());
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
        for (size_t i = 0, n = catalogDimensions.size(); i < n; i++) {
            matchAttributes.push_back(AttributeDesc("match_" + catalogDimensions[i].getBaseName(),
                                                    TID_INT64, 0, CompressorType::NONE));
        }

        matchAttributes.addEmptyTagAttribute();

        ArrayDistPtr undefDist = ArrayDistributionFactory::getInstance()->construct(dtUndefined,
                                                                                    DEFAULT_REDUNDANCY);
        return ArrayDesc("bestmatch", matchAttributes, patternDimensions,
                         undefDist, // not known until the physical stage
                         query->getDefaultArrayResidency() );
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalBestMatch, "bestmatch");


} //namespace
