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
 * LogicalSaveBmp.cpp
 *
 *  Created on: 8/15/12
 *      Author: poliocough@gmail.com
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/**
 * Logical savebmp operator.
 */
class LogicalSaveBmp: public LogicalOperator
{
  public:
    /**
     * Sets parameters to one input array and one string constant.
     */
    LogicalSaveBmp(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
              })
            }
        };
        return &argSpec;
    }

    /**
     * Checks to make sure that the input schema and infers a sample single-cell result schema.
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& input = schemas[0];
        if(input.getDimensions().size() != 2)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must be two-dimensional";
        }

        if(input.getDimensions()[0].isMaxStar() ||
           input.getDimensions()[1].isMaxStar())
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must be within valid range";
        }

        if(input.getAttributes().size() < 3)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION) << "Input to savebmp must have at least 3 attributes";
        }

        for (const auto& attr : input.getAttributes(true))
        {
            if (attr.getType() != TID_UINT8)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                     SCIDB_LE_ILLEGAL_OPERATION) <<
                    "The first 3 attributes of the input to savebmp must be of type uint8";
            }
        }

        Attributes outputAttrs;
        outputAttrs.push_back(AttributeDesc(
                                  "status", TID_STRING, 0, CompressorType::NONE));
        outputAttrs.push_back(AttributeDesc(
                                  "file_size", TID_DOUBLE, 0, CompressorType::NONE));

        Dimensions outputDims;
        outputDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));

        stringstream ss;
        ss << 0;
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("savbmp_output", outputAttrs, outputDims,
                         localDist,
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSaveBmp, "savebmp");


} //namespace
