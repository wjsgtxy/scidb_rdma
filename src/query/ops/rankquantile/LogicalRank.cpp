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
 * LogicalRank.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <memory>

#include <query/LogicalOperator.h>
#include <system/Exceptions.h>
#include <query/LogicalExpression.h>
#include "RankCommon.h"

using namespace std;

namespace scidb
{

/**
 * @brief The operator: rank().
 *
 * @par Synopsis:
 *   rank( srcArray [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Computes the rankings of an array, based on the ordering of attr (within each group as specified by the list of groupbyDims, if provided).
 *   If groupbyDims is not specified, global ordering will be performed.
 *   If attr is not specified, the first attribute will be used.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - attr: which attribute to sort on. The default is the first attribute.
 *   - groupbyDim: if provided, the ordering will be performed among the records in the same group.
 *
 * @par Output array:
 *        <
 *   <br>   attr: only the specified attribute in srcAttrs is retained.
 *   <br>   attr_rank: the source attribute name followed by '_rank'.
 *   <br> >
 *   <br> [
 *   <br>   srcDims: the shape does not change.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalRank: public LogicalOperator
{
public:
    LogicalRank(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_ATTRIBUTE_NAME)),
                    RE(RE::STAR, {
                       RE(PP(PLACEHOLDER_DIMENSION_NAME))
                    })
                 })
              })
            },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        ArrayDesc const& input = schemas[0];

        assert(schemas.size() == 1);

        string attName = _parameters.empty()
            ? input.getAttributes(true).firstDataAttribute().getName()
            : param<OperatorParamReference>(0)->getObjectName();

        const AttributeDesc* inputAttributeID = nullptr;
        for (const auto& att : input.getAttributes())
        {
            if (att.getName() == attName)
            {
                inputAttributeID = &att;
                break;
            }
        }
        if (!inputAttributeID) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR14);
        }

        assert (!inputAttributeID->isEmptyIndicator());

        Dimensions dims = input.getDimensions();
        if (_parameters.size()>1)
        {
            if (input.isDataframe()) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_BAD_DATAFRAME_USAGE)
                    << getLogicalName() << "Dataframe cells cannot be ranked along dimensions";
            }

            vector<int> groupBy(_parameters.size()-1);
            size_t i, j;
            for (i = 0; i < _parameters.size() - 1; i++)
            {
                auto paramRef = param<OperatorParamReference>(i + 1);
                const string& dimName = paramRef->getObjectName();
                const string& dimAlias = paramRef->getArrayName();
                for (j = 0; j < dims.size(); j++)
                {
                    if (dims[j].hasNameAndAlias(dimName, dimAlias))
                    {
                        break;
                    }
                }
                if (j >= dims.size()) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
                }
            }
        }

       return getRankingSchema(input, query, *inputAttributeID);
    }
};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRank, "rank")

}
