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
 * LogicalAverageRank.cpp
 *  Created on: May 11, 2011
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
 * @brief The operator: avg_rank().
 *
 * @par Synopsis:
 *   avg_rank( srcArray [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Ranks the array elements, where each element is ranked as the average of the upper bound (UB) and lower bound (LB) rankings.
 *   The LB ranking of an element E is the number of elements less than E, plus 1.
 *   The UB ranking of an element E is the number of elements less than or equal to E, plus 1.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 0 or 1 attribute to rank with. If no attribute is provided, the first attribute is used.
 *   - an optional list of groupbyDims used to group the elements, such that the rankings are calculated within each group.
 *     If no groupbyDim is provided, the whole array is treated as one group.
 *
 * @par Output array:
 *        <
 *   <br>   attr: the source attribute to rank with.
 *   <br>   attr_rank: the source attribute name, followed by '_rank'.
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - avg_rank(A, sales, year) <sales:double, sales_rank: uint64> [year, item] =
 *     <br> year, item, sales, sales_rank
 *     <br> 2011,  2,   31.64,    2
 *     <br> 2011,  3,   19.98,    1
 *     <br> 2012,  1,   41.65,    3
 *     <br> 2012,  2,   40.68,    2
 *     <br> 2012,  3,   26.64,    1
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For any element with a distinct value, its UB ranking and LB ranking are equal.
 *
 */
class LogicalAverageRank: public LogicalOperator
{
public:
    LogicalAverageRank(const std::string& logicalName, const std::string& alias):
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
            }
        }

        if (!inputAttributeID) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR14);
        }

        assert(!inputAttributeID->isEmptyIndicator());

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

                if (j >= dims.size())
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST)
                        << dimName << "input" << dims;
                }
            }
        }

        return getRankingSchema(input, query, *inputAttributeID);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAverageRank, "avg_rank")

}
