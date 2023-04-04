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
 * LogicalQuantile.cpp
 *  Created on: Mar 11, 2011
 *      Author: poliocough@gmail.com
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/AutochunkFixer.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/Query.h>
#include <query/UserQueryException.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: quantile().
 *
 * @par Synopsis:
 *   quantile( srcArray, numQuantiles [, attr {, groupbyDim}*] )
 *
 * @par Summary:
 *   Computes the quantiles of an array, based on the ordering of attr (within each group as specified by groupbyDim, if specified).
 *   If groupbyDim is not specified, global ordering will be performed.
 *   If attr is not specified, the first attribute will be used.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - numQuantiles: the number of quantiles.
 *   - attr: which attribute to sort on. The default is the first attribute.
 *   - groupbyDim: if provided, the ordering will be performed among the records in the same group.
 *
 * @par Output array:
 *        <
 *   <br>   percentage: a double value from 0.0 to 1.0
 *   <br>   attr_quantile: the source attribute name followed by '_quantile'.
 *   <br> >
 *   <br> [
 *   <br>   groupbyDims (if provided)
 *   <br>   quantile: start=0, end=numQuantiles, chunkInterval=numQuantiles+1
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <v:int64> [i=0:5,3,0] =
 *     <br> i,  v
 *     <br> 0,  0
 *     <br> 1,  1
 *     <br> 2,  2
 *     <br> 3,  3
 *     <br> 4,  4
 *     <br> 5,  5
 *   - quantile(A, 2) <percentage, v_quantile>[quantile=0:2,3,0] =
 *     <br> {quantile} percentage, v_quantile
 *     <br> {0}           0,         0
 *     <br> {1}           0.5,       2
 *     <br> {2}           1,         5
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalQuantile: public LogicalOperator
{
    AutochunkFixer _fixer;

public:
    LogicalQuantile(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_UINT32)),
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


    std::string getInspectable() const override { return _fixer.str(); }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        ArrayDesc const& input = schemas[0];

        assert(schemas.size() == 1);

        auto lexp = param<OperatorParamLogicalExpression>(0)->getExpression();
        uint32_t numQuantilesPlusOne = 1 + evaluate(lexp, TID_UINT32).getUint32();
        if (numQuantilesPlusOne < 2) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR17,
                _parameters[0]->getParsingContext());
        }

        string attName = _parameters.size() > 1
            ? param<OperatorParamReference>(1)->getObjectName()
            : input.getAttributes(true).firstDataAttribute().getName();

        bool found = false;
        AttributeDesc inputAttribute;
        for (size_t i = 0; i < input.getAttributes().size(); ++i)
        {
            auto const& att = input.getAttributes().findattr(i);
            if (att.getName() == attName)
            {
                found = true;
                inputAttribute = att;
            }
        }
        if (!found) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR14);
        }

        Attributes outputAttrs;
        outputAttrs.push_back(
            AttributeDesc("percentage", TID_DOUBLE, 0,CompressorType::NONE));
        outputAttrs.push_back(
            AttributeDesc(attName + "_quantile", inputAttribute.getType(),
                          AttributeDesc::IS_NULLABLE, CompressorType::NONE));

        _fixer.clear();
        Dimensions outputDims;
        if (_parameters.size()>2)
        {
            if (input.isDataframe()) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_BAD_DATAFRAME_USAGE)
                    << getLogicalName() << "Dataframe cells cannot be ranked along dimensions";
            }

            size_t i, j;
            for (i = 0; i < _parameters.size() - 2; i++)
            {
                auto paramRef = param<OperatorParamReference>(i + 2);
                const string& dimName = paramRef->getObjectName();
                const string& dimAlias = paramRef->getArrayName();
                for (j = 0; j < input.getDimensions().size(); j++)
                {
                    DimensionDesc const& dim = input.getDimensions()[j];
                    if (dim.hasNameAndAlias(dimName, dimAlias))
                    {
                        if (dim.getEndMax() == CoordinateBounds::getMax()) {
                            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_QUANTILE_REQUIRES_BOUNDED_ARRAY);
                        }

                        //no overlap
                        _fixer.takeDimension(outputDims.size()).fromArray(0).fromDimension(j);
                        outputDims.push_back(DimensionDesc( dim.getBaseName(),
                                                            dim.getNamesAndAliases(),
                                                            dim.getStartMin(),
                                                            dim.getCurrStart(),
                                                            dim.getCurrEnd(),
                                                            dim.getEndMax(),
                                                            dim.getRawChunkInterval(),
                                                            0));
                        break;
                    }
                }
                if (j >= input.getDimensions().size()) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DLA_ERROR16);
                }
            }
        }

        DimensionDesc quantileDim(
                "quantile",
                0,                     // startMin
                0,                     // currStart
                numQuantilesPlusOne-1, // currEnd
                numQuantilesPlusOne-1, // endMax
                numQuantilesPlusOne,   // chunkInterval
                0);                    // chunkOverlap
        outputDims.push_back(quantileDim);

        return ArrayDesc(input.getName()+"_quantile", outputAttrs, outputDims,
                         createDistribution(dtUndefined),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalQuantile, "quantile")

}
