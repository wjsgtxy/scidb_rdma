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
 * LogicalSubArray.cpp
 *
 *  Created on: May 20, 2010
 *      Author: knizhnik@garret.ru
 */

#include "SubArray.h"

#include <query/LogicalOperator.h>

#include <query/Expression.h>
#include <system/Exceptions.h>

namespace scidb
{

/***
 * Helper function to set the dimension start and length properties in the array descriptor.
 * Constructs a new array descriptor with the appropriate dimensions.
 ***/
ArrayDesc setDimensions(ArrayDesc desc, Coordinates& lowPos,
                        Coordinates& highPos,
                        std::shared_ptr<Query> const& query)
{
    Dimensions dims = desc.getDimensions();
    Dimensions newDims(dims.size());

    for (size_t i = 0, n = dims.size(); i < n; i++) {
        DimensionDesc const& srcDim = dims[i];
        size_t end = std::max(highPos[i] - lowPos[i],0L);
        newDims[i] = DimensionDesc(srcDim.getBaseName(),
                srcDim.getNamesAndAliases(), 0, 0, end,
                end, srcDim.getRawChunkInterval(),
                srcDim.getChunkOverlap());
    }

    /***
     * FIXME: Don't really know what are the number of cells and the size of the array
     **/
    return ArrayDesc(desc.getName(),
                     desc.getAttributes(),
                     newDims,
                     createDistribution(dtUndefined),
                     desc.getResidency());
}

/**
 * @brief The operator: subarray().
 *
 * @par Synopsis:
 *   subarray( srcArray {, lowCoord}+ {, highCoord}+ )
 *
 * @par Summary:
 *   Produces a result array from a specified, contiguous region of a source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - the low coordinates
 *   - the high coordinates
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims, where the range of every dimension is reduced to [0..windowSize-1]
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
 *   - subarray(A, 2011, 1, 2012, 2) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br>  0,     1,      7,     31.64
 *     <br>  1,     0,      5,     41.65
 *     <br>  1,     1,      9,     40.68
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Almost the same as between(). The only difference is that the dimensions are "cropped".
 *
 */
class LogicalSubArray: public LogicalOperator {
public:
    LogicalSubArray(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Dataframe input not allowed.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::PLUS, {
                     RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)),
                     RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))
                 })
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas,
                          std::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 1);
        if (_parameters.size() != 0 && _parameters.size() != inputSchemas[0].getDimensions().size() * 2)
        {
            std::stringstream ss; ss << "zero or exactly "<< inputSchemas[0].getDimensions().size() << " pairs of coordinates";
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3)
                  << "subarray" << ss.str();
        }

        for (Parameters::const_iterator it = _parameters.begin();
                it != _parameters.end(); ++it) {
            assert(
                    ((std::shared_ptr<OperatorParam>&)*it)->getParamType() == PARAM_LOGICAL_EXPRESSION);
            assert(
                    ((std::shared_ptr<OperatorParamLogicalExpression>&)*it)->isConstant());
        }

        ArrayDesc& desc = inputSchemas[0];
        Dimensions const& dims = desc.getDimensions();
        size_t nDims = dims.size();

        // Fetch the low and high coordinates of the subarray window from the operator parameters
        Coordinates lowPos(nDims);
        Coordinates highPos(nDims);

        for (size_t i = 0; i < nDims; i++) {
            Value const& low =
                evaluate(
                    ((std::shared_ptr<
                        OperatorParamLogicalExpression>&) _parameters[i])->getExpression(), TID_INT64);
            if ( low.isNull()  || low.getInt64() < dims[i].getStartMin())
            {
                lowPos[i] = dims[i].getStartMin();
            }
            else
            {
                lowPos[i] = low.getInt64();
            }
            Value const& high =
                evaluate(
                    ((std::shared_ptr<
                        OperatorParamLogicalExpression>&) _parameters[i + nDims])->getExpression(), TID_INT64);
            if ( high.isNull() || high.getInt64() > dims[i].getEndMax())
            {
                highPos[i] = dims[i].getEndMax();
            }
            else
            {
                highPos[i] = high.getInt64();
            }
            if (lowPos[i] > highPos[i])
            {
                highPos[i] = lowPos[i] - 1;
                /*
                     throw USER_QUERY_EXCEPTION(SCIDB_E_WRONG_SYNTAX,
                     "Invalid coordinate range",
                     _parameters[i]->getParsingContext());
                 */
            }
        }

        /***
         * We first create a physical schema for the array and modify the dimension start and length
         * parameters.
         */
        return setDimensions(desc, lowPos, highPos, query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSubArray, "subarray")

} // namespace scidb
