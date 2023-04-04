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
 * LogicalSlice.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/AutochunkFixer.h>
#include <query/UserQueryException.h>

namespace scidb {

/**
 * @brief The operator: slice().
 *
 * @par Synopsis:
 *   slice( srcArray {, dim, dimValue}* )
 *
 * @par Summary:
 *   Produces a 'slice' of the source array, by holding zero or more dimension values constant.
 *   The result array does not include the dimensions that are used for slicing.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - dim: one of the dimensions to be used for slicing.
 *   - dimValue: the constant value in the dimension to slice.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims less the list of slicing dims
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
class LogicalSlice: public LogicalOperator
{
    AutochunkFixer _fixer;

public:
    LogicalSlice(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Dataframe inputs make no sense.
    }

    std::string getInspectable() const override
    {
        return _fixer.str();
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_DIMENSION_NAME)),  // dimension to slice by
                    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))  // value
                 })
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& schema = schemas[0];
        Dimensions const& dims = schema.getDimensions();
        size_t nDims = dims.size();
        size_t nParams = _parameters.size();
        assert((nParams & 1) == 0 || nParams >= nDims*2);
        Dimensions newDims(nDims - nParams/2);
        if (newDims.size() <= 0)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_SLICE_ERROR1);
        std::vector<std::string> sliceDimName(nParams/2);
        for (size_t i = 0; i < nParams; i+=2) {
            sliceDimName[i >> 1]  = ((std::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectName();
        }
        _fixer.clear();
        size_t j = 0;
        for (size_t i = 0; i < nDims; i++) {
            const std::string dimName = dims[i].getBaseName();
            int k = safe_static_cast<int>(sliceDimName.size());
            while (--k >= 0
                   && sliceDimName[k] != dimName
                   && !(sliceDimName[k][0] == '_' && (size_t)atoi(sliceDimName[k].c_str()+1) == i+1));
            if (k < 0)
            {
                if (j >= newDims.size())
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DUPLICATE_DIMENSION_NAME,
                                               _parameters[i]->getParsingContext()) << dimName;
                _fixer.takeDimension(j).fromArray(0).fromDimension(i);
                newDims[j++] = dims[i];
            }
        }

        return ArrayDesc(schema.getName(), schema.getAttributes(), newDims,
                         createDistribution(dtUndefined),
                         schema.getResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSlice, "slice")


}  // namespace ops
