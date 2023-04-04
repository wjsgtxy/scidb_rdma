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
 * LogicalXgrid.cpp
 *
 *  Created on: Jul 19, 2010
 *      Author: Knizhnik
 */

#include "LogicalXgrid.h"

#include <query/Expression.h>
#include <system/Exceptions.h>

namespace scidb {

using namespace std;

LogicalXgrid::LogicalXgrid(const std::string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
{
    _properties.dataframe = false; // Dataframe inputs make no sense.
}

PlistSpec const* LogicalXgrid::makePlistSpec()
{
    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_INPUT)),
             RE(RE::PLUS, {
                RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))
             })
          })
        }
    };
    return &argSpec;

}

ArrayDesc LogicalXgrid::inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
{
    assert(schemas.size() == 1);
    if (_parameters.size() != schemas[0].getDimensions().size())
    {
        stringstream ss; ss << "exactly "<< schemas[0].getDimensions().size() << " scale values";

        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT)
            << "xgrid" << ss.str();
    }



    ArrayDesc const& desc = schemas[0];
    Dimensions const& dims = desc.getDimensions();
    size_t nDims = dims.size();

    // (No need for an AutochunkFixer: obeying the scale factors will fix the intervals.)
    _scaleFactors.clear();
    _scaleFactors.resize(nDims, 1);
    Dimensions newDims(nDims);
    int32_t grid;
    bool autochunk = false;

    for (size_t i = 0; i < nDims; ++i)
    {
        // Get grid scaling factor and validate it.
        if (desc.getDimensions()[i].isMaxStar()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_XGRID_ERROR1);
        }
        grid = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[i])->getExpression(),
                         TID_INT32).getInt32();
        if (grid <= 0) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_XGRID_ERROR2);
        }

        // Create corresponding new dimension based on scaled-up old dimension.  If the old
        // dimension was autochunked, we leave it autochunked... but save the scale factor in _scaleFactors
        // so that scaling can occur in physical execute().
        //
        DimensionDesc const& srcDim = dims[i];
        int64_t chunkInterval = srcDim.getRawChunkInterval();
        if (chunkInterval == DimensionDesc::AUTOCHUNKED) {
            // Scale it later.
            _scaleFactors[i] = grid;
            autochunk = true;
        } else {
            // Scale it now.
            chunkInterval *= grid;
        }

        newDims[i] = DimensionDesc(srcDim.getBaseName(),
                                   srcDim.getNamesAndAliases(),
                                   srcDim.getStartMin(),
                                   srcDim.getCurrStart(),
                                   srcDim.getCurrStart() + srcDim.getLength()*grid - 1,
                                   srcDim.getStartMin() + srcDim.getLength()*grid - 1,
                                   chunkInterval, 0);
    }

    if (!autochunk) {
        // Tell physical operator: no need to do any scaling at all.
        _scaleFactors.clear();
    }

    return ArrayDesc(desc.getName(), desc.getAttributes(), newDims,
                     desc.getDistribution(),
                     desc.getResidency());
}


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalXgrid, "xgrid")

}  // namespace scidb
