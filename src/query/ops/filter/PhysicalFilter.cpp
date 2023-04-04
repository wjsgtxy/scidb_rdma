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
 * PhysicalFilter.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <array/SpatialRangesChunkPosIterator.h>
#include "query/PhysicalOperator.h"

#include "array/Array.h"
#include "query/ops/filter/FilterArray.h"
#include "../between/BetweenArray.h"

namespace scidb {

using namespace std;


class PhysicalFilter : public PhysicalOperator
{
public:
   PhysicalFilter(const std::string& logicalName,
                  const std::string& physicalName,
                  const Parameters& parameters,
                  const ArrayDesc& schema)
           : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const override
    {
        // TODO:OPTAPI optimization opportunities here
        return inputBoundaries[0];
    }

    /// @brief Wrapper of Expression::extractSpatialConstraints()
    void extractSpatialConstraints(
        std::shared_ptr<SpatialRanges>& spatialRangesPtr,
        bool& hasOtherClauses) const
    {
        assert(_parameters.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        auto param = dynamic_pointer_cast<OperatorParamPhysicalExpression>(_parameters[0]);
        assert(param);
        auto expr = param->getExpression();
        assert(expr);

        if (!expr->isDeterministic()) {
            // filter() only accepts expressions that are deterministic.
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CONSTANT_EXPRESSION_EXPECTED);
        }

        // In general, a FilterArray is returned.
        // Our goal is to improve performance, by extracting dimension-range conditions which enables a BetweenArray.
        // The rationale is that filtering on dimension ranges is way faster than filtering on attributes.

        const size_t numDims = _schema.getDimensions().size();
        spatialRangesPtr = std::make_shared<SpatialRanges>(numDims);
        expr->extractSpatialConstraints(spatialRangesPtr, hasOtherClauses);
    }

    /**
     * Filter is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        // In general, a FilterArray is returned.
        // Our goal is to improve performance, by extracting dimension-range conditions which enables a BetweenArray.
        // The rationale is that filtering on dimension ranges is way faster than filtering on attributes.

        std::shared_ptr<SpatialRanges> spatialRangesPtr;
        bool hasOtherClauses = false;
        extractSpatialConstraints(spatialRangesPtr, hasOtherClauses);

        auto ret = inputArrays[0];
        if (hasOtherClauses || spatialRangesPtr->ranges().empty()) {
            ret = std::make_shared<FilterArray>(
                _schema, ret,
                dynamic_pointer_cast<OperatorParamPhysicalExpression>(_parameters[0])->getExpression(),
                query, _tileMode);
        }
        if (!spatialRangesPtr->ranges().empty()) {
            if (_tileMode) {
                // MaterializedArray(BetweenArray(MaterializedArray()))
                ret = std::make_shared<MaterializedArray>(ret, query);
                ret = std::make_shared<BetweenArray>(_schema, spatialRangesPtr, ret);
                ret = std::make_shared<MaterializedArray>(ret, query);
            } else {
                ret = std::make_shared<BetweenArray>(_schema, spatialRangesPtr, ret);
            }
        }
        return ret;
    }

};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFilter, "filter", "physicalFilter")

}  // namespace scidb
