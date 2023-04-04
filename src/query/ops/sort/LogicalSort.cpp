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
 * LogicalSort.cpp
 *
 *  Created on: May 6, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include "PrettySortArray.h"

#include <array/SortArray.h>
#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: sort().
 *
 * @par Synopsis:
 *   sort( srcArray {, attr [asc | desc]}* {, CHUNK_SIZE} )
 *   sort( srcArray {, attr [asc | desc]}* {, chunk_size: CHUNK_SIZE} )
 *
 * @par Summary:
 *   Produces a 1D array by sorting the non-empty cells of a source array.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - attr: the list of attributes to sort by. If no attribute is provided, the first attribute will be used.
 *   - asc | desc: whether ascending or descending order of the attribute should be used. The default is asc.
 *   - chunkSize: the size of a chunk in the result array. If not provided, 1M will be used.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs: all the attributes are retained.
 *   <br> >
 *   <br> [
 *   <br>   n: start=0, end=CoordinateBounds::getMax(), chunk interval = min{defaultChunkSize, #logical cells in srcArray)
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   Assuming null < NaN < other values
 *
 */
class LogicalSort: public LogicalOperator
{
public:
    LogicalSort(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    { }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR,  { RE(PP(PLACEHOLDER_ATTRIBUTE_NAME)) }),     // key(s)
                 RE(RE::QMARK, { RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) }) // chunk_size
              })
            },

            // keywords
            { "chunk_size", RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { "dimension",  RE(PP(PLACEHOLDER_DIMENSION_NAME).setMustExist(false)) },
            { "_coords",    RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        // As far as chunk sizes, they can be a pain! So we allow the user to specify an optional chunk size
        // as part of the sort op.

        assert(schemas.size() >= 1);
        ArrayDesc const& inSchema = schemas[0];
        size_t chunkSize = 0;
        Parameter chunkParam = findKeyword("chunk_size");
        // First expression parameter is the positional chunk_size.
        for (size_t i =0; i<_parameters.size(); i++)
        {
            if(_parameters[i]->getParamType()==PARAM_LOGICAL_EXPRESSION)
            {
                if (chunkParam) {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL)
                        << "sort" << "chunk_size" << (i + 1);
                }
                chunkParam = _parameters[i];
                break;
            }
        }
        if (chunkParam) {
            chunkSize = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)chunkParam)->getExpression(),
                                 TID_INT64).getInt64();
            if(chunkSize <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
            }
        }

        string dimName("$n");
        Parameter dim = findKeyword("dimension");
        if (dim) {
            auto dimRef = dynamic_pointer_cast<OperatorParamDimensionReference>(dim);
            assert(dimRef);
            dimName = dimRef->getObjectName();
        }

        bool wantCoords = SORT_DEFAULT_WANT_COORDS;
        Parameter coords = findKeyword("_coords");
        if (coords) {
            auto lExp = dynamic_pointer_cast<OperatorParamLogicalExpression>(coords);
            assert(lExp);
            wantCoords = evaluate(lExp->getExpression(), TID_BOOL).getBool();
        }

        // the residency (of the input) is not quite right,
        // but it will get corrected in PhysicalSort::getOutputDistribution()
        if (wantCoords) {
            return PrettySortArray::makeOutputSchema(inSchema, dimName, chunkSize);
        } else {
            SortArray sorter(inSchema, arena::getArena());
            sorter.setChunkSize(chunkSize)
                .setDimName(dimName);
            return sorter.getOutputSchema(); // *Not* the "expanded" schema.
        }
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSort, "sort")


}  // namespace scidb
