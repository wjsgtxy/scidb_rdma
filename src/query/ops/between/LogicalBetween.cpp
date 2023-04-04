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
 * LogicalBetween.cpp
 *
 *  Created on: Oct 22, 2010
 *      Author: knizhnik@garret.ru
 */

#include <query/LogicalOperator.h>

#include <system/Exceptions.h>


namespace scidb {

/**
 * @brief The operator: between().
 *
 * @par Synopsis:
 *   between( srcArray {, lowCoord}+ {, highCoord}+ )
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
 *   - between(A, 2011, 1, 2012, 2) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - Almost the same as subarray. The only difference is that the dimensions
 *     retain the original start/end/boundaries.
 *
 */
class LogicalBetween: public  LogicalOperator
{
  public:
    LogicalBetween(const std::string& logicalName, const std::string& alias)
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

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        ArrayDesc const& desc = schemas[0];
        size_t nDims = desc.getDimensions().size();

        if (nDims * 2 != _parameters.size()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENT2)
                << "an integer specifying the bounding coordinate.";
        }

        return schemas[0].addEmptyTagAttribute();
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBetween, "between")


}  // namespace scidb
