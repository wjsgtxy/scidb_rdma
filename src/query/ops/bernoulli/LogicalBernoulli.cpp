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
 * LogicalBernoulli.cpp
 *
 *  Created on: Feb 14, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <system/Exceptions.h>


using namespace std;

namespace scidb {

/**
 * @brief The operator: bernoulli().
 *
 * @par Synopsis:
 *   bernoulli( srcArray, probability [, seed] )
 *
 * @par Summary:
 *   Evaluates whether to include a cell in the result array by generating a random number and
 *   checks if it is less than probability.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - probability: the probability threshold, in [0..1]
 *   - an optional seed for the random number generator.
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
 *   - bernoulli(A, 0.5, 100) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalBernoulli: public LogicalOperator
{
public:
    LogicalBernoulli(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_DOUBLE)), // probability
                 RE(RE::QMARK, {
                         RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))  // seed
                 })
              })
            },
            {"seed", RE(PP(PLACEHOLDER_CONSTANT, TID_INT32)) }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        // ArrayDesc consumes the new copy, source is discarded.
        return schemas[0].addEmptyTagAttribute();
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalBernoulli, "bernoulli")

} //namespace
