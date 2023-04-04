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

#ifndef LOGICAL_XGRID_H
#define LOGICAL_XGRID_H

/*
 * @file LogicalXgrid.h
 */

#include <query/LogicalOperator.h>

namespace scidb {

/**
 * @brief The operator: xgrid().
 *
 * @par Synopsis:
 *   xgrid( srcArray {, scale}+ )
 *
 * @par Summary:
 *   Produces a result array by 'scaling up' the source array.  Within each dimension, the operator
 *   duplicates each cell a specified number of times before moving to the next cell.  A scale must
 *   be provided for every dimension.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - scale: for each dimension, a scale is provided telling how much larger the dimension should grow.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims where every dimension is expanded by a given scale
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
 *   - xgrid(A, 1, 2) <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  3,      7,     31.64
 *     <br> 2011,  4,      7,     31.64
 *     <br> 2011,  5,      6,     19.98
 *     <br> 2011,  6,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      5,     41.65
 *     <br> 2012,  3,      9,     40.68
 *     <br> 2012,  4,      9,     40.68
 *     <br> 2012,  5,      8,     26.64
 *     <br> 2012,  6,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalXgrid: public LogicalOperator
{
public:
    typedef int32_t ScaleFactor;
    typedef std::vector<ScaleFactor> ScaleFactors;

    LogicalXgrid(const std::string& logicalName, const std::string& alias);

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query);

    /// Intended for use by PhysicalXgrid only.  These are *not* identical to the parameter list!
    ScaleFactors const& getScaleFactors() const { return _scaleFactors; }

    static PlistSpec const* makePlistSpec();

private:
    ScaleFactors    _scaleFactors;
};

}  // namespace scidb

#endif
