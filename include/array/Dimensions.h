#ifndef DIMENSIONS_H_
#define DIMENSIONS_H_

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

#include <iosfwd>
#include <memory>
#include <vector>

namespace scidb {

class DimensionDesc;

/**
 * Vector of DimensionDesc type
 */
typedef std::vector<DimensionDesc> Dimensions;

void printDimNames(std::ostream&, const Dimensions&);
void printSchema(std::ostream&,const Dimensions&);
std::ostream& operator<<(std::ostream&,const Dimensions&);

/** Return true iff any dimension in dims has an overlap. */
bool hasOverlap(Dimensions const& dims);

/** Return true iff any dimension in dims is autochunked. */
bool isAutochunked(Dimensions const& dims);

/** Return true iff dims is a dataframe dimension specification. */
bool isDataframe(Dimensions const& dims);

/** Return true iff cluster uses physical id for $inst dataframe dimension. */
bool dataframePhysicalIdMode();

/**
 * Create canonical dataframe dimension specification.
 *
 * @param chunkSize if specified, use as $seq dimension's chunk
 *                  length, else mark $seq dimension as "autochunked"
 *
 * @note If you change the canonical dataframe dimensions, you will
 *       probably have to write code to convert old dataframes when
 *       they are restored from backup!
 */
Dimensions makeDataframeDimensions(int64_t chunkSize = -1);

/**
 * Well-known names of dataframe dimensions.
 * @{
 */
constexpr char const * const DFD_INST = "$inst";
constexpr char const * const DFD_SEQ  = "$seq";
/** @} */

/**
 * Dimension numbers for dataframe dimensions.
 * @{
 */
constexpr size_t const DF_INST_DIM = 0;
constexpr size_t const DF_SEQ_DIM = 1;
constexpr size_t const DF_NUM_DIMS = 2;
/** @} */

}  // namespace scidb

#endif  /* ! DIMENSIONS_H_ */
