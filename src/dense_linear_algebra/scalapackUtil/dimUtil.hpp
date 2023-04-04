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
#ifndef DIMUTIL_HPP_
#define DIMUTIL_HPP_
///
/// dimUtil.hpp
///
///

#include <array/Array.h>
// std C++
// std C
#include <assert.h>

namespace scidb {

// preliminaries:

// f( {row,col}, transpose) -> dimension subscript
const bool transpose=true;

// index for row or column depends on transposition flag
inline size_t rowColIndex(bool column, bool transposed) { return (column xor transposed); }

// get dimension with optional transposition, not for direct use, see syntactic sugar below
inline const DimensionDesc& dimSubscript(const Dimensions& dims, size_t idx, bool transpose=false)
{
    assert(!transpose || idx <= 1) ;	// transpose only applies to first two dimensions

    bool actualIndex = rowColIndex(idx, transpose);
    assert(actualIndex <= dims.size());

    return dims[actualIndex];
}

//
// syntactic sugar for convenience and readability
//

// operate on Dimensions
enum RowCol_e {ROW=0, COL=1};
inline size_t nRow(const Dimensions& dims, bool transpose=false) {
    return dimSubscript(dims, ROW, transpose).getLength(); }
inline size_t nCol(const Dimensions& dims, bool transpose=false) {
    return dimSubscript(dims, COL, transpose).getLength(); }
inline unsigned int chunkRow(const Dimensions& dims, bool transpose=false) {
    return safe_static_cast<unsigned int>(dimSubscript(dims, ROW, transpose).getChunkInterval()); }
inline unsigned int chunkCol(const Dimensions& dims, bool transpose=false) {
    return safe_static_cast<unsigned int>(dimSubscript(dims, COL, transpose).getChunkInterval()); }

// operate on ArrayDesc -- handy for LogicalOperator::inferSchema() overloads
inline size_t nRow(const ArrayDesc& desc, bool transpose=false) { return nRow(desc.getDimensions(), transpose); }
inline size_t nCol(const ArrayDesc& desc, bool transpose=false) { return nCol(desc.getDimensions(), transpose); }
inline unsigned int chunkRow(const ArrayDesc& desc, bool transpose=false) { return chunkRow(desc.getDimensions(), transpose); }
inline unsigned int chunkCol(const ArrayDesc& desc, bool transpose=false) { return chunkCol(desc.getDimensions(), transpose); }

// operate on Array -- handy for PhysicalOperator::execute() overloads
inline size_t nRow(std::shared_ptr<Array>& array, bool transpose=false) { return nRow(array->getArrayDesc(), transpose); }
inline size_t nCol(std::shared_ptr<Array>& array, bool transpose=false) { return nCol(array->getArrayDesc(), transpose); }
inline unsigned int chunkRow(std::shared_ptr<Array>& array, bool transpose=false) { return chunkRow(array->getArrayDesc(), transpose); }
inline unsigned int chunkCol(std::shared_ptr<Array>& array, bool transpose=false) { return chunkCol(array->getArrayDesc(), transpose); }

} // namespace scidb

#endif /* DIMUTIL_HPP_ */

