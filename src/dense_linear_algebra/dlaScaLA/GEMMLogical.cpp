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

// C++
// std C
#include <stdlib.h>
// de-facto standards
// SciDB
#include <log4cxx/logger.h>
#include <query/LogicalOperator.h>
#include <query/AutochunkFixer.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/BlockCyclic.h>

// MPI/ScaLAPACK
#include <scalapackUtil/dimUtil.hpp>
#include <scalapackUtil/ScaLAPACKLogical.hpp>
// local
#include "GEMMOptions.hpp"
#include "DLAErrors.h"

using namespace std;
using namespace scidb;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.libdense_linear_algebra.ops.gemm"));

namespace scidb
{
 ///
 /// @brief The operator: gemm().
 ///
 /// @par Synopsis:
 ///   gemm( leftArray, rightArray, accumulateArray )
 ///
 /// @par Summary:
 ///   Produces a result array via matrix multiplication of leftArray with rightArray and addition of accumulateArray
 ///   All matrices must have a single numeric attribute of type 'double', two dimensions, and the chunk size of 32x32
 ///   leftArray and rightArray must have the same size of 'inner' dimension, i.e. leftArray second dimension and rightArray first dimension.
 ///    acumulateArray must have the shape of a matrix-multiplication-product, i.e. leftArray first dimension by rightArray second dimension.
 ///
 /// @par Input:
 ///   - leftArray: the left matrix with two dimensions: leftDim1, leftDim2
 ///   - rightArray: the right matrix with two dimensions: rightDim1, rightDim2
 ///
 /// @par Output array:
 ///        <
 ///   <br>   <double:gemm>: the result attribute
 ///   <br> >
 ///   <br> [
 ///   <br>   leftDim1
 ///   <br>   rightDim2
 ///   <br> ]
 ///
 /// @par Examples:
 ///   n/a
 ///
 /// @par Errors:
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR2 -- if attribute count != 1
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR5 -- if attribute type is not double in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR3 -- if number of dimensions != 2 in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR9 -- if sizes are not bounded in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR41 -- if chunk interval is too small in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR42 -- if chunk interval is too large in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR40 -- if there is chunk overlap in any of the arrays
 ///   DLANameSpace:SCIDB_SE_INFER_SCHEMA:DLA_ERROR10 -- if the chunk sizes in any of the input arrays are not identical (until auto-repart is working)
 ///
 /// @par Notes:
 ///   n/a
 ///
class GEMMLogical: public LogicalOperator
{
    AutochunkFixer _fixer;

public:
    GEMMLogical(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Disallow dataframe input.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT))
              })
            },
            // TODO: Note that TRANS is the standard ScaLAPACK shorthand for transpose or conjugate transpose
            // TODO: Once checked in, give S.Marcus the pointer to the TRANS,ALPHA,BETA doc from ScaLAPACK
            //       and have him include the pointer to the netlib page "for more detail"
            {"transa", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            {"transb", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            {"alpha", RE(PP(PLACEHOLDER_CONSTANT, TID_DOUBLE)) },
            {"beta", RE(PP(PLACEHOLDER_CONSTANT, TID_DOUBLE)) }
        };
        return &argSpec;
    }

    std::string getInspectable() const override
    {
        return _fixer.str();
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query);
};

ArrayDesc GEMMLogical::inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
{
    LOG4CXX_TRACE(logger, "GEMMLogical::inferSchema(): begin.");

    enum dummy  {ROW=0, COL=1};
    enum dummy2 {AA=0, BB, CC, NUM_MATRICES};  // which matrix: f(AA,BB,CC) = alpha AA BB + beta CC

    //
    // array checks (first 3 arguments)
    //
    assert(schemas.size() == NUM_MATRICES);
    checkScaLAPACKLogicalInputs(schemas, query, NUM_MATRICES, NUM_MATRICES);

    GEMMOptions options(_kwParameters, /*logicalOp:*/true);

    //
    // cross-matrix constraints:
    //

    // check: cross-argument sizes
    if (nCol(schemas[AA], options.transposeA) != nRow(schemas[BB], options.transposeB)) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first matrix columns must equal second matrix rows (after optional transposes) ");
    }
    if (nRow(schemas[AA], options.transposeA) != nRow(schemas[CC])) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first and third matrix must have equal number of rows (after optional 1st matrix transpose)");
    }
    if (nCol(schemas[BB], options.transposeB) != nCol(schemas[CC])) {
        throw (PLUGIN_USER_EXCEPTION(DLANameSpace, SCIDB_SE_INFER_SCHEMA, DLA_ERROR4)
               << "first and third matrix must have equal number of columns (after optional 1st matrix transpose)");
    }

    // TODO: check: ROWS * COLS is not larger than largest ScaLAPACK fortran INTEGER

    // TODO: check: total size of "work" to scalapack is not larger than largest fortran INTEGER
    //       hint: have Cmake adjust the type of slpp::int_t
    //       hint: maximum ScaLAPACK WORK array is usually determined by the function and its argument sizes


    //
    // inputs look good, create and return the output schema
    // note that the output has the dimensions and name bases of the third argument C
    // so that we can iterate on C, by repeating the exact same query,
    // NOTE: we are SUPER careful not to change its dim names if they are already distinct.
    //       to make the iteration as simple as possible
    //
    const Dimensions& dimsCC = schemas[CC].getDimensions();

    std::pair<string, string> distinctNames = ScaLAPACKDistinctDimensionNames(dimsCC[ROW].getBaseName(),
                                                                              dimsCC[COL].getBaseName());
    _fixer.clear();
    Dimensions outDims(2);
    outDims[ROW] = DimensionDesc(distinctNames.first,
                                 dimsCC[ROW].getStartMin(),
                                 dimsCC[ROW].getCurrStart(),
                                 dimsCC[ROW].getCurrEnd(),
                                 dimsCC[ROW].getEndMax(),
                                 dimsCC[ROW].getRawChunkInterval(),
                                 0);
    _fixer.takeDimension(ROW).fromArray(CC).fromDimension(ROW);

    outDims[COL] = DimensionDesc(distinctNames.second,
                                 dimsCC[COL].getStartMin(),
                                 dimsCC[COL].getCurrStart(),
                                 dimsCC[COL].getCurrEnd(),
                                 dimsCC[COL].getEndMax(),
                                 dimsCC[COL].getRawChunkInterval(),
                                 0);
    _fixer.takeDimension(COL).fromArray(CC).fromDimension(COL);

    Attributes atts;
    atts.push_back(AttributeDesc("gemm", TID_DOUBLE, 0, CompressorType::NONE));

    LOG4CXX_TRACE(logger, "GEMMLogical::inferSchema(): end.");
    // ArrayDesc consumes the new copy, source is discarded.
    return ArrayDesc("GEMM", atts.addEmptyTagAttribute(), outDims,
                     scidb::createDistribution(dtUndefined),
                     query->getDefaultArrayResidency());
}

REGISTER_LOGICAL_OPERATOR_FACTORY(GEMMLogical, "gemm");

} //namespace
