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
///
/// ScaLAPACKLogical.hpp
///
///

#ifndef SCALAPACK_LOGICAL_HPP_
#define SCALAPACK_LOGICAL_HPP_


// std C
// std C++
// de-facto standards

// SciDB

#include <query/Query.h>
#include <query/LogicalOperator.h>
#include <system/Exceptions.h>
#include <system/BlockCyclic.h>
// MPI/ScaLAPACK
#include <scalapackUtil/dimUtil.hpp>

// locals
#include "DLAErrors.h"
#include "scalapackFromCpp.hpp"   // TODO JHM : rename slpp::int_t


namespace scidb {

/// no actual ScaLAPACKLogical class yet,
/// just helpers for the Logicals are all that are needed so far

/// Check dimensions from logical inferSchema().
/// @returns nothing
/// @throw if the input schemas are not suitable for ScaLAPACK processing
/// @see checkScaLAPACKInputs
///
void checkScaLAPACKLogicalInputs(std::vector<ArrayDesc> const& schemas,
                                 std::shared_ptr<Query> query,
                                 size_t nMatsMin, size_t nMatsMax);


/// Check dimensions from physical execute()
///
/// @returns nothing
/// @throw if the input arrays are not suitable for ScaLAPACK processing
/// @see checkScaLAPACKSchemas
///
/// @note This and the previous routine are wrappers around the same logic.  Because some schema
/// dimensions are autochunked (that is, their chunk intervals are not known at logical
/// inferSchema() time), their intervals must be rechecked for ScaLAPACK acceptability at
/// PhysicalOperator::execute() time.  Rather than perform major surgery to get just the necessary
/// rechecks, it seemed clearer to wrap the same logic, and assert that only errors we might expect
/// at physical execute time actually occur.
///
/// @note Ideally this call would be made just once for the query, from
/// PhysicalFoo::preSingleExecute().  Unfortunately the input schemas are not available for
/// inspection there.
///
void checkScaLAPACKPhysicalInputs(std::vector< std::shared_ptr<Array> > const& inputs,
                                  std::shared_ptr<Query> query,
                                  size_t nMatsMin, size_t nMatsMax);


/// constructs distinct dimension names, from names that may or may not be distinct
std::pair<std::string, std::string> ScaLAPACKDistinctDimensionNames(const std::string& a, const std::string& b);

void log4cxx_debug_dimensions(const std::string& prefix, const Dimensions& dims);

} // namespace

#endif /* SCALAPACK_LOGICAL_HPP_ */
