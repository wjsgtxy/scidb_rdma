#ifndef AGGREGATEUTILS_H_
#define AGGREGATEUTILS_H_
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

#include <memory>
#include <vector>

namespace scidb {

class Aggregate;
class ArrayDistribution;
class ArrayResidency;
class PhysicalOperator;
class Query;
class Value;

using AggregatePtr = std::shared_ptr<Aggregate>;
using ArrayDistPtr = std::shared_ptr<const ArrayDistribution>;
using ArrayResPtr = std::shared_ptr<const ArrayResidency>;

/**
 * @brief Merge an aggregate states vector, cluster-wide.
 * @param aggs the aggregates
 * @param inStates the corresponding local aggregate state values
 * @param query the query we act on behalf of
 * @param phyOp the PhysicalOperator for the SG
 * @param distribution DistType for MemArray to hold inStates
 * @param residency denotes the instances involved in the query
 * @return redistributed merged state vector
 *
 * @description You have a bunch of instance-local aggregate states.
 * You want to combine your local states with those from other
 * instances to get cluster-wide (actually, residency-wide) merged
 * states.  This utility function does the job.
 */
std::vector<Value> redistributeAggregateStates(std::vector<AggregatePtr> const& aggs,
                                               std::vector<Value> const& inStates,
                                               std::shared_ptr<Query> const& query,
                                               std::shared_ptr<PhysicalOperator> const& phyOp,
                                               ArrayDistPtr const& distribution,
                                               ArrayResPtr const& residency);


}  // namespace scidb

#endif
