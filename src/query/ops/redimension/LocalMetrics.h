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

/**
 * @file LocalMetrics.h
 *
 * @description A container for accumulating local metrics.
 */

#ifndef LOCAL_METRICS_H
#define LOCAL_METRICS_H

#include "RedimensionCommon.h"

#include <array/ArrayDesc.h>
#include <array/Coordinate.h>
#include <query/Aggregate.h>
#include <system/Exceptions.h>

#include <ostream>

namespace scidb {

/**
 * @brief Specialized container for holding metrics needed for automatic chunk size calculation, @em
 * aka autochunking.
 */
class LocalMetrics {
public:

    /**
     * @brief Construct a container to hold redimension metrics.
     *
     * @param destSchema        target schema of redimension
     * @param trackCollisions   true iff we care about cell collisions
     */
    LocalMetrics(ArrayDesc const& destSchema, bool trackCollisions);

    /**
     * @brief Accumulate all relevant metrics for a destination cell at these coordinates.
     * @note If there are no local chunks, this will never be called.
     * @throws SCIDB_SE_INTERNAL if redistribute() has already been called.
     */
    void accumulate(mgd::vector<Value> const& destCell, Coordinates const& destCoords);

    /**
     * @brief Exchange these metrics cluster-wide.
     *
     * @param query the query we act on behalf of
     * @param phyOp the PhysicalOperator for the SG
     * @param distribution the DistType for distributing chunks
     * @param residency denotes the instances involved in the query
     */
    void redistribute(shared_ptr<Query> const& query,
                      std::shared_ptr<PhysicalOperator> const& phyOp,
                      ArrayDistPtr const& distribution,
                      ArrayResPtr const& residency);

    /**
     * @brief Getters for various metric results.
     * @note Returns local result if called before redistribute(), global result if called after.
     * @{
     */
    Coordinate getDimMin(size_t dimNo) const { return getDimResult(dimNo, 0).getInt64(); }
    Coordinate getDimMax(size_t dimNo) const { return getDimResult(dimNo, 1).getInt64(); }
    size_t getDimDistinctCount(size_t dimNo) const { return getDimResult(dimNo, 2).getUint64(); }
    size_t getOverallDistinct() const;
    size_t getNumCollisions() const;
    uint32_t getLargestAttrAvgSize(AttributeID* which = nullptr) const;
    /** @} */

    size_t dimCount() const { return _targetSchema.getDimensions().size(); }

private:
    enum Mode { ACCUMULATING, REPORTING } _mode;

    const ArrayDesc _targetSchema;      // Target schema
    bool            _trackCollisions;   // Track cell collisions
    bool            _haveVarLenAttrs;   // Variable length attributes present
    uint32_t        _biggestFixedSize;  // Size of biggest fixed-length attribute
    AttributeID     _biggestFixedAttrId;// Id of biggest fixed-length attribute
    Value           _coordValue;        // Allocate once, avoid reallocations
    Value           _destCoordsValue;   // Ditto

    typedef std::vector<AggregatePtr> AggVector;
    typedef std::vector<Value> StateVector;
    typedef std::vector<ssize_t> IndexMap;

    AggVector   _aggs;
    StateVector _states;
    IndexMap    _attrMap;

    Value getDimResult(size_t dimNo, size_t which) const;
};

std::ostream& operator<< (std::ostream& os, LocalMetrics const& lm);

} // namespace scidb

#endif /* LOCAL_METRICS_H */
