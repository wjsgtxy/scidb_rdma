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
 * @file LocalMetrics.cpp
 */

#include "LocalMetrics.h"
#include "RedimensionCommon.h"

#include <query/AggregateUtils.h>

namespace scidb {

// _states[0] is overall cell metrics, then come per-dimension metrics.
inline size_t DIM_INDEX(size_t dimNo) { return dimNo + 1; }

LocalMetrics::LocalMetrics(ArrayDesc const& destSchema, bool trackCollisions)
    : _mode(ACCUMULATING)
    , _targetSchema(destSchema)
    , _trackCollisions(trackCollisions)
    , _haveVarLenAttrs(false)
    , _biggestFixedSize(1)
    , _biggestFixedAttrId(INVALID_ATTRIBUTE_ID)
    , _coordValue(sizeof(Coordinate))
    , _destCoordsValue(destSchema.getDimensions().size() * sizeof(Coordinate))
{
    AggregateLibrary& aggLib = *AggregateLibrary::getInstance();
    AggregatePtr agg;

    // Building the internal _aggs[] aggregates vector.  The first entry is for aggregating over
    // complete cell Coordinates.
    Type tBinary = TypeLibrary::getType(TID_BINARY);
    agg = aggLib.createAggregate("_compose", tBinary);
    CompositeAggregate* compAgg = dynamic_cast<CompositeAggregate*>(agg.get());
    SCIDB_ASSERT(compAgg != nullptr);
    compAgg->add(aggLib.createAggregate("approxdc", tBinary));
    if (_trackCollisions) {
        compAgg->add(aggLib.createAggregate("_maxrepcnt", tBinary));
    }
    _aggs.push_back(agg);

    // Next comes one composite (min, max, dc) aggregate for each target dimension.
    Type tInt64 = TypeLibrary::getType(TID_INT64);
    agg = aggLib.createAggregate("_compose", tInt64);
    compAgg = dynamic_cast<CompositeAggregate*>(agg.get());
    compAgg->add(aggLib.createAggregate("min", tInt64))
            .add(aggLib.createAggregate("max", tInt64))
            .add(aggLib.createAggregate("approxdc", tInt64));
    for (size_t i = 0; i < destSchema.getDimensions().size(); ++i) {
        _aggs.push_back(agg);
    }

    // Last comes one "avg" aggregate for each variable length attribute (if there are any).
    //
    // We only care about one attribute metric for now: the average size of the largest attribute.
    // That's trivial if all attributes are fixed-size, but with one or more variable-size
    // attributes, we need to compute averages for all of them.
    //
    Attributes const& destAttrs = destSchema.getAttributes(/*excludeEmptyBitmap:*/ true);
    agg = aggLib.createAggregate("avg", TypeLibrary::getType(TID_UINT32));
    _attrMap.resize(destAttrs.size(), -1);
    for (const auto& dstAttr : destAttrs) {
        // Has to work for user-defined types too: can't use IS_VARLEN() macro here.
        Type type = TypeLibrary::getType(dstAttr.getType());
        if (type.variableSize()) {
            _haveVarLenAttrs = true;
            _aggs.push_back(agg->clone()); // clone here is paranoid
            _attrMap[dstAttr.getId()] = _aggs.size() - 1;
        } else if (type.byteSize() > _biggestFixedSize) {
            _biggestFixedSize = type.byteSize();
            _biggestFixedAttrId = dstAttr.getId();
        }
    }

    // Aggregates all set, now initialize some states for them.
    _states.resize(_aggs.size());
    for (size_t i = 0; i < _aggs.size(); ++i) {
        _aggs[i]->initializeState(_states[i]);
    }
}


void LocalMetrics::accumulate(mgd::vector<Value> const& destCell, Coordinates const& destCoords)
{
    if (_mode != ACCUMULATING) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
            << "Redistributed LocalMetrics object cannot accumulate more input";
    }

    // Accumulate metrics based on full destination coordinates.
    _destCoordsValue.setView(&destCoords[0], destCoords.size() * sizeof(Coordinate));
    _aggs[0]->accumulateIfNeeded(_states[0], _destCoordsValue);

    // Accumulate per-dimension metrics.
    for (size_t dim = 0; dim < destCoords.size(); ++dim) {
        _coordValue.setInt64(destCoords[dim]);
        _aggs[DIM_INDEX(dim)]->accumulateIfNeeded(_states[DIM_INDEX(dim)], _coordValue);
    }

    // Accumulate attribute size metrics.
    if (_haveVarLenAttrs) {
        Value sz;
        const auto& targetAttributes = _targetSchema.getAttributes(true);
        SCIDB_ASSERT(destCell.size() == targetAttributes.size() + 2);
        for (const auto& targetAttr : targetAttributes) {
            ssize_t index = _attrMap[targetAttr.getId()];
            if (index < 0 || destCell[targetAttr.getId()].isNull()) {  // DJG offset
                continue;
            }
            sz.setUint32(destCell[targetAttr.getId()].size());  // DJG offset
            _aggs[index]->accumulateIfNeeded(_states[index], sz);
        }
    }
}


void LocalMetrics::redistribute(std::shared_ptr<Query> const& query,
                                std::shared_ptr<PhysicalOperator> const& phyOp,
                                ArrayDistPtr const& distribution,
                                ArrayResPtr const& residency)
{
    _states = redistributeAggregateStates(_aggs,
                                          _states,
                                          query,
                                          phyOp,
                                          distribution,
                                          residency);
    _mode = REPORTING;
}


Value LocalMetrics::getDimResult(size_t dimNo, size_t which) const
{
    SCIDB_ASSERT(dimNo < _targetSchema.getDimensions().size());
    SCIDB_ASSERT(which < 3);
    Value result;
    size_t idx = DIM_INDEX(dimNo);
    CompositeAggregate* compAgg =
        dynamic_cast<CompositeAggregate*>(_aggs[idx].get());
    compAgg->finalResult(result, _states[idx], which);

    // For min and max, isNull() means nothing has been accumulated yet.
    // Return something reasonable and non-null to make callers happier.
    if (result.isNull() && which < 2) {
        if (which) {
            // max
            result.setInt64(std::numeric_limits<int64_t>::min());
        } else {
            // min
            result.setInt64(std::numeric_limits<int64_t>::max());
        }
    }

    return result;
}


size_t LocalMetrics::getOverallDistinct() const
{
    Value result;
    CompositeAggregate* compAgg =
        dynamic_cast<CompositeAggregate*>(_aggs[0].get());
    compAgg->finalResult(result, _states[0], 0);
    return result.getUint64();
}


size_t LocalMetrics::getNumCollisions() const
{
    Value result;
    if (_trackCollisions) {
        CompositeAggregate* compAgg =
            dynamic_cast<CompositeAggregate*>(_aggs[0].get());
        compAgg->finalResult(result, _states[0], 1);
        SCIDB_ASSERT(!result.isNull());
    } else {
        result.setUint64(0UL);
    }
    return result.getUint64();
}


uint32_t LocalMetrics::getLargestAttrAvgSize(AttributeID* which) const
{
    // Pretend we have a non-nil output parameter.
    AttributeID localAid;
    if (!which) {
        which = &localAid;
    }

    // No variable length attributes?  Easy.
    if (!_haveVarLenAttrs) {
        *which = _biggestFixedAttrId;
        return _biggestFixedSize;
    }

    // Scan the mapped variable length attributes for the biggest.
    Value v;
    double largest = 0.0;
    for (const auto& targetAttr : _targetSchema.getAttributes(true)) {
        ssize_t index = _attrMap[targetAttr.getId()];
        if (index < 0) {
            continue;
        }
        _aggs[index]->finalResult(v, _states[index]);
        double d = v.getDouble();
        if (d > largest) {
            largest = d;
            *which = targetAttr.getId();
        }
    }
    if (largest == 0.0) {
        // No cells accumulated, or all nulls?  Fake it.
        largest = static_cast<double>(_biggestFixedSize);
    }

    // We don't care about fractional bytes.
    return safe_static_cast<uint32_t>(::llround(largest));
}


std::ostream& operator<< (std::ostream& os, LocalMetrics const& m)
{
    os << "LocalMetrics(";

    // These entries are "overall" stats.
    os << "odc=" << m.getOverallDistinct()
       << ", col=" << m.getNumCollisions()
       << ", aas=" << m.getLargestAttrAvgSize()
       << ',';

    // Per-dimension stats.
    for (size_t dim = 0; dim < m.dimCount(); ++dim) {
        os << "\n  [" << dim
           << "](min=" << m.getDimMin(dim)
           << ", max=" << m.getDimMax(dim)
           << ", adc=" << m.getDimDistinctCount(dim)
           << "),";
    }

    os << ')';
    return os;
}

} // namespace scidb
