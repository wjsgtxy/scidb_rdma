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
 * @file ChunkEstimator.cpp
 *
 * @description Common chunk size computation logic for CreateArrayUsing and for autochunking
 * redimension().  The meat of this code was originally written by Jonathon based on a prototype by
 * Donghui.  Wrapped up inside a nice, warm class by MJL.
 *
 * @author Jonathon Bell <jbell@paradigm4.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "ChunkEstimator.h"

#include <system/Config.h>
#include <array/DimensionDesc.h>

#include <sstream>
#include <string>

using namespace std;

namespace scidb {

ChunkEstimator::ChunkEstimator(Dimensions& dims)
    : _how(BY_CONFIG)
    , _dims(dims)
    , _overallDistinctCount(0UL)
    , _collisionCount(0UL)
    , _desiredValuesPerChunk(0UL)
    , _desiredMibsPerChunk(0UL)
    , _bigAttrSize(sizeof(double))
{ }

ChunkEstimator& ChunkEstimator::setTargetCellCount(uint64_t v)
{
    _desiredMibsPerChunk = 0UL;
    _desiredValuesPerChunk = v;
    _how = BY_CELLS;
    return *this;
}

ChunkEstimator& ChunkEstimator::setTargetMibCount(uint64_t v)
{
    _desiredValuesPerChunk = 0UL;
    _desiredMibsPerChunk = v;
    _how = BY_PSIZE;
    return *this;
}

ChunkEstimator& ChunkEstimator::setSyntheticInterval(ssize_t dimNum,
                                                     uint64_t nCollisions,
                                                     float collisionRatio)
{
    SCIDB_ASSERT(dimNum >= 0);

    if (!_dims[dimNum].isAutochunked()) {
        LOG4CXX_TRACE(_logger, "Synthetic interval length estimate ignored");
        return *this;
    }

    if (nCollisions == 0) {
        // They did ask for a synthetic dimension after all.
        nCollisions = 1;
    }

    Config* cfg = Config::getInstance();
    uint64_t maxSynthInterval =
        cfg->getOption<uint64_t>(CONFIG_AUTOCHUNK_MAX_SYNTHETIC_INTERVAL);
    if (nCollisions > maxSynthInterval) {
        if (_logger) {
            LOG4CXX_WARN(_logger, "Estimated " << nCollisions
                         << " collisions, but capping synthetic interval at "
                         << maxSynthInterval
                         << " (autochunk-max-synthetic-interval)");
        }

        ASSERT_EXCEPTION(maxSynthInterval > 0,
                         "autochunk-max-synthetic-interval must be greater-than zero");
        nCollisions = maxSynthInterval;
    }

    // Leave startMin alone.
    int64_t startMin = _dims[dimNum].getStartMin();
    nCollisions = static_cast<uint64_t>(nCollisions * static_cast<long double>(collisionRatio));
    nCollisions = collisionRatio == 0.0 ? 1 : nCollisions;
    _dims[dimNum].setEndMax(startMin + nCollisions - 1);
    _dims[dimNum].setChunkInterval(nCollisions);
    _dims[dimNum].setChunkOverlap(0);

    return *this;
}

/**
 * The first four Statistics elements (loBound,hiBound,interval,overlap)
 * are bool Values that are true iff the corresponding value is
 * pegged... so, false if the value is to be inferred by the go()
 * routine.  These bools are set up automatically by CreateArrayUsing,
 * but for redimension we have to generate them appropriately.
 */
void ChunkEstimator::inferStatsFromSchema()
{
    SCIDB_ASSERT(_statsVec.size() == _dims.size());

    for (size_t i = 0, n = _dims.size(); i < n; ++i) {
        _statsVec[i][loBound].setBool(true);
        _statsVec[i][hiBound].setBool(true);
        _statsVec[i][interval].setBool(!_dims[i].isAutochunked());
        _statsVec[i][overlap].setBool(true);
    }
}

void ChunkEstimator::computeTargetCellCount()
{
    Config* cfg = Config::getInstance();

    // When the estimation method is dictated by an operator parameter
    // (BY_CELLS, BY_PSIZE), that takes precedence over anything found
    // in the config.ini file (BY_CONFIG).

    if (_how == BY_CONFIG) {
        // In the config.ini, BY_PSIZE takes precedence over BY_CELLS.
        _desiredMibsPerChunk =
            cfg->getOption<uint64_t>(CONFIG_TARGET_MB_PER_CHUNK);
        if (_desiredMibsPerChunk) {
            _how = BY_PSIZE;
        } else {
            _desiredValuesPerChunk =
                cfg->getOption<uint64_t>(CONFIG_TARGET_CELLS_PER_CHUNK);
            if (!_desiredValuesPerChunk) {
                _desiredValuesPerChunk = 1000000UL; // See SDB-1819.
            }
            _how = BY_CELLS;
        }
    } else {
        // Operator parameter must have set the estimation method.
        SCIDB_ASSERT((_how == BY_CELLS && _desiredValuesPerChunk) ||
                     (_how == BY_PSIZE && _desiredMibsPerChunk));
    }

    // If BY_PSIZE, compute the target cell count based on dense
    // chunks of the largest attribute found.
    if (_how == BY_PSIZE) {
        SCIDB_ASSERT(_bigAttrSize > 0);
        size_t bytesPerChunk = _desiredMibsPerChunk * MiB;
        _desiredValuesPerChunk = max(bytesPerChunk / _bigAttrSize, 1UL);
    }

    SCIDB_ASSERT(_how != BY_CONFIG);        // Estimation method chosen!
    SCIDB_ASSERT(_desiredValuesPerChunk);   // Target cell count computed!
}

/**
 *  Round the proposed chunk interval to the nearest power of two,  where
 *  'nearest' means that the base two logarithm is rounded to the nearest
 *  integer.
 *
 *  @note We do this only when estimating BY_CELLS, otherwise (for
 *        BY_PSIZE) we'd never be able to specify a non-power-of-2
 *        physical size.  (Actually it seems odd for BY_CELLS too, but
 *        let's remain backward compatible.)
 */
static int64_t roundLog2(int64_t ci)
{
    SCIDB_ASSERT(ci > 0);                            // Validate argument
    return static_cast<int64_t>(pow(2.0,round(log2(static_cast<double>(ci)))));
}

void ChunkEstimator::go()
{
    using std::max;
    using std::min;

    if (_logger) {
        LOG4CXX_DEBUG(_logger, "" << __PRETTY_FUNCTION__ << ": " << *this);
    }

    // First, are we ready?
    SCIDB_ASSERT(!_dims.empty());
    SCIDB_ASSERT(_dims.size() == _statsVec.size());
    computeTargetCellCount();
    inferStatsFromSchema();

    // By now we should have read the config and settled on a method.
    SCIDB_ASSERT(_how != BY_CONFIG);

    if (_logger) {
        LOG4CXX_DEBUG(_logger, "Estimating: " << *this);
    }

    size_t         numChunksFromN(max(_overallDistinctCount / _desiredValuesPerChunk,1UL));
    size_t                      N(0);            // Inferred intervals
    Dimensions::iterator        d(_dims.begin());// Dimension iterator
    int64_t                remain(CoordinateBounds::getMaxLength());// Cells to play with

    for (Statistics& s : _statsVec)                  // For each dimension
    {
        if (!s[interval].getBool())                  // ...infer interval?
        {
            N += 1;                                  // ....seen another
        }
        else                                         // ...user specified
        {
            numChunksFromN *= d->getChunkInterval();
            numChunksFromN /= max(s[distinct].getInt64(), 1L);

            remain /= (d->getChunkInterval() + d->getChunkOverlap());
        }

        ++d;
    }

    double const chunksPerDim = max(
        pow(static_cast<double>(numChunksFromN),1.0/static_cast<double>(N)),
        1.0);

    d = _dims.begin();                               // Reset dim iterator

    for (Statistics& s : _statsVec)                  // For each dimension
    {
        if (!s[loBound].getBool())                   // ...infer loBound?
        {
            d->setStartMin(s[minimum].getInt64());   // ....assign default
        }

        if (!s[hiBound].getBool())                   // ...infer hiBound?
        {
            d->setEndMax(s[maximum].getInt64());     // ....assign default
        }

        if (!s[overlap].getBool())                   // ...infer overlap?
        {
            d->setChunkOverlap(0);                   // ....assign default
        }

        if (!s[interval].getBool())                   // ...infer interval?
        {
            int64_t hi = s[maximum].getInt64();      // ....highest actual
            int64_t lo = s[minimum].getInt64();      // ....lowest  actual
            int64_t ci  = static_cast<int64_t>(      // ....chunk interval
                static_cast<double>((hi - lo + 1))/chunksPerDim);

            ci = max(ci,1L);                         // ....clamp at one
            if (_how == BY_CELLS) {
                ci = roundLog2(ci);                  // ....nearest power
            }
            ci = min(ci,remain);                     // ....clamp between
            ci = max(ci,1L);                         // ....one and remain

            d->setChunkInterval(ci);                 // ....update schema

            remain /= (d->getChunkInterval() + d->getChunkOverlap());
        }

        ++d;                                         // ...next dimension
    }
}

static const char* bool_or_null(Value const& v)
{
    return v.isNull() ? "null" : v.getBool() ? "T" : "F";
}

static string int64_or_null(Value const& v)
{
    if (v.isNull()) {
        return "null";
    } else {
        stringstream ss;
        ss << v.getInt64();
        return ss.str();
    }
}

std::ostream& operator<< (std::ostream& os, ChunkEstimator::Statistics const& stats)
{
    os << "stat([" << bool_or_null(stats[ChunkEstimator::loBound])
       << ',' << bool_or_null(stats[ChunkEstimator::hiBound])
       << ',' << bool_or_null(stats[ChunkEstimator::interval])
       << ',' << bool_or_null(stats[ChunkEstimator::overlap])
       << "],min=" << int64_or_null(stats[ChunkEstimator::minimum])
       << ",max=" << int64_or_null(stats[ChunkEstimator::maximum])
       << ",dc=" << int64_or_null(stats[ChunkEstimator::distinct])
       << ')';
    return os;
}

std::ostream& operator<< (std::ostream& os, ChunkEstimator const& ce)
{
    os << "ChunkEstimator(by"
       << (ce.getHow() == ChunkEstimator::How::BY_CONFIG ? "Cfg"
           : (ce.getHow() == ChunkEstimator::How::BY_PSIZE ? "Size" : "Cells"))
       << ",odc=" << ce.getOverallDistinct()
       << ",targets=(" << ce.getTargetCellCount()
       << " cells, " << ce.getTargetMibCount() << " MiB)"
       << ",col=" << ce.getCollisionCount()
       << ",stats=[";
    for (auto const& s : ce.getAllStatistics()) {
        os << "\n  " << s;
    }
    os << "])";
    return os;
}

} // namespace scidb
