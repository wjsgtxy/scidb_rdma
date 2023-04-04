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
 * @file ChunkEstimator.h
 *
 * @description Common chunk size computation logic for autochunking redimension (and formerly for
 * CreateArrayUsing, now removed).  The meat of this code was originally written by Jonathon based
 * on a prototype by Donghui.  Wrapped up inside a nice, warm class by MJL.
 *
 * @author Jonathon Bell <jbell@paradigm4.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef CHUNK_ESTIMATOR_H
#define CHUNK_ESTIMATOR_H


#include <array/Dimensions.h>
#include <query/TypeSystem.h> // For template function definitions of Value.

#include <log4cxx/logger.h>

#include <array>
#include <vector>

namespace scidb {

class ChunkEstimator
{
public:

    enum Statistic
    {
        loBound,
        hiBound,
        interval,
        overlap,
        minimum,
        maximum,
        distinct
    };

    typedef std::array<Value,distinct+1> Statistics;
    typedef std::vector<Statistics>      StatsVector;

    enum How { BY_CONFIG, BY_CELLS, BY_PSIZE };

    /// Constructor used by redimension().
    ChunkEstimator(Dimensions& inOutDims);

    /// Setters, "named parameter idiom".
    /// @{
    ChunkEstimator& setTargetCellCount(uint64_t v);
    ChunkEstimator& setTargetMibCount(uint64_t v);
    ChunkEstimator& setOverallDistinct(uint64_t v)         { _overallDistinctCount = v; return *this; }
    ChunkEstimator& setBigAttributeSize(uint32_t v)        { _bigAttrSize = v; return *this; }
    ChunkEstimator& setSyntheticInterval(ssize_t dimNum, uint64_t nCollisions, float collisionRatio);
    ChunkEstimator& setAllStatistics(StatsVector const& v) { _statsVec = v; return *this; }
    ChunkEstimator& addStatistics(Statistics const& stats) { _statsVec.push_back(stats); return *this; }
    ChunkEstimator& setLogger(log4cxx::LoggerPtr lp)       { _logger = lp; return *this; }
    /// @}

    /// Getters, for benefit of operator<<()
    /// @{
    StatsVector const& getAllStatistics() const { return _statsVec; }
    uint64_t    getCollisionCount() const { return _collisionCount; }
    uint64_t    getOverallDistinct() const { return _overallDistinctCount; }
    uint64_t    getTargetCellCount() const { return _desiredValuesPerChunk; }
    uint64_t    getTargetMibCount() const { return _desiredMibsPerChunk; }
    How         getHow() const { return _how; }
    /// @}

    /// When all parameters are loaded, go compute estimates and fill in the [in,out] dimensions.
    void go();

private:
    void inferStatsFromSchema();
    void computeTargetCellCount();

    How             _how;
    Dimensions&     _dims;
    uint64_t        _overallDistinctCount;
    uint64_t        _collisionCount;
    uint64_t        _desiredValuesPerChunk;
    uint64_t        _desiredMibsPerChunk;
    uint32_t        _bigAttrSize;
    StatsVector     _statsVec;
    log4cxx::LoggerPtr _logger;
};

std::ostream& operator<< (std::ostream& os, ChunkEstimator::Statistics const&);
std::ostream& operator<< (std::ostream& os, ChunkEstimator const&);

} // namespace scidb

#endif /* ! CHUNK_ESTIMATOR_H */
