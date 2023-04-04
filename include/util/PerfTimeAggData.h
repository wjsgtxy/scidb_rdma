/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file PerfTimeAggData.h
 */

#ifndef PERF_TIME_AGG_DATA_H
#define PERF_TIME_AGG_DATA_H

#include <util/PerfTimeAggData.h>

#include <query/Query.h>


namespace scidb {


/// @class PerfTimeAggData
/// @brief Timings collected by SopedWaitTimer are
///        aggregated into the data held by this class.
///        There is one object of this class tracked by
///        each query. (In the future there may
///        be one for all activity not associated with a
///        particular query.) The data is logged at destruction.
///
class PerfTimeAggData
{
public:
    PerfTimeAggData();

    ~PerfTimeAggData();

    /// legacy support for Query::getActiveTimeMicroseconds()
    uint64_t getActiveRealTimeMicroseconds() const;

    /// Add duration of a ScopedWaitTimer to _waitRealUsec and increment _waitRealCount
    /// @param pta scope enumeration
    /// @param ptw wait enumeration
    /// @param real the seconds waited on a PerfTimeWait inside a PerfTimeScope
    void addToWait(const perfTimeScope_e pts, const perfTimeWait_e ptw, const double real);

    /// Add cumulative duration of a PerfTimeScope to the _scopeCumu{Real,User,Syst}Usec data
    /// @param pts scope enumeration
    /// @param real the seconds of real time elapsed during the pts
    /// @param user the seconds of user-thread time used during the pts
    /// @param syst the seconds of system-thread time used during the pts
    void addToScopeCumu(const perfTimeScope_e pts, const double real, const double user, const double syst);
    /// Add child subtotal of a PerfTimeScope to the _scopeChild{{Real,User,Syst}Usec data
    /// @param pts scope enumeration
    /// @param real the seconds of real time elapsed during the pts
    /// @param user the seconds of user-thread time used during the pts
    /// @param syst the seconds of system-thread time used during the pts
    void addToScopeChild(const perfTimeScope_e pts, const double real, const double user, const double syst);
    /// Add self subtotal of a PerfTimeScope to the _scopeSelf{{Real,User,Syst}Usec data
    ///        (self time = cumulative time - child time)
    /// @param pts scope enumeration
    /// @param real the seconds of real time elapsed during the pts
    /// @param user the seconds of user-thread time used during the pts
    /// @param syst the seconds of system-thread time used during the pts
    void addToScopeSelf(const perfTimeScope_e pts, const double real, const double user, const double syst);

    /// add information from this object to the scidb.log
    /// @param usecElapsedStart microsecond timestamp of the query creation
    /// @param query Query
    void printLog(int64_t usecElapsedStart, const Query& query);
private:
    /// add the detailed information from this object to the scidb.log
    /// @param usecTotal total duration of query up to this point
    /// @param query Query
    void printLogDetailed(int64_t usecTotal, const Query& query);

    /// used to peridically print a key to perfTime abbreviations
    void printLogKey();

    // added at wait points. PTS varies less than PTW, typically
    std::atomic_int_fast64_t    _waitRealUsec [PTS_NUM][PTW_NUM];
    std::atomic_int_fast64_t    _waitRealCount[PTS_NUM][PTW_NUM];

    // added only by ~PerfTimeScope
    std::atomic_int_fast64_t    _scopeCumuRealUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeCumuUserUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeCumuSystUsec[PTS_NUM];

    std::atomic_int_fast64_t    _scopeChildRealUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeChildUserUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeChildSystUsec[PTS_NUM];

    std::atomic_int_fast64_t    _scopeSelfRealUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeSelfUserUsec[PTS_NUM];
    std::atomic_int_fast64_t    _scopeSelfSystUsec[PTS_NUM];
};

} // namespace scidb

#endif // PERF_TIME_AGG_DATA_H
