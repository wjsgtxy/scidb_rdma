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
 * @file PerfTimeAggData.cpp
 */

#include <util/PerfTimeAggData.h>

#include <log4cxx/logger.h>

namespace {
    log4cxx::LoggerPtr logger      (log4cxx::Logger::getLogger("scidb.perftimeaggdata"));
    log4cxx::LoggerPtr loggerDetail(log4cxx::Logger::getLogger("scidb.perftimeaggdata.detail"));
}

namespace scidb {

PerfTimeAggData::PerfTimeAggData()
{
    memset(_waitRealUsec, 0, sizeof(_waitRealUsec));
    memset(_waitRealCount, 0, sizeof(_waitRealCount));

    memset(_scopeCumuRealUsec, 0, sizeof(_scopeCumuRealUsec));
    memset(_scopeCumuUserUsec, 0, sizeof(_scopeCumuUserUsec));
    memset(_scopeCumuSystUsec, 0, sizeof(_scopeCumuSystUsec));

    memset(_scopeChildRealUsec, 0, sizeof(_scopeChildRealUsec));
    memset(_scopeChildUserUsec, 0, sizeof(_scopeChildUserUsec));
    memset(_scopeChildSystUsec, 0, sizeof(_scopeChildSystUsec));

    memset(_scopeSelfRealUsec, 0, sizeof(_scopeSelfRealUsec));
    memset(_scopeSelfUserUsec, 0, sizeof(_scopeSelfUserUsec));
    memset(_scopeSelfSystUsec, 0, sizeof(_scopeSelfSystUsec));
}


PerfTimeAggData::~PerfTimeAggData()
{
    // TODO JHM: moveH perfTimeLog() from PerfTime.cpp to here (or at least part of it)
}

/// continue to support p4 OpStatsQuery.cpp
uint64_t PerfTimeAggData::getActiveRealTimeMicroseconds() const
{
    SCIDB_ASSERT(                _scopeCumuRealUsec[PTS_CMHJ_COMP_EXE_QUERY] >= 0);
    return static_cast<uint64_t>(_scopeCumuRealUsec[PTS_CMHJ_COMP_EXE_QUERY]);
}

void PerfTimeAggData::addToWait(const perfTimeScope pts, const perfTimeWait_e ptw, const double realSec)
{
    // protect memory from out-of-bounds writes
    ASSERT_EXCEPTION(pts >= 0,      "pts less than zero");
    ASSERT_EXCEPTION(pts < PTS_NUM, "pts too large");
    ASSERT_EXCEPTION(ptw >= 0,      "ptw less than zero");
    ASSERT_EXCEPTION(ptw < PTW_NUM, "ptw too large");

    if(ptwToCategory(ptw) == PTWC_ZERO) {
        LOG4CXX_WARN(logger, "PerfTimeAggData::addToWait: unexpected wait ignored: " << ptwName(ptw)
                                     << ", " << realSec << " s ");
        return;
    }

    _waitRealUsec [pts][ptw] += int64_t(realSec*1.0e6);
    _waitRealCount[pts][ptw] += 1;
}

void PerfTimeAggData::addToScopeCumu(const perfTimeScope_e pts, const double realSec,
                                     const double userSec, const double systSec)
{
    LOG4CXX_TRACE(logger, "addToScopeCumu " << ptsName(pts) << " realSec: " << realSec << " s ");
    // memory bounds check
    ASSERT_EXCEPTION(pts >= 0,      "pts less than zero");
    ASSERT_EXCEPTION(pts < PTS_NUM, "pts too large");

    _scopeCumuRealUsec[pts] += int64_t(realSec*1.0e6);
    _scopeCumuUserUsec[pts] += int64_t(userSec*1.0e6);
    _scopeCumuSystUsec[pts] += int64_t(systSec*1.0e6);
}

void PerfTimeAggData::addToScopeChild(const perfTimeScope_e pts, const double realSec,
                                      const double userSec, const double systSec)
{
    LOG4CXX_TRACE(logger, "addToScopeChild " << ptsName(pts) << " realSec: " << realSec << " s ");
    // memory bounds check
    ASSERT_EXCEPTION(pts >= 0,      "pts less than zero");
    ASSERT_EXCEPTION(pts < PTS_NUM, "pts too large");

    _scopeChildRealUsec[pts] += int64_t(realSec*1.0e6);
    _scopeChildUserUsec[pts] += int64_t(userSec*1.0e6);
    _scopeChildSystUsec[pts] += int64_t(systSec*1.0e6);
}

void PerfTimeAggData::addToScopeSelf(const perfTimeScope_e pts, const double realSec,
                                     const double userSec, const double systSec)
{
    LOG4CXX_TRACE(logger, "addToScopeSelf " << ptsName(pts) << " realSec: " << realSec << " s ");
    // memory bounds check
    ASSERT_EXCEPTION(pts >= 0,      "pts less than zero");
    ASSERT_EXCEPTION(pts < PTS_NUM, "pts too large");

    _scopeSelfRealUsec[pts] += int64_t(realSec*1.0e6);
    _scopeSelfUserUsec[pts] += int64_t(userSec*1.0e6);
    _scopeSelfSystUsec[pts] += int64_t(systSec*1.0e6);
}

void PerfTimeAggData::printLog(int64_t usecElapsedStart, const Query& query)
{
    int64_t usecNow = perfTimeGetElapsedInMicroseconds();
    int64_t usecTotal = usecNow - usecElapsedStart;

    // aggregate the detailed wait measurements _waitRealUsec[PTS_*][PTW_*]
    // into the groups ptscUsec[PTSC_*] and ptwcUsec[PTWC_*]
    int64_t ptscUsec[PTSC_NUM];
    int64_t ptwcUsec[PTWC_NUM];
    memset(ptscUsec, 0, sizeof(ptscUsec));
    memset(ptwcUsec, 0, sizeof(ptwcUsec));

    for (perfTimeScope_e pts=PTS_FIRST; pts< PTS_NUM; pts = perfTimeScope_e(pts+1)) {
        perfTimeScopeCategory_e ptsc = ptsToCategory(pts);
        for (perfTimeWait_e ptw=PTW_FIRST; ptw< PTW_NUM; ptw = perfTimeWait_e(ptw+1)) {
            perfTimeWaitCategory_e ptwc = ptwToCategory(ptw);
            const int64_t uSec = _waitRealUsec[pts][ptw];

            SCIDB_ASSERT(ptwc < PTWC_NUM) ;
            ptwcUsec[ptwc] += uSec;
            SCIDB_ASSERT(ptsc < PTSC_NUM) ;
            ptscUsec[ptsc] += uSec;
        }
    }
    // TODO JHM: consider checks of top level scope(s) vs sum(ptscUsec)
    // TODO JHM: consider checks of top level scope cumu >= sum(children)

    int64_t ptwcUsecSum = 0;
    for (unsigned i=PTWC_FIRST; i< PTWC_NUM; i++) {
        ptwcUsecSum += ptwcUsec[i];
    }

    std::stringstream ssStats;
    ssStats << std::fixed << std::setprecision(6)
            <<  "TOT "  << double(usecTotal) * 1.0e-6 ;

    ssStats << " ACT "  << double(_scopeCumuRealUsec[PTS_CMHJ_COMP_EXE_QUERY]) * 1.0e-6;

    ssStats << " CPU "   << double(_scopeCumuUserUsec[PTS_CMHJ_COMP_EXE_QUERY]) * 1.0e-6 +
                            double(_scopeCumuUserUsec[PTS_CMHJ_COMP_EXE_QUERY]) * 1.0e-6 ;

    ssStats << " wPG "   << double(ptwcUsec[PTWC_PG]) * 1.0e-6
            << " wFSr "  << double(ptwcUsec[PTWC_FS_RD]) * 1.0e-6
            << " wFSw "  << double(ptwcUsec[PTWC_FS_WR]) * 1.0e-6
            << " wFSws " << double(ptwcUsec[PTWC_FS_WR_SYNC]) * 1.0e-6
            << " wFSf "  << double(ptwcUsec[PTWC_FS_FL]) * 1.0e-6
            << " wSMld " << double(ptwcUsec[PTWC_SM_LOAD]) * 1.0e-6
            << " wBFrd " << double(ptwcUsec[PTWC_BF_RD]) * 1.0e-6
            << " wSMcm " << double(ptwcUsec[PTWC_SM_CMEM]) * 1.0e-6
            << " wREP "  << double(ptwcUsec[PTWC_REP])*1.0e-6
            << " wNETr " << double(ptwcUsec[PTWC_NET_RCV]) * 1.0e-6
            << " wNETs " << double(ptwcUsec[PTWC_NET_SND])* 1.0e-6
            << " wNETrr "<< double(ptwcUsec[PTWC_NET_RCV_R])* 1.0e-6
            << " wNETrc "<< double(ptwcUsec[PTWC_NET_RCV_C])* 1.0e-6
            << " wSGr "  << double(ptwcUsec[PTWC_SG_RCV]) * 1.0e-6
            << " wSGb "  << double(ptwcUsec[PTWC_SG_BAR]) * 1.0e-6
            << " wBAR "  << double(ptwcUsec[PTWC_BAR])*1.0e-6
            << " wJsrt"  << double(ptwcUsec[PTWC_SORT_JOB])*1.0e-6
            << " wEXT "  << double(ptwcUsec[PTWC_EXT]) * 1.0e-6
            << " wSEMo " << double(ptwcUsec[PTWC_SEMA]) * 1.0e-6
            << " wEVo "  << double(ptwcUsec[PTWC_EVENT]) * 1.0e-6
            << " wLTCH " << double(ptwcUsec[PTWC_LATCH]) * 1.0e-6
            << " wRare " << double(ptwcUsec[PTWC_RARE]) * 1.0e-6
            << " wZero " << double(ptwcUsec[PTWC_ZERO]) * 1.0e-6;

    LOG4CXX_INFO (logger, "QueryTiming " << query.getInstanceID() << " " << query.getQueryID()
                                          << " " << ssStats.str() << " " << query.getQueryString());

    static uint64_t _perfTimeLogCount = 0;
    const uint LINES_LOGGED_PER_KEY_PRINTED= 500;
    if (_perfTimeLogCount++ % LINES_LOGGED_PER_KEY_PRINTED == 0) {
        printLogKey();
    }

    printLogDetailed(usecTotal, query);
}

/**
 * to enable the additional logging available from this routine, not usually enabled,
 * add a line like this one:
 *
 * log4j.scidb.perftimeaggdata.detail=TRACE
 *
 * to scidb's log4cxx config file (e.g.  /opt/scidb/<VER>/share/scidb/log4cxx.properties)
 * For additonal info goto logging.apache.org/log4cxx and search for "configuration file".
 */


void PerfTimeAggData::printLogDetailed(int64_t usecTotal, const Query& query)
{
    std::stringstream ssStats;
    ssStats << std::fixed << std::setprecision(6)
        <<  "TOT "  << double(usecTotal) * 1.0e-6;

    ssStats << "%";
    for(size_t pts= 0; pts < PTS_NUM; ++pts) {
        // first the 1-per-scope info for that PTS, which is currently triplets of real,user,system time
        if (_scopeCumuRealUsec[pts]+_scopeCumuUserUsec[pts]+_scopeCumuSystUsec[pts] > 0) {
            ssStats << "@";
            ssStats << ptsName(perfTimeScope_e(pts)) << "," << "PT_SUM_CUMU" << ":"
                    << " " << double(_scopeCumuRealUsec[pts]) * 1.0e-6
                    << " " << double(_scopeCumuUserUsec[pts]) * 1.0e-6
                    << " " << double(_scopeCumuSystUsec[pts]) * 1.0e-6;
        }
        if (_scopeChildRealUsec[pts]+_scopeChildUserUsec[pts]+_scopeChildSystUsec[pts] > 0) {
            ssStats << "@";
            ssStats << ptsName(perfTimeScope_e(pts)) << "," << "PT_SUM_CHILD" << ":"
                    << " " << double(_scopeChildRealUsec[pts]) * 1.0e-6
                    << " " << double(_scopeChildUserUsec[pts]) * 1.0e-6
                    << " " << double(_scopeChildSystUsec[pts]) * 1.0e-6;
        }
        if (_scopeSelfRealUsec[pts]+_scopeSelfUserUsec[pts]+_scopeSelfSystUsec[pts] > 0) {
            ssStats << "@";
            ssStats << ptsName(perfTimeScope_e(pts)) << "," << "PT_SUM_SELF" << ":"
                    << " " << double(_scopeSelfRealUsec[pts]) * 1.0e-6
                    << " " << double(_scopeSelfUserUsec[pts]) * 1.0e-6
                    << " " << double(_scopeSelfSystUsec[pts]) * 1.0e-6;
        }

        // and now the multiple-per-scope (per wait) which is real time + call counts
        // (since user+system time too expensive to collect as frequently as waits are called)
        for(size_t ptw= 0; ptw < PTW_NUM; ++ptw) {
            int64_t uSecR = _waitRealUsec[pts][ptw];
            int64_t count = _waitRealCount[pts][ptw];
            if (count > 0) {
                ssStats << "@";
                ssStats << ptsName(perfTimeScope_e(pts)) << "," << ptwName(perfTimeWait_e(ptw)) << ":"
                << " " << double(uSecR) * 1.0e-6
                << " " << count;
            }
        }
    }
    ssStats << "%";

    LOG4CXX_TRACE (loggerDetail, "QueryTimingDetail "
                   << query.getInstanceID() << " " << query.getQueryID()
                   << " " << ssStats.str() << " " << query.getQueryString());
}

/**
 * logs detailed information about the values logged by perfTimeLog
 */

void PerfTimeAggData::printLogKey()
{
    for(size_t ptwc= 0; ptwc < PTWC_NUM; ++ptwc) {
        LOG4CXX_DEBUG (logger, "QueryTiming Key: " << ptwcName(perfTimeWaitCategory_e(ptwc))
                               << " : " << ptwcMessage(perfTimeWaitCategory_e(ptwc)));
    }
}

} // namespace scidb
