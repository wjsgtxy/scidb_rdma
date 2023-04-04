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
 * @file PerfTime.cpp
 *
 * @brief Implementation of PerfTime.h
 *
 */

#include <util/PerfTime.h>

#include <atomic>
#include <cmath>

#include <query/Query.h>
#include <query/executor/ScopedQueryThread.h>
#include <system/Config.h>
#include <util/PerfTimeAggData.h>
#include <util/PerfTimeScope.h>

namespace scidb
{
log4cxx::LoggerPtr perfLogger = log4cxx::Logger::getLogger("scidb.perftime");

double perfTimeGetReal() noexcept
{
    try {
        struct timeval tv;
        auto result = ::gettimeofday(&tv, NULL);
        SCIDB_ASSERT(result==0);  // never fails in practice
        return double(tv.tv_sec) + double(tv.tv_usec) * 1.0e-6;         // slow, initial implementation
    }
    catch (...){;} // timing should not change the code path above

    return NAN; // NaN will keep things going while still invalidating the timing
}

void perfTimeGetThreadTimes(double& usr, double& sys) noexcept
{
    try {
        struct rusage rUsage;
        auto result = ::getrusage(RUSAGE_THREAD, &rUsage);
        SCIDB_ASSERT(result==0);  // never fails in practice

        usr = double(rUsage.ru_utime.tv_sec) +
              double(rUsage.ru_utime.tv_usec) * 1.0e-6;

        sys = double(rUsage.ru_stime.tv_sec) +
              double(rUsage.ru_stime.tv_usec) * 1.0e-6;

    }
    // failure of timing should not interfere with completing a query
    // NaN will keep the caller going while still invalidating the timing result
    catch (...){
        usr = NAN;
        sys = NAN;
    }
}

// TODO: Marty's duplication of stuff to review
//            File a ticket on this, see email to Marty 5/26/2016 prior to
//            removing this message
//            AND DON'T TOUCH THE FOLLOWING CODE

uint64_t perfTimeGetElapsedInMicroseconds() noexcept
{
    try {
        struct timeval tv;
        auto result = ::gettimeofday(&tv, NULL);
        SCIDB_ASSERT(result==0);  // never fails in practice

        return (static_cast<uint64_t>(tv.tv_sec) * 1000000) + static_cast<uint64_t>(tv.tv_usec);
    }
    catch (...){;} // timing should not change the code path above

    return 0;
}

uint64_t perfTimeGetCpuInMicroseconds(int who) noexcept
{
    try {
        struct rusage rUsage;
        auto result = ::getrusage(who, &rUsage);
        SCIDB_ASSERT(result==0);  // never fails in practice

        return (static_cast<uint64_t>(rUsage.ru_utime.tv_sec) * 1000000) +
                static_cast<uint64_t>(rUsage.ru_utime.tv_usec) +
               (static_cast<uint64_t>(rUsage.ru_stime.tv_sec) * 1000000) +
                static_cast<uint64_t>(rUsage.ru_stime.tv_usec);
    }
    catch (...){;} // timing should not change the code path above

    return 0;
}

// gives names to scopecategories (PTSs)

struct PTSC_TableEntry {
  const char* ptsc_str;
  const char* ptsc_message;
};

const PTSC_TableEntry PTSC_Table[PTSC_NUM] =
{
#define PTSC_ENTRY(C, CMESSAGE) [C] = {#C, CMESSAGE}
    PTSC_TABLE_ENTRIES
#undef PTSC_ENTRY
};

const char* ptscName(const perfTimeScopeCategory_e ptsc)
{
    SCIDB_ASSERT(ptsc < PTSC_NUM);
    return PTSC_Table[ptsc].ptsc_str ;
}

// map PTS to Categories (PTSCs)
struct PTS_TableEntry {
  perfTimeScope_e    pts;
  const char*        pts_str;
  const char*        pts_message;
  perfTimeScopeCategory_e ptsc;
};

const PTS_TableEntry PTS_Table[PTS_NUM] =
{
#define PTS_ENTRY(A, AC, A_MSG) [A] = {A, #A, A_MSG, AC}
    PTS_TABLE_ENTRIES
#undef PTS_ENTRY
};

perfTimeScopeCategory_e ptsToCategory(const perfTimeScope_e pts)
{
    SCIDB_ASSERT(pts < PTS_NUM);
    return PTS_Table[pts].ptsc;
}

const char* ptsName(const perfTimeScope_e pts)
{
    SCIDB_ASSERT(pts < PTS_NUM);
    return PTS_Table[pts].pts_str;
}

// gives names to wait categories (PTWCs)

struct PTWC_TableEntry {
  const char* ptwc_str;
  const char* ptwc_message;
};

const PTWC_TableEntry PTWC_Table[PTWC_NUM] =
{
#define PTWC_ENTRY(C, CMESSAGE) [C] = {#C, CMESSAGE}
    PTWC_TABLE_ENTRIES
#undef PTWC_ENTRY
};

const char* ptwcName(const perfTimeWaitCategory_e ptwc)
{
    SCIDB_ASSERT(ptwc < PTWC_NUM);
    return PTWC_Table[ptwc].ptwc_str ;
}

const char* ptwcMessage(const perfTimeWaitCategory_e ptwc)
{
    SCIDB_ASSERT(ptwc < PTWC_NUM);
    return PTWC_Table[ptwc].ptwc_message ;
}


// gives names to wait numbers (PTWs) and maps thems to more general categories (PTCs)
struct PTW_TableEntry {
  perfTimeWait_e     ptw;
  const char*        ptw_str;
  const char*        ptw_message;
  perfTimeWaitCategory_e ptwc;
};

const PTW_TableEntry PTW_Table[PTW_NUM] =
{
#define PTW_ENTRY(W, WC, W_MSG) [W] = {W, #W, W_MSG, WC}
    PTW_TABLE_ENTRIES
#undef PTW_ENTRY
};

perfTimeWaitCategory_e ptwToCategory(const perfTimeWait_e ptw)
{
    SCIDB_ASSERT(ptw < PTW_NUM);
    return PTW_Table[ptw].ptwc;
}

const char* ptwName(const perfTimeWait_e ptw)
{
    SCIDB_ASSERT(ptw < PTW_NUM);
    return PTW_Table[ptw].ptw_str;
}

void logNoqueryWaits(const perfTimeWait_e ptw, const double sec)
{
    LOG4CXX_DEBUG(perfLogger, "perfTimeAdd: didn't log " << ptwName(ptw) << " " << sec);
}

bool ScopedWaitTimer::_isWaitTimingEnabled = false;

void ScopedWaitTimer::adjustWaitTimingEnabled()
{
    _isWaitTimingEnabled = Config::getInstance()->getOption<int>(CONFIG_PERF_WAIT_TIMING);
    if (_isWaitTimingEnabled) {
        LOG4CXX_INFO(perfLogger, "The SciDB process does do perf wait timing.");
    } else {
        LOG4CXX_INFO(perfLogger, "The SciDB process does not do perf wait timing.");
    }
}

bool ScopedWaitTimer::isWaitTimingEnabled() noexcept
{
    return _isWaitTimingEnabled;
}


// if not thread_local, this would need to be std::atomic_uint_fast32_t
thread_local uint64_t ScopedWaitTimer::_nestingDepth(0);

ScopedWaitTimer::ScopedWaitTimer(perfTimeWait_e ptw, bool logOnCompletion) noexcept
:
    _ptw(ptw),
    _secStartReal(0),
    _isEnabled(ptw != PTW_UNTIMED && isWaitTimingEnabled()),
    _logOnCompletion(logOnCompletion)
{
    init();
}

void ScopedWaitTimer::init() noexcept
{
    if(!_isEnabled) { return; }

    SCIDB_ASSERT(_ptw < PTW_NUM); // (except this one that prevents memory scribbling)

    try {
        static int NESTING_ALLOWED=1 ; // change to 0 to debug unintended nesting
                                       // TODO: this may become a ctor parameter
                                       //   so that "top-level" ScopedWaitTimer won't be
                                       //   accidentally nested, but its use inside
                                       //   semaphore, event, etc can be more permissive
        if (!NESTING_ALLOWED) {
            assert(_nestingDepth==0);       // check that there is only one object at a time per thread
                                            // to avoid nesting these on the stack, which would
                                            // result in over-measurement.
        }
        _nestingDepth++ ;
        if(_nestingDepth==1) {   // only the outermost nesting does the timing in dtor
                                 // so this is a very helpful performance optimization

            _secStartReal = perfTimeGetReal(); // vdso call
        }
    }
    catch (...){;} // timing should not change the code path above
}

ScopedWaitTimer::~ScopedWaitTimer()  noexcept
{
    if(!_isEnabled) { return; }

    // note: surround CPU mesurement with elapsed measurement
    //       so that secBlocked has less of a chance
    //       of being negative.  This does introduce
    //       an positive bias of elapsed vs cpu time
    //       but tends to keep secBlocked from going negative
    //       as much, which is a little less astonishing
    //       This is subject to change as we learn more
    //       about the clocks involved and their relative
    //       precisions
    try {
        assert(_nestingDepth > 0);

        if(_nestingDepth==1) {   // only the outermost nesting does the timing

            auto secReal = perfTimeGetReal()       - _secStartReal;     // vdso, not syscall

            PerfTimeScope::addToWait(_ptw, secReal);

            if (_logOnCompletion) {
                LOG4CXX_INFO(perfLogger, "ScopedWaitTimer " << ptwName(_ptw));
            }
        }
        _nestingDepth-- ;
    }
    catch (...){;} // timing should not change the code path above
}

} // namespace
