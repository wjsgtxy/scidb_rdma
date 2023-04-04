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
 * @file PerfTimeScope.cpp
 */

#include <util/PerfTimeScope.h>
#include <util/PerfTimeAggData.h>

#include <log4cxx/logger.h>

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.perftimescope"));
}

namespace scidb {

thread_local PerfTimeScope* PerfTimeScope::_scopeCurr(nullptr);

PerfTimeScope::PerfTimeScope(perfTimeScope_e pts)
:
    _pts(pts),
    _secStartReal(perfTimeGetReal()),
    _secChildReal(0.0),
    _secChildUser(0.0),
    _secChildSyst(0.0)
{
    perfTimeGetThreadTimes(_secStartUser, _secStartSyst);

    _scopeSaved = _scopeCurr;  // push current scope

    // construction of self must be finished
    // before exposing pointer to self
    _scopeCurr = this;         // become current for this thread
}

PerfTimeScope::~PerfTimeScope()
{
    popScope();
}

void PerfTimeScope::addToWait(perfTimeWait_e ptw, double secReal)
{
    std::shared_ptr<Query> query = Query::getQueryPerThread();  // current query
    if (query) {
        if (_scopeCurr) {
            // and add the time for this scope query's refPerfTimeAggData
            query->refPerfTimeAggData().addToWait(_scopeCurr->_pts, ptw, secReal);
        } else {
            LOG4CXX_TRACE(logger, "PerfTimeScope::addToWait:  no _scopeCurr");
            // TODO JHM: extend when no scope by using a default scope
        }
    } else {
        // TODO JHM: extend when !query by using a global PerfTimeAggData
        LOG4CXX_TRACE(logger, "PerfTimeScope::addToWait:  no query");
    }
}

void PerfTimeScope::popScope()
{
    // determine our Cumulative, Child, and Net (self) times

    // cumulative time
    double secCumReal = perfTimeGetReal() - _secStartReal;       // cumulative or gross time
    double secNowUser, secNowSyst;
    perfTimeGetThreadTimes(secNowUser, secNowSyst);
    double secCumUser = secNowUser - _secStartUser;
    double secCumSyst = secNowSyst - _secStartSyst;

    LOG4CXX_TRACE(logger, "PerfTimeScope::popScope Cumu  " << ptsName(_pts)
                           << " r " << secCumReal    << " u " << secCumUser    << " s " << secCumSyst);

    LOG4CXX_TRACE(logger, "PerfTimeScope::popScope Child " << ptsName(_pts)
                           << " r " << _secChildReal << " u " << _secChildUser << " s " << _secChildSyst);

    // subtract the time of our own children to get our net or "self" time
    double secNetReal = secCumReal - _secChildReal;                   // self, local, or net time
    double secNetUser = secCumUser - _secChildUser ;
    double secNetSyst = secCumSyst - _secChildSyst;
    LOG4CXX_TRACE(logger, "PerfTimeScope::popScope Self  " << ptsName(_pts)
                           << " r " << secNetReal    << " u " << secNetUser    << " s " << secNetSyst);

    std::shared_ptr<Query> query = Query::getQueryPerThread();
    if(query) {
        query->refPerfTimeAggData().addToScopeCumu (_pts, secCumReal, secCumUser, secCumSyst);
        query->refPerfTimeAggData().addToScopeChild(_pts, _secChildReal, _secChildUser, _secChildSyst);
        query->refPerfTimeAggData().addToScopeSelf (_pts, secNetReal, secNetUser, secNetSyst);
    } else {
        ;   // TODO JHM: consider having a global PerfTimeAggData
    }

    // add our cumulative times to our parent (if any)
    addToParent(secCumReal, secCumUser, secCumSyst);

    // change _perfTimeScope only after the above has used it
    _scopeCurr = _scopeSaved;               // pop from local var
}


void PerfTimeScope::addToParent(double secCumReal, double secCumUser, double secCumSyst)
{
    if (_scopeSaved) {
        LOG4CXX_TRACE(logger, "PerfTimeScope::addToParent Self  " << ptsName(_pts)
                               << " parent " << ptsName(_scopeSaved->_pts)
                               << " r " << secCumReal    << " u " << secCumUser    << " s " << secCumSyst);
        _scopeSaved->_secChildReal += secCumReal;
        _scopeSaved->_secChildUser += secCumUser;
        _scopeSaved->_secChildSyst += secCumSyst;
    }
}

} // namespace scidb
