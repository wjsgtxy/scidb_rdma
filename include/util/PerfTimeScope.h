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
 * @file PerfTimeScope.h
 */

#ifndef PERF_TIME_SCOPE_H
#define PERF_TIME_SCOPE_H

#include <util/PerfTimeScope.h>

#include <query/Query.h>


namespace scidb {


///
/// @brief Marks a scope of performance measurement that will
///        be reported in scidb.log at query end.
///
/// TODO: JHM, this class and documentation to be replaced
///       in the next 2 commits.
//
/// @note mostly a placeholder at this point, this one does
///       what the prior functionality did, when it was part
///       of ScopedActiveQueryThread, but is on the verge
///       of being replaced as soon as the replacement can
///       be re-based onto this set of code changes that implement
///       just the split of PerfTimeScope out of ScopedActiveQueryThread.
///       This is a temporary which will be replaced within the
///       next working day or two, when the new functionality is
///       tested and CR'd.
///
class PerfTimeScope
{
public:
    PerfTimeScope(perfTimeScope_e pts);
    ~PerfTimeScope();

    /// used by a PerfWaitTimer to report its duration.
    /// This method looks up the current scope for the thread and
    /// the PerfTimeAggData from the thread's query and calls
    /// PerfTimeAggData() with the scope enum, the wait enum, and the time
    /// @param ptw the perfTimeWait_e of the PerfWaitTimer
    /// @param secReal the real time elapsed during the PerfWaitTimer's existence
    static void addToWait(perfTimeWait_e ptw, double secReal);
private:

    /// used by a nested (child) PerfTimeScope to report its duration-triple to its parent
    /// (if there is one).  This allows separating the durations spent within
    /// the parent itself ("self" times) from the time spent within nested
    /// scopes ("child" times).  "cum" in these arguments is a remindering that
    /// the calling child reports its cumulative times to the parent.
    /// @param secCumReal the real time elapsed during the child PerfTimeScope's existence.
    /// @param secCumUser the user time used by the thread during the PerfTimeScope's existence.
    /// @param secCumSyst the system time used by the thread during the PerfTimeScope's existence.
    void addToParent(double secCumReal, double secCumUser, double secCumSyst);

    /// calculate our own cumulative time, derive our self time by subtracting
    /// cumulative times reported by children, and report our cumulative time
    /// to our parent scope (if any).  Lastly, replace the per-thread pointer to
    /// the current scope, with that of our parent, which we saved at our creation.
    void popScope();

    // place to push/pop our "parent" scope
    PerfTimeScope* _scopeSaved;

    // place to lookup the "current" scope
    static thread_local PerfTimeScope* _scopeCurr;

    perfTimeScope_e  _pts;    // our enumerator

    // times saved by ctor
    double _secStartReal;
    double _secStartUser;
    double _secStartSyst;

    // accumulated times from nested "child" scopes
    double _secChildReal;
    double _secChildUser;
    double _secChildSyst;
};

} // namespace scidb

#endif // PERF_TIME_SCOPE_H
