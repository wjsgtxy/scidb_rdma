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
 * @file Semaphore.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class
 */

#include <util/Semaphore.h>

#include <system/Exceptions.h>

namespace scidb
{

namespace {
  const timespec DEFAULT_ERRCHK_TS = { Semaphore::DEFAULT_ERRCHK_INTERVAL, 0 };
}

Semaphore::Semaphore()
{
    if (::sem_init(_sem, 0, 0) == -1) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
            << "sem_init" << ::strerror(errno) << errno;
    }
}

Semaphore::~Semaphore()
{
    if (::sem_destroy(_sem) == -1) {
        assert(false);          // throwing from dtor is bad, try this first
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
            << "sem_destroy" << ::strerror(errno) << errno;
    }
}

void Semaphore::enter(perfTimeWait_e tw)
{
    {
        ScopedWaitTimer timer(tw);  // destruction updates the timing of tw
        do
        {
            if (::sem_wait(_sem) == 0) {
                return;
            }
        } while (errno == EINTR);
    }

    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
        << "sem_wait" << ::strerror(errno) << errno;
}

void Semaphore::enter(size_t count, perfTimeWait_e tw)
{
    for (size_t i = 0; i < count; i++)
    {
        enter(tw);
    }
}

namespace {

timespec intervalToAbsTime(timespec const& interval)
{
    timespec ts;
    if (::clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                               SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    ts.tv_sec = ts.tv_sec + interval.tv_sec;
    ts.tv_nsec = ts.tv_nsec + interval.tv_nsec;
    if (ts.tv_nsec >= 1000000000) {
        ++ts.tv_sec;
        ts.tv_nsec -= 1000000000;
    }
    return ts;
}

} // anonymous namespace


bool Semaphore::enter(
    ErrorChecker& errorChecker,
    perfTimeWait_e tw,
    timespec const* interval)
{
    if (errorChecker && !errorChecker()) {
        return false;
    }

    if (!interval) {
        interval = &DEFAULT_ERRCHK_TS;
    }

    timespec ts = intervalToAbsTime(*interval);
    ScopedWaitTimer timer(tw);         // destruction updates the timing of tw
    while (true)
    {
        if (::sem_timedwait(_sem, &ts) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != ETIMEDOUT) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "sem_timedwait" << ::strerror(errno) << errno;
        }
        if (errorChecker && !errorChecker()) {
           return false;
        }

        ts = intervalToAbsTime(*interval);
    }
}

bool Semaphore::enter(
    size_t          count,
    ErrorChecker&   errorChecker,
    perfTimeWait_e  tw,
    timespec const* interval)
{
    for (size_t i = 0; i < count; i++) {
        if (!enter(errorChecker, tw, interval)) {
            return false;
        }
    }
    return true;
}

void Semaphore::release(size_t count)
{
    assert(count > 0);

    for (size_t i = 0; i < count; i++) {
        if (::sem_post(_sem) == -1) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "sem_post" << ::strerror(errno) << errno;
        }
    }
}

} //namespace
