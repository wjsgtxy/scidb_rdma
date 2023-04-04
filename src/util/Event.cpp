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
 * @file Event.cpp
 *
 * @author knizhnik@garret.ru, roman.simakov@gmail.com
 *
 * @brief POSIX conditional variable
 */

#include <util/Event.h>
#include <util/Mutex.h>
#include <system/Exceptions.h>

namespace scidb
{

namespace {
    const timespec DEFAULT_ERRCHK_TS = { Event::DEFAULT_ERRCHK_INTERVAL, 0 };
}

Event::Event()
{
    if (int e = ::pthread_cond_init(&_cond, NULL)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR)
            << "pthread_cond_init" << ::strerror(e) << e;
    }
}


Event::~Event()
{
    if (int e = ::pthread_cond_destroy(&_cond)) {
        assert(false);          // throwing from dtor is bad, try this first
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR)
            << "pthread_cond_destroy" << ::strerror(e) << e;
    }
}


bool Event::wait(Mutex& cs,
                 ErrorChecker& errorChecker,
                 perfTimeWait_e tw,
                 timespec const* interval)
{
    if (!interval) {
        interval = &DEFAULT_ERRCHK_TS;
    }

    cs.checkForDeadlock();
    if (errorChecker) {
        if (!errorChecker()) {
            return false;
        }

        _signaled = false;
        do {
            timespec ts;
            if (::clock_gettime(CLOCK_REALTIME, &ts) == -1) {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                       SCIDB_LE_CANT_GET_SYSTEM_TIME);
            }
            ts.tv_sec = ts.tv_sec + interval->tv_sec;
            ts.tv_nsec =ts.tv_nsec + interval->tv_nsec;
            if (ts.tv_nsec >= 1000000000) {
                ++ts.tv_sec;
                ts.tv_nsec -= 1000000000;
            }

            int e;
            {   ScopedWaitTimer timer(tw);
                e = ::pthread_cond_timedwait(&_cond, &cs._mutex, &ts);
            }
            if (e == 0) {
                return true;
            }
            if (e != ETIMEDOUT)
            {
                assert(false);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR)
                    << "pthread_cond_timedwait" << ::strerror(e) << e;
            }
            if (!errorChecker()) {
                return false;
            }
        } while (!_signaled);
    }
    else
    {
        int e;
        {   ScopedWaitTimer timer(tw);
            e = ::pthread_cond_wait(&_cond, &cs._mutex);
        }
        if (e) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR)
                << "pthread_cond_wait" << ::strerror(e) << e;
        }
    }
    return true;
}


void Event::signal()
{
    _signaled = true;
    if (int e = ::pthread_cond_broadcast(&_cond)) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_THREAD_EVENT_ERROR)
            << "pthread_cond_broadcast" << ::strerror(e) << e;
    }
}

} // namespace
