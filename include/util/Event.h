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
 * @file Event.h
 *
 * @author knizhnik@garret.ru, roman.simakov@gmail.com
 *
 * @brief POSIX conditional variable
 */

#ifndef EVENT_H_
#define EVENT_H_

#include <util/PerfTime.h>
#include <functional>
#include <pthread.h>

namespace scidb
{

class Mutex;

class Event
{
    pthread_cond_t  _cond;
    bool _signaled { false };

public:

    /**
     * User-supplied function called at error polling intervals.
     * @return true to keep waiting, false to quit
     * @note May throw any scidb::Exception.
     */
    typedef std::function<bool()> ErrorChecker;
    enum { DEFAULT_ERRCHK_INTERVAL = 1 }; // in seconds

    Event();
    ~Event();

    /**
     * Wait for the Event to become signalled (based on the POSIX (timed) conditional variable+mutex).
     * If the event is signalled before the call to wait,
     * the wait will never return. The signal will cause the wait()'s of *all* threads to return.
     *
     * @param cs  held mutex to release during wait, reacquired when wait is done
     * @param errorChecker  if set, it will be invoked periodically and the false return code will force
     *                      this wait() to return regardless of whether the signal is received.
     * @param tw        category in which to include the elapsed waiting time
     * @param interval  frequency to poll for errors with errorChecker
     *
     * @note The errorChecker must also check for the condition predicate for which this Event is
     *       used because of the unavoidable race condition between the timer expiring (in
     *       pthread_cond_timedwait) and another thread signalling.
     */
    bool wait(Mutex& cs,
              ErrorChecker& errorChecker,
              perfTimeWait_e tw,
              timespec const* interval = nullptr);

    /**
     * Signal the event.
     */
    void signal();
};

} // namespace scidb

#endif  /* ! EVENT_H_ */
