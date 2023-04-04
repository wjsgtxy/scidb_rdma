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
 * @file Condition.h
 *
 * @brief Wrapper for POSIX condition variable.  This class differs from
 * the Event class in that it can be signaled multiple times, each signal
 * waking up only a single thread on the head of the waiting queue.
 */

#ifndef CONDITION_H_
#define CONDITION_H_

#include <stdio.h>
#include <errno.h>
#include <util/Mutex.h>
#include <util/PerfTime.h>
#include <system/Exceptions.h>

namespace scidb
{

class Condition
{
private:
    pthread_cond_t  _cond;

public:

    Condition()
    {
        if (pthread_cond_init(&_cond, NULL)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                   SCIDB_LE_OPERATION_FAILED) 
                << "pthread_cond_init";
        }
    }

    ~Condition()
    {
        if (pthread_cond_destroy(&_cond)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                   SCIDB_LE_OPERATION_FAILED)
                << "pthread_cond_destroy";
        }
    }

    /**
     * Enter the condition queue at the tail and wait.  Thread is woken when the
     * Condition is signalled while calling thread is at head of the queue.
     * Due to spurious signals, thread must check condition predicate again
     * when it is woken up.
     * @param cs mutex associated with this Condition, 
     *           the same mutex has to be used the corresponding wait/signal
     */
    bool wait(Mutex& cs, perfTimeWait_e tw)
    {
        cs.checkForDeadlock();

        int e;
        {   
            ScopedWaitTimer timer(tw);
            e = pthread_cond_wait(&_cond, &cs._mutex);
        }
        if (e) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                   SCIDB_LE_OPERATION_FAILED)
                << "pthread_cond_wait";
        }
        return true;
    }

    /* Wake the waiting thread on the head of the Condition
       queue.
     */
    void signal()
    {
        if (pthread_cond_signal(&_cond))
        {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                   SCIDB_LE_OPERATION_FAILED)
                << "pthread_cond_signal";
        }
    }

    /* Wake all waiting threads on the Condition queue.
     */
    void broadcast()
    {
        if (pthread_cond_broadcast(&_cond))
        {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                   SCIDB_LE_OPERATION_FAILED)
                << "pthread_cond_broadcast";
        }
    }
};

}

#endif
