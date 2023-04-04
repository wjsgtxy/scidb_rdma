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
 * @file Thread.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Thread class for executing jobs from queue
 */

#ifndef THREAD_H_
#define THREAD_H_

#include <assert.h>
#include <pthread.h>
#include <boost/noncopyable.hpp>

#include <util/Job.h>
#include <util/JobQueue.h>
#include <util/ThreadPool.h>


namespace scidb
{

extern "C" typedef void *(*pthread_callback)(void *);

/**
 * A class that extracts a job from a thread pool and executes it.
 */
class Thread: private boost::noncopyable
{
private:
    pthread_t _handle;
    ThreadPool& _threadPool;
    size_t _index;
    std::shared_ptr<Job> _currentJob;
    bool _isStarted;

    /// A wrapper over _threadFunction().
    static void* threadFunction(void* arg);

    /// Use an infinite while loop to pop a job from ThreadPool's job queue, and call Job::execute().
    void _threadFunction();

public:
    /**
     * @param[in] threadPool  the thread pool to extract job from.
     * @param[in] index       the index in the vector ThreadPool::_currentJobs to store the extracted job.
     */
    Thread(ThreadPool& threadPool, size_t index);

    /**
     * Call pthread_create() to create and start a thread, which essentially
     * calls Thread::threadFunction(this).
     */
    void start();

    bool isStarted();
    virtual ~Thread();
    /**
     * Put this thread to sleep
     * @param nanoSec number of nanoseconds to sleep
     */
    static void nanoSleep(uint64_t nanoSec);
};


/// @return current time in nanoseconds
uint64_t getTimeInNanoSecs();
/// @return return (getTimeInNanoSecs() - startTimeNanoSec >= timeoutNanoSec && timeoutNanoSec > 0)
bool hasExpired(uint64_t startTimeNanoSec, uint64_t timeoutNanoSec);


} //namespace

#endif /* THREAD_H_ */
