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
 * @file ThreadPool.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The ThreadPool class
 */
#include <util/ThreadPool.h>

#include <util/InjectedErrorCodes.h>
#include "util/Thread.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

namespace scidb
{

ThreadPool::ThreadPool(size_t threadCount, std::shared_ptr<JobQueue> queue, const std::string& name)
: InjectedErrorListener(InjectErrCode::THREAD_START),
  _queue(queue),
  _currentJobs(threadCount),
  _threadCount(threadCount),
  _terminatedThreads(std::make_shared<Semaphore>()),
  _name(name)

{
    _shutdown = false;
    if (_threadCount <= 0) {
        throw InvalidArgumentException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread count";
    }
}

void ThreadPool::start()
{
    ScopedMutexLock lock(_mutex, PTW_SML_THREAD_POOL);

    if (_shutdown) {
        throw AlreadyStoppedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool cannot be started after being stopped";
    }
    if (_threads.size() > 0) {
        throw AlreadyStartedException(REL_FILE, __FUNCTION__, __LINE__)
        << "thread pool can be started only once";
    }
    assert(_threadCount>0);

    _threads.reserve(_threadCount);
    for (size_t i = 0; i < _threadCount; i++)
    {
        std::shared_ptr<Thread> thread(new Thread(*this, i));
        _threads.push_back(thread);
        thread->start();
        getInjectedErrorListener().throwif(__LINE__, __FILE__);
    }
}

bool ThreadPool::isStarted()
{
    ScopedMutexLock lock(_mutex, PTW_SML_THREAD_POOL);
    return _threads.size() > 0;
}

class FakeJob : public Job
{
public:
    FakeJob(): Job(std::shared_ptr<Query>(), "FakeJob") {
    }

    virtual void run()
    {
    }
};

void ThreadPool::stop()
{
    std::vector<std::shared_ptr<Thread> > threads;
    { // scope
        ScopedMutexLock lock(_mutex, PTW_SML_THREAD_POOL);
        if (_shutdown) {
            return;
        }
        threads.swap(_threads);
        _shutdown = true;
    }
    size_t nThreads = threads.size();
    for (size_t i = 0; i < threads.size(); ++i) {
        if (threads[i]->isStarted()) {
            // TODO: figure out and comment the purpose behind
            // having a FakeJob and enqueuing it at this point
            // to each started thread
            _queue->pushJob(std::shared_ptr<Job>(new FakeJob()));
        } else {
            --nThreads;
        }
    }
    _terminatedThreads->enter(nThreads, PTW_SEM_THREAD_TERM);
}

InjectedErrorListener ThreadPool::s_injectedErrorListener(InjectErrCode::THREAD_START);

} //namespace
