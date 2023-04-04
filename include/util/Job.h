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
 * @file Job.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The base class for jobs of thread pool
 */

#ifndef JOB_H_
#define JOB_H_

#include <atomic>
#include <memory>
#include <stack>
#include <string>
#include <vector>

#include <util/Semaphore.h>
#include <util/Mutex.h>

namespace scidb
{
class Exception;
class WorkQueue;
class Query;
class SerializationCtx;

/**
 * Base virtual class for a job to be executed by threads in a ThreadPool.
 */
class Job : public std::enable_shared_from_this<Job>
{
private:
    Semaphore _done;
    std::atomic_bool _removed;
    const std::string _name;

    friend class WorkQueue;

    // Every time a job is executed, its pointer is pushed on this stack.
    static thread_local std::stack<std::weak_ptr<Job> > _jobStack;

    /// On-stack object that pushes and pops a given job on/off the
    /// per-thread job stack.
    class StackHelper
    {
    public:
        explicit StackHelper(const std::shared_ptr<Job>& curJob);
        ~StackHelper();
    private:
        std::shared_ptr<Query> _savedQuery;
        bool _restoreQuery { false };
    };

    friend class StackHelper;

protected:

    typedef std::function<void()> Handler;
    std::shared_ptr<Exception> _error;
    std::shared_ptr<Query> _query; //XXX TODO: make it a weak_ptr ?

    // When a job is executed multiple times using executeOnQueue(),
    // _wq, _wqSCtx, _currHandler need to be set accordingly
    // _wq, _wqSCtx are set by the WorkQueue invoking executeOnQueue()
    // _currHandler must be set by the job algorithm prior
    // to scheduling the next invocation of executeOnQueue()

    Mutex _currStateMutex;
    std::weak_ptr<WorkQueue> _wq;
    std::weak_ptr<SerializationCtx> _wqSCtx;
    Handler _currHandler;

    /// This method must be implemented in child classes
    /// It gets invoked by Job::execute() when this job is executed directly on a JobQueue or
    /// by Job::executeOnQueue() when this job is executed on a WorkQueue
    virtual void run() = 0;

public:

    /// Get the top job from the thread local stack
    /// @pre the stack cannot be empty
    static std::shared_ptr<Job> getCurrentJobPerThread();

    Job(std::shared_ptr<Query>const& query, const std::string& name)
    :
      _done(),
      _removed(false),
      _name(name),
      _query(query)        // Null query pointers are permitted.
    {
    }

    virtual ~Job()
    {
    }

    std::shared_ptr<Query> getQuery()
    {
        return _query;
    }

    const std::string& name() const { return _name; }

    /**
     * The (pool of) threads servicing this Job's JobQueue call this method
     */
    void execute();

    /**
     * FOR INTERNAL USE ONLY.
     * If this job is enqueued onto a WorkQueue in a form of a WorkItem,
     * this method is called. A given job can be executed multiple times
     * (presumable to execute different steps of an algorithm) using this method.
     * @param wq the WorkQueue executing this job
     * @throw WorkQueue::PushBackException if this job is re-enqueued onto another under the overflow condition
     * @see WorkQueue::PushBackException
     */
    void executeOnQueue(std::weak_ptr<WorkQueue>& wq,
                        std::shared_ptr<SerializationCtx>& sCtx);

    /// Flags for #wait()
    enum { CAN_RAISE = 0x01, CAN_MULTIWAIT = 0x02 };

    /// Waits until job is done
    bool wait(perfTimeWait_e tw, unsigned flags = CAN_MULTIWAIT);

    /// Force to skip job execution
    void skip()
    {
        _removed = true;
    }

    void rethrow();
};

} //namespace

#endif /* JOB_H_ */
