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
 * @file WorkQueue.h
 *
 * @brief The WorkQueue class
 *
 * A queue of work items that limits the max number of simultaneously dispatched items.
 * It uses an external thread-pool (i.e. JobQueue). The intent is that a collection
 * of cooperating WorkQueues can use a single thread-pool (which is easy to control),
 * but will not starve each other if the total max of outstanding items is no greater
 * than the number of threads in the pool.
 */

#ifndef WORKQUEUE_H_
#define WORKQUEUE_H_

#include <deque>
#include <memory>
#include <string>

#include <util/Mutex.h>
#include <util/Job.h>
#include "Utility.h"
namespace scidb
{

class Job;
class JobQueue;

/**
 * Serialization context is used to ensure that serialized work items remain serialized when they change WorkQueues.
 * Using the same serialization context from one WorkQueue to the next prevents the original (serialized) queue
 * from spawning more items for as long as the context is alive.
 */
class SerializationCtx
{
public:
    /// Constructor
    explicit SerializationCtx() {}
    /// Destructor
    virtual ~SerializationCtx();
    /// Record an upstream work queue
    /// @param wq queue to release at the end of this object life time
    void record(std::weak_ptr<scidb::WorkQueue>& wq);
private:
    // MAX_QUEUES is used in debug builds
    // Currently in SciDB there are no queues having more than MAX_QUEUEs WorkQueue stages.
    // A given SerializationCtx can ensure that it does not span more than MAX_QUEUE WorkQueues.
    static const size_t MAX_QUEUES=4;
    typedef std::deque< std::weak_ptr<scidb::WorkQueue> > QList;
    QList _queuesToRelease;
private:
    SerializationCtx(const SerializationCtx&);
    SerializationCtx& operator=(const SerializationCtx&);
};


class WorkQueue : public std::enable_shared_from_this<WorkQueue>
{
public: // subclasses

    friend class SerializationCtx;

    /**
     * A work item type that can be executed by WorkQueue
     * @param WorkQueue a pointer to the queue executing this item
     * @param SerializationCtx a pointer the item's serialization context
     *        If the serialization context is not used (i.e. its shared_ptr is not recorded),
     *        The WorkQueue of origin (and any subsequent ones) will consider the item complete.
     */
    typedef std::function<void(std::weak_ptr<WorkQueue>&,
            std::shared_ptr<SerializationCtx>&)> WorkItem;

    /**
     * Exception to indicate that a work item intends to remain active after its execution on this WorkQueue.
     * This means that this queue will consider it "outstanding" (and possibly not spawn new items) until
     * release() is called by the code orchestrated by the work item. This is a public exception but the client
     * code does not need to use it directly. See WorkQueue::SerializationCtx and WorkQueue::schedule.
     */
    DECLARE_SYSTEM_EXC_SUBCLASS_W_ARGS(PushBackException,
                                       SCIDB_SE_INTERNAL,
                                       SCIDB_LE_UNKNOWN_ERROR,
                                       "WorkQueue::PushBackException");

    /// Exception to indicate that this WorkQueue is full
    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(OverflowException,
                                      SCIDB_SE_NO_MEMORY,
                                      SCIDB_LE_RESOURCE_BUSY);

    /// Exception to indicate that a WorkQueue is in invalid state
    DECLARE_SYSTEM_EXC_SUBCLASS_W_ARGS(InvalidStateException,
                                       SCIDB_SE_INTERNAL,
                                       SCIDB_LE_UNKNOWN_ERROR,
                                       "WorkQueue::InvalidStateException");
public: // methods

    /**
     * Constructor
     * @param jobQueue where work items are executed
     */
    WorkQueue(const std::shared_ptr<JobQueue>& jobQueue, const std::string& name);  // TODO: reduce number of ctor variations

    /**
     * Constructor
     * @param jobQueue where work items are executed
     * @param maxOutstanding the max number of work items that can be executing concurrently
     */
    WorkQueue(const std::shared_ptr<JobQueue>& jobQueue, const std::string& name,
            uint32_t maxOutstanding);
    /**
     * Constructor
     * @param jobQueue where work items are executed
     * @param maxOutstanding the max number of work items that can be executing concurrently
     * @param maxSize the max number of work items (including maxOutstanding) that can be enqueued on this WorkQueue at any time
     */
    WorkQueue(const std::shared_ptr<JobQueue>& jobQueue, const std::string& name,
            uint32_t maxOutstanding,
            uint32_t maxSize);

    /**
     * Destructor
     */
    virtual ~WorkQueue()
    {
    }

    const std::string& name() const { return _name; }

    /**
     * Enqueue a WorkItem
     * @throw WorkQueue::OverflowException if there is no space left on the queue
     */
    void enqueue(WorkItem& work)
    {
        {
            ScopedMutexLock lock(_mutex, PTW_SML_WQ);
            if ((_size()+1)>_maxSize) {
                OverflowException e(REL_FILE, __FUNCTION__, __LINE__);
                e << "too many requests";
                throw e;
            }
            std::shared_ptr<SerializationCtx> sCtx = std::make_shared<SerializationCtx>();
            InternalWorkItem item = std::bind(&invokeWithContext, work, sCtx, std::placeholders::_1);
            _workQueue.push_back(InternalWorkItem());
            _workQueue.back().swap(item);
            assert(!item);
        }
        spawn();
    }

    /**
     * Reserve space on this WorkQueue for future enqueing
     * @throw WorkQueue::OverflowException if there is no space left on the queue
     */
    void reserve()
    {
        ScopedMutexLock lock(_mutex, PTW_SML_WQ);
        if ((_size()+1)>_maxSize) {
            OverflowException e(REL_FILE, __FUNCTION__, __LINE__);
            e << "too many requests";
            throw e;
        }
        ++_reserved;
        assert(_size() <= (_maxSize+_outstanding));
    }

    /**
     * Unreserve previously reserve()'d space from this WorkQueue
     * @throw WorkQueue::InvalidStateException if there are no current reservations
     */
    void unreserve()
    {
        ScopedMutexLock lock(_mutex, PTW_SML_WQ);
        assert(_size() <= (_maxSize+_outstanding));
        if (_reserved<=0) {
            assert(false);
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
        }
        --_reserved;
    }

    /**
     * Reserve space on this queue while executing on another queue (possibly the same one)
     * @param fromQueue the queue where this call is executed
     * @throw WorkQueue::OverflowException if there is no space left on the queue and fromQueue!=this
     */
    void reserve(std::shared_ptr<WorkQueue>& fromQueue)
    {
        const bool isSameQueue = (this == fromQueue.get());

        ScopedMutexLock lock(_mutex, PTW_SML_WQ);

        assert(_size() <= (_maxSize+_outstanding));

        if ((_size()+1) <= _maxSize) {
            ++_reserved;
            assert(_size() <= (_maxSize+_outstanding));
            return;
        }

        if (isSameQueue) {

            assert(_outstanding>0);
            ++_reserved;
            assert(_outstanding <= _maxOutstanding);
            assert(_size() <= (_maxSize+_outstanding));
            return;
        }

        OverflowException e(REL_FILE, __FUNCTION__, __LINE__);
        e << "too many requests";
        throw e;
    }


    /**
     * Enqueue a previously reserve()'d WorkItem. On successful enqueing,
     * the reservation is used up (no unreserve() call is necessary).
     * @throw WorkQueue::InvalidStateException if there are no reservations
     */
    void enqueueReserved(WorkItem& work,
            std::shared_ptr<SerializationCtx>& sCtx);


    /**
     * Enqueue a WorkItem from a different WorkQueue
     * @note IMPORTANT:
     * This method can only be invoked from within the WorkItem *currently* being executed by fromQueue.
     * If there is no space left on this queue, the item is still enqueued but it is *not* released from fromQueue.
     * When queue space becomes available, fromQueue will be notified by this WorkQueue "to complete transfer".
     * To prevent fromQueue from releasing the current item on its own this method throws WorkQueue::PushBackException.
     * If the number of unreleased items in fromQueue is too high (>=maxOutstanding), fromQueue will stall until some are released.
     * This mechanism is designed as the inter-queue flow control.
     * @param work WorkItem to enqueue
     * @param fromQueue must not be NULL, fromQueue is allowed to be the same as this WorkQueue
     * @throw WorkQueue::PushBackException if the item is enqueued under the overflow condition
     */
    void reEnqueue(WorkItem& work, std::shared_ptr<WorkQueue>& fromQueue);

    /**
     * Same functionality as reEnqueue() but also uses the SerializationCtx
     * to ensure that the execution of the WorkItem stays serialized until its completion.
     * (i.e. the origin queue fromQueue does not release the item before it actually runs to completion).
     * @param work WorkItem to enqueue
     * @param fromQueue must not be NULL, fromQueue is allowed to be the same as this WorkQueue
     * @param sCtx the serialization context which releases the item from all the queues after its completion
     * @throw WorkQueue::PushBackException if the item is enqueued under the overflow condition
     */
    void reEnqueueSerialized(WorkItem& work,
            std::shared_ptr<WorkQueue>& fromQueue,
            std::shared_ptr<SerializationCtx>& sCtx );

    /// Start executing work items
    /// @param jobQueue an override for the JobQueue at construction time
    void start(const std::shared_ptr<JobQueue>& jobQueue = std::shared_ptr<JobQueue>());

    /// Stop executing work items.
    /// The buffered items will remain not-executed.
    void stop();

    /// @return true if the queue can execute work items
    bool isStarted()
    {
        ScopedMutexLock lock(_mutex, PTW_SML_WQ);
        return _isStarted;
    }

    /// @return the current queue size (including outstanding)
    uint32_t size()
    {
        ScopedMutexLock lock(_mutex, PTW_SML_WQ);
        return _size();
    }

    /**
     * Transfer a Job from one WorkQueue to run on another WorkQueue.
     * The Job object is NOT directly inserted on the internal JobQueue.
     * Instead, a WorkItem is created that calls Job::executeOnWorkQueue()
     * @param the job to transfer
     * @param toQueue the destination WorkQueue where the job is to be run, must not be empty
     * @param fromQueue the origin WorkQueue where the job is currently running
     *        fromQueue can be empty or can be the same as toQueue
     * @param sCtx the serialization context which releases the item from all the queues after its completion
     * @note If fromQueue is NULL and toQueue is full,
     *       the job will not be enqueued and the job's query will be set in an error state.
     *       If fromQueue==toQueue, the job is re-enqueued to the end of the current (i.e. fromQueue) queue.
     */
    static void transfer(std::shared_ptr<Job>& job,
            std::shared_ptr<WorkQueue>& toQueue,
            std::weak_ptr<WorkQueue>& fromQueue,
            std::shared_ptr<SerializationCtx>& sCtx);

    /**
     * Schedule a Job to run on a WorkQueue where space has previously been reserve()'d.
     * The Job object is NOT directly inserted on the internal JobQueue.
     * Instead, a WorkItem is created that calls Job::executeOnWorkQueue().
     * @param the job to schedule
     * @param toQueue the destination WorkQueue where the job is to be run, must not be empty
     * @param sCtx the serialization context which releases the item from all the queues after its completion
     */
    static void scheduleReserved(std::shared_ptr<Job>& job,
            std::shared_ptr<WorkQueue>& toQueue,
            std::shared_ptr<SerializationCtx>& sCtx);

private:

    /// Invoke the work item with a serialization context from the fromQueue
    static void invokeWithContext(WorkItem& work,
            std::shared_ptr<SerializationCtx>& sCtx,
            std::weak_ptr<WorkQueue>& fromQueue)
    {
        work(fromQueue, sCtx);
    }

    /// @return the current queue size (including outstanding)
    uint32_t _size()
    {
        // mutex must be locked
        return safe_static_cast<uint32_t>(_outstanding + _reserved + _workQueue.size());
    }

    /// Mark the item as complete i.e. decrement the outstanding count etc.
    /// @note No locks must be taken
    void release()
    {
        {
            ScopedMutexLock lock(_mutex, PTW_SML_WQ);
            assert(_outstanding>0);
            --_outstanding;
            assert(_outstanding < _maxOutstanding);
            assert(_size() <= (_maxSize+_outstanding));
        }
        spawn();
    }

    /// Spawn more work items if possible
    void spawn();

    /// Helper method
    void reEnqueueInternal(WorkItem& work,
            std::shared_ptr<WorkQueue>& fromQueue,
            std::shared_ptr<SerializationCtx>& sCtx,
            bool isSameQueue);


private:
    WorkQueue();
    WorkQueue(const WorkQueue&);
    WorkQueue& operator=(const WorkQueue&);

    const static uint32_t DEFAULT_MAX_OUTSTANDING = 1;
    const static uint32_t DEFAULT_MAX_SIZE = 1000000;

private:

    typedef std::function<void(std::weak_ptr<WorkQueue>&)> InternalWorkItem;

    /// Utility class to execute a WorkItem on a JobQueue
    class WorkQueueJob : public Job
    {
    public:
        virtual ~WorkQueueJob() {}

        WorkQueueJob(WorkQueue::InternalWorkItem& work,
                std::shared_ptr<WorkQueue> workQ)
        : Job(std::shared_ptr<Query>(), "WorkQueueJob"), _workQueue(workQ)
        {
            /*
             * the swap saves memory allocation/copy
             * but it also dangerous because it clears the
             * passed in InternalWorkItem.
             * This class is private, so it should be OK
             */
            _workItem.swap(work);
        }

    private:
        WorkQueueJob();
        WorkQueueJob(const WorkQueueJob&);
        WorkQueueJob& operator=(const WorkQueueJob&);

        void run() override
        {
            assert(_workItem);
            try {
                _workItem(_workQueue);
            } catch(const scidb::WorkQueue::PushBackException& ) {
                cleanupWorkItem(false);
                return;
            } catch (const scidb::Exception& e) {
                cleanupWorkItem(true);
                e.raise();
            }
            cleanupWorkItem(true);
        }

        void cleanupWorkItem(bool releaseFromQueue)
        {
            WorkQueue::InternalWorkItem().swap(_workItem); //destroy
            std::shared_ptr<WorkQueue> wq;
            if (releaseFromQueue &&
                    ( wq = _workQueue.lock())) {
                wq->release();
            }
        }
        WorkQueue::InternalWorkItem _workItem;
        std::weak_ptr<WorkQueue> _workQueue;
    };
    friend class WorkQueueJob;

    private:
    std::shared_ptr<JobQueue> _jobQueue;
    std::deque<InternalWorkItem> _workQueue;
    std::deque< std::pair<InternalWorkItem, std::weak_ptr<WorkQueue> > > _overflowQueue;
    uint32_t _maxOutstanding;
    uint32_t _maxSize;
    uint32_t _outstanding;
    uint32_t _reserved;
    Mutex _mutex;
    bool _isStarted;
    const std::string _name;
};

} //namespace scidb

#endif /* WORKQUEUE_H_ */
