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
 * @file ReplicationManager.h
 *
 * @details Simple replication manager based on the original version
 * that was included in the old storage manager code.  See the wiki
 * page DEV/How+Chunk+Replication+Works for an overview.
 */

#ifndef REPLICATION_MANAGER_H_
#define REPLICATION_MANAGER_H_

#include <deque>
#include <map>
#include <network/NetworkManager.h>
#include <util/Event.h>
#include <util/Mutex.h>
#include <util/Thread.h>
#include <util/Notification.h>
#include <query/Query.h>
#include <util/InjectedError.h>
#include <array/Address.h>
#include <array/CachedDBChunk.h>

namespace scidb
{

    /**
     * Class that manages data replication for an array
     */
    class ReplicationManager : public Singleton<ReplicationManager>
    {
    public:

        /**
         * Represents a chunk and its replication state
         */
        class Item
        {
        public:
            Item(InstanceID instanceId,
                 const std::shared_ptr<MessageDesc>& chunkMsg,
                 const std::shared_ptr<Query>& query) :
                _instanceId(instanceId),
                _chunkMsg(chunkMsg),
                _query(query),
                _isDone(false),
                _isStalled(false)
                {
                    assert(instanceId != INVALID_INSTANCE);
                    assert(chunkMsg);
                    assert(query);
                }
            virtual ~Item() {}

            std::weak_ptr<Query> getQuery() { return _query; }

            InstanceID getInstanceId() { return _instanceId; }

            std::shared_ptr<MessageDesc> getChunkMsg() { return _chunkMsg; }

            /**
             * @return true iff the chunk has been sent to the network manager or an error has occurred
             */
            bool isDone() const { return _isDone; }

            /** @return true iff chunk not yet transmitted due to flow control */
            bool isStalled() const { return _isStalled; }

            /**
             * @throw if the replication failed
             */
            void throwIfError()
            {
                if (_error) {
                    assert(_isDone);
                    _error->raise();
                    SCIDB_UNREACHABLE();
                }
            }

            /**
             * @return true iff the replication failed
             */
            bool hasError() const
            {
                assert(!_error || _isDone);
                return bool(_error);
            }

        private:

            void setDone()
                {
                    _isDone = true;
                    _isStalled = false;
                    _chunkMsg.reset();
                }

            void setDone(const std::shared_ptr<scidb::Exception>& e)
                {
                    setDone();
                    _error = e;
                }

            void setStalled() { _isStalled = true; }

            friend class ReplicationManager;
            Item(const Item&);
            Item& operator=(const Item&);

            InstanceID                        _instanceId;
            std::shared_ptr<MessageDesc>      _chunkMsg;
            std::weak_ptr<Query>              _query;
            bool                              _isDone;
            bool                              _isStalled;
            std::shared_ptr<scidb::Exception> _error;
        };

        typedef std::vector<std::shared_ptr<Item> > ItemVector;

    private:

        typedef std::deque<std::shared_ptr<Item> > RepItems;
        typedef std::map<InstanceID, std::shared_ptr<RepItems> > RepQueue;
        typedef std::pair<size_t,size_t> IndexRange;

    public:

        ReplicationManager();
        virtual ~ReplicationManager() {}

        /**
         * Start the operations
         */
        void start(const std::shared_ptr<JobQueue>& jobQueue);

        /**
         * stop the operations and release resources
         */
        void stop();

        /**
         * replicate an item
         * @note Only public because needed by ReplicationContext.
         */
        void send(const std::shared_ptr<Item>& item);

        /**
         * wait until the item is sent to network manager
         * @note Only public because needed by ReplicationContext.
         */
        void wait(const std::shared_ptr<Item>& item);

        /**
         * discard the item
         */
        void abort(const std::shared_ptr<Item>& item)
            {
                assert(item);
                ScopedMutexLock cs(_repMutex, PTW_SML_REP);
                if (item->isDone()) {
                    return;
                }
                item->setDone(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_INTERNAL,SCIDB_LE_UNKNOWN_ERROR));
            }

        /**
         * Return whether replication is started
         */
        bool isStarted()
            {
                ScopedMutexLock cs(_repMutex, PTW_SML_REP);
                return bool(_lsnrId);
            }

        /**
         * Reserve space on the inbound replication queue and get a
         * work item which will schedule a replication job on the inbound replication queue
         * @param job replication job
         * @return WorkQueue::WorkItem for running on any queue
         * @throws WorkQueue::OverflowException
         */
        WorkQueue::WorkItem getInboundReplicationItem(std::shared_ptr<Job>& job)
            {
                // no synchronization is needed because _inboundReplicationQ
                // is initialized at start() time and is never changed later
                assert(isStarted());
                assert(_inboundReplicationQ);
                assert(job);
                std::shared_ptr<Reservation> res = std::make_shared<Reservation>(_inboundReplicationQ, job);

                WorkQueue::WorkItem item = std::bind(&Reservation::enqueue,
                                                     res,
                                                     std::placeholders::_1,
                                                     std::placeholders::_2);
                return item;
            }

        /**
         * Replicate a chunk to other instances
         * @param[in] desc Array descriptor for target array
         * @param[in] addr Chunk address
         * @param[in] chunk Chunk to replicate (use nullptr for tombstones)
         * @param[in] data Buffer containing (compressed or uncompressed) chunk data
         * @param[in] compressedSize Size of data when compressed
         * @param[in] decompressedSize Size of data when decompressed
         * @param[in] query Pointer to current query
         * @param[in,out] replicas Vector of replica items created during replication
         *
         * @note You must subsequently call waitForReplicas(), otherwise replication messages
         * stalled by Connection::Channel flow control will never be sent.  You may make multiple
         * replicate() calls prior to calling waitForReplicas(), and the new replication items are
         * appended to 'replicas'.
         */
        void replicate(ArrayDesc const& desc,
                       PersistentAddress const& addr,
                       CachedDBChunk const* chunk,
                       void const* data,
                       size_t compressedSize,
                       size_t decompressedSize,
                       std::shared_ptr<Query> const& query,
                       ItemVector& replicas);

        /**
         * Wait for the replica items (i.e. chunks) to be sent to NetworkManager
         * @param replicas a list of replica items to wait on
         */
        void waitForReplicas(ItemVector& replicas);

        /**
         * Abort any outstanding replica items (in case of errors)
         * @param replicas a list of replica items to abort
         */
        void abortReplicas(ItemVector const& replicas);

        /**
         * Determine if a given chunk is a primary replica on this instance
         * @param chunk to examine
         * @param redundancy for the array of this chunk
         * @return true if the chunk is a primary replica
         */
        bool isPrimaryReplica(CachedDBChunk const* chunk,
                              size_t redundancy);

        /**
         * Return the physical instance ID of the primary chunk copy (i.e.
         * of the zeroth replica)
         * @param desc chunk array descriptor
         * @param address chunk address containing the chunk coordinates
         */
        InstanceID getPrimaryInstanceId(ArrayDesc const& desc,
                                        Coordinates const& address);
        InstanceID getPrimaryInstanceId(ArrayDesc const& desc,
                                        PersistentAddress const& address)
            { return getPrimaryInstanceId(desc, address.coords); }

        /**
         * @brief Check if chunk should be considered by DBArrayIterator
         * @param desc chunk array descriptor
         * @param addr address of chunk
         * @param chunkIid instance id of chunk owner
         * @param query ref to query
         *
         * @details Scans of persistent arrays will only see this
         * chunk if (a) it is @em not a replica, or (b) it is a
         * replica whose home instance is dead according to the
         * query's liveness information, and whose address hashes to
         * @em this instance (given the liveness and array residency).
         */
        bool isResponsibleFor(ArrayDesc const& desc,
                              PersistentAddress& addr,
                              InstanceID chunkIid,
                              std::shared_ptr<Query> const& query);

    private:

        /**
         * Make sure the range @ curIndex is inserted in ascending order
         */
        void insertLastInOrder(IndexRange* ranges, size_t curIndex);

        /**
         * Assert that the ranges are ordered, not overlapping, and dont cover
         * maxSize elements
         */
        void validateRangesInDebug(const IndexRange* ranges,
                                   const size_t nRanges,
                                   const size_t maxSize);
        /**
         * Given an index into the elements not covered by unavailableRanges,
         * map that index into the index in [0,maxSize-1].
         * @param index essentially, it is the 0-based index as if the elements
         *        in unavailable ranges did not exist.
         * @param unavailableRanges an array of IndexRange's
         * @param nRanges number of ranges in unavailableRanges
         * @param maxSize max possible index + 1
         * @return index in [0,maxSize-1] which is not in any of unavailableRanges
         */
        size_t findIndexInResidency(const size_t index,
                                    const IndexRange* unavailableRanges,
                                    const size_t nRanges,
                                    const size_t maxSize);

        /**
         *  Return the logical instance position of the primary chunk copy
         *  (i.e. of the zeroth replica)
         *  @param desc chunk array descriptor
         *  @param address persistent address containing the chunk coordinates
         *         or coords themselves
         */
        InstanceID getPrimaryInstanceIdPosition(ArrayDesc const& desc,
                                                Coordinates const& coords);
        InstanceID getPrimaryInstanceIdPosition(ArrayDesc const& desc,
                                                PersistentAddress const& address)
            { return getPrimaryInstanceIdPosition(desc, address.coords); }

        /**
         * Finds the instance for the next replica and updates
         * replicas[nReplicas] with that value. It also adds all the instances
         * running on the server of replicas[nReplicas] to unavailableRanges.
         * @param res array residency
         * @param targetIndex index of the instance ignoring unavaialbleRanges
         * @param [in,out] unavailableRanges ranges of indeces to be excluded from the residency
         *        for the purposes of locating the instance @ targetInstance
         * @param replicas the current list of replica targets of length nReplicas
         * @param nReplicas the current length of replicas and unavaliableRanges
         * @param resSize the number of instances in the residency
         * @return number of instances no longer available for replication
         */
        size_t computeNextReplicaTarget(const ArrayResPtr& res,
                                        const size_t targetIndex,
                                        IndexRange* unavailableRanges,
                                        InstanceID* replicas,
                                        const size_t nReplicas,
                                        const size_t resSize);

        /**
         * Assign replication instances for the particular chunk
         * @param[in/out] replicas  array of InstanceIDs to be filled in,
         *                          must have size > redundancy
         * @param[in] desc  array schema
         * @param[in] address  address of chunk to replicate
         */
        void getReplicasInstanceId(InstanceID* replicas,
                                   ArrayDesc const& desc,
                                   PersistentAddress const& address);

        /**
         * Class to manage space reservation and enqueing on the inbound replication queue
         */
        class Reservation
        {
        public:
            /**
             * Constructor reserves space on a queue
             * @param queue for reservation
             * @param job to schedule on the queue
             */
            Reservation(const std::shared_ptr<WorkQueue>& queue,
                        const std::shared_ptr<Job>& job)
                : _queue(queue), _job(job)
                {
                    assert(queue);
                    assert(job);
                    queue->reserve();
                }

            /**
             * Destructor unreserves space on the queue if the job was not enqueued
             */
            ~Reservation()
                {
                    if ( std::shared_ptr<WorkQueue> q = _queue.lock()) {
                        q->unreserve();
                    }
                }

            /**
             * Enqueue the job onto the queue
             * @param fromQueue is the queue invoking this method
             * @param sCtx serialization context
             * @see scidb::WorkQueue
             */
            void enqueue(std::weak_ptr<scidb::WorkQueue>& fromQueue,
                         std::shared_ptr<SerializationCtx>& sCtx)
                {
                    if ( std::shared_ptr<WorkQueue> q = _queue.lock()) {
                        // sCtx is ignored because _inboundReplicationQ is single-threaded,
                        // and there is no need to hold up fromQueue,
                        // which just transfers its jobs to _inboundReplicationQ.
                        std::shared_ptr<SerializationCtx> emptySCtx;
                        WorkQueue::scheduleReserved(_job, q, emptySCtx);
                    }
                    _queue.reset();
                }

        private:

            std::weak_ptr<WorkQueue> _queue;
            std::shared_ptr<Job> _job;
    };

    void handleConnectionStatus(
        Notification<NetworkManager::ConnectionStatus>::MessageTypePtr
        connStatus);
    bool sendItem(RepItems& ri);
    void clear();
    static bool keepWaiting(const std::shared_ptr<Item>& item);

    ReplicationManager(const ReplicationManager&);
    ReplicationManager& operator=(const ReplicationManager&);

    RepQueue _repQueue;
    Mutex    _repMutex;
    Event    _repEvent;
    Notification<NetworkManager::ConnectionStatus>::SubscriberID _lsnrId;

    std::shared_ptr<WorkQueue> _inboundReplicationQ;
};
}
#endif
