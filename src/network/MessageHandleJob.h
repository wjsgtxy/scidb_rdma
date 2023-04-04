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

/*
 * MessageHandleJob.h
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef MESSAGEHANDLEJOB_H_
#define MESSAGEHANDLEJOB_H_

#include <network/NetworkMessage.h> // for MessageID
#include <network/proto/scidb_msg.pb.h>

#include <array/AttributeID.h>
#include <util/Job.h>

#include <boost/asio.hpp>

#include <memory>
#include <stdint.h>

namespace scidb
{
    class Array;
    class InstanceLiveness;
    class MessageDesc;
    class NetworkManager;
    class Query;
    class WorkQueue;

    /**
     * The class created by network message handler for adding to queue to be processed
     * by thread pool and handle message from client.
     */
    class MessageHandleJob: public Job
    {
    public:

        /**
         * @brief Get/set channel priority.
         * @see SessionProperties
         * @{
         */
        int getPriority() { return _priority; }
        void setPriority(int p) { _priority = p; }
        /** @} */

        MessageHandleJob(const std::shared_ptr<MessageDesc>& messageDesc)
            : Job(std::shared_ptr<Query>(), "MessageHandleJob")
            , _priority(0)
            , _messageDesc(messageDesc)
        { }

        /**
         * @brief Enqueue this job to a given queue.
         * @param queue the WorkQueue for running this job
         * @param action how to handle a work queue overflow
         * @param handleOverflow set query to error state (else throw)
         * @throws WorkQueue::OverflowException if queue has no space and !handleOverflow
         * @note After you call enqueue, do NOT read or write this
         *       MessageHandleJob (except with extreme care; see
         *       below).  The dispatch() function is single threaded
         *       before calling enqueue(); but after enqueue() is
         *       called, other threads may read or change its content.
         *       In most cases, you do not need to read/write after
         *       calling enqueue().  But if you really do, protect ALL
         *       accesses to it with a mutex.
         */
        void enqueue(const std::shared_ptr<WorkQueue>& queue, bool handleOverflow = true);

        /**
         * Based on its contents this message is prepared and scheduled to run
         * on an appropriate queue.
         * @param nm NetworkManager pointer to use for dispatching
         */
        virtual void dispatch(NetworkManager* nm) = 0;

    private:
        int _priority;

    protected:
        std::shared_ptr<MessageDesc> _messageDesc;
        std::shared_ptr<boost::asio::deadline_timer> _timer;

        /// Reschedule this job if array locks are not taken
        void reschedule(uint64_t delayMicroSec);

        /// Reschedule this job on the same WorkQueue
        void reschedule();

        /// For instance-to-instance messages, confirm sender is cluster member.
        void confirmSenderMembership();

        /**
         * Validate remote message information identifying a chunk
         * @throws scidb::SystemException if the Job::_query is invalid or the arguments are invalid
         * @note in Debug build invalid arguments will cause an abort()
         * @param array to which the chunk corresponds
         * @param msgType message ID
         * @param arrayType 'objType' identifier used in some messages or ~0
         * @param attId the chunk's attribute ID
         * @param physInstanceID physical instance ID of message source
         */
        void validateRemoteChunkInfo(const Array* array,
                                     const MessageID msgID,
                                     const uint32_t arrayType,
                                     const AttributeID attId,
                                     const InstanceID physInstanceID);

    private:
        /// Handler for for the array lock timeout. It reschedules the current job
        static void handleRescheduleTimeout(std::shared_ptr<Job>& job,
                                            std::shared_ptr<WorkQueue>& toQueue,
                                            std::shared_ptr<SerializationCtx>& sCtx,
                                            std::shared_ptr<boost::asio::deadline_timer>& timer,
                                            const boost::system::error_code& error);
    };

    class ServerMessageHandleJob : public MessageHandleJob
    {
    public:
        ServerMessageHandleJob(const std::shared_ptr<MessageDesc>& messageDesc);
        virtual ~ServerMessageHandleJob();

        /**
         * Based on its contents this message is prepared and scheduled to run
         * on an appropriate queue.
         * @see MessageHandleJob::dispatch
         * @param workQueue a system queue for running jobs that are guaranteed to make progress
         */
        void dispatch(NetworkManager* nm) override;

    protected:
        /// Implementation of Job::run()
        /// @see Job::run()
        void run() override;

    private:
        NetworkManager& _networkManager;

        /**
         * Any message on the wire includes a physical sender ID in it.
         * Users of NetworkManager deal with logical (sender AND receiver) IDs.
         * A typical pattern is: when a ServerMessageHandleJob receives a
         * message, it translates the sender ID from physical to logical.
         */
        size_t _logicalSourceId;

        bool _mustValidateQuery;

        typedef void(ServerMessageHandleJob::*MsgHandler)();
        static MsgHandler getMsgHandler(MessageType mType);

        void sgSync();
        void handlePreparePhysicalPlan();
        void handleExecutePhysicalPlan();
        void handleQueryResult();
        void handleRemoteChunk();
        void handleFetch();
        void handleSGFetchChunk(bool inBackground=false);
        void handleSyncRequest();
        void handleSyncResponse();
        void handleError();
        void handleNotify();
        void handleWait();
        void handleBarrier();
        void handleBufferSend();
        void handleReplicaSyncResponse();
        void handleChunkReplica();
        void handleInstanceStatus();
        void handleResourcesFileExists();
        void throwOnInvalidMessage();
        void handleAbortRequest();
        void handleCommitRequest();
        void handleUpdateQueryResult();
        void handleAbortResponse();
        void handleCommitResponse();

    };  // class ServerMessageHandleJob
} // namespace

#endif /* MESSAGEHANDLEJOB_H_ */
