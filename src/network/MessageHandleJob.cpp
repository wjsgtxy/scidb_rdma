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
 * MessageHandleJob.cpp
 *
 *  Created on: Jan 12, 2010
 *      Author: roman.simakov@gmail.com
 */

// TODO: split MessageHandleJob and ServerMessageHandleJob
//       methods to separate files

#include <network/MessageHandleJob.h>

#include <memory>
#include <log4cxx/logger.h>

#include <array/CompressedBuffer.h>

#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <query/PullSGContext.h>
#include <query/Multiquery.h>
#include <query/Query.h>
#include <query/QueryProcessor.h>
#include <query/RemoteArray.h>
#include <query/RemoteMergedArray.h>
#include <query/executor/ScopedQueryThread.h>
#include <rbac/Session.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Resources.h>
#include <system/Warnings.h>
#include <util/OnScopeExit.h>
#include <util/PerfTime.h>
#include <util/PerfTimeScope.h>
#include <array/ArrayIterator.h>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.msgs"));

void MessageHandleJob::reschedule()
{
    // prepare for a callback
    bool locked = _currStateMutex.isLockedByThisThread();
    SCIDB_ASSERT(locked);

    std::shared_ptr<WorkQueue> toQ(_wq.lock());
    assert(toQ);
    std::shared_ptr<SerializationCtx> sCtx(_wqSCtx.lock());
    assert(sCtx);
    std::shared_ptr<Job> thisJob(shared_from_this());
    WorkQueue::transfer(thisJob, toQ, _wq, sCtx);
}

void MessageHandleJob::reschedule(uint64_t delayMicroSec)
{
    // prepare for a callback
    SCIDB_ASSERT(_currStateMutex.isLockedByThisThread());

    assert(delayMicroSec>0);
    std::shared_ptr<WorkQueue> toQ(_wq.lock());
    assert(toQ);
    std::shared_ptr<SerializationCtx> sCtx(_wqSCtx.lock());
    assert(sCtx);
    std::shared_ptr<Job> thisJob(shared_from_this());

    // try again on the same queue after a delay
    toQ->reserve(toQ);
    try {
        if (!_timer) {
            _timer = std::make_shared<boost::asio::deadline_timer>(NetworkManager::getInstance()->getIOService());
        }
        boost::system::error_code ec;
        size_t rc = _timer->expires_from_now(boost::posix_time::microseconds(delayMicroSec), ec);
        if (ec) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                << "boost::asio::expires_from_now" << rc << ec.value() << ec.message() << delayMicroSec;
        }
        typedef std::function<void(const boost::system::error_code& error)> TimerCallback;
        TimerCallback func = std::bind(&handleRescheduleTimeout,
                                       thisJob,
                                       toQ,
                                       sCtx,
                                       _timer,
                                       std::placeholders::_1);
        _timer->async_wait(func);
    } catch (const scidb::Exception& e) {
        toQ->unreserve();
        e.raise();
    }
}

void
MessageHandleJob::handleRescheduleTimeout(std::shared_ptr<Job>& job,
                                          std::shared_ptr<WorkQueue>& toQueue,
                                          std::shared_ptr<SerializationCtx>& sCtx,
                                          std::shared_ptr<boost::asio::deadline_timer>& timer,
                                          const boost::system::error_code& error)
{
    static const char *funcName="MessageHandleJob::handleRescheduleTimeout: ";
    if (error == boost::asio::error::operation_aborted) {
        LOG4CXX_ERROR(logger, funcName
                      <<"Lock timer cancelled: "
                      <<" queue=" << toQueue.get()
                      <<", job="<<job.get()
                      <<", queryID="<<job->getQuery()->getQueryID());
        assert(false);
    } else if (error) {
        LOG4CXX_ERROR(logger, funcName
                      <<"Lock timer encountered error: "<<error
                      <<" queue=" << toQueue.get()
                      <<", job="<<job.get()
                      <<", queryID="<<job->getQuery()->getQueryID());
        assert(false);
    }
    // we will try to schedule anyway
    WorkQueue::scheduleReserved(job, toQueue, sCtx);
}

void MessageHandleJob::confirmSenderMembership()
{
    const MembershipID minMembId(0);
    Cluster* cluster = Cluster::getInstance();
    InstMembershipPtr membership(cluster->getInstanceMembership(minMembId));

   InstanceID srcInstanceId = _messageDesc->getSourceInstanceID();
   ASSERT_EXCEPTION(isValidPhysicalInstance(srcInstanceId),
                    "Peer instance has invalid physical id");
    membership->confirmMembership(srcInstanceId);
}

void MessageHandleJob::validateRemoteChunkInfo(const Array* array,
                                               const MessageID msgId,
                                               const uint32_t objType,
                                               const AttributeID attId,
                                               const InstanceID physicalSourceId)
{
    if (!array) {
        // the query must be deallocated, validate() should fail
        _query->validate();
        stringstream ss;
        ss << "Unable to find remote array for remote message:"
           << " messageID="<<msgId
           << " array type="<<objType
           << " attributeID="<<attId
           << " from " << Iid(physicalSourceId)
           << " for queryID="<<_query->getQueryID()
           << " for phyOpID="<<_messageDesc->getPhysicalOperatorID();
        ASSERT_EXCEPTION(false, ss.str());
    }

    const auto& attrs = array->getArrayDesc().getAttributes();
    auto foundAttr = (attrs.find(attId) != attrs.end());
    if (!foundAttr) {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<<msgId
           << " invalid attributeID="<<attId
           << " array type="<<objType
           << " from " << Iid(physicalSourceId)
           << " for queryID="<<_query->getQueryID()
           << " for phyOpID="<<_messageDesc->getPhysicalOperatorID();
        ASSERT_EXCEPTION(false, ss.str());
    }
}

void MessageHandleJob::enqueue(const std::shared_ptr<WorkQueue>& q, bool handleOverflow)
{
    static const char *funcName = "MessageHandleJob::enqueue: ";
    LOG4CXX_TRACE(logger, funcName <<  strMsgType(_messageDesc->getMessageType())
                  << " for queryID=" << _messageDesc->getQueryID()
                  << ", from instanceID=" << Iid(_messageDesc->getSourceInstanceID()));
    if (!q) {
        LOG4CXX_WARN(logger,  funcName << "Dropping " << strMsgType(_messageDesc->getMessageType())
                     << " for queryID=" << _messageDesc->getQueryID()
                     << ", from instanceID=" << Iid(_messageDesc->getSourceInstanceID())
                     << " because the query appears deallocated (no work queue)");
        return;
    }

    std::shared_ptr<Job> thisJob(shared_from_this());
    WorkQueue::WorkItem work = std::bind(&Job::executeOnQueue,
                                         thisJob,
                                         std::placeholders::_1,
                                         std::placeholders::_2);
    thisJob.reset();
    assert(work);
    try {
        q->enqueue(work);
        LOG4CXX_TRACE(logger, funcName << " q->enque(work), q->isStarted"<<q->isStarted());
    } catch (const WorkQueue::OverflowException& e) {
        if (handleOverflow) {
            assert(_query);
            LOG4CXX_ERROR(logger,  funcName <<
                          "Overflow exception from the message queue ("
                          << q.get()
                          <<") queryID="<<_query->getQueryID() << " : " << e.what());
                arena::ScopedArenaTLS arenaTLS(_query->getArena());
                _query->handleError(e.clone());
        } else {
            LOG4CXX_WARN(logger, funcName << " OverflowException with handleOverflow false");
            e.raise();
        }
    }
}
// 构造一个job
ServerMessageHandleJob::ServerMessageHandleJob(const std::shared_ptr<MessageDesc>& messageDesc)
: MessageHandleJob(messageDesc),
  _networkManager(*NetworkManager::getInstance()),
  _logicalSourceId(INVALID_INSTANCE),
  _mustValidateQuery(true)
{
    confirmSenderMembership();

    Cluster* cluster = Cluster::getInstance();
    const QueryID queryID = _messageDesc->getQueryID();
    const OperatorID phyOpID = _messageDesc->getPhysicalOperatorID();

    // dz from trace to debug
    LOG4CXX_DEBUG(logger, "Creating a new job for "
                  << strMsgType(_messageDesc->getMessageType())
                  << " from instance=" << Iid(_messageDesc->getSourceInstanceID()) // src是s0-i0, master
                  << " with message size=" << _messageDesc->getMessageSize()
                  << " for queryID=" << queryID
                  << " for phyOpID=" << phyOpID);

    if (queryID.isValid()) {
       if (_messageDesc->getMessageType() == mtNotify) {
           InstanceID srcInstanceId = _messageDesc->getSourceInstanceID();
           ASSERT_EXCEPTION(srcInstanceId != cluster->getLocalInstanceId(),
                            "mtNotify cannot be sent from the local source");
           // TODO:  Try get-by-id, create if it fails.  We'll be here for each snippet!
           _query = Query::create(queryID,srcInstanceId);
        } else {
           _query = Query::getQueryByID(queryID);
        }
    } else {
        ASSERT_EXCEPTION(queryID.isFake(), "Invalid query ID in server message");

        LOG4CXX_TRACE(logger, "Creating fake query for " << strMsgType(_messageDesc->getMessageType())
                      << " from instance=" << Iid(_messageDesc->getSourceInstanceID()));

       std::shared_ptr<const scidb::InstanceLiveness> myLiveness =
           cluster->getInstanceLiveness();
       assert(myLiveness);
       _query = Query::createFakeQuery(INVALID_INSTANCE,
                                       cluster->getLocalInstanceId(),
                                       myLiveness);
    }
    assert(_query);
    if (_messageDesc->getMessageType() == mtChunkReplica) {
        _networkManager.registerMessage(_messageDesc, NetworkManager::mqtReplication);
    } else {
        _networkManager.registerMessage(_messageDesc, NetworkManager::mqtNone);
    }
}

ServerMessageHandleJob::~ServerMessageHandleJob()
{
    std::shared_ptr<MessageDesc> msgDesc;
    _messageDesc.swap(msgDesc);
    SCIDB_ASSERT(msgDesc);
    SCIDB_ASSERT(!_messageDesc);

    if (msgDesc->getMessageType() == mtChunkReplica) {
        _networkManager.unregisterMessage(msgDesc, NetworkManager::mqtReplication);
    } else {
        _networkManager.unregisterMessage(msgDesc, NetworkManager::mqtNone);
    }

    LOG4CXX_TRACE(logger, "Destroying a job for "
                  << strMsgType(msgDesc->getMessageType())
                  << " from instance=" << Iid(msgDesc->getSourceInstanceID())
                  << " with message size=" << msgDesc->getMessageSize()
                  << " for queryID=" << msgDesc->getQueryID()
                  << " for phyOpID=" << msgDesc->getPhysicalOperatorID());
}

// 分发消息
void ServerMessageHandleJob::dispatch(NetworkManager* nm)
{
    SCIDB_ASSERT(nm == &_networkManager);

    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());

    if (messageType == mtNone || !isScidbMessage(messageType)) {
        assert(false);
        throw (SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
               << messageType);
    }

    const QueryID queryID = _messageDesc->getQueryID();
    const OperatorID phyOpID = _messageDesc->getPhysicalOperatorID();
    const InstanceID physicalSourceId = _messageDesc->getSourceInstanceID();

    // dz from trace to debug
    LOG4CXX_DEBUG(logger, "ServerMessageHandleJob::"<<__func__
                  << " Message of type=" << strMsgType(messageType)
                  << ", for queryID=" << queryID
                  << ", for phyOpID=" << phyOpID
                  << ", from instanceID=" << Iid(physicalSourceId));

    // Set the initial message handler
    _currHandler = std::bind(getMsgHandler(messageType), this);

    switch (messageType)
    {
    case mtChunkReplica:
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);
        std::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
        ArrayID arrId = chunkRecord->array_id();
        LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: mtChunkReplica sourceId="
                      << _logicalSourceId
                      << ", arrId="<<arrId
                      << ", queryID="<<_query->getQueryID());
        if (arrId <= 0 || _logicalSourceId == _query->getInstanceID()) {
            assert(false);
            stringstream ss;
            ss << "Invalid ArrayID=0 from InstanceID=" << Iid(physicalSourceId)
               << " for QueryID="<<queryID;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << ss.str());
        }
        std::shared_ptr<ReplicationContext> replicationCtx = _query->getReplicationContext();

        // in debug only because ReplicationContext is single-threaded
        assert(replicationCtx->_chunkReplicasReqs[_logicalSourceId].increment() > 0);

        if (logger->isTraceEnabled()) {
            const uint64_t available = _networkManager.getAvailable(NetworkManager::mqtReplication, physicalSourceId);
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Replication queue available="<<available);
        }
        std::shared_ptr<Job> thisJob(shared_from_this());
        replicationCtx->enqueueInbound(arrId, thisJob);
        return;
    }
    break;
    case mtSyncRequest:
    {
        std::shared_ptr<WorkQueue> q = _query->getSGQueue();
        assert(q);
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                          << " for query ("<<queryID<<")");
        }
        enqueue(q);
        return;
    }
    break;
    case mtSyncResponse:
    {
        // mtSyncRequest/mtSyncResponse should follow the same path (dispatched to the same queues)
        // as mtFetch/mtRemoteChunk to make sure there are not chunks in-flight. By itself,
        // receiving mtSyncResponse does not guarantee that the remote instance will no longer
        // send another mtRemoteChunk because of some earlier mtFetch requesting prefetching.
        // To guarantee that no inbound mtRemoteChunks will be send, syncSG() needs to be preceded with
        // the arrival of the EOFs for all array attributes.
        std::shared_ptr<WorkQueue> q = _query->getBufferReceiveQueue();
        assert(q);
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator receive queue size="
                          << q->size()
                          << ", messageType=" << mtSyncResponse << " (mtSyncResponse)"
                          << " for query ("<<queryID<<")"
                          << " for operator("<<phyOpID<<")");
        }
        enqueue(q);
        return;
    }
    break;
    case mtBufferSend:
    case mtUpdateQueryResult:
    {
        std::shared_ptr<WorkQueue> q = _query->getBufferReceiveQueue();
        assert(q);
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: BufferSend queue size="<<q->size()
                          <<", messageType=" << strMsgType(messageType)
                          << " for query ("<<queryID<<")");
        }
        enqueue(q);
        return;
    }
    case mtRecoverChunk:
    case mtResourcesFileExistsRequest:
    case mtResourcesFileExistsResponse:
    {
        _mustValidateQuery = false;
    }
    break;
    case mtCommitResponse:
    case mtAbortResponse:
    {
        _mustValidateQuery = false;
        std::shared_ptr<WorkQueue> q = _query->getErrorQueue();
        enqueue(q);
        return;
    }
    break;
    case mtError:
    case mtAbortRequest:
    case mtCommitRequest:
    {
        _mustValidateQuery = false;
        std::shared_ptr<WorkQueue> q = _query->getErrorQueue();
        if (logger->isTraceEnabled() && q) {
            LOG4CXX_TRACE(logger, "Error queue size="<<q->size()
                          << " for query ("<<queryID<<")");
        }
        // We must not drop query-state-change messages to properly complete a query.
        // Therefore, in a unlikely event of the error queue being full, we will stall
        // the network thread until the queue drains. The queue is expected to drain (without a deadlock)
        // because mtError, mtAbortRequest, mtCommitRequest handlers do not require any
        // additional network communication.
        std::function<void()> work = std::bind(&MessageHandleJob::enqueue,
                                               this,
                                               q,
                                               /*handleOverflow:*/ false);
        Query::runRestartableWork<void, scidb::WorkQueue::OverflowException>(work);
        return;
    }
    break;
    case mtFetch:
    {
        LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" case mtFetch begin");
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);

        std::shared_ptr<scidb_msg::Fetch> record = _messageDesc->getRecord<scidb_msg::Fetch>();
        uint32_t objType = record->obj_type();
        switch(objType) {
        case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" sub case REMOTE_ARRAY_OBJ fallthrough");
            // fallthrough
        case PullSGArray::SG_ARRAY_OBJ_TYPE:
        {
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" sub case SG_ARRAY_OBJ");

            // This is only in debug-build because getSGQeuue() returns a single-threaded queue
            assert(_query->chunkReqs[_logicalSourceId].increment() > 0);

            // RemoteArray and PullSGArray use the per-operator context and the SGQueue
            std::shared_ptr<WorkQueue> q = _query->getSGQueue();
            assert(q);

            if (logger->isTraceEnabled()) {
                LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                              << " for query ("<<queryID<<")"
                              << " for operator("<<phyOpID<<")");
            }
            enqueue(q);
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" sub case SG_ARRAY_OBJ enqueued to OperatorQueue");
            return;
        }

        // RemoteMergedArray does NOT use operator context; so no need
        // to use the operator queue.
        case RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE:
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" sub case MERGED_ARRAY_OBJ");
            if (record->has_session_info() &&
                record->session_info().has_job_priority())
            {
                setPriority(record->session_info().job_priority());
            }
            // can potentially block
            enqueue(nm->getRequestQueue(getPriority()));
            LOG4CXX_TRACE(logger, "ServerMessageHandleJob::"<<__func__<<" sub case MERGED_ARRAY_OBJ enqueued to RequestQ");
            return;

        default:
            ASSERT_EXCEPTION(false,
                             "ServerMessageHandleJob::dispatch need to handle all cases that call mtFetch!");
        } // end switch
    }
    case mtPreparePhysicalPlan:
    {
        // The Query::_continuation binds the ClientMessageHandleJob instance together with
        // a functor to later execute when the coordinator receives the mtPreparePhysicalPlan message
        // (note:  the coordinator broadcasts this message to all instances and receives this message
        // from itself).  The ClientMessageHandleJob has state relevant to the query (like the
        // Connection object) that needs to be preserved across the state machine, so storing it on the
        // query allows us to recall it here and continue processing.
        //
        // All multiqueries have their own ClientMessageHandleJob instance which is preserved in
        // the continuation, but subqueries do not have their own ClientMessageHandleJob instance.
        // subqueries on the coordinator will pass-through this conditional without a continuation
        // set on the Query.
        if (_query->isCoordinator()) {
            if (_query->isSub()) {
                // The multiquery will pass through this point and will invoke the continuation
                // which executes the next subquery automatically.
                return;
            }
            ASSERT_EXCEPTION(physicalSourceId ==
                             Cluster::getInstance()->getLocalInstanceId(),
                             "Unexpected non-local source of physical plan on coordinator");
            Query::Continuation cont;
            _query->swapContinuation(cont);
            if (!cont) {
                std::shared_ptr<Exception> err = _query->getError();
                if (!err || SCIDB_E_NO_ERROR == err->getLongErrorCode()) {
                    SCIDB_ASSERT(false); //suicide ...
                    throw std::runtime_error("Unexpected empty continuation must occur only on a query error");
                }
            } else {
                cont(_query);
            }
            return;
        }
        std::shared_ptr<scidb_msg::PhysicalPlan> record = _messageDesc->getRecord<scidb_msg::PhysicalPlan>();

        // Handle session information.  Though "optional" in the protobufs definition, these fields
        // really are required at this point.
        ASSERT_EXCEPTION(record->has_session_info(), "Query plan is missing session info");
        ASSERT_EXCEPTION(record->session_info().has_job_priority(), "Query plan is missing job_priority");
        ASSERT_EXCEPTION(record->session_info().has_session_json(), "Query plan is missing session_json");
        ASSERT_EXCEPTION(!_query->getSession(), "Query already has attached session");
        setPriority(record->session_info().job_priority());
        _query->attachSession(Session::fromJson(record->session_info().session_json()));
        mst::setupQueryOnWorker(_query, record);

        // On the worker, ensure that per-instance worker locks still get cleaned-up,
        // because the coordinator cannot be responsible for those.
        SCIDB_ASSERT(!_query->isCoordinator());
        Query::Finalizer f = std::bind(&Query::releaseLocks, std::placeholders::_1);
        _query->pushVotingFinalizer(f);

        // can potentially block
        enqueue(nm->getRequestQueue(getPriority()));
        return;
    }
    break;
    case mtRemoteChunk: // reply to mtFetch
    {
        _logicalSourceId = _query->mapPhysicalToLogical(physicalSourceId);

        uint32_t objType = _messageDesc->getRecord<scidb_msg::Chunk>()->obj_type();
        switch(objType) {
        case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
        case PullSGArray::SG_ARRAY_OBJ_TYPE:
        {
            std::shared_ptr<WorkQueue> q = _query->getBufferReceiveQueue();
            assert(q);
            if (logger->isTraceEnabled()) {
                LOG4CXX_TRACE(logger, "ServerMessageHandleJob::dispatch: Operator queue size="<<q->size()
                              << ", messageType=" << mtRemoteChunk << " (mtRemoteChunk)"
                              << " for query ("<< queryID<<")"
                              << " for operator("<<phyOpID<<")");
            }
            enqueue(q);
            return;
        }
        // RemoteMergedArray does NOT use operator context; so no need to use the BufferReceiveQueue.
        case RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE:
            enqueue(nm->getWorkQueue());
            return;
        default:
            ASSERT_EXCEPTION(false, "ServerMessageHandleJob::dispatch need to handle all cases of mtRemoteChunk!");
        } // end switch
    }
    default:
    break;
    };

    enqueue(nm->getWorkQueue());
}

void ServerMessageHandleJob::run()
{
    static const char *funcName = "ServerMessageHandleJob::run: ";
    assert(_messageDesc);
    assert(isScidbMessage(_messageDesc->getMessageType()));

    // Inject the query arena into thread-local storage.
    arena::ScopedArenaTLS arenaTLS(_query->getArena());

    OnScopeExit fqd([this] () { Query::destroyFakeQuery(_query.get()); });

    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, funcName << "Starting message handling: " << strMsgType(messageType)
                  << ", queryID=" << _messageDesc->getQueryID());
    try
    {
        if (_mustValidateQuery) {
            Query::validateQueryPtr(_query);
        }

        // Execute the current handler
        ASSERT_EXCEPTION(_currHandler, "ServerMessageJob handler is not set");

        _currHandler();

        LOG4CXX_TRACE(logger, funcName << "Finishing message handling: " << strMsgType(messageType));
    }
    catch ( const Exception& e)
    {
        assert(_messageDesc);
        LOG4CXX_ERROR(logger, funcName << "Error occurred in message handler: "
                      << e.what()
                      << ", messageType = " << strMsgType(messageType)
                      << ", sourceInstance = " << Iid(_messageDesc->getSourceInstanceID())
                      << ", queryID="<<_messageDesc->getQueryID());
        assert(messageType != mtCancelQuery);

        if (!_query) {
            assert(false);
            LOG4CXX_DEBUG(logger, funcName << "Query " << _messageDesc->getQueryID() << " is already destructed");
        } else {

            if (messageType == mtPreparePhysicalPlan) {
                SCIDB_ASSERT(!_query->isCoordinator());
                LOG4CXX_DEBUG(logger, funcName << "Execution of query " << _messageDesc->getQueryID()
                              << " is aborted on worker");

                // Will this trigger the abort reporting to the coordinator?
                auto abortedOnDone = _query->done(e.clone());

                if (abortedOnDone) {
                    LOG4CXX_DEBUG(logger,
                                  "Abort complete on worker, replying to coordinator " <<
                                  _query->getQueryID());
                    auto abortResponse = std::make_shared<MessageDesc>(mtAbortResponse);
                    std::shared_ptr<scidb_msg::DummyQuery> record =
                        abortResponse->getRecord<scidb_msg::DummyQuery>();
                    const auto& queryID = _query->getQueryID();
                    abortResponse->setQueryID(queryID);
                    record->set_cluster_uuid(Cluster::getInstance()->getUuid());
                    const auto& coordinatorID = _messageDesc->getSourceInstanceID();
                    _networkManager.sendPhysical(coordinatorID, abortResponse);
                }
            } else {
                LOG4CXX_DEBUG(logger, funcName << "Handle error for query " << _messageDesc->getQueryID());
                _query->handleError(e.clone());
            }

            if (messageType != mtError && messageType != mtAbortRequest) {

                // Tell both coordinator and remote message sender about the error.

                std::shared_ptr<MessageDesc> errorMessage =
                    makeErrorMessageFromException(e, _messageDesc->getQueryID());

                InstanceID const physicalCoordinatorID = _query->getPhysicalCoordinatorID();
                if (! _query->isCoordinator()) {
                    // Worker always tells the coordinator.
                    _networkManager.sendPhysical(physicalCoordinatorID, errorMessage);
                }
                if (physicalCoordinatorID != _messageDesc->getSourceInstanceID() &&
                    _query->getInstanceID() != _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()))
                {
                    // Message I was handling came from neither the coordinator nor the local
                    // instance.  Tell its sender about the error.  (If the local instance *is* the
                    // coordinator, it already knows due to the Query::handleError() call above.)
                    _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), errorMessage);
                }
            }
        }
    }
}

ServerMessageHandleJob::MsgHandler ServerMessageHandleJob::getMsgHandler(MessageType msgType)
{
    static vector<MsgHandler> handlers;
    static Mutex theLock;

    if (handlers.empty()) {
        ScopedMutexLock take(theLock, PTW_SML_SERVER_MESSAGE_HANDLE_JOB);
        if (handlers.empty()) {
            handlers.resize(size_t(mtSystemMax),
                            &ServerMessageHandleJob::throwOnInvalidMessage);

#           define HANDLE(_x)  handlers[mt ## _x] = &ServerMessageHandleJob::handle ## _x
            HANDLE(PreparePhysicalPlan);
            HANDLE(Fetch);
            HANDLE(ChunkReplica);
            HANDLE(ReplicaSyncResponse);
            HANDLE(QueryResult);
            HANDLE(Error);
            HANDLE(SyncRequest);
            HANDLE(SyncResponse);
            HANDLE(RemoteChunk);
            HANDLE(Notify);
            HANDLE(Wait);
            HANDLE(Barrier);
            HANDLE(BufferSend);
            HANDLE(AbortRequest);
            HANDLE(CommitRequest);
            HANDLE(UpdateQueryResult);
            HANDLE(CommitResponse);
            HANDLE(AbortResponse);
#           undef HANDLE

            // These bad boys overload the same handler.
            // XXX Maybe they don't need to, eh?
            handlers[mtResourcesFileExistsRequest] = &ServerMessageHandleJob::handleResourcesFileExists;
            handlers[mtResourcesFileExistsResponse] = &ServerMessageHandleJob::handleResourcesFileExists;
        }
    }

    SCIDB_ASSERT(msgType < mtSystemMax);
    return handlers[msgType];
}

void ServerMessageHandleJob::throwOnInvalidMessage()
{
    static const char *funcName = "ServerMessageHandleJob::throwOnInvalidMessage: ";
    const MessageType messageType = static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_ERROR(logger,  funcName << "Unknown/unexpected message type " << messageType);
    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
}

void ServerMessageHandleJob::handlePreparePhysicalPlan()
{
    static const char *funcName = "ServerMessageHandleJob::handlePreparePhysicalPlan: ";
    ScopedActiveQueryThread saqt(_query);
    PerfTimeScope pts(PTS_SMHJ_HPPP);

    std::shared_ptr<scidb_msg::PhysicalPlan> ppMsg = _messageDesc->getRecord<scidb_msg::PhysicalPlan>();

    const string clusterUuid = ppMsg->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "unknown cluster UUID=" << clusterUuid);

    const string physicalPlan = ppMsg->physical_plan();

    LOG4CXX_DEBUG(logger,  funcName << "Preparing physical plan: "
                  << " queryID=" << _messageDesc->getQueryID()
                  << " phyOpID=" << _messageDesc->getPhysicalOperatorID()
                  << " physicalPlan='" << physicalPlan << "'");

    const bool isDeadlockPossible = false;  //XXX TODO: enable this option in config.ini
    if (isDeadlockPossible) {
        // tell the coordinator we are running
        const QueryID queryID = _query->getQueryID();
        LOG4CXX_DEBUG(logger, "Send message to waiting coordinator in queryID: " << queryID);
        std::shared_ptr<MessageDesc> messageDesc = makeWaitMessage(queryID);
        _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), messageDesc);
    }

    std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

    queryProcessor->parsePhysical(physicalPlan, _query);
    LOG4CXX_DEBUG(logger,  funcName << "Physical plan was parsed")

    handleExecutePhysicalPlan();
}

void ServerMessageHandleJob::handleExecutePhysicalPlan()
{
   // NOTE: called by handlePreparePhysicalPlan, there is no message that drives this
   //       perhaps this should be prefixed with an underscore and made private
   static const char *funcName = "ServerMessageHandleJob::handleExecutePhysicalPlan: ";

   try {
      if (_query->isCoordinator()) {
          throwOnInvalidMessage();
      }

      LOG4CXX_DEBUG(logger, funcName << "Running physical plan: queryID=" << _messageDesc->getQueryID()
                                     << " phyOpID=" << _messageDesc->getQueryID())

      std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

      _query->start();

      try {
         queryProcessor->execute(_query);
         LOG4CXX_DEBUG(logger,  funcName << "Query was executed");
      } catch (const std::bad_alloc& e) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
      }
      _query->done();

      // Conclude the voting phase by sending the worker result back to the coordinator.
      std::shared_ptr<MessageDesc> resultMessage = std::make_shared<MessageDesc>(mtQueryResult);
      resultMessage->setQueryID(_query->getQueryID());

      _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), resultMessage);
      LOG4CXX_DEBUG(logger,
                    funcName << ": Result was sent to instance " <<
                    Iid(_messageDesc->getSourceInstanceID()));
   }
   catch (const scidb::Exception& e)
   {
       if (e.getShortErrorCode() == SCIDB_SE_QPROC
           && e.getLongErrorCode() == SCIDB_LE_QUERY_CANCELLED) {
           LOG4CXX_INFO(logger, "Query " << _query->getQueryID()
                        << " was cancelled while executing physical plan");
       }
       else {
           LOG4CXX_ERROR(logger,  funcName << "QueryID = " << _query->getQueryID()
                         << " encountered the error: "
                         << e.what());
       }

       e.raise();
   }
}

void ServerMessageHandleJob::handleQueryResult()
{
    static const char *funcName = "ServerMessageHandleJob::handleQueryResult: ";
    ScopedActiveQueryThread saqt(_query);
    PerfTimeScope pts(PTS_SMHJ_HQR);

    if  (!_query->isCoordinator()) {
        throwOnInvalidMessage();
    }

    const string arrayName = _messageDesc->getRecord<scidb_msg::QueryResult>()->array_name();

    LOG4CXX_DEBUG(logger,  funcName << "Received query result from instance "
                  << Iid(_messageDesc->getSourceInstanceID())
                  << ", queryID=" << _messageDesc->getQueryID()
                  << ", arrayName=" << arrayName);

    // Signaling to query context to defreeze
    _query->semResults.release();
}

void ServerMessageHandleJob::sgSync()
{
    // in debug only because this executes on a single-threaded queue
    assert(_logicalSourceId!=INVALID_INSTANCE);
    assert(_logicalSourceId<_query->chunkReqs.size());
    assert(!_query->chunkReqs[_logicalSourceId].decrement());
}

void ServerMessageHandleJob::handleReplicaSyncResponse()
{
    static const char *funcName = "ServerMessageHandleJob::handleReplicaSyncResponse: ";
    std::shared_ptr<ReplicationContext> replicationCtx(_query->getReplicationContext());
    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    std::shared_ptr<scidb_msg::DummyQuery> responseRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    ArrayID arrId = responseRecord->payload_id();
    if (arrId <= 0 || _logicalSourceId == _query->getInstanceID()) {
        assert(false);
        stringstream ss;
        ss << "Invalid ArrayID=0 from InstanceID=" << Iid(_messageDesc->getSourceInstanceID())
           <<" for QueryID="<<_query->getQueryID();
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
               << ss.str());
    }
    LOG4CXX_TRACE(logger, funcName << "arrId="<<arrId
                  << ", sourceId="<<_logicalSourceId
                  << ", queryID="<<_query->getQueryID());
    replicationCtx->replicationAck(_logicalSourceId, arrId);
}

void ServerMessageHandleJob::handleChunkReplica()
{
    static const char *funcName = "ServerMessageHandleJob::handleReplicaChunk: ";
    assert(static_cast<MessageType>(_messageDesc->getMessageType()) == mtChunkReplica);
    assert(_logicalSourceId != INVALID_INSTANCE);
    assert(_query);

    std::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    ArrayID arrId = chunkRecord->array_id();
    assert(arrId>0);

    LOG4CXX_TRACE(logger, funcName << "arrId="<<arrId
                  << ", sourceId="<<_logicalSourceId << ", queryID="<<_query->getQueryID());

    std::shared_ptr<ReplicationContext> replicationCtx(_query->getReplicationContext());

    assert(_logicalSourceId<replicationCtx->_chunkReplicasReqs.size());
    // in debug only because this executes on a single-threaded queue
    assert(!replicationCtx->_chunkReplicasReqs[_logicalSourceId].decrement());

    if (chunkRecord->eof()) {
        // last replication message for this arrId from _logicalSourceId
        assert(replicationCtx->_chunkReplicasReqs[_logicalSourceId].test());
        // when all eofs are received the work queue for this arrId can be removed

        _query->validate(); // to make sure no previous errors in replication

        LOG4CXX_TRACE(logger, "handleReplicaChunk: received eof");

        // ack the eof message back to _logicalSourceId
        std::shared_ptr<MessageDesc> responseMsg = std::make_shared<MessageDesc>(mtReplicaSyncResponse);
        std::shared_ptr<scidb_msg::DummyQuery> responseRecord = responseMsg->getRecord<scidb_msg::DummyQuery>();
        responseRecord->set_payload_id(arrId);
        responseMsg->setQueryID(_query->getQueryID());

        _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), responseMsg);
        return;
    }

    const CompressorType  compMethod = static_cast<CompressorType>(chunkRecord->compression_method());
    const size_t decompressedSize = chunkRecord->decompressed_size();
    const AttributeID attributeID = chunkRecord->attribute_id();
    const size_t count = chunkRecord->count();
    Coordinates coordinates;
    for (int i = 0; i < chunkRecord->coordinates_size(); i++) {
        coordinates.push_back(chunkRecord->coordinates(i));
    }

    std::shared_ptr<Array> dbArr = replicationCtx->getPersistentArray(arrId);
    assert(dbArr);

    if(chunkRecord->tombstone())
    { // tombstone record
        dbArr->removeLocalChunk(_query, coordinates);
    }
    else if (decompressedSize <= 0)
    { // what used to be clone of replica
        assert(false);
        stringstream ss;
        ss << "Invalid chunk decompressedSize=" << decompressedSize
           << " from InstanceID=" << Iid(_messageDesc->getSourceInstanceID())
           << " for QueryID="<<_query->getQueryID();
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str());
    }
    else
    { // regular chunk
        const auto& dbArrAttrs = dbArr->getArrayDesc().getAttributes();
        const auto& attr = dbArrAttrs.find(attributeID);
        SCIDB_ASSERT(attr != dbArrAttrs.end());
        std::shared_ptr<ArrayIterator> outputIter = dbArr->getIterator(*attr);
        std::shared_ptr<CompressedBuffer> compressedBuffer =
            dynamic_pointer_cast<CompressedBuffer>(_messageDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        Chunk& outChunk = outputIter->newChunk(coordinates);
        try
        {
            outChunk.decompress(*compressedBuffer);
            outChunk.setCount(count);
            outChunk.write(_query);
        }
        catch (const scidb::Exception& e)
        {
            outputIter->deleteChunk(outChunk);
            e.raise();
        }
    }
}

void ServerMessageHandleJob::handleRemoteChunk()
{
    static const char *funcName = "ServerMessageHandleJob::handleRemoteChunk: ";

    std::shared_ptr<scidb_msg::Chunk> chunkRecord = _messageDesc->getRecord<scidb_msg::Chunk>();
    const uint32_t objType = chunkRecord->obj_type();
    const AttributeID attId = chunkRecord->attribute_id();
    assert(_query);

    // Must have been set in dispatch().
    assert(_logicalSourceId == _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()));

    //
    // SPECIAL CASE: RemoteMergedArray, which does not use PhysicalOperatorContext
    //               returns early before PhysicalOperatorID is validated, etc.
    //
    if (objType == RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE) {
        std::shared_ptr<RemoteMergedArray> rma = _query->getMergedArray();
        validateRemoteChunkInfo(rma.get(), _messageDesc->getMessageType(), objType,
                                attId, _messageDesc->getSourceInstanceID());
        rma->handleChunkMsg(_messageDesc);
        return;
    }

    //
    // lookup PhysicalOperatorContext up and validate it
    //
    const OperatorID msgOpID = _messageDesc->getPhysicalOperatorID();
    auto phyOp = _query->getPhysicalOperatorByID(msgOpID);
    SCIDB_ASSERT(phyOp);

    auto phyOpCtx = phyOp->getOperatorContext();
    if(!phyOpCtx) {
        // TODO: these can be removed once a majority of operators
        //       are pullSG-compatible
        LOG4CXX_TRACE(logger, funcName <<__func__<<" bad phyOpCtx --------");
        std::string objTypeStr = (objType==RemoteArray::REMOTE_ARRAY_OBJ_TYPE) ? "REMOTE_ARRAY_OBJ_TYPE" :
                                 ((objType==RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE) ? "MERGED_ARRAY_OBJ_TYPE" :
                                  ((objType==PullSGArray::SG_ARRAY_OBJ_TYPE) ?    "SG_ARRAY_OBJ_TYPE" :
                                   "unknown"));
        LOG4CXX_TRACE(logger, funcName << " objType " << objTypeStr
                                       << " phyOp " << static_cast<void*>(phyOp.get())
                                       << " getPhysicalName() " << phyOp->getPhysicalName()
                                       << " getOperatorID() " << phyOp->getOperatorID()
                                       << " opContext " << static_cast<void*>(phyOpCtx.get()));
    }
    SCIDB_ASSERT(phyOpCtx);

    switch(objType)
    {
    case RemoteArray::REMOTE_ARRAY_OBJ_TYPE:
    {
        std::shared_ptr<RemoteArrayContext> context = RemoteArray::getContext(phyOpCtx);
        std::shared_ptr<RemoteArray> ra = context->getInboundArray(_logicalSourceId);
        validateRemoteChunkInfo(ra.get(), _messageDesc->getMessageType(), objType,
                            attId, _messageDesc->getSourceInstanceID());
        ra->handleChunkMsg(_messageDesc);
    }
    break;
    case PullSGArray::SG_ARRAY_OBJ_TYPE:
    {
        // phyOpContext must be a PullSGContext for this message type
        auto sgCtx = dynamic_pointer_cast<PullSGContext>(phyOpCtx);
        if (sgCtx == NULL) {
            // throw an exception containing its typeid for debug purposes
            std::shared_ptr<OperatorContext> ctx = phyOp->getOperatorContext();
            string txt = phyOpCtx ? typeid(*phyOpCtx).name() : "NULL" ;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
                   << txt);
        }
        std::shared_ptr<PullSGArray> arr = sgCtx->getResultArray();
        validateRemoteChunkInfo(arr.get(), _messageDesc->getMessageType(), objType,
                                attId, _messageDesc->getSourceInstanceID());
        arr->handleChunkMsg(_messageDesc,_logicalSourceId);
    }
    break;
    default:
    {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<< strMsgType(_messageDesc->getMessageType())
           << " attributeID="<<attId
           << " array type="<<objType
           << " from InstanceID=" << Iid(_messageDesc->getSourceInstanceID())
           << " for queryID="<<_query->getQueryID();
        ASSERT_EXCEPTION(false, ss.str());
    }

    }
}

// TODO: consider rename to handleFetchChunk()
// TODO: add comment about what uses this type of message handling
// TODO:   and the ..._OBJ_TYPE distinctions
void ServerMessageHandleJob::handleFetch()
{
    static const char *funcName = "ServerMessageHandleJob::handleFetch: ";
    std::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();
    const QueryID queryID = _messageDesc->getQueryID();
    const OperatorID msgOpID = _messageDesc->getPhysicalOperatorID();
    const uint32_t attributeId = fetchRecord->attribute_id();
    const bool positionOnly = fetchRecord->position_only();
    const uint32_t objType = fetchRecord->obj_type();

    LOG4CXX_TRACE(logger, funcName << " attributeID=" << attributeId
                                   << " for queryID=" << queryID
                                   << " for msgOpID=" << msgOpID
                                   << " from instanceID="<< Iid(_messageDesc->getSourceInstanceID()));

    SCIDB_ASSERT(queryID.isValid());
    SCIDB_ASSERT(queryID == _query->getQueryID());

    if (objType>PullSGArray::SG_ARRAY_OBJ_TYPE) {
        stringstream ss;
        ss << "Malformed remote message: "
           << " messageID="<< strMsgType(_messageDesc->getMessageType())
           << " attributeID="<<attributeId
           << " invalid array type="<<objType
           << " from InstanceID="<< Iid(_messageDesc->getSourceInstanceID())
           << " for queryID="<<queryID
           << " for msgOpID="<<msgOpID;
        ASSERT_EXCEPTION(false, ss.str());
    }

    if (objType==PullSGArray::SG_ARRAY_OBJ_TYPE) {
        LOG4CXX_TRACE(logger, funcName << " objType SG_ARRAY, delegating to handleSGFetchChunk");
        handleSGFetchChunk();
        sgSync();
        return;
    }

    LOG4CXX_TRACE(logger, funcName << " RemoteArray or RemoteMergedArray ");
    // At this point, the puller is either a RemoteArray or a RemoteMergedArray.
    //   - RemoteArray uses Query::_outboundArrays, and allows any instance to pull from any instance.
    //   - RemoteMergedArray uses Query::_currentResultArray, and *only* allows the coordinator pull from a worker instance.
    //
    SCIDB_ASSERT(objType == RemoteArray::REMOTE_ARRAY_OBJ_TYPE ||
                 objType == RemoteMergedArray::MERGED_ARRAY_OBJ_TYPE);

    std::shared_ptr<Array> resultArray;

    if (objType == RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
        SCIDB_ASSERT(queryID==_query->getQueryID());
        ScopedActiveQueryThread saqt(_query);
        PerfTimeScope pts(PTS_SMHJ_FET_CHUNK);

        LOG4CXX_TRACE(logger, funcName << " objType REMOTE_ARRAY");

        const OperatorID phyOpID = _messageDesc->getPhysicalOperatorID();
        auto phyOp = _query->getPhysicalOperatorByID(phyOpID);
        SCIDB_ASSERT(phyOp);
        LOG4CXX_TRACE(logger, funcName << " phyOp " << static_cast<void*>(phyOp.get())
                                       << " phyOp->getOperatorID() " << phyOp->getOperatorID()
                                       << " phyOp->getPhysicalName() " << phyOp->getPhysicalName() );

        auto phyOpContext = phyOp->getOperatorContext();
        LOG4CXX_TRACE(logger, funcName << " phyOpContext: " << static_cast<void*>(phyOpContext.get()));
        SCIDB_ASSERT(phyOpContext);

        assert(_logicalSourceId == _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID()));
        resultArray = RemoteArray::getContext(phyOpContext)->getOutboundArray(_logicalSourceId);

        auto msgOp = _query->getPhysicalOperatorByID(msgOpID);
        SCIDB_ASSERT(msgOp);

    } else {
        SCIDB_ASSERT(queryID==_query->getQueryID());
        ScopedActiveQueryThread saqt(_query);
        PerfTimeScope pts(PTS_SMHJ_FET_MCHUNK);

        LOG4CXX_TRACE(logger, funcName << " objType MERGED_ARRAY");
        // NOTE: phyOp from input is invalid for merged arrays

        if  (_query->isCoordinator()) {
            // this message type never FROM coordinator
            throwOnInvalidMessage();
            return;
        }
        // ok, get our portion of the result for sending to the coordinator
        resultArray = _query->getCurrentResultArray();
    }

    SCIDB_ASSERT(queryID==_query->getQueryID());
    ScopedActiveQueryThread saqt(_query);
    PerfTimeScope pts(PTS_SMHJ_FET_CHUNK_COMMON);

    validateRemoteChunkInfo(resultArray.get(), _messageDesc->getMessageType(), objType,
                            attributeId, _messageDesc->getSourceInstanceID());

    const auto& resultAttrs = resultArray->getArrayDesc().getAttributes();
    const auto& attr = resultAttrs.find(attributeId);
    SCIDB_ASSERT(attr != resultAttrs.end());
    std::shared_ptr<ConstArrayIterator> iter = resultArray->getConstIterator(*attr);

    std::shared_ptr<MessageDesc> chunkMsg;

    if (!iter->end())
    {
        LOG4CXX_TRACE(logger, funcName << " ! iter->end()");
        std::shared_ptr<scidb_msg::Chunk> chunkRecord;
        if (!positionOnly) {
            LOG4CXX_TRACE(logger, funcName << " ! positionOnly");
            const ConstChunk* chunk = &iter->getChunk();
            std::shared_ptr<CompressedBuffer> buffer = std::make_shared<CompressedBuffer>();
            std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
            if (resultArray->getArrayDesc().getEmptyBitmapAttribute() != NULL &&
                !chunk->getAttributeDesc().isEmptyIndicator()) {
                LOG4CXX_TRACE(logger, funcName << "calling chunk->getEmptyBitmap()");
                emptyBitmap = chunk->getEmptyBitmap();
                LOG4CXX_TRACE(logger, funcName << "returned from chunk->getEmptyBitmap()");
            }

            // isSameServer: an optimization, no compression between instances on the same server
            auto const destInstanceId = _messageDesc->getSourceInstanceID();
            ASSERT_EXCEPTION(isValidPhysicalInstance(destInstanceId), "getChunkMesg: destInstanceId is not physical");
            auto const thisInstanceId = _query->getInstanceID();
            ASSERT_EXCEPTION(isValidPhysicalInstance(thisInstanceId), "getChunkMesg: thisInstanceId  is not physical");
            const bool isSameServer = getServerId(destInstanceId) == getServerId(thisInstanceId);

            chunk->compress(*buffer, emptyBitmap, isSameServer);
            emptyBitmap.reset(); // the bitmask must be cleared before the iterator is advanced (bug?)
            chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk, buffer);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
            chunkRecord->set_compression_method(static_cast<int32_t>(buffer->getCompressionMethod()));
            chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
            chunkRecord->set_count(chunk->isCountKnown() ? chunk->count() : 0);
            const Coordinates& coordinates = chunk->getFirstPosition(false);
            for (size_t i = 0; i < coordinates.size(); i++) {
                chunkRecord->add_coordinates(coordinates[i]);
            }
            LOG4CXX_TRACE(logger, funcName << " chunk message prepared, coords: "
                          << CoordsToStr(coordinates) << " count: " << chunk->count()
                          << " size:" << buffer->getDecompressedSize());
             ++(*iter);

        } else {
            LOG4CXX_TRACE(logger, funcName << " positionOnly ");
            chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk);
            chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        }
        LOG4CXX_TRACE(logger, funcName << " chunkRecord updated");

        chunkMsg->setQueryID(queryID);

        // REMOTE_ARRAY messages requre a valid opID
        if (objType==RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
            SCIDB_ASSERT(msgOpID.isValid());
            chunkMsg->setPhysicalOperatorID(msgOpID);
        }

        chunkRecord->set_eof(false);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        if (!iter->end() || positionOnly)
        {
            LOG4CXX_TRACE(logger, funcName << "has next is true");
            chunkRecord->set_has_next(true);
            const Coordinates& next_coordinates = iter->getPosition();
            for (size_t i = 0; i < next_coordinates.size(); i++) {
                chunkRecord->add_next_coordinates(next_coordinates[i]);
            }
        }
        else
        {
            LOG4CXX_TRACE(logger, funcName << "has next is false");
            chunkRecord->set_has_next(false);
        }

        std::shared_ptr<Query> query = Query::getQueryByID(queryID);
        SCIDB_ASSERT(query.get() == _query.get());

        if (query->getWarnings().size())
        {
            //Propagate warnings gathered on coordinator to client
            vector<Warning> v = query->getWarnings();
            for (vector<Warning>::const_iterator it = v.begin(); it != v.end(); ++it)
            {
                ::scidb_msg::Chunk_Warning* warn = chunkRecord->add_warnings();
                warn->set_code(it->getCode());
                warn->set_file(it->getFile());
                warn->set_function(it->getFunction());
                warn->set_line(it->getLine());
                warn->set_what_str(it->msg());
                warn->set_strings_namespace(it->getStringsNamespace());
                warn->set_stringified_code(it->getStringifiedCode());
            }
            query->clearWarnings();
        }

        LOG4CXX_TRACE(logger, funcName << "Prepared message with chunk data");
    }
    else // iter-end
    {
        LOG4CXX_TRACE(logger, funcName << " iter->end()");
        chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk);
        std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
        chunkMsg->setQueryID(queryID);
        // REMOTE_ARRAY messages requre a valid opID
        if (objType==RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
            SCIDB_ASSERT(msgOpID.isValid());
            chunkMsg->setPhysicalOperatorID(msgOpID);
        }
        chunkRecord->set_eof(true);
        chunkRecord->set_obj_type(objType);
        chunkRecord->set_attribute_id(attributeId);
        LOG4CXX_TRACE(logger, funcName << "Prepared message with information "
                      << "that there are no unread chunks");
    }

    _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), chunkMsg);

    if (objType==RemoteArray::REMOTE_ARRAY_OBJ_TYPE) {
        sgSync();
        return;
    }

    LOG4CXX_TRACE(logger, funcName << "Remote chunk was sent to client");
}

void ServerMessageHandleJob::handleSGFetchChunk(bool inBackground)
{
    static const char *funcName = "ServerMessageHandleJob::handleSGFetchChunk: ";
    LOG4CXX_TRACE(logger, funcName);

    std::shared_ptr<scidb_msg::Fetch> fetchRecord = _messageDesc->getRecord<scidb_msg::Fetch>();
    const QueryID queryID = _messageDesc->getQueryID();

    SCIDB_ASSERT(queryID==_query->getQueryID());
    ScopedActiveQueryThread saqt(_query);
    PerfTimeScope pts(PTS_SMHJ_FET_SGCHUNK);

    ASSERT_EXCEPTION((fetchRecord->has_attribute_id()), funcName);
    const uint32_t attributeId = fetchRecord->attribute_id();
    ASSERT_EXCEPTION((fetchRecord->has_obj_type()), funcName);
    const uint32_t objType = fetchRecord->obj_type();
    ASSERT_EXCEPTION((objType == PullSGArray::SG_ARRAY_OBJ_TYPE), funcName);
    const uint64_t fetchId = fetchRecord->fetch_id();
    ASSERT_EXCEPTION((fetchId>0 && fetchId<uint64_t(~0)), funcName);

    const OperatorID phyOpID = _messageDesc->getPhysicalOperatorID();
    SCIDB_ASSERT(phyOpID.isValid());
    auto phyOp = _query->getPhysicalOperatorByID(phyOpID);
    SCIDB_ASSERT(phyOp);
    LOG4CXX_TRACE(logger, funcName << " phyOpID " << phyOpID
                                   << " phyOp " << static_cast<void*>(phyOp.get())
                                   << " phyOp->getOperatorID() " << phyOp->getOperatorID()
                                   << " phyOp->getPhysicalName() " << phyOp->getPhysicalName());

    auto phyOpContext = phyOp->getOperatorContext();
    SCIDB_ASSERT(phyOpContext);
    LOG4CXX_TRACE(logger, funcName << " phyOpContext: " << static_cast<void*>(phyOpContext.get()));

    if (inBackground) {
        LOG4CXX_TRACE(logger, funcName << "Fetching remote chunk attributeID=" << attributeId
                      << " for queryID=" << queryID
                      << " for phyOpID=" << phyOpID
                      << " in background");

    } else {
        LOG4CXX_TRACE(logger, funcName << "Fetching remote chunk attributeID=" << attributeId
                      << " for queryID=" << queryID
                      << " for phyOpID=" << phyOpID
                      << " fetchID=" << fetchId
                      << " from instanceID=" << Iid(_messageDesc->getSourceInstanceID()));
    }

    std::shared_ptr<PullSGContext> sgCtx = dynamic_pointer_cast<PullSGContext>(phyOpContext);
    if (sgCtx == NULL) {
        std::shared_ptr<OperatorContext> ctx = phyOpContext;
        string txt = ctx ? typeid(*ctx).name() : "NULL" ;
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << txt);
    }

    PullSGContext::ChunksWithDestinations chunksToSend;
    if (inBackground) {
        LOG4CXX_TRACE(logger, funcName << "calling produceChunksInBackground");
        sgCtx->produceChunksInBackground(_query, attributeId, chunksToSend);
    } else {
        LOG4CXX_TRACE(logger, funcName << "calling sgCtx->getNextChunks()");

        ASSERT_EXCEPTION((fetchRecord->has_position_only()), funcName);
        const bool positionOnlyOK = fetchRecord->position_only();
        ASSERT_EXCEPTION((fetchRecord->has_prefetch_size()), funcName);
        const uint32_t prefetchSize = fetchRecord->prefetch_size();

        _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
        sgCtx->getNextChunks(_query, _logicalSourceId, attributeId,
                             positionOnlyOK, prefetchSize, fetchId,
                             chunksToSend);
    }

    for (PullSGContext::ChunksWithDestinations::iterator iter = chunksToSend.begin();
         iter != chunksToSend.end(); ++iter) {

        const InstanceID instance = iter->first;
        std::shared_ptr<MessageDesc>& chunkMsg = iter->second;

        LOG4CXX_TRACE(logger, funcName << "Forwarding chunk attributeID=" << attributeId
                      << " for queryID=" << queryID
                      << " for phyOpID=" << phyOpID
                      << " to (logical) instanceID=" << Iid(instance));

        LOG4CXX_TRACE(logger, funcName << "chunk original OpID" << chunkMsg->getPhysicalOperatorID().getValue());
        chunkMsg->setPhysicalOperatorID(phyOpID);
        LOG4CXX_TRACE(logger, funcName << "chunk updated OpID" << chunkMsg->getPhysicalOperatorID().getValue());
        SCIDB_ASSERT(chunkMsg->getPhysicalOperatorID() == phyOpID);  // better be, or it won't make sense on the other end?

        if (instance == _query->getInstanceID() ) { // 只有逻辑id一致，才会使用这个 send local，也就是同一个instance才会使用send local，其他包括本机其他的也不行
            _networkManager.sendLocal(_query, chunkMsg);
        } else {
            // remote
            _networkManager.sendPhysical(_query->mapLogicalToPhysical(instance), chunkMsg);
        }
    }

    LOG4CXX_TRACE(logger, funcName << chunksToSend.size() << " chunks sent");

    if (sgCtx->setupToRunInBackground(attributeId)) {
        Handler h = std::bind(&ServerMessageHandleJob::handleSGFetchChunk,
                              this,
                              true);
        _currHandler.swap(h);
        SCIDB_ASSERT(_currHandler);
        reschedule();
    }
}

void ServerMessageHandleJob::handleSyncRequest()
{
    static const char *funcName = "ServerMessageHandleJob::handleSyncRequest: ";

    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    assert(_logicalSourceId!=INVALID_INSTANCE);

    assert(_logicalSourceId<_query->chunkReqs.size());

    // in debug only because this executes on a single-threaded queue
    assert(_query->chunkReqs[_logicalSourceId].test());

    std::shared_ptr<MessageDesc> syncMsg = std::make_shared<MessageDesc>(mtSyncResponse);
    syncMsg->setQueryID(_messageDesc->getQueryID());

    if (_logicalSourceId == _query->getInstanceID()) {
        _networkManager.sendLocal(_query, syncMsg);
    } else {
        _networkManager.sendPhysical(_messageDesc->getSourceInstanceID(), syncMsg);
    }
    LOG4CXX_TRACE(logger, funcName
                  << "Sync confirmation was sent to instance "
                  << Iid(_messageDesc->getSourceInstanceID()));

}

void ServerMessageHandleJob::handleBarrier()
{
    static const char *funcName = "ServerMessageHandleJob::handleBarrier: ";
    std::shared_ptr<scidb_msg::DummyQuery> barrierRecord =
        _messageDesc->getRecord<scidb_msg::DummyQuery>();
    uint64_t payloadId = barrierRecord->payload_id();

    LOG4CXX_TRACE(logger, funcName << "handling barrier message in query "
                  << _messageDesc->getQueryID()
                  << " for payload " << payloadId);

    assert(payloadId < MAX_BARRIERS);
    _query->semSG[payloadId].release();
}

void ServerMessageHandleJob::handleSyncResponse()
{
    static const char *funcName = "ServerMessageHandleJob::handleSyncResponse: ";

    LOG4CXX_TRACE(logger, funcName << "Receiving confirmation for sync message and release syncSG in query"
                  << _messageDesc->getQueryID());

    // Signaling to query to release SG semaphore inside physical operator and continue to work
    // This can run on any queue because the state of SG (or pulling SG) should be such that it is
    // expecting only this single message (so no other messages need to be ordered wrt to this one).
    _query->syncSG.release();
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleError()
{
    static const char *funcName = "ServerMessageHandleJob::handleError: ";
    std::shared_ptr<scidb_msg::Error> errorRecord = _messageDesc->getRecord<scidb_msg::Error>();

    const string clusterUuid = errorRecord->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "unknown cluster UUID=" << clusterUuid);

    const string errorText = errorRecord->what_str();
    const int32_t errorCode = errorRecord->long_error_code();
    const auto shortErrorCode = errorRecord->short_error_code();

    if (shortErrorCode == SCIDB_SE_QPROC
        && errorCode == SCIDB_LE_QUERY_CANCELLED) {
        LOG4CXX_INFO(logger, "Instance " << Iid(_messageDesc->getSourceInstanceID()) <<
                     " reports that query " << _messageDesc->getQueryID() <<
                     " was cancelled.");
    }
    else {
        LOG4CXX_ERROR(logger, funcName
                      << " Error on processing query " << _messageDesc->getQueryID()
                      << " on instance " << Iid(_messageDesc->getSourceInstanceID())
                      << ". Query coordinator ID: " << Iid(_query->getPhysicalCoordinatorID())
                      << ". Message short error code: " << shortErrorCode
                      << ". Message long error code: " << errorCode
                      << ". Message txt: " << errorText);
    }

    assert(_query->getQueryID() == _messageDesc->getQueryID());

    std::shared_ptr<Exception> e = makeExceptionFromErrorMessage(_messageDesc);
    bool isAbort = false;
    if (errorCode == SCIDB_LE_QUERY_NOT_FOUND || errorCode == SCIDB_LE_QUERY_NOT_FOUND2)
    {
        if (_query->getPhysicalCoordinatorID() == _messageDesc->getSourceInstanceID()) {
            // The coordinator does not know about this query, we will also abort the query
            isAbort = true;
        }
        else
        {
            // A remote instance did not find the query, it must be out of sync (because of restart?).
            e = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_NO_QUORUM)
                << "Remote instance is unaware of query id (perhaps due to restart)";
        }
    }
    assert(arena::Arena::getArenaTLS() == _query->getArena());
    if (isAbort) {
        _query->handleAbort();
    } else {
        _query->handleError(e);
    }
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleAbortRequest()
{
    static const char *funcName = "ServerMessageHandleJob::handleAbortRequest: ";

    std::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "Unknown cluster UUID=" << clusterUuid);

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceInstanceID()
        || _query->isCoordinator()) {
        throwOnInvalidMessage();
    }

    if (_query->handleAbort()) {
        LOG4CXX_DEBUG(logger,
                      "Abort complete on worker, replying to coordinator " <<
                      _query->getQueryID());
        auto abortResponse = std::make_shared<MessageDesc>(mtAbortResponse);
        std::shared_ptr<scidb_msg::DummyQuery> record =
            abortResponse->getRecord<scidb_msg::DummyQuery>();
        const auto& queryID = _query->getQueryID();
        abortResponse->setQueryID(queryID);
        record->set_cluster_uuid(Cluster::getInstance()->getUuid());
        const auto& coordinatorID = _messageDesc->getSourceInstanceID();
        _networkManager.sendPhysical(coordinatorID, abortResponse);
    }
}

void ServerMessageHandleJob::handleAbortResponse()
{
    SCIDB_ASSERT(_query);
    SCIDB_ASSERT(_messageDesc);
    SCIDB_ASSERT(_messageDesc->getQueryID() == _query->getQueryID());

    if (!_query->isCoordinator()) {
        throwOnInvalidMessage();
    }

    LOG4CXX_DEBUG(logger, "Handling 2PC abort response from "
                  << Iid(_messageDesc->getSourceInstanceID())
                  << " for query " << _query->getQueryID());

    SCIDB_ASSERT(_query->isCoordinator());

    size_t remaining = 0;
    bool installedQuery = false;
    const auto removed =
        _query->markWorkerDone(_messageDesc->getSourceInstanceID(),
                               remaining,
                               installedQuery);
    if (!installedQuery) {
        // This can happen when an instance starts generating liveness events
        // before all other instances have installed the query during prepareQuery.
        // An instance fakes an abort message from the coordinator to itself
        // on a qualifying liveness change and will send an abort response back to
        // the coordinator for an abort that the coordinator didn't send.  At this
        // point, the query isn't installed on the coordinator so we don't have any
        // workers registered with the query, so drop this abort response.
        LOG4CXX_DEBUG(logger, "Query " << _query->getQueryID()
                      << " is not installed across the cluster yet");
        return;
    }

    if (remaining > 0) {
        LOG4CXX_DEBUG(logger, "Waiting for " << remaining
                      << " workers to finish the 2PC completion step for query "
                      << _query->getQueryID());
    }
    else {
        LOG4CXX_DEBUG(logger, "Finished waiting for all workers to complete 2PC for query "
                      << _query->getQueryID());
    }

    if (removed && remaining == 0) {
        LOG4CXX_DEBUG(logger, "Invoking 2PC finalizers during abort response handling "
                      "for query " << _query->getQueryID());
        _query->executeCompletionFinalizers();
    }
}

// must run on _query->errorQueue
void ServerMessageHandleJob::handleCommitRequest()
{
    static const char *funcName = "ServerMessageHandleJob::handleCommitRequest: ";
    ScopedActiveQueryThread saqt(_query);
    PerfTimeScope pts(PTS_SMHJ_HC);

    std::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "Unknown cluster UUID=" << clusterUuid);

    if (_query->getPhysicalCoordinatorID() != _messageDesc->getSourceInstanceID()
        || _query->isCoordinator()) {
        throwOnInvalidMessage();
    }

    _query->handleCommit();
    LOG4CXX_DEBUG(logger,
                  "Commit complete on worker, replying to coordinator " <<
                  _query->getQueryID());
    auto commitResponse = std::make_shared<MessageDesc>(mtCommitResponse);
    std::shared_ptr<scidb_msg::DummyQuery> respRecord =
        commitResponse->getRecord<scidb_msg::DummyQuery>();
    const auto& queryID = _query->getQueryID();
    commitResponse->setQueryID(queryID);
    respRecord->set_cluster_uuid(Cluster::getInstance()->getUuid());
    const auto& coordinatorID = _messageDesc->getSourceInstanceID();
    _networkManager.sendPhysical(coordinatorID, commitResponse);
}

void ServerMessageHandleJob::handleCommitResponse()
{
    SCIDB_ASSERT(_query);
    SCIDB_ASSERT(_messageDesc);
    SCIDB_ASSERT(_messageDesc->getQueryID() == _query->getQueryID());

    if (!_query->isCoordinator()) {
        throwOnInvalidMessage();
    }

    LOG4CXX_DEBUG(logger, "Handling 2PC commit response from "
                  << Iid(_messageDesc->getSourceInstanceID())
                  << " for query " << _query->getQueryID());

    SCIDB_ASSERT(_query->isCoordinator());

    size_t remaining = 0;
    bool installedQuery = false;
    const auto removed =
        _query->markWorkerDone(_messageDesc->getSourceInstanceID(),
                               remaining,
                               installedQuery);
    if (!installedQuery) {
        // This should never happen because no instance can respond to a commit
        // before the query is installed everywhere.
        // Log it and throw the exception, to be sure this is captured somewhere.
        LOG4CXX_ERROR(logger, "Query " << _query->getQueryID()
                      << " is not installed across the cluster yet ON COMMIT");
        ASSERT_EXCEPTION(false, "Query " << _query->getQueryID()
                         << " is not installed across the cluster yet ON COMMIT");
    }

    if (remaining > 0) {
        LOG4CXX_DEBUG(logger, "Waiting for " << remaining
                      << " workers to finish the 2PC completion step for query "
                      << _query->getQueryID());
    }
    else {
        LOG4CXX_DEBUG(logger, "Finished waiting for all workers to complete 2PC for query "
                      << _query->getQueryID());
    }

    if (removed && remaining == 0) {
        LOG4CXX_DEBUG(logger, "Invoking 2PC finalizers during commit response handling "
                      "for query " << _query->getQueryID());
        _query->executeCompletionFinalizers();
    }
}

void ServerMessageHandleJob::handleNotify()
{
    static const char *funcName = "ServerMessageHandleJob::handleNotify: ";

    LOG4CXX_DEBUG(logger, funcName << "Notify on processing query "
                  << _messageDesc->getQueryID() << " from instance "
                  << Iid(_messageDesc->getSourceInstanceID()));

    std::shared_ptr<scidb_msg::Liveness> record = _messageDesc->getRecord<scidb_msg::Liveness>();

    if (!record ||
        !record->IsInitialized()) {
        throwOnInvalidMessage();
    }

    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "Unknown cluster UUID=" << clusterUuid);

    const InstanceID sourceInstanceId = _messageDesc->getSourceInstanceID();
    SCIDB_ASSERT(isValidPhysicalInstance(sourceInstanceId));

    if  (_query->isCoordinator() ||
         _query->getPhysicalCoordinatorID() != sourceInstanceId) {
        throwOnInvalidMessage();
    }

    InstLivenessPtr liveness = parseLiveness(*record);
    if (!liveness ||
        liveness->find(sourceInstanceId) == NULL ||
        liveness->isDead(sourceInstanceId)) {
        throwOnInvalidMessage();
    }

    if (!_query->getCoordinatorLiveness()->isEqual(*liveness)) {
        // local _query has a non-matching liveness but it should not be a problem
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_LIVENESS_MISMATCH);
    }

    // report back to the coordinator
    const QueryID queryID = _query->getQueryID();
    LOG4CXX_DEBUG(logger, "Send message to waiting coordinator in queryID: " << queryID);
    std::shared_ptr<MessageDesc> messageDesc = makeWaitMessage(queryID);
    _networkManager.sendPhysical(sourceInstanceId, messageDesc);
}

void ServerMessageHandleJob::handleWait()
{
    static const char *funcName = "ServerMessageHandleJob::handleWait: ";

    std::shared_ptr<scidb_msg::DummyQuery> record = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    const string clusterUuid = record->cluster_uuid();
    ASSERT_EXCEPTION(clusterUuid == Cluster::getInstance()->getUuid(),
                     funcName << "Unknown cluster UUID=" << clusterUuid);

    if  (!_query->isCoordinator()) {
        throwOnInvalidMessage();
    }
    LOG4CXX_DEBUG(logger, funcName << "Wait from instance "
                  << Iid(_messageDesc->getSourceInstanceID())
                  << " on processing query "
                  << _messageDesc->getQueryID());

    _query->semResults.release(); // 释放一个条件变量，需要等待所有的节点都回复了，才释放完
}

void ServerMessageHandleJob::handleBufferSend()
{
    std::shared_ptr<scidb_msg::DummyQuery> msgRecord = _messageDesc->getRecord<scidb_msg::DummyQuery>();
    handleUpdateQueryResult();
}

void ServerMessageHandleJob::handleUpdateQueryResult()
{
    assert(_query);
    _logicalSourceId = _query->mapPhysicalToLogical(_messageDesc->getSourceInstanceID());
    {
        ScopedMutexLock mutexLock(_query->_receiveMutex, PTW_SML_RECEIVE_MUTEX);
        _query->_receiveMessages[_logicalSourceId].push_back(_messageDesc);
    }
    _query->_receiveSemaphores[_logicalSourceId].release();
}


void ServerMessageHandleJob::handleResourcesFileExists()
{
    static const char *funcName = "ServerMessageHandleJob::handleResourcesFileExists: ";
    LOG4CXX_TRACE(logger, funcName << " called");
    Resources::getInstance()->handleFileExists(_messageDesc);
}

} // namespace
