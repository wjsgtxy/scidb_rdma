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
 * @file NetworkManager.cpp
 * @author roman.somakov@gmail.com
 *
 * @brief NetworkManager class implementation.
 */
#include <network/NetworkManager.h>

#include <sys/types.h>
#include <boost/format.hpp>
#include <memory>
#include <monitor/InstanceStats.h>
#include <monitor/MonitorConfig.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h> // for makeErrorMessageFromException
#include <network/AuthMessageHandleJob.h>
#include <network/ClientMessageHandleJob.h>
#include <network/Connection.h>
#include <network/OrderedBcast.h>
#include <network/ThrottledScheduler.h>
#include <array/SharedBuffer.h>
#include <query/Query.h>
#include <system/Config.h>
#include <system/UserException.h>
#include <system/Utils.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>
#include <util/Notification.h>
#include <util/InjectedErrorCodes.h>

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * N e t w o r k M a n a g e r
 */
volatile bool NetworkManager::_shutdown=false;

NetworkManager::NetworkManager()
    :   InjectedErrorListener(InjectErrCode::FINAL_BROADCAST),
        _acceptor(_ioService,
                boost::asio::ip::tcp::endpoint(
                    boost::asio::ip::tcp::v4(),
                    safe_static_cast<uint16_t>(
                        Config::getInstance()->getOption<int>(CONFIG_PORT)))), // config_port就是配置文件中的port，默认是0
        _input(_ioService),
        _aliveTimer(_ioService),
        _aliveTimeout(DEFAULT_ALIVE_TIMEOUT_MICRO),
        _selfInstanceID(INVALID_INSTANCE),
        _selfThreadID((pthread_t)0),
        _currConnGenId(0),
        _repMessageCount(0),
        _maxRepSendQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_SEND_QUEUE_SIZE)),
        _maxRepReceiveQSize(Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE)),
        _randInstanceIndx(0),
        _aliveRequestCount(0),
        _memUsage(0),
        _msgHandlerFactory(new DefaultNetworkMessageFactory),
        _ipAddress(""),
        _livenessSubscriberID(0)
{
    // Note: that _acceptor is 'fully opened', i.e. bind()'d, listen()'d and polled as needed
    _acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true)); // 注意，这个reuse参数的含义：表示让处于time wait的socket，端口立即释放，可以重复绑定

    Scheduler::Work func = std::bind(&NetworkManager::handleLiveness);
    _livenessHandleScheduler =
       std::shared_ptr<ThrottledScheduler>(new ThrottledScheduler(DEFAULT_LIVENESS_HANDLE_TIMEOUT,
                                                             func, _ioService));
    LOG4CXX_DEBUG(logger, "Network manager is initialized, pointer is " << this);
}

NetworkManager::~NetworkManager()
{
    LOG4CXX_DEBUG(logger, "Network manager is shutting down");
    _ioService.stop();
    if (_livenessSubscriberID) {
        Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);
    }
}

void NetworkManager::run(std::shared_ptr<JobQueue> jobQueue,
                         InstanceID instanceID,
                         const std::string& address)
{
    LOG4CXX_DEBUG(logger, "NetworkManager::run()"); // called from entry.cpp:292 (runSciDB)
    _selfThreadID = ::pthread_self();
	LOG4CXX_DEBUG(logger, "NetworkManager::_selfThreadID:" << std::hex << _selfThreadID << std::dec); // dz 运行ioservice的main线程
    _selfInstanceID = instanceID; // 自己的id

    Config *cfg = Config::getInstance();
    assert(cfg);

    if (cfg->getOption<int>(CONFIG_PORT) == 0) {
        LOG4CXX_WARN(logger, "NetworkManager::run(): Starting to listen on an arbitrary port! (--port=0)"); // port我没有设置，为啥没有这个告警产生呢
    }
    LOG4CXX_DEBUG(logger, "config port is " << cfg->getOption<int>(CONFIG_PORT));

    boost::asio::ip::tcp::endpoint endPoint = _acceptor.local_endpoint();
    const uint16_t port = getPort();

    fetchInstances(); // run初始化的时候也会获取instance

    ServerCounter sc;
    _instanceMembership->visitInstances(sc);
    const size_t serverCount = sc.getCount();
    const size_t redundancy = cfg->getOption<size_t>(CONFIG_REDUNDANCY);
    if (redundancy >= serverCount) {
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
                << redundancy << serverCount << MAX_REDUNDANCY;
    }

    _ipAddress = endPoint.address().to_string();  // Save the value for resource monitoring
    _jobQueue = jobQueue;

    // make sure we have at least one thread in the client request queue
    const uint32_t nJobs = std::max(cfg->getOption<int>(CONFIG_EXECUTION_THREADS), 3);

    uint32_t adminJobs = std::max(cfg->getOption<int>(CONFIG_ADMIN_QUERIES), 1);
    adminJobs = std::min(nJobs-2, adminJobs);

    uint32_t clientJobs = std::max(cfg->getOption<int>(CONFIG_CLIENT_QUERIES), 0);
    if (clientJobs == 0) {
        // ignore this config
        clientJobs = nJobs-adminJobs-1;
    }
    clientJobs = std::min(clientJobs, nJobs-adminJobs-1);

    const uint32_t nRequests = std::max(cfg->getOption<int>(CONFIG_REQUESTS),1);

    SCIDB_ASSERT(clientJobs > 0);
    SCIDB_ASSERT(adminJobs > 0);
    SCIDB_ASSERT(clientJobs + adminJobs < nJobs);

	// dz 三个队列 request，admin request, work
    _requestQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerRequestWorkQueue", clientJobs, nRequests);

    _adminRequestQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerAdminRequestWorkQueue", adminJobs, adminJobs*2);

    _workQueue = std::make_shared<WorkQueue>(jobQueue, "NetworkManagerWorkQueue", nJobs - 1);

    LOG4CXX_INFO(logger, "Network manager is started on "
                 << address << ":" << port << " instance #" << Iid(_selfInstanceID));

    if (!cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
       startInputWatcher();
    }

    Notification<InstanceLiveness>::Subscriber listener = std::bind(&handleLivenessNotification,
                                                                    std::placeholders::_1);
    _livenessSubscriberID = Notification<InstanceLiveness>::subscribe(listener);

    OrderedBcastManager::getInstance()->init();

    startAccept();

    _aliveTimer.expires_from_now(boost::posix_time::microseconds(0));  //i.e. immediately
    _aliveTimer.async_wait(NetworkManager::handleAlive);

    LOG4CXX_DEBUG(logger, "Start connection accepting and async message exchanging");

    InjectedErrorListener::start();

    // main loop
    _ioService.run();

    InjectedErrorListener::stop();
}

void NetworkManager::handleShutdown()
{
   LOG4CXX_INFO(logger, "SciDB is going down ...");
   ConnectionMap outConns;
   {
       ScopedMutexLock scope(_mutex, PTW_SML_NM);
       assert(_shutdown);
       boost::system::error_code ec;
       _acceptor.close(ec); //ignore error
       _input.close(ec); //ignore error
       outConns.swap(_outConnections);
       getIOService().stop();
   }
}

void NetworkManager::startInputWatcher()
{
   _input.assign(STDIN_FILENO);
   _input.async_read_some(boost::asio::buffer((void*)&one_byte_buffer, sizeof(one_byte_buffer)),
                          std::bind(&NetworkManager::handleInput,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2));
}

void NetworkManager::handleInput(const boost::system::error_code& error, size_t bytes_transferr)
{
    boost::system::error_code ec;
    _input.close(ec); //ignoring error

   if (error == boost::system::errc::operation_canceled) {
      return;
   }
   if (!error) {
      LOG4CXX_INFO(logger, "Got std input event. Terminating myself.");
      // Send SIGTERM to ourselves
      // to initiate the normal shutdown process
      assert(one_byte_buffer == 1);
      kill(getpid(), SIGTERM);
   } else {
      LOG4CXX_INFO(logger, "Got std input error: "
                   << error.message() << " (" << error << ')'
                   << ". Killing myself.");
      // let us die
      kill(getpid(), SIGKILL);
   }
}

void NetworkManager::startAccept()
{
   assert(_selfInstanceID != INVALID_INSTANCE);
   std::shared_ptr<Connection> newConnection =
           std::make_shared<Connection>(*this, getNextConnGenId(), _selfInstanceID); // 这里是新建的一个conn链接，是要等待连接过来的，所以没有目标instanceid. 同时这里的genid就是新创建的一个
   LOG4CXX_DEBUG(logger, "mgr just create a new conn to accept, is " << newConnection); // dz
   _acceptor.async_accept(newConnection->getSocket(),
                          std::bind(&NetworkManager::handleAccept,
                                    this,
                                    newConnection,
                                    std::placeholders::_1));
}

void NetworkManager::handleAccept(std::shared_ptr<Connection>& newConnection,
                                  const boost::system::error_code& error)
{
    if (error == boost::system::errc::operation_canceled) {
        return;
    }

    if (false) {
        // XXX TODO: we need to provide bookkeeping to limit the number of client connection
        LOG4CXX_DEBUG(logger, "Connection dropped: too many connections");
        return;
    }
    if (!error)
    {
        // XXX TODO: we need to provide bookkeeping to reap stale incoming connections
        LOG4CXX_DEBUG(logger, "Accepted connection " << std::hex << newConnection.get() << std::dec);
//        LOG4CXX_DEBUG(logger, "Accepted connection dz shareptr is " << std::hex << newConnection << std::dec); // dz 经测试，是一样的
        newConnection->start(); // start的时候，socket的连接就已经建立链，可以获取到远端的ip和port了
        startAccept();
    }
    else
    {
        LOG4CXX_ERROR(logger, "Error #" << error
                      << " : '" << error.message()
                      << "' when accepting connection");
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_ACCEPT_CONNECTION)
              << error << error.message();
    }
}

void NetworkManager::dispatchMessage(const std::shared_ptr<Connection>& connection,
                                     const std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" start");

    SCIDB_ASSERT(messageDesc);
    try
    {
        NetworkMessageFactory::MessageHandler handler;

        bool fromClient = false;
        std::shared_ptr<Session> sess;
        if (connection) {
            sess = connection->getSession();
            LOG4CXX_DEBUG(logger, "dz: dispatchMessage, sptr conn is " << connection << ", conn session is " << sess);
            if (sess) {
                fromClient = sess->remoteIsClient(); // 返回的是session中的 is_peer成员变量，这个是初始化来的，true表示对方也是instance，false表示对方是client application
                LOG4CXX_DEBUG(logger, "has session, and from client is " << fromClient);
            }
        } else {
            // No connection implies sendLocal(), so we can trust the // 从本地发送，应该是同一个ins的，就不需要校验session了
            // message's source instance id.
            fromClient = (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE);
            LOG4CXX_DEBUG(logger, "has no conn, implies sendLocal, and from client is " << fromClient); // 实际绝大部分是0
        }

        if (connection && !sess) {
            // Connection has no Session object yet, so client must
            // still be authenticating.  Authentication is *required*
            // before any other kind of dispatch is possible.
            std::shared_ptr<MessageHandleJob> job =
                std::make_shared<AuthMessageHandleJob>(connection, messageDesc);
            job->dispatch(this);
            LOG4CXX_DEBUG(logger, "dz: conn no session, dispatch a auth job."); // dz
            handler = std::bind(&NetworkManager::publishMessage, std::placeholders::_1);
        }
        else if (!handleNonSystemMessage(messageDesc, handler)) { // 看是否是非scidb消息，即插件消息
            assert(!handler);

            if (fromClient)
            {
                ASSERT_EXCEPTION(messageDesc->getSourceInstanceID() == CLIENT_INSTANCE,
                                 "Client pretending to be instance");
                std::shared_ptr<ClientMessageHandleJob> job =
                        std::make_shared<ClientMessageHandleJob>(connection, messageDesc);
//                LOG4CXX_DEBUG(logger, "mgr before dispatch msg to ClientMessageHandleJob");
                job->dispatch(this);
            }
            else
            {
                ASSERT_EXCEPTION(messageDesc->getSourceInstanceID() != CLIENT_INSTANCE,
                                 "Instance failed to provide source id");
                std::shared_ptr<MessageHandleJob> job =
                        std::make_shared<ServerMessageHandleJob>(messageDesc);
                LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" created a ServerMessageHandleJob(message)");
//                LOG4CXX_DEBUG(logger, "mgr before dispatch msg to ServerMessageHandleJob"); // dz
                job->dispatch(this);
                LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" dispatched it");
            }
            handler = std::bind(&NetworkManager::publishMessage, std::placeholders::_1);
        }
        if (handler) {
            LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" calling dispatchMessageToListener)");
            dispatchMessageToListener(connection, messageDesc, handler);
        }
    }
    catch (const Exception& e)
    {
        // It's possible to continue message handling for other queries so we just log an error message.
        //XXX memory & overflow errors need to be considered especially for local messages
        InstanceID instanceId = messageDesc->getSourceInstanceID();
        MessageType messageType = static_cast<MessageType>(messageDesc->getMessageType());
        QueryID queryId = messageDesc->getQueryID();

        auto queryCancelled = (e.getShortErrorCode() == SCIDB_SE_QPROC &&
                               e.getLongErrorCode() == SCIDB_LE_QUERY_CANCELLED);
        if (queryCancelled) {
            LOG4CXX_INFO(logger, "Request " << strMsgType(messageType) <<
                         " from " << Iid(instanceId) <<
                         " not processed because query " << queryId <<
                         " was cancelled (this is OK).");
        }
        else {
            LOG4CXX_ERROR(logger, "In message handler: " << e.what());
            LOG4CXX_ERROR(logger, "Exception in message handler: messageType = " <<
                          strMsgType(messageType));
            LOG4CXX_ERROR(logger, "Exception in message handler: source instance ID = " <<
                          Iid(instanceId));
        }

        if (messageType != mtError
            && messageType != mtCancelQuery
            && messageType != mtAbortRequest
            && messageType != mtAbortResponse
            && queryId.isValid()
            && instanceId != INVALID_INSTANCE
            && instanceId != _selfInstanceID
            && instanceId != CLIENT_INSTANCE)
        {
            std::shared_ptr<MessageDesc> errorMessage = makeErrorMessageFromException(e, queryId);
            _sendPhysical(instanceId, errorMessage); // if possible
            LOG4CXX_DEBUG(logger, "Notified sender " << Iid(instanceId) << " that query " <<
                          queryId << (queryCancelled ? "was cancelled" : "had error")
                          << " on this instance.");
        }
    }
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" ****end");
}

// dz 这里处理消息的conn都是inconn，因为inconn才有session。out的时没有session的，out不需要校验，只有外面来的消息才需要session来验证对方身份。
// 所以我们要保这里的conn，使用rdma接收消息之后，要用保存下来的
// conn作为这里的参数。这里保存的，是一开始还没有通过rdma处理消息的时候，最初处理消息的时候保存下来的。
void NetworkManager::handleMessage(std::shared_ptr< Connection >& connection,
                                   const std::shared_ptr<MessageDesc>& messageDesc)
{
   if (_shutdown) {
      handleShutdown();
      return;
   }

   SCIDB_ASSERT(!_mutex.isLockedByThisThread());
   SCIDB_ASSERT(messageDesc);

   // dz 直接输出shared ptr 地址和 对应的原始指针地址都是一样的 connection.get()
//   LOG4CXX_DEBUG(logger, "dz: mgr handle msg, conn is " << connection);
   LOG4CXX_TRACE(logger, "handleMessage(): msg is " << messageDesc->str()); // 这个desc的str会将record也格式化输出
   if(messageDesc->getMessageType() == mtOBcastRequest){ // dz add
       LOG4CXX_DEBUG(logger, "handleMessage(): msg is " << messageDesc->str());
   }

   if (messageDesc->getMessageType() == mtAlive) { // dz 注意，如果这里是mtAlive，就不处理了
       return;
   }

   InstanceID instanceId = messageDesc->getSourceInstanceID();

   // During authentication we'll temporarily be receiving messages on
   // what is otherwise an outbound connection, hence the test for
   // isInbound() here.  The authentication handshake itself is
   // responsible for ensuring the ordering of auth messages.
   LOG4CXX_DEBUG(logger, "dz:handleMessage from remote: " << Iid(instanceId) << ", conn is " << connection
        << ", type: " << strMsgType(messageDesc->getMessageType()) << ", bytes: " << messageDesc->getMessageSize());
//        << ", conn remote id is " << Iid(connection->getRemoteInstanceId())); // dz: 注意，conn中不保存remote的id

   if (isValidPhysicalInstance(instanceId) && connection->isInbound()) { // dz 只有对方的instance是有效的，不是client和invalid，同时当前conn中remote id是 invalid instance才能进入！

       // This is how we enforce instance-to-instance FIFO ordering of messages
       // It can still be lossy, but a single TCP connection at a time should
       // guarantee FIFO ordering.

       const uint64_t connGenId = connection->getGenId();
       std::pair<ConnectionGenMap::iterator,bool> res =
               _inConnections.insert(std::make_pair(instanceId, connGenId)); // inconn只有这一个地方插入
       if(res.second){
           LOG4CXX_DEBUG(logger, "dz first _inConnections insert, instanceId is " <<  Iid(instanceId) << " , conn is " << connection);
       }

       // dz add 保存接收的这个conn，用于后续分发rdma消息的时候，能够找到一个
       // 这个接受的conn，一开始接受了之后，会通过tcp收到一些消息，然后这里就会插入，后面通过rdma收到消息之后，再通过这个conn处理
       // 主要，我们需要等待这个conn有了session之后，才插入到rdma的conn里面，否则一开始，rdma连接建立了，但是tcp这里还没有auth完成，是不能通过这个conn分发消息的
       // 在后面的dispatch的时候会被session连接掉
       auto sess = connection->getSession();
       if(sess){
           auto res2 = _inSharedConns.insert(std::make_pair(instanceId, connection));
           if(res2.second){
               // 插入成功，说明是第一个到ins id的conn
               LOG4CXX_DEBUG(logger, "dz first rdma map " << Iid(instanceId) << " to scidb conn " << _inSharedConns[instanceId]);
           }
       }

       if (!res.second) { // 上面插入失败了，已经存在到instanceId的conn了，返回的pair first的迭代器是已经存在的数据
//           LOG4CXX_DEBUG(logger, "dz: _inConnections insert failed, already exist a conn");
           if (res.first->second == connGenId) { // 已经存在的就是目前的这个conn
             // normal case
           } else if (res.first->second < connGenId) { // 目前这个conn的genid更新，所以要更新
               // record the new connection
               res.first->second = connGenId; // 更新_inConnections中这个 这个instance对应的 connGenId
               LOG4CXX_DEBUG(logger, "dz: _inConnections replace the old conn with a new one"); // 这个目前从日志看没有发生过

               // dz 同时replace _inSharedConns
               _inSharedConns[instanceId] = connection;
               LOG4CXX_DEBUG(logger, "_inSharedConns replace id " << instanceId << " now has new conn " << _inSharedConns[instanceId]);

               // report the problem
               bool dummy(false);
               if (!isDead(instanceId, dummy)) {
                   handleConnectionErrorToLive(instanceId); // dz 没有保证顺序，日志中出现了很多这个错误
               }
           } else {
               // ignore the old connection 这个是以前的conn的消息
               LOG4CXX_WARN(logger,
                            "NOT dispatching message from an old connection from instanceID="
                            << Iid(instanceId) << " messageType=" << strMsgType(messageDesc->getMessageType()));
               connection->disconnect();
               return;
           }
       }
   }

   if(MonitorConfig::getInstance()->isEnabled())
   {
       InstanceStats::getInstance()->addToNetRecv(messageDesc->getMessageSize());

       std::shared_ptr<scidb::Query> query = Query::getQueryByID(messageDesc->getQueryID(), false);
       if(query) {
            query->getStats().addToNetRecv(messageDesc->getMessageSize());
       }
   }

   if (messageDesc->getMessageType() == mtControl) {
       handleControlMessage(messageDesc);
       return;
   }

   dispatchMessage(connection, messageDesc);
}

// dz 处理控制类消息，主要好像是流控方面的
void NetworkManager::handleControlMessage(const std::shared_ptr<MessageDesc>& msgDesc)
{
    assert(msgDesc);
    std::shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    assert(record);

    InstanceID instanceId = msgDesc->getSourceInstanceID();
    if (instanceId == CLIENT_INSTANCE) {
        return;
    }
    //XXX TODO: convert assert()s to connection->close()
    if(!record->has_local_gen_id()) {
        assert(false);
        return;
    }
    if(!record->has_remote_gen_id()) {
        assert(false);
        return;
    }

    const google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>& entries = record->channels();
    for(  google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
          iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        if(!entry.has_id()) {
            assert(false);
            return;
        }
        if(!entry.has_available()) {
            assert(false);
            return;
        }
        if(!entry.has_local_sn()) {
            assert(false);
            return;
        }
        if(!entry.has_remote_sn()) {
            assert(false);
            return;
        }
        MessageQueueType mqt = static_cast<MessageQueueType>(entry.id());
        if (mqt < mqtNone || mqt >= mqtMax) {
            assert(false);
            return;
        }
    }

    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    ConnectionMap::iterator cIter = _outConnections.find(instanceId);
    if (cIter == _outConnections.end()) {
        return;
    }
    std::shared_ptr<Connection>& connection = cIter->second;
    if (!connection) {
        return;
    }

    uint64_t peerLocalGenId = record->local_gen_id();
    uint64_t peerRemoteGenId = record->remote_gen_id();
    for(google::protobuf::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = entries.begin();
        iter != entries.end(); ++iter) {

        const scidb_msg::Control_Channel& entry = (*iter);
        const MessageQueueType mqt  = static_cast<MessageQueueType>(entry.id());
        const uint64_t available    = entry.available(); // 应该是表示对方目前队列容量还有多少，也就是我还可以发送多少消息过去
        const uint64_t peerRemoteSn = entry.remote_sn(); //my last SN seen by peer 被对方看见的我的最后一个序列号
        const uint64_t peerLocalSn  = entry.local_sn();  //last SN sent by peer to me 对方发给我的最后一个的序列号
        // dz TODO trace->debug
        LOG4CXX_DEBUG(logger, "FlowCtl: RCV iid=" << Iid(instanceId)
                      << " avail=" << available
                      << " mqt=" << mqt
                      << " peer: ( lclseq=" << peerLocalSn
                      << " rmtseq=" << peerRemoteSn
                      << " lclgen=" << peerLocalGenId
                      << " rmtgen=" << peerRemoteGenId
                      << " )");
        // 设置队列状态，这个函数只在这里调用过
        connection->setRemoteQueueState(mqt, available,
                                        peerRemoteGenId, peerLocalGenId,
                                        peerRemoteSn, peerLocalSn); // 要反过来，对方发送过来的peerRemoteSn,是我这边的sn，所以放到localSn这个参数里面
    }
}

std::shared_ptr<WorkQueue> NetworkManager::getRequestQueue(int p)
{
    ASSERT_EXCEPTION(SessionProperties::validPriority(p),
                     "Unexpected session priority");

    if (p == SessionProperties::ADMIN) {
        return _adminRequestQueue;
    } else {
        return _requestQueue;
    }
}

uint64_t NetworkManager::getAvailable(MessageQueueType mqt, InstanceID forInstanceID)
{
    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt==mqtNone);
        return MAX_QUEUE_SIZE;
    }
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
    return _getAvailable(mqt, forInstanceID);
}

uint64_t NetworkManager::getAvailableRepSlots()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    const uint64_t messageCount = _repMessageCount;
    const uint64_t maxReceiveQSize = _maxRepReceiveQSize;

    uint64_t softLimit = 3*maxReceiveQSize/4;
    if (softLimit==0) {
        softLimit=1;
    }

    uint64_t availableSlots = 0;
    if (softLimit > messageCount) {
        availableSlots = (softLimit - messageCount);
    }
    return availableSlots;
}

bool NetworkManager::isBufferSpaceLow()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    fetchInstances();
    const uint64_t numInst = _instanceMembership->getNumInstances();
    const uint64_t availableSlots = getAvailableRepSlots();
    return (availableSlots < numInst);
}

uint64_t NetworkManager::_getAvailable(MessageQueueType mqt, InstanceID forInstanceID)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    assert(mqt==mqtReplication);

    fetchInstances();
    const uint64_t numInst = _instanceMembership->getNumInstances();

    const uint64_t availableSlots = getAvailableRepSlots();
    uint64_t available = 0;

    if (availableSlots>0) {

        available = availableSlots / numInst;

        if (available == 0) {
            // There is some space for the incoming chunks,
            // but not enough to accomodate one from each instance.
            // Since we dont know who is going to send to us, we choose a random instance.
            // The instances around it (provided the space allows) get a green light.
            // The same random instance remains the number of instances requests because
            // a control message typically is broadcast resulting in #instances requests at once.
            // Empirically, it seems to work as expected. If an overflow does occur, the query will abort.
            if ((_aliveRequestCount++ % numInst) == 0) {
                _randInstanceIndx  = uint64_t(Query::getRandom()) % numInst;
            }
            uint64_t forInstanceIndx(0);
            try {
                forInstanceIndx = _instanceMembership->getIndex(forInstanceID);
            } catch (scidb::InstanceMembership::NotFoundException& e) {
                LOG4CXX_WARN(logger, "Available queue size=" << available
                             << " for queue "<< mqt
                             << " for non-existent instanceID=" << Iid(forInstanceID));
                return available;
            }
            const uint64_t distLeft  = _randInstanceIndx > forInstanceIndx ?
                                       _randInstanceIndx - forInstanceIndx : forInstanceIndx - _randInstanceIndx;
            const uint64_t distRight = numInst - distLeft;
            if ( distLeft <= availableSlots/2 ||
                 distRight < availableSlots/2 ) {
                available = 1;
            }
        }
    }
    LOG4CXX_TRACE(logger, "Available queue size=" << available
                  << " for queue "<< mqt
                  << " for instanceID=" << Iid(forInstanceID));
    return available;
}

void NetworkManager::registerMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                     MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    _memUsage += messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::registerMessage _memUsage=" << _memUsage);

    // mqtReplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    ++_repMessageCount;

    LOG4CXX_TRACE(logger, "Registered message " << _repMessageCount
                  << " for queue "<<mqt << " aliveTimeout="<<_aliveTimeout);

    scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
}

void NetworkManager::unregisterMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                       MessageQueueType mqt)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    assert(_memUsage>= messageDesc->getMessageSize());

    _memUsage -= messageDesc->getMessageSize();

    LOG4CXX_TRACE(logger, "NetworkManager::unregisterMessage _memUsage=" << _memUsage);

    // mqtRplication is the only supported type for now
    if (mqt != mqtReplication) {
        assert(mqt == mqtNone);
        return;
    }

    --_repMessageCount;
    LOG4CXX_TRACE(logger, "Unregistered message " << _repMessageCount+1
                  << " for queue "<<mqt  << " aliveTimeout="<<_aliveTimeout);

    scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
}

/// internal
uint64_t
NetworkManager::getSendQueueLimit(MessageQueueType mqt)
{
    // mqtRplication is the only supported type for now
    if (mqt == mqtReplication) {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
        fetchInstances();
        SCIDB_ASSERT(_instanceMembership->getNumInstances()>0);
        return (_maxRepSendQSize / _instanceMembership->getNumInstances());
    }
    SCIDB_ASSERT(mqt==mqtNone);
    return MAX_QUEUE_SIZE;
}

uint64_t
NetworkManager::getReceiveQueueHint(MessageQueueType mqt)
{
    // mqtRplication is the only supported type for now
    if (mqt == mqtReplication) {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
        fetchInstances();
        SCIDB_ASSERT(_instanceMembership->getNumInstances()>0);
        return (_maxRepReceiveQSize / _instanceMembership->getNumInstances());
    }
    SCIDB_ASSERT(mqt==mqtNone);
    return MAX_QUEUE_SIZE;
}

void
NetworkManager::scheduleAliveNoLater(const time_t timeoutMicro)
{
    if ( _aliveTimeout > timeoutMicro) {
        _aliveTimeout = timeoutMicro;
        getIOService().post(std::bind(&NetworkManager::handleAlive,
                                      boost::system::error_code()));
    }
}

bool
NetworkManager::handleNonSystemMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                       NetworkMessageFactory::MessageHandler& handler)
{
   assert(messageDesc);
   MessageID msgID = messageDesc->getMessageType();
   if (!isPluginMessage(msgID)) {
      return false;
   }
   handler = _msgHandlerFactory->getMessageHandler(msgID);
   if (!handler) {
      LOG4CXX_WARN(logger, "Registered message handler (MsgID="<< msgID <<") is empty!");
      return true;
   }
   return true;
}

void NetworkManager::publishMessage(const std::shared_ptr<MessageDescription>& msgDesc)
{
   LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" begin");
   std::shared_ptr<const MessageDescription> msg(msgDesc);
   Notification<MessageDescription> event(msg);
   event.publish();
}

void NetworkManager::dispatchMessageToListener(const std::shared_ptr<Connection>& connection,
                                               const std::shared_ptr<MessageDesc>& messageDesc,
                                               NetworkMessageFactory::MessageHandler& handler)
{
    // no locks must be held
    SCIDB_ASSERT(!_mutex.isLockedByThisThread());

    std::shared_ptr<MessageDescription> msgDesc;

    if (messageDesc->getSourceInstanceID() == CLIENT_INSTANCE) {
        msgDesc = std::shared_ptr<MessageDescription>(
            new DefaultMessageDescription(connection,
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    } else {
        msgDesc = std::shared_ptr<MessageDescription>(
            new DefaultMessageDescription(messageDesc->getSourceInstanceID(),
                                          messageDesc->getMessageType(),
                                          messageDesc->getRecord<Message>(),
                                          messageDesc->getBinary(),
                                          messageDesc->getQueryID()
                                          ));
    }
    // invoke in-line, the handler is not expected to block
    LOG4CXX_TRACE(logger, "NetworkManager::"<<__func__<<" calling handler(msgDesc)");
    handler(msgDesc);
}

void
NetworkManager::_sendPhysical(InstanceID targetInstanceID, // 内部使用的函数
                              std::shared_ptr<MessageDesc>& messageDesc,
                              MessageQueueType mqt /* = mqtNone */)
{
    SCIDB_ASSERT(isValidPhysicalInstance(targetInstanceID)); // 注意，这里的id是物理id，调用函数前需要已经完成了逻辑到物理的转换
    if (_shutdown) {
        handleShutdown();
        boost::system::error_code aborted = boost::asio::error::operation_aborted;
        handleConnectionError(targetInstanceID, messageDesc->getQueryID(), aborted);
        return;
    }
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    assert(_selfInstanceID != INVALID_INSTANCE);
    assert(targetInstanceID != _selfInstanceID);

    // Opening connection if it's not opened yet
    std::shared_ptr<Connection> connection = _outConnections[targetInstanceID]; // map直接下标访问key，如果不存在，则插入key，用默认构造函数构造一个value出来返回
    if (!connection)
    {
        // 不存在到target的conn，开始建立连接
        fetchInstances();
        try {
            const InstanceDesc& instanceDesc = _instanceMembership->getConfig(targetInstanceID); // dz 从这个里面获取到了instance对应的ip和port
            const uint64_t genId(0);
            connection = std::make_shared<Connection>(*this, genId, _selfInstanceID, targetInstanceID); // 注意，这里创建的genId是0
            _outConnections[targetInstanceID] = connection; // outconn 只有这一个地方插入
            LOG4CXX_DEBUG(logger, "dz: _outConnections insert conn " << connection << ", targetInstanceID is " << Iid(targetInstanceID) << ", conn is going to connectAsync");

            connection->connectAsync(instanceDesc.getHost(), instanceDesc.getPort()); // dz _outConnections 是连接远端的
        } catch (const scidb::InstanceMembership::NotFoundException& e) {
            if (isDebug()) {
                std::shared_ptr<Query> query(Query::getQueryByID(messageDesc->getQueryID(), false));
                if (query) {
                    SCIDB_ASSERT(_instanceMembership->getId() >
                                 query->getCoordinatorLiveness()->getMembershipId());
                }
            }
            handleConnectionError(targetInstanceID, messageDesc->getQueryID(), e);
            return;
        }
    }

    // Sending message through connection
    connection->sendMessage(messageDesc, mqt); // dz这里的发送只是发送到队列里面，然后异步调用conn 的 pushnextmessage

    if (mqt == mqtReplication) {
        scheduleAliveNoLater(CONTROL_MSG_TIMEOUT_MICRO);
    }
}

void
NetworkManager::sendPhysical(InstanceID targetInstanceID,
                            std::shared_ptr<MessageDesc>& messageDesc,
                            MessageQueueType mqt)
{
    fetchInstances(); // 先获取了instance数据
    _sendPhysical(targetInstanceID, messageDesc, mqt);

   if(MonitorConfig::getInstance()->isEnabled()) // resource-monitoring参数是否设置了，设置了就在这个query中添加信息
   {
       InstanceStats::getInstance()->addToNetSend(messageDesc->getMessageSize());

       std::shared_ptr<scidb::Query> query = Query::getQueryByID(messageDesc->getQueryID(), false);
       if(query) {
            query->getStats().addToNetSend(messageDesc->getMessageSize());
       }
   }
}

void NetworkManager::broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   _broadcastPhysical(messageDesc);
}

void NetworkManager::MessageSender::operator() (const InstanceDesc& i)
{
    const InstanceID targetInstanceID = i.getInstanceId();
    if (targetInstanceID != _selfInstanceID) {
        _nm._sendPhysical(targetInstanceID, _messageDesc);
    }
}

// This function is all test code.
bool NetworkManager::_generateConnErrorOnCommitOrAbort(const std::shared_ptr<MessageDesc>& messageDesc)
{
    if (!isDebug()) { return false; }

    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    bool generateError = (messageDesc->getMessageType() == mtAbortRequest ||
                          messageDesc->getMessageType() == mtCommitRequest);
    if (generateError) {
        try {
            InjectedErrorListener::throwif(__LINE__, __FILE__);
            generateError = false;
        } catch (const scidb::Exception& e) {
            generateError = true;
        }
    }
    if (generateError) {
        for (auto& connPair : _outConnections) {
            if (connPair.second) { connPair.second->disconnect(); }
        }
       _outConnections.clear();
    }
    if (generateError) {
        LOG4CXX_WARN(logger, "NetworkManager::_generateConnErrorOnCommitOrAbort: " << "true" );
    }
    return generateError;
}

void NetworkManager::_broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    // We are broadcasting to the instances IN THE CURRENT membership,
    // which may be different from the query (of the message) membership.
    // Those who did not participate in the query will drop the message.
    // Those who are no longer in the membership are gone anyway.
    fetchInstances();

    if (!_generateConnErrorOnCommitOrAbort(messageDesc)) {
        MessageSender ms(*this, messageDesc, _selfInstanceID);
        _instanceMembership->visitInstances(ms);
    }
}

// dz 将物理查询计划发送到各个子节点  这个是不会发生死锁的情况下调用的，但是默认是不调用这个的
void NetworkManager::broadcastLogical(std::shared_ptr<MessageDesc>& messageDesc)
{
    if (!messageDesc->getQueryID().isValid()) {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
    }
   std::shared_ptr<Query> query = Query::getQueryByID(messageDesc->getQueryID());
   const size_t instancesCount  = query->getInstancesCount();
   InstanceID myInstanceID      = query->getInstanceID();
   assert(instancesCount>0);
   {
      ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
      for (size_t targetInstanceID = 0; targetInstanceID < instancesCount; ++targetInstanceID) // targetInstanceid为啥是从0开始的？？不应该是query中自带的吗？ new：逻辑id就是query里面instances的下标而已，会有个对应到物理id的转换
      {
          if (targetInstanceID != myInstanceID) {
              send(targetInstanceID, messageDesc, query);
          }
      }
   }
}

void NetworkManager::fetchInstances()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    if (!_instanceMembership) { // 如果获取过了就不获取了
        _instanceMembership = Cluster::getInstance()->getInstanceMembership(0);
        _randInstanceIndx = 0;
    }
}

void NetworkManager::fetchLatestInstances()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
    _instanceMembership = Cluster::getInstance()->getInstanceMembership(0);
    _randInstanceIndx = 0;
}

void
NetworkManager::sendLocal(const std::shared_ptr<Query>& query,
                          const std::shared_ptr<MessageDesc>& messageDesc)
{
    const InstanceID physicalId = query->mapLogicalToPhysical(query->getInstanceID()); // 逻辑id->物理id
    messageDesc->setSourceInstanceID(physicalId);
    sendLocal(messageDesc);
}

// 这个send local，只会是同一个ins才会使用，即使是同一台server不同ins也不会使用这个函数，而是使用send physical
void
NetworkManager::sendLocal(const std::shared_ptr<MessageDesc>& messageDesc) // 一种情况是abort query，自己给自己发送，自己设置的remote id作为src id
{
    LOG4CXX_DEBUG(logger, "dz enter send local func, msg is " << messageDesc->str());
    // the source instance ID can be any valid instance
    // ??? why should it be anything other than the local instance?
    const InstanceID physicalId = messageDesc->getSourceInstanceID();
    SCIDB_ASSERT(isValidPhysicalInstance(physicalId));

    // NOTICE: the message is dispatched by the current thread (rather than by the networking thread)
    // to guarantee ordering. The current thread should also be prepared to handle any exceptions
    // thrown by the dispatch logic (primarily OverflowException(s) & bad_alloc).
    LOG4CXX_DEBUG(logger, "dz send local to dispatch msg");
    dispatchMessage(std::shared_ptr<Connection>(), messageDesc); // 注意，这里是空的conn
}

void
NetworkManager::send(InstanceID targetInstanceId,
                     std::shared_ptr<MessageDesc>& msg)
{
   assert(msg);
   if (!msg->getQueryID().isValid()) {
       throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MESSAGE_MISSED_QUERY_ID);
   }
   std::shared_ptr<Query> query = Query::getQueryByID(msg->getQueryID());

   send(targetInstanceId, msg, query);
}

void
NetworkManager::receive(InstanceID sourceInstanceID,
                        std::shared_ptr<MessageDesc>& msg,
                        std::shared_ptr<Query>& query)
{
    Semaphore::ErrorChecker ec = std::bind(&Query::validate, query);
    ScopedWaitTimer timer(PTW_SWT_NET_RCV);
    query->_receiveSemaphores[sourceInstanceID].enter(ec, PTW_SEM_NET_RCV);
    ScopedMutexLock mutexLock(query->_receiveMutex, PTW_SML_RECEIVE_MUTEX);
    if (query->_receiveMessages[sourceInstanceID].empty()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INSTANCE_OFFLINE) << Iid(sourceInstanceID);
    }
    assert(!query->_receiveMessages[sourceInstanceID].empty());
    msg = query->_receiveMessages[sourceInstanceID].front();
    query->_receiveMessages[sourceInstanceID].pop_front();
}

void NetworkManager::send(InstanceID targetInstanceId, // 注意，这个id是逻辑id
                          std::shared_ptr<MessageDesc>& msg,
                          std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(msg);
    SCIDB_ASSERT(query);
    msg->setQueryID(query->getQueryID());
    InstanceID target = query->mapLogicalToPhysical(targetInstanceId); // 注意，这里转换了逻辑id到物理id

    ScopedWaitTimer timer(PTW_SWT_NET_SND);
    sendPhysical(target, msg);
}

void NetworkManager::send(InstanceID targetInstanceId,
                          std::shared_ptr<SharedBuffer> const& data,
                          std::shared_ptr< Query> & query)
{
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtBufferSend, data);
    send(targetInstanceId, msg, query);
}

std::shared_ptr<SharedBuffer> NetworkManager::receive(InstanceID sourceInstanceID, std::shared_ptr< Query> & query)
{
    std::shared_ptr<MessageDesc> msg;
    ScopedWaitTimer timer(PTW_SWT_NET_RCV);
    receive(sourceInstanceID, msg, query);
    return msg->getBinary();
}

void NetworkManager::_handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& liveInfo)
{
    if (logger->isDebugEnabled()) {
        MembershipID membId = liveInfo->getMembershipId();
        uint64_t ver = liveInfo->getVersion();

        LOG4CXX_DEBUG(logger, "New liveness information, membershipID=" << membId<<", ver="<<ver);
        for (auto const& i : liveInfo->getDeadInstances()) {
            LOG4CXX_DEBUG(logger, "Dead instanceID=" << Iid(i.getInstanceId())
                          << ", genID=" << i.getGenerationId());
        }
        for (auto const& i : liveInfo->getLiveInstances()) {
            LOG4CXX_DEBUG(logger, "Live instanceID=" << Iid(i.getInstanceId())
                          << ", genID=" << i.getGenerationId());
        }
    }

    if (_shutdown) {
       handleShutdown();
       return;
    }

    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    if (_instanceLiveness &&
        _instanceLiveness->getVersion() == liveInfo->getVersion()) {
       assert(_instanceLiveness->isEqual(*liveInfo));
       return;
    }

    assert(!_instanceLiveness ||
           _instanceLiveness->getVersion() < liveInfo->getVersion());
    _instanceLiveness = liveInfo;

    if (!_instanceMembership ||
        _instanceMembership->getId() != _instanceLiveness->getMembershipId()) {
        SCIDB_ASSERT(_instanceLiveness->getMembershipId() >= _instanceMembership->getId());
        fetchLatestInstances();
    }

    _handleLiveness();
}

static void markWorkerDeadForQueries(InstanceID id)
{
    auto marker = [id] (const auto& query) {
        SCIDB_ASSERT(query);
        if (query->isCoordinator() && id != CLIENT_INSTANCE) {
            size_t remaining = 0;
            bool hasInstalledQuery = false;
            const auto markedDone =
                query->markWorkerDone(id, remaining, hasInstalledQuery);
            LOG4CXX_DEBUG(logger, "While marking aborted worker for query "
                          << query->getQueryID()
                          << " on liveness error, there were " << remaining
                          << " workers remaining (markedDone=" << markedDone
                          << ", hasInstalledQuery=" << hasInstalledQuery
                          << ")");
            if (markedDone && remaining == 0) {
                LOG4CXX_DEBUG(logger,
                              "Executing completion finalizers for query "
                              << query->getQueryID());
                arena::ScopedArenaTLS arenaTLS(query->getArena());
                query->executeCompletionFinalizers();
            }
        }
    };

    Query::visitQueries(Query::Visitor(marker));
}

void NetworkManager::_handleLiveness()
{
    LOG4CXX_DEBUG(logger, "dz: _handleLiveness start ");
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

   SCIDB_ASSERT(_instanceLiveness);
   SCIDB_ASSERT(_instanceMembership);
   SCIDB_ASSERT(_instanceMembership->getId() >= _instanceLiveness->getMembershipId());

   for (ConnectionMap::iterator connIter = _outConnections.begin();
        connIter != _outConnections.end(); ) {

       const InstanceID id = (*connIter).first;
       bool becauseNotMember(false);

       if (!isDead(id, becauseNotMember)) {
           ++connIter;
           continue;
       }
       std::shared_ptr<Connection>& connection = (*connIter).second;
       if (connection) {
           LOG4CXX_DEBUG(logger, "NetworkManager::_handleLiveness: disconnecting from"
                         << " dead instance "<<id);
           // TODO: Should this be done regardless of whether the connection is present?
           // The connection's entry in the outbound connections container will be
           // erased either way, and this work is done to ensure that the connection
           // is disconnected before that happens.  It stands to reason that associated
           // queries should also be updated to reflect downed workers.  Yet, we have
           // separate logic to abort queries on connection failures.
           markWorkerDeadForQueries(id);
           connection->disconnect();
           connection.reset();
       }
       LOG4CXX_DEBUG(logger, "_handleLiveness dz: _outConnections erase conn " << connection);
       _outConnections.erase(connIter++);
       if (becauseNotMember) { _inConnections.erase(id); }
   }

   if (_instanceLiveness->getMembershipId() != _instanceMembership->getId() ||
       _instanceLiveness->getNumDead() > 0) {
       SCIDB_ASSERT(_instanceLiveness->getMembershipId() <= _instanceMembership->getId());
      _livenessHandleScheduler->schedule();
   }
}

void NetworkManager::_handleAlive(const boost::system::error_code& error)
{
    if (error == boost::asio::error::operation_aborted) {
        LOG4CXX_TRACE(logger, "NetworkManager::_handleAlive: aborted");
        return;
    }
    if (error) {
        LOG4CXX_DEBUG(logger, "NetworkManager::_handleAlive: #"
                      << error << " (" << error.message() << ")");
    }

    std::shared_ptr<MessageDesc> messageDesc = std::make_shared<MessageDesc>(mtAlive);

    if (_shutdown) {
       handleShutdown();
       LOG4CXX_WARN(logger, "NetworkManager::_handleAlive: shutdown");
       return;
    }

    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

        _broadcastPhysical(messageDesc);

        LOG4CXX_TRACE(logger, "NetworkManager::_handleAlive: last timeout="<<_aliveTimeout<<" microsecs"
                      << ", replication msgCount="<<_repMessageCount);

        if (!isBufferSpaceLow()) {
            _aliveTimeout = DEFAULT_ALIVE_TIMEOUT_MICRO;
        } else if (_repMessageCount <= 0 ) {
            _aliveTimeout += CONTROL_MSG_TIME_STEP_MICRO; //+10msec
            // In DEFAULT_ALIVE_TIMEOUT_MICRO / CONTROL_MSG_TIME_STEP_MICRO * CONTROL_MSG_TIMEOUT_MICRO (~= 50 sec)
            // of quiet the timeout will increase from CONTROL_MSG_TIMEOUT_MICRO to DEFAULT_ALIVE_TIMEOUT_MICRO
        }

        if (_aliveTimeout >= DEFAULT_ALIVE_TIMEOUT_MICRO) {
            _aliveTimeout = DEFAULT_ALIVE_TIMEOUT_MICRO;
            _aliveTimer.expires_from_now(boost::posix_time::microseconds(DEFAULT_ALIVE_TIMEOUT_MICRO));
        } else {
            _aliveTimer.expires_from_now(boost::posix_time::microseconds(CONTROL_MSG_TIMEOUT_MICRO));
        }

        _aliveTimer.async_wait(NetworkManager::handleAlive);
    }

    // Mutex not held for this.
    AuthMessageHandleJob::slowAuthKiller();
}

static void abortQueryOnConnError(const InstanceID remoteInstanceId,
                                  const std::shared_ptr<Query>& query,
                                  const std::shared_ptr<Exception>& excPtr)
{
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(excPtr);

    if (query->isCoordinator() && remoteInstanceId != CLIENT_INSTANCE) {
        LOG4CXX_DEBUG(logger, "Worker instance " << Iid(remoteInstanceId)
                      << " for query " << query->getQueryID()
                      << " disconnected, discount it from the number of expected 2PC waiters");
        // Mark the worker as done as it died and the query's aborted.
        size_t remaining = 0;
        bool hasInstalledQuery = false;
        const auto markedDone =
            query->markWorkerDone(remoteInstanceId, remaining, hasInstalledQuery);
        LOG4CXX_DEBUG(logger, "While aborting query " << query->getQueryID()
                      << " on connection error, there were " << remaining
                      << " workers remaining (markedDone=" << markedDone
                      << ", hasInstalledQuery=" << hasInstalledQuery
                      << ")");
        // The number of workers remaining can be zero at this point
        // only if all other workers have died or finished aborting.
        // In the case that two or more workers died, then both will come through
        // abortQueryOnConnError, decrementing themselves from the expected
        // remaining workers.  But only the last aborted connection will
        // pass through this next check.
        if (markedDone && remaining == 0) {
            // This is the case where the worker that died was also the last
            // one that we needed to hear from in order to invoke 2PC completion
            // finalizers.  Unfortunately, we'll never hear from him again
            // during this query's lifetime yet we still need to invoke those
            // finalizers.
            LOG4CXX_DEBUG(logger,
                          "Ensuring completion finalizers are invoked "
                          "on final worker disconnect for query " << query->getQueryID());
            const bool checkFinalState = query->checkFinalState();
            LOG4CXX_DEBUG(logger, "Query " << query->getQueryID() << " hasInstalledQuery="
                          << hasInstalledQuery << " checkFinalState=" << checkFinalState);
            std::shared_ptr<Query> pquery = query;
            {
                arena::ScopedArenaTLS arenaTLS(pquery->getArena());
                pquery->executeCompletionFinalizers();
            }
            LOG4CXX_DEBUG(logger, "Forced completion finalizers for query "
                          << pquery->getQueryID());
            return;
        }
    }
    else {
        LOG4CXX_DEBUG(logger, "Instance " << Iid(remoteInstanceId)
                      << " disconnected for query " << query->getQueryID());
    }

    // Abort if the instance we detect as dead was:
    //   1. A valid instance ID of any kind (e.g., worker, coordinator).
    //  and
    //   2. The coordinator instance for this query.
    const bool isAbort = isValidPhysicalInstance(remoteInstanceId) &&
                         (remoteInstanceId == query->getPhysicalCoordinatorID());


    // abortQueryOnConnError is called for every query on this instance, therefore
    // allow multiqueries to abort their subqueries, avoiding a double-abort scenario.
    // Additionally, if we bailed here because query was a multiquery, there's no
    // guarantee that any subqueries remain (we could have disconnected after the last
    // subquery executed but before the multiquery itself committed/aborted), thus
    // potentially leaking the multiquery.
    if (query->isSub()) {
        LOG4CXX_DEBUG(logger, "Deferring query abort for subquery " << query->getQueryID()
                      << " to multiquery " << query->getMultiqueryID());
        return;
    }

    if (isAbort) {
        LOG4CXX_DEBUG(logger, "Aborting query " << query->getQueryID()
                      << " (kind=" << query->getKind()
                      << ") on connection error");
        std::shared_ptr<MessageDesc> msg = makeAbortMessage(query->getQueryID());
        // The coordinator for this query is unavailable, so cause the query to
        // abort by sending ourselves an abort request as though the coordinator
        // sent it.
        msg->setSourceInstanceID(remoteInstanceId); // 自己设置远端id作为 src id
        NetworkManager::getInstance()->sendLocal(msg); // 本地自己发送到
    } else {
        auto what = excPtr->getWhatStr();
        LOG4CXX_DEBUG(logger, "Forwarding exception " << what
                      << " to query " << query->getQueryID()
                      << " (kind=" << query->getKind()
                      << ") on connection error");
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        if (!query->isMulti()) {
            // Some worker died, queue-up the error for the next pull or call
            // to Query::validate.
            query->handleError(excPtr);
        }
        else {
            // Multiqueries are a bit different in that they're never pulled and
            // they execute right away so they're in the OK state, so we can't
            // just lodge the exception and expect someone to notice it and
            // propagate it back to the coordinator.  Rather, just abort the
            // multiquery and if we're not the coordinator, respond to the
            // coordinator for 2PC abort, so it knows that this worker finished.
            if (query->done(excPtr) && !query->isCoordinator()) {
                LOG4CXX_DEBUG(logger,
                              "Abort complete on worker, replying to coordinator " <<
                              query->getQueryID());
                auto abortResponse = std::make_shared<MessageDesc>(mtAbortResponse);
                std::shared_ptr<scidb_msg::DummyQuery> record =
                    abortResponse->getRecord<scidb_msg::DummyQuery>();
                const auto& queryID = query->getQueryID();
                abortResponse->setQueryID(queryID);
                record->set_cluster_uuid(Cluster::getInstance()->getUuid());
                const auto coordinatorID = query->getCoordinatorID();
                NetworkManager::getInstance()->send(coordinatorID, abortResponse);
            }
        }
    }
}

void NetworkManager::reconnect(InstanceID instanceId) // dz 这里的reconnect并没有重新建立连接，下面注释说会按照需求重新restart
{
    bool isRemoteInstanceLive = false;

    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

        ConnectionMap::iterator connIter = _outConnections.find(instanceId);
        if (connIter != _outConnections.end()) {

            SCIDB_ASSERT((*connIter).first == instanceId);

            std::shared_ptr<Connection>& connection = (*connIter).second;
            LOG4CXX_DEBUG(logger, "dz: reconnect conn " << connection << " is going to erase"); // dz add
            if (connection) {
                SCIDB_ASSERT(isValidPhysicalInstance(instanceId));

                LOG4CXX_DEBUG(logger, "NetworkManager::reconnect: disconnecting from "
                              << Iid(instanceId));
                connection->disconnect();
                connection.reset(); // dz 注意，这里reset就要析构这个connection了！new: reset只会将引用计数减一，只有到0了才会析构

                bool dummy(false);
                isRemoteInstanceLive = (!isDead(instanceId, dummy));
            }

//            LOG4CXX_DEBUG(logger, "dz: _outConnections erase conn"); // dz add
            _outConnections.erase(connIter);
        }
        // the connection will be restarted on-demand
    }

    if (isRemoteInstanceLive) {
        handleConnectionErrorToLive(instanceId);
    }
}

/**
 * NetworkManager needs to schedule a cancel job which injects a query arena to the thread-local storage.
 * Query::handleCancel asserts the query arena was already in the thread-local storage.
 * This routine fills in the gap.
 */
void setArenaTLSAndCancel(const std::shared_ptr<Query>& query)
{
    arena::ScopedArenaTLS arenaTLS(query->getArena());
    query->handleCancel();
}


void NetworkManager::handleClientDisconnect(const QueryID& queryId,
                                            const ClientContext::DisconnectHandler& dh)
{
    if (!queryId.isValid()) {
      return;
   }

   LOG4CXX_DEBUG(logger, boost::str(boost::format("Client for query %lld disconnected") % queryId));
   std::shared_ptr<Query> query = Query::getQueryByID(queryId, false);
   if (!query) {
       LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryId
                     << " not found, nothing to do");
       return;
   }

   arena::ScopedArenaTLS arenaTLS(query->getArena());
   if (!dh) {
       // Handles the empty std::function DisconnectHandler case.
       assert(query->isCoordinator());
       std::shared_ptr<scidb::WorkQueue> errorQ = query->getErrorQueue();

       if (!errorQ) {
           LOG4CXX_TRACE(logger, "Query " << query->getQueryID()
                         << " no longer has the queue for error reporting,"
                         " it must be no longer active");
           return;
       }

       WorkQueue::WorkItem item = std::bind(&setArenaTLSAndCancel, query);
       std::function<void()> work = std::bind(&WorkQueue::enqueue, errorQ, item);
       item = nullptr;
       // XXX TODO: handleCancel() sends messages, and stalling the network thread can theoretically
       // cause a deadlock when throttle control is enabled. So, when it is enabled,
       // we can handle the throttle-control exceptions in handleCancel() to avoid the deadlock
       // (independently of this code).
       Query::runRestartableWork<void, WorkQueue::OverflowException>(work);

   } else {
       // Does this codepath ever get invoked?  The DisconnectHandler looks to be always
       // an empty std::function.
       WorkQueue::WorkItem item = std::bind(dh, query);
       try {
           _workQueue->enqueue(item);
       } catch (const WorkQueue::OverflowException& e) {
           LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
           assert(false);
           query->handleError(e.clone());
       }
   }
}

namespace {
    void runSessionCallback(pthread_t netmgrTid, Session::Callback cb)
    {
        // Must *not* run in network manager thread!
        LOG4CXX_DEBUG(logger, "dz run session calll back"); // dz 加日志
        SCIDB_ASSERT(!::pthread_equal(netmgrTid, ::pthread_self()));
        cb();
    }
}

void NetworkManager::handleSessionClose(std::shared_ptr<Session>& sessp)
{
    LOG4CXX_DEBUG(logger, "dz handleSessionClose"); // dz 加日志
    // Run self-contained session close callback in another thread.
    Session::Callback cb = sessp->swapCleanup(Session::Callback());
    SCIDB_ASSERT(cb);
    WorkQueue::WorkItem item =
        std::bind(&runSessionCallback, _selfThreadID, cb);
    _requestQueue->enqueue(item);
}

void NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                           const QueryID& queryID,
                                           const boost::system::error_code& error)
{
    if (!queryID.isValid()) {
        return;
    }
    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: Post error #"
                  << error << " (" << error.message() << ") to query " << queryID);

    std::shared_ptr<Query> query = Query::getQueryByID(queryID, false);
    if (!query) {
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryID
                      << " not found, nothing to do");
        return;
    }

    std::shared_ptr<SystemException> excPtr =
            SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2);
    *excPtr << error << error.message();

    abortQueryOnConnError(remoteInstanceId, query, excPtr);
}

void NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                           const QueryID& queryID,
                                           const Exception& ex)
{
    if (!queryID.isValid()) {
        return;
    }
    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: Post exception \""
                  << ex.what() << "\" to query " << queryID);

    std::shared_ptr<Query> query = Query::getQueryByID(queryID, false);
    if (!query) {
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Query " << queryID
                      << " not found, nothing to do");
        return;
   }

   abortQueryOnConnError(remoteInstanceId, query, ex.clone());
}

bool NetworkManager::isDead(InstanceID remoteInstanceId, bool& becauseNotMember)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

    SCIDB_ASSERT(remoteInstanceId != _selfInstanceID);

    fetchInstances();

    if (_instanceLiveness) {

        SCIDB_ASSERT(_instanceLiveness->getMembershipId() <= _instanceMembership->getId());
        // if remoteInstanceId is no longer in the membership,
        // we treat it as dead

        return  (_instanceLiveness->isDead(remoteInstanceId) ||
                (_instanceMembership->getId() != _instanceLiveness->getMembershipId() &&
                 (becauseNotMember = !_instanceMembership->isMember(remoteInstanceId))));
    }
    return (becauseNotMember = !_instanceMembership->isMember(remoteInstanceId));
}

void
NetworkManager::handleConnectionError(const InstanceID remoteInstanceId,
                                      const std::set<QueryID>& queries,
                                      const boost::system::error_code& error)
{
    for (QueryID const& qid : queries) {
        handleConnectionError(remoteInstanceId, qid, error);
    }
}

void
NetworkManager::handleConnectionErrorToLive(const InstanceID remoteInstanceId)
{
    SCIDB_ASSERT(isValidPhysicalInstance(remoteInstanceId));

    // When a TCP connection error occurs, there might be some messages
    // in flight which have already left our queues, so there is no
    // reliable way to detect which queries are affected without some
    // registration mechanism. Such a mechanism seems an overkill
    // at this point, so just abort all the queries with the following
    // two exceptions:
    // 1. For a client connection it does not matter because
    // all the queries attached to that connection will be aborted.
    // This method is not invoked for the client connections.
    // 2. For a "dead" instance, we should not abort all the queries
    // because the failed attempts to connect to that instance may
    // abort the queries not using the dead instance.
    // The queries which include remoteInstanceId in their live sets
    // are supposed to be notified via the liveness mechanism.

    // NOTE: remoteInstanceId may be dead by now and some queries (not using it)
    // may get aborted unnecessarily - life is tough, it is a race anyway

    LOG4CXX_ERROR(logger, "NetworkManager::handleConnectionError: "
                  "Connection error - aborting ALL queries");

    std::shared_ptr<SystemException> excPtr =
            SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2);
    *excPtr << "(unknown)" << "possible connection state loss";

    size_t qNum = Query::visitQueries(Query::Visitor(std::bind(&abortQueryOnConnError,
                                                               remoteInstanceId,
                                                               std::placeholders::_1,
                                                               excPtr)));

    LOG4CXX_TRACE(logger, "NetworkManager::handleConnectionError: "
                  "Aborted " << qNum << " queries");

    dispatchErrorToListener(remoteInstanceId);
}

namespace {
// DisconnectMessageDesc needs to be a separate class, at this point in time.  This
// MessageDesc derived class is turned into a "MessageDescription" (yes, that is a
// different class) so that Subscribers to Notification<MessageDescription> will be
// notified.  The problem is that "mtNone" (which is the only used messageType for
// DisconnectMessageDesc) is invalid in the base class because `bool validate()` will
// throw an exception. So this class is needed to override the methods: 'createRecord' and
// 'validate'.
class DisconnectMessageDesc : public MessageDesc
{
public:
    DisconnectMessageDesc() {}
    virtual ~DisconnectMessageDesc() {}
    virtual bool validate() { return true; }
protected:
    virtual MessagePtr createRecord(MessageID messageType) override
    {
        return MessagePtr(new scidb_msg::DummyQuery()); // dz 这个消息类型不一样
    }
private:
    DisconnectMessageDesc(const DisconnectMessageDesc&) = delete ;
    DisconnectMessageDesc& operator=(const DisconnectMessageDesc&) = delete ;
};
}

void
NetworkManager::dispatchErrorToListener(InstanceID remoteInstanceId)
{
    SCIDB_ASSERT(!_mutex.isLockedByThisThread());
    SCIDB_ASSERT(isValidPhysicalInstance(remoteInstanceId));

    NetworkMessageFactory::MessageHandler handler = std::bind(&NetworkManager::publishMessage,
                                                              std::placeholders::_1);

    std::shared_ptr<MessageDesc> msgDesc = std::make_shared<DisconnectMessageDesc>();
    msgDesc->setSourceInstanceID(remoteInstanceId);
    msgDesc->initRecord(mtNone); // Here mtNone indicates error condition.
    dispatchMessageToListener(std::shared_ptr<Connection>(),
                              msgDesc, handler);
}

bool
NetworkManager::DefaultNetworkMessageFactory::isRegistered(const MessageID& msgID)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   return (_msgHandlers.find(msgID) != _msgHandlers.end()); // dz：判断消息是否注册，_msgHandlers会绑定消息类型的处理函数
}

// dz：添加消息类型
bool
NetworkManager::DefaultNetworkMessageFactory::addMessageType(const MessageID& msgID,
                                                             const MessageCreator& msgCreator,
                                                             const MessageHandler& msgHandler)
{
   if (!isPluginMessage(msgID)) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   return  _msgHandlers.insert(
              std::make_pair(msgID,
                 std::make_pair(msgCreator, msgHandler))).second; //dz： map insert second 是插入是否成功
}

bool
NetworkManager::DefaultNetworkMessageFactory::removeMessageType(const MessageID& msgId)
{
   if (!isPluginMessage(msgId)) {
      return false;
   }
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
   return (_msgHandlers.erase(msgId) > 0);
}


MessagePtr
NetworkManager::DefaultNetworkMessageFactory::createMessage(const MessageID& msgID)
{
   MessagePtr msgPtr;
   NetworkMessageFactory::MessageCreator creator;
   {
      ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
      MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgID);
      if (iter != _msgHandlers.end()) {
         creator = iter->second.first;
      }
   }
   if (creator) {
      msgPtr = creator(msgID);
   }
   return msgPtr;
}

NetworkMessageFactory::MessageHandler
NetworkManager::DefaultNetworkMessageFactory::getMessageHandler(const MessageID& msgType)
{
   ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

   MessageHandlerMap::const_iterator iter = _msgHandlers.find(msgType);
   if (iter != _msgHandlers.end()) {
      NetworkMessageFactory::MessageHandler handler = iter->second.second;
      return handler;
   }
   NetworkMessageFactory::MessageHandler emptyHandler;
   return emptyHandler;
}



} // namespace scidb
