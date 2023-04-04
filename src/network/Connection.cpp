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
 * Connection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "Connection.h"

#include <network/NetworkManager.h>

#include <system/Auth.h>
#include <system/Config.h>
#include <system/Constants.h>
#include <system/UserException.h>
#include <util/Notification.h>
#include <util/Timing.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>

#include <log4cxx/logger.h>
#include <memory>
#include <sstream>

#include "rdma/prototypes.h"
#include "rdma/RdmaUtilities.h"
#include "rdma/RdmaCommManager.h"
#include "rdma/RdmaConnection.h"


using namespace std;
namespace bae = boost::asio::error;

// dz add 定义在entry.cpp中的全局变量
extern bool g_use_rdma;

namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

typedef std::unordered_map<InstanceID, std::shared_ptr<scidb::RdmaConnection>> RConnectionMap;
/***
 * C o n n e c t i o n
 */
Connection::Connection(
    NetworkManager& networkManager,
    uint64_t genId,
    InstanceID selfInstanceId,
    InstanceID instanceID): // dz 注意，声明的时候instanceID有个默认参数INVALID_INSTANCE，这个是远端的id
    BaseConnection(networkManager.getIOService()), // dz 这里用networkmanager的ioservice来初始化了它的父类 baseconnection
    _messageQueue(instanceID, *this),
    _networkManager(networkManager),
    _remoteInstanceId(instanceID), // 默认INVALID_INSTANCE，注意，invalid和client的id是一样的
    _selfInstanceId(selfInstanceId),
    _sessPri(SessionProperties::NORMAL),
    _genId(genId),
    _connectionState(NOT_CONNECTED),
    _remotePort(0),
    _isSending(false),
    _logConnectErrors(true),
    _flushThenDisconnect(false),
    _lastRecv(getCoarseTimestamp())
{
   assert(selfInstanceId != INVALID_INSTANCE); // self的id直接传参进来的
   // _remoteInstanceId如果传入的默认的invalid=client，那么这就是一个接受的conn，不是发送的conn！！！
   LOG4CXX_DEBUG(logger, "dz: construct a new conn: " << this << ", _remoteInstanceId: " << Iid(_remoteInstanceId) << ", _selfInstanceId: " << Iid(_selfInstanceId));
}

// start的时候，socket的连接就已经建立链，可以获取到远端的ip和port了
// 注意，如果是在netmgr中先创建的conn(此时远端ins id是invalid=client), 然后用这个conn的socket接受的远端请求，然后start，那么这里面的remote还是invalid或者说client id。
// 只有我方主动连接到远端的，那么这个时候创建的conn，才会在构造函数中提供远端的remote ins id。上面那个接受的conn，其实也不需要知道对方的id是多少，我们只需要知道发送的时候，远端的ins id是多少。
void Connection::start()
{
    assert(!_error);

    // Direct transition to AUTH_IN_PROGRESS since we are the server end.
    assert(_connectionState == NOT_CONNECTED);
    _connectionState = AUTH_IN_PROGRESS;

    setRemoteIpAndPort(); // dz 建立的socket连接可以获取到远端的ip和端口

    LOG4CXX_DEBUG(logger, "conn " << this <<" start, Connection received from " << getPeerId());
    // The first work we should do is reading initial message from client
    _networkManager.getIOService().post(std::bind(&Connection::readMessage,shared_from_this()));
}


/*
 * @brief Read from socket sizeof(MessageHeader) bytes of data to _messageDesc.
 */
void Connection::readMessage()
{
   LOG4CXX_TRACE(logger, "Reading next message header");

   assert(!_messageDesc);
   _messageDesc = std::make_shared<ServerMessageDesc>();
   // XXX TODO: add a timeout after we get the first byte
   boost::asio::async_read(_socket,
                           boost::asio::buffer(&_messageDesc->getMessageHeader(),
                                               sizeof(_messageDesc->getMessageHeader())),
                           std::bind(&Connection::handleReadMessage,
                                     shared_from_this(),
                                     std::placeholders::_1,
                                     std::placeholders::_2));
}

/**
 * @brief Validate _messageDesc & read from socket
 *    _messageDesc->getMessageHeader().getRecordSize() bytes of data
 */
void Connection::handleReadMessage(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   if(!_messageDesc->validate() ||
      _messageDesc->getMessageHeader().getSourceInstanceID() == _selfInstanceId) {
      LOG4CXX_ERROR(logger, "Connection::handleReadMessage: unknown/malformed message,"
                            " closing connection: " << this);
      handleReadError(bae::make_error_code(bae::eof));
      return;
   }

   assert(bytes_transferr == sizeof(_messageDesc->getMessageHeader()));
   assert(_messageDesc->getMessageHeader().getSourceInstanceID() != _selfInstanceId);
   assert(_messageDesc->getMessageHeader().getNetProtocolVersion() == scidb_msg::NET_PROTOCOL_CURRENT_VER);

   boost::asio::async_read(_socket,
                           _messageDesc->_recordStream.prepare(
                               _messageDesc->getMessageHeader().getRecordSize()), // dz：准备这么多的空间，读取的数据就放在_recordStream里面
                           std::bind(&Connection::handleReadRecordPart,
                                     shared_from_this(),
                                     std::placeholders::_1,
                                     std::placeholders::_2));

   LOG4CXX_TRACE(logger, "Connection::handleReadMessage: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " from instanceID="
            << Iid(_messageDesc->getMessageHeader().getSourceInstanceID())
            << " ; recordSize="
            << _messageDesc->getMessageHeader().getRecordSize()
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());
}

/*
 * @brief  If header indicates data is available, read from
 *         socket _messageDesc->binarySize bytes of data.
 */
void Connection::handleReadRecordPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   assert(_messageDesc->validate());

   assert(  _messageDesc->getMessageHeader().getRecordSize() ==
            bytes_transferr);

   assert(  _messageDesc->getMessageHeader().getSourceInstanceID() !=
            _selfInstanceId);

   if (!_messageDesc->parseRecord(bytes_transferr)) { // dz：这里是不是说明header是没有序列化的，只有后面的record属于Google那个消息才序列
       LOG4CXX_ERROR(logger,
                     "Network error in handleReadRecordPart: cannot parse record for "
                     << " msgID="
                     << _messageDesc->getMessageHeader().getMessageType()
                     << ", closing connection");

       handleReadError(bae::make_error_code(bae::eof));
       return;
   }
   _messageDesc->prepareBinaryBuffer(); // dz 准备binary数据buffer

   LOG4CXX_TRACE(logger,
        "handleReadRecordPart: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());

   if (_messageDesc->_messageHeader.getBinarySize())
   {
       boost::asio::async_read(_socket,
                               boost::asio::buffer(_messageDesc->_binary->getWriteData(),
                                                   _messageDesc->_binary->getSize()),
                               std::bind(&Connection::handleReadBinaryPart,
                                         shared_from_this(),
                                         std::placeholders::_1,
                                         std::placeholders::_2));
       return;
   }

   handleReadBinaryPart(error, 0); // dz:没有binary数据
}

/*
 * @brief Invoke the appropriate dispatch routine
 */
void Connection::handleReadBinaryPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }
   assert(_messageDesc);

   assert(_messageDesc->getMessageHeader().getBinarySize() ==
          bytes_transferr);

   std::shared_ptr<MessageDesc> msgPtr;
   _messageDesc.swap(msgPtr); // dz注意，这里新建了一个msgPtr,然后和_messageDesc交换了！！！这样_messageDesc就空出来了，可以接受下一次的消息了

   _lastRecv = getCoarseTimestamp();

   // mtHangup means we need not read from this connection again, it
   // is for outbound peer traffic.  (We did read from it during
   // authentication, but we are done now.)
   //
   // NOTE, however, that because of SDB-5702 mtHangup is not
   // currently being used. // dz TODO 注意，现在没有使用这个mtHangup的消息了，我们也可以不用考虑了
   //
   if (msgPtr->getMessageType() == mtHangup) {
       if (isOutbound()) {
           LOG4CXX_TRACE(logger, "Hanging up on outbound connection to "
                         << getPeerId() << " on conn=" << hex << this << dec);
           // We just need to NOT queue up another readMessage() call.  // dz 应该是流控功能，不再读取消息了
           return;
       }
       LOG4CXX_DEBUG(logger, "DROPPED mtHangup from non-peer " << getPeerId()
                     << " on conn=" << hex << this << dec);
   }
   else {
       std::shared_ptr<Connection> self(shared_from_this());
       // dz add, 消息都收到了，处理消息
       _networkManager.handleMessage(self, msgPtr);
   }

   // Preparing to read new message
   assert(_messageDesc.get() == NULL); // dz，这里调用的shareptr的get，返回原始指针，上面交换了，那么这里的原始指针就为空了
   readMessage();
}

void Connection::sendMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    pushMessage(messageDesc, mqt);
   _networkManager.getIOService().post(std::bind(&Connection::pushNextMessage,
                                                 shared_from_this()));
}

void Connection::sendMessageDisconnectOnError(std::shared_ptr<MessageDesc>& m,
                                              NetworkManager::MessageQueueType mqt)
{
    try {
        sendMessage(m, mqt);
    }
    catch (const scidb::Exception& e) {
        try { disconnect(); } catch (...) {}
        e.raise();
    }
}

void Connection::pushMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        // dz add
//        LOG4CXX_DEBUG(logger, "dz pushMessage " << messageDesc->str());

        LOG4CXX_TRACE(logger, "pushMessage: send message queue size = "
                      << _messageQueue.size()
                      << " for instanceID=" << Iid(_remoteInstanceId)
                      << " flushThenDisconnect=" << _flushThenDisconnect
                      << " conn=" << hex << this << dec);

        if(_flushThenDisconnect)
        {
            // Do not allow new messages.
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR2)
                       << "(not connected)" << "pending disconnect after flush";
        }

        if (isAuthenticating() && isAuthMessage(messageDesc->getMessageType()))
        {
            // Auth handshake must happen first!
            LOG4CXX_TRACE(logger, "pushMessage: pushing an auth msg");
            connStatus = _messageQueue.pushUrgent(messageDesc);
        } else {
            LOG4CXX_TRACE(logger, "pushMessage: pushing a msg for mqt=" << mqt);
            connStatus = _messageQueue.pushBack(mqt, messageDesc);
        }
        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        LOG4CXX_TRACE(logger, "pushMessage: will publishQueueSize");
        _networkManager.getIOService().post(std::bind(&Connection::publishQueueSize,
                                                      shared_from_this()));
    }
}

// 从队列中弹出要发送的消息
std::shared_ptr<MessageDesc> Connection::popMessage()
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    std::shared_ptr<MessageDesc> msg;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        connStatus = _messageQueue.popFront(msg); // 目前一共2个队列，轮询这两个队列，pop消息发送
        // If popped msg while authenticating, then no connStatus
        // (because auth happens on non-flow-controlled channel).
        SCIDB_ASSERT(!msg || !isAuthenticating() || !connStatus);
        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(std::bind(&Connection::publishQueueSize,
                                                      shared_from_this()));
    }
    if (logger->isTraceEnabled() && msg) {
        LOG4CXX_TRACE(logger, "popMessage: popped "
                      << strMsgType(msg->getMessageType())
                      << " from conn=" << hex << this << dec);
    }
    return msg;
}

void Connection::setRemoteQueueState(NetworkManager::MessageQueueType mqt,  uint64_t size,
                                     uint64_t localGenId, uint64_t remoteGenId,
                                     uint64_t localSn, uint64_t remoteSn)
{
    assert(mqt != NetworkManager::mqtNone);
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        connStatus = _messageQueue.setRemoteState(mqt, size,
                                                  localGenId, remoteGenId,
                                                  localSn, remoteSn); // dz TODO 2023年2月22日 17:17:28 设置状态crash了
        // todo 2022年12月20日 00:03:57 改成debug
        LOG4CXX_DEBUG(logger, "setRemoteQueueSize: remote queue size = "
                      << size <<" for instanceID=" << Iid(_remoteInstanceId)
                      << " for queue " << mqt
                      << ", conn=" << hex << this << dec);

        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(
            std::bind(&Connection::publishQueueSize, shared_from_this()));
    }
    _networkManager.getIOService().post(std::bind(&Connection::pushNextMessage,
                                                  shared_from_this()));
}

bool
Connection::publishQueueSizeIfNeeded(const std::shared_ptr<const NetworkManager::ConnectionStatus>& connStatus)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    if (!connStatus) {
        return false;
    }
    _statusesToPublish[connStatus->getQueueType()] = connStatus;
    return true;
}

void Connection::publishQueueSize()
{
    ConnectionStatusMap toPublish;
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
        toPublish.swap(_statusesToPublish);
    }
    for (ConnectionStatusMap::iterator iter = toPublish.begin();
         iter != toPublish.end(); ++iter) {

        std::shared_ptr<const NetworkManager::ConnectionStatus>& status = iter->second;
        NetworkManager::MessageQueueType mqt = iter->first;
        assert(mqt == status->getQueueType());
        assert(mqt != NetworkManager::mqtNone);
        assert(mqt < NetworkManager::mqtMax);
        LOG4CXX_TRACE(logger, "FlowCtl: PUB iid=" << Iid(_remoteInstanceId)
                      << " avail=" << status->getAvailableQueueSize()
                      << " mqt=" << mqt);
        assert(_remoteInstanceId == status->getPhysicalInstanceId());

        Notification<NetworkManager::ConnectionStatus> event(status);
        event.publish();
    }
}

size_t Connection::queueSize()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    return _messageQueue.size();
}

void Connection::handleSendMessage(const boost::system::error_code& error,
                                   size_t bytes_transferred,
                                   std::shared_ptr< std::list<std::shared_ptr<MessageDesc> > >& msgs,
                                   size_t bytes_sent)
{
   _isSending = false; // dz 只有这一个地方把这个改过来了
   if (!error) { // normal case
       assert(msgs);
       assert(bytes_transferred == bytes_sent);
       /*if (logger->isTraceEnabled()) {
           for (auto& messageDesc : *msgs) {
               LOG4CXX_TRACE(logger, "handleSendMessage: bytes_transferred="
                             << messageDesc->getMessageSize()
                             << ", "<< getPeerId()
                             << ", " << strMsgType(messageDesc->getMessageType())
                             << ", conn=" << hex << this << dec);
           }
       }*/
       // dz add 发送结束了，发了哪些数据
//       LOG4CXX_DEBUG(logger, "dzs: sended message is follow:");
       for (auto& messageDesc : *msgs) {
           LOG4CXX_DEBUG(logger, "handleSendMessage: bytes: "
                   << messageDesc->getMessageSize()
                   << ", "<< getPeerId()
                   << ", type: " << strMsgType(messageDesc->getMessageType())
                   << ", conn: " << hex << this << dec << ", query id: " <<  messageDesc->getQueryID());
//           LOG4CXX_DEBUG(logger, "send message query id is " << messageDesc->getQueryID());
       }

       pushNextMessage();
       return;
   }

   // error case

   assert(error != bae::interrupted);
   assert(error != bae::would_block);
   assert(error != bae::try_again);

   LOG4CXX_ERROR(logger, "Network error #" << error
                 << " (" << error.message() << ") in " << __FUNCTION__
                 << ", peer " << getPeerId());

   for (std::list<std::shared_ptr<MessageDesc> >::const_iterator i = msgs->begin();
        i != msgs->end(); ++i) {
       const std::shared_ptr<MessageDesc>& messageDesc = *i;
       _networkManager.handleConnectionError(_remoteInstanceId, messageDesc->getQueryID(), error);
   }

   if (_connectionState == CONNECTED || _connectionState == AUTH_IN_PROGRESS) {
       // dz add
       LOG4CXX_DEBUG(logger, "handleSendMessage failed, and conn status is connected or auth in progress, then disconnect");
       disconnectInternal();
   }
   if (_remoteInstanceId == INVALID_INSTANCE) {
       LOG4CXX_TRACE(logger, "Not recovering connection from "<<getPeerId());
       return;
   }

   LOG4CXX_DEBUG(logger, "Recovering connection to " << getPeerId());
   _networkManager.reconnect(_remoteInstanceId);
}

// 发送消息的函数
void Connection::pushNextMessage()
{
   // Always use this local copy in this routine.
   bool flushThenDisconnect = _flushThenDisconnect;

   if (_connectionState != CONNECTED && _connectionState != AUTH_IN_PROGRESS) { // dz 只有这两个状态能够发送数据，其他不行
      assert(!_isSending);
      LOG4CXX_TRACE(logger, "Not yet connected to " << getPeerId());
      return;
   }
   if (_isSending) { // dz 在发送数据的时候，不能再次发送数据
      LOG4CXX_TRACE(logger, "Already sending to " << getPeerId());
      return;
   }

   vector<boost::asio::const_buffer> constBuffers;  // dz：这是一个vector，里面有很多要发送的buffer
   typedef std::list<std::shared_ptr<MessageDesc> > MessageDescList;
   std::shared_ptr<MessageDescList> msgs = std::make_shared<MessageDescList>(); //XXX TODO: can be created once
   size_t size(0);
   const size_t maxSize(32*KiB); //XXX TODO: pop all the messages!!!

   bool hasOrderMsgs = false; // 是否有需要确保顺序的消息，有的话不通过rdma发送

   // Get sendable messages from the multichannel queue and serialize
   // them into constBuffers.
   while (true) {
       std::shared_ptr<MessageDesc> messageDesc = popMessage(); // dz 不断的从队列中pop出来要发送的消息，然后将这些消息发送出去
       if (!messageDesc) {
           break;
       }
       msgs->push_back(messageDesc);

       // dz add test todo 待删除
//       if(messageDesc->getMessageType() == mtNotify){
//           std::shared_ptr<scidb_msg::Liveness> livemsg = dynamic_pointer_cast<scidb_msg::Liveness>(messageDesc->_record);
//           LOG4CXX_DEBUG(logger, "send mtNotify cluster_uuid is " << livemsg->cluster_uuid() << ", membership_id is "
//                                                    << livemsg->membership_id() << ", version is " << livemsg->version());
//       }
       MessageID type = messageDesc->getMessageType();
       if (type != mtAlive) {
           if(!hasOrderMsgs){
               if(type == mtOBcastRequest || type == mtOBcastReply || type == mtLiveness || type == mtLivenessAck){
                   hasOrderMsgs = true;
               }
           }
           // mtAlive are useful only if there is no other traffic
           messageDesc->_messageHeader.setSourceInstanceID(
                _selfInstanceId);

           messageDesc->writeConstBuffers(constBuffers);
           size += messageDesc->getMessageSize(); // 获取的是3部分的消息大小之和
           if (size >= maxSize) { // dz 注意这里有最大消息的限制，一次性最大传输32kb数据，感觉会比32kb要大一点，因为是先添加，然后再判断的
               break;
           }
       }
   }

   if (msgs->empty()) { // 消息为空，没有要发送的消息
        if(flushThenDisconnect)
        {
            // 1)  A thread can add a message to _messageQueue only if it gets the mutex and
            //     subsequently _flushThenDisconnect=false
            //     See:  pushMessage()
            //
            // 2)  A thread trying to add a message to _messageQueue must thow an exception
            //     if it gets the mutex and subsequently _flushThenDisconnect=true
            //     See:  pushMessage()
            //
            // 3)  A thread can only set _flushThenDisconnect=true after waiting for the mutex
            //     See:  flushThenDisconnect()
            //
            // 4)  When processing queue messages:
            //      A)  If _flushThenDisconnect=false at the beginning of pushNextMessage() then
            //          the thread goes about business as usual
            //
            //      B)  If _flushThenDisconnect=true at the beginning of pushNextMessage() then
            //          we have a guarantee that no new messages can be added to the _messageQueue
            //          other than the messages that will be read in the while loop above (see #2).
            //          The local copy of _flushThenDisconnect thus ensures the equality of msgs
            //          with _messageQueue until the code reaches this disconnect point.
            //
            //      C) If the value of _flushThenDisconnect changes from false to true after the
            //         local copy of flushThenDisconnect is made but prior to the if(msgs->empty()),
            //         then the new message pushed in pushNextMessage() is guaranteed to be
            //         scheduled by the thread that inserted the new message into the _messageQueue.
            //
            disconnectInternal();
        }
        return;
   }

    // Once authentication handshake is done, peer-to-peer messaging
    // can include flow control messages.
    if (_remoteInstanceId != CLIENT_INSTANCE && !isAuthenticating()) { // dz 不是客户端，同时没有在auth过程中
       std::shared_ptr<MessageDesc> controlMsg = makeControlMessage();
       if (controlMsg) {
           msgs->push_back(controlMsg);
           controlMsg->writeConstBuffers(constBuffers);
           controlMsg->_messageHeader.setSourceInstanceID(
                _selfInstanceId);
           size += controlMsg->getMessageSize();
       }
    }

    if (size == 0) { //XXX TODO: aliveMsg can be created once // dz 什么都没有要发送的，就发送一个mtAlive消息，应该是心跳消息
       assert(!msgs->empty());
       assert(msgs->front()->getMessageType() == mtAlive);
       std::shared_ptr<MessageDesc>& aliveMsg = msgs->front();
       aliveMsg->writeConstBuffers(constBuffers);
       aliveMsg->_messageHeader.setSourceInstanceID(_selfInstanceId);
       size += aliveMsg->getMessageSize();
    }

    if(g_use_rdma && !hasOrderMsgs){
        // 如果使用rdma
//        LOG4CXX_DEBUG(logger, "conn before rdma, src id is " << Iid(_selfInstanceId) <<
//            ", remote id is " << Iid(_remoteInstanceId));

        if(_remoteInstanceId != CLIENT_INSTANCE && _remoteInstanceId != INVALID_INSTANCE && _selfInstanceId != CLIENT_INSTANCE){
            // 注意，这么多消息，目的地是一样的，都是同一个remoteInstance，只有节点之间才通过rdma传输消息，节点和client之间不通过rdma传输
            // 注意，这个是out conn，remote id应该都是有效的，因为在之前mgr _sendPhysical那边就判断过了，主要是client端自己_selfInstanceId，不能通过rdma发送

            // dz 遍历这个msgs，注册对应的内存，然后通过rdma发送出去
            RConnectionMap& mp = RdmaCommManager::getInstance()->_routConnections; // 多线程访问需要加锁 TODO 之前用auto，可能是拷贝复制了！！！
//            LOG4CXX_DEBUG(logger, "rdma conn mp is " << &mp);
            int ret;
            bool flag = false;
            {
                ScopedMutexLock scope(RdmaCommManager::getInstance()->_outMutex, PTW_SML_NM); // todo 第二个参数用的network manager的
                flag = (mp.find(_remoteInstanceId) != mp.end()); // todo 每次发送都去判断一下，太浪费了，效率比较低
            }
            if(flag){
//                LOG4CXX_DEBUG(logger, "_connectionState is " << _connectionState << ", _session is " << _session);
//            if(_connectionState == CONNECTED  && _session){ // 注意，out conn不会有session，session在对方的conn上面，这里只用判断是否连接已经建立了就OK了
                // CONNECTED状态是在发送了auth logon之后，在authDoneinternal里面设置的，其实这个时候只是发了一个auth logon的消息，就默认已经通过认证了，改成connected状态了
                if(_connectionState == CONNECTED){
                    // tcp连接已经建立了，同时节点之间auth已经通过了，即有了session，才能通过rdma发送消息，要不会被session的校验拦截assert报错
                    LOG4CXX_DEBUG(logger, "send use rdma, remote id: " << Iid(_remoteInstanceId) <<
                        ", ip: " << _remoteIp.to_string() << ", port: " << _remotePort << ", this origin conn: " << this);

                    std::shared_ptr<RdmaConnection> conn = mp.find(_remoteInstanceId)->second;
                    ret = conn->send_remote(constBuffers);
                    if(ret != 0){
                        LOG4CXX_ERROR(logger, "rdma send failed");
                    }
                    // 本来是在回调中调用的，这里就直接输出了
                    for (auto& messageDesc : *msgs) {
                        LOG4CXX_DEBUG(logger, "rdma send msgs: bytes_transferred="
                                << messageDesc->getMessageSize()
                                << ", "<< getPeerId()
                                << ", message type is " << strMsgType(messageDesc->getMessageType())
                                << ", conn=" << hex << this << dec << ", query id: " <<  messageDesc->getQueryID());
                    }

                    _isSending = false; // todo 原本的逻辑应该是在发送成功的回调中才改成false，这里默认发送成功，先不处理发送异常
                    return; // 这里return了之后，就不会走下面boost的async_write了
                }
            }else{
                // 连接还没有建立，则先建立到对端的连接, 需要对端的instance id和远端的ip
                // msg中的message header不含有对端的instance id，因为刚开始也不知道对端的id，只知道对端的ip
                // 所以这里先传入invalid instance，暂时先等待接收到时候创建链接吧 new 2022年12月18日 20:45:19 ：其实这里已经有对方的 ins id了，netmgr初始化的时候fetch了
                LOG4CXX_INFO(logger, "first rdma connect, self Iid: " << Iid(_selfInstanceId) << " , remote Iid: "
                    << Iid(_remoteInstanceId) << ", ip is " << _remoteIp.to_string() << ", port is " << _remotePort);

                RdmaCommManager* rdma_mgr = RdmaCommManager::getInstance();
//                LOG4CXX_INFO(logger,"Connection get rdmamgr is " << rdma_mgr);
                rdma_mgr->connect_remote(_selfInstanceId, _remoteInstanceId, _remoteIp.to_string());
//                LOG4CXX_INFO(logger,"conn connect_remote end.");
                // 因为要建立链接，所以当前消息任然通过下面的boost发送
            }
        }
    }

    // 注意，如果消息通过上面的rdma conn发送了，那么就不通过这里的socket发送了
   boost::asio::async_write(_socket,
                            constBuffers, // dz：把多个buffer一起发送出去
                            std::bind(&Connection::handleSendMessage,
                                      shared_from_this(),
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      msgs, // dz：注意，msgs也回调带过去了，存储了当前发送过去的消息，如果出错了，可以针对每一个消息做错误处理
                                      size));

   _isSending = true;
}

MessagePtr
Connection::ServerMessageDesc::createRecord(MessageID messageType)
{
    if (isScidbMessage(messageType)) {
      return MessageDesc::createRecord(messageType);
   }

   // Plugin message, consult the factory.
   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = NetworkManager::getInstance()->getNetworkMessageFactory();
   assert(msgFactory);
   MessagePtr recordPtr = msgFactory->createMessage(messageType);

   if (!recordPtr) {
      LOG4CXX_ERROR(logger, "Unknown message type " << strMsgType(messageType) << " (" << messageType << ')');
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
   }
   return recordPtr;
}

bool
Connection::ServerMessageDesc::validate()
{
   if (MessageDesc::validate()) { // dz：对消息头的pb协议的校验和消息类型的校验    
      return true;
   }
   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = NetworkManager::getInstance()->getNetworkMessageFactory();
   assert(msgFactory);
   return msgFactory->isRegistered(getMessageType());
}

void Connection::onResolve(std::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                           const boost::system::error_code& err,
                           boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
 {
    assert(query);
    assert(resolver);

    if (_connectionState != CONNECT_IN_PROGRESS ||
        _query != query) {
       LOG4CXX_DEBUG(logger, "Dropping resolve query "
                     << query->host_name() << ":" << query->service_name());
       return;
    }

    boost::asio::ip::tcp::resolver::iterator end;
    if (err || endpoint_iterator == end) {
       _error = err ? err : bae::host_not_found;
       if (_logConnectErrors) {
          _logConnectErrors = false;
          LOG4CXX_ERROR(logger, "Network error #"
                        << _error << " (" << _error.message() << ")"
                        << " while resolving name of "
                        << getPeerId() << ", "
                        << _query->host_name() << ":" << _query->service_name());

       }
       disconnectInternal();
       _networkManager.reconnect(_remoteInstanceId);
       return;
    }

    LOG4CXX_TRACE(logger, "Connecting to the first candidate for: "
                  << _query->host_name() << ":" << _query->service_name());
    boost::asio::ip::tcp::endpoint ep = *endpoint_iterator;
    _socket.async_connect(ep,
                          std::bind(&Connection::onConnect,
                                    shared_from_this(),
                                    resolver,
                                    query,
                                    ++endpoint_iterator,
                                    std::placeholders::_1));
 }

void Connection::onConnect(std::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                           boost::asio::ip::tcp::resolver::iterator endpoint_iterator,
                           const boost::system::error_code& err)

{
   assert(query);
   assert(resolver);
   boost::asio::ip::tcp::resolver::iterator end;

   if (_connectionState != CONNECT_IN_PROGRESS ||
       _query != query) {
      LOG4CXX_TRACE(logger, "Dropping resolve query "
                    << query->host_name() << ":" << query->service_name());
      return;
   }

   if (err && endpoint_iterator == end) {
      if (_logConnectErrors) {
         _logConnectErrors = false;
         LOG4CXX_ERROR(logger, "Network error #"
                       << err << " (" << err.message() << ")"
                       << " while connecting to "
                       << getPeerId() << ", "
                       << _query->host_name() << ":" << _query->service_name() << ", conn: " << this);
      }
      disconnectInternal();
      _error = err;
      _networkManager.reconnect(_remoteInstanceId); // dz 出现过 #system:111 (Connection refused)错误，断开后重新建立连接的
      return;
   }

   if (err) {
      LOG4CXX_TRACE(logger, "Connecting to the next candidate,"
                    << getPeerId() << ", "
                    << _query->host_name() << ":" << _query->service_name()
                    << "Last error #"
                    << err.value() << "('" << err.message() << "')");
      _error = err;
      _socket.close();
      boost::asio::ip::tcp::endpoint ep = *endpoint_iterator;
      _socket.async_connect(ep,
                            std::bind(&Connection::onConnect,
                                      shared_from_this(),
                                      resolver,
                                      query,
                                      ++endpoint_iterator,
                                      std::placeholders::_1));
      return;
   }

    configConnectedSocket();
    setRemoteIpAndPort();

    LOG4CXX_DEBUG(logger, "Connected to "
                  << getPeerId() << ", "
                  << _query->host_name() << ":"
                  << _query->service_name()
                  << ", conn=" << hex << this << dec);

    // Send instance-to-instance authentication logon and assume that
    // authentication succeeds (if not, peer will disconnect).
    // See SDB-5702.
    _connectionState = AUTH_IN_PROGRESS;
    MessageDescPtr logonMsg = makeLogonMessage();
    // dz 注意，这个onConnect是本端主动 connect remote之后的回调，被动连接不会走到这里来！！ connectAsync从mgr那边调用过来的
    LOG4CXX_DEBUG(logger, "conn onConnect, before send logon on msg, this is " << this << ", self id is " << Iid(_selfInstanceId));
    sendMessage(logonMsg); // dz 他这里直接就发送logon的消息，然后就假定auth成功了，但其实现在连接还没有建立，假定auth成功之后，就能发送消息了
    authDoneInternal();         // Becomes readMessage() if SDB-5702 is solved.
}

void Connection::authDone()
{
    if (::pthread_equal(::pthread_self(), _networkManager.getThreadId())) {
        LOG4CXX_TRACE(logger, "authDone: conn=" << hex << this << dec << ", in netmgr");
        authDoneInternal();
    } else {
        LOG4CXX_TRACE(logger, "authDone: conn=" << hex << this << dec);
        _networkManager.getIOService().post(
            std::bind(&Connection::authDoneInternal, shared_from_this()));
    }
}

void Connection::authDoneInternal()
{
    LOG4CXX_DEBUG(logger, "dz authDoneInternal conn is " << this << ", peer is " << getPeerId());
    if (_connectionState == AUTH_IN_PROGRESS) {
        _connectionState = CONNECTED;

        LOG4CXX_TRACE(logger, "authDoneInternal: conn=" << hex << this << dec
                      << " is now connected");

        _error.clear();
        _query.reset();
        _logConnectErrors = true;

        {
            ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
            _messageQueue.authDone();
        }

        pushNextMessage();
    } else {
        SCIDB_ASSERT(_connectionState == DISCONNECTED);
        LOG4CXX_TRACE(logger, "authDoneInternal: conn=" << hex << this << dec
                      << " disconnected");
    }
}

void Connection::connectAsync(const string& address, uint16_t port)
{
   _networkManager.getIOService().post(std::bind(&Connection::connectAsyncInternal,
                                                 shared_from_this(),
                                                 address,
                                                 port));
}

void Connection::connectAsyncInternal(const string& address, uint16_t port)
{
   if (_connectionState == CONNECTED ||
       _connectionState == CONNECT_IN_PROGRESS) {
      LOG4CXX_WARN(logger, "Already connected/ing! Not Connecting to " << address << ":" << port);
      return;
   }


   disconnectInternal(); // dz todo 连接这个地址，先断开了链接！！！如果一开始没有连接会如何？
   LOG4CXX_DEBUG(logger, "Connecting (async) to " << address << ":" << port);

   //XXX TODO: switch to using scidb::resolveAsync()
//   std::shared_ptr<boost::asio::ip::tcp::resolver>  resolver(new boost::asio::ip::tcp::resolver(_socket.get_io_service()));
    std::shared_ptr<boost::asio::ip::tcp::resolver>  resolver(new boost::asio::ip::tcp::resolver(GET_IO_SERVICE(_socket))); // dz 保证兼容性
   stringstream serviceName;
   serviceName << port;
   _query.reset(new boost::asio::ip::tcp::resolver::query(address, serviceName.str()));
   _error.clear();
   _connectionState = CONNECT_IN_PROGRESS;
   resolver->async_resolve(*_query,
                           std::bind(&Connection::onResolve,
                                     shared_from_this(),
                                     resolver,
                                     _query,
                                     std::placeholders::_1,
                                     std::placeholders::_2));
}

void Connection::attachQuery(
    QueryID queryID,
    ClientContext::DisconnectHandler& dh)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _activeClientQueries[queryID] = dh;
}

void Connection::attachQuery(QueryID queryID)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ClientContext::DisconnectHandler dh;  // is an empty std::function
    attachQuery(queryID, dh);
}

void Connection::detachQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _activeClientQueries.erase(queryID);
}

void Connection::disconnectInternal()
{
   LOG4CXX_DEBUG(logger, "Disconnecting from " << getPeerId() << ", this: " << this);
   _socket.close();
   _connectionState = DISCONNECTED;
   _query.reset();
   _remoteIp = boost::asio::ip::address();
   _remotePort = 0;
   ClientQueries clientQueries;
   {
       ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
       clientQueries.swap(_activeClientQueries);
   }

   LOG4CXX_TRACE(logger, str(boost::format("Number of active client queries %lld") % clientQueries.size()));

   for (ClientQueries::const_iterator i = clientQueries.begin();
        i != clientQueries.end(); ++i)
   {
       assert(_remoteInstanceId == CLIENT_INSTANCE);
       QueryID queryID = i->first;
       const ClientContext::DisconnectHandler& dh = i->second;
       _networkManager.handleClientDisconnect(queryID, dh);
   }

   if (_session && _session->hasCleanup()) {
       LOG4CXX_DEBUG(logger, "dz before mgr handleSessionClose " << this); // TODO dz 加日志
       _networkManager.handleSessionClose(_session);
   }
   // dz todo 释放rdma资源
}

void Connection::disconnect()
{
    _networkManager.getIOService().post(std::bind(&Connection::disconnectInternal,
                                                  shared_from_this()));
}

void Connection::flushThenDisconnect()
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    _flushThenDisconnect = true;
}


void Connection::handleReadError(const boost::system::error_code& error)
{
   assert(error);

   if (error != bae::eof) {
      LOG4CXX_ERROR(logger, "Network error while reading, #"
                    << error << " (" << error.message() << "), this: " << this);
   } else {
      LOG4CXX_TRACE(logger, "Sender disconnected (eof on read)");
   }
   if (_connectionState == CONNECTED || _connectionState == AUTH_IN_PROGRESS) {
       LOG4CXX_DEBUG(logger, "dz handleReadError before disconnectInternal, this: " << this);
       disconnectInternal();
   }
}

Connection::~Connection()
{
//   LOG4CXX_TRACE(logger, "~Destroying connection to " << getPeerId() << ", this is " << this);
    LOG4CXX_DEBUG(logger, "~Destroying connection to " << getPeerId() << ", this is " << this);
   abortMessages();
   disconnectInternal();
}

void Connection::abortMessages()
{
    MultiChannelQueue connQ(_remoteInstanceId, *this);
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
        connQ.swap(_messageQueue);
    }
    // dz trace to debug
    LOG4CXX_DEBUG(logger, "Aborting "<< connQ.size()
                  << " buffered connection messages to "
                  << getPeerId());
   connQ.abortMessages();
}

string Connection::getPeerId() const
{
    stringstream ss;
    ss << Iid(_remoteInstanceId);
    if (boost::asio::ip::address() != _remoteIp) {
       boost::system::error_code ec;
       string ip(_remoteIp.to_string(ec));
       assert(!ec);
       ss << " (" << ip << ':' << _remotePort << ')';
    }
    return ss.str();
}

string Connection::getRemoteEndpointName() const
{
    stringstream ss;
    ss << _remoteIp.to_string() << ':' << _remotePort;
    return ss.str();
}

void Connection::setRemoteIpAndPort()
{
   boost::system::error_code ec;
   boost::asio::ip::tcp::endpoint endpoint = _socket.remote_endpoint(ec);
   if (!ec)
   {
      _remoteIp = endpoint.address();
      _remotePort = endpoint.port();
   }
   else
   {
      LOG4CXX_ERROR(logger,
                    "Could not get the remote IP from connected socket to/from "
                    << getPeerId()
                    << ": " << ec.message() << " (" << ec << ")");
   }
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::_push(NetworkManager::MessageQueueType mqt,
                                     const std::shared_ptr<MessageDesc>& msg,
                                     bool ontoBack)
{
    assert(msg);
    assert(mqt<NetworkManager::mqtMax);

    std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt, _connection);
    }
    bool isActiveBefore = channel->isActive();

    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    status =  ontoBack ? channel->pushBack(msg) : channel->pushUrgent(msg);
    ++_size;

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::popFront(std::shared_ptr<MessageDesc>& msg)
{
    assert(!msg);

    Channel *channel(NULL);
    uint32_t start = _currChannel; // 从当前channel开始
    while (true) {
        ++_currChannel;
        channel = _channels[_currChannel % NetworkManager::mqtMax].get(); // 获取vector中智能指针的原始指针 mqtMax=2，也就是只有2个队列，一个普通队列，一个replica队列
        if ((_currChannel % NetworkManager::mqtMax) == (start % NetworkManager::mqtMax)) {
            break; // 2个队列都遍历了
        }
        if (channel == nullptr) {
            continue;
        }
        if (!channel->isActive()) { // 队列如果流控阻塞了，那么不发送
            continue;
        }
        break;
    }
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (channel && channel->isActive()) {

        status = channel->popFront(msg);
        assert(msg);
        --_size;

        _activeChannelCount -= (!channel->isActive());
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<MessageDesc> Connection::makeControlMessage()
{
    std::shared_ptr<MessageDesc> msgDesc = std::make_shared<MessageDesc>(mtControl);
    assert(msgDesc);

    namespace gpb = google::protobuf;

    std::shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    gpb::uint64 localGenId=0;
    gpb::uint64 remoteGenId=0;
    assert(record);
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);

        localGenId = _messageQueue.getLocalGenId();
        record->set_local_gen_id(localGenId);

        remoteGenId = _messageQueue.getRemoteGenId();
        record->set_remote_gen_id(remoteGenId);

        LOG4CXX_TRACE(logger, "Create mtControl message localGenId=" << localGenId
                      <<", remoteGenId=" << remoteGenId);

        // Gather sequence numbers for flow controlled channels.
        // Recall mqtNone is not flow controlled.
        for (uint32_t mqt = (NetworkManager::mqtNone+1); mqt < NetworkManager::mqtMax; ++mqt) {
            const gpb::uint64 localSn   = _messageQueue.getLocalSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint64 remoteSn  = _messageQueue.getRemoteSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint32 id        = mqt;
            scidb_msg::Control_Channel* entry = record->add_channels();
            assert(entry);
            entry->set_id(id);
            entry->set_local_sn(localSn);
            entry->set_remote_sn(remoteSn);
        }
    }

    gpb::RepeatedPtrField<scidb_msg::Control_Channel>* entries = record->mutable_channels();
    for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::iterator iter = entries->begin();
        iter != entries->end(); ++iter) {
        scidb_msg::Control_Channel& entry = (*iter);
        assert(entry.has_id());
        const NetworkManager::MessageQueueType mqt = static_cast<NetworkManager::MessageQueueType>(entry.id());
        const gpb::uint64 available =
            _networkManager.getAvailable(NetworkManager::MessageQueueType(mqt), _remoteInstanceId);
        entry.set_available(available);
    }

    if (logger->isTraceEnabled()) {
        const gpb::RepeatedPtrField<scidb_msg::Control_Channel>& channels = record->channels();
        for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = channels.begin();
            iter != channels.end(); ++iter) {
            const scidb_msg::Control_Channel& entry = (*iter);
            const NetworkManager::MessageQueueType mqt =
                static_cast<NetworkManager::MessageQueueType>(entry.id());
            const uint64_t available    = entry.available();
            const uint64_t remoteSn     = entry.remote_sn();
            const uint64_t localSn      = entry.local_sn();

            LOG4CXX_TRACE(logger, "FlowCtl: SND iid=" << Iid(_remoteInstanceId)
                          << " avail=" << available
                          << " mqt=" << mqt
                          << " lclseq=" << localSn
                          << " rmtseq=" << remoteSn
                          << " lclgen=" << localGenId
                          << " rmtgen=" << remoteGenId);
        }
    }
    return msgDesc;
}

std::shared_ptr<MessageDesc> Connection::makeLogonMessage()
{
    std::shared_ptr<MessageDesc> result = std::make_shared<MessageDesc>(mtAuthLogon);
    std::shared_ptr<scidb_msg::AuthLogon> record = result->getRecord<scidb_msg::AuthLogon>();
    record->set_username(Iid(_selfInstanceId).str());
    record->set_priority(SessionProperties::NORMAL);
    record->set_authtag(auth::strMethodTag(AUTH_I2I)); // 注意, 这里有AUTH_I2I消息

    // In the future, something more challenging; for now, just the
    // cluster uuid.
    record->set_signature(Cluster::getInstance()->getUuid());

    return result;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::setRemoteState(NetworkManager::MessageQueueType mqt,
                                              uint64_t rSize,
                                              uint64_t localGenId, uint64_t remoteGenId,
                                              uint64_t localSn, uint64_t remoteSn)
{
    // XXX TODO: consider turning asserts into exceptions
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (mqt>=NetworkManager::mqtMax) {
        assert(false);
        return status;
    }
    if (remoteGenId < _remoteGenId) { // 远端的id小于本地存储的远端id，就有问题了，说明这个消息过时了
        assert(false); // TODO 2023年2月22日 17:19:21 在这里crash了
        return status;
    }
    if (localGenId > _localGenId) { // 对方发送给我的我这边的id，比当前我的id还大，那肯定也是有问题的
        assert(false);
        return status;
    }
    if (localGenId < _localGenId) {
        localSn = 0;
    }

    std::shared_ptr<Channel>& channel = _channels[mqt]; // channels是个vector
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt, _connection);
    }
    if (!channel->validateRemoteState(rSize, localSn, remoteSn)) {
        assert(false);
        return status;
    }
    if (remoteGenId > _remoteGenId) {
        _remoteGenId = remoteGenId; // 更新远端id
    }
    bool isActiveBefore = channel->isActive();

    status = channel->setRemoteState(rSize, localSn, remoteSn);

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

uint64_t
Connection::MultiChannelQueue::getAvailable(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return NetworkManager::MAX_QUEUE_SIZE;
    }
    return channel->getAvailable();
}

uint64_t
Connection::MultiChannelQueue::getLocalSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getLocalSeqNum();
}

uint64_t
Connection::MultiChannelQueue::getRemoteSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getRemoteSeqNum();
}

void
Connection::MultiChannelQueue::authDone()
{
    size_t numActive = 0;
    for (auto& channel : _channels) {
        if (channel) {
            channel->authDone();
            numActive += channel->isActive();
        }
    }
    _activeChannelCount = numActive;

    SCIDB_ASSERT(bool(numActive) == isActive());
    SCIDB_ASSERT(_activeChannelCount<=NetworkManager::mqtMax);
}

void
Connection::MultiChannelQueue::abortMessages()
{
    for (auto& channel : _channels) {
        if (channel) {
            channel->abortMessages();
        }
    }
    _activeChannelCount = 0;
    _size = 0;
}

void
Connection::MultiChannelQueue::swap(MultiChannelQueue& other)
{
    // No swapping across Connections!
    SCIDB_ASSERT(&_connection == &other._connection);

    const InstanceID instanceId = _instanceId;
    _instanceId = other._instanceId;
    other._instanceId = instanceId;

    const uint32_t currMqt = _currChannel;
    _currChannel = other._currChannel;
    other._currChannel = currMqt;

    const size_t activeCount = _activeChannelCount;
    _activeChannelCount = other._activeChannelCount;
    other._activeChannelCount = activeCount;

    const uint64_t size = _size;
    _size = other._size;
    other._size = size;

    const uint64_t remoteGenId = _remoteGenId;
    _remoteGenId = other._remoteGenId;
    other._remoteGenId = remoteGenId;

    const uint64_t localGenId = _localGenId;
    _localGenId = other._localGenId;
    other._localGenId = localGenId;

    _channels.swap(other._channels);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::pushBack(const std::shared_ptr<MessageDesc>& msg)
{
    if ( !_msgQ.empty() &&  msg->getMessageType() == mtAlive) {
        // mtAlive are useful only if there is no other traffic
        assert(_mqt == NetworkManager::mqtNone);
        return  std::shared_ptr<NetworkManager::ConnectionStatus>();
    }
    const uint64_t spaceBefore = getAvailable();
    if (spaceBefore == 0) {
        NetworkManager::OverflowException ex(REL_FILE, __FUNCTION__, __LINE__);
        NetworkManager::setQueueType(ex, _mqt);
        throw ex;
    }
    _msgQ.push_back(msg);
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::pushUrgent(const std::shared_ptr<MessageDesc>& msg)
{
    // The only messages that get pushed on the front are the
    // (high-priority) authentication messages.
    SCIDB_ASSERT(isAuthMessage(msg->getMessageType()));

    const uint64_t spaceBefore = getAvailable();

    // Push even if no spaceBefore, auth messages disregard _sendQueueLimit.
    // Keep auth messages in FIFO order.
    auto pos = _msgQ.begin();
    while (pos != _msgQ.end() && isAuthMessage((*pos)->getMessageType())) {
        ++pos;
    }
    _msgQ.insert(pos, msg);

    const uint64_t spaceAfter = getAvailable();

    return getNewStatus(spaceBefore, spaceAfter);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::popFront(std::shared_ptr<MessageDesc>& msg)
{
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (!isActive()) {
        msg.reset();
        return status;
    }
    const uint64_t spaceBefore = getAvailable(); // 获取队列中的剩余空间大小，用limit减去当前队列大小即可
    msg = _msgQ.front();
    _msgQ.pop_front(); // 从队头弹出
    ++_localSeqNum; // 本地序列号+1
    const uint64_t spaceAfter = getAvailable();
    status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "popFront: Channel "<< _mqt
                  << " to " << Iid(_instanceId) << " "
                  << ((isActive()) ? "ACTIVE" : "BLOCKED"));

    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::setRemoteState(uint64_t rSize, uint64_t localSn, uint64_t remoteSn)
{
   const uint64_t spaceBefore = getAvailable();
    _remoteSize = rSize;
    _remoteSeqNum = remoteSn;
    _localSeqNumOnPeer = localSn;
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "setRemoteState: Channel "<< _mqt
                  << " to " << Iid(_instanceId)
                  << ", remoteSize="<<_remoteSize
                  << ", remoteSeqNum="<<_remoteSeqNum
                  << ", remoteSeqNumOnPeer="<<_localSeqNumOnPeer);
    return status;
}

void
Connection::Channel::abortMessages()
{
    MessageQueue mQ;
    mQ.swap(_msgQ);
    LOG4CXX_TRACE(logger, "abortMessages: Aborting "<< mQ.size()
                  << " buffered connection messages to "
                  << Iid(_instanceId));
    std::set<QueryID> queries;
    for (MessageQueue::iterator iter = mQ.begin();
         iter != mQ.end(); ++iter) {
        std::shared_ptr<MessageDesc>& messageDesc = (*iter);
        queries.insert(messageDesc->getQueryID());
    }
    mQ.clear();
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    boost::system::error_code aborted = bae::operation_aborted;
    networkManager->handleConnectionError(_instanceId, queries, aborted);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::getNewStatus(const uint64_t spaceBefore,
                                  const uint64_t spaceAfter)
{
    if ((spaceBefore != spaceAfter) && (spaceBefore == 0 || spaceAfter == 0)) {
        return std::make_shared<NetworkManager::ConnectionStatus>(_instanceId, _mqt, spaceAfter);
    }
    return std::shared_ptr<NetworkManager::ConnectionStatus>();
}

uint64_t
Connection::Channel::getAvailable() const // 队列中还有多少剩余空间存放消息
{
    const uint64_t localLimit = _sendQueueLimit;
    const uint64_t localSize  = _msgQ.size();

    if (localSize >= localLimit) {
        return 0;
    }
    return localLimit - localSize;
}

void
Connection::setAuthenticator(std::shared_ptr<Authenticator>& author)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    ASSERT_EXCEPTION(!_session, "Connection already authenticated?!");
    if (_authenticator) {
        throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_AUTHENTICATION_ERROR)
            << "Multiple logins on one connection";
    }
    _authenticator = author;
}

void
Connection::setSession(std::shared_ptr<Session>& sess)
{
    ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
    LOG4CXX_DEBUG(logger, "dz setSession this conn is " << this); // TODO dz 加日志
    ASSERT_EXCEPTION(_authenticator, "No authenticator, so who's calling me?");
    ASSERT_EXCEPTION(!_session, "Already authenticated?!");
    _authenticator.reset();
    _session = sess;
}

} // namespace
