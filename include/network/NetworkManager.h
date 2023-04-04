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
 * NetworkManager.h
 *      Author: roman.simakov@gmail.com
 *      Description: NetworkManager class provides high-level API
 *      for message exchanging and also register instance in system catalog.
 */

#ifndef NETWORKMANAGER_H_
#define NETWORKMANAGER_H_

#include <network/NetworkMessageFactory.h>
#include <network/ClientMessageDescription.h>
#include <network/ClientContext.h>

#include <boost/asio.hpp>
#include <log4cxx/logger.h>

#include <array/SharedBuffer.h>             // (grr) only due to inline use

#include <monitor/MonitorConfig.h>
#include <system/Cluster.h>
#include <util/InjectedError.h>
#include <util/JobQueue.h>
#include <network/NetworkMessage.h>
#include <util/Singleton.h>
#include <util/WorkQueue.h>

static const time_t DEFAULT_LIVENESS_HANDLE_TIMEOUT = 60; //sec
static const time_t DEFAULT_ALIVE_TIMEOUT_MICRO = 5*1000*1000; // 5sec
static const time_t CONTROL_MSG_TIMEOUT_MICRO = 100*1000; // 100 msec
static const time_t CONTROL_MSG_TIME_STEP_MICRO = 10*1000; // 10 msec

namespace scidb
{

class Connection;
class MessageDesc;
class Session;
class ThrottledScheduler;

/***
 * The network manager implementation depends on system catalog API
 * and storage manager API. It will register itself online in system catalog and read instance number from
 * storage manager. It's necessary since storage content will be described in system
 * catalog with instance number and its content must be related to instance number.
 * So the best place to store instance number is a storage manager. By reading
 * this value scidb instance can find itself in system catalog and register itself
 * as online. In other words instance number is a local data part number. Registering
 * instance in system catalog online means pointing where data part with given number is.
 *
 * @note As a naming convention, member functions by default interact with logical instances.
 *       A function that interacts with physical instances should have a name that ends with 'Physical'.
 *       E.g. send() takes as input a logical instanceID, and sendPhysical() takes as input a physical instanceID.
 * @note Operator code typically interacts with logical instances.
 *
 */
class NetworkManager: public Singleton<NetworkManager>, public InjectedErrorListener
{
 public:
    /**
     * Queue types logically partition the send/receive
     * buffer(queue) space for the flow control purposes. // 用于流控
     * The back pressure is performed separately for each type.
     * The purpose for dividing the buffer space into
     * independent pools is to avoid resource starvation when running
     * multi-phase algorithms. For example, replication is the last
     * phase in a query involving a storing operator. The buffer space
     * used for replication must not be used by any other network
     * activity in the query to guarantee progress. Other multi-phase
     * algorithms can also use the same mechanism to divide resources
     * among the phases if desired. mqtNone does not trigger any flow
     * control mechanisms and is the default queue type.
     */
    typedef enum
    {
        mqtNone=0, // 默认的消息队列类型，不会触发流控机制
        mqtReplication,
        mqtMax //end marker // 注意这个可以用来初始化队列的个数，比如这里是2，队列就是2个
    } MessageQueueType;

    uint16_t getPort() const
    {
        return _acceptor.local_endpoint().port(); // 这个port是boost acceptor确定的监听端口
    }

    static const uint64_t MAX_QUEUE_SIZE = ~0;

 private:

    friend class Cluster;
    friend class Connection;
    friend class RdmaCommManager; // dz add

    // Service of boost asio
    boost::asio::io_service _ioService;

    // Acceptor of incoming connections
    boost::asio::ip::tcp::acceptor _acceptor;

    boost::asio::posix::stream_descriptor _input;
    char one_byte_buffer;

    // A timer to check connections for alive
    boost::asio::deadline_timer _aliveTimer;
    time_t _aliveTimeout;

    // InstanceID of this instance of manager
    InstanceID _selfInstanceID;

    // Thread running the boost::asio event loop
    pthread_t _selfThreadID;

    /**
     *  A pool of connections to other instances.
     */
    typedef std::map<InstanceID, std::shared_ptr<Connection> > ConnectionMap;
    ConnectionMap _outConnections;

    typedef std::map<InstanceID, uint64_t> ConnectionGenMap;
    /// instance ID <-> generation ID used to maintain only one connection per remote instance
    ConnectionGenMap _inConnections; // dz 这个感觉是用来排序用的，只保留一个节点到节点的连接，旧连接收到的消息在handle message的时候会忽略

    ConnectionMap _inSharedConns; // dz 新增，用来接收远端instance消息之后，分发消息，里面保存有session，in是专门用来接收消息用的，不会发送消息
    uint64_t _currConnGenId;
    /// @note NOT thread-safe
    uint64_t getNextConnGenId() { return ++_currConnGenId; }

    // Job queue for processing received messages
    std::shared_ptr< JobQueue> _jobQueue;

    // Current cluster instance membership
    InstMembershipPtr _instanceMembership;

    // Current cluster instance liveness,
    // which may not be in sync with the membership
    // (i.e. may have a different membership ID) at any given time.
    // Instance addition/removal can cause the two to deviate temporarily.
    InstLivenessPtr _instanceLiveness;

    /// A functor class that sends a given message to a given instance
    class MessageSender
    {
    public:
        MessageSender(NetworkManager& nm,
                      std::shared_ptr<MessageDesc>& messageDesc,
                      InstanceID selfInstanceID)
        :  _nm(nm), _messageDesc(messageDesc), _selfInstanceID(selfInstanceID)
        {}
        void operator() (const InstanceDesc& i);

    private:
        NetworkManager& _nm;
        std::shared_ptr<MessageDesc>& _messageDesc;
        InstanceID _selfInstanceID;
    };

    friend class MessageSender;

    Mutex _mutex;

    static volatile bool _shutdown;

    // A timer to handle dead instances
    std::shared_ptr<ThrottledScheduler> _livenessHandleScheduler;

//    static const time_t DEFAULT_LIVENESS_HANDLE_TIMEOUT = 60; //sec
//    static const time_t DEFAULT_ALIVE_TIMEOUT_MICRO = 5*1000*1000; // 5sec
//    static const time_t CONTROL_MSG_TIMEOUT_MICRO = 100*1000; // 100 msec
//    static const time_t CONTROL_MSG_TIME_STEP_MICRO = 10*1000; // 10 msec

    uint64_t _repMessageCount;
    uint64_t _maxRepSendQSize;
    uint64_t _maxRepReceiveQSize;

    uint64_t _randInstanceIndx;
    uint64_t _aliveRequestCount;

    uint64_t _memUsage;

    class DefaultMessageDescription : virtual public ClientMessageDescription
    {
    public:

    DefaultMessageDescription( InstanceID instanceID,
                               MessageID msgID,
                               MessagePtr msgRec,
                               std::shared_ptr<SharedBuffer> bin,
                               QueryID qId )
    : _instanceID(instanceID),
        _msgID(msgID),
        _msgRecord(msgRec),
        _binary(bin),
        _queryId(qId)
    {
        SCIDB_ASSERT(msgRec.get());
    }

    DefaultMessageDescription( const std::shared_ptr<ClientContext>& clientCtx,
                               MessageID msgID,
                               MessagePtr msgRec,
                               std::shared_ptr<SharedBuffer> bin,
                               QueryID qId )
    : _instanceID(CLIENT_INSTANCE),
        _clientCtx(clientCtx),
        _msgID(msgID),
        _msgRecord(msgRec),
        _binary(bin),
        _queryId(qId)
        {
            SCIDB_ASSERT(clientCtx);
            SCIDB_ASSERT(msgRec.get());
        }

        virtual InstanceID getSourceInstanceID() const   {return _instanceID; }
        virtual MessagePtr getRecord()                   {return _msgRecord; }
        virtual MessageID getMessageType() const         {return _msgID; }
        virtual QueryID getQueryId() const             {return _queryId; }
        virtual ClientContext::Ptr getClientContext()    {return _clientCtx; }

        virtual boost::asio::const_buffer getBinary()
        {
            if (_binary) {
                return boost::asio::const_buffer(_binary->getConstData(), _binary->getSize());
            }
            return boost::asio::const_buffer(NULL, 0);
        }

        virtual std::shared_ptr<scidb::SharedBuffer> getMutableBinary() override { return _binary; }
        virtual ~DefaultMessageDescription() {}

    private:
        DefaultMessageDescription();
        DefaultMessageDescription(const DefaultMessageDescription&);
        DefaultMessageDescription& operator=(const DefaultMessageDescription&);
        InstanceID _instanceID;
        ClientContext::Ptr _clientCtx;
        MessageID _msgID;
        MessagePtr _msgRecord;
        std::shared_ptr<SharedBuffer> _binary;
        QueryID _queryId;
    };

    class DefaultNetworkMessageFactory : public NetworkMessageFactory
    {
    public:
       DefaultNetworkMessageFactory() {}
       virtual ~DefaultNetworkMessageFactory() {}
       bool isRegistered(const MessageID& msgId) override ;
       bool addMessageType(const MessageID& msgId,
                           const NetworkMessageFactory::MessageCreator& msgCreator,
                           const NetworkMessageFactory::MessageHandler& msgHandler) override ;
       bool removeMessageType(const MessageID& msgId) override ;
       MessagePtr createMessage(const MessageID& msgId) override ;
       NetworkMessageFactory::MessageHandler getMessageHandler(const MessageID& msgId) override ;
    private:
       DefaultNetworkMessageFactory(const DefaultNetworkMessageFactory&);
       DefaultNetworkMessageFactory& operator=(const DefaultNetworkMessageFactory&);

       typedef std::map<MessageID,
                        std::pair<NetworkMessageFactory::MessageCreator,
                                  NetworkMessageFactory::MessageHandler> >  MessageHandlerMap;
       MessageHandlerMap _msgHandlers;
       Mutex _mutex;
    };

    std::shared_ptr<DefaultNetworkMessageFactory>  _msgHandlerFactory;
    std::shared_ptr<WorkQueue> _workQueue;
    std::shared_ptr<WorkQueue> _requestQueue;
    std::shared_ptr<WorkQueue> _adminRequestQueue;

    /// Store the ip address as a string on behalf of the resource monitor
    std::string     _ipAddress;

    void startAccept();
    void handleAccept(std::shared_ptr<Connection>& newConnection,
                      const boost::system::error_code& error);

    void startInputWatcher();
    void handleInput(const boost::system::error_code& error, size_t bytes_transferr);

    Notification<InstanceLiveness>::SubscriberID _livenessSubscriberID;
    void _handleAlive(const boost::system::error_code& error);
    static void handleAlive(const boost::system::error_code& error)
    {
        getInstance()->_handleAlive(error);
    }

    /**
     * Immediately run & reschedule the next "alive" broadcast
     * if the current broadcast period is smaller than the given one
     * @param timeoutMicro set _aliveTimout to this value
     */
    void scheduleAliveNoLater(const time_t timeoutMicro);

    /**
     * internal use only
     */
    void handleControlMessage(const std::shared_ptr<MessageDesc>& msgDesc);

    void _sendPhysical(InstanceID physicalInstanceID,
                       std::shared_ptr<MessageDesc>& messageDesc,
                       MessageQueueType flowControlType = mqtNone);

    static void handleLivenessNotification(std::shared_ptr<const InstanceLiveness> liveInfo)
    {
       getInstance()->_handleLivenessNotification(liveInfo);
    }

    void _handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& liveInfo);

    static void handleLiveness()
    {
       getInstance()->_handleLiveness();
    }

    void _handleLiveness();

   /**
    * @param remote instance ID
    * @param [in/out] becauseNotMember is defined only when it is set to false on input
    * and the return value is true. In that case it indicates that the instance is not a member.
    * It may be useful to distinguish a dead member from a non-memeber.
    * @return true if the remote instance is either dead or not in the membership at all;
    *         otherwise false and becauseNotMember is undefined
    * @note may update the current cluster membership
    */
    bool isDead(InstanceID remoteInstanceId,  bool& becauseNotMember);

    static void publishMessage(const std::shared_ptr<MessageDescription>& msgDesc);

    /// Logic for dispatching network messages
    /// @throw scidb::Exception (message specific)
    /// @throw WorkQueue::OverflowException
    /// @throw std::bad_alloc
    void dispatchMessage(const std::shared_ptr< Connection >& connection,
                         const std::shared_ptr<MessageDesc>& messageDesc);

    /// Helper that notifies listeners about a given message
    void dispatchMessageToListener(const std::shared_ptr<Connection>& connection,
                                   const std::shared_ptr<MessageDesc>& messageDesc,
                                   NetworkMessageFactory::MessageHandler& handler);
    /// Helper that notifies listeners about a message indicating
    // a terminated connection from a given instance
    void dispatchErrorToListener(const InstanceID remoteInstanceId);

    /// Helper that notifies a given query of a connection error
    void handleConnectionError(const InstanceID remoteInstanceId, const QueryID& queryId,
                               const boost::system::error_code& error);

    /// Helper that notifies a given query of a connection error
    void handleConnectionError(const InstanceID remoteInstanceId, const QueryID& queryId,
                               const Exception& ex);

    /// Helper that notifies a set of queries of a connection error
    void handleConnectionError(const InstanceID remoteInstanceId,
                               const std::set<QueryID>& queries,
                               const boost::system::error_code& error);

    /// Helper that notifies all of the current queries of a connection error
    void handleConnectionErrorToLive(const InstanceID remoteInstanceId);

    /**
     * Helper that does per-query cleanup when a client disconnects.
     * @param queryId the id of a query associated with a client connection
     * @param dh the disconnect handler if any registered by the user
     */
    void handleClientDisconnect(const QueryID& queryId,
                                const ClientContext::DisconnectHandler& dh);

    /**
     * @brief Helper that enqueues session cleanup when a client disconnects.
     *
     * @param sessp shared pointer to session object
     *
     * @details On client disconnect, session close activity needs to
     * happen on a work queue (rather than in the NetworkManager thread)
     * because it may block.  For example, PAM session tear-down may
     * involve communication with a remote authentication service.
     */
    void handleSessionClose(std::shared_ptr<Session>& sessp);

    /**
     * Handle the messages which are not generated by the SciDB engine proper
     * but registered with NetworkMessageFactory
     * @param messageDesc message description
     * @param handler gets assigned if the return value is true
     * @return true if the message is handled, false if it is a system message
     */
    bool handleNonSystemMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                                NetworkMessageFactory::MessageHandler& handler);

    /**
     * Request the latest information about the member  instances from system catalog.
     */
    void fetchLatestInstances();

    /**
     * Make sure the information about the member instances is cached.
     */
    void fetchInstances();

    /**
     * Get currently known instance liveness
     * @return liveness or NULL if not yet known
     * @see scidb::Cluster::getInstanceLiveness()
     */
    std::shared_ptr<const InstanceLiveness> getInstanceLiveness()
    {
       ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
       fetchInstances();
       return _instanceLiveness;
    }

    InstanceID getPhysicalInstanceID()
    {
       ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);
       return _selfInstanceID;
    }

    void reconnect(InstanceID instanceID);
    void handleShutdown();
    uint64_t getAvailableRepSlots();
    bool isBufferSpaceLow();
    uint64_t _getAvailable(MessageQueueType mqt, InstanceID forInstanceID);
    void _broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc);
    bool _generateConnErrorOnCommitOrAbort(const std::shared_ptr<MessageDesc>& msg);

protected:
    /// Constructor
    NetworkManager();

    /// Destructor
    ~NetworkManager();
    friend class Singleton<NetworkManager>;

public:

    uint64_t getUsedMemSize() const
    {
        // not synchronized, relying on 8byte atomic load
        return _memUsage;
    }

    /**
     * Retrieve the ip address for this network manager
     * @param ipAddress - the variable to receive the ip address
     */
    void getIpAddress(std::string &ipAddress)
    {
        ipAddress = _ipAddress;
    }

    /** Retrieve thread id of run thread */
    pthread_t getThreadId() const { return _selfThreadID; }

    /**
     *  This method send asynchronous message to a physical instance.
     *  @param physicalInstanceID is a instance number for sending message to.
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *  @param mqt the queue to use for this message
     */
    void sendPhysical(InstanceID physicalInstanceID,
                      std::shared_ptr<MessageDesc>& messageDesc,
                      MessageQueueType mqt = mqtNone);

    /**
     *  This method sends out an asynchronous message to every logical instance
     *  except this instance (using per-query instance ID maps)
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     */
    void broadcastLogical(std::shared_ptr<MessageDesc>& messageDesc);

    /**
     *  This method sends out asynchronous message to every physical instance except this instance
     *  @param MessageDesc contains Google Protocol Buffer message and octet data.
     *
     */
    void broadcastPhysical(std::shared_ptr<MessageDesc>& messageDesc);

    /// Send a given message to a set of instances returned by a given functor
    /// when invoked on the elements in a range specified by the iterators begin & end.
    /// @note If an instance ID is not in the current membership, the message to that instance
    /// is dropped and the query associated with the message (if any) is notified.
    /// @note The message is NOT delivered to this instance.
    /// @param begin iterator pointing to the beginning of a range
    /// @param end iterator pointing to the end of the range
    /// @param func functor producing a physical instance ID based on an iterator value
    /// @param messageDesc message to send
    template<class Iterator, class InstanceIdFunc>
    void multicastPhysical(Iterator begin, Iterator end, InstanceIdFunc& func,
                           std::shared_ptr<MessageDesc>& messageDesc)
    {
        ScopedMutexLock mutexLock(_mutex, PTW_SML_NM);

        fetchInstances();

        for (; begin != end; ++begin) {
            const InstanceID physId = func(*begin);
            if (physId != _selfInstanceID) {
                _sendPhysical(physId, messageDesc);
            }
        }
    }

    /// This method handle messages received by connections. Called by Connection class
    void handleMessage(std::shared_ptr< Connection >& connection,
                       const std::shared_ptr<MessageDesc>& messageDesc);

    /// This method block own thread
    void run(std::shared_ptr<JobQueue> jobQueue,
             InstanceID instanceID,
             const std::string& address);

    /**
     * @return a queue suitable for running tasks that can always make progress
     * i.e. dont *wait* for any conditions messages from remote instances or anything of that sort
     * @note the progress requirement is to ensure there is no deadlock
     * @note No new threads are created as a result of adding work to the queue
     */
    std::shared_ptr<WorkQueue> getWorkQueue()
    {
        return _workQueue;
    }

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can *wait*
     * for any conditions or messages from remote instances.
     * @note HOWEVER, these conditions must be satisfied by jobs/work items NOT executed on this queue (to avoid deadlock).
     * @note No new threads are created as a result of adding work to the queue
     */
    std::shared_ptr<WorkQueue> getRequestQueue(int p = 0);

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can always make progress
     * @note the progress requirement is to ensure there is no deadlock
     * @note No new threads are created as a result of adding work to the queue
     *       The items on this queue may not out-live the queue itself, i.e.
     *       once the queue is destroyed an unspecified number of elements may not get to run.
     */
    std::shared_ptr<WorkQueue> createWorkQueue(const std::string& name)
    {
        return std::make_shared<WorkQueue>(_jobQueue, name);
    }

    /**
     * Intended for internal use only.
     * @return a queue suitable for running tasks that can always make progress
     * @note No new threads are created as a result of adding work to the queue
     *       The items on this queue will not out-live the queue itself, i.e.
     *       once the queue is destroyed an unspecified number of elements may not get to run.
     * @param maxOutstanding max number of concurrent work items (the actual number of threads may be fewer)
     * @param maxSize max queue length including maxOutstanding
     */
    std::shared_ptr<WorkQueue> createWorkQueue(const std::string& name, uint32_t maxOutstanding, uint32_t maxSize)
    {
        return std::make_shared<WorkQueue>(_jobQueue, name, maxOutstanding, maxSize);
    }

    /// Register incoming message for control flow purposes against a given queue
    void registerMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                         MessageQueueType mqt);
    /// Unregister incoming message for control flow purposes against a given queue when it is deallocated
    void unregisterMessage(const std::shared_ptr<MessageDesc>& messageDesc,
                           MessageQueueType mqt);

    /**
     * Get available receive buffer space for a given channel
     * This is the amount advertised to the sender to keep it from
     * overflowing the receiver's buffers
     * (currently the accounting is done in terms of the number of messages rather than bytes
     *  and no memory is pre-allocated for the messages)
     * @param mqt the channel ID
     * @param forInstanceID the ID of the destination
     * @return number of messages the receiver is willing to accept currently (per sender)
     */
    uint64_t getAvailable(MessageQueueType mqt, InstanceID forInstanceID);

    /// internal
    uint64_t getSendQueueLimit(MessageQueueType mqt);
    uint64_t getReceiveQueueHint(MessageQueueType mqt);

    boost::asio::io_service& getIOService()
    {
        return _ioService;
    }

    // Network Interface for operators

    void send(InstanceID logicalTargetID,
              std::shared_ptr<MessageDesc>& msg);

    void send(InstanceID logicalTargetID,
              std::shared_ptr<MessageDesc>& msg,
              std::shared_ptr<Query>& query);

    void receive(InstanceID logicalSourceID,
                 std::shared_ptr<MessageDesc>& msg,
                 std::shared_ptr<Query>& query);

    /**
     * Send a message to the local instance (from the local instance) on behlf of a given query
     * @throw NetworkManager::OverflowException currently not thrown, but is present for API completeness
     */
    void sendLocal(const std::shared_ptr<Query>& query,
                   const std::shared_ptr<MessageDesc>& messageDesc);
    /**
     * Send a message to the local instance (from the local instance)
     * @throw NetworkManager::OverflowException
     */
    void sendLocal(const std::shared_ptr<MessageDesc>& messageDesc);

    // MPI-like functions
    void send(InstanceID logicalTargetID,
              std::shared_ptr< SharedBuffer> const& data,
              std::shared_ptr<Query>& query);

    std::shared_ptr<SharedBuffer> receive(InstanceID logicalSourceID,
                                          std::shared_ptr<Query>& query);

    static void shutdown()
    {
       _shutdown = true;
    }
    static bool isShutdown()
    {
       return _shutdown;
    }

    /**
     * Get a factory to register new network messages and
     * to retrieve already registered message handlers.
     * @return the message factory
     */
    std::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
    {
       std::shared_ptr<NetworkMessageFactory> ptr(_msgHandlerFactory);
       return ptr;
    }

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(OverflowException,
                                      SCIDB_SE_NO_MEMORY,
                                      SCIDB_LE_NETWORK_QUEUE_FULL);

    /// Use Exception::setMemo() to mark the exception with the queue type.
    static void setQueueType(OverflowException& e, MessageQueueType mqt)
    {
        e.setMemo(mqt);
    }

    /// Use Exception::getMemo() to retrieve the queue type from the exception.
    static MessageQueueType getQueueType(OverflowException const& e)
    {
        return boost::any_cast<MessageQueueType>(e.getMemo());
    }

    /**
     * A notification class that reports changes in a connection status
     * Receivers can subscribe to this notification via scidb::Notification<ConnectionStatus>
     * 连接的状态类
     */
    class ConnectionStatus
    {
    public:
    ConnectionStatus(InstanceID instanceId, MessageQueueType mqt, uint64_t queueSize)
    : _instanceId(instanceId), _queueType(mqt), _queueSize(queueSize)
        {
            SCIDB_ASSERT(instanceId != INVALID_INSTANCE);
        }
        ~ConnectionStatus() {}
        InstanceID getPhysicalInstanceId() const { return _instanceId; }
        uint64_t getAvailableQueueSize() const { return _queueSize; }
        MessageQueueType getQueueType() const { return _queueType;}
    private:
        ConnectionStatus(const ConnectionStatus&);
        ConnectionStatus& operator=(const ConnectionStatus&);
        InstanceID _instanceId;
        MessageQueueType _queueType;
        uint64_t _queueSize;
    };
};


} //namespace


#endif /* NETWORKMANAGER_H_ */
