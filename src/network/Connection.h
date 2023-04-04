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
 * Connection.h
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <network/BaseConnection.h>
#include <network/ClientContext.h>

#include <deque>
#include <map>
#include <stdint.h>
#include <boost/asio.hpp>
#include <memory>
#include <unordered_map>

#include <util/Mutex.h>
#include <network/BaseConnection.h>
#include <network/MessageDesc.h>
#include <network/NetworkManager.h>

// dz rdma相关头文件
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>


namespace scidb
{
    class Authenticator;
    class Session;

/**
 * Class for connect to asynchronous message exchanging between
 * network managers. It is used by network manager itself for sending message
 * to another instance and by client to connect to scidb instance. // 用于节点间异步传输消息的，以及client和实例间的消息传输
 *
 * @note
 * All operations are executed on the io_service::run() thread.
 * If/when multiple threads ever execute io_service::run(), io_service::strand
 * should/can be used to serialize this class execution.
 */
    class Connection
        : virtual public ClientContext,
          private BaseConnection,
          public std::enable_shared_from_this<Connection>
    {
    private:

        /**
         * A FIFO message stream/channel. It is associated/identified
         * with/by a value of type NetworkManager::MessageQueueType
         */ // 先进先出的队列
        class Channel
        {
        public:
            Channel(InstanceID instanceId,
                    NetworkManager::MessageQueueType mqt, // 有普通的消息，不会触发流控
                    Connection const& conn)
                : _instanceId(instanceId)
                , _mqt(mqt)
                , _remoteSize(0)
                , _localSeqNum(0)
                , _remoteSeqNum(0)
                , _localSeqNumOnPeer(0)
                , _sendQueueLimit(1)
                , _authenticating(!conn.isConnected())
            {
                assert(mqt < NetworkManager::mqtMax);
                NetworkManager* networkManager = NetworkManager::getInstance();
                assert(networkManager);
                _sendQueueLimit = networkManager->getSendQueueLimit(mqt);
                _sendQueueLimit = (_sendQueueLimit>1) ? _sendQueueLimit : 1;
                _remoteSize = networkManager->getReceiveQueueHint(mqt);
            }

            ~Channel() {}

            /**
             * Push a message into the tail end of the channel
             *
             * @param msg message to insert
             * @throws NetworkManager::OverflowException if there is not enough
             *         space on either sender or receiver side
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             */
            std::shared_ptr<NetworkManager::ConnectionStatus> pushBack(const std::shared_ptr<MessageDesc>& msg);

            /**
             * Add an authentication message onto the head of the channel.
             *
             * @param msg authentication message to insert
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             *
             * @note Does not throw OverflowException, since this is only called
             *       for authentication handshake messages which *must* get
             *       transmitted regardless of flow control considerations.
             */
            std::shared_ptr<NetworkManager::ConnectionStatus> pushUrgent(const std::shared_ptr<MessageDesc>& msg);

            /**
             * Pop the next available message (if any) from the channel
             *
             * @param msg next dequeued message, which can be empty if the
             *            channel is empty or if the receiver is out of space
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             */
            std::shared_ptr<NetworkManager::ConnectionStatus> popFront(std::shared_ptr<MessageDesc>& msg);

            /**
             * Set the available channel space on the receiver
             *
             * @param rSize remote queue size (in number of messages for now)
             * @param localSeqNum the last sequence number generated by this instance as observed by the peer
             * @param remoteSeqNum the last sequence number generated by the peer (as observed by this instance)
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             */
            std::shared_ptr<NetworkManager::ConnectionStatus> setRemoteState(uint64_t remoteSize,
                                                                               uint64_t localSeqNum,
                                                                               uint64_t remoteSeqNum);
            /**
             * Validate the information received from the peer
             *
             * @param rSize remote queue size (in number of messages for now)
             * @param localSeqNum the last sequence number generated by this instance as observed by the peer
             * @param remoteSeqNum the last sequence number generated by the peer (as observed by this instance)
             * @return true if peer's information is consistent with the local information; false otherwise
             * // 如果对方的信息与本地一致，为true
             */
            bool validateRemoteState(uint64_t remoteSize,
                                     uint64_t localSeqNum,
                                     uint64_t remoteSeqNum) const
            {
                return (_localSeqNum>=localSeqNum);
            }

            /// @brief Are there messages ready to be popped? 判断队列是否阻塞，如果阻塞，就不能弹出消息了，等待流控结束才行
            bool isActive() const
            {
                assert(_localSeqNum>=_localSeqNumOnPeer);
                // "I think the remote peer has room, and I've got something to send."
                bool active = ((_remoteSize > (_localSeqNum-_localSeqNumOnPeer)) && !_msgQ.empty());
                if (active && _authenticating) {
                    // If authenticating, I've only got something to send if it's an auth message.
                    active = isAuthMessage(_msgQ.front()->getMessageType());
                }
                return active;
            }

            /// @brief Authentication complete, activate for non-auth messages.
            void authDone() { assert(_authenticating); _authenticating = false; }

            /// Drop any buffered messages and abort their queries
            void abortMessages();

            std::shared_ptr<NetworkManager::ConnectionStatus> getNewStatus(const uint64_t spaceBefore,
                                                                             const uint64_t spaceAfter);

            /// @brief Get available space (in number of messages for now)
            /// @return number of messages before sendQueueLimit is exceeded
            uint64_t getAvailable() const ;

            uint64_t getLocalSeqNum() const
            {
                return _localSeqNum;
            }

            uint64_t getRemoteSeqNum() const
            {
                return _remoteSeqNum;
            }

        private:
            Channel();
            Channel(const Channel& other);
            Channel& operator=(const Channel& right);
        private:
            InstanceID _instanceId;
            NetworkManager::MessageQueueType _mqt;
            uint64_t _remoteSize;
            uint64_t _localSeqNum;
            uint64_t _remoteSeqNum;
            uint64_t _localSeqNumOnPeer;
            typedef std::deque<MessageDescPtr> MessageQueue; // 用deque双端队列做消息队列
            MessageQueue _msgQ;
            uint64_t _sendQueueLimit; // 队列大小
            bool _authenticating;
        };

        /**
         * A message queue with multiple parallel FIFO channels/streams:
         * one channel per NetworkManager::MessageQueueType.
         * FIFO is enforced on per-channel basis.
         * The channels are drained in a round-robin fashion.
         */
        class MultiChannelQueue
        {
        public:
            MultiChannelQueue(InstanceID instanceId, Connection const& conn)
                : _instanceId(instanceId)
                , _channels(NetworkManager::mqtMax)
                , _currChannel(NetworkManager::mqtNone)
                , _activeChannelCount(0)
                , _size(0)
                , _remoteGenId(0)
                , _localGenId(getNextGenId())
                , _connection(conn)
            {}

            ~MultiChannelQueue() {}

            /**
             * Append a new message to the end of the queue of a given MessageQueueType
             *
             * @param mqt message queue type to identy the appropriate channel
             * @param msg message to append
             * @throws NetworkManager::OverflowException if there is not enough
                       queue space on either sender or receiver side
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             */
            std::shared_ptr<NetworkManager::ConnectionStatus>
            pushBack(NetworkManager::MessageQueueType mqt,
                     const std::shared_ptr<MessageDesc>& msg)
            {
                return _push(mqt, msg, /*atBack:*/ true);
            }

            /**
             * Add an auth message to the front of the non-flow-controlled queue
             *
             * @param msg auth message to append
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             *
             * @note Does not throw any NetworkManager::OverflowException, since
             *       this is used only for connection authentication (and so the
             *       message *must* get sent regardless of flow control
             *       considerations).
             */
            std::shared_ptr<NetworkManager::ConnectionStatus>
            pushUrgent(const std::shared_ptr<MessageDesc>& msg)
            {
                return _push(NetworkManager::mqtNone, msg, /*atBack:*/ false);
            }

            /**
             * Dequeue the next available message if any.
             *
             * @param msg next dequeued message, which can be empty if the
             *            channel is empty or if the receiver is out of space
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             * @note When connection is authenticating, only auth messages will
             *       be popped.
             */
            std::shared_ptr<NetworkManager::ConnectionStatus>
            popFront(std::shared_ptr<MessageDesc>& msg);

            /**
             * Set the available queue space on the receiver
             *
             * @param mqt message queue type to identy the appropriate channel
             * @param rSize remote queue size (in number of messages for now)
             * @param localSeqNum the last sequence number generated by this instance as observed by the peer
             * @param remoteSeqNum the last sequence number generated by the peer (as observed by this instance)
             * @return a new status indicating a change from the previous status,
             *         used to indicate transitions to/from the out-of-space state
             */
            std::shared_ptr<NetworkManager::ConnectionStatus> setRemoteState(NetworkManager::MessageQueueType mqt,
                                                                               uint64_t rSize,
                                                                               uint64_t localGenId,
                                                                               uint64_t remoteGenId,
                                                                               uint64_t localSeqNum,
                                                                               uint64_t remoteSeqNum);

            /**
             * Get available queue space on i.e. min(sender_space,receive_space)
             *
             * @param mqt message queue type to identy the appropriate channel
             * @return queue size (in number of messages for now)
             */
            uint64_t getAvailable(NetworkManager::MessageQueueType mqt) const ;

            /// Are there messages ready to be dequeued ?
            bool isActive() const
            {
                assert(_activeChannelCount<=NetworkManager::mqtMax);
                return (_activeChannelCount > 0);
            }

            /// @brief Authentication complete, activate non-auth messages.
            /// @note Recomputes _activeChannelCount.
            void authDone();

            uint64_t size() const
            {
                return _size;
            }

            uint64_t getLocalGenId() const
            {
                return _localGenId;
            }

            uint64_t getRemoteGenId() const
            {
                return _remoteGenId;
            }

            /// Abort enqued messages and their queries
            void abortMessages();
            void swap(MultiChannelQueue& other);

            uint64_t getLocalSeqNum(NetworkManager::MessageQueueType mqt) const ;
            uint64_t getRemoteSeqNum(NetworkManager::MessageQueueType mqt) const ;

        private:
            MultiChannelQueue();
            MultiChannelQueue(const MultiChannelQueue& other);
            MultiChannelQueue& operator=(const MultiChannelQueue& right);

            std::shared_ptr<NetworkManager::ConnectionStatus> _push(
                NetworkManager::MessageQueueType mqt,
                const std::shared_ptr<MessageDesc>& msg,
                bool atBack);

        private:
            InstanceID _instanceId;
            // # of channels is not expected to be large/or changing, so vector should be OK
            typedef std::vector<std::shared_ptr<Channel> > Channels; // 包含多个channel
            Channels _channels;
            uint32_t _currChannel;
            size_t   _activeChannelCount;
            uint64_t _size; // 队列中的消息个数
            uint64_t _remoteGenId;
            uint64_t _localGenId;
            Connection const& _connection;

            static uint64_t getNextGenId()
            {
                const uint64_t billion = 1000000000;
                struct timespec ts;
                if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
                    assert(false);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
                }
                return (ts.tv_sec*billion + ts.tv_nsec);
            }
        };

      private:
        MessageDescPtr _messageDesc;            // Holds incoming message as it is assembled
        MultiChannelQueue _messageQueue; // 注意，这个队列是multichannel的
        NetworkManager& _networkManager;
        InstanceID _remoteInstanceId;
        InstanceID _selfInstanceId;
        int _sessPri;
        std::shared_ptr<Session> _session;
        std::shared_ptr<Authenticator> _authenticator;
        const uint64_t _genId;

        enum ConnectionState
        {
            NOT_CONNECTED,       // Initial state
            CONNECT_IN_PROGRESS, // Connecting as client to remote peer
            AUTH_IN_PROGRESS,    // Authentication handshake in progress
            CONNECTED,           // Connection established and authenticated
            DISCONNECTED,        // Connection is going away
        };

        ConnectionState _connectionState;

        boost::asio::ip::address _remoteIp;
        uint16_t _remotePort;
		// dz add rdma_cm_id 成员变量用下划线开始
//		rdma_cm_id* _rdma_cm_id = nullptr;

        boost::system::error_code _error;
        std::shared_ptr<boost::asio::ip::tcp::resolver::query> _query;
        typedef std::unordered_map<QueryID, ClientContext::DisconnectHandler> ClientQueries;
        ClientQueries _activeClientQueries;
        Mutex _mutex;
        bool _isSending;
        bool _logConnectErrors;
        bool _flushThenDisconnect;
        time_t _lastRecv;

        typedef
        std::map<NetworkManager::MessageQueueType, std::shared_ptr<const NetworkManager::ConnectionStatus> >
        ConnectionStatusMap;
        ConnectionStatusMap _statusesToPublish;

		// dz add rdma func
        void initRdma();
		
        void handleReadError(const boost::system::error_code& error);
        void onResolve(std::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                       std::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                       const boost::system::error_code& err,
                       boost::asio::ip::tcp::resolver::iterator endpoint_iterator);

        void onConnect(std::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                       std::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                       boost::asio::ip::tcp::resolver::iterator endpoint_iterator,
                       const boost::system::error_code& err);
        void disconnectInternal();
        void connectAsyncInternal(const std::string& address, uint16_t port);
        void authDoneInternal();
        void abortMessages();

        void readMessage();
        void handleReadMessage(const boost::system::error_code&, size_t);
        void handleReadRecordPart(const boost::system::error_code&, size_t);
        void handleReadBinaryPart(const boost::system::error_code&, size_t);
        void handleSendMessage(const boost::system::error_code&, size_t,
                               std::shared_ptr< std::list<std::shared_ptr<MessageDesc> > >&,
                               size_t);
        void pushNextMessage();
        void setRemoteIpAndPort();
        void pushMessage(std::shared_ptr<MessageDesc>& messageDesc,
                         NetworkManager::MessageQueueType mqt);
        std::shared_ptr<MessageDesc> popMessage();
        bool publishQueueSizeIfNeeded(const std::shared_ptr<const NetworkManager::ConnectionStatus>& connStatus);
        void publishQueueSize();
        std::shared_ptr<MessageDesc> makeControlMessage();
        std::shared_ptr<MessageDesc> makeLogonMessage();

      private:
        Connection() = delete;
        Connection(const Connection& other) = delete ;
        Connection& operator=(const Connection& right) = delete;

      public:
        Connection(NetworkManager& networkManager,
                   uint64_t genId,
                   InstanceID selfInstanceId,
                   InstanceID instanceID = INVALID_INSTANCE); // 注意，这里有个默认参数
        virtual ~Connection();

        virtual void attachQuery(QueryID queryID, ClientContext::DisconnectHandler& dh);
        void attachQuery(QueryID queryID);
        virtual void detachQuery(QueryID queryID);

        bool isConnected() const
        {
            return  _connectionState == CONNECTED;
        }

        bool isAuthenticating() const
        {
            return  _connectionState == AUTH_IN_PROGRESS;
        }

        uint64_t getGenId() const { return _genId; }
        time_t getLastRecv() const { return _lastRecv; }

        InstanceID getRemoteInstanceId() const { return _remoteInstanceId; }
        bool isInbound() const { return _remoteInstanceId == INVALID_INSTANCE; } // remote是invalid就表示是inbound消息
        bool isOutbound() const { return !isInbound(); }

        /**
         * Determine how many entries still reside in the connections queue.
         */
        size_t queueSize();


        /// The first method executed for the incoming connected socket
        void start();

        void sendMessage(std::shared_ptr<MessageDesc>& messageDesc,
                         NetworkManager::MessageQueueType mqt = NetworkManager::mqtNone);

        /// Same as sendMessage(), but disconnect if an exception is raised.
        void sendMessageDisconnectOnError(
            std::shared_ptr<MessageDesc>& messageDesc,
            NetworkManager::MessageQueueType mqt = NetworkManager::mqtNone);

        /**
         * Asynchronously connect to the remote site, address:port.
         * It does not wait for the connect to complete.
         * If the connect operation fails, it is scheduled for
         * a reconnection (with the currently available address:port from SystemCatalog).
         * Connection operations can be invoked immediately after the call to connectAsync.
         * @param address[in] target DNS name or IP(v4)
         * @param port target port
         */
        void connectAsync(const std::string& address, uint16_t port);

        /**
         * Mark the Connection as successfully authenticated.
         */
        void authDone();

        /**
         * Disconnect the socket and abort all in-flight async operations
         */
        virtual void disconnect();

        /**
         * Wait for all queued messages to be sent, then send disconnect() on the wire.
         */
        void flushThenDisconnect();

        boost::asio::ip::tcp::socket& getSocket()
        {
            return _socket;
        }

        /** Remote peer name (instance, ipaddr, port) */
        std::string getPeerId() const;

        /** Bare endpoint name without InstanceID information */
        std::string getRemoteEndpointName() const;

        /// For internal use
        void setRemoteQueueState(NetworkManager::MessageQueueType mqt,
                                 uint64_t size,
                                 uint64_t localGenId,
                                 uint64_t remoteGenId,
                                 uint64_t localSn,
                                 uint64_t remoteSn);

        uint64_t getAvailable(NetworkManager::MessageQueueType mqt) const
        {
            return _messageQueue.getAvailable(mqt);
        }

        class ServerMessageDesc : public MessageDesc
        {
          public:
            ServerMessageDesc() {}
            ServerMessageDesc(std::shared_ptr<SharedBuffer>& binary)
                : MessageDesc(binary) {}
            virtual ~ServerMessageDesc() {}
            virtual bool validate();
          protected:
            virtual MessagePtr createRecord(MessageID messageType);
          private:
            ServerMessageDesc(const ServerMessageDesc&);
            ServerMessageDesc& operator=(const ServerMessageDesc&);
        };

        /// @brief Set/get associated Authenticator object.
        /// @{
        void setAuthenticator(std::shared_ptr<Authenticator>& author);
        std::shared_ptr<Authenticator> getAuthenticator() const
            { return _authenticator; }
        /// @}

        /// @brief Set/get associated Session object.
        /// @note setSession() also clears the authenticator pointer.
        /// @{
        void setSession(std::shared_ptr<Session>& sess);
        std::shared_ptr<Session> getSession() const { return _session; }
        /// @}

        /// @brief Set/get requested session priority.
        /// @note Setter assumes validity of @c p already checked.
        /// @note Might not have a Session object yet (if not
        ///       authenticated yet), so need a place to keep this
        ///       until we do.
        /// @see SessionProperties
        /// @{
        void setSessionPriority(int p) { _sessPri = p; }
        int getSessionPriority() const { return _sessPri; }
        /// @}

    };  // class Connection

    typedef std::shared_ptr<Connection> ConnectionPtr;
}

#endif /* CONNECTION_H_ */
