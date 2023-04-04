/**
 * @file RdmaBaseConnection.h
 *
 * @author: dz
 *
 * @brief The RdmaBaseConnection class
 *
 * The file includes the main data structures and interfaces used in message exchanging.
 * Also the file contains BaseConnection class for synchronous connection and message exchanging.
 * This class is used in client code. The scidb engine will use a class which is derived from BaseConnection.
 */

#ifndef RDMABASECONNECTION_H_
#define RDMABASECONNECTION_H_

#include <cstdint>
#include <sstream>

#include <boost/asio.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/exception_ptr.hpp>
#include <log4cxx/logger.h>

#include <network/proto/scidb_msg.pb.h> // for NET_PROTOCOL_CURRENT_VER
#include <system/Exceptions.h> // for ASSERT_EXCEPTION
#include <system/Utils.h>      // for SCIDB_ASSERT
#include <array/SharedBuffer.h>

#include <network/BaseConnection.h>

// 简单点，这个类不用了
// rdma头文件
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

namespace scidb
{
class MessageDesc;


/**
 * Base class for connection to a network manager and send message to it.
 * Class uses sync mode and knows nothing about NetworkManager.
 */
//class RdmaBaseConnection : public BaseConnection // dz public继承
class RdmaBaseConnection
{
protected:

        boost::asio::ip::tcp::socket _socket;
        /**
         * Set socket options such as TCP_KEEP_ALIVE
         */
        void configConnectedSocket();

        /**
         * rdma相关资源
         * demo里面所有的都是static的，我都去掉了
         */
        struct rdma_cm_id *id;
        struct ibv_mr *mr, *send_mr; // 这些指针都没有通过智能指针管理，后面可以用智能指针
        int send_flags;
        uint8_t send_msg[16];
        uint8_t recv_msg[16];

public:
        RdmaBaseConnection(boost::asio::io_service& ioService);
        virtual ~RdmaBaseConnection();

        /// Connect to remote site
        void connect(std::string address, uint32_t port); // dz 覆盖父类的

        virtual void disconnect();

        boost::asio::ip::tcp::socket& getSocket() {
            return _socket;
        }

        /**
         * Send message to peer and read message from it.
         * @param inMessageDesc a message descriptor for sending message.
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message.
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        std::shared_ptr<MessageDesc_tt> sendAndReadMessage(std::shared_ptr<MessageDesc>& inMessageDesc);

         /**
         * Send message to peer
         * @param inMessageDesc a message descriptor for sending message.
         * @throw System::Exception
         */
        void send(std::shared_ptr<MessageDesc>& messageDesc);

        /**
         * Receive message from peer
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        std::shared_ptr<MessageDesc_tt> receive();

        static log4cxx::LoggerPtr logger;
};

template <class MessageDesc_tt>
std::shared_ptr<MessageDesc_tt> RdmaBaseConnection::sendAndReadMessage(std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(RdmaBaseConnection::logger, "The sendAndReadMessage: begin");
    send(messageDesc); // 注意：这是个同步的方法
    std::shared_ptr<MessageDesc_tt> resultDesc = receive<MessageDesc_tt>();
    LOG4CXX_TRACE(RdmaBaseConnection::logger, "The sendAndReadMessage: end");
    return resultDesc;
}

template <class MessageDesc_tt>
std::shared_ptr<MessageDesc_tt> RdmaBaseConnection::receive()
{
    LOG4CXX_TRACE(RdmaBaseConnection::logger, "RdmaBaseConnection::receive: begin");
    std::shared_ptr<MessageDesc_tt> resultDesc(new MessageDesc_tt());
    try
    {
        // Reading message description
        size_t readBytes = read(_socket, boost::asio::buffer(&resultDesc->_messageHeader, sizeof(resultDesc->_messageHeader)));
        assert(readBytes == sizeof(resultDesc->_messageHeader));
        ASSERT_EXCEPTION((resultDesc->validate()), "RdmaBaseConnection::receive:");
        ASSERT_EXCEPTION((resultDesc->_messageHeader.getNetProtocolVersion() == scidb_msg::NET_PROTOCOL_CURRENT_VER), "RdmaBaseConnection::receive:");

        // Reading serialized structured part
        readBytes = read(_socket, resultDesc->_recordStream.prepare(
            resultDesc->_messageHeader.getRecordSize()));
        assert(readBytes == resultDesc->_messageHeader.getRecordSize());

        LOG4CXX_TRACE(RdmaBaseConnection::logger,
            "RdmaBaseConnection::receive: recordSize="
                << resultDesc->_messageHeader.getRecordSize());

        bool rc = resultDesc->parseRecord(readBytes);
        ASSERT_EXCEPTION(rc, "RdmaBaseConnection::receive:");

        resultDesc->prepareBinaryBuffer();

        if (resultDesc->_messageHeader.getBinarySize() > 0)
        {
            readBytes = read(_socket,
                boost::asio::buffer(
                    resultDesc->_binary->getWriteData(),
                    resultDesc->_binary->getSize()));

            assert(readBytes == resultDesc->_binary->getSize());
        }

        LOG4CXX_TRACE(RdmaBaseConnection::logger,
            "read message: messageType="
                << resultDesc->_messageHeader.getMessageType()
                << " ; binarySize="
                << resultDesc->_messageHeader.getBinarySize());

        LOG4CXX_TRACE(RdmaBaseConnection::logger, "RdmaBaseConnection::receive: end");
    }
    catch (const boost::system::system_error &se)
    {
        std::stringstream ss;
        ss << "Read failed: " << se.code().message() << " (" << se.code() << ')';
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "receive" << ss.str();
    }
    catch (const boost::exception &e)
    {
        LOG4CXX_TRACE(RdmaBaseConnection::logger, "RdmaBaseConnection::receive: exception: " << boost::diagnostic_information(e));
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE) << "receive" << boost::diagnostic_information(e);
    }
    return resultDesc;
}

}

#endif
/* SYNCCONNECTION_H_ */
