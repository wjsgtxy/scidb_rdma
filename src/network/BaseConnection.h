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
 * @file BaseConnection.h
 *
 * @author: roman.simakov@gmail.com
 *
 * @brief The BaseConnection class
 *
 * The file includes the main data structures and interfaces used in message exchanging.
 * Also the file contains BaseConnection class for synchronous connection and message exchanging.
 * This class is used in client code. The scidb engine will use a class which is derived from BaseConnection.
 */

#ifndef BASECONNECTION_H_
#define BASECONNECTION_H_

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


#if BOOST_VERSION >= 107000
#define GET_IO_SERVICE(s) ((boost::asio::io_context&)(s).get_executor().context())
#else
#define GET_IO_SERVICE(s) ((s).get_io_service())
#endif

namespace scidb
{
class MessageDesc;


/**
 * Base class for connection to a network manager and send message to it.
 * Class uses sync mode and knows nothing about NetworkManager.
 */
class BaseConnection
{
protected:

        boost::asio::ip::tcp::socket _socket;
        /**
         * Set socket options such as TCP_KEEP_ALIVE
         */
        void configConnectedSocket();

public:
        BaseConnection(boost::asio::io_service& ioService);
        virtual ~BaseConnection();

        /// Connect to remote site
        void connect(std::string address, uint32_t port);

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
std::shared_ptr<MessageDesc_tt> BaseConnection::sendAndReadMessage(std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: begin");
    send(messageDesc);
    std::shared_ptr<MessageDesc_tt> resultDesc = receive<MessageDesc_tt>();
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: end");
    return resultDesc;
}

template <class MessageDesc_tt>
std::shared_ptr<MessageDesc_tt> BaseConnection::receive()
{
    LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: begin");
    std::shared_ptr<MessageDesc_tt> resultDesc(new MessageDesc_tt());
    try
    {
        // Reading message description
        size_t readBytes = read(_socket, boost::asio::buffer(&resultDesc->_messageHeader,
                                                             sizeof(resultDesc->_messageHeader)));
        assert(readBytes == sizeof(resultDesc->_messageHeader));
        ASSERT_EXCEPTION((resultDesc->validate()), "BaseConnection::receive:");
        // TODO: This must not be an assert but exception of correct handled backward compatibility
        ASSERT_EXCEPTION(
            (   resultDesc->_messageHeader.getNetProtocolVersion() ==
                scidb_msg::NET_PROTOCOL_CURRENT_VER),
            "BaseConnection::receive:");

        // Reading serialized structured part
        readBytes = read(_socket, resultDesc->_recordStream.prepare(
            resultDesc->_messageHeader.getRecordSize()));
        assert(readBytes == resultDesc->_messageHeader.getRecordSize());

        LOG4CXX_TRACE(BaseConnection::logger,
            "BaseConnection::receive: recordSize="
                << resultDesc->_messageHeader.getRecordSize());

        bool rc = resultDesc->parseRecord(readBytes);
        ASSERT_EXCEPTION(rc, "BaseConnection::receive:");

        resultDesc->prepareBinaryBuffer();

        if (resultDesc->_messageHeader.getBinarySize() > 0)
        {
            readBytes = read(_socket,
                boost::asio::buffer(
                    resultDesc->_binary->getWriteData(),
                    resultDesc->_binary->getSize()));

            assert(readBytes == resultDesc->_binary->getSize());
        }

        LOG4CXX_TRACE(BaseConnection::logger,
            "read message: messageType="
                << resultDesc->_messageHeader.getMessageType()
                << " ; binarySize="
                << resultDesc->_messageHeader.getBinarySize());

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: end");
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
        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: exception: "
                      << boost::diagnostic_information(e));
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "receive" << boost::diagnostic_information(e);
    }
    return resultDesc;
}

}

#endif /* SYNCCONNECTION_H_ */
