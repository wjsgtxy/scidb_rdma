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
 * BaseConnection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include "BaseConnection.h"

#include <memory>

#include <network/MessageDesc.h>
#include <system/Exceptions.h>



using namespace std;
namespace asio = boost::asio;


namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
log4cxx::LoggerPtr BaseConnection::logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * B a s e C o n n e c t i o n
 */
BaseConnection::BaseConnection(boost::asio::io_service& ioService)
    : _socket(ioService)
{ }

BaseConnection::~BaseConnection()
{
    disconnect();
}

void BaseConnection::connect(string address, uint32_t port)
{
   LOG4CXX_DEBUG(logger, "Connecting to " << address << ":" << port)

//   asio::ip::tcp::resolver resolver(_socket.get_io_service());
    asio::ip::tcp::resolver resolver(GET_IO_SERVICE(_socket)); // dz boost1.7之后移除了这个方法，保证兼容性

   stringstream serviceName;
   serviceName << port;
   asio::ip::tcp::resolver::query query(address, serviceName.str());
   asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
   asio::ip::tcp::resolver::iterator end;

   boost::system::error_code error = boost::asio::error::host_not_found;
   while (error && endpoint_iterator != end)
   {
      _socket.close();
      _socket.connect(*endpoint_iterator++, error);
   }
   if (error)
   {
      LOG4CXX_FATAL(logger, "Error #" << error << " when connecting to " <<
                    address << ':' << port << " (" << error.message() << ')');
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR)
         << error << address << port;
   }

   configConnectedSocket();
   LOG4CXX_DEBUG(logger, "Connected to " << address << ":" << port);
}

void BaseConnection::configConnectedSocket()
{
   boost::asio::ip::tcp::no_delay no_delay(true);
   _socket.set_option(no_delay);
   boost::asio::socket_base::keep_alive keep_alive(true);
   _socket.set_option(keep_alive);

   int s = _socket.native_handle();
   int optval;
   socklen_t optlen = sizeof(optval);

   /* Set the option active */
   optval = 1;
   if(setsockopt(s, SOL_TCP, TCP_KEEPCNT, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPCNT)");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPIDLE, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPIDLE)");
   }
   optval = 30;
   if(setsockopt(s, SOL_TCP, TCP_KEEPINTVL, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPINTVL)");
   }

   if (logger->isTraceEnabled()) {
      boost::asio::socket_base::receive_buffer_size optionRecv;
      _socket.get_option(optionRecv);
      int size = optionRecv.value();
      LOG4CXX_TRACE(logger, "Socket receive buffer size = " << size);
      boost::asio::socket_base::send_buffer_size optionSend;
      _socket.get_option(optionSend);
      size = optionSend.value();
      LOG4CXX_TRACE(logger, "Socket send buffer size = " << size);
   }
}

void BaseConnection::disconnect()
{
    _socket.close();
    LOG4CXX_DEBUG(logger, "Disconnected")
}

void BaseConnection::send(std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::send begin");
    try
    {
        std::vector<boost::asio::const_buffer> constBuffers;
        messageDesc->_messageHeader.setSourceInstanceID(
            CLIENT_INSTANCE); // 注意，这里发过去的消息，都是当作client处理的

        messageDesc->writeConstBuffers(constBuffers);
        boost::asio::write(_socket, constBuffers);

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::send end");
    }
    catch (const boost::system::system_error &se)
    {
        stringstream ss;
        ss << "Write failed: " << se.code().message() << " (" << se.code() << ')';
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "send" << ss.str();
    }
    catch (const boost::exception &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "send" << boost::diagnostic_information(e);
    }
}

} // namespace scidb
