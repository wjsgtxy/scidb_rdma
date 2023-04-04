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
#ifndef CCM_CONNECTION_H_
#define CCM_CONNECTION_H_

// header files from the ccm module
#include "CcmMsgHeader.h"
#include <Ccm.pb.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
// c++ standard libraries
#include <unordered_set>

namespace google { namespace protobuf {
class Message;
}}  // namespace google::protobuf

namespace scidb { namespace ccm {

class CcmProperties;
class CcmSession;
class CcmSessionCapsule;
enum class CcmErrorCode;
enum class CcmMsgType;

/**
 * @class CcmConnection
 *
 * @ingroup Ccm
 *
 * Request messages read from the connection are processed in a
 * run-to-completion model.  The session and message type are specified in the
 * header. The connection will process the complete message for the session
 * before beginning processing on another message (possibly from another
 * session).
 */
class CcmConnection : public std::enable_shared_from_this<CcmConnection>
{
  public:
    // TODO: If the properties object is going to be owned by the manager, would
    // it not make sense to have a back-reference to the manager itself, in case
    // you need to interact with it in some way beyond asking about the
    // properties? (namely interaction with the CcmSessionCapsule (which is
    // currently in the CcmSessionCapsule::Impl).
    CcmConnection(boost::asio::io_service& io_service,
                  CcmProperties const& props,
                  boost::asio::ssl::context& context,
                  CcmSessionCapsule& cap);
    ~CcmConnection() noexcept;

    void start();

    /**
     */
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::lowest_layer_type& getSocket();

    /**
     * Create a new active session for processing this message.
     *
     * @param timeOut the session time-out (in seconds)
     *
     * @param a shared_ptr to the new active session.
     */
    std::shared_ptr<CcmSession> createSession(long timeOut);

    /**
     * Create a new active session for processing this message.
     *
     * The new active session will have a session expiration time-out as set in the
     * CcmProperties when the connection was created.
     *
     * @param a shared_ptr to the new active session.
     */
    std::shared_ptr<CcmSession> createSession();

    void removeSession();

    void sendResponse(CcmMsgType, std::shared_ptr<google::protobuf::Message>);

    void sendResponse(CcmMsgType, std::shared_ptr<google::protobuf::Message>, const std::string&);

    void sendError(CcmErrorCode code);

  private:
    void _stop() noexcept;

    void _handleReadTimeOut(const boost::system::error_code& ec);
    void _startReadProcessing();
    void _handleTlsHandshake(const boost::system::error_code& e);
    void _handleReadData(const boost::system::error_code& ec, size_t bytes_read);
    void _handleRecordPortion(const boost::system::error_code& ec, size_t bytes_read);
    template <typename T>
    bool _parseRecordPortion(T&);
    void _processRequestMessage();
    void _postSendMessage(const boost::system::error_code& ec, size_t bytes_transferr);

    // Networking/socket parameters
    boost::asio::ip::tcp::socket _socket;
    CcmProperties const& _props;
    boost::asio::ssl::stream<boost::asio::ip::tcp::socket> _sslStream;
    boost::asio::deadline_timer _readTimer;
    // CcmSession management variables
    CcmSessionCapsule& _sessionCapsule;
    std::shared_ptr<CcmSession> _activeSession;
    // Status of connection variables
    bool _stopped;
    // read/write data from socket management
    CcmMsgHeader _readHeader;
    boost::asio::streambuf _messageStream;
};

}}  // namespace scidb::ccm

#endif
