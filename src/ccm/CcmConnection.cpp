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

// The header file for the implementation details in this file
#include "CcmConnection.h"
// header files from the ccm module
#include "CcmErrorCode.h"
#include "CcmMsgType.h"
#include "CcmSession.h"
#include "CcmSessionCapsule.h"
#include <ccm/CcmProperties.h>
// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
// third-party libraries
#include <log4cxx/logger.h>
// c++ standard libraries
#include <iomanip>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmConnection"));
}

namespace asio = boost::asio;
namespace gpb = google::protobuf;

namespace scidb { namespace ccm {

CcmConnection::CcmConnection(asio::io_service& ioService,
                             const CcmProperties& props,
                             asio::ssl::context& context,
                             CcmSessionCapsule& cap)
    : _socket(ioService)
    , _props(props)
    , _sslStream(ioService, context)
    , _readTimer(ioService)
    , _sessionCapsule(cap)
    , _activeSession()
    , _stopped(false)
{
    // set the duration, but do NOT schedule it with async_wait.
    _readTimer.expires_at(boost::posix_time::pos_infin);
    LOG4CXX_TRACE(logger, "Creating CcmConnection");
}

asio::ssl::stream<asio::ip::tcp::socket>::lowest_layer_type& CcmConnection::getSocket()
{
    if (_props.getTLS()) {
        return _sslStream.lowest_layer();
    }
    return _socket;
}

CcmConnection::~CcmConnection() noexcept
{
    LOG4CXX_TRACE(logger, "Destructing CcmConnection");
    _stop();
}

void CcmConnection::start()
{
    if (_props.getTLS()) {
        _sslStream.async_handshake(asio::ssl::stream_base::server,
                                   std::bind(&CcmConnection::_handleTlsHandshake,
                                             shared_from_this(),
                                             std::placeholders::_1));
    } else {
        _startReadProcessing();
    }
}

void CcmConnection::_stop() noexcept
{
    // Make certain to use the error_code not the throwing
    // forms of boost::asio operations. stop() MAY NOT throw.
    LOG4CXX_TRACE(logger, "Stopping CcmConnection");
    if (_stopped) {
        LOG4CXX_INFO(logger, "Connection stopped more than once.");
        return;
    }

    boost::system::error_code ec;

    if (_props.getTLS()) {
        _sslStream.shutdown(ec);
        if (ec) {
            LOG4CXX_WARN(logger,
                         "Boost asio error in _sslStream.shutdown(): " << ec << ": " << ec.message());
        }
    }

    getSocket().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    if (ec) {
        LOG4CXX_WARN(logger, "socket shutdown error in stop(): " << ec << ": " << ec.message());
    }
    getSocket().close(ec);
    if (ec) {
        LOG4CXX_WARN(logger, "socket shutdown error in stop(): " << ec << ": " << ec.message());
    }

    _stopped = true;
    // "...and soon the last shared pointer reference to this connection object will go away."
    // Unless, of course, it already has and this function is being called from the destructor.
    LOG4CXX_TRACE(logger, "CcmConnection STOPPED");
}

void CcmConnection::_handleReadTimeOut(const boost::system::error_code& ec)
{
    if (ec == asio::error::operation_aborted) {
        LOG4CXX_TRACE(logger, "_readTimer aborted/rescheduled for:  " << _activeSession->getId());
    } else {
        ASSERT_EXCEPTION(!ec, "Unexpected error code in _handleReadTimeOut:" << ec.message());
        LOG4CXX_TRACE(logger, "Input timer for reading expired: " << _activeSession->getId());
        sendError(CcmErrorCode::READ_TIMEOUT);
        _stop();
    }
}

void CcmConnection::_startReadProcessing()
{
    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to read next message after connection is stopped.");
        return;
    }

    if (_messageStream.size() > 0) {
        // The client sent more then one request on the socket (without waiting for the
        // response to the last one).
        LOG4CXX_TRACE(logger, "Still have " << _messageStream.size() << " bytes in _messageStream");
        boost::system::error_code ec;
        _handleReadData(ec, _messageStream.size());
        return;
    }

    if (_props.getTLS()) {
        asio::async_read(_sslStream,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&CcmConnection::_handleReadData,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    } else {
        asio::async_read(_socket,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&CcmConnection::_handleReadData,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }
}

void CcmConnection::_handleTlsHandshake(const boost::system::error_code& ec)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempting to handle TLS handshake connection is stopped." << ec << ec.message());
        return;
    }
    if (ec) {
        // TLS/SSL Failed handshake
        LOG4CXX_WARN(logger, "TLS handshake failed " << ec << " -- " << ec.message());
        return;
    }
    _startReadProcessing();
}

void CcmConnection::_handleReadData(const boost::system::error_code& ec, size_t bytes_read)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempting to handle ReadData after connection is stopped: "
                          << ec << ec.message() << " bytes_read " << bytes_read);
        return;
    }

    if (ec) {
        // It's very likely that the client application exited and closed the socket, but
        // sendResponse (invoked by sendErrorMessage) checks that the socket is available
        LOG4CXX_INFO(logger,
                     "CcmConnection::handleReadData boost condition: " << ec << ": " << ec.message());
        sendError(CcmErrorCode::GENERAL_ERROR);
        return;
    }

    LOG4CXX_TRACE(logger, "read " << bytes_read << "; _messageStream size " << _messageStream.size());

    // We have started reading a new message (at least one byte). Start the read timer so
    // that we get the full message or time out.
    if (_readTimer.expires_at().is_infinity()) {
        LOG4CXX_TRACE(logger, "Set Initial _read_timer");
        _readTimer.expires_from_now(boost::posix_time::seconds(_props.getReadTimeOut()));
        _readTimer.async_wait(
            std::bind(&CcmConnection::_handleReadTimeOut, shared_from_this(), std::placeholders::_1));
    } else {
        // The entire message (header and protobuf) must be read before the _read_timer
        // expires.  So keep existing time-out, and prevent the degenerate bad-actor case
        // where the client sends one byte, waits 'slightly less than the timer', then
        // sends the next byte.
        LOG4CXX_TRACE(logger, "_read_timer is already running... So don't reset");
    }

    if (_messageStream.size() < sizeof(CcmMsgHeader)) {
        // We need more data to have enough to populate the _readHeader.
        if (_props.getTLS()) {
            asio::async_read(_sslStream,
                             _messageStream,
                             asio::transfer_at_least(1),
                             std::bind(&CcmConnection::_handleReadData,
                                       shared_from_this(),
                                       std::placeholders::_1,
                                       std::placeholders::_2));

        } else {
            asio::async_read(_socket,
                             _messageStream,
                             asio::transfer_at_least(1),
                             std::bind(&CcmConnection::_handleReadData,
                                       shared_from_this(),
                                       std::placeholders::_1,
                                       std::placeholders::_2));
        }
    } else {
        std::iostream is(&_messageStream);
        is.read(reinterpret_cast<char*>(&_readHeader), sizeof(CcmMsgHeader));

        LOG4CXX_TRACE(logger,
                      "Read from _messageStream into _readHeader; "
                          << " _messageStream size now " << _messageStream.size()
                          << "; HEADER is: " << _readHeader);

        if (_readHeader.isValid()) {
            // mjl wants this assert just in case the const & changed since we tested
            // it at the top of the function.
            SCIDB_ASSERT(!ec);

            // dz add
//            if(_readHeader.getType() == )

            // We have the header so go work on the record (protobuf) data.
            _handleRecordPortion(ec, _messageStream.size());
        } else {
            // The header has invalid values.
            //   Rather than try to figure out why the version/type are invalid just close the
            //   connection. If the session was authenticated, it still is. So reset the
            //   connection and let the client re-establish a new connection with a fresh
            //   stream of data.
            _activeSession.reset();
            LOG4CXX_TRACE(logger, "Invalid message header: " << _readHeader);
            sendError(CcmErrorCode::INVALID_MSG_HEADER);
            _stop();
        }
    }
}

void CcmConnection::_handleRecordPortion(const boost::system::error_code& ec, size_t bytes_read)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempt to read protobuf after connection is stopped: "
                          << ec.message() << "; bytes_read: " << bytes_read);
        return;
    }

    auto streamSize = _messageStream.size();
    auto recordSize = _readHeader.getRecordSize();
    if (ec) {
        // It's very likely that the client application exited and closed the
        // socket, but sendResponse (called by sendError) checks that the socket
        // is available.
        LOG4CXX_INFO(logger, "handleRecordPortion boost error: " << ec << ": " << ec.message());
        sendError(CcmErrorCode::GENERAL_ERROR);
        return;
    }

    if (streamSize >= recordSize) {
        LOG4CXX_TRACE(logger, "bytes read: " << bytes_read << "; stream size:  " << streamSize);
        _processRequestMessage();
    } else {
        // We don't have enough data to create the record so read some more.
        SCIDB_ASSERT(!ec);
        LOG4CXX_TRACE(logger,
                      " Read more for protobuf. Needed: " << recordSize << "; Have: " << streamSize);
        asio::async_read(_socket,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&CcmConnection::_handleRecordPortion,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }
}

template <typename T>
bool CcmConnection::_parseRecordPortion(T& msg)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to parse Record (protobuf) after connection is stopped .");
        return false;
    }
    auto recordSize = _readHeader.getRecordSize();

    SCIDB_ASSERT(_messageStream.size() >= recordSize);

    // We could just pass the _messageStream to
    // google::protobuf::Message.ParseFromIstream() but we don't because the
    // stream is doing read-ahead.  ParseFromIstream(...) consumes the entire
    // input stream.[In addition Message::ParseFromIstream() is a heavyweight
    // I/O operation which requires we use Message and are not able to use
    // MessageLite. (Using ParseFromIstream means we cannot link to
    // libprotobuf-lite.so)].  So use ParseFromString().
    asio::streambuf::const_buffers_type bufs = _messageStream.data();
    std::string str(asio::buffers_begin(bufs), asio::buffers_begin(bufs) + recordSize);
    _messageStream.consume(recordSize);
    bool rc = msg.ParseFromString(str);
    // clang-format off
    LOG4CXX_TRACE(logger,
                  "Protobuf parsing success = " << std::boolalpha << rc
                  << "; msg.IsInitialized() = " << msg.IsInitialized() << std::noboolalpha
                  << "; _messageStream.size() is now " << _messageStream.size());
    //clang-format on

    return rc && msg.IsInitialized();
}

void CcmConnection::_processRequestMessage()
{
    // We have finished a full read of a request message (the header and the
    // protocol buffer), so abort the _read_timer and set the duration to
    // indicate that the timer /would/ expire at infinity if it async_wait()
    // were called on the timer.  This is a sentinel value so that we can see
    // whether the timer is running or not in _handleReadData(). Do NOT
    // async_wait on the timer here.
    _readTimer.expires_at(boost::posix_time::pos_infin);


    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to process Request message after connection is stopped .");
        return;
    }
    SCIDB_ASSERT(_readHeader.isValid());
    _activeSession = _sessionCapsule.getSession(Uuid(_readHeader.getSession()));
    if (!_activeSession) {
        _activeSession = _sessionCapsule.createSession(_props.getSessionTimeOut());
    }


    /*
     * We still have a switch case but this should be the ONE AND ONLY location of a
     * switch on msgtype.
     */
    switch (_readHeader.getType()) {
        case CcmMsgType::AuthLogonMsg: {
            LOG4CXX_TRACE(logger, "CcmConnection: Processing AuthLogon");
            msg::AuthLogon protoMsg;
            if (_parseRecordPortion<msg::AuthLogon>(protoMsg)) {
                _activeSession->processAuthLogon(shared_from_this(), protoMsg);
            } else {
                // The protobuf was bad, who knows what remains in the incoming socket
                // We need to close the connection.
                // However, We do not explicitly remove the session.
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::AuthResponseMsg: {
            LOG4CXX_TRACE(logger, "CcmConnection: Processing AuthResponse");
            msg::AuthResponse protoMsg;
            if (_parseRecordPortion<msg::AuthResponse>(protoMsg)) {
                _activeSession->processAuthResponse(shared_from_this(), protoMsg);
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::ExecuteQueryMsg: {
            LOG4CXX_TRACE(logger, "CcmConnection: Processing ExecuteQuery");
            // dz add 用于统计查询时间消耗
            LOG4CXX_FATAL(logger, "dzs: CcmConnection: Processing ExecuteQuery");

            msg::ExecuteQuery protoMsg;
            if (_parseRecordPortion<msg::ExecuteQuery>(protoMsg)) {
                // Execute Query Can take a 'very long' time. so cancel the session timer
                _sessionCapsule.cancelTimer(_activeSession->getId());
                _activeSession->processExecuteQuery(shared_from_this(), protoMsg);
                if (_activeSession) {
                    // and re-enable it here. if we still have the session (which can be
                    // reset in the case of an invalid message for the current
                    // CcmSessionState)
                    _sessionCapsule.restartTimer(_activeSession->getId(),
                                                 _activeSession->getTimeOut());
                }
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::FetchIngotMsg: {
            LOG4CXX_TRACE(logger, "CcmConnection: Processing FetchIngot: ");
            msg::FetchIngot protoMsg;
            if (_parseRecordPortion<msg::FetchIngot>(protoMsg)) {
                // TODO (Phase 2): this too, may need to cancel the timer while we
                // wait for the back-end to generate data in a long running query.
                _activeSession->processFetchIngot(shared_from_this(), protoMsg);
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        default: {
            LOG4CXX_WARN(logger,
                         "CcmConnection: Invalid Message Type value: "
                         << _readHeader.getType() << " sent as a  Client Request.");
            sendError(CcmErrorCode::INVALID_REQUEST);
            _stop();
        } break;
    }
}

void CcmConnection::removeSession()
{
    SCIDB_ASSERT(_activeSession);
    _sessionCapsule.removeSession(_activeSession->getId());
    _activeSession.reset();
}

std::shared_ptr<CcmSession> CcmConnection::createSession(long timeOut)
{
    _activeSession = _sessionCapsule.createSession(timeOut);
    return _activeSession;
}

std::shared_ptr<CcmSession> CcmConnection::createSession()
{
    return createSession(_props.getSessionTimeOut());
}

void CcmConnection::sendResponse(CcmMsgType type, std::shared_ptr<gpb::Message> msg)
{
    std::string a;
    sendResponse(type, msg, a);
}

void CcmConnection::sendResponse(CcmMsgType type, std::shared_ptr<gpb::Message> msg, const std::string & binary)
{
    if (_stopped) {
        LOG4CXX_WARN(logger, "Attempting to send response on stopped/closed socket.");
        return;
    }

    // If the socket is closed,  we cannot send anything, so don't try.
    if (!getSocket().is_open()) {
        return;
    }

    if (!_activeSession) {
        _activeSession = _sessionCapsule.invalidSession();
    }
    LOG4CXX_TRACE(logger, "SENDING (" << type << ") CcmSession: " << _activeSession->getId());
    CcmMsgHeader writeHeader;
    writeHeader.setType(type);
    asio::streambuf writeBuffer;
    std::ostream output(&writeBuffer);
    msg->SerializeToOstream(&output);
    writeHeader.setSession(_activeSession->getId().data());
    writeHeader.setRecordSize(static_cast<uint32_t>(writeBuffer.size()));

    std::vector<asio::const_buffer> buffers;
    buffers.push_back(asio::buffer(&writeHeader, sizeof(writeHeader)));
    buffers.push_back(asio::buffer(writeBuffer.data()));
    if (binary.size() > 0) {
        buffers.push_back(asio::buffer(binary));
    }
    if (_props.getTLS()) {
        asio::async_write(_sslStream,
                          buffers,
                          std::bind(&CcmConnection::_postSendMessage,
                                    shared_from_this(),
                                    std::placeholders::_1,
                                    std::placeholders::_2));

    } else {
        asio::async_write(_socket,
                          buffers,
                          std::bind(&CcmConnection::_postSendMessage,
                                    shared_from_this(),
                                    std::placeholders::_1,
                                    std::placeholders::_2));
    }
}

void CcmConnection::sendError(CcmErrorCode code)
{
    auto msg = std::make_shared<msg::Error>();
    msg->set_code(static_cast<uint32_t>(code));
    msg->set_text(errorCodeToString(code));
    sendResponse(CcmMsgType::ErrorMsg, msg);
}

void CcmConnection::_postSendMessage(const boost::system::error_code& ec, size_t bytesSent)
{
    if (ec) {
        // TODO (Phase 2) -- double check this reasoning): Should this:
        // 1. throw  an exception, or
        // 2. stop the connection [This is what we do now], or
        // 3. Just log
        //    a. and return
        //    b. and startReadProcessing(); (i.e. continue processing next messages)
        //
        // It's futile to attempt to send yet another response (an Error this time) to the
        // client.  Which is why (right now) we just close the connection (big hammer
        // which may be incorrect if the reason was because the payload being sent (think
        // fetchingotresponse) was too big.)
        LOG4CXX_ERROR(logger,
                      "Sending failed: " << bytesSent << " bytes (reported as sent),"
                                         << " ec=" << ec.message() << ": " << ec.message());
        _stop();
        return;
    }
    LOG4CXX_TRACE(logger, "sent " << bytesSent << " bytes");
    _startReadProcessing();
}

}}  // namespace scidb::ccm

//  LocalWords:  CcmConnection
