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
#include "ClientCommManager.h"
// header files from the ccm module
#include "CcmConnection.h"
#include "CcmSessionCapsule.h"
#include <ccm/CcmProperties.h>
// third-party libraries
#include <log4cxx/logger.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.ClientCommManager"));
}

namespace scidb { namespace ccm {

class ClientCommManager::Impl
{
  public:
    explicit Impl(CcmProperties const& props);
    ~Impl() noexcept = default;

    void run();
    void stop();

  private:
    void _startAcceptNextConnection();
    void _handleConnectionAccept(boost::system::error_code ec);

    boost::asio::io_service _ios;
    CcmProperties const& _props;
    CcmSessionCapsule _sessionCapsule;
    tcp::acceptor _acceptor;
    std::shared_ptr<CcmConnection> _nextConnection;
    bool _running;
};

ClientCommManager::ClientCommManager(CcmProperties const& props)
    : _impl(std::make_unique<Impl>(props))
{
    LOG4CXX_DEBUG(logger, "Creating ClientCommManager on port " << props.getPort());
}

ClientCommManager::~ClientCommManager() noexcept = default;

void ClientCommManager::run()
{
    _impl->run();
}

void ClientCommManager::stop()
{
    _impl->stop();
}

//
// ClientCommManager:Impl
//
ClientCommManager::Impl::Impl(CcmProperties const& props)
    : _ios()
    , _props(props)
    , _sessionCapsule(_ios)
    , _acceptor(_ios, tcp::endpoint(tcp::v4(), _props.getPort()))
    , _nextConnection(nullptr)
    , _running(false)
{}

void ClientCommManager::Impl::run()
{
    LOG4CXX_INFO(logger, "Starting ClientCommManager");
    try {
        if (_running) {
            LOG4CXX_WARN(logger, "ClientCommManager is already running.");
            return;
        }
        LOG4CXX_TRACE(logger, "CCM Settings -- " << _props);
        _startAcceptNextConnection();
        _running = true;
        _ios.run();
    } catch (std::exception& e) {
        LOG4CXX_ERROR(logger,
                      "Unexpected failure when running client communications manager." << e.what());
    }
    _running = false;
}

void ClientCommManager::Impl::stop()
{
    LOG4CXX_TRACE(logger, "Stopping ClientCommManager");
    _ios.stop();
    _running = false;
    LOG4CXX_INFO(logger, "ClientCommManager stopped.");
}

void ClientCommManager::Impl::_handleConnectionAccept(boost::system::error_code ec)
{
    if (!ec) {
        _nextConnection->start();
    } else {
        LOG4CXX_ERROR(logger, "Error from accept() on socket. (" << ec.message() << ")");
    }
    // Start accept for the next incoming connection.
    _startAcceptNextConnection();
}

void ClientCommManager::Impl::_startAcceptNextConnection()
{
    namespace tls = boost::asio::ssl;
    tls::context context(tls::context::sslv23);
    context.set_options(
        tls::context::default_workarounds  // Implement various bug workarounds.
        // | tls::context::no_compression     // Disable compression. Compression is disabled by default.
        | tls::context::no_sslv2       // Disable SSL v2.
        | tls::context::no_sslv3       // Disable SSL v3.
        | tls::context::no_tlsv1       // Disable TLS v1.
        | tls::context::single_dh_use  // Always create a new key when using tmp_dh parameters.
    );
    // TODO (Phase 2): The boost ssl socket has been plumbed here and in
    // CcmConnection but is not currently supported in Phase one (MVP)
    // release. The context given above is insufficient to actually create a TLS
    // socket. TLS is the first action item in Phase Two.
    _nextConnection = std::make_shared<CcmConnection>(_ios, _props, context, _sessionCapsule);

    // register a call back to process when a connection is accepted on the
    // socket.  handleConnectionAccept calls startAcceptNextConnection after
    // starting the accepted connection so that the second and subsequent
    // connections are started.
    _acceptor.async_accept(_nextConnection->getSocket(),
                           std::bind(&ClientCommManager::Impl::_handleConnectionAccept,
                                     this,
                                     std::placeholders::_1));
}

}}  // namespace scidb::ccm
