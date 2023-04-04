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
 * @file AuthMessageHandleJob.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @description Handle incoming authentication handshake messages.
 * These could be the client-side of the handshake if we are making
 * contact with another instance, or they could be server-side for
 * either remote clients or other instances contacting us.
 *
 * @p The handshake itself involves four messages AuthLogon,
 * AuthChallenge, AuthResponse, and AuthComplete.  Informally:
 *
 * @verbatim
 * Authenticating                           SciDB Instance
 *     Entity
 *
 *      ---- Logon: I am Sir Lancelot of the Lake! ------->
 *                                                        |
 *      <---- Challenge: What is your favorite color? ----+
 *      |
 *      +-------------- Response: Blue! ------------------>
 *                                                        |
 *      <--------- Complete: Right.  Off you go. ---------+
 * @endverbatim
 *
 * @p Normal instance-to-instance traffic is split across two
 * uni-directional TCP connections (inbound and outbound), but for the
 * AUTH_I2I handshake we use the same connection for both sides of the
 * handshake to keep things simple.  When the handshake is done, the
 * server side sends an mtHangup and the connection goes back to being
 * used in one direction only.
 *
 * @note All the message types emitted here MUST be considered
 *       isAuthMessage() or else the client will never see them (they
 *       won't escape the Connection::Channel).  A subtle corollary:
 *       we cannot use mtError to signal auth errors, because a
 *       non-auth mtError could get out of the Channel ahead of
 *       handshake messages and cause a premature disconnect.
 */

#include "AuthMessageHandleJob.h"

#include "Connection.h"
#include "InstanceAuthenticator.h"
#include "MessageUtils.h"
#include <rbac/Authenticator.h>
#include <rbac/SessionProperties.h>
#include <system/UserException.h>
#include <util/Timing.h>        // for getCoarseTimestamp()

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.auth"));

Mutex AuthMessageHandleJob::s_mutex;
AuthMessageHandleJob::WeakConnList AuthMessageHandleJob::s_inProgress;


AuthMessageHandleJob::AuthMessageHandleJob(ConnectionPtr const& connection,
                                           MessageDescPtr const& messageDesc)
    : MessageHandleJob(messageDesc)
    , _connection(connection)
{
    assert(connection);
    assert(messageDesc);
}


void AuthMessageHandleJob::enqueue(const std::shared_ptr<WorkQueue>& q)
{
    try {
        inherited::enqueue(q, /*handleOverflow:*/ false);
    }
    catch (WorkQueue::OverflowException& e) {
        // THIS SHOULD NEVER HAPPEN: Authentication messages should be
        // immune from queue size restrictions.
        LOG4CXX_ERROR(logger, "Internal error: "
                      "Overflow exception during authentication on queue @"
                      << hex << q << dec << " - " << e.what());
        sendAuthError(e);
        _connection->flushThenDisconnect();
    }
}


void AuthMessageHandleJob::run()
{
    MessageType messageType =
        static_cast<MessageType>(_messageDesc->getMessageType());
    SCIDB_ASSERT(isAuthMessage(messageType));

    LOG4CXX_TRACE(logger, "Starting auth handling: " << strMsgType(messageType)
                  << " on conn=" << hex << _connection << dec);

    ASSERT_EXCEPTION(_currHandler, "AuthMessageJob handler is not set");
    try {
        _currHandler();
    }
    catch (Exception& e) {
        LOG4CXX_ERROR(logger, "Exception while handling "
                      << strMsgType(messageType) << ": " << e.what());
        sendAuthError(e);
        _connection->flushThenDisconnect();
    }

    LOG4CXX_TRACE(logger, "Done handling " << strMsgType(messageType));
}


void AuthMessageHandleJob::dispatch(NetworkManager* nm)
{
    MessageType msgType =
        static_cast<MessageType>(_messageDesc->getMessageType());
    LOG4CXX_TRACE(logger, "Dispatching " << strMsgType(msgType)
                  << " on conn=" << hex << _connection << dec
                  << " from " << _connection->getPeerId());
    ASSERT_EXCEPTION(isAuthMessage(msgType),
                     "Dispatch non-auth message while authenticating?!");

    // Set appropriate handler and put this job on a work queue, where
    // blocking is allowed.  (No blocking permitted here in the
    // NetworkManager event loop thread!)
    //
    // IMPORTANT: After any call to enqueue(), this object can no
    // longer be mutated: another thread may be working on it.

    try {
        switch (msgType) {

        case mtAuthLogon:
        {
            shared_ptr<scidb_msg::AuthLogon> record =
                _messageDesc->getRecord<scidb_msg::AuthLogon>();

            // Use requested priority for now; abort connection later
            // if it turns out user is not authorized.
            if (record->has_priority()) {
                int pri = record->priority();
                if (SessionProperties::validPriority(pri)) {
                    _connection->setSessionPriority(pri);
                } else {
                    LOG4CXX_DEBUG(logger, "Bad priority " << pri
                                  << " requested for conn=" << hex
                                  << _connection << dec << " (ignored)");
                }
            }

            // Logon for both cluster-internal authentication methods
            // must be handled in the NetworkManager thread, for
            // different reasons.
            //
            // MPI authentication requires quick turnaround or the
            // MPISlaveProxy code will time out (for example, in the
            // mu.mu_update test).
            //
            // Instance-to-instance authentication uses only a single
            // message (see SDB-5702), so if auth succeeds we must
            // install a Session object immediately before another
            // incoming message can be dispatched.
            //
            AuthMethod method = AUTH_NONE;
            if (record->has_authtag()) {
                method = auth::tagToMethod(record->authtag());
            }
            if (method == AUTH_MPI || method == AUTH_I2I) {
                handleAuthLogon();
                break;
            }

            startAuthTimer();   // For external clients only.
            _currHandler = std::bind(&AuthMessageHandleJob::handleAuthLogon, this);
            enqueue(nm->getRequestQueue(_connection->getSessionPriority()));
        }
        break;

        case mtAuthChallenge:
        {
            _currHandler = std::bind(&AuthMessageHandleJob::handleAuthChallenge, this);
            enqueue(nm->getRequestQueue(_connection->getSessionPriority()));
        }
        break;

        case mtAuthResponse:
        {
            if (AUTH_MPI == _connection->getAuthenticator()->getAuthMethod()) {
                // Do MPI auth here in NetworkManager thread, see comment above.
                handleAuthResponse();
            } else {
                _currHandler = std::bind(&AuthMessageHandleJob::handleAuthResponse, this);
                enqueue(nm->getRequestQueue(_connection->getSessionPriority()));
            }
        }
        break;

        case mtAuthComplete:
        {
            _currHandler = std::bind(&AuthMessageHandleJob::handleAuthComplete, this);
            enqueue(nm->getRequestQueue(_connection->getSessionPriority()));
        }
        break;

        default:
            throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_AUTHENTICATION_ERROR)
                << "Bad protocol message";
            break;
        }
    }
    catch (const Exception& e) {
        LOG4CXX_ERROR(logger, "Uncaught exception while dispatching "
                      << strMsgType(msgType) << " auth msg from "
                      << _connection->getPeerId() << ": " << e.what());
        sendAuthError(e);
        _connection->flushThenDisconnect();
    }
}


void AuthMessageHandleJob::handleAuthLogon()
{
    // Read the AuthLogon message, create an authenticator, and use it
    // to generate the first challenge.

    shared_ptr<scidb_msg::AuthLogon> record =
        _messageDesc->getRecord<scidb_msg::AuthLogon>();

    AuthMethod method = AUTH_NONE;
    string auth_tag;
    if (record->has_authtag()) {
        auth_tag = record->authtag();
        method = auth::tagToMethod(auth_tag);
    }

    // Backward compatibility for harness tests etc.
    string username(record->username());
    if (username == rbac::DEPRECATED_DBA_USER) {
        LOG4CXX_TRACE(logger, "Map " << username << " to " << rbac::DBA_USER);
        username = rbac::DBA_USER;
    }

    LOG4CXX_TRACE(logger, "Handling mtAuthLogon for auth=" << auth_tag
                  << " user='" << username
                  << "' on conn=" << hex << _connection << dec
                  << " from " << _connection->getPeerId());

    // Create server-side authenticator, install it in _connection.
    // Throws if _connection already has one.
    AuthenticatorPtr auth =
        Authenticator::create(auth_tag, username, _connection);
    SCIDB_ASSERT(auth);
    SCIDB_ASSERT(_connection->getAuthenticator() == auth);

    // Send non-peer clients an authentication challenge, step 2 in
    // the four-way handshake.
    if (method != AUTH_I2I) {
        sendAuthChallenge(auth);
        return;
    }

    // All instance-to-instance messages including this AuthLogon must
    // pass cluster membership check, see comment in dispatch() above.
    confirmSenderMembership();

    // Instance-to-instance authentication uses just this single
    // message, to avoid a suspected race condition in the
    // mu.error.update_with_kill test (see SDB-5702).  The AuthLogon
    // message must have a proper signature, which we verify here.

    if (!record->has_signature()) {
        LOG4CXX_ERROR(logger, "Internal error: I2I logon lacks signature, disconnecting");
        _connection->flushThenDisconnect();
        return;
    }

    Authenticator::Status status = auth->putResponse(record->signature());
    if (status != Authenticator::ALLOW) {
        LOG4CXX_DEBUG(logger, "Authentication DENIED for peer "
                      << _connection->getPeerId() << " on conn="
                      << hex << _connection << dec
                      << ", reason: " << auth->getFailureReason());
        _connection->flushThenDisconnect();
        return;
    }

    try {
        auth->installSession();
    }
    catch (Exception const& ex) {
        status = Authenticator::DENY; // Revoke good status.
        LOG4CXX_DEBUG(logger, "Authentication DENIED for peer "
                      << _connection->getPeerId() << " on conn="
                      << hex << _connection << dec
                      << ", reason: " << ex.what());
        // Nothing sent back, just close it.
        _connection->flushThenDisconnect();
    }
    if (status == Authenticator::ALLOW) {
        // No exception, all good.
        LOG4CXX_DEBUG(logger, "Authentication granted for peer "
                      << _connection->getPeerId() << " on conn="
                      << hex << _connection << dec);
        _connection->authDone();
    }
}


void AuthMessageHandleJob::sendAuthChallenge(AuthenticatorPtr& auth)
{
    // Ask authentication machinery for first challenge and random cookie.
    Authenticator::Challenge ch = auth->getChallenge();
    Authenticator::Cookie cookie = auth->makeCookie();

    // Prepare the challenge.
    MessageDescPtr msg = make_shared<MessageDesc>(mtAuthChallenge);
    auto rec = msg->getRecord<scidb_msg::AuthChallenge>();
    rec->set_cookie(cookie);
    rec->set_method(auth->getAuthMethod());
    rec->set_code(ch.code);
    rec->set_text(ch.text);

    LOG4CXX_TRACE(logger, "Sending AuthChallenge to " << _connection->getPeerId()
                  << " on conn=" << hex << _connection << dec);

    _connection->sendMessageDisconnectOnError(msg);
}


void AuthMessageHandleJob::handleAuthChallenge()
{
    // NOTE: This is dead code but for now we leave it in so that
    // later we can investigate SDB-5702.

    // We are a client in an AUTH_I2I handshake.  Our AuthResponse
    // needs to prove we are a member of the cluster.

    LOG4CXX_TRACE(logger, "Received AuthChallenge from " << _connection->getPeerId()
                  << " on conn=" << hex << _connection << dec);

    auto challenge = _messageDesc->getRecord<scidb_msg::AuthChallenge>();
    Authenticator::Cookie cookie = challenge->cookie();

    string answer = InstanceAuthenticator::getResponse(challenge->text());

    MessageDescPtr msg = make_shared<MessageDesc>(mtAuthResponse);
    auto response = msg->getRecord<scidb_msg::AuthResponse>();
    response->set_cookie(cookie);
    response->set_text(answer);

    LOG4CXX_TRACE(logger, "Sending AuthResponse to " << _connection->getPeerId()
                  << " on conn=" << hex << _connection << dec);

    _connection->sendMessageDisconnectOnError(msg);
}


void AuthMessageHandleJob::handleAuthResponse()
{
    AuthenticatorPtr auth = _connection->getAuthenticator();
    if (!auth) {
        throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_AUTHENTICATION_ERROR)
            << "Bad handshake: unexpected client response";
    }

    if (logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "Received AuthResponse from "
                      << _connection->getPeerId()
                      << " on conn=" << hex << _connection << dec);
    }

    // Make sure cookie matches!
    shared_ptr<scidb_msg::AuthResponse> rec =
        _messageDesc->getRecord<scidb_msg::AuthResponse>();
    if (rec->cookie() != auth->getCookie()) {
        throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_AUTHENTICATION_ERROR)
            << "Bad handshake: cookie mismatch";
    }

    // Show response to the authenticator and see what it wants to do.
    Authenticator::Status status = Authenticator::DENY;
    try {
        status = auth->putResponse(rec->text());
    }
    catch (Exception& ex) {
        // Some Authenticator subclasses use low-level code that can
        // throw.  Logon failures should look the same to clients, no
        // matter the reason for the failure.  Attackers should not
        // know whether they got the username or the password wrong.
        // (Change the admin login on your home router from "admin" to
        // something else!)
        LOG4CXX_DEBUG(logger, "Authenticator::putResponse raised "
                      << ex.what());
        status = Authenticator::DENY;
    }

    switch (status) {

    case Authenticator::DENY:
        cancelAuthTimer();
        LOG4CXX_DEBUG(logger, "Authentication DENIED for user '"
                      << auth->getUser() << "', auth="
                      << auth::strMethodTag(auth->getAuthMethod()) << ", "
                      << _connection->getPeerId() << " on conn="
                      << hex << _connection << dec
                      << ", reason: " << auth->getFailureReason());
        sendAuthDeny(auth->getFailureReason());
        _connection->flushThenDisconnect();
        break;

    case Authenticator::ALLOW:
        cancelAuthTimer();
        try {
            auth->installSession();

            // Victory!
            LOG4CXX_DEBUG(logger, "Authentication granted for user '"
                          << auth->getUser() << "', auth="
                          << auth::strMethodTag(auth->getAuthMethod()) << ", "
                          << _connection->getPeerId() << " on conn="
                          << hex << _connection << dec);
        }
        catch (Exception const& ex) {
            LOG4CXX_DEBUG(logger, "Authentication DENIED for user '"
                          << auth->getUser() << "', auth="
                          << auth::strMethodTag(auth->getAuthMethod()) << ", "
                          << _connection->getPeerId() << " on conn="
                          << hex << _connection << dec
                          << ", reason: " << ex.what());

            status = Authenticator::DENY; // Revoke good status.
            sendAuthError(ex);
            _connection->flushThenDisconnect();
        }
        if (status == Authenticator::ALLOW) {
            // No exception, transmit success.

            // XXX By sending AuthComplete first we open a window
            // before authDoneInternal runs were an incoming message
            // could be bad.  Might this be the reason for
            // mu.error.update_with_kill failures (that required
            // single-message i2i auth?)

            sendAuthAllow(auth->getRemark());
            _connection->authDone();
            if (auth->isPeerToPeer()) {
                sendHangup();
            }
        }
        break;

    case Authenticator::CHALLENGE:
        LOG4CXX_TRACE(logger, "Yet another challenge must be met");
        sendAuthChallenge(auth);
        break;

    default:
        ASSERT_EXCEPTION_FALSE("Unexpected Authenticator::Status value");
    }
}


void AuthMessageHandleJob::sendAuthAllow(string const& info)
{
    // (No set_cookie() call needed for mtAuthComplete.)
    MessageDescPtr msg = make_shared<MessageDesc>(mtAuthComplete);
    auto rec = msg->getRecord<scidb_msg::AuthComplete>();
    rec->set_authenticated(true);
    if (!info.empty()) {
        // Could be something like "Account expires in <N> days".
        rec->set_reason(info);
        LOG4CXX_INFO(logger, "Sending AuthComplete: ALLOW to "
                     << _connection->getPeerId()
                     << " on conn=" << hex << _connection << dec
                     << " with remark \"" << info << '"');
    }
    else if (logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "Sending AuthComplete: ALLOW to "
                      << _connection->getPeerId()
                      << " on conn=" << hex << _connection << dec);
    }

    _connection->sendMessageDisconnectOnError(msg);
}


// Just sends message, *caller* is responsible for disconnecting.
void AuthMessageHandleJob::sendAuthDeny(string const& reason)
{
    // (No set_cookie() call needed for mtAuthComplete.)
    MessageDescPtr msg = make_shared<MessageDesc>(mtAuthComplete);
    auto rec = msg->getRecord<scidb_msg::AuthComplete>();
    rec->set_authenticated(false);
    if (!reason.empty()) {
        rec->set_reason(reason);
    }

    if (logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "Sending AuthComplete: DENY to "
                      << _connection->getPeerId()
                      << " on conn=" << hex << _connection << dec
                      << ", reason: " << (reason.empty() ? "(none)" : reason));
    }

    _connection->sendMessageDisconnectOnError(msg);
}


void AuthMessageHandleJob::sendAuthError(Exception const& ex)
{
    // Just assume "for client", it only affects the what() string.
    MessageDescPtr msg =
        makeErrorMessageFromExceptionForClient(ex, _messageDesc->getQueryID());
    msg->setMessageType(mtAuthError);

    if (logger->isTraceEnabled()) {
        LOG4CXX_TRACE(logger, "Sending AuthError to " << _connection->getPeerId()
                      << " on conn=" << hex << _connection << dec
                      << ", reason: " << ex.what());
    }

    _connection->sendMessageDisconnectOnError(msg);
}


// Since authenticated client is another instance, it need no longer
// keep reading from this, its outbound connection.  Here we send it
// an mtHangup message to politely clear the readMessage event loop
// from its NetworkManager's internal state.
//
// NOTE: mtHangup is not considered an authentication handshake
// message, and so is pushed onto the *back* of the connection's
// MultiChannelQueue.
//
void AuthMessageHandleJob::sendHangup()
{
    // NOTE: This is dead code but for now we leave it in so that
    // later we can investigate SDB-5702.

    LOG4CXX_TRACE(logger, "Sending mtHangup to " << _connection->getPeerId()
                  << " on conn=" << hex << _connection << dec);

    MessageDescPtr msg = make_shared<MessageDesc>(mtHangup);
    auto rec = msg->getRecord<scidb_msg::Hangup>();
    rec->set_code(0);
    _connection->sendMessageDisconnectOnError(msg);
}


void AuthMessageHandleJob::handleAuthComplete()
{
    // NOTE: This is dead code but for now we leave it in so that
    // later we can investigate SDB-5702.

    LOG4CXX_TRACE(logger, "Handling mtAuthComplete from "
                  << _connection->getPeerId()
                  << " on conn=" << hex << _connection << dec);

    // Client side of AUTH_I2I only!  External clients are using a
    // ClientAuthenticator subclass.

    auto complete = _messageDesc->getRecord<scidb_msg::AuthComplete>();
    if (complete->authenticated()) {
        // We're a winner!
        _connection->authDone();
    } else {
        LOG4CXX_ERROR(logger, "Peer refused my connection: " << complete->reason());
        _connection->disconnect();
    }
}


void AuthMessageHandleJob::startAuthTimer()
{
    ScopedMutexLock lock(s_mutex, PTW_SML_AUTH_MESSAGE_HANDLE_JOB);
    s_inProgress.push_back(_connection);
}


void AuthMessageHandleJob::cancelAuthTimer()
{
    ScopedMutexLock lock(s_mutex, PTW_SML_AUTH_MESSAGE_HANDLE_JOB);
    auto pos = s_inProgress.begin();
    while (pos != s_inProgress.end()) {
        ConnectionPtr connp = pos->lock();
        if (!connp) {
            s_inProgress.erase(pos++);
        } else if (connp == _connection) {
            s_inProgress.erase(pos);
            break;
        } else {
            ++pos;
        }
    }
}


void AuthMessageHandleJob::slowAuthKiller()
{
    static int timeout = -1;
    if (timeout < 0) {
        Config& cfg = *Config::getInstance();
        timeout = cfg.getOption<int>(CONFIG_CLIENT_AUTH_TIMEOUT);
        SCIDB_ASSERT(timeout > 0);
        LOG4CXX_INFO(logger, "Clients must authenticate within "
                     << timeout << " seconds");
    }

    // Atomically grab contents of the in-progress list.
    WeakConnList conns;
    {
        ScopedMutexLock lock(s_mutex, PTW_SML_AUTH_MESSAGE_HANDLE_JOB);
        conns.swap(s_inProgress);
    }
    if (conns.empty()) {
        return;                 // Nothing to do.
    }


    // Run through the list and decide whether to disconnect or keep
    // the connection.
    time_t now = getCoarseTimestamp();
    WeakConnList keep;
    for (auto pos = conns.begin(); pos != conns.end(); ++pos) {
        ConnectionPtr connp = pos->lock();
        if (!connp) {
            continue;
        }

        time_t delta = now - connp->getLastRecv();
        if (delta < timeout) {
            // Timestamps are assigned with the monotonic clock, and
            // s_inProgress is kept in FIFO order, so we are done.
            // Keep the rest.
            keep.assign(pos, conns.end());
            break;
        }

        // Seems worthy of at least INFO-level logging: could be a DoS attack.
        LOG4CXX_WARN(logger, "Client " << connp->getPeerId()
                     << " login timed out, connection closed"); // dz client这里超时了，实际遇到了
        connp->disconnect();
    }

    // Atomically put the keepers back at the front of the list
    // (maintaining FIFO order).
    {
        ScopedMutexLock lock(s_mutex, PTW_SML_AUTH_MESSAGE_HANDLE_JOB);
        s_inProgress.splice(s_inProgress.begin(), keep);
    }
}

} // namespace scidb
