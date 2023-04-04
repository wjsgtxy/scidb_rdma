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
#include "CcmSessionState.h"
// header files from the ccm module
#include "CcmConnection.h"
#include "CcmConversation.h"
#include "CcmErrorCode.h"
#include "CcmMsgType.h"
#include "CcmSession.h"
// SciDB modules
#include <system/Exceptions.h>
#include <system/Utils.h>  // For SCIDB_ASSERT
// third-party libraries
#include <log4cxx/logger.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmSessionState"));
}

namespace scidb { namespace ccm {

//
// CcmSessionState
//
CcmSessionState::CcmSessionState(CcmSession& cs)
    : _session(cs)
{}

CcmSessionState::~CcmSessionState() noexcept
{
    LOG4CXX_TRACE(logger, "  - Destructing " << getStateName() << ".");
}

std::shared_ptr<CcmConversation> CcmSessionState::createConversation()
{
    return _session._createConversation();
}

void CcmSessionState::cancelConversation()
{
    _session._cancelConversation();
}

std::shared_ptr<CcmConversation> CcmSessionState::getConversation()
{
    return _session._getConversation();
}

void CcmSessionState::printProcessingMsg(const std::string& msgName) const
{
    LOG4CXX_TRACE(logger, "  - " << getStateName() << ":  Processing " << msgName);

    // dz add
    LOG4CXX_DEBUG(logger, "  - " << getStateName() << ":  Processing " << msgName);
}

//
// UnauthSessionState
//
UnauthSessionState::UnauthSessionState(CcmSession& cs)
    : CcmSessionState(cs)
{
    _stateName = "UnauthSessionState";
}

void UnauthSessionState::processAuthLogon(std::shared_ptr<CcmConnection>& ccon, msg::AuthLogon& rec)
{
    createConversation()->processAuthLogon(ccon, rec);
}

void UnauthSessionState::processAuthResponse(std::shared_ptr<CcmConnection>& ccon,
                                             msg::AuthResponse& /*rec*/)
{
    // The Session is still in the initial state, so this is an unexpected
    // message. (AuthLogon for this session was not been called first).
    LOG4CXX_TRACE(logger, "UnauthSessionState::processAuthResponse -- SesssionID: " << _session.getId());
    if (!getConversation()) {
        LOG4CXX_TRACE(logger,
                      "Attempt to AuthResponse without (re)starting the 4-way handshake with AuthLogon");
        ccon->removeSession();
    } else {
        LOG4CXX_TRACE(logger, "Attempt to AuthResponse after a failed authentication attempt.");
    }
    ccon->sendError(CcmErrorCode::RESTART_HANDSHAKE);
}

void UnauthSessionState::processExecuteQuery(std::shared_ptr<CcmConnection>& ccon,
                                             msg::ExecuteQuery& /*rec*/)
{
    // This execute query request was sent
    //   1. after the session timed out, or as the first message without ever beginning
    //      the 4-way handshake. (a _conversation was never started). There is no reason
    //      to keep the session identifier that was generated when this message arrived.
    //   2. a failed authentication attempt (a conversation was started, so we'll keep
    //      allow the conversation/session to continue (until it times out at least).
    if (!getConversation()) {
        LOG4CXX_TRACE(logger, "Attempt to execute query without Authenticating first");
        // No reason to keep this session, the client has never seen it.
        ccon->removeSession();
    } else {
        LOG4CXX_TRACE(logger, "Attempt to execute query after a failed authentication attempt.");
    }

    ccon->sendError(CcmErrorCode::UNAUTH_EXECUTE);
}

void UnauthSessionState::processFetchIngot(std::shared_ptr<CcmConnection>& ccon,
                                           msg::FetchIngot& /*rec*/)
{
    // This FetchIngot request was sent
    //   1. after the session timed out, or as the first message without ever beginning
    //      the 4-way handshake. (a _conversation was never started). There is no reason
    //      to keep the sessionId that was generated when this message arrived.
    //   2. a failed authentication attempt (a conversation was started, so we'll keep
    //      allow the conversation/session to continue (until it times out at least).
    if (!getConversation()) {
        LOG4CXX_TRACE(logger, "Attempt to fetch ingot without Authenticating first");
        // No reason to keep this session, the client has never seen it.
        ccon->removeSession();
    } else {
        LOG4CXX_TRACE(logger, "Attempt to fetch ingot after a failed authentication attempt.");
    }
    ccon->sendError(CcmErrorCode::UNAUTH_FETCH);
}

//
// StartingSessionState
//
StartingSessionState::StartingSessionState(CcmSession& cs)
    : CcmSessionState(cs)
{
    _stateName = "StartingSessionState";
}

void StartingSessionState::processAuthLogon(std::shared_ptr<CcmConnection>& ccon, msg::AuthLogon& rec)
{
    // The User has (inadvertently?) sent a second authlogon using an existing session...
    // It's difficult to know the exact intent of the client.
    // AuthLogon is a essentially a "request new session start", however, the existing
    // session's timer was reset before this function was called.
    //  1. Keep this CcmSession/CcmConversation in the current state (it may time out later...)
    //  2. Create a new session and send back an authchallenge for the new
    //     session.

    // TODO (Phase 2): Check if there is a requested session time-out in the
    //     rec.properties. and pass that as the time-out.
    // TODO (Phase 2):
    //     Since this an 'somewhat unexpected' message so one other possibility is to
    //     reduce the time-out on the new session. For now we'll use the default time-out
    //     configured in the CcmConnection
    std::shared_ptr<CcmSession> secondSession = ccon->createSession();
    secondSession->processAuthLogon(ccon, rec);
}

void StartingSessionState::processAuthResponse(std::shared_ptr<CcmConnection>& ccon,
                                               msg::AuthResponse& rec)
{
    std::shared_ptr<CcmConversation> conversation = getConversation();
    conversation->processAuthResponse(ccon, rec);
}

void StartingSessionState::processExecuteQuery(std::shared_ptr<CcmConnection>& ccon,
                                               msg::ExecuteQuery& /*rec*/)
{
    // Client can't execute a query yet.
    //
    // Leave the session in the starting state Allowing for the user to send the
    // authresponse (assuming that the client still has the cookie for the session)
    ccon->sendError(CcmErrorCode::EXEC_IN_START);
}

void StartingSessionState::processFetchIngot(std::shared_ptr<CcmConnection>& ccon,
                                             msg::FetchIngot& /*rec*/)
{
    ccon->sendError(CcmErrorCode::FETCH_IN_START);
}

//
// AuthedSessionState
//
AuthedSessionState::AuthedSessionState(CcmSession& cs)
    : CcmSessionState(cs)
{
    _stateName = "AuthedSessionState";
}

void AuthedSessionState::processAuthLogon(std::shared_ptr<CcmConnection>& ccon,
                                                 msg::AuthLogon& rec)
{
    // Unexpected AuthLogon for an already existing and active session.
    // Perhaps this is a "keep alive" (which is a not uncommon in other protocols)
    //
    // Possible Responses:
    // 1. Send AuthComplete message based upon the username in the rec and the username specified
    //    when the session completed authentication.
    //    - if match, then send AuthComplete(success)
    //    - if they do NOT, then send an AuthComplete(false), and remove the session.
    // 2. Start a new session, leaving the current session (which has had the session timer reset).
    //    The client did just ask for a new session (by sending an AuthLogon)
    //
    // We use Option 2.
    LOG4CXX_INFO(logger,
                 "AuthLogon received for already authenticated session: "
                     << _session.getId() << " being terminated. Starting a new Session authentication.");

    std::shared_ptr<CcmSession> secondSession = ccon->createSession();
    secondSession->processAuthLogon(ccon, rec);
    // Do not change the state of `this` original session(state) specified in the message
    // header.
}

void AuthedSessionState::processAuthResponse(std::shared_ptr<CcmConnection>& ccon,
                                                    msg::AuthResponse& /*rec*/)
{
    // After Starting an AuthResponse doesn't make sense.
    // Inform the client, but do not remove the session.
    ccon->sendError(CcmErrorCode::UNEXPECTED_MSGTYPE);
}

void AuthedSessionState::processExecuteQuery(std::shared_ptr<CcmConnection>& ccon,
                                                    msg::ExecuteQuery& rec)
{
    std::shared_ptr<CcmConversation> conversation = getConversation();
    conversation->processExecuteQuery(ccon, rec);
}

void AuthedSessionState::processFetchIngot(std::shared_ptr<CcmConnection>& ccon,
                                                  msg::FetchIngot& rec)
{
    std::shared_ptr<CcmConversation> conversation = getConversation();
    conversation->processFetchIngot(ccon, rec);
}

}}  // namespace scidb::ccm
