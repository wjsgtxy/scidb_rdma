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
#include "CcmConversationState.h"
// header files from the ccm module
#include "CcmBridge.h"
#include "CcmConnection.h"
#include "CcmConversation.h"
#include "CcmErrorCode.h"
#include "CcmMsgType.h"
#include "CcmSession.h"
#include "CcmSessionState.h"
// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
#include <system/Exceptions.h>
// third-party libraries
#include <log4cxx/logger.h>
// c++ standard libraries
#include <iostream>
#include <string>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmConversationState"));
}

namespace scidb { namespace ccm {

//
// CcmConversationState
//

CcmConversationState::CcmConversationState(CcmConversation& conversation)
    : _conversation(conversation)
{}

CcmConversationState::~CcmConversationState() noexcept
{
    LOG4CXX_TRACE(logger, "    - Destructing " << _stateName << ".");
}

void CcmConversationState::processAuthLogon(std::shared_ptr<CcmConnection>, msg::AuthLogon&)
{
    ASSERT_EXCEPTION(false, "Unexpected/Unimplemented processAuthLogon call for state: " << _stateName);
}

void CcmConversationState::processAuthResponse(std::shared_ptr<CcmConnection>, msg::AuthResponse&)
{
    ASSERT_EXCEPTION(false,
                     "Unexpected/Unimplemented processAuthResponse call for state: " << _stateName);
}

void CcmConversationState::processExecuteQuery(std::shared_ptr<CcmConnection>, msg::ExecuteQuery&)
{
    ASSERT_EXCEPTION(false,
                     "Unexpected/Unimplemented processExecuteQuery call for state: " << _stateName);
}

void CcmConversationState::processFetchIngot(std::shared_ptr<CcmConnection>, msg::FetchIngot&)
{
    ASSERT_EXCEPTION(false, "Unexpected/Unimplemented processFetchIngot call for state: " << _stateName);
}

void CcmConversationState::onSessionExit() noexcept
{
    bool success = bridge::destroy(getSession().getId());
    if (success) {
        LOG4CXX_TRACE(logger,
                      "    - Destroyed southbound session connection  (" << getSession().getId()
                                                                         << ") -- removed");
    } else {
        // It's possible and okay that no southbound existed (at least in Phase 1 -- since
        // the southbound is not established until both username and password have been
        // obtained from the client).  For instance an Session where AuthLogon was given,
        // and then the connection closed and the session timed out (no password
        // given so southbound connection never established).
        LOG4CXX_TRACE(logger,
                      "    - Destroyed southbound session connection (" << getSession().getId()
                                                                        << ") -- NOT Found");
    }
}

void CcmConversationState::changeState(std::shared_ptr<CcmConversationState> newState)
{
    _conversation._changeState(newState);
}

void CcmConversationState::changeSessionState(std::shared_ptr<CcmSessionState> newState)
{
    getSession()._changeState(newState);
}

CcmSession& CcmConversationState::getSession()
{
    return _conversation.getSession();
}

void CcmConversationState::printProcessingMsg(const std::string& msgName) const
{
    LOG4CXX_TRACE(logger, "    - " << getStateName() << ":  Processing " << msgName);
}

//
// UnauthConvState
//
UnauthConvState::UnauthConvState(CcmConversation& conversation)
    : CcmConversationState(conversation)
{
    _stateName = "UnauthConvState";
}

void UnauthConvState::processAuthLogon(std::shared_ptr<CcmConnection> ccon, msg::AuthLogon& rec)
{
    // // TODO (Phase 2)
    // - The southbound bridge does not support/expose the four way handshake.
    //   so we will capture the username from the Authlogon, and just immediately request
    //   the response from the client. As a result, we do not support multiple
    //   authchallenge/authresponse exchanges.
    // auto authStatus = bridge::authlogon(_conversation.getSession().getId(),
    //                                     _conversation.getId());
    //
    // So just move to StartingSessionState and StartingConvState
    auto newSessionState = std::make_shared<StartingSessionState>(getSession());
    auto newState = std::make_shared<StartingConvState>(_conversation);

    Uuid cookie(Uuid::create());
    newState->setCookie(cookie);
    if (rec.has_username()) {
        newState->setUsername(rec.username());
    }

    auto message = std::make_shared<msg::AuthChallenge>();
    message->set_challenge("Can you Send an authresponse now?");
    message->set_cookie(cookie);
    ccon->sendResponse(CcmMsgType::AuthChallengeMsg, message);

    changeSessionState(newSessionState);
    changeState(newState);
    // Nothing more should be done.
    // Once the conversation state has changed, ~this~ is no longer valid.
}

// StartingConvState
StartingConvState::StartingConvState(CcmConversation& conversation)
    : CcmConversationState(conversation)
{
    _stateName = "StartingConvState";
}

void StartingConvState::processAuthResponse(std::shared_ptr<CcmConnection> ccon,
                                                    msg::AuthResponse& rec)
{
    // (Phase2..3)
    // When the interaction to the Southbound does not batch up the user/name password
    // and we are actually using the cookie given by the Southbound, we will pass that
    // cookie to the southbound::AuthResponse and THAT will give us back an error which we
    // will send to the client. Right now this cookie is somewhat of a dummy, but not a
    // bad idea since the client interface will need it in the next phases.
    if (!_cookie.empty() && rec.has_cookie() && (rec.cookie() == _cookie)) {
        LOG4CXX_TRACE(logger, "    -     We have a cookie match");
    } else {
        ASSERT_EXCEPTION(!_cookie.empty(), "No client authentication cookie set");

        std::string cookie;
        if (rec.has_cookie()) {
            cookie = rec.cookie();
        } else {
            cookie = "(no cookie)";
        }
        LOG4CXX_TRACE(logger, "    -     Ooops. Cookies don't match. Record Cookie is " << cookie);
        // Send an error:
        ccon->sendError(CcmErrorCode::AUTH_COOKIE_MISMATCH);
        // Return to the Unauthenticated state
        auto newSessionState = std::make_shared<UnauthSessionState>(getSession());
        auto newState = std::make_shared<UnauthConvState>(_conversation);
        changeSessionState(newSessionState);
        changeState(newState);
        return;
    }

    // We, unfortunately, here and throughout the entire code base use a string to store
    // the password.
    // As MJL points out in "safebuffer", there is a great deal of work needed to make
    // passing of passwords in a truly safe manner (a quixotic endeavor).
    std::string password;
    if (rec.has_answer()) {
        password = rec.answer();
    }

    LOG4CXX_TRACE(logger,
                  "    -     We HAVE Username from earlier '" << _username << "'"
                                                              << "    -     And now we have  '"
                                                              << "TODO:HashPassword"
                                                              << "'");

    // TODO (Phase 2) SessionPriority settings and additional required messages
    bool isAuthed = bridge::authenticate(_username,
                                         password,
                                         getSession().getId(),
                                         scidb::SessionProperties::NORMAL);

    auto message = std::make_shared<msg::AuthComplete>();
    message->set_authenticated(isAuthed);
    if (isAuthed) {
        message->set_reason("Success");
    } else {
        message->set_reason("Invalid username or password");
    }

    ccon->sendResponse(CcmMsgType::AuthCompleteMsg, message);

    if (isAuthed) {
        auto newSessionState = std::make_shared<AuthedSessionState>(getSession());
        auto newState = std::make_shared<AuthedConvState>(_conversation);

        changeSessionState(newSessionState);
        changeState(newState);
    } else {
        // Reset the session and conversation to the Unauth State.
        auto newSessionState = std::make_shared<UnauthSessionState>(getSession());
        auto newState = std::make_shared<UnauthConvState>(_conversation);

        changeSessionState(newSessionState);
        changeState(newState);
    }
}

//
// AuthedConvState
//
AuthedConvState::AuthedConvState(CcmConversation& conversation)
    : CcmConversationState(conversation)
{
    _stateName = "AuthedConvState";
}

void AuthedConvState::processFetchIngot(std::shared_ptr<CcmConnection> ccon,
                                                       msg::FetchIngot& /*rec*/)
{
    // Phase 1 note:
    // - In Phase 1, a query NEVER has more than one ingot because the entire array is
    //      written into the single string by ArrayWriter.
    // SO one of the following probably happened:
    //    1. The query was a failure (and the error message was ignored by the client which sent
    //       a fetchIngot anyway.
    //    2. A fetchIngot was called without calling ExecuteQuery (that resulted in success) first.
    ccon->sendError(CcmErrorCode::NOQUERY_FETCH);
    // It's an error but not a session error, so no reason to change the CcmSessionState nor
    // the CcmConversationState
}

void AuthedConvState::processExecuteQuery(std::shared_ptr<CcmConnection> ccon,
                                                         msg::ExecuteQuery& message)
{
    // The client will give us the AFL. AFL is currently the only language support (with
    // the addition of the AQL statemente: 'CREATE ARRAY' )
    std::string afl = message.query();
    std::string format = "dcsv";
    if (message.has_ingot_format()) {
        format = message.ingot_format();
    }

    LOG4CXX_TRACE(logger, "    - AFL :" << afl);

    // Call the bridge to execute the query for us.  The Phase 1 API
    // will block while SciDB processes the query and the result
    // will be populated into queryResult.

    // TODO (Phase 2): In the next phase of development, the execute call will be
    //                 non-blocking, and the query can begin executing, and the CCM can
    //                 respond that the query is executing.  In that case it would be the
    //                 FetchIngot call that probably blocks.
    auto queryResult = bridge::executeQuery(afl, format, _conversation.getSession().getId());

    bool success = queryResult->success();
    bool has_data = true;
    if (!success) {
        ccon->sendError(CcmErrorCode::INVALID_QUERY);
        // TODO (Phase 2): We should set the  location within the string where the
        //      syntax error occurred. (maybe an Error has a key/value map of additional
        //      information -- maybe I'm mixing abstraction layers?)
        // IN the case of (most) errors, we will not change the Session State
        //  and the CcmConversationState would remain in Authenticated.
    } else if (!has_data) {
        // TODO (Phase 2): The current idea is that we will be able to determine if the
        // executeQuery has any data that it needs to return:
        // 1. DDL and store/inserts which do not return data to the client can return the
        //    AuthedConvState. (or even queries which are 'non-selective')
        //    - IF we do this, then the processFetchIngot above will need to return
        //      a. No Data for Query
        //      b. or No such query. (if these things are different).
        //    IF the query is executed successfully to completion and there is no data
        //    Then we do not change the CcmSessionState, NOR do we change the CcmConversationState
    } else {
        _conversation.setQueryResult(queryResult);
        // 2. queries which have data that needs to be fetched (via fetchIngot) will to the
        //    executingQuery
        auto response = std::make_shared<msg::ExecuteQueryResponse>();
        // Right now the ccmConversationId between the client and Ccm is actually the
        // the ccmSessionId. We don't allow for multiple conversations per session.
        response->set_conversation_id(_conversation.getId());
        ccon->sendResponse(CcmMsgType::ExecuteQueryResponseMsg, response);

        // dz add 用于统计查询时间
        LOG4CXX_FATAL(logger, "dzs: query success.");

        // The Session State remains unchanged. We're still Authenticated.
        // The Conversion State goes into the Executing State.
        auto newState = std::make_shared<ExecConvState>(_conversation);
        changeState(newState);
    }
}

//
// ExecConvState
//

ExecConvState::ExecConvState(CcmConversation& conversation)
    : CcmConversationState(conversation)
{
    _stateName = "ExecConvState";
}

void ExecConvState::processExecuteQuery(std::shared_ptr<CcmConnection> ccon,
                                                     msg::ExecuteQuery& /*rec*/)
{
    // There is an unfinished CcmConversation, so return a Warning/Error
    // to the client to that effect.
    // If a client wants multiple queries, then the client will need to open multiple
    // Sessions. (Which can even be on the same connection).
    ccon->sendError(CcmErrorCode::BUSY);
}

void ExecConvState::processFetchIngot(std::shared_ptr<CcmConnection> ccon,
                                                   msg::FetchIngot& msg)
{
    // std::string convId = _conversation.getId();
    // if (!msg.has_conversation_id() || msg.conversation_id() == convId) {
    //     std::ostringstream os;
    //     os << "CcmConversation identifiers do not match.";
    //     if (msg.has_conversation_id()) {
    //         os << " client conversation_id: " << msg.conversation_id();
    //     }
    //     os << " CcmConversationId: " << convId;
    //     LOG4CXX_WARN(logger, os.str());
    //     ccon->sendError(CcmErrorCode::FETCH_BAD_ID);
    //     return;
    // }

    auto queryResult = _conversation.getQueryResult();
    auto resultIter = _conversation.getCur();
    std::string resultPayload;
    if (resultIter != queryResult->end()) {
        // A Note about deferencing the iterator.
        // This may block until some point that the back-end (southbound) can
        // return multiple data ingots.
        //
        // To truly do a non-blocking call we need to implement the "client give me a
        // socket and I'll pump data to you" feature being added to the client
        // communications manager. The design of such a feature has not begun as of yet.
        resultPayload = *resultIter;
        ++resultIter;

    } else {
        // Nothing more to give

        // TODO (Phase 2): This would be an error, but the IngotProducer::iterator is not close
        //                 to being done. The IngotProducer is just a wrapper around a string
        //                 and the "end" is an empty string.
        //
        // For Phase 1: we need to return an empty string, when we have a successful query
        //             with no data.. And we have to go with the assumption that if the
        //             CcmConversationState is in Executing, that means we have a
        //             successfully executed query. In the case of a successful DDL query
        //             we have no array to send to the arraywriter, so there is no way a
        //             string can be generated.
        //
    }

    auto message = std::make_shared<msg::FetchIngotResponse>();
    message->set_conversation_id(_conversation.getId());
    message->set_binary_size(resultPayload.size());
    message->set_more(false);
    ccon->sendResponse(CcmMsgType::FetchIngotResponseMsg, message, resultPayload);

    // Phase 1 ONLY
    SCIDB_ASSERT(resultIter == queryResult->end());

    if (resultIter == queryResult->end()) {
        // We have now sent all the ingots for the queryResult. So back to the
        // 'authenticated'(ready for the next query) state
        // auto newState = std::make_unique<AuthedConvState>(_conversation);
        // changeState(std::move(newState));
        auto newState = std::make_shared<AuthedConvState>(_conversation);
        changeState(newState);
    }
}

}}  // namespace scidb::ccm
