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
#include "CcmSession.h"
// header files from the ccm module
#include "CcmConnection.h"
#include "CcmConversation.h"
#include "CcmSessionState.h"
// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
// third-party libraries
#include <log4cxx/logger.h>
// c++ standard libraries
#include <iostream>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmSession"));
}

namespace scidb { namespace ccm {

CcmSession::~CcmSession() noexcept
{
    try {
        LOG4CXX_TRACE(logger, "  - Destructing Session: " << _id);
        if (_conversation) {
            _conversation->onSessionExit();
            _conversation.reset();
        } else {
            LOG4CXX_TRACE(logger, "  -       _conversation already nullptr ");
        }
    } catch (...) {
        // DEEP Paranoia. Just in case LOG4CXX_TRACE throws
        // ignore
    }
}

CcmSession::CcmSession(Uuid id, long timeOut)
    : _id(id)
    , _timeOut(timeOut)
    , _state(std::make_unique<UnauthSessionState>(*this))
    , _conversation(nullptr)
{}

void CcmSession::processAuthLogon(std::shared_ptr<CcmConnection> ccon, msg::AuthLogon& rec)
{
    _state->printProcessingMsg("AuthLogon");
    // TODO (Phase 2) if rec contains new sessionTimeOut value set it
    // if (rec.has_time_out()) {
    //     sessionTimeOut(rec.time_out());
    // }
    // IF we use properties then
    // this->processSessionSettings(rec.getProperties());
    //  // or however google protocol buffers deals with "inner messages"
    _state->processAuthLogon(ccon, rec);
}

void CcmSession::processAuthResponse(std::shared_ptr<CcmConnection> ccon, msg::AuthResponse& rec)
{
    _state->printProcessingMsg("AuthResponse");
    _state->processAuthResponse(ccon, rec);
}

void CcmSession::processExecuteQuery(std::shared_ptr<CcmConnection> ccon, msg::ExecuteQuery& rec)
{
    _state->printProcessingMsg("ExecuteQuery");
    _state->processExecuteQuery(ccon, rec);
}

void CcmSession::processFetchIngot(std::shared_ptr<CcmConnection> ccon, msg::FetchIngot& rec)
{
    _state->printProcessingMsg("FetchIngot");
    _state->processFetchIngot(ccon, rec);
}

const Uuid& CcmSession::getId() const
{
    return _id;
}

long CcmSession::getTimeOut() const
{
    return _timeOut;
}

void CcmSession::setTimeOut(long timeOut)
{
    _timeOut = timeOut;
}

void CcmSession::_changeState(std::shared_ptr<CcmSessionState> newState)
{
    LOG4CXX_TRACE(logger,
                  "  - Changing Session State"
                      << " from '" << _state->getStateName() << "'"
                      << " to '" << newState->getStateName() << "'");
    _state = newState;
}

std::shared_ptr<CcmConversation> CcmSession::_createConversation()
{
    // We only allow a single on-going CcmConversation for a given Ccm session.
    //    - Possibly in the future we will allow for multiple conversations, that is to
    //      say, allow a session to run multiple queries (from for example the same Remote
    //      Client connection)

    // It is possible that an existing conversation existed (if for example Authentication failed.
    // and both the session and conversation were reset to the
    // UnauthSessionState/UnauthConvState. In such a case we replace the conversaion now.
    if (_conversation) {
        LOG4CXX_TRACE(logger, "  - Abandoning CcmConversation: " << _conversation->getId());
    }
    _conversation = std::make_shared<CcmConversation>(*this);
    LOG4CXX_TRACE(logger, "  -   Creating conversation: " << _conversation->getId());
    return _conversation;
}

void CcmSession::_cancelConversation()
{
    _conversation.reset();
}

std::shared_ptr<CcmConversation> CcmSession::_getConversation()
{
    return _conversation;
}

}}  // namespace scidb::ccm
