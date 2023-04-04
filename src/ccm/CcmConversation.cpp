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
#include "CcmConversation.h"
// header files from the ccm module
#include "CcmConnection.h"
#include "CcmConversationState.h"
#include "CcmSession.h"
#include "Uuid.h"
// third-party libraries
#include <log4cxx/logger.h>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmConversation"));
}

namespace scidb { namespace ccm {

CcmConversation::CcmConversation(CcmSession& cs)
    : _state(std::make_unique<UnauthConvState>(*this))
    , _id(cs.getId())  // Just use the sessionId for now.
    , _queryResult()
    , _cur()
    , _ccmSession(cs)
{
    // There is no reason to use a separate conversation Uuid, There cannot be multiple
    // queries per session. So we're just reusing that, rather than deal with maintaining
    // and updating a separate conversation.
    LOG4CXX_TRACE(logger, "    - CcmConversationId is " << _id);
}

CcmConversation::~CcmConversation() noexcept
{
    LOG4CXX_TRACE(logger, "    - Destructing CcmConversation: " << _id);
}

void CcmConversation::processAuthLogon(std::shared_ptr<CcmConnection>& ccon, msg::AuthLogon& rec)
{
    _state->printProcessingMsg("AuthLogon");
    _state->processAuthLogon(ccon, rec);
}

void CcmConversation::processAuthResponse(std::shared_ptr<CcmConnection>& ccon, msg::AuthResponse& rec)
{
    _state->printProcessingMsg("AuthResponse");
    _state->processAuthResponse(ccon, rec);
}

void CcmConversation::processExecuteQuery(std::shared_ptr<CcmConnection>& ccon, msg::ExecuteQuery& rec)
{
    _state->printProcessingMsg("ExecuteQuery");
    _state->processExecuteQuery(ccon, rec);
}

void CcmConversation::processFetchIngot(std::shared_ptr<CcmConnection>& ccon, msg::FetchIngot& rec)
{
    _state->printProcessingMsg("FetchIngot");
    _state->processFetchIngot(ccon, rec);
}

void CcmConversation::onSessionExit() noexcept
{
    _state->onSessionExit();
}

Uuid CcmConversation::getId() const
{
    return _id;
}

CcmSession& CcmConversation::getSession()
{
    return _ccmSession;
}

void CcmConversation::setQueryResult(std::shared_ptr<bridge::IngotProducer> result)
{
    _queryResult = result;
    _cur = _queryResult->begin();
}

std::shared_ptr<bridge::IngotProducer> CcmConversation::getQueryResult()
{
    return _queryResult;
}

bridge::IngotProducer::iterator& CcmConversation::getCur()
{
    return _cur;
}

void CcmConversation::_changeState(std::shared_ptr<CcmConversationState> newState)
{
    auto previous = _state;
    LOG4CXX_TRACE(logger,
                  "    - Changing CcmConversation State"
                      << " from '" << _state->getStateName() << "'"
                      << " to '" << newState->getStateName() << "'");
    _state = newState;
}

}}  // namespace scidb::ccm
