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
#ifndef CCM_CONVERSATION_H_
#define CCM_CONVERSATION_H_

// header files from the ccm module
#include "Uuid.h"
#include "CcmBridge.h"  // for bridge::IngotProducer::iterator
// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {
class CcmConnection;
class CcmConversationState;
class CcmSession;
namespace msg {
class AuthLogon;
class AuthResponse;
class ExecuteQuery;
class FetchIngot;
}  // namespace msg

/**
 *
 * @ingroup Ccm
 */
class CcmConversation
{
    friend class CcmConversationState;

  public:
    explicit CcmConversation(CcmSession&);
    ~CcmConversation() noexcept;

    void processAuthLogon(std::shared_ptr<CcmConnection>& ccon, msg::AuthLogon& rec);
    void processAuthResponse(std::shared_ptr<CcmConnection>& ccon, msg::AuthResponse& rec);
    void processExecuteQuery(std::shared_ptr<CcmConnection>& ccon, msg::ExecuteQuery& rec);
    void processFetchIngot(std::shared_ptr<CcmConnection>& ccon, msg::FetchIngot& rec);

    void onSessionExit() noexcept;

    Uuid getId() const;
    CcmSession& getSession();
    void setQueryResult(std::shared_ptr<bridge::IngotProducer> result);
    std::shared_ptr<bridge::IngotProducer> getQueryResult();
    bridge::IngotProducer::iterator& getCur();

  private:
    void _changeState(std::shared_ptr<CcmConversationState> newState);

    std::shared_ptr<CcmConversationState> _state;
    Uuid _id;
    std::shared_ptr<bridge::IngotProducer>
        _queryResult;  //!< The result of the currently active query (one or none)
    bridge::IngotProducer::iterator
        _cur;                 //!< The current location with in the response to retrieve next
    CcmSession& _ccmSession;  //!< The session which owns this CcmConversation
};

}}  // namespace scidb::ccm

#endif
