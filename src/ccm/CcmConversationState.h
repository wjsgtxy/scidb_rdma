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
#ifndef CCM_CONVERSATION_STATE_H_
#define CCM_CONVERSATION_STATE_H_

// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {
class CcmConnection;
class CcmConversation;
class CcmSession;
class CcmSessionState;
namespace msg {
class AuthLogon;
class AuthResponse;
class ExecuteQuery;
class FetchIngot;
}  // namespace msg

/**
 * @brief The current state of a conversion between the Ccm and the SciDB back-end.
 *
 * @ingroup Ccm
 *
 * A CcmConversation corresponds to a database transaction running on the SciDB back-end. In the
 * future, if/when transactions consisting of multiple statements is implemented
 * in SciDB, the CcmConversation will correspond to that entire transaction.
 *
 * A CcmConversation can be in one of four states:
 *
 *   - UnauthConvState
 *
 *   - StartingConvState
 *
 *   - AuthedConvState
 *
 *   - ExecConvState
 *
 * Using a state machine pattern allows for the easy addition of new states and means of
 * processing new (protobuf) messages as the client communication manager is extended.
 *
 * @see Behavioral Patterns: State.
 *        Design patterns: elements of reusable object-oriented software.
 *        Addison-Wesley Longman Publishing Co., Inc. Boston, MA, USA ©1995
 *        ISBN:0-201-63361-2
 */
class CcmConversationState
{
  public:
    explicit CcmConversationState(CcmConversation& conversation);
    virtual ~CcmConversationState() noexcept;

    virtual void processAuthLogon(std::shared_ptr<CcmConnection> ccon, msg::AuthLogon& rec);
    virtual void processAuthResponse(std::shared_ptr<CcmConnection> ccon, msg::AuthResponse& rec);
    virtual void processExecuteQuery(std::shared_ptr<CcmConnection> ccon, msg::ExecuteQuery& rec);
    virtual void processFetchIngot(std::shared_ptr<CcmConnection> ccon, msg::FetchIngot& rec);

    virtual void onSessionExit() noexcept;

    // For Debugging messages
    void printProcessingMsg(const std::string& msgName) const;
    std::string getStateName() const { return _stateName; }

  protected:
    CcmSession& getSession();

    /**
     *
     * Change the state (destroying @c this and setting a new
     * CcmConversionState) of the owning CcmConversation.
     *
     * @param newState The next state of the owning @c _conversation
     *
     * @warning Once the conversation state has changed, ~this~ is no longer
     *          valid past the end of the function that calls @c changeState().
     *
     */
    void changeState(std::shared_ptr<CcmConversationState> newState);
    void changeSessionState(std::shared_ptr<CcmSessionState> newState);

    CcmConversation& _conversation;
    std::string _stateName;
};

/**
 *  @class  UnauthConvState
 *
 *   A new (or expired) conversion state requiring the start of the 4-way
 *   handshake. This state mirrors the CcmSession Unauthenticated state, as the
 *   Conversation is communicating with SciDB to authentication on behalf of the
 *   Session.
 */
class UnauthConvState final : public CcmConversationState
{
  public:
    explicit UnauthConvState(CcmConversation& conversation);
    virtual ~UnauthConvState() noexcept = default;

    void processAuthLogon(std::shared_ptr<CcmConnection> ccon, msg::AuthLogon& rec) override;
};

/**
 * @class StartingConvState
 *
 * A conversation in which the client has initiated the first part of the 4-way
 * authentication process. This state mirrors the CcmSession Unauthenticated
 * state, as the Conversation is communicating with SciDB to authentication on
 * behalf of the Session.
 */
class StartingConvState final : public CcmConversationState
{
  public:
    explicit StartingConvState(CcmConversation& conversation);
    virtual ~StartingConvState() noexcept = default;

    void processAuthResponse(std::shared_ptr<CcmConnection> ccon, msg::AuthResponse& rec) override;

    void setCookie(std::string cookie) { _cookie = cookie; }
    void setUsername(std::string username) { _username = username; }

  private:
    std::string _cookie;
    std::string _username;
};

/**
 * @class AuthedConvState
 *
 * A conversation in which the client has completed the 4-way handshake. The
 * conversation is ready to begin processing ExecuteQuery messages. A
 * conversation enters this state along with the CcmSession initially upon
 * successful Authentication.
 */
class AuthedConvState final : public CcmConversationState
{
  public:
    explicit AuthedConvState(CcmConversation& conversation);
    virtual ~AuthedConvState() noexcept = default;

    void processFetchIngot(std::shared_ptr<CcmConnection> ccon, msg::FetchIngot& rec) override;
    void processExecuteQuery(std::shared_ptr<CcmConnection> ccon, msg::ExecuteQuery&) override;
};

/**
 * @class ExecConvState
 *
 * A conversation is actively executing a query and/or waiting for the client to
 * retrieve additional result data. When a query has thoroughly processed by a
 * client the CcmConversation will transition back to the Authenticated State.
 */
class ExecConvState : public CcmConversationState
{
  public:
    ExecConvState(CcmConversation& conversation);
    virtual ~ExecConvState() noexcept = default;

    void processExecuteQuery(std::shared_ptr<CcmConnection> ccon, msg::ExecuteQuery&) override;
    void processFetchIngot(std::shared_ptr<CcmConnection> ccon, msg::FetchIngot& rec) override;
};

}}  // namespace scidb::ccm

#endif
