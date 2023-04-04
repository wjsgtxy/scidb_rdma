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
#ifndef CCM_SESSION_STATE_H_
#define CCM_SESSION_STATE_H_

// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {
class CcmConversation;
class CcmConnection;
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
 *
 *  The current state of a Ccm session. A client session can be in one of three
 *  states:
 *
 *  - UnauthSessionState :: a new (or expired) session is requiring the start of
 *      the 4-way handshake.
 *
 *  - StartingSessionState :: A CcmSession in which the client has initiated the
 *      first part of the 4-way authentication process.

 *  - Authenticated :: A CcmSession in which the client has completed the 4-way
 *      handshake. And conversations (i.e queries) can be executed.
 *
 *
 * @see Behavioral Patterns: State.
 *        Design patterns: elements of reusable object-oriented software.
 *        Addison-Wesley Longman Publishing Co., Inc. Boston, MA, USA ©1995
 *        ISBN:0-201-63361-2
 */
class CcmSessionState
{
  public:
    explicit CcmSessionState(CcmSession& cs);
    virtual ~CcmSessionState() noexcept;

    virtual void processAuthLogon(std::shared_ptr<CcmConnection>&, msg::AuthLogon&) = 0;
    virtual void processAuthResponse(std::shared_ptr<CcmConnection>&, msg::AuthResponse&) = 0;
    virtual void processExecuteQuery(std::shared_ptr<CcmConnection>&, msg::ExecuteQuery&) = 0;
    virtual void processFetchIngot(std::shared_ptr<CcmConnection>&, msg::FetchIngot&) = 0;

    // For logging messages
    void printProcessingMsg(const std::string& msgName) const;
    std::string getStateName() const { return _stateName; }

  protected:
    std::shared_ptr<CcmConversation> createConversation();
    std::shared_ptr<CcmConversation> getConversation();
    void cancelConversation();

    CcmSession& _session;
    std::string _stateName;
};

class UnauthSessionState final : public CcmSessionState
{
  public:
    explicit UnauthSessionState(CcmSession& cs);
    virtual ~UnauthSessionState() noexcept = default;

    void processAuthLogon(std::shared_ptr<CcmConnection>&, msg::AuthLogon&) override;
    void processAuthResponse(std::shared_ptr<CcmConnection>&, msg::AuthResponse&) override;
    void processExecuteQuery(std::shared_ptr<CcmConnection>&, msg::ExecuteQuery&) override;
    void processFetchIngot(std::shared_ptr<CcmConnection>&, msg::FetchIngot&) override;
};

class StartingSessionState final : public CcmSessionState
{
  public:
    explicit StartingSessionState(CcmSession&);
    virtual ~StartingSessionState() noexcept = default;

    void processAuthLogon(std::shared_ptr<CcmConnection>&, msg::AuthLogon&) override;
    void processAuthResponse(std::shared_ptr<CcmConnection>&, msg::AuthResponse&) override;
    void processExecuteQuery(std::shared_ptr<CcmConnection>&, msg::ExecuteQuery&) override;
    void processFetchIngot(std::shared_ptr<CcmConnection>&, msg::FetchIngot&) override;
};

class AuthedSessionState final : public CcmSessionState
{
  public:
    explicit AuthedSessionState(CcmSession&);
    virtual ~AuthedSessionState() noexcept = default;

    void processAuthLogon(std::shared_ptr<CcmConnection>&, msg::AuthLogon&) override;
    void processAuthResponse(std::shared_ptr<CcmConnection>&, msg::AuthResponse&) override;
    void processExecuteQuery(std::shared_ptr<CcmConnection>&, msg::ExecuteQuery&) override;
    void processFetchIngot(std::shared_ptr<CcmConnection>&, msg::FetchIngot&) override;
};
}}  // namespace scidb::ccm

#endif
