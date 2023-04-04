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
#ifndef CCM_SESSION_H_
#define CCM_SESSION_H_

// header files from the ccm module
#include "Uuid.h"
// c++ standard libraries
#include <memory>

namespace scidb { namespace ccm {
class CcmConnection;
class CcmConversation;
class CcmSessionState;
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
 *  A Ccm session encapsulates an end-user's interaction with the system, which
 *  can be multiplexed over several CcmConnection objects.
 *
 *  A CcmSession is a state machine which goes through 3 main states:
 *
 *   1. Unauthenticated. The client's session has not begun the 4-way handshake
 *        with the back-end necessary to provide appropriate user level access
 *        to the SciDB resources.
 *
 *   2. Starting. The user has provided the first of two Requests in the
 *        4-way handshake (AuthLogon with username has been provided to the
 *        Ccm), and the System has responded with an AuthChallenge.
 *
 *   3. Authenticated. The user has provided the second of two Requests in the
 *       4-way handshake (AuthRepsonse with password has been provided to the
 *       Ccm) and the CCM has established a Back-end connection to process
 *       forthcoming requests from the client.
 *
 *  A CcmSession has an associated timer that will destroy the session due to
 *  inactivity after a period of time (duration) specified in seconds. The timer
 *  is reset each time the client interacts by sending a message (on any
 *  CcmConnection).
 *
 *  Baring error conditions, once the Session is Authenticated, it will remain
 *  in that state until it times out. Errant/incorrect messages from the client
 *  may, for security or other purposes, also cause the session to be destroyed.
 *
 *  @todo (for BJC), to be addressed either here or in external documentation: This
 *  needs additional explanation of exactly what those error conditions are.
 *
 *  Once a session has been destroyed, The client session is unusable, and the
 *  client will need to re-establish a new session by starting at the
 *  Unauthenticated state.
 *
 *  The session state machine also has a contains a CcmConversation (which is,
 *  itself a state machine) that is used to maintain additional query processing
 *  state after a session has reached the Authenticated session state.
 *
 *  A CcmSession state machine can process the following messages. These
 *  messages are google protocol buffers read by the CcmConnection and defined
 *  in Ccm.proto:
 *
 *  - AuthLogin -- used to initiate a 4-way handshake to bind a SciDB connection
 *                 to the Session. On success the state machine will transition
 *                 to the Starting state.
 *
 * - AuthResponse -- used to continue the 4-way handshake necessary to bind the
 *                   SciDB connection to the session. On success, the state
 *                   machine will transition to the Authenticated state.. On
 *                   failure the session state machine will terminate, and the
 *                   client must restart the 4-way handshake.
 *
 * - ExecuteQuery -- Used to send a query to the bound SciDB connection. In
 *                   nominal conditions the state of the session state machine
 *                   should be in the Authenticated state to process this
 *                   message. When this message is received, the message is
 *                   relayed to the CcmConversation. Irrespective the success or
 *                   failure of the query, the session state machine remains in
 *                   the Authenticated state. The ExecuteQuery command relays
 *                   the contents of the Message to its CcmConversation.
 *
 * - FetchIngot -- Used to retrieve data from the previous query execution.
 *                 nominal conditions the state of the session state machine
 *                 should be in the Authenticated state to process this
 *                 message. When this message is received, the message is
 *                 relayed to the CcmConversation. Irrespective the success or
 *                 failure of the fetch message, the session state machine
 *                 remains in the Authenticated state. The ExecuteQuery command
 *                 relays the contents of the Message to its CcmConversation.
 *
 *
 */

class CcmSession : public std::enable_shared_from_this<CcmSession>
{
    friend class CcmConversationState;
    friend class CcmSessionState;

  public:
    /**
     * Constructor.
     *
     * @param id the Uuid of used to associate this session with its state machine.
     *
     * @param timeOut the duration(in seconds) to set the expiration timer after
     *        a new message for the session is received.
     */
    CcmSession(Uuid id, long timeOut);
    ~CcmSession() noexcept;

    void processAuthLogon(std::shared_ptr<CcmConnection> ccon, msg::AuthLogon& rec);
    void processAuthResponse(std::shared_ptr<CcmConnection> ccon, msg::AuthResponse& rec);
    void processExecuteQuery(std::shared_ptr<CcmConnection> ccon, msg::ExecuteQuery& rec);
    void processFetchIngot(std::shared_ptr<CcmConnection> ccon, msg::FetchIngot& rec);

    /**
     * Get the UUID of the the Session. This UUID is used in the CcmHeader of
     * the binary message format.
     */
    const Uuid& getId() const;

    /**
     * Get the duration (in seconds) of the time-out for the session. Each time
     * a message arrives for processing by a session's state machine, the timer
     * will be reset to expire after this many seconds.
     */
    long getTimeOut() const;

    /**
     * Set the duration (in seconds) of the time-out for the session. Each time
     * a message arrives for processing by a session's state machine, the timer
     * will be reset to expire after this many seconds.
     */
    void setTimeOut(long);

  private:
    void _changeState(std::shared_ptr<CcmSessionState> newState);

    std::shared_ptr<CcmConversation> _createConversation();
    void _cancelConversation();
    std::shared_ptr<CcmConversation> _getConversation();

    Uuid _id;       //!< unique id (uuid) of the session
    long _timeOut;  //!< expiration duration (seconds) between subsequent messages
    std::shared_ptr<CcmSessionState> _state;         //!< Current state of the session
    std::shared_ptr<CcmConversation> _conversation;  //!< The active Conversation

    // boost::asio::deadline_timer _expirationTimer;
    // bool _isCurrentlyActive
};

}}  // namespace scidb::ccm

#endif
