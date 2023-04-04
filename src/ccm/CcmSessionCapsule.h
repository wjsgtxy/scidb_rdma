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
#ifndef CCM_SESSION_CAPSULE_H_
#define CCM_SESSION_CAPSULE_H_

// header files from the ccm module
#include "Uuid.h"
// SciDB modules
#include <util/Mutex.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>
// c++ standard libraries
#include <unordered_map>

namespace scidb { namespace ccm {

class CcmSession;

/**
 * @brief A container of known CcmSessions.
 *
 * @ingroup Ccm
 *
 * This container holds CcmSessions which have started, have some processing
 * state, and have not timed out. CcmSessions which time out are removed from
 * the capsule.
 *
 * This capsule is shared among all CcmConnections, since a connection is not
 * bound to a particular session. When a message is received by a connection,
 * the connection will obtain a shared_pointer to the session state machine
 * associated with the session UUID contained in the incoming message header.
 *
 *
 *
 * @note For those unfamiliar with the word, 'capsule'; a capsule is a small
 * case or container, especially a round or cylindrical one. Although there is
 * no aspect of this container being round or cylindrical, using the word map,
 * set, or list implies some aspect of the underlying implementation and
 * probable API which do not apply to this object so an English word without
 * implied meaning in computer science was chosen.
 *
 * @note Performance considerations -- Any call to a public member functions
 * will lock a member Mutex.
 */
class CcmSessionCapsule : public std::enable_shared_from_this<CcmSessionCapsule>
{
  public:
    explicit CcmSessionCapsule(boost::asio::io_service& ios);
    ~CcmSessionCapsule() noexcept = default;

    /**
     * @brief Create a new session
     *
     * @param timeOut the duration (in seconds) of inactivity in the newly
     *                created @c CcmSession before it is removed from the
     *                capsule.
     *
     * @return a new CcmSession
     */
    std::shared_ptr<CcmSession> createSession(long timeOut);

    /**
     * @brief Get a session from the session capsule
     *
     * @param sessionId the session identifier (key) to find in the session
     *                  capsule.
     *
     * @param ccmSessionId the ccm session identifier to associate with the
     *                     back-end @c scidb::Connection
     *
     * @return the Session from the session map if found otherwise a @c nullptr
     */
    std::shared_ptr<CcmSession> getSession(const Uuid& ccmSessionId);

    /**
     * @brief Remove a session from the session capsule
     *
     * @param sessionId the session identifier (key) of the @c CcmSession to
     *                  remove
     */
    void removeSession(const Uuid& sessionId);

    /**
     * @brief Cancel the inactivity timer of the session associated with the
     * given session identifier
     *
     * @param ccmSessionId the sessionId key for session that will have the
     *                     time-out timer canceled
     */
    void cancelTimer(const Uuid& ccmSessionId);

    /**
     * @brief Set the duration of the inactivity time and restart if for the Ccm
     * session associated with the provided session identifier.
     *
     * @param sessionId the session identifier (key) for session that will now
     *                  have a new session time-out
     *
     * @param timeOut the new duration (in seconds) of the inactivity timer for
     *                the session. A non-positive value will cause the timer to
     *                be processed as soon as the io_service is able.
     */
    void restartTimer(const Uuid&, long timeOut);

    /**
     * @brief return a shared_ptr to in valid session
     *
     * An invalid session with a know session identifier properties is needed to
     * send a message (such as an error) to the client. The Uuid for this is
     * session identifier is 00000000-0000-0000-0000-000000000000.
     *
     * @return a shared pointer to a session with which has the UUID invalid
     *         Uuid
     */
    std::shared_ptr<CcmSession> invalidSession() const;

  private:
    void _handleTimeOut(const Uuid& ccmSessionId, const boost::system::error_code& ec);
    void _addTimer(const Uuid& ccmSessionId, long timeOut);
    void _updateTimeOut(const Uuid& sessionId, long timeOut);

    std::unordered_map<Uuid, std::shared_ptr<CcmSession>> _sessions;
    std::unordered_map<Uuid, std::shared_ptr<boost::asio::deadline_timer>> _timers;
    boost::asio::io_service& _ios;
    mutable Mutex _mutex;
};

}}  // namespace scidb::ccm

#endif
