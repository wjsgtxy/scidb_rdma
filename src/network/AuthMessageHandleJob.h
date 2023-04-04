/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file AuthMessageHandleJob.h
 *
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief The job for handling authentication messages.
 *
 * @description
 * We separate out authentication message handling into its own job so
 * we can force every new connection to exchange authentication
 * messages, regardless of the strength of the authentication method
 * used or who the connection is from.
 *
 * Like client messages, authentication messages (from client or other
 * instance) must be kept synchronous with respect to the connection.
 * After authentication, servers (but not clients) can start async
 * messaging.
 */

#ifndef AUTH_MESSAGE_HANDLE_JOB_H
#define AUTH_MESSAGE_HANDLE_JOB_H

#include "MessageHandleJob.h"

#include <util/Mutex.h>
#include <list>

namespace scidb
{

class Authenticator;
class Connection;
class NetworkManager;

/**
 * Job for handling authentication messages.
 */
class AuthMessageHandleJob : public MessageHandleJob
{
    typedef MessageHandleJob inherited;

 public:
    AuthMessageHandleJob(const std::shared_ptr<Connection>& connection,
                         const std::shared_ptr<MessageDesc>& messageDesc);

    /**
     * Based on its contents this message is prepared and scheduled to run
     * on an appropriate queue.
     * @see MessageHandleJob::dispatch
     */
    void dispatch(NetworkManager* nm) override;

    /** Hook for periodic GC of authentications that are taking too long. */
    static void slowAuthKiller();

 protected:

    /// Implementation of Job::run()
    /// @see Job::run()
    void run() override;

    // XXX Need timer on Connections to abort on auth delays.  (Maybe
    // the timer hinted at in Connection::readMessage() is the
    // answer.)

 private:
    std::shared_ptr<Connection> _connection;

    /// Helper for scheduling this message on a given queue
    void enqueue(const std::shared_ptr<WorkQueue>& q);

    /// These methods are used when a remote client or peer is trying
    /// to authenticate with us.
    /// @{
    void handleAuthLogon();
    void sendAuthChallenge(std::shared_ptr<Authenticator>&);
    void handleAuthResponse();
    void sendAuthAllow(std::string const& info);
    void sendAuthDeny(std::string const& reason);
    void sendAuthError(scidb::Exception const& ex);
    void sendHangup();
    /// @}

    /// These methods are used when *we* are trying to authenticate
    /// with a remote peer using AUTH_I2I.
    /// @{
    void handleAuthChallenge();
    void handleAuthComplete();
    /// @}

    // Machinery for killing external client connections that are
    // taking too long to authenticate.
    void startAuthTimer();
    void cancelAuthTimer();
    typedef std::list<std::weak_ptr<Connection> > WeakConnList;
    static Mutex s_mutex;
    static WeakConnList s_inProgress;
};

} // namespace

#endif /* ! AUTH_MESSAGE_HANDLE_JOB_H */
