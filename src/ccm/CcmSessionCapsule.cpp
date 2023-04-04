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
#include "CcmSessionCapsule.h"
// header files from the ccm module
#include "CcmSession.h"
// third-party libraries
#include <log4cxx/logger.h>

using boost::asio::deadline_timer;
using boost::posix_time::seconds;

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmSessionCapsule"));
}

namespace scidb { namespace ccm {

CcmSessionCapsule::CcmSessionCapsule(boost::asio::io_service& ios)
    : _ios(ios)
{}

std::shared_ptr<CcmSession> CcmSessionCapsule::invalidSession() const
{
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    // TODO: (Phase 2)  Should this be a class variable for when we need to
    // set the null session. Then it only needs to be created ONCE.
    //
    // TODO (Phase 2): We should also check for the null sessions in "the right" places.
    std::shared_ptr<CcmSession> p;
    try {
        Uuid nullid;
        p = std::make_shared<CcmSession>(nullid, std::numeric_limits<long>::max());
        LOG4CXX_TRACE(logger, "   - Creating 'untimed' Null session:  " << p->getId());
    } catch (scidb::Exception& e) {
        LOG4CXX_FATAL(logger, "(scidb::Exception) CcmSessionCapsule::nullSession" << e.what());
    } catch (std::exception& e) {
        LOG4CXX_FATAL(logger, "(std::exception) CcmSessionCapsule::nullSession: " << e.what());
    }
    return p;
}

std::shared_ptr<CcmSession> CcmSessionCapsule::createSession(long timeOut)
{
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    Uuid u = Uuid::create();
    std::shared_ptr<CcmSession> p = std::make_shared<CcmSession>(u, timeOut);
    _sessions.insert(std::make_pair(u, p));
    LOG4CXX_TRACE(logger,
                  " - Added New CcmSession: " << u << " (" << _sessions.size()
                                              << " sessions in capsule)");
    _addTimer(u, timeOut);
    return (p);
}

std::shared_ptr<CcmSession> CcmSessionCapsule::getSession(const Uuid& ccmSessionId)
{
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    auto session = _sessions.find(ccmSessionId);
    if (session != _sessions.end()) {
        LOG4CXX_TRACE(logger,
                      " - Looking for session identifier: " << ccmSessionId << " - found"
                                                            << " (" << _sessions.size()
                                                            << " sessions in capsule)");
        _updateTimeOut(ccmSessionId, session->second->getTimeOut());
        return session->second;
    }
    LOG4CXX_TRACE(logger,
                  " - Looking for session identifier: " << ccmSessionId << " - NOT found"
                                                        << " (" << _sessions.size()
                                                        << " sessions in capsule)");
    return nullptr;
}

void CcmSessionCapsule::removeSession(const Uuid& ccmSessionId)
{
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    auto ccmSessionCount = _sessions.erase(ccmSessionId);
    auto timersCount = _timers.erase(ccmSessionId);
    if (ccmSessionCount != timersCount) {
        LOG4CXX_ERROR(logger,
                      "Removed different number of Sessions and Timers: "
                          << ccmSessionCount << " sessions, " << timersCount << " timers");
    }
}

void CcmSessionCapsule::cancelTimer(const Uuid& ccmSessionId)
{
    LOG4CXX_TRACE(logger, "Canceling Timer for: " << ccmSessionId);
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    auto timer_iter = _timers.find(ccmSessionId);
    if (timer_iter != _timers.end()) {
        auto& timer = timer_iter->second;
        timer->cancel();
    } else {
        // The session timed out (and the session/timer were removed by handleTimeOut)
        // before the long-running incoming message (ExecuteQueryMsg) tried to suspend the
        // timer.
        LOG4CXX_TRACE(logger,
                      " - Timer for " << ccmSessionId
                                      << " not found. Cannot suspend non-existent timer");
    }
}

void CcmSessionCapsule::restartTimer(const Uuid& ccmSessionId, long timeOut)
{
    LOG4CXX_TRACE(logger, "Rescheduling Timer for: " << ccmSessionId);
    if (timeOut < 0L) {
        LOG4CXX_WARN(logger,
                     "CcmSession timer for " << ccmSessionId << " scheduled for immediate removal.");
    }
    ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
    _updateTimeOut(ccmSessionId, timeOut);
}

/**
 * @brief The callback function to execute when a session timer expires.
 *
 * When a session expires, this function is called (asynchronously) and the session is
 * removed from the session capsule.
 *
 * @note This function @b locks the Mutex and should not be locked prior being
 *       called. Since this method is called asynchronously
 */
void CcmSessionCapsule::_handleTimeOut(const Uuid& ccmSessionId, const boost::system::error_code& ec)
{
    if (ec != boost::asio::error::operation_aborted) {
        // timer expired...
        LOG4CXX_TRACE(logger,
                      " - (handleTimeOut) Timer Expired for " << ccmSessionId
                                                              << " ... removing session");
        ScopedMutexLock lock(_mutex, PTW_CCM_CACHE);
        _sessions.erase(ccmSessionId);
        _timers.erase(ccmSessionId);
        LOG4CXX_TRACE(logger,
                      " - CcmSession/Timer Count After: (" << _sessions.size() << ", " << _timers.size()
                                                           << ")");
    }
    // The time-out was explicitly aborted either via cancel or being rescheduled. so
    // There is nothing to do.
}

/**
 * @brief Add a session expiration timer for the given session id.
 *
 * @param ccmSessionId the session identifier for which to add a corresponding expiration
 *                     timer.
 *
 * @param timeOut the duration (in seconds) the of the added timer
 *
 * @note This function does @b NOT lock the mutex, so locking should occur PRIOR to being
 *       called.
 */
void CcmSessionCapsule::_addTimer(const Uuid& ccmSessionId, long timeOut)
{
    auto timer = std::make_shared<deadline_timer>(_ios, seconds(timeOut));
    _timers.insert(std::make_pair(ccmSessionId, timer));
    LOG4CXX_TRACE(logger,
                  " - Added timer for " << ccmSessionId << "(session,timer count: " << _sessions.size()
                                        << ", " << _timers.size() << ")");
    timer->async_wait(std::bind(&CcmSessionCapsule::_handleTimeOut,
                                this,
                                ccmSessionId,
                                std::placeholders::_1));
}

/**
 * @brief Reset (update) the expiration timer for the given session id.
 *
 * @param ccmSessionId the session identifier for which to add a
 *                     corresponding expiration timer.
 *
 * @param timeOut the duration (in seconds) the of the added timer
 *
 * @note This function does @b NOT lock the mutex, so locking should occur
 *       PRIOR to being called.
 */
void CcmSessionCapsule::_updateTimeOut(const Uuid& ccmSessionId, long timeOut)
{
    LOG4CXX_TRACE(logger,
                  " - Updating timer (" << timeOut << " seconds) for " << ccmSessionId
                                        << ". session/timers: (" << _sessions.size() << ", "
                                        << _timers.size() << ")");
    auto timer_iter = _timers.find(ccmSessionId);
    if (timer_iter != _timers.end()) {
        auto& timer = timer_iter->second;
        // Reschedule the timer to expire TIMEOUT seconds from 'now'
        // if the timer was running (number canceled > 0), otherwise,
        // no one was waiting for the timer so leave things that way.
        if (timer->expires_from_now(seconds(timeOut)) > 0) {
            timer->async_wait(std::bind(&CcmSessionCapsule::_handleTimeOut,
                                        this,
                                        ccmSessionId,
                                        std::placeholders::_1));
        }
    }  //  else {
    //     // So what happened to our timer?
    //     // If we have a session and no timer, then there was some unexpected code path.
    //     // However, if the ccm session timed out (and the session/timer were removed from the maps)
    //     // then there's nothing to be done.
    //     auto sess_iter = _sessions.find(ccmSessionId);
    //     if (sess_iter != _sessions.end()) {
    //         LOG4CXX_WARN(logger, " - Re-adding new timer for CcmSession:" << ccmSessionId);
    //         _addTimer(ccmSessionId, timeOut);
    //     } else {
    //         LOG4CXX_TRACE(logger,
    //                       " - Failed to update time-out for non-existent (already timed out) session:
    //                       "
    //                           << ccmSessionId);
    //     }
    // }
}

}}  // namespace scidb::ccm
