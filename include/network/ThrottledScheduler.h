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

/**
 * @file ThrottledScheduler.h
 *
 * @brief Contains a class that allows to limit how often a particular piece of work is executed.
 * You can schedule() work as often as you like, but work will never be executed more frequently
 * than every maxDelay seconds.
 *
 */
#ifndef THROTTLEDSCHEDULER_H
#define THROTTLEDSCHEDULER_H

#include <network/Scheduler.h>

#include <assert.h>
#include <boost/asio.hpp>
#include <memory>
#include <util/Mutex.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>

namespace scidb
{

class ThrottledScheduler :
        public Scheduler,
        virtual public std::enable_shared_from_this<ThrottledScheduler>
{
public:
    ThrottledScheduler(int64_t maxDelay,
                       Work& work,
                       boost::asio::io_service& io_service)
        : _maxDelay(maxDelay)
        , _work(work)
        , _timer(io_service)
        , _lastRun(0)
        , _isScheduled(false)
        , _isRunning(false)
    {
       assert(_work);
    }

    virtual ~ThrottledScheduler() {}
    ThrottledScheduler(const ThrottledScheduler&) = delete;
    ThrottledScheduler& operator=(const ThrottledScheduler&) = delete;

    void schedule() override
    {
       ScopedMutexLock lock(_mutex, PTW_SML_THROTTLED_SCHEDULER);
       if (_isScheduled) {
           return;
       }
       if (_isRunning) {
          _isScheduled = true;
          return;
       }
       _schedule();
    }

 private:

    void _schedule()
    {
       // must be locked
       assert(!_isRunning);

       time_t now = ::time(nullptr);
       if (now == ((time_t) -1)) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
       }

       time_t secToWait = _maxDelay - (now - _lastRun);
       secToWait = (secToWait > 0) ? secToWait : 0;
       assert(secToWait <= _maxDelay);

       boost::system::error_code ec;
       _timer.expires_from_now(boost::posix_time::seconds(secToWait), ec);
       if (ec) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                  SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR)
               << ec;
       }

       _timer.async_wait(std::bind(&ThrottledScheduler::_run,
                                   shared_from_this(),
                                   std::placeholders::_1));
       _isScheduled = true;
   }

   void _run(const boost::system::error_code& error)
   {
      if (error == boost::asio::error::operation_aborted) {
         return;
      }
      if (error) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_TIMER_RETURNED_UNEXPECTED_ERROR2) << error;
      }

      {
         ScopedMutexLock lock(_mutex, PTW_SML_THROTTLED_SCHEDULER);
         assert(_isScheduled);
         assert(!_isRunning);
         _isScheduled = false;
         _isRunning = true;
         _lastRun = ::time(nullptr);
         if (_lastRun < 0) {
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_GET_SYSTEM_TIME);
         }
      }

      try {
          if (_work) {
              _work();
          }
      } catch (const scidb::Exception& e) {
         _reschedule();
         e.raise();
      }
      _reschedule();
   }

   void _reschedule()
   {
      ScopedMutexLock lock(_mutex, PTW_SML_THROTTLED_SCHEDULER);
      assert(_isRunning);
      _isRunning = false;
      if (_isScheduled) {
         _schedule();
      }
   }

private:
   int64_t _maxDelay;
   Work _work;
   boost::asio::deadline_timer _timer;
   int64_t _lastRun;
   bool _isScheduled;
   bool _isRunning;
   Mutex _mutex;
};

} // namespace scidb

#endif
