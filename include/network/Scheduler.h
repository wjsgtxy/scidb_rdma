#ifndef SCHEDULER_H_
#define SCHEDULER_H_
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

namespace scidb {
   /**
    * A scheduler  that runs *at most* every specified period.
    * It schedules itself to run in max(0, ((lastTime + period) - currentTime)) seconds.
    * It is not recurring - every execution needs to be explicitly scheduled.
    * When scheduled for the first time, it is run immediately.
    */
   class Scheduler
   {
   public:
      typedef std::function<void()> Work;
      virtual void schedule() = 0;
      virtual ~Scheduler() {}
   };

}  // namespace scidb
#endif
