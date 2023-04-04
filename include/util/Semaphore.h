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
 * @file Semaphore.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class for synchronization
 */

#ifndef SEMAPHORE_H_
#define SEMAPHORE_H_

#include <util/PerfTime.h>

#include <functional>
#include <semaphore.h>

namespace scidb
{

class Semaphore
{
    sem_t _sem[1];

public:

    /**
     * User-supplied function called at error polling intervals.
     * @return true to keep waiting, false to quit
     * @note May throw any scidb::Exception.
     */
    typedef std::function<bool()> ErrorChecker;
    enum { DEFAULT_ERRCHK_INTERVAL = 1 }; // in seconds

    Semaphore();
    ~Semaphore();

    /**
     * @brief Enter (wait on) semaphore
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    void enter(perfTimeWait_e tw);

    /**
     * @brief Enter (wait on) the semaphore count times
     * @param[in] count - number of enter() calls to make, may be zero
     * @param[in] tw - category in which to include the time spent blocked
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    void enter(size_t count, perfTimeWait_e tw);

    /**
     * @brief Enter (wait on) the semaphore, with error polling.
     *
     * @param[in] errorChecker - error checking functor
     * @param[in] tw - category in which to include the time spent blocked
     * @param[in] interval - frequency to poll for errors using errorChecker
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    bool enter(ErrorChecker& errorChecker,
               perfTimeWait_e tw,
               timespec const* interval = nullptr);

    /**
     * @brief Enter (wait on) semaphore count times, with error polling.
     *
     * @param[in] count - number of enter() calls to make, may be zero
     * @param[in] errorChecker - error checking functor
     * @param[in] tw - category in which to include the time spent blocked
     * @param[in] interval - frequency to poll for errors using errorChecker
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    bool enter(size_t count,
               ErrorChecker& errorChecker,
               perfTimeWait_e tw,
               timespec const* interval = nullptr);

    /**
     * @brief Release (post to) the semaphore count times.
     * @param[in] - count how many times to release
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    void release(size_t count = 1);
};

} // namespace

#endif /* ! SEMAPHORE_H_ */
