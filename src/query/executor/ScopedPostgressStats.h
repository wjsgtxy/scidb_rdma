/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file ScopedActiveCpuInstanceStats.h
 */

#ifndef SCOPED_POSTGRESS_STATS_H
#define SCOPED_POSTGRESS_STATS_H

#include <monitor/InstanceStats.h>
#include <query/Query.h>

namespace scidb
{
    class ScopedPostgressStats
    {
    private:
        bool        _enabled;
        uint64_t    _secStartElapsed;

    public:
        ScopedPostgressStats(bool enabled)
            : _enabled(enabled)
            , _secStartElapsed(0.0)
        {
            if(_enabled) {
                _secStartElapsed = perfTimeGetElapsedInMicroseconds();
            }
        }

        ~ScopedPostgressStats()
        {
            if(_enabled)
            {
                uint64_t deltaTime = perfTimeGetElapsedInMicroseconds() - _secStartElapsed;
                SCIDB_ASSERT(InstanceStats::getInstance());
                InstanceStats::getInstance()->addToPostgressTime(deltaTime);

                std::shared_ptr<Query> query = Query::getQueryPerThread();
                if(query) {
                    query->getStats().addToPostgressTime(deltaTime);
                }
            }
        }
    };
} // namespace scidb

#endif // SCOPED_POSTGRESS_STATS_H

