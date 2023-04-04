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
 * @file ScopedQueryThread.cpp
 */

#include "ScopedQueryThread.h"

#include <log4cxx/logger.h>

namespace {
// Using same logger name as in Job.cpp.
log4cxx::LoggerPtr qptlog(log4cxx::Logger::getLogger("scidb.qpt"));
}

namespace scidb {

namespace {
    std::string qpToId(std::shared_ptr<Query> const& qp)
    {
        return qp ? qp->getQueryID().toString() : "null";
    }
}

thread_local bool ScopedActiveQueryThread::_activated {false};

ScopedActiveQueryThread::ScopedActiveQueryThread(std::shared_ptr<Query>& query)
{
    ASSERT_EXCEPTION(not _activated, "Cannot nest ScopedActiveQueryThread objects!");
    _activated = true;

    _savedQuery = Query::getQueryPerThread();
    if (_savedQuery.lock() == query) {
        // The thread-local query pointer is already being managed
        // by Job::StackHelper.  Don't mess with it.
        LOG4CXX_TRACE(qptlog, __func__
                      << ": {\"op\": \"no-op\", \"old\": null, \"qpt\": \""
                      << qpToId(query) << "\"}");
    } else {
        LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"swapin\""
                      << ", \"old\": \"" << qpToId(_savedQuery.lock())
                      << "\", \"qpt\": \"" << qpToId(query) << "\"}");
        Query::setQueryPerThread(query);
        _restoreQuery = true;
    }
}

ScopedActiveQueryThread::~ScopedActiveQueryThread()
{
    ASSERT_EXCEPTION(_activated, "Cannot nest ScopedActiveQueryThread objects!");
    _activated = false;

    if (_restoreQuery) {
        LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"swapout\""
                      << ", \"old\": \"" << qpToId(Query::getQueryPerThread())
                      << "\", \"qpt\": \"" << qpToId(_savedQuery.lock()) << "\"}");
        Query::setQueryPerThread(_savedQuery.lock());
    }
}

} // namespace scidb
