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
 * @file Job.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Job class
 */

#include <util/Job.h>

#include <log4cxx/logger.h>
#include <util/WorkQueue.h>
#include <query/Query.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));
static log4cxx::LoggerPtr qptlog(log4cxx::Logger::getLogger("scidb.qpt"));

namespace scidb
{

namespace {
    std::string qpToId(std::shared_ptr<Query> const& qp)
    {
        return qp ? qp->getQueryID().toString() : "null";
    }
}

    thread_local std::stack<std::weak_ptr<Job> > Job::_jobStack;

    std::shared_ptr<Job> Job::getCurrentJobPerThread()
    {
        ASSERT_EXCEPTION(!_jobStack.empty(), "Empty job stack");
        return std::shared_ptr<Job>(_jobStack.top());
    }

    Job::StackHelper::StackHelper(const std::shared_ptr<Job>& job)
    {
        ASSERT_EXCEPTION(job != nullptr, "Null job cannot be pushed on stack");
        _jobStack.push(job);

        auto query = job->getQuery();
        if (query) {
            _restoreQuery = true;
            _savedQuery = Query::getQueryPerThread();
            Query::setQueryPerThread(query);
            LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"push\", \"top\": \"" << job
                          << "\", \"old\": \"" << qpToId(_savedQuery)
                          << "\", \"qpt\": \"" << qpToId(query) << "\"}");
        } else {
            LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"keep\", \"top\": \"" << job
                         << "\", \"old\": null, \"qpt\": \""
                         << qpToId(Query::getQueryPerThread()) << "\"}");
        }
    }

    Job::StackHelper::~StackHelper()
    {
        ASSERT_EXCEPTION(!_jobStack.empty(), "Empty job stack");
        _jobStack.pop();

        Job* j = _jobStack.empty() ? nullptr : _jobStack.top().lock().get();
        if (_restoreQuery) {
            LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"pop\", \"top\": \"" << j
                          << "\", \"old\": \"" << qpToId(Query::getQueryPerThread())
                          << "\", \"qpt\": \"" << qpToId(_savedQuery) << "\"}");
            Query::setQueryPerThread(_savedQuery);
        } else {
            LOG4CXX_TRACE(qptlog, __func__ << ": {\"op\": \"kept\", \"top\": \"" << j
                          << "\", \"old\": null, \"qpt\": \""
                          << qpToId(Query::getQueryPerThread()) << "\"}");
        }
    }

    void Job::executeOnQueue(std::weak_ptr<WorkQueue>& wq,
                             std::shared_ptr<SerializationCtx>& sCtx)
    {
        StackHelper scope(shared_from_this());
        ScopedMutexLock cs(_currStateMutex, PTW_SML_JOB_XOQ);
        _wq=wq;
        _wqSCtx = sCtx;
        run();
        // Note that it is safe to modify the state of this object after the call to run()
        // because it is protected by a mutex
    }

    void Job::execute()
    {
        if (!_removed) {
            const char *err_msg = "Job::execute: unhandled exception";
            try {
                StackHelper scope(shared_from_this());
                run();
            } catch (Exception const& x) {
                _error = x.clone();
                LOG4CXX_ERROR(logger, err_msg
                              << "\ntype: " << typeid(x).name()
                              << "\njobType: " << typeid(*this).name()
                              << "\nmesg: " << x.what()
                              << "\nqueryID = "<<(_query ? _query->getQueryID() : INVALID_QUERY_ID));
            } catch (const std::exception& e) {
                try {
                    _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                    LOG4CXX_ERROR(logger, err_msg
                                  << "\ntype: " << typeid(e).name()
                                  << "\njobType: " << typeid(*this).name()
                                  << "\nmesg: " << e.what()
                                  << "\nqueryID = "<<(_query ? _query->getQueryID() : INVALID_QUERY_ID));
                } catch (...) {}
                throw;
            } catch (...) {
                try {
                    _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << err_msg;
                    LOG4CXX_ERROR(logger, err_msg);
                } catch (...) {}
                throw;
            }
        }
        _done.release();
    }

    // Waits until job is done
    bool Job::wait(perfTimeWait_e tw, unsigned flags)
    {
        _done.enter(tw);
        if (flags & CAN_MULTIWAIT) {
            _done.release(); // allow multiple waits
        }
        if (_error && _error->getShortErrorCode() != SCIDB_E_NO_ERROR) {
            if (flags & CAN_RAISE)
            {
                _error->raise();
            }
            return false;
        }
        return true;
    }

    void Job::rethrow()
    {
        _error->raise();
    }
} //namespace
