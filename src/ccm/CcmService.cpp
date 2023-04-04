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
#include <ccm/CcmService.h>
// header files from the ccm module
#include "ClientCommManager.h"
#include <ccm/CcmProperties.h>
// SciDB modules
#include <system/Config.h>
#include <util/Job.h>
#include <util/JobQueue.h>
#include <util/ThreadPool.h>
// third-party libraries
#include <log4cxx/logger.h>
// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmService"));
}

namespace scidb { namespace ccm {

const std::string serviceName("CcmService");
const std::string serviceJobName("CcmServiceJob");

struct CcmService::Impl
{
    Impl();
    ~Impl() noexcept;

    void start();
    void stop();

    /**
     * The CcmServiceJob is responsible for starting the client communication manager
     * (CCM) using SciDb's threading mechanism.
     *
     * This Job is different than most jobs in the system because it is not intended to
     * run and exit and make space available on the JobQueue for another Job, but rather
     * this job runs until stopped (usually at shutdown of the instance) by the call to
     * stop()..
     *
     * (Phase2). Currently, the CCM only runs a single thread for message processing.
     *    This is inadequate for the front end. The concept of Maximum number of active
     *    sessions needs be added. When that is done the run() method in CCMServiceJob
     *    will need to schedule 'worker threads' onto the _queue sufficient to handle
     *    ccm-max-active-threads. It will also need to stop those threads in stop()
     */
    class CcmServiceJob : public scidb::Job
    {
      public:
        CcmServiceJob();
        virtual ~CcmServiceJob() noexcept = default;

        void run() override;
        void stop();

      private:
        std::shared_ptr<ClientCommManager> _manager{nullptr};
    };

    std::shared_ptr<JobQueue> _queue{nullptr};
    std::shared_ptr<ThreadPool> _threadPool{nullptr};
    std::shared_ptr<CcmServiceJob> _myJob{nullptr};
    bool _running{false};
};

CcmService::CcmService()
    : _impl(std::make_unique<CcmService::Impl>())
{}

CcmService::~CcmService() noexcept = default;

void CcmService::start()
{
    _impl->start();
}

void CcmService::stop()
{
    _impl->stop();
}

CcmService::Impl::Impl()
    : _queue(std::shared_ptr<JobQueue>(new JobQueue(serviceName)))
    , _threadPool(std::shared_ptr<ThreadPool>(new ThreadPool(1, _queue, serviceName)))
{}

CcmService::Impl::~Impl() noexcept
{
    try {
        LOG4CXX_TRACE(logger, "Shutting down CcmService threadPool");
        _threadPool->stop();
    } catch (std::exception const& e) {
        // threadPool can throw (deep in Semaphore::release).
        LOG4CXX_WARN(logger,
                     "CCmService failed to stop thread pool during destruction. (" << e.what() << ")");
    }
}

void CcmService::Impl::start()
{
    ASSERT_EXCEPTION(!_threadPool->isStarted(), "Ccm service started multiple times.");

    LOG4CXX_INFO(logger, "Starting CcmService");
    _threadPool->start();
    _myJob = std::make_shared<CcmServiceJob>();
    _queue->pushJob(_myJob);
    _running = true;
}

void CcmService::Impl::stop()
{
    LOG4CXX_INFO(logger, "Stopping CcmService");
    if (_myJob) {
        _myJob->stop();
        _myJob.reset();
    }
    _running = false;
}

CcmService::Impl::CcmServiceJob::CcmServiceJob()
    : Job(std::shared_ptr<Query>(), serviceJobName)
{}

void CcmService::Impl::CcmServiceJob::run()
{
    ASSERT_EXCEPTION(!_manager, "ClientCommService Job: error on start; already running");

    // TODO (Phase 2): The value of 3 is completely arbitrary.
    //       This should deal with a "configurable max-runs-per-timeperiod" exit to
    //       prevent excessive up/down/up/down thrashing. Or perhaps go away altogether
    //       as the phase 2 and later tasks are addressed.
    int start_count = 0;
    bool stopped = false;
    while (!stopped && start_count++ < 3) {
        LOG4CXX_INFO(logger, "Starting " << name());
        SCIDB_ASSERT(!_manager);
        CcmProperties props;
        props.setPort(Config::getInstance()->getOption<int>(CONFIG_CCM_PORT))
            .setTLS(Config::getInstance()->getOption<int>(CONFIG_CCM_TLS))
            .setSessionTimeOut(Config::getInstance()->getOption<int>(CONFIG_CCM_SESSION_TIME_OUT))
            .setReadTimeOut(Config::getInstance()->getOption<int>(CONFIG_CCM_READ_TIME_OUT));
        try {
            _manager = std::make_shared<ClientCommManager>(props);
            _manager->run();
        } catch (boost::system::system_error& e) {
            if (e.code().value() == boost::asio::error::address_in_use) {
                // This instance is NOT the winner to bind to the Ccm port.
                LOG4CXX_DEBUG(logger, "ClientCommManager could not start... " << e.what());
                // No reason to continue trying.
                stopped = true;
            } else {
                LOG4CXX_WARN(logger, "Failed to start ClientCommManager: " << e.what());
            }
        } catch (std::exception const& e) {
            LOG4CXX_ERROR(logger, "Failed to start ClientCommManager: " << e.what());
        }
        _manager.reset();
    }
}

void CcmService::Impl::CcmServiceJob::stop()
{
    if (_manager) {
        _manager->stop();
    }
}

}}  // namespace scidb::ccm
