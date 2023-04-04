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

#include <array/MemArray.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIManager.h>
#include <mpi/MPIPhysical.hpp>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Query.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/shm/SharedMemoryIpc.h>

#include <log4cxx/logger.h>

#define STARTMSG "MPI_TEST: " << __FUNCTION__ << "(): starting "
#define INFOMSG  "MPI_TEST: " << __FUNCTION__ << "(): "
#define FAILMSG  "MPI_TEST: " << __FUNCTION__ << "(): FAILED: "
#define PASSMSG  "MPI_TEST: " << __FUNCTION__ << "(): passed "

namespace scidb
{
using namespace std;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi.test"));

std::shared_ptr<MpiSlaveProxy> newMPISlaveProxyForTests(uint64_t launchId,
                                                          const std::shared_ptr<Query>& query,
                                                          const std::string& installPath,
                                                          uint32_t timeout, uint32_t delay)
{
    return std::make_shared<MpiSlaveProxy>(launchId, query, installPath,
                                             timeout, delay);
}

class PhysicalMpiTest: public MPIPhysical
{
  public:

    PhysicalMpiTest(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : MPIPhysical(logicalName, physicalName, parameters, schema),
      _mustLaunch(false), NUM_LAUNCH_TESTS(3)
    {
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        _mustLaunch = true;
    }
    void setQuery(const std::shared_ptr<Query>& query)
    {
        // slow down the workers to make sure the slaves are started before
        // the workers have set up the context
        if (!query->isCoordinator()) {
            const int WORKER_QUERY_EXECUTION_DELAY_SEC = 10;
            ::sleep(WORKER_QUERY_EXECUTION_DELAY_SEC);
        }
        MPIPhysical::setQuery(query);
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        MpiManager::getInstance()->cleanup();

        assert(_ctx);
        const InstMembershipPtr membership =
           Cluster::getInstance()->getInstanceMembership(query->getCoordinatorLiveness()->getMembershipId());
        const string& installPath = MpiManager::getInstallPath(membership);

        syncBarrier(0, query);
        syncBarrier(1, query);

        testMultipleLaunches(installPath, membership, query);
        testEcho(installPath, membership,  query);
        testBadMessageFlood(installPath, membership,  query); // prereq: launchId>0
        testBadHandshake(installPath, membership,  query);
        testBadStatus(installPath, membership,  query);
        testSlowSlave(installPath, membership,  query);
        testSlaveExit(installPath, membership,  query);

        _ctx.reset();
        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
    }

    void launchMpiJob(std::shared_ptr<MpiLauncher>& launcher,
                      std::vector<std::string>& args,
                      const std::shared_ptr<const InstanceMembership>& membership,
                      const std::shared_ptr<Query>& query,
                      const size_t maxSlaves)
    {
        launcher->launch(args, membership, maxSlaves);

        vector<pid_t> pids;
        launcher->getPids(pids);
        for (vector<pid_t>::const_iterator i=pids.begin(); i != pids.end(); ++i) {
            LOG4CXX_INFO(logger, INFOMSG << "Launched PID= "<<(*i));
        }

        if (!launcher->isRunning()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in MPI launcher: MpiLauncher::isRunning returned false";
        }
    }

    void testMultipleLaunches(const std::string& installPath,
                              const std::shared_ptr<const InstanceMembership>& membership,
                              std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG);

        for ( size_t i=0; i < NUM_LAUNCH_TESTS; ++i)
        {
            LOG4CXX_INFO(logger, INFOMSG << "MULTI-LAUNCH test " << i);
            _ctx->getNextLaunchId();
            uint64_t launchId =_ctx->getNextLaunchId();
            uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
            if (launchId-2 != oldLaunchId) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in managing last launch ID";
            }

            std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));

            if (i>0) {
                try {
                    _ctx->setSlaveInternal(launchId-3, slave);
                    LOG4CXX_INFO(logger, FAILMSG << "Bug in MPI context, setSlave should fail");
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                           << "MPI_TEST: Bug in MPI context: MpiOperatorContext::setSlave did not fail";
                } catch(scidb::SystemException& e) {
                    if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                        e.raise();
                    }
                    if (e.getErrorMessage().find(mpi::E_DECREASING_LAUNCH_IDS) == std::string::npos) {
                        e.raise();
                    }
                    // expected
                }
            }

            _ctx->setSlave(slave);

            try {
                _ctx->setSlaveInternal(launchId-1, slave);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI context: MpiOperatorContext::setSlave did not fail";
            } catch(scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_DECREASING_LAUNCH_IDS) == std::string::npos) {
                    e.raise();
                }
                // expected
            }

            std::shared_ptr<MpiLauncher> launcher;
            if (_mustLaunch) {
                launcher = std::shared_ptr<MpiLauncher>(
                    MpiManager::getInstance()->newMPILauncher(launchId, query));
                // Perform some negative testing on the launcher object
                try {
                    vector<pid_t> pids;
                    launcher->getPids(pids);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                        << "MPI_TEST: Bug in MPI launcher: MpiLauncher::getPids did not fail";
                } catch(scidb::MpiLauncher::InvalidStateException& e) {
                    // expected
                }
                try {
                    launcher->destroy();
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                        << "MPI_TEST: Bug in MPI launcher: MpiLauncher::destroy did not fail";
                } catch(scidb::MpiLauncher::InvalidStateException& e) {
                    // expected
                }
                if (launcher->isRunning()) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                        << "MPI_TEST: Bug in MPI launcher: MpiLauncher::isRunning returned true";
                }
                try {
                    _ctx->setLauncherInternal(launchId-1, launcher);
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                        << "MPI_TEST: Bug in MPI context: MpiOperatorContext::setLauncher did not fail";
                } catch(scidb::SystemException& e) {
                    if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                        e.raise();
                    }
                    if (e.getErrorMessage().find(mpi::E_DECREASING_LAUNCH_IDS) == std::string::npos) {
                        e.raise();
                    }
                    // expected
                }

                _ctx->setLauncher(launcher);
                std::vector<std::string> args;
                launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
            }

            // Create IPC
            string clusterUuid = Cluster::getInstance()->getUuid();
            InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();
            string ipcName = mpi::getIpcName(installPath, clusterUuid, query->getQueryID(), instanceId, launchId);

            // Construct slave command
            mpi::Command cmd;
            cmd.setCmd(string("DUMMY_COMMAND"));
            cmd.addArg(ipcName);

            try {
                slave->sendCommand(cmd, _ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::sendCommand did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForStatus(_ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::waitForStatus did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForExit(_ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::waitForExit did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            //-------------------- Get the handshake
            slave->waitForHandshake(_ctx);

            // After the handshake the old slave must be gone
            std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);

            // cleanup the slave from the previous launch (if any)
            if (oldSlave) {
                oldSlave->destroy();
                oldSlave.reset();
            }
            _ctx->complete(oldLaunchId);

            std::shared_ptr<SharedMemoryIpc> shmIpc(mpi::newSharedMemoryIpc(ipcName));

            try {
                _ctx->addSharedMemoryIpc(launchId-1, shmIpc);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI context: MpiOperatorContext::addSharedMemoryIpc did not fail";
            } catch(scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_DECREASING_LAUNCH_IDS) == std::string::npos) {
                    e.raise();
                }
                // expected
            }

            _ctx->addSharedMemoryIpc(launchId, shmIpc);

            LOG4CXX_INFO(logger, INFOMSG << "IPC name = " << ipcName);

            // Perform some negative testing on the shared memory object
            const uint64_t SMALL_SHM_SIZE = 777;
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            try {
                shmIpc->getSize();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::getSize did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            try {
                shmIpc->get();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::get did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            char* ptr(NULL);

            try {
                shmIpc->create(SharedMemoryIpc::RDWR);
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                stringstream ss;
                ss << FAILMSG << "shared_memory_mmap: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                    << ss.str();
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                stringstream ss;
                ss << FAILMSG << "Bug in creating shared memory: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << ss.str();
            }
            if (shmIpc->getName() != ipcName
                || shmIpc->getSize() != 0
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "MPI_TEST: Bug in creating shared memory object: "
                          "SharedMemoryIpc::get* returned invalid value";
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                stringstream ss;
                ss << FAILMSG << "shared_memory_mmap: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                    << ss.str();
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                stringstream ss;
                ss << FAILMSG << "Bug in truncating shared memory: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << ss.str();
            }

            if (shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in truncating shared memory: "
                       "SharedMemoryIpc::get* returned invalid value";
            }
            try {
                ptr = reinterpret_cast<char*>(shmIpc->get());
            } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
                stringstream ss;
                ss << FAILMSG << "shared_memory_mmap: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                    << ss.str();
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                stringstream ss;
                ss << FAILMSG << "Bug in mapping shared memory: " << e.what();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << ss.str();
            }
            if (!ptr
                || shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: "
                       "SharedMemoryIpc::get* returned invalid value";
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: "
                       "SharedMemoryIpc::truncate did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            shmIpc->close();

            try {
                shmIpc->truncate(SMALL_SHM_SIZE, true);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }
            if (!ptr
                || shmIpc->getName() != ipcName
                || shmIpc->getSize() != SMALL_SHM_SIZE
                || shmIpc->getAccessMode() != SharedMemoryIpc::RDWR) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::get* returned invalid value";
            }
            try {
                shmIpc->truncate(SMALL_SHM_SIZE);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in mapping shared memory: SharedMemoryIpc::truncate did not fail";
            } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
                // expected
            }

            char digits[10] = { '0', '1', '2', '3','4','5','6','7','8','9' };
            ptr[SMALL_SHM_SIZE-1] = '\0';
            for (size_t i=0; i < (SMALL_SHM_SIZE-1); ++i) {
                ptr[i] = digits[instanceId%10];
            }

            LOG4CXX_TRACE(logger, INFOMSG << "BUF:\n" << ptr );

            // Send command to slave
            slave->sendCommand(cmd, _ctx);

            // Get the command result
            slave->waitForStatus(_ctx);

            cmd.clear();
            cmd.setCmd(string("EXIT"));

            slave->sendCommand(cmd, _ctx);

            // Wait for the slave to disconnect
            slave->waitForExit(_ctx);

            try {
                slave->sendCommand(cmd, _ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::sendCommand did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            try {
                slave->waitForStatus(_ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::waitForStatus did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }
            try {
                slave->waitForExit(_ctx);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI slave state management: MpiSlaveProxy::waitForExit did not fail";
            } catch(scidb::MpiSlaveProxy::InvalidStateException& e) {
                // expected
            }

            shmIpc->close();
            if (!shmIpc->remove()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                    << "MPI_TEST: shared_memory_remove";
            }
            shmIpc->unmap();
            if (_mustLaunch) {
                launcher->destroy();
            }
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testEcho(const std::string& installPath,
                  const std::shared_ptr<const InstanceMembership>& membership,
                  std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "ECHO test");

	uint64_t launchId    = _ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI_TEST: Bug in manging last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
	    launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

	//-------------------- Create IPCs
	string clusterUuid = Cluster::getInstance()->getUuid();
	InstanceID instanceId = Cluster::getInstance()->getLocalInstanceId();
	string ipcName = mpi::getIpcName(installPath, clusterUuid, query->getQueryID(), instanceId, launchId);

	string ipcNameIn = ipcName+".in";
	string ipcNameOut = ipcName+".out";

        LOG4CXX_INFO(logger, INFOMSG << "IPC name.in = " << ipcNameIn);
        LOG4CXX_INFO(logger, INFOMSG << "IPC name.out = " << ipcNameOut);

	std::shared_ptr<SharedMemoryIpc> shmIpcIn(mpi::newSharedMemoryIpc(ipcNameIn));
	_ctx->addSharedMemoryIpc(launchId, shmIpcIn);

	std::shared_ptr<SharedMemoryIpc> shmIpcOut(mpi::newSharedMemoryIpc(ipcNameOut));
	_ctx->addSharedMemoryIpc(launchId, shmIpcOut);

	const int64_t LARGE_SHM_SIZE = 64*MiB;

	char* ptrIn(NULL);
	char* ptrOut(NULL);
        try {
            shmIpcIn->create(SharedMemoryIpc::RDWR);
            shmIpcIn->truncate(LARGE_SHM_SIZE);
            ptrIn = reinterpret_cast<char*>(shmIpcIn->get());
            shmIpcOut->create(SharedMemoryIpc::RDWR);
            shmIpcOut->truncate(LARGE_SHM_SIZE);
            ptrOut = reinterpret_cast<char*>(shmIpcOut->get());
        } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
            stringstream ss;
            ss << FAILMSG << "Cannot map shared memory: " << e.what();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                << ss.str();
        } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
            stringstream ss;
            ss << FAILMSG << "Bug in mapping shared memory: " << e.what();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }

        char digits[10] = { '0', '1', '2', '3','4','5','6','7','8','9' };
        ptrIn[LARGE_SHM_SIZE-1] = '\0';
        ptrOut[LARGE_SHM_SIZE-1] = '\0';

        memset(ptrIn, digits[instanceId%10], (LARGE_SHM_SIZE-1));
        memset(ptrOut, ~digits[instanceId%10], (LARGE_SHM_SIZE-1));

        mpi::Command cmd;
        cmd.setCmd(string("ECHO"));
        cmd.addArg(ipcNameIn);
        cmd.addArg(ipcNameOut);

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_INFO(logger, INFOMSG << "Checking slave status");

        try {
            slave->waitForStatus(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in reporting error from slave: MpiSlave::waitForStatus did not report failure";
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                e.raise();
            }
        }
        // Check the shared memory contents
        if (::memcmp(ptrIn, ptrOut, LARGE_SHM_SIZE) != 0) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in echoing data from MPI slave: corrupt data";
        }
        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        slave->waitForExit(_ctx);

        // Cleanup for object no longer in use
        try {
            shmIpcOut->truncate(0,true);
            shmIpcIn->truncate(0,true);
        } catch(scidb::SharedMemoryIpc::SystemErrorException& e) {
            LOG4CXX_WARN(logger, INFOMSG << "Cannot truncate shared memory: " << e.what());
        } catch(scidb::SharedMemoryIpc::InvalidStateException& e) {
            stringstream ss;
            ss << FAILMSG << "Bug in truncating shared memory: " << e.what();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }
        shmIpcIn->unmap();
        shmIpcOut->unmap();
        shmIpcIn->close();
        shmIpcOut->close();
        if (!shmIpcOut->remove()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                << "MPI_TEST: shmIpcOut shared_memory_remove";
        }
        if (!shmIpcIn->remove()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED)
                << "MPI_TEST: shmIpcIn shared_memory_remove";
        }
        if (_mustLaunch) {
            launcher->destroy();
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }


    static const uint32_t SLAVE_DELAY_SEC = 10;   // the slave is told to sleep this long before resonding
    static const uint32_t SLAVE_TIMEOUT_SEC = 5;  // the slave must respond within this much total time
                                                  // these settings allow us to test a slow slave timing out

    void testSlowSlave(const std::string& installPath,
                        const std::shared_ptr<const InstanceMembership>& membership,
                        std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "SLOW_SLAVE test");

	    uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in managing last launch ID";
        }

        syncBarrier(static_cast<int>(launchId), query);

        const uint32_t LAUNCHER_CHECK_DELAY_SEC=1;
        SCIDB_ASSERT(SLAVE_TIMEOUT_SEC > LAUNCHER_CHECK_DELAY_SEC);

        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE test, SLAVE_TIMEOUT_SEC: " << SLAVE_TIMEOUT_SEC);
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE test, LAUNCHER_CHECK_DELAY_SEC: " << LAUNCHER_CHECK_DELAY_SEC);
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE test, SLAVE_DELAY_SEC: " << SLAVE_DELAY_SEC);
        SCIDB_ASSERT(SLAVE_TIMEOUT_SEC < SLAVE_DELAY_SEC);  // because for SLOW_SLAVE (A), we want it to time out

        std::shared_ptr<MpiSlaveProxy> slave = newMPISlaveProxyForTests(launchId, query, installPath,
                                                                              SLAVE_TIMEOUT_SEC,
                                                                              LAUNCHER_CHECK_DELAY_SEC);
        _ctx->setSlave(slave);

        std::shared_ptr<MpiLauncher> launcher;
        if (_mustLaunch) {
            launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
            _ctx->setLauncher(launcher);
            stringstream ss;
            ss << SLAVE_DELAY_SEC;
            std::vector<std::string> args;
            args.push_back(ss.str());
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
        }

        // slave should delay sending a handshake so the following should timeout and land in the catch block
        try {
            LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (A) waiting for handshake");
            slave->waitForHandshake(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in MPI slave: MpiSlaveProxy::waitForHandshake (A) did not timeout";
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (A) error other than OP_FAILED during handshake");
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (A) E_COMM_TIMEOUT during handshake");
                e.raise();
            }
            // expected path
            LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (A) timed out as expected during handshake");
        }

        bool ok=false;
        time_t timeStart = ::time(nullptr);
        time_t timeSlaveTooSlow = timeStart + SLAVE_TIMEOUT_SEC;
        size_t i=0;
        for (time_t time=timeStart; time <= timeSlaveTooSlow; time = ::time(nullptr), ++i) {
            try {
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (B) waiting for handshake #" << i);
                slave->waitForHandshake(_ctx);
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (B) successfull waitForHandshake #" << i);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (B) error other than OP_FAILED during handshake #" << i);
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                    LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (B) E_COMM_TIMEOUT during handshake #" << i);
                    e.raise();
                }
                // expected path
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (B) acceptable timeout during handshake #" << i);
            }
        }
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (B) waitForHandshake, ok is " << ok);
        if (!ok) {
            LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (B) never received the second handshake from the slow slave");
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: SLOW_SLAVE: (B) MpiSlaveProxy::waitForHandshake never received the second handshake from the slow slave";
        }

        // After the handshake the old slave must be gone
        std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
        if (oldSlave) {
            oldSlave->destroy();
            oldSlave.reset();
        }
        _ctx->complete(oldLaunchId);

        // Send command one more time
        mpi::Command cmd;
        cmd.setCmd(string("SLOW_SLAVE"));
        stringstream ss;
        ss << SLAVE_DELAY_SEC;
        cmd.addArg(ss.str());

        slave->sendCommand(cmd, _ctx);

        // slave should send an unexpected response
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (C) waiting for status");
        if (slave->waitForStatus(_ctx, false) != static_cast<uint64_t>(SLAVE_DELAY_SEC)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI_TEST: Bug in MPI slave: (C) MpiSlaveProxy::waitForStatus did not fail on bad status";
        }
        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        try {
            // slave should delay exiting
            LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (D) waitForExit #1");
            slave->waitForExit(_ctx);
            LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (D) waitForExit #1 did not timeout");
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI_TEST: Bug in MPI slave: (D) MpiSlaveProxy::waitForExit did not timeout";
        } catch (scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (D) error other than OP_FAILED during waitForExit #1");
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (D) E_COMM_TIMEOUT during waitForExit #1");
                e.raise();
            }
            // expected
            LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (D) timed out as expected during waitForExit #1");
        }
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (E) waitForExits again");

        ok=false;
        timeStart = ::time(nullptr);
        timeSlaveTooSlow = timeStart + SLAVE_TIMEOUT_SEC;
        i=0;
        for (time_t time=timeStart; time <= timeSlaveTooSlow; time = ::time(nullptr), ++i) {
            try {
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (E) waitForExit #" << i);
                slave->waitForExit(_ctx);
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (E) successfull waitForExit #" << i);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (E) error other than OP_FAILED during waitForExit #"<< i);
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                    LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (E) E_COMM_TIMEOUT during waitForExit #" << i);
                    e.raise();
                }
                // expected
                LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (E) acceptable timeout during waitForExit #" << i);
            }
        }
        LOG4CXX_INFO(logger, INFOMSG << "SLOW_SLAVE: (E) waitForExit, ok is " << ok);
        if (!ok) {
            LOG4CXX_ERROR(logger, INFOMSG << "SLOW_SLAVE: (E) never received the second waitForExit from the slow slave");
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI_TEST: SLOW_SLAVE (E) MpiSlaveProxy::waitForExit timeout";   // <<<<<<<<<<<<<<<<<<<<<<<<<<<<< ERROR
        }
        if (_mustLaunch) {
            launcher->destroy();
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testSlaveExit(const std::string& installPath,
                          const std::shared_ptr<const InstanceMembership>& membership,
                          std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "ABNORMAL_EXIT test");

	uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in manging last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath, SLAVE_TIMEOUT_SEC));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
	    launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	//Get the handshake
        LOG4CXX_INFO(logger, INFOMSG << "ABNORMAL_EXIT: waiting for handshake");
        bool ok=false;
        // TODO, this is still done with max tries and probably is still subject
        //       to issues when liveness-timeout is changed
        //       But no current test is broken because of this, so we will make this
        //       change separately from the initial fix for sdb-6294
        const uint32_t maxTries = 1 + scidb::getLivenessTimeout() / SLAVE_TIMEOUT_SEC ;
        for (uint32_t i=0; i < maxTries; ++i) {
            try {
                slave->waitForHandshake(_ctx);
                ok = true;
                break;
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                    e.raise();
                }
                // expected
            }
        }
        if (!ok) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI_TEST: MpiSlaveProxy::waitForHandshake timeout";
        }

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

        // Send command
        mpi::Command cmd;
        cmd.setCmd(string("ABNORMAL_EXIT"));
        const int SLAVE_ERR_EXIT_CODE = 7;
        stringstream ss;
        ss << SLAVE_ERR_EXIT_CODE;
        cmd.addArg(ss.str());

        syncBarrier(static_cast<int>(launchId), query);

        LOG4CXX_INFO(logger, INFOMSG << "ABNORMAL_EXIT: sending command");

        slave->sendCommand(cmd, _ctx);

        bool eofConsumed=false;
        // slave should not respond
        try {
            slave->waitForStatus(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in MPI slave: MpiSlaveProxy::waitForStatus did not fail";
        } catch (scidb::SystemException& e) {

            if ((e.getLongErrorCode() == SCIDB_LE_UNKNOWN_ERROR) &&
                (e.getErrorMessage().find(mpi::E_EARLY_DISCONNECT) != std::string::npos) ){
                // expected
                // the EOF message should already be consumed by waitForStatus
                eofConsumed=true;
            } else if (_mustLaunch &&
                       (e.getLongErrorCode() == SCIDB_LE_OPERATION_FAILED) &&
                       (e.getErrorMessage().find(mpi::E_ALREADY_TERMINATED) != std::string::npos) ){
                // expected
            } else {
                LOG4CXX_ERROR(logger, FAILMSG << "ABNORMAL_EXIT: BUG in waitForStatus !!!");
                e.raise();
            }
        }

        LOG4CXX_INFO(logger, INFOMSG << "ABNORMAL_EXIT: waitForExit now");
        try {
            slave->waitForExit(_ctx);
            if (eofConsumed) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                       << "MPI_TEST: Bug in MPI slave: MpiSlaveProxy::waitForExit did not timeout";
            }
        } catch (scidb::SystemException& e) {
            if (!eofConsumed) {
                e.raise();
            }
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_COMM_TIMEOUT) == std::string::npos) {
                e.raise();
            }
            // expected
        }

        // launcher should fail to complete cleanly
        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI launcher: MpiLauncher destroy() did not fail as expected";
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    e.raise();
                }
                // expected
            }
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testBadMessage(const std::string& installPath,
                        const std::shared_ptr<const InstanceMembership>& membership,
                        std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG <<  "BAD_MSG from slave test");

	uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in managing last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	//-------------------- Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_MSG"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_INFO(logger, INFOMSG << "Checking slave status");
        try {
            slave->waitForStatus(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in detecting invalid status from slave: "
                   "MpiSlave::waitForStatus did not report invalid status";
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_INVALID_SLAVE_STATUS) == std::string::npos) {
                e.raise();
            }
        }

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI launcher: MpiLauncher destroy() did not fail as expected";
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    e.raise();
                }
                // expected
            }
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testBadMessageFlood(const std::string& installPath,
                             const std::shared_ptr<const InstanceMembership>& membership,
                             std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "BAD_MSG_FLOOD from slave test");

	uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in managing last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_MSG_FLOOD"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_INFO(logger, INFOMSG << "Checking slave status");
        const size_t MSG_NUM = 10000;
        size_t i=0;
        const int WAIT_FOR_SLAVE_ERR_MSGS_SEC=2;
        ::sleep(WAIT_FOR_SLAVE_ERR_MSGS_SEC); // let slave messages accumulate
        for (; i <= MSG_NUM; ++i) {
            try {
                slave->waitForStatus(_ctx);
                break;
            } catch(scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                    e.raise();
                }
                if (e.getErrorMessage().find(mpi::E_INVALID_SLAVE_STATUS) == std::string::npos) {
                    e.raise();
                }
                // expected
            }
        }
        if (i > MSG_NUM) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in slave message delivery/order";
        }
        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            launcher->destroy();
        }

        // clear all the bogus messages the slave has sent us
        slave = std::shared_ptr<MpiSlaveProxy>(new MpiSlaveProxy(launchId+1, query, installPath));
        // Get the malformed handshake
        try {
            slave->waitForHandshake(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in detecting invalid handshake from slave: "
                   "MpiSlave::waitForHandshake did not report invalid PID";
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_BAD_HANDSHAKE_PID) == std::string::npos) {
                e.raise();
            }
            // expected
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testBadHandshake(const std::string& installPath,
                          const std::shared_ptr<const InstanceMembership>& membership,
                          std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "BAD_HANDSHAKE from slave test");

	uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in managing last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_HANDSHAKE"));

        slave->sendCommand(cmd, _ctx);

        LOG4CXX_INFO(logger, INFOMSG << "Checking slave status");
        try {
            slave->waitForStatus(_ctx);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in detecting invalid status from slave: "
                   "MpiSlave::waitForStatus did not report invalid status";
        } catch(scidb::SystemException& e) {
            if (e.getLongErrorCode() != SCIDB_LE_UNKNOWN_ERROR) {
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_INVALID_SLAVE_STATUS) == std::string::npos) {
                e.raise();
            }
            // expected
        }

        cmd.clear();
        cmd.setCmd(string("EXIT"));

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        slave->waitForExit(_ctx);

        if (_mustLaunch) {
            launcher->destroy();
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

    void testBadStatus(const std::string& installPath,
                       const std::shared_ptr<const InstanceMembership>& membership,
                       std::shared_ptr<Query>& query)
    {
        LOG4CXX_INFO(logger, STARTMSG << "BAD_STATUS from slave test");

	uint64_t launchId =_ctx->getNextLaunchId();
        uint64_t oldLaunchId = _ctx->getLastLaunchIdInUse();
        if (launchId-1 != oldLaunchId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << "MPI_TEST: Bug in managing last launch ID";
        }

	std::shared_ptr<MpiSlaveProxy> slave(new MpiSlaveProxy(launchId, query, installPath));
	_ctx->setSlave(slave);

	std::shared_ptr<MpiLauncher> launcher;
	if (_mustLaunch) {
	    launcher = std::shared_ptr<MpiLauncher>(MpiManager::getInstance()->newMPILauncher(launchId, query));
	    _ctx->setLauncher(launcher);
            std::vector<std::string> args;
            launchMpiJob(launcher, args, membership, query, query->getInstancesCount());
	}

        LOG4CXX_INFO(logger, INFOMSG << "BAD_STATUS: waiting for handshake");
	// Get the handshake
	slave->waitForHandshake(_ctx);

	// After the handshake the old slave must be gone
	std::shared_ptr<MpiSlaveProxy> oldSlave = _ctx->getSlave(oldLaunchId);
	if (oldSlave) {
	    oldSlave->destroy();
	    oldSlave.reset();
	}
	_ctx->complete(oldLaunchId);

        mpi::Command cmd;
        cmd.setCmd(string("BAD_STATUS"));

        LOG4CXX_INFO(logger, INFOMSG << "BAD_STATUS: waiting for barrier");

        syncBarrier(static_cast<int>(launchId), query);

        slave->sendCommand(cmd, _ctx);

        // slave should be disconnected
        try {
            slave->waitForExit(_ctx);
        } catch (scidb::SystemException& e) {
            if (!_mustLaunch) {
                e.raise();
            }
            if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                e.raise();
            }
            if (e.getErrorMessage().find(mpi::E_LAUNCHER_FAILED) == std::string::npos) {
                e.raise();
            }

            // expected
        }

        LOG4CXX_INFO(logger, INFOMSG << "BAD_STATUS: waitForExit complete");

        if (_mustLaunch) {
            try {
                launcher->destroy();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                    << "MPI_TEST: Bug in MPI launcher: MpiLauncher destroy() did not fail as expected";
            } catch (scidb::SystemException& e) {
                if (e.getLongErrorCode() != SCIDB_LE_OPERATION_FAILED) {
                    e.raise();
                }
                // expected
            }
        }

        LOG4CXX_INFO(logger, PASSMSG);
    }

private:
    bool _mustLaunch;
    const uint64_t NUM_LAUNCH_TESTS;
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalMpiTest, "_mpi_test", "PhysicalMpiTest");

}
