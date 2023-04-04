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

#include <mpi/MPIManager.h>

#include <sys/types.h>
#include <signal.h>
#include <ostream>
#include <memory>
#include <log4cxx/logger.h>
#include <system/UserException.h>
#include <system/Cluster.h>
#include <system/Utils.h>
#include <system/Config.h>
#include <network/Network.h>
#include <util/FileIO.h>
#include <mpi/MPISlaveProxy.h>
#include <mpi/MPIUtils.h>
#include <network/proto/scidb_msg.pb.h>
#include <network/NetworkMessageFactory.h>

using namespace std;

namespace scidb
{
namespace mpi
{
SharedMemoryIpc::SharedMemoryIpcType_t getShmIpcType()
{
    if (scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_SHM_TYPE) == "FILE") {
        return SharedMemoryIpc::FILE_TYPE;
    }
    return SharedMemoryIpc::SHM_TYPE;
}
}

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.mpi"));

bool startsWith(const std::string& left, const std::string& right)
{
  if (right.size() > left.size()) {
    return false;
  }
  return (0 == left.compare(0, right.size(), right));
}

MpiManager::MpiManager()
  :
    _event(),
    _isReady(false),
    _mpiResourceTimeout(MPI_RESOURCE_TIMEOUT_SEC)
{
    const time_t MIN_CLEANUP_PERIOD_SEC = 5;
    scidb::Scheduler::Work workItem = std::bind(&MpiManager::initiateCleanup);
    _cleanupScheduler = scidb::getScheduler(workItem, MIN_CLEANUP_PERIOD_SEC);

    string mpiTypeStr = scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_TYPE);
    assert(!mpiTypeStr.empty());
    _mpiInstallDir = scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_DIR);
    assert(!_mpiInstallDir.empty());

    if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::MPICH2SCIDB)) ) {
      _mpiType = mpi::MPICH2SCIDB;
      _mpiDaemonBin = mpi::MPICH2SCIDB_DAEMON_BIN;
      _mpiLauncherBin = mpi::MPICH_LAUNCHER_BIN;
    } else if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::MPICH_FLAVOR)) ) {
      _mpiType = mpi::MPICH_FLAVOR;
      _mpiDaemonBin = mpi::MPICH_DAEMON_BIN;
      _mpiLauncherBin = mpi::MPICH_LAUNCHER_BIN;
    } else if (startsWith(mpiTypeStr, mpi::getMpiTypeStr(mpi::OPENMPI_FLAVOR)) ) {
      _mpiType = mpi::OPENMPI_FLAVOR;
      _mpiDaemonBin = mpi::OMPI_DAEMON_BIN;
      _mpiLauncherBin = mpi::OMPI_LAUNCHER_BIN;
    } else {
      throw (SYSTEM_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_IN_CONFIGURATION_FILE)
             << string("Unrecognized MPI type value: ")+mpiTypeStr);
   }
}

void MpiManager::initiateCleanup()
{
    scidb::WorkQueue::WorkItem item = std::bind(&MpiErrorHandler::cleanAll);
    std::shared_ptr<scidb::WorkQueue> q = scidb::getWorkQueue();
    try {
        q->enqueue(item);
    } catch (const scidb::WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(logger, "Overflow exception from the work queue: "<<e.what());
        assert(false);
        // it is not fatal, try again
        MpiManager::getInstance()->cleanup();
    }
}
void MpiManager::cleanup()
{
    _cleanupScheduler->schedule();
}
void MpiManager::init()
{
    LOG4CXX_TRACE(logger, "MpiManager::init() has been called");

    std::shared_ptr<scidb::NetworkMessageFactory> factory = scidb::getNetworkMessageFactory();

    std::shared_ptr<MpiMessageHandler> msgHandler(new MpiMessageHandler());
    factory->addMessageType(scidb::mtMpiSlaveHandshake,
                            std::bind(&MpiMessageHandler::createMpiSlaveHandshake,
                                      msgHandler,
                                      std::placeholders::_1),
                            std::bind(&MpiMessageHandler::handleMpiSlaveHandshake,
                                      msgHandler,
                                      std::placeholders::_1));

    factory->addMessageType(scidb::mtMpiSlaveResult,
                            std::bind(&MpiMessageHandler::createMpiSlaveResult,
                                      msgHandler,
                                      std::placeholders::_1),
                            std::bind(&MpiMessageHandler::handleMpiSlaveResult,
                                      msgHandler,
                                      std::placeholders::_1));

    scidb::NetworkMessageFactory::MessageHandler emptyHandler;
    factory->addMessageType(scidb::mtMpiSlaveCommand,
                            std::bind(&MpiMessageHandler::createMpiSlaveCommand,
                                      msgHandler,
                                      std::placeholders::_1),
                            emptyHandler);

    LOG4CXX_TRACE(logger, "MpiManager::init() mtMpiSlaveHandshake, mtMpiSlaveResult, mtMpiSlaveCommand registered");
}

/**
 * Creates symbolic links to mpirun, orted, and scidb_mpi_slave
 * @return true if links are created (or have already existed), false otherwise
 */
void MpiManager::initMpiLinks(const std::string& installPath,
                              const std::string& mpiPath,
                              const std::string& pluginPath)
{
    // mpi
    string target = getMpiDir(installPath);

    int rc = ::symlink(mpiPath.c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
            << "symlink" << rc << err << ::strerror(err) << target;
    }

    // mpi_slave_scidb
    target = mpi::getSlaveBinFile(installPath);
    rc = ::symlink(mpi::getSlaveSourceBinFile(pluginPath).c_str(), target.c_str());
    if (rc !=0 && errno != EEXIST) {
        int err = errno;
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
            << "symlink" << rc << err << ::strerror(err) << target;
    }
}

void MpiManager::forceInitMpi()
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    initMpi();
}

void MpiManager::initMpi()
{
    const MembershipID minMembId(0);
    const std::shared_ptr<const InstanceMembership> membership =
       Cluster::getInstance()->getInstanceMembership(minMembId);

    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string dir = mpi::getLogDir(installPath);
    int rc;
    if (0 != (rc = scidb::File::createDir(dir))) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
               << dir << ::strerror(rc);
    }
    dir = mpi::getPidDir(installPath);
    if (0 != (rc = scidb::File::createDir(dir))) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
            << dir << ::strerror(rc);
    }
    if (mpi::getShmIpcType() == SharedMemoryIpc::FILE_TYPE) {
        dir = mpi::getIpcDir(installPath);
        if (0 != (rc = scidb::File::createDir(dir))) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_DIRECTORY)
                << dir << ::strerror(rc);
        }
    }
    initMpiLinks(installPath,
                 scidb::Config::getInstance()->getOption<string>(CONFIG_MPI_DIR),
                 scidb::Config::getInstance()->getOption<string>(CONFIG_PLUGINSDIR));
    _isReady = true;
    // read the proc table and kill all the mpi procs started by the old process of this instance
    MpiErrorHandler::killAllMpiProcs();
    cleanup();
}

bool MpiManager::checkForError(scidb::QueryID queryId, double startTime, double timeout)
{
    Query::validateQueryPtr(Query::getQueryByID(queryId));
    return (!mpi::hasExpired(startTime, timeout));
}

std::shared_ptr<MpiOperatorContext>
MpiManager::checkAndSetCtx(const std::shared_ptr<Query>& query,
                           const std::shared_ptr<MpiOperatorContext>& ctx)
{
    const scidb::QueryID queryId = query->getQueryID();
    LOG4CXX_TRACE(logger, "MpiManager::checkAndSetCtx: queryID="<<queryId << ", ctx=" << ctx.get());
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    if (!_isReady) {
        assert(ctx);
        initMpi();
    }

    // Note: cascading of scalapack operators is allowed even though we use the same ctx for all of them,
    // because each of them only runs in their execute, not concurrently, producing a result in
    // (once-shared-but-no-longer)  memory that should have no dependence on MPI after the execute() returns.
    // This implies that MpiOperatorContext must be inserted by the operator at the beginning of execution
    // (not by the slave messages, which are during).
    // Hence, assert(ctx) above.
    return checkAndSetCtxAsync(query, ctx);
}

std::shared_ptr<MpiOperatorContext>
MpiManager::checkAndSetCtxAsync(const std::shared_ptr<Query>& query,
                                const std::shared_ptr<MpiOperatorContext>& ctx)
{
    const scidb::QueryID queryId = query->getQueryID();

    LOG4CXX_TRACE(logger, "MpiManager::checkAndSetCtxAsync: queryID="<<queryId << ", ctx=" << ctx.get());
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);

    query->validate();

    std::pair<ContextMap::iterator, bool> res = _ctxMap.insert(ContextMap::value_type(queryId, ctx));
    if (res.second) { // did it get inserted ?
        assert((*res.first).second == ctx);
        try {
            if (!ctx) {
                assert(false);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
                       << "MpiManager::checkAndSetCtxAsync");
            }
            std::shared_ptr<MpiErrorHandler> eh(new MpiErrorHandler(ctx));
            Query::Finalizer f = std::bind(&MpiErrorHandler::finalize, eh, std::placeholders::_1);
            query->pushVotingFinalizer(f);
            query->pushErrorHandler(eh);
        } catch (const scidb::Exception& e) {
            removeCtx(queryId);
            cleanup();
            e.raise();
        }
        cleanup();
    }
    return (*res.first).second;
}

bool MpiManager::removeCtx(scidb::QueryID queryId)
{
    LOG4CXX_DEBUG(logger, "MpiManager::removeCtx: queryID="<<queryId);
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    return (_ctxMap.erase(queryId) > 0);
}

void MpiOperatorContext::setSlave(const std::shared_ptr<MpiSlaveProxy>& slave)
{
    setSlaveInternal(slave->getLaunchId(), slave);
}

void
MpiOperatorContext::setSlaveInternal(uint64_t launchId,
                                     const std::shared_ptr<MpiSlaveProxy>& slave)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = getIter(launchId);
    iter->second->_slave = slave;
}

void
MpiOperatorContext::setLauncherInternal(uint64_t launchId,
                                        const std::shared_ptr<MpiLauncher>& launcher)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = getIter(launchId);
    iter->second->_launcher = launcher;
}

bool
MpiOperatorContext::addSharedMemoryIpc(uint64_t launchId,
                                       const std::shared_ptr<SharedMemoryIpc>& ipc)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = getIter(launchId);
    bool isInserted = iter->second->_shmIpcs.insert(ipc).second;
    assert(isInserted);
    return isInserted;
}

std::shared_ptr<MpiSlaveProxy>
MpiOperatorContext::getSlave(uint64_t launchId)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return std::shared_ptr<MpiSlaveProxy>();
    }
    return iter->second->_slave;
}

std::shared_ptr<MpiLauncher>
MpiOperatorContext::getLauncher(uint64_t launchId)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return std::shared_ptr<MpiLauncher>();
    }
    return iter->second->_launcher;
}

std::shared_ptr<SharedMemoryIpc>
MpiOperatorContext::getSharedMemoryIpc(uint64_t launchId, const std::string& name)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {
        return std::shared_ptr<SharedMemoryIpc>();
    }
    std::shared_ptr<SharedMemoryIpc> key(new SharedMemory(name));
    LaunchInfo::ShmIpcSet::iterator ipcIter = iter->second->_shmIpcs.find(key);
    if (ipcIter == iter->second->_shmIpcs.end()) {
        return std::shared_ptr<SharedMemoryIpc>();
    }
    return (*ipcIter);
}

std::shared_ptr<scidb::ClientMessageDescription>
MpiOperatorContext::popMsg(uint64_t launchId,
                           LaunchErrorChecker& errChecker)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    LaunchMap::iterator iter = _launches.find(launchId);
    while ((iter == _launches.end()) ||
           (!iter->second->_msg)) {
        Event::ErrorChecker ec =
        std::bind(&MpiOperatorContext::checkForError, this, launchId, errChecker);
        _event.wait(_mutex, ec, PTW_EVENT_EXT_LAUNCH);
        iter = _launches.find(launchId);
    }
    std::shared_ptr<scidb::ClientMessageDescription> msg;
    (iter->second->_msg).swap(msg);
    return msg;
}

void
MpiOperatorContext::pushMsg(uint64_t launchId,
                            const std::shared_ptr<scidb::ClientMessageDescription>& msg)
{
    ScopedMutexLock lock(_mutex, PTW_SML_MPIM);
    const bool dontUpdateLastInUse = false;
    LaunchMap::iterator iter = getIter(launchId, dontUpdateLastInUse);
    iter->second->_msg = msg;

    _event.signal();
}

void MpiOperatorContext::clear(LaunchCleaner& cleaner)
{
    if (cleaner) {
        for (LaunchMap::iterator iter = _launches.begin();
             iter != _launches.end(); ++iter) {
            cleaner(iter->first, iter->second.get());
        }
    }
    _launches.clear();
}

bool
MpiOperatorContext::checkForError(uint64_t launchId, LaunchErrorChecker& errChecker)
{
    Query::validateQueryPtr(_query.lock());

    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter != _launches.end() && iter->second->_msg) {
        // the message is ready, we must have missed the signal on timeout
        return false;
    }
    if (errChecker && !errChecker(launchId, this)) {
        return false;
    }
    return true;
}

MpiOperatorContext::LaunchMap::iterator
MpiOperatorContext::getIter(uint64_t launchId, bool updateLastLaunchId)
{
    LaunchMap::iterator iter = _launches.find(launchId);
    if (iter == _launches.end()) {

        if (_lastLaunchIdInUse >= launchId) {
            // When we are populating a new context from the operator,
            // the last launch ID in use is not allowed to decrease.
            // The contract is that the launch IDs must strictly increase.
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context does not allow for decreasing launch IDs");
        }
        if (_launches.size() > 1) {
            // each launch must be serialized by the coordinator,
            // workers also process launches serially, so at any moment
            // there can be messages from at most 2 launches (slaves).
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context is corrupted");
        }
        std::shared_ptr<LaunchInfo> linfo(new LaunchInfo);
        iter = _launches.insert(make_pair(launchId, linfo)).first;
    }
    if (updateLastLaunchId) {
        _lastLaunchIdInUse = std::max(_lastLaunchIdInUse,launchId);
    }
    return iter;
}

MessagePtr MpiMessageHandler::createMpiSlaveCommand(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveCommand());
}

MessagePtr MpiMessageHandler::createMpiSlaveHandshake(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveHandshake());
}

void MpiMessageHandler::handleMpiSlaveHandshake(const std::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveHandshake, scidb::mtMpiSlaveHandshake>(messageDesc);
}

MessagePtr MpiMessageHandler::createMpiSlaveResult(MessageID id)
{
    return scidb::MessagePtr(new scidb_msg::MpiSlaveResult());
}

void MpiMessageHandler::handleMpiSlaveResult(const std::shared_ptr<scidb::MessageDescription>& messageDesc)
{
    handleMpiSlaveMessage<scidb_msg::MpiSlaveResult, scidb::mtMpiSlaveResult>(messageDesc);
}

void MpiMessageHandler::handleMpiSlaveDisconnect(uint64_t launchId,
                                                 const std::shared_ptr<scidb::Query>& query)
{
    if (!query) {
        return;
    }
    arena::ScopedArenaTLS arenaTLS(query->getArena());

    std::shared_ptr< scidb::ClientMessageDescription >
        cliMsg(new MpiOperatorContext::EofMessageDescription());
    try {
        MpiMessageHandler::processMessage(launchId, cliMsg, query);
    } catch (const scidb::Exception& e) {
        query->handleError(e.clone());
    }
}

void MpiMessageHandler::processMessage(uint64_t launchId,
                                       const std::shared_ptr<scidb::ClientMessageDescription>& cliMsg,
                                       const std::shared_ptr<scidb::Query>& query)
{
    assert(arena::Arena::getArenaTLS() == query->getArena());

    try {
        std::shared_ptr<MpiOperatorContext> emptyCtx;
        std::shared_ptr<MpiOperatorContext> ctx =
            MpiManager::getInstance()->checkAndSetCtxAsync(query, emptyCtx);
        assert(ctx);

        if (!ctx) {
            // the context did not exist before this call
            // XXX TODO: Currently, we do not allow more than one MPI query to execute concurrently.
            //           The serialization is implemented in MpiManager::checkAndSetCtx() called by MPIPhysical.
            //           That method can block for a long time; therefore, we must not call it
            //           when processing a client message (on the network thread).
            //           So, to still guarantee the serialized query execution,
            //           MPIPhysical::launchSlaves() inserts a barrier which guarantees
            //           that all instances insert a new MpiOperatorContext before any slaves are spawned
            //           (and any slave message are sent). Here, we error out if the context is not already set.
            //           When the serialized access restriction is lifted, we should be able to remove the barrier
            //           and allow MpiMessageHandler::processMessage() insert a new context if it does not exist.
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "MPI-based operator context not found");
        }
        ctx->pushMsg(launchId, cliMsg);

        LOG4CXX_DEBUG(logger, "MpiMessageHandler::processMessage: queryID="<< query->getQueryID()
                      << ", ctx = " << ctx.get()
                      << ", launchId = " << launchId
                      << ", messageType = " << strMsgType(cliMsg->getMessageType())
                      << ", queryID = " << cliMsg->getQueryId());
    }
    catch (const scidb::Exception& e)
    {
        LOG4CXX_ERROR(logger, "MpiMessageHandler::processMessage:"
                      " Error occurred in slave message handler: "
                      << e.what()
                      << ", messageType = " << strMsgType(cliMsg->getMessageType())
                      << ", queryID = " << cliMsg->getQueryId());
        e.raise();
    }
}

MpiErrorHandler::~MpiErrorHandler()
{}

void MpiErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    MpiOperatorContext::LaunchCleaner cleaner = std::bind(&MpiErrorHandler::clean,
                                                          query->getQueryID(),
                                                          std::placeholders::_1,
                                                          std::placeholders::_2);
    _ctx->clear(cleaner);
}

void MpiErrorHandler::finalize(const std::shared_ptr<Query>& query)
{
    MpiManager::getInstance()->removeCtx(query->getQueryID());

    const uint64_t lastIdInUse = _ctx->getLastLaunchIdInUse();

    LOG4CXX_TRACE(logger, "MpiErrorHandler::finalize: destorying last slave for launch = " << lastIdInUse);

    std::shared_ptr<MpiSlaveProxy> slave = _ctx->getSlave(lastIdInUse);
    if (slave) {
        slave->destroy();
    }
}

void MpiErrorHandler::clean(scidb::QueryID queryId, uint64_t launchId,
                            MpiOperatorContext::LaunchInfo* info)
{
    MpiOperatorContext::LaunchInfo::ShmIpcSet& ipcs = info->_shmIpcs;
    for (MpiOperatorContext::LaunchInfo::ShmIpcSet::const_iterator ipcIter = ipcs.begin();
         ipcIter != ipcs.end(); ++ipcIter) {
        const std::shared_ptr<SharedMemoryIpc>& ipc = *ipcIter;

        ipc->close();
        if (!ipc->remove()) {
            LOG4CXX_ERROR(logger, "Failed to remove shared memory IPC = "
                          << ipc->getName());
        }
    }
    if (info->_slave) {
        try {
            info->_slave->destroy(true);
            assert(launchId == info->_slave->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy slave for launch = "
                                  << info->_slave->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
    if (info->_launcher) {
        try {
            info->_launcher->destroy(true);
            assert(launchId == info->_launcher->getLaunchId());
        } catch (std::exception& e) {
            LOG4CXX_ERROR(logger, "Failed to destroy launcher for launch = "
                          << info->_launcher->getLaunchId()
                          << " ("<< queryId << ")"
                          << " because: "<<e.what());
        }
    }
}

static void getQueryId(std::set<scidb::QueryID>* queryIds,
                       const std::shared_ptr<scidb::Query>& q)
{
    queryIds->insert(q->getQueryID());
    LOG4CXX_DEBUG(logger, "Next query ID: "<<q->getQueryID());
}

void MpiErrorHandler::cleanupLauncherPidFile(const std::string& installPath,
                                             const std::string& clusterUuid,
                                             const std::string& fileName)
{
    const char *myFuncName = "MpiErrorHandler::cleanupLauncherPidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    assert(pids.size()==2);
    LOG4CXX_DEBUG(logger,  myFuncName << ": killing launcher pid group = " << pids[0]);

    // kill launcher's proc group
    if (!killProc(installPath, clusterUuid, -pids[0])) {
        LOG4CXX_DEBUG(logger,  myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::cleanupSlavePidFile(const std::string& installPath,
                                          const std::string& clusterUuid,
                                          const std::string& fileName,
                                          QueryID queryId)
{
    const char *myFuncName = "MpiErrorHandler::cleanupSlavePidFile";
    std::vector<pid_t> pids;
    if (!mpi::readPids(fileName, pids)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read pids for: "<<fileName);
        return;
    }
    bool removeFile = true;

    // kill slave
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave pid = "<<pids[0]);
    if (killProc(installPath, clusterUuid, pids[0], queryId)) {
        removeFile = false;
    }

    // kill slave parent (orted/hydra_pmi_proxy)
    LOG4CXX_DEBUG(logger, myFuncName << ": killing slave ppid ="<<pids[1]);
    if (killProc(installPath, clusterUuid, pids[1], queryId)) {
        removeFile = false;
    }
    if (removeFile) {
        LOG4CXX_DEBUG(logger, myFuncName << ": removing file: "<<fileName);
        scidb::File::remove(fileName.c_str(), false);
    }
}

void MpiErrorHandler::cleanAll()
{
    const char *myFuncName = "MpiErrorHandler::cleanAll";
    // read ipc files

    const MembershipID minMembId(0);
    const std::shared_ptr<const InstanceMembership> membership =
       Cluster::getInstance()->getInstanceMembership(minMembId);

    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string uuid = Cluster::getInstance()->getUuid();
    InstanceID myInstanceId = Cluster::getInstance()->getLocalInstanceId();

    std::string ipcDirName = mpi::getIpcDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": SHM directory: "<< ipcDirName);

    // read shared memory ipc files
    std::list<std::string> ipcFiles;
    try {
        scidb::File::readDir(ipcDirName.c_str(), ipcFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << ipcDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            e.raise();
        }
        ipcFiles.clear();
    }

    // read pid files
    std::string pidDirName = mpi::getPidDir(installPath);
    LOG4CXX_TRACE(logger, myFuncName << ": PID directory: "<< pidDirName);

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(pidDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "<< pidDirName
                     <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            e.raise();
        }
        pidFiles.clear();
    }

    // collect query IDs AFTER reading the files
    std::set<QueryID> queryIds;
    scidb::Query::visitQueries(Query::Visitor(std::bind(&getQueryId, &queryIds, std::placeholders::_1)));

    // identify dead shm objects
    for (std::list<std::string>::iterator iter = ipcFiles.begin();
         iter != ipcFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next SHM object: "<<fileName);
        QueryID queryId;
        uint64_t launchId=0;
        uint64_t instanceId=myInstanceId;

        if (!mpi::parseSharedMemoryIpcName(fileName,
                                           uuid,
                                           instanceId,
                                           queryId,
                                           launchId)) {
            // ignore file with unknown name
            continue;
        }

        if (instanceId != myInstanceId
            || queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }

        const string ipcFileName = ipcDirName+"/"+fileName;

        LOG4CXX_DEBUG(logger, myFuncName << ": removing SHM object: "<< ipcFileName);

        if (File::remove(ipcFileName.c_str(), false) != 0) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to remove SHM object: "<< ipcFileName);
        }
    }

    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_DEBUG(logger, myFuncName << ": next pid object: "<<fileName);
        QueryID queryId;
        uint64_t coordId=0;
        uint64_t id=0;
        uint64_t launchId=0;
        int n=0;
        int rc = ::sscanf(fileName.c_str(), "%" PRIu64 ".%" PRIu64 ".%" PRIu64 ".%n",
                          &coordId, &id, &launchId, &n);
        if (rc == EOF || rc < 3) {
            // ignore file with unknown name
            continue;
        }

        queryId = QueryID(coordId,id);

        if (queryIds.find(queryId) != queryIds.end()) {
            // ignore live file
            continue;
        }
        assert(n >= 0);
        size_t len = fileName.size();
        assert(static_cast<size_t>(n) <= len);

        if (fileName.compare(n, len-n, mpi::LAUNCHER_BIN)==0) {
            cleanupLauncherPidFile(installPath, uuid, pidDirName+"/"+fileName);
        } else if (fileName.compare(n, len-n, mpi::SLAVE_BIN)==0) {
            cleanupSlavePidFile(installPath, uuid, pidDirName+"/"+fileName);
        }
    }
}

void MpiErrorHandler::killAllMpiProcs()
{
    const char *myFuncName = "MpiErrorHandler::killAllMpiProcs";
    const MembershipID minMembId(0);
    const std::shared_ptr<const InstanceMembership> membership =
       Cluster::getInstance()->getInstanceMembership(minMembId);

    const std::string& installPath = MpiManager::getInstallPath(membership);

    std::string clusterUuid = Cluster::getInstance()->getUuid();

    // read pid files
    std::string procDirName = mpi::getProcDirName();

    std::list<std::string> pidFiles;
    try {
        scidb::File::readDir(procDirName.c_str(), pidFiles);
    } catch (const scidb::SystemException& e) {
        LOG4CXX_WARN(logger, myFuncName << ": unable to read: "
                     << procDirName <<" because: "<<e.what());
        if (e.getLongErrorCode() != scidb::SCIDB_LE_SYSCALL_ERROR) {
            e.raise();
        }
        pidFiles.clear();
    }
    // identify dead pid files
    for (std::list<std::string>::iterator iter = pidFiles.begin();
         iter != pidFiles.end(); ++iter) {
        const std::string& fileName = *iter;
        LOG4CXX_TRACE(logger, myFuncName << ": next pid object: "<<fileName);
        pid_t pid=0;
        int rc = ::sscanf(fileName.c_str(), "%d", &pid);
        if (rc == EOF || rc < 1) {
            // ignore file with unknown name
            continue;
        }
        assert(pid >= 1);
        killProc(installPath, clusterUuid, pid);
    }
}

bool MpiErrorHandler::killProc(const std::string& installPath,
                               const std::string& clusterUuid,
                               pid_t pid,
                               QueryID queryId)
{
    const char *myFuncName = "MpiErrorHandler::killProc";
    bool doesProcExist = false;
    if (pid > 0) {
        if (!MpiManager::getInstance()->canRecognizeProc(installPath, clusterUuid, pid, queryId)) {
            return doesProcExist;
        }
        LOG4CXX_DEBUG(logger, myFuncName << ": killing pid = "<<pid);
    } else {
        LOG4CXX_DEBUG(logger, myFuncName << ": killing process group pid = "<<pid);
    }
    assert(pid != ::getpid());
    assert(pid != ::getppid());
    assert(pid > 1 || pid < -1);

    // kill proc
    int rc = ::kill(pid, SIGKILL);
    int err = errno;
    if (rc!=0) {
        if (err==EINVAL) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                << "kill" << rc << err << ::strerror(err) << pid;
        }
        if (err!=ESRCH) {
            LOG4CXX_ERROR(logger, myFuncName << ": failed to kill pid = "
                          << pid << " err = " << err);
        } else {
            LOG4CXX_DEBUG(logger, myFuncName << ": failed to kill pid = "
                          << pid << " err = " << err);
        }
    }
    doesProcExist = (rc==0 || err!=ESRCH);
    return doesProcExist;
}

bool MpiManager::canRecognizeProc(const std::string& installPath,
                                  const std::string& clusterUuid,
                                  pid_t pid,
                                  QueryID myQueryId)

{
    const char *myFuncName = "MpiErrorHandler::canRecognizeProc";
    assert(pid>0);
    stringstream ss;
    ss<<pid;
    const std::string pidFileName = ss.str();

    std::string procName;
    if (!mpi::readProcName(pidFileName, procName)) {
        LOG4CXX_WARN(logger, myFuncName << ": cannot read:" << pidFileName);
        return false;
    }

    if (procName != mpi::getSlaveBinFile(installPath) &&
        procName != getLauncherBinFile(installPath) &&
        procName != getDaemonBinFile(installPath)) {
        LOG4CXX_TRACE(logger, myFuncName <<
                      ": unrecognizable process (pid="<<pid<<") command line: "<<procName);
        return false;
    }

    std::string procEnvVar;
    if (!mpi::readProcEnvVar(pidFileName,
                        mpi::SCIDBMPI_ENV_VAR,
                             procEnvVar)) {
        LOG4CXX_DEBUG(logger, myFuncName << ": cannot extract " << mpi::SCIDBMPI_ENV_VAR
                      << " from " << pidFileName);
        return false;
    }

    QueryID queryId;
    uint64_t launchId(0);
    if (!mpi::parseScidbMPIEnvVar(procEnvVar, clusterUuid, queryId, launchId)) {
        LOG4CXX_DEBUG(logger, myFuncName << ": cannot parse " << procEnvVar << " from " << pidFileName);
        return false;
    }

    if (queryId == myQueryId) {
        return true;
    }
    if (!queryId.isValid()) {
        LOG4CXX_DEBUG(logger, myFuncName << ": query " << queryId << " invalid, return true");
        return true;
    }
    if (Query::getQueryByID(queryId, false)) {
        LOG4CXX_DEBUG(logger, myFuncName << ": query " << queryId << " found but not mine, return false");
        return false;
    }
    LOG4CXX_DEBUG(logger, myFuncName << ": query " << queryId << " not found, return true");
    return true;
}


std::string MpiManager::getInstallPath(const InstMembershipPtr& membership)
{
    InstanceID myId = Cluster::getInstance()->getLocalInstanceId();
    const InstanceDesc& inst = membership->getConfig(myId);
    SCIDB_ASSERT(inst.getInstanceId() == myId);

    const std::string& path = inst.getPath();
    if (!scidb::isFullyQualified(path)) {
        throw (USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_NON_FQ_PATH_ERROR) << path);
    }
    return path;
}

MpiLauncher* MpiManager::newMPILauncher(uint64_t launchId, const std::shared_ptr<scidb::Query>& q)
{
  if (_mpiType == mpi::MPICH2SCIDB ) {
    return new MpiLauncherMPICH12(launchId, q);
  } else if (_mpiType == mpi::MPICH_FLAVOR ) {
    return new MpiLauncherMPICH(launchId, q);
  } else if (_mpiType == mpi::OPENMPI_FLAVOR ) {
    return new MpiLauncherOMPI(launchId, q);
  }
  throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
         << "MpiManager::newMPILauncher");
  return NULL;
}

MpiLauncher* MpiManager::newMPILauncher(uint64_t launchId,
                                        const std::shared_ptr<scidb::Query>& q,
                                        uint32_t timeout)
{
  if (_mpiType == mpi::MPICH2SCIDB) {
    return new MpiLauncherMPICH12(launchId, q, timeout);
  } else if (_mpiType == mpi::MPICH_FLAVOR ) {
    return new MpiLauncherMPICH(launchId, q, timeout);
  } else if (_mpiType == mpi::OPENMPI_FLAVOR ) {
    return new MpiLauncherOMPI(launchId, q, timeout);
  }
  throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
         << "MpiManager::newMPILauncher");
  return NULL;
}

}
