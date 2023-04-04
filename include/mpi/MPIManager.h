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
 * @file MpiManager.h
 *      MpiManager class provides a simple interface to MPI functionality.
 *      It assumes that the process is started as an MPI job and allows it clients to run
 *      MPI functions on its (single) internal thread.
 */

#ifndef MPIMANAGER_H_
#define MPIMANAGER_H_

#include <set>
#include <map>
#include <memory>
#include <boost/asio.hpp>

#include <util/Singleton.h>
#include <network/ClientMessageDescription.h>
#include <network/ClientContext.h>
#include <network/Scheduler.h>
#include <network/NetworkMessage.h>
#include <util/OnScopeExit.h>
#include <query/Query.h>
#include <util/Event.h>
#include <util/Mutex.h>
#include <util/shm/SharedMemoryIpc.h>
#include <mpi/MPILauncher.h>
#include <mpi/MPIUtils.h>
#include <system/Config.h>

namespace scidb
{
    class MpiSlaveProxy;

    /**
     * MPI-based operator context attached to the query object in order to
     * communicate with the slave process and perform the usual query cleanup activities.
     * @note The context does not allow for more than 2 launch/ids to be active at a time.
     *       This is to say that a MPI-based query is expected to interact with the slaves serially
     *       (i.e. to start and complete() the communication with a one slave before proceeding
     *       to communicate with the next).
     */
    class MpiOperatorContext : virtual public OperatorContext
    {
    public:
        /// The type of message returned by popMsg() when the slave disconnects
        class EofMessageDescription : public scidb::ClientMessageDescription
        {
        public:
            virtual InstanceID getSourceInstanceID() const  { return CLIENT_INSTANCE; }
            virtual MessagePtr getRecord()                  { return MessagePtr(); }
            virtual MessageID getMessageType() const        { return scidb::mtNone; }
            virtual boost::asio::const_buffer getBinary()   { return boost::asio::const_buffer(NULL, 0); }
            virtual std::shared_ptr<scidb::SharedBuffer> getMutableBinary() override { return std::shared_ptr<scidb::SharedBuffer>(); }
            virtual ~EofMessageDescription()                {}
            virtual QueryID getQueryId() const              { return INVALID_QUERY_ID; }
            virtual ClientContext::Ptr getClientContext()   { return ClientContext::Ptr(); }
        };

    public:

        /// Constructor
        MpiOperatorContext(const std::weak_ptr<scidb::Query>& query)
        : _query(query),
	_event(),
        _launchId(0),
        _lastLaunchIdInUse(0)
        {}

        /// Destructor
        virtual ~MpiOperatorContext() {}

        /// Get a launcher for a given launch ID
        std::shared_ptr<MpiLauncher> getLauncher(uint64_t launchId);

        /// Set a launcher for a given launch ID
        void setLauncher(const std::shared_ptr<MpiLauncher>& launcher)
        {
            setLauncherInternal(launcher->getLaunchId(), launcher);
        }

        /// for internal use
        void setLauncherInternal(uint64_t launchId, const std::shared_ptr<MpiLauncher>& launcher);

        /// Get a slave for a given launch ID
        std::shared_ptr<MpiSlaveProxy> getSlave(uint64_t launchId);

        /// Get the launch ID of the last slave (in which this instance participated)
        uint64_t getLastLaunchIdInUse() const
        {
            //XXX mutex ?
            return _lastLaunchIdInUse;
        }

        /// Set a slave proxy for a given launch ID
        void setSlave(const std::shared_ptr<MpiSlaveProxy>& slave);

        /// for internal use
        void setSlaveInternal(uint64_t launchId, const std::shared_ptr<MpiSlaveProxy>& slave);

        /// Get a shared memory IPC object for a given launch ID
        std::shared_ptr<SharedMemoryIpc> getSharedMemoryIpc(uint64_t launchId, const std::string& name);

        /// Set a shared memory IPC object for a given launch ID
        bool addSharedMemoryIpc(uint64_t launchId, const std::shared_ptr<SharedMemoryIpc>& ipc);

        typedef std::function<bool (uint64_t, MpiOperatorContext*)> LaunchErrorChecker;
        /**
         * Get the next message from the slave
         * @param launchId the launch ID corresponding to the slave
         * @return the next message; the client context referenced by the message does not change for the same launch ID
         */
        std::shared_ptr<scidb::ClientMessageDescription> popMsg(uint64_t launchId,
                                                                  LaunchErrorChecker& errChecker);
        /**
         * Add the next message from the slave
         * @param launchId the launch ID corresponding to the slave
         * @param the next message
         */
        void pushMsg(uint64_t launchId, const std::shared_ptr<scidb::ClientMessageDescription>& msg);

        /**
         * Must be called when the launch related state is no longer in use
         */
        size_t complete(uint64_t launchId)
        {
            ScopedMutexLock lock(_mutex, PTW_SML_MPI_OP_CONTEXT);
            return _launches.erase(launchId);
        }

        uint64_t getNextLaunchId()
        {
            ScopedMutexLock lock(_mutex, PTW_SML_MPI_OP_CONTEXT);
            return ++_launchId;
        }

    private:

        friend class MpiErrorHandler;

        typedef struct
        {
            struct lessShmIpc
            {
                bool operator() (const std::shared_ptr<SharedMemoryIpc>& l,
                                 const std::shared_ptr<SharedMemoryIpc>& r) const
                {
                    return (l->getName() < r->getName());
                }
            };
            typedef std::set< std::shared_ptr<SharedMemoryIpc>, lessShmIpc >  ShmIpcSet;

            std::shared_ptr<scidb::ClientMessageDescription> _msg;
            std::shared_ptr<MpiLauncher>     _launcher;
            ShmIpcSet                          _shmIpcs;
            std::shared_ptr<MpiSlaveProxy>   _slave;
        } LaunchInfo;

        typedef std::function<void (uint64_t, LaunchInfo*)> LaunchCleaner;
        typedef std::map<uint64_t, std::shared_ptr<LaunchInfo> > LaunchMap;

        void clear(LaunchCleaner& cleaner);

    private:

        bool checkForError(uint64_t launchId, LaunchErrorChecker& errChecker);

        LaunchMap::iterator getIter(uint64_t launchId, bool updateLastLaunchId=true);

    private:
        std::weak_ptr<scidb::Query> _query;
        LaunchMap _launches;
        scidb::Event _event;
        scidb::Mutex _mutex;
        uint64_t _launchId;
        uint64_t _lastLaunchIdInUse;
    };

    /**
     * A class containing the interfaces for handling messages sent/received to/from the MPI slave process
     */
    class MpiMessageHandler
    {
    public:
        MpiMessageHandler() {}
        virtual ~MpiMessageHandler() {}

        inline scidb::MessagePtr createMpiSlaveCommand(scidb::MessageID id);
        inline scidb::MessagePtr createMpiSlaveHandshake(scidb::MessageID id);
        /**
         * Handler for the handshake message coming from a slave
         * @param messageDescription HB message pointer
         */
        void handleMpiSlaveHandshake(const std::shared_ptr<scidb::MessageDescription>& messageDesc);

        inline scidb::MessagePtr createMpiSlaveResult(scidb::MessageID id);

        /**
         * Handler for the command result message coming from a slave
         * @param messageDescription HB message pointer
         */
        void handleMpiSlaveResult(const std::shared_ptr<scidb::MessageDescription>& messageDesc);

        /**
         * Handler invoked on client disconnect
         * @param launchId launch ID
         * @param query associated with this slave connection
         */
        static void handleMpiSlaveDisconnect(uint64_t launchId, const std::shared_ptr<scidb::Query>& query);

    private:

        template <class MessageType_tt, int MessageTypeId_tv>
        void handleMpiSlaveMessage(const std::shared_ptr<scidb::MessageDescription>& messageDesc);

        /// @throws scidb::Exception if message cannot be processed
        static void processMessage(uint64_t launchId,
                                   const std::shared_ptr<scidb::ClientMessageDescription>& cliMsg,
                                   const std::shared_ptr<scidb::Query>& query);

        MpiMessageHandler(const MpiMessageHandler&);
        MpiMessageHandler& operator=(const MpiMessageHandler&);
    };

    template <class MessageType_tt, int MessageTypeId_tv>
    void MpiMessageHandler::handleMpiSlaveMessage(const std::shared_ptr<scidb::MessageDescription>& messageDesc)
    {
        bool isExpectedMsg = (messageDesc->getMessageType() == MessageTypeId_tv);
        bool isClientMsg   = (messageDesc->getSourceInstanceID() == scidb::CLIENT_INSTANCE);

        std::shared_ptr<scidb::ClientMessageDescription> cliMsg =
            std::dynamic_pointer_cast<scidb::ClientMessageDescription>(messageDesc);
        if (!cliMsg) {
            assert(false);
            if (!isExpectedMsg) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
                       << messageDesc->getMessageType());
            }
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "Invalid message in MPI slave handler");
        }
        ClientContext::Ptr cliCtx = cliMsg->getClientContext();
        if(!cliCtx) {
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << "Client context is not set in MPI slave handler");
        }
        scidb::OnScopeExit clientCtxCleaner([&cliCtx] () {
                cliCtx->disconnect();
            });

        if (!isExpectedMsg) {
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE)
                   << messageDesc->getMessageType());
        }
        if (!isClientMsg) {
            assert(false);
            std::stringstream ss;
            ss << "Invalid source of message in MPI slave handler: "
               << messageDesc->getSourceInstanceID();
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << ss.str());
        }
        // so we have a client message.

        // but what is this check for?
        if (boost::asio::buffer_size(messageDesc->getBinary()) != 0) {
            std::stringstream ss;
            ss << "Invalid message content in MPI slave handler"
               << " boost::asio::buffer_size(messageDesc->getBinary()) "
               << boost::asio::buffer_size(messageDesc->getBinary());
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                   << ss.str());
        }

        scidb::QueryID queryId = cliMsg->getQueryId();

        std::shared_ptr<MessageType_tt> result =
            std::dynamic_pointer_cast<MessageType_tt>(cliMsg->getRecord());

        if (!result) {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_MESSAGE_FORMAT)
                   << MessageTypeId_tv);
        }
        const uint64_t launchId = result->launch_id();

        std::shared_ptr<scidb::Query> query = Query::getQueryByID(queryId, true);
        assert(query);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        try {
            processMessage(launchId, cliMsg, query);
            clientCtxCleaner.cancel();
        } catch (const scidb::Exception& e) {
            query->handleError(e.clone());
        }
    }

    /**
     * A singleton class that performs the initialization and tear down of the MPI related infrastructure
     */
    class MpiManager: public Singleton<MpiManager>
    {
    private:

        typedef std::map<scidb::QueryID, std::shared_ptr<MpiOperatorContext> >  ContextMap;
        ContextMap   _ctxMap;
        scidb::Mutex _mutex;
        scidb::Event _event;
        bool _isReady;
        // currently MPI queries are serialized
        // after _mpiResourceTimeout seconds waiting a blocked query will error out
        uint32_t _mpiResourceTimeout;
        std::shared_ptr<scidb::Scheduler> _cleanupScheduler;
	size_t _mpiType;
	std::string _mpiInstallDir;
	std::string _mpiDaemonBin;
	std::string _mpiLauncherBin;
        // time to wait until another scalapack/mpi query completes
        static const uint32_t MPI_RESOURCE_TIMEOUT_SEC=10;

        void initMpi();
        static void initiateCleanup();
        static void initMpiLinks(const std::string& installPath,
                                 const std::string& mpiPath,
                                 const std::string& pluginPath);
        static bool checkForError(scidb::QueryID queryId,
                                  double startTime,
                                  double timeout);
    public:
        /**
         * @param installPath this instance install/data path
         * @return MPI installation relative to installPath
         */
        static std::string getMpiDir(const std::string& installPath)
        {
            return installPath+"/"+mpi::MPI_DIR;
        }

        /**
         * @param installPath this instance install/data path
         * @return filename of the MPI launcher relative to installPath
         */
        std::string getLauncherBinFile(const std::string& installPath)
        {
	    return _mpiInstallDir + "/bin/" + _mpiLauncherBin;
        }

        /**
         * @param installPath this instance install/data path
         * @return filename of the MPI daemon process (e.g. orted) relative to installPath
         */
        std::string getDaemonBinFile(const std::string& installPath)
        {
	    return _mpiInstallDir + "/bin/" + _mpiDaemonBin;
        }

        /**
         * Check if a process with a given pid belongs to this cluster/instance
         * @param installPath this instance install/data path
         * @param pid of the process to check
         * @param queryId the current query ID or 0
         * @return true if procName is an MPI slave started by this SciDB instance
         *              or it is an MPI launcher/daemon started by this cluster and
         *              (does not belong to any existing query or does belong to the one with queryId);
         *         false otherwise
         * @note this method can produce false positives (for example because pids wrap around)
         * @todo XXX this method does not work correctly under OMPI
         *           (because we dont set the correct environment for launcher/daemon)
         */
        bool canRecognizeProc(const std::string& installPath,
			      const std::string& clusterUuid,
			      pid_t pid,
                              QueryID queryId);
    public:
        explicit MpiManager();
        virtual ~MpiManager() {}

        /**
         * Must be called before any threading/networking starts or at shared lib load time.
         * It sets up network message handlers and other pieces of infrastructure
         * to enable MPI based operators.
         */
        void init();

        /**
         * Clean up any stale MPI related state.
         * This includes shared memory files/objects, pid files, etc.
         */
        void cleanup();

        static std::string getInstallPath(const InstMembershipPtr& membership);

        /// @note Enforces only a single context at a time (i.e. serializes queries which call this method).
        std::shared_ptr<MpiOperatorContext> checkAndSetCtx(const std::shared_ptr<scidb::Query>& query,
                                                             const std::shared_ptr<MpiOperatorContext>& ctx);
        std::shared_ptr<MpiOperatorContext> checkAndSetCtxAsync(const std::shared_ptr<scidb::Query>& query,
                                                                  const std::shared_ptr<MpiOperatorContext>& ctx);
        bool removeCtx(scidb::QueryID queryId);

	MpiLauncher* newMPILauncher(uint64_t launchId, const std::shared_ptr<scidb::Query>& q);
	MpiLauncher* newMPILauncher(uint64_t launchId, const std::shared_ptr<scidb::Query>& q, uint32_t timeout);

        /// for internal use
        void forceInitMpi();
    };

    /**
     * A class that can be used as a scidb::Query error handler and finalizer.
     * Using this error handler does not guarantee that all resources related
     * MPI slave processes (i.e. procs, shmIpc, pid files) are cleaned up in
     * case a query is aborted. This is a best effort cleanup which needs to
     * be complemented with a peridic cleanup based on the existing pid files.
     */
    class MpiErrorHandler: public Query::ErrorHandler
    {
        public:
        /**
         * Constructor
         * @param ctx MPI-based operator context to use
         * @param max number of launches performed on using the ctx
         */
        MpiErrorHandler(const std::shared_ptr<MpiOperatorContext>& ctx)
        : _ctx(ctx)
        {
            assert(ctx);
        }

        /// Destructor
        virtual ~MpiErrorHandler();

        /**
         * Clean up the MpiOperatorContext recorded internally
         * For all launches, it will try to
         * SharedMemoryIpc::remove, MpiSlaveProxy::destroy, and MpiLauncher::destroy(true)
         * ignoring the errors.
         * @param query current query context
         */
        virtual void handleError(const std::shared_ptr<Query>& query);

        /**
         * Clean up the MpiSlaveProxy (MpiSlaveProxy::destroy)
         * for the last launch using the operator context recorded internally
         * ignoring the errors.
         * @param query current query context
         */
        void finalize(const std::shared_ptr<Query>& query);

        /**
         * Perform the cleanup of all resources left behind
         * Those include the slave and launcher processes, pid files, shared memory objects
         */
        static void cleanAll();

        /**
         * Kill all processes left behind by the old process of this instance
         */
        static void killAllMpiProcs();

        /**
         * @param installPath this instance install/data path
         * @param clusterUuid SciDB cluster UUID
         * @param pid of the process to kill
         * @param queryId the current query ID or INVALID_QUERY_ID (default)
         * @return true if the process with pid is a valid SciDB process
         *         and ::kill() either succeeded or failed unexpectedly;
         *         otherwise false
         */
        static bool killProc(const std::string& installPath,
                             const std::string& clusterUuid,
                             pid_t pid,
                             QueryID queryId=INVALID_QUERY_ID);
        /**
         * Read the launcher pid (=pgrp) from the file and try to kill
         * launcher's process group. If any procs exist, the file is removed.
         * @param installPath this instance install/data path
         * @param clusterUuid SciDB cluster UUID
         * @param fileName of the MPI launcher
         */
        static void cleanupLauncherPidFile(const std::string& installPath,
                                           const std::string& clusterUuid,
                                           const std::string& fileName);
        /**
         * Read the slave pid & ppid from the file and try to kill
         * the corresponding procs. If the procs no longer exist, the file is removed.
         * @param installPath this instance install/data path
         * @param clusterUuid SciDB cluster UUID
         * @param fileName of the MPI slave
         * @param queryId the current query ID or INVALID_QUERY_ID (default)
         */
        static void cleanupSlavePidFile(const std::string& installPath,
                                        const std::string& clusterUuid,
                                        const std::string& fileName,
                                        QueryID queryId=INVALID_QUERY_ID);

        private:
        MpiErrorHandler(const MpiErrorHandler&);
        MpiErrorHandler& operator=(const MpiErrorHandler&);

        static void clean(scidb::QueryID queryId, uint64_t launchId,
                          MpiOperatorContext::LaunchInfo* info);
        std::shared_ptr<MpiOperatorContext> _ctx;
    };

} //namespace
#endif /* MPIMANAGER_H_ */
