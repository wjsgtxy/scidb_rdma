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

/*
 * @file SciDBExecutor.cpp
 *
 * @author  roman.simakov@gmail.com
 *
 * @brief SciDB API internal implementation to coordinate query execution.
 *
 * This implementation is used by server side of remote protocol and
 * can be loaded directly to user process and transform it into scidb instance.
 * Maybe useful for debugging and embedding scidb into users applications.
 */

#include <query/executor/SciDBExecutor.h>

#include <memory>
#include <string>
#include <log4cxx/logger.h>

#include <SciDBAPI.h>
#include <network/Connection.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <network/MessageHandleJob.h>
#include <network/OrderedBcast.h>
#include <query/Multiquery.h>
#include <query/Query.h>
#include <query/QueryProcessor.h>
#include <query/Serialize.h>
#include <query/optimizer/Optimizer.h>
#include <system/Cluster.h>
#include <system/Config.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Session.h>
#include <util/InjectedError.h>
#include <util/InjectedErrorCodes.h>

using namespace std;
using NsComm = scidb::namespaces::Communicator;

namespace scidb
{

class SessionProperties;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.executor"));

namespace syncUtils
{
        /// Validate query until timeout expires
        bool validateQueryWithTimeout(uint64_t startTime,
                                      uint64_t timeout,
                                      std::shared_ptr<Query>& query)
        {
            bool rc = query->validate();
            assert(rc);
            if (hasExpired(startTime, timeout)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY)
                        << "remote query processor not ready to execute, try again later";
            }
            return rc;
        }

        /// Notify the workers about the query
        void notify(std::shared_ptr<Query>& query)
        {
            ASSERT_EXCEPTION(query->isCoordinator(), "Only coordinator is supposed to call syncUtils::notify()");
            {
                LOG4CXX_DEBUG(logger, "Send message from coordinator for waiting instances in queryID: "
                              << query->getQueryID());

                std::shared_ptr<MessageDesc> msg = std::make_shared<Connection::ServerMessageDesc>();
                msg->initRecord(mtNotify);
                std::shared_ptr<scidb_msg::Liveness> record = msg->getRecord<scidb_msg::Liveness>();
                bool res = serializeLiveness(query->getCoordinatorLiveness(), record.get());
                SCIDB_ASSERT(res);
                msg->setQueryID(query->getQueryID());
                NetworkManager::getInstance()->broadcastLogical(msg);
            }
        }

        /// Wait for confirmations the workers about the query
        void wait(const std::shared_ptr<Query>& query, uint64_t timeoutNanoSec)
        {
            ASSERT_EXCEPTION(query->isCoordinator(), "Only coordinator is supposed to call syncUtils::wait()");
            {
                const size_t instancesCount = query->getInstancesCount() - 1;
                LOG4CXX_DEBUG(logger, "Waiting for notification in queryID from " << instancesCount << " instances"); // 等待所有的worker确认

                Semaphore::ErrorChecker errorChecker;
                if (timeoutNanoSec > 0) {
                    errorChecker = std::bind(&validateQueryWithTimeout,
                                             getTimeInNanoSecs(),
                                             timeoutNanoSec,
                                             query);
                } else {
                    errorChecker = std::bind(&Query::validate, query);
                }
                query->semResults.enter(instancesCount, errorChecker, PTW_SEM_RESULTS_QP);
            }
        }

        void waitForInstall(const std::shared_ptr<Query>& query)
        {
            ASSERT_EXCEPTION(query->isCoordinator(),
                             "Only coordinator is supposed to call syncUtils::waitForInstall()");

            // At this point, a liveness change could come in and cause an abort because liveness
            // messages will propagate as soon as the query is installed on a worker.  In that case,
            // the query has workers installed but will be treated as though it doesn't.  So
            // multiple invocations to 2PC finalizers could happen before we get to set the
            // _installedQuery bit.  That is fine, because they'll be executed only once.
            // We can't set this in the _execStage critical section because the liveness
            // notifications will cause a deadlock if we're waiting here for the same instance
            // that also caused the liveness notification to fire.

            const uint64_t FOREVER = 0;
            syncUtils::wait(query, FOREVER);

            // The query may be considered installed on all workers only after we've heard
            // back from all of them.  Until then, it may not be installed anywhere or may
            // only be installed on some subset of the total workers for this query.  If
            // any liveness changing event happens after syncUtils::wait and before we
            // set the installed flag here, then the query will be treated as not installed
            // anywhere.  Again, as above, that is fine because at this point the only
            // completion finalizer available is to destroy the query and that will be
            // executed only once, should the criteria in Query::executeCompletionFinalizers
            // be met.
            query->installedQuery();
        }
}

/**
 * Engine implementation of the SciDBAPI interface
 */
class SciDBExecutor : public scidb::SciDBServer
                    , public InjectedErrorListener
{
    public:

    SciDBExecutor()
    :
        InjectedErrorListener(InjectErrCode::QUERY_BROADCAST)
    {
    }
    virtual ~SciDBExecutor()
    {
        // no need to call InjectedErrorListener::stop();
    }

    void* connect(SessionProperties const& sessionProperties,
                  const std::string& connectionString,
                  uint16_t port) override
    {
        ASSERT_EXCEPTION_FALSE(
            "connect - not needed, to implement in engine");

        // Shutting down warning
        return NULL;
    }

    void disconnect(void* connection = NULL)
    {
        ASSERT_EXCEPTION(
            false,
            "disconnect - not needed, to implement in engine");
    }

    void fillUsedPlugins(const ArrayDesc& desc, vector<string>& plugins)
    {
        for (const auto& attr : desc.getAttributes()) {
            const string& libName = TypeLibrary::getTypeLibraries().getObjectLibrary(attr.getType());
            if (libName != "scidb")
                plugins.push_back(libName);
        }
    }

    void prepareQuery(const std::string& queryString,
                      bool afl,
                      const std::string& programOptions,
                      QueryResult& queryResult,
                      void* connection)
    {
        ASSERT_EXCEPTION(connection, "NULL connection");

        // Chosen Query ID should *not* be already in use!
        if (Query::getQueryByID(queryResult.queryID, false)) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SciDBExecutor::prepareQuery";
        }

        // Query string must be of reasonable length!
        size_t querySize = queryString.size();
        size_t maxSize = Config::getInstance()->getOption<size_t>(CONFIG_QUERY_MAX_SIZE);
        if (querySize > maxSize) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_TOO_BIG) << querySize << maxSize;
        }

        std::shared_ptr<Connection> &scidb_connection =
            *reinterpret_cast<std::shared_ptr<Connection> *>(connection);
        ASSERT_EXCEPTION(scidb_connection, "NULL scidb_connection");
        ASSERT_EXCEPTION(scidb_connection->getSession(), "Connection has no session");

        // Create local query object, tie it to our session!
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        std::shared_ptr<Query> query = queryProcessor->createQuery(
            queryString,
            queryResult.queryID,
            scidb_connection->getSession());
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        ASSERT_EXCEPTION(
            queryResult.queryID == query->getQueryID(),
            "queryResult.queryID == query->getQueryID()");

        // register the query on the thread
        // so that the performance of everything after this can be tracked
        // its ugly that this can't be done by the caller
        // maybe the query should be created before its prepared
        // then we wouldn't have to assume the caller is the client thread (ugly)
        Query::setQueryPerThread(query);

        LOG4CXX_DEBUG(logger, "Parsing query(" << query->getQueryID() << "): "
            << " user_id=" << query->getSession()->getUser().getId()
            << " " << queryString << "");

        try {
            prepareQueryBeforeLocking(query, queryProcessor, afl, programOptions);
            query->acquireLocks(); //can throw "try-again", i.e. LockBusyException
            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);
        } catch (const scidb::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.clone());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
    }

    virtual void retryPrepareQuery(const std::string& queryString,
                                 bool afl,
                                 const std::string& programOptions,
                                   QueryResult& queryResult)
    {
        std::shared_ptr<Query>  query = Query::getQueryByID(queryResult.queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        assert(queryResult.queryID == query->getQueryID());
        try {

            query->retryAcquireLocks();  //can throw "try-again", i.e. LockBusyException

            std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);

        } catch (const scidb::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.clone());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
   }

    void prepareQueryBeforeLocking(std::shared_ptr<Query>& query,
                                   std::shared_ptr<QueryProcessor>& queryProcessor,
                                   bool afl,
                                   const std::string& programOptions)
    {
       query->validate();
       query->programOptions = programOptions;
       query->start();

       // Install the query on the workers
       syncUtils::notify(query);
       InjectedErrorListener::throwif(__LINE__, __FILE__);

       // first pass to collect the array names in the query
       queryProcessor->parseLogical(query, afl);
       LOG4CXX_TRACE(logger, "Query parseLogical 1 finished");

       // Collect lock requests and requests for access rights.
       queryProcessor->inferAccess(query);

       // Authorization: check inferred access rights.
       NsComm::checkAccess(query->getSession().get(), query->getRights());

       // Wait for the workers to install the query
       syncUtils::waitForInstall(query);
   }

    void prepareQueryAfterLocking(std::shared_ptr<Query>& query,
                                  std::shared_ptr<QueryProcessor>& queryProcessor,
                                  bool afl,
                                  QueryResult& queryResult)
    {
        query->validate();

        // second pass under the array locks
        queryProcessor->parseLogical(query, afl);
        LOG4CXX_TRACE(logger, "Query parseLogical 2 finished");

        // inheritance must be determined before here
        const ArrayDesc& desc = queryProcessor->inferTypes(query);
        LOG4CXX_TRACE(logger, "Query types are inferred");

        fillUsedPlugins(desc, queryResult.plugins);

        std::ostringstream planString;
        query->logicalPlan->toString(planString);   // TODO: possibly needed only when the operator is explain.
        // This is sent back to the client in postPrepareQuery, which means that the client
        // always has a cached copy of the explained logical plan.  PhysicalExplainLogical.cpp
        // does this same work yet returns its result as an array.
        queryResult.explainLogical = planString.str();

        queryResult.selective = query->logicalPlan->getRoot()->isSelective();
        queryResult.requiresExclusiveArrayAccess = query->doesExclusiveArrayAccess();

        query->stop();
        LOG4CXX_DEBUG(logger, "The query is prepared");
   }

    /**
     * Spawn and execute a subquery.
     *
     * Executed for multiqueries only.
     *
     * @param multiquery A pointer to the multiquery from which this will spawn and
     * execute a new subquery.
     * @param subqueryIndex An integer from zero to Query::_numberOfSubQueries that
     * indicates which subquery to create.
     */
    void executeSubquery(std::shared_ptr<Query> multiquery,
                         size_t subqueryIndex) override
    {
        SCIDB_ASSERT(multiquery->isCoordinator());
        SCIDB_ASSERT(multiquery->isMulti());
        SCIDB_ASSERT(subqueryIndex < multiquery->getSubqueryCount());

        std::shared_ptr<Query> subquery;
        subquery = multiquery->spawn(subqueryIndex);
        LOG4CXX_DEBUG(logger, "Spawned subquery " << subquery->getQueryID()
                      << " from query " << multiquery->getQueryID());
        arena::ScopedArenaTLS arenaTLS(subquery->getArena());

        // Install the new subquery on the workers and wait
        // for the workers to complete installation.
        syncUtils::notify(subquery);
        syncUtils::waitForInstall(subquery);

        std::shared_ptr<QueryProcessor> subqueryProc = QueryProcessor::create();
        subquery->logicalPlan = // Make the logical plan at subqueryIndex be this subquery's logical plan.
            std::make_shared<LogicalPlan>(multiquery->logicalPlan->getRoot()->getChildren()[subqueryIndex]);
        subqueryProc->inferAccess(subquery);  // populate subquery with latest lock versions
        subquery->acquireLocks();  // get the latest known array versions for the requested locks
        subqueryProc->inferTypes(subquery);  // re-run inferSchema with new array versions

        // Send the subquery physical plan to the workers.  The subquery doesn't stash
        // anything in the QueryResult object.
        std::string explainLogical;
        auto queryID = subquery->getQueryID();
        sendPhysPlan(subquery, queryID, explainLogical);
    }

    /// Create and optimize the physical query plan,
    /// broadcast the plan maintaining a global order
    /// to avoid deadlocks caused by thread starvation.
    void startExecuteQuery(const std::string& queryString,
                           bool afl,
                           QueryResult& queryResult)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        LOG4CXX_TRACE(logger, "startExecuteQuery: queryID=" << queryResult.queryID);

        // Executing query string
        std::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);

        // If I'm a multiquery, spawn a subquery and execute it.
        // Then, continue executing myself by sending the physical
        // plan for the multiquery to the workers.
        if (query->isMulti()) {
            executeSubquery(query, 0);
        }

        sendPhysPlan(query,
                     queryResult.queryID,
                     queryResult.explainLogical);
    }

    void sendPhysPlan(std::shared_ptr<Query> query,
                      QueryID queryID,
                      std::string& explainLogical)
    {
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        SCIDB_ASSERT(query->getQueryID() == queryID);
        SCIDB_ASSERT(query->getSession());

        if (!query->logicalPlan->getRoot()) {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_WAS_EXECUTED);
        }
        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        explainLogical = planString.str();

        const bool isDdl = query->logicalPlan->isDdl();
        LOG4CXX_DEBUG(logger, "The physical plan is detected as " << (isDdl ? "DDL" : "DML") );

        // Note: Optimization is done during execution, rather than prepare,
        //       for unknown reason.  [There was a (never-implemented)
        //       desire to re-optimize based on partial results, e.g. the
        //       cardinality of a subquery result might affect the plan for
        //       the remainder of query execution.  Search for "snippet
        //       execution" on the wiki. -mjl]
        std::shared_ptr<Optimizer> optimizer =  Optimizer::create();
        try
        {
            query->start(); // 修改query的状态为start

            queryProcessor->createPhysicalPlan(optimizer, query);
            if (logger->isDebugEnabled())
            {
                std::ostringstream planString;
                query->getCurrentPhysicalPlan()->toString(planString);
                LOG4CXX_DEBUG(logger, "Created physical plan:\n" << planString.str());
            }

            queryProcessor->optimize(optimizer, query, isDdl); // 优化物理查询计划
            LOG4CXX_TRACE(logger, "Physical plan optimized");

            // Execution of single part of physical plan
            queryProcessor->preSingleExecute(query); // 在master上执行一下预处理？

            {
                std::ostringstream planString;
                query->getCurrentPhysicalPlan()->toString(planString);
                query->explainPhysical += planString.str() + ";";
                LOG4CXX_DEBUG(logger, "The optimized physical plan is:\n" << planString.str());

                // Serialize physical plan and sending it out
                const string physicalPlan = serializePhysicalPlan(query->getCurrentPhysicalPlan());
                LOG4CXX_DEBUG(logger, "The serialized form of the physical plan: queryID="
                              << queryID << ", physicalPlan='" << physicalPlan << "'");
                auto preparePhysicalPlanMsg = std::make_shared<MessageDesc>(mtPreparePhysicalPlan); // 创建的mtPreparePhysicalPlan消息
                std::shared_ptr<scidb_msg::PhysicalPlan> preparePhysicalPlanRecord =
                        preparePhysicalPlanMsg->getRecord<scidb_msg::PhysicalPlan>();
                preparePhysicalPlanMsg->setQueryID(query->getQueryID());
                mst::updatePhysPlanMessage(query, preparePhysicalPlanRecord);
                preparePhysicalPlanRecord->set_physical_plan(physicalPlan);

                int priority = static_cast<MessageHandleJob*>(
                    Job::getCurrentJobPerThread().get())->getPriority();
                preparePhysicalPlanRecord->mutable_session_info()->set_job_priority(priority);
                LOG4CXX_DEBUG(logger, "priority is " << priority);

                preparePhysicalPlanRecord->mutable_session_info()->set_session_json(query->getSession()->toJson());

                Cluster* cluster = Cluster::getInstance();
                assert(cluster);
                preparePhysicalPlanRecord->set_cluster_uuid(cluster->getUuid());

                const bool isDeadlockPossible = false; //XXX TODO: enable this option in config.ini
                if (isDeadlockPossible) {
                    NetworkManager::getInstance()->broadcastLogical(preparePhysicalPlanMsg); // dz 将物理查询计划发送到各个子节点！
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                    LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                                  << query->getInstancesCount() - 1 << " instances"); // 等待从这么多个节点返回的物理查询计划收到确认
                } else {
                    // 广播了计划，但是发送的消息类型是mtOBcastRequest，我感觉是将上面的mtPreparePhysicalPlan封装到了mtOBcastRequest里面，封装在 payload_message 这个字段里面
                    // 对端收到了之后，解析了消息之后再产生一个对应的消息发送给自己，也就是send local。
                    OrderedBcastManager::getInstance()->broadcast(preparePhysicalPlanMsg);
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                    LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                            << query->getInstancesCount() - 1 << " instances"); // dz add ,之前这个分支是没有这个日志的，只有上面的分支有
                }
            }
            query->stop(); // 修改标志位 _completionStatus = init
            query->sentPhysPlan(); // 修改了query的标志位 _sentPhysPlan为true
        }
        catch (const std::bad_alloc& e)
        {
            std::shared_ptr<SystemException> se =
                    SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR);
            se << e.what();
            query->done(se);
            se->raise();
        }
        catch (const Exception& e)
        {
            query->done(e.clone());
            e.raise();
        }
    }

    /// Execute the pre-created physical plan
    bool completeExecuteQuery(QueryResult& queryResult,
                              const std::shared_ptr<Query>& query)
    {
        SCIDB_ASSERT(queryResult.queryID.isValid());
        SCIDB_ASSERT(query);
        SCIDB_ASSERT(query->isCoordinator());

        arena::ScopedArenaTLS arenaTLS(query->getArena());

        // Executing query string
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

        SCIDB_ASSERT(mst::getLockingQueryID(query) == queryResult.queryID);
        SCIDB_ASSERT(query->isSub() || !queryResult.explainLogical.empty());

        const bool isDeadlockPossible = false; //XXX TODO: enable this option in config.ini
        if (isDeadlockPossible) {
            LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                          << query->getInstancesCount() - 1 << " instances");
            // Make sure ALL instances are ready to run. If the coordinator does not hear
            // from the workers within a timeout, the query is aborted. This is done to prevent a deadlock
            // caused by thread starvation.
            // The deadlock is only possible if OrderedBcastManager is not used.
            // XXX TODO: In a long term we should probably solve the problem of thread starvation
            // XXX TODO: using for asynchronous execution techniques rather than global ordering.
            int deadlockTimeoutSec = Config::getInstance()->getOption<int>(CONFIG_DEADLOCK_TIMEOUT);
            if (deadlockTimeoutSec <= 0) {
                deadlockTimeoutSec = 10;
            }
            static const uint64_t NANOSEC_PER_SEC = 1000 * 1000 * 1000;
            syncUtils::wait(query, static_cast<uint64_t>(deadlockTimeoutSec)*NANOSEC_PER_SEC);
        }

        try {
            query->start();

            const size_t instanceCount = query->getInstancesCount();
            LOG4CXX_DEBUG(logger, "dz: ins count is: " << instanceCount);

            // Execution of local part of physical plan
            queryProcessor->execute(query);

            LOG4CXX_DEBUG(logger, "Query " << query->getQueryID()
                          << " executed locally, awaiting remote responses");

            // Wait for results from every instance except itself
            Semaphore::ErrorChecker ec = std::bind(&Query::validate, query);
            query->semResults.enter(instanceCount-1, ec, PTW_SEM_RESULTS_EX); // 信号量，等待？

            LOG4CXX_DEBUG(logger, "Query " << query->getQueryID()
                          << " remote responses received"); // dz 所有的节点都回复了？

            // Check error state
            query->validate();

            queryProcessor->postSingleExecute(query);

            query->done();
        }
        catch (const std::bad_alloc& e)
        {
            std::shared_ptr<SystemException> se =
                    SYSTEM_EXCEPTION_SPTR(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR);
            se << e.what();
            query->done(se);
            se->raise();
        }
        catch (const Exception& e)
        {
            query->done(e.clone());
            e.raise();
        }

        if (query->isSub()) {
            SCIDB_ASSERT(!query->isAutoCommit());
            return false;  // Tell the caller to reschedule the job for the next subquery.
        }

        queryResult.queryID = query->getQueryID();
        queryResult.explainPhysical = query->explainPhysical;
        queryResult.selective = query->getCurrentResultArray().get()!=nullptr;
        queryResult.autoCommit = query->isAutoCommit();
        if (queryResult.selective) {
            SCIDB_ASSERT(!queryResult.autoCommit);
            queryResult.array = query->getCurrentResultArray();
        }

        LOG4CXX_DEBUG(logger, "The result of query (autoCommit="
                      << queryResult.autoCommit
                      <<", selective="<<queryResult.selective
                      <<") is returned");
        if (queryResult.array) {
            LOG4CXX_DEBUG(logger, "[pipe]:\n" << queryResult.array->printPipe());
        }

        return true;  // Job complete!
    }

    void cancelQuery(QueryID queryID, void* connection)
    {
        LOG4CXX_DEBUG(logger, "Cancelling query " << queryID)
        std::shared_ptr<Query> query = Query::getQueryByID(queryID);
        arena::ScopedArenaTLS arenaTLS(query->getArena());

        query->handleCancel();
    }

    // deprecated in 16.6
    virtual void newClientStart(void* connection,
                                const SessionProperties& sessionProperties,
                                const std::string& userInfoFileName)
    {
        ASSERT_EXCEPTION_FALSE(
            "newClientStart - not needed, to implement in engine");
    }

    // deprecated in 16.6
    virtual void newClientStart(void* connection,
                                const std::string& userInfoFileName)
    {
        ASSERT_EXCEPTION_FALSE(
            "newClientStart - not needed, to implement in engine");
    }
} _sciDBExecutor;


SciDBServer& getSciDBExecutor()
{
   _sciDBExecutor.InjectedErrorListener::start();
    return _sciDBExecutor;
}

} // namespace scidb
