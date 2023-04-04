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
 * @file Query.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of query context methods
 */

#include <query/Query.h>

#include <time.h>
#include <iostream>
#include <iomanip>


#include <log4cxx/logger.h>

#include <array/ArrayName.h>
#include <array/DBArray.h>
#include <array/ReplicationMgr.h>
#include <query/LogicalQueryPlan.h>
#include <query/Multiquery.h>
#include <query/PhysicalQueryPlan.h>
#include <query/RemoteArray.h>
#include <malloc.h>
#include <memory>
#include <monitor/MonitorCommunicator.h>
#include <monitor/QueryStats.h>
#include <network/MessageDesc.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>

#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <system/Warnings.h>

#include <rbac/NamespaceDesc.h>
#include <util/iqsort.h>
#include <util/LockManager.h>
#include <util/PerfTime.h>
#include <util/PerfTimeAggData.h>

namespace scidb
{

using namespace std;
using namespace arena;

// Query class implementation
Mutex Query::queriesMutex;
Query::Queries Query::_queries;
uint32_t Query::nextID = 0;
log4cxx::LoggerPtr replogger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr Query::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr UpdateErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr RemoveErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr BroadcastAbortErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");

boost::mt19937 Query::_rng;
std::atomic<uint64_t> Query::_numOutstandingQueries;

thread_local std::weak_ptr<Query> Query::_queryPerThread;

#ifdef COVERAGE
    extern "C" void __gcov_flush(void);
#endif

size_t Query::PendingRequests::increment()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_A);
    _nReqs += 1;
    return _nReqs;
}

bool Query::PendingRequests::decrement()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_B);
    if (--_nReqs == 0 && _sync) {
        _sync = false;
        return true;
    }
    return false;
}

bool Query::PendingRequests::test()
{
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_C);
    if (_nReqs != 0) {
        _sync = true;
        return false;
    }
    return true;
}

std::shared_ptr<Query>
Query::createFakeQuery(InstanceID coordID,
                       InstanceID localInstanceID,
                       const InstLivenessPtr& liveness,
                       int32_t *longErrorCode)
{
    std::shared_ptr<Query> query = std::make_shared<Query>(QueryID::getFakeQueryId());
    try {
        query->init(coordID, localInstanceID, liveness);
    }
    catch (const scidb::Exception& e) {
        if (longErrorCode != NULL) {
            *longErrorCode = e.getLongErrorCode();
        } else {
            destroyFakeQuery(query.get());
            e.raise();
        }
    }
    catch (const std::exception& e) {
        destroyFakeQuery(query.get());
        throw;
    }
    return query;
}

void Query::destroyFakeQuery(Query* q)
 {
     if (q!=NULL && q->getQueryID().isFake()) {
         try {
             arena::ScopedArenaTLS arenaTLS(q->getArena());
             q->handleAbort();
         } catch (scidb::Exception&) { }
     }
 }


Query::Query(const QueryID& queryID):
    _queryID(queryID),
    _livenessSubscriberID(0),
    _instanceID(INVALID_INSTANCE),
    _coordinatorID(INVALID_INSTANCE),
    _error(SYSTEM_EXCEPTION_SPTR(SCIDB_E_NO_ERROR, SCIDB_E_NO_ERROR)),
    _rights(new rbac::RightsMap()),
    _completionStatus(INIT),
    _commitState(UNKNOWN),
    _creationTime(time(NULL)),
    _useCounter(0),
    _doesExclusiveArrayAccess(false),
    _procGrid(NULL),
    _isAutoCommit(false),
    _perfTimeAggData(std::make_unique<PerfTimeAggData>()),
    _usecRealAtStart(perfTimeGetElapsedInMicroseconds()),
    _queryStats(this),
    _kind(Kind::NORMAL),
    _subqueryCount(0),
    _subqueryIndex(0),
    _multiquery(nullptr),
    _subquery(nullptr),
    fetch(true),
    semResults(),
    syncSG()
{
    ++_numOutstandingQueries;
}

Query::~Query()
{
    arena::ScopedArenaTLS arenaTLS(_arena);

    // Reset all data members that may have Value objects.
    // These objects were allocated from the query arena, and
    // must be deallocated when the query context is still in thread-local storage.
    logicalPlan.reset();
    _mergedArray.reset();
    _currentResultArray.reset();
    std::vector<std::shared_ptr<PhysicalPlan>>().swap(_physicalPlans);
    _operatorContext.reset();

    LOG4CXX_TRACE(_logger, "Query::~Query() " << _queryID << " "<<(void*)this);
    if (_arena) {
        LOG4CXX_DEBUG(_logger, "Query._arena:" << *_arena);
    }

    _perfTimeAggData->printLog(_usecRealAtStart, *this); // 这个可以打印query执行时间！！

    delete _procGrid ; _procGrid = NULL ;
    --_numOutstandingQueries;
}

void Query::init(InstanceID coordID,
                 InstanceID localInstanceID,
                 const InstLivenessPtr& liveness)
{
   assert(liveness);
   assert(localInstanceID != INVALID_INSTANCE);
   {
      ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_A);

      validate();

      assert( _queryID != INVALID_QUERY_ID);

   /* Install a special arena within the query that all local operator arenas
      should delagate to; we're now using a LeaArena here -  an adaptation of
      Doug Lea's design with a tunable set of bin sizes - because it not only
      supports recycling but also suballocates all of the blocks it hands out
      from large - currently 64 MiB - slabs that are given back to the system
      en masse no later than when the query completes;  the hope here is that
      this reduces the overall fragmentation of the system heap...*/
      {
          assert(_arena == 0);
          stringstream ss ;
          ss << "query "<<_queryID;
          _arena = newArena(Options(ss.str().c_str()).lea(arena::getArena(),64*MiB));
      }

      assert(!_coordinatorLiveness);
      _coordinatorLiveness = liveness;
      assert(_coordinatorLiveness);

      size_t nInstances = _coordinatorLiveness->getNumLive();
      if (nInstances <= 0) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);
      }
      assert(_liveInstances.size() == 0);
      _liveInstances.clear();
      _liveInstances.reserve(nInstances);

      const InstanceLiveness::LiveInstances& liveInstances =
         _coordinatorLiveness->getLiveInstances(); // 获取所有的存活ins
      assert(liveInstances.size() == nInstances);
      for ( InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
        iter != liveInstances.end(); ++iter) {
         _liveInstances.push_back((*iter).getInstanceId());
      }

      _physInstanceID = localInstanceID;
      _instanceID = mapPhysicalToLogical(localInstanceID);
      assert(_instanceID != INVALID_INSTANCE);
      assert(_instanceID < nInstances);

      _defaultArrResidency = //XXX TODO: use _liveInstances instead !
         std::make_shared<MapArrayResidency>(_liveInstances.begin(),
                                             _liveInstances.end()); // 用一个vector<instanID> 创建了一个MapArrayResidency，这个类里面也有一个vector，就是用的这个vector swap的，表示当前所有存活的ins
        SCIDB_ASSERT(_defaultArrResidency->size() == _liveInstances.size());
        SCIDB_ASSERT(_defaultArrResidency->size() > 0);

      if (coordID == INVALID_INSTANCE) {
         // *this is on the coordinator
         _coordinatorID = INVALID_INSTANCE;
         std::shared_ptr<Query::ErrorHandler> ptr(new BroadcastAbortErrorHandler());
         pushErrorHandler(ptr);
      } else {
         // *this is on a worker, get the ID of the coordinator
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nInstances);
      }

      _receiveSemaphores.resize(nInstances, Semaphore());
      _receiveMessages.resize(nInstances);
      chunkReqs.resize(nInstances);

      if (coordID == INVALID_INSTANCE) {
          // Coordinator finalizers.
          pushCompletionFinalizer([] (const auto& query) {
                                      Query::destroyFinalizer(query);
                                  });
          pushVotingFinalizer([] (const auto& query) {
                                  Query::broadcastCommitFinalizer(query);
                              });
      }
      else {
          // Worker finalizers.
          pushVotingFinalizer([] (const auto& query) {
                                  Query::destroyFinalizer(query);
                              });
      }

      _errorQueue = NetworkManager::getInstance()->createWorkQueue("QueryErrorWorkQueue");
      assert(_errorQueue);
      _errorQueue->start();
      _bufferReceiveQueue = NetworkManager::getInstance()->createWorkQueue("QueryBufferReceiveWorkQueue");
      assert(_bufferReceiveQueue);
      _bufferReceiveQueue->start();
      _sgQueue = NetworkManager::getInstance()->createWorkQueue("QueryOperatorWorkQueue");
      _sgQueue->stop();
      assert(_sgQueue);
      _replicationCtx = std::make_shared<ReplicationContext>(shared_from_this(),
                                                             nInstances);
      assert(_replicationCtx);

      {
          ScopedMutexLock prot(_execStage, PTW_SML_QUERY_ERROR_K);
          // Remember the workers active for this query, later removing them
          // from the _liveWorkers container as they report back for 2PC completion
          // (or disconnect/crash/fail).
          if (isCoordinator()) {
              LOG4CXX_TRACE(_logger, "The coordinator physical instance ID is " << _physInstanceID);
              Query::InstanceVisitor visitor =
                  [this] (const auto& query, auto physInstanceID) mutable {
                      SCIDB_ASSERT(errorMutex.isLockedByThisThread());
                      if (physInstanceID != _physInstanceID) {
                          LOG4CXX_TRACE(_logger, "Worker physical instance ID " << physInstanceID
                                        << " is a participant in query " << _queryID);
                          _liveWorkers.insert(physInstanceID);
                      }
                  };
              listLiveInstances(visitor);
          }
      }
   }

   // register for notifications
   std::shared_ptr<Query> self = shared_from_this();
   Notification<InstanceLiveness>::Subscriber listener =
       [self] (auto liveness) { self->handleLivenessNotification(liveness); };
   _livenessSubscriberID = Notification<InstanceLiveness>::subscribe(listener);

   LOG4CXX_DEBUG(_logger, "Initialized query (" << _queryID << ")");
}

void Query::broadcastCommitFinalizer(const std::shared_ptr<Query>& q)
{
    assert(q);
    if (q->wasCommitted()) {
        LOG4CXX_DEBUG(_logger, "Sending request-to-commit to worker instances for query"
                     << q->getQueryID());
        std::shared_ptr<MessageDesc>  msg(makeCommitMessage(q->getQueryID()));
        NetworkManager::getInstance()->broadcastPhysical(msg);
    }
}

std::shared_ptr<Query> Query::insert(const std::shared_ptr<Query>& query)
{
    assert(query);
    SCIDB_ASSERT(query->getQueryID().isValid());

    SCIDB_ASSERT(queriesMutex.isLockedByThisThread());

    pair<Queries::iterator,bool> res =
       _queries.insert( std::make_pair ( query->getQueryID(), query ) );

    if (res.second) {
        const uint32_t nRequests =
           std::max(Config::getInstance()->getOption<int>(CONFIG_REQUESTS),1);
        if (_queries.size() > nRequests) {
            _queries.erase(res.first);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_RESOURCE_BUSY)
                   << "too many queries");
        }
        assert(res.first->second == query);
        LOG4CXX_DEBUG(_logger, "Allocating query (" << query->getQueryID() << ")");
        LOG4CXX_DEBUG(_logger, "Number of allocated queries = " << _queries.size());

        SCIDB_ASSERT(Query::getQueryByID(query->getQueryID(), false) == query);
        return query;
    }
    return res.first->second;
}

QueryID Query::generateID()
{
   const QueryID queryID(Cluster::getInstance()->getLocalInstanceId(), getTimeInNanoSecs());
   LOG4CXX_DEBUG(_logger, "Generated queryID: instanceID="
                 << queryID.getCoordinatorId()
                 << ", time=" <<  queryID.getId()
                 << ", queryID=" << queryID);
   return queryID;
}

std::shared_ptr<Query> Query::create(QueryID queryID,
                                     InstanceID instanceId)
{
    SCIDB_ASSERT(queryID.isValid());

    std::shared_ptr<Query> query = std::make_shared<Query>(queryID);
    assert(query);
    assert(query->_queryID == queryID);

    std::shared_ptr<const scidb::InstanceLiveness> myLiveness =
       Cluster::getInstance()->getInstanceLiveness();
    assert(myLiveness);

    query->init(instanceId,
                Cluster::getInstance()->getLocalInstanceId(),
                myLiveness);
    bool isDuplicate = false;
    {

       ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY_QUERIES_A);
       isDuplicate = (insert(query) != query);
    }
    if (isDuplicate) {
        QueryID dup(query->_queryID);
        query->_queryID = QueryID::getFakeQueryId();
        destroyFakeQuery(query.get());
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_QUERY_ID)
            << dup;
    }
    return query;
}

void Query::start()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_B);
    checkNoError();
    if (_completionStatus == INIT) {
        _completionStatus = START;
    }
}

void Query::stop()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_C);
    checkNoError();
    if (_completionStatus == START) {
        _completionStatus = INIT;
    }
}

void Query::setAutoCommit()
{
    SCIDB_ASSERT(isCoordinator());
    _isAutoCommit = true;
}

void Query::pushErrorHandler(const std::shared_ptr<ErrorHandler>& eh)
{
    assert(eh);
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_D);
    checkNoError();
    _errorHandlers.push_back(eh);
}

void Query::pushVotingFinalizer(const Finalizer& f,
                                Query::Order order)
{
    SCIDB_ASSERT(f);
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_E);
    checkNoError();
    if (order == Query::Order::BACK) {
        _votingFinalizers.push_back(f);
    }
    else if (order == Query::Order::FRONT) {
        _votingFinalizers.push_front(f);
    }
    else {
        SCIDB_ASSERT(false);
    }
}

void Query::pushCompletionFinalizer(const Finalizer& f,
                                    Query::CompletionOrder order)
{
    SCIDB_ASSERT(f);
    SCIDB_ASSERT(isCoordinator());

    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_E);
    // Cannot 'checkNoError' here because a completion finalizer that
    // responds to the client could be added during exception handling.
    if (order == Query::CompletionOrder::DEFAULT) {
        _completionFinalizers.push_back(f);
    }
    else if (order == Query::CompletionOrder::DEFER) {
        if (_completionFinalizers.empty()) {
            _completionFinalizers.push_front(f);
        }
        else {
            // Basically, Query::destroyFinalizer is
            // always, always, always last to execute.
            auto front = _completionFinalizers.begin();
            _completionFinalizers.insert(++front, f);
        }
    }
    else {
        SCIDB_UNREACHABLE();
    }
}

void Query::done()
{
    bool isCommit = false;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_F);

        if (!_isAutoCommit &&
            SCIDB_E_NO_ERROR != _error->getLongErrorCode())
        {
            _completionStatus = ERROR;
            _error->raise();
        }
        _completionStatus = OK;
        if (_isAutoCommit && isCoordinator()) {
            // Initiate commit on coordinator, workers will
            // commit when they process the commit request broadcast.
            isCommit = true;
        }
    }
    if (isCommit) {
        handleComplete();
    }
}

bool Query::done(const std::shared_ptr<Exception>& unwindException)
{
    bool isAbort = false;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_G);
        if (SCIDB_E_NO_ERROR == _error->getLongErrorCode())
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
        _completionStatus = ERROR;
        isAbort = (_commitState != UNKNOWN);

        // The coordinator may not have started executing the multiquery parent.
        // Also, the multiquery is installed on every instance but it's not executed until
        // after all subqueries are, so it will definitely be in the UNKNOWN state
        // for most of its lifetime.  When a subquery aborts, it will abort the
        // multiquery and that has to happen regardless of its state.
        isAbort = isAbort || isMulti();

        LOG4CXX_DEBUG(_logger, "Query::done: queryID=" << _queryID
                      << ", _commitState=" << _commitState
                      << ", errorCode=" << _error->getLongErrorCode()
                      << ", isAbort=" << (isAbort ? "true" : "false")
                      << ", kind=" << _kind);
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (isAbort) {
        return handleAbort();
    }
    return false;
}

bool Query::doesExclusiveArrayAccess()
{
    return _doesExclusiveArrayAccess;
}

std::shared_ptr<LockDesc>
Query::requestLock(std::shared_ptr<LockDesc>& requestedLock)
{
    assert(requestedLock);
    assert(!requestedLock->isLocked());
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_H);

    if (requestedLock->getLockMode() > LockDesc::RD) {
        _doesExclusiveArrayAccess = true;
    }

    pair<QueryLocks::const_iterator, bool> res =
       _requestedLocks.insert(requestedLock);
    assert(requestedLock->getQueryId() == _queryID ||
           (_multiquery && (requestedLock->getQueryId() == getMultiqueryID())));
    if (res.second) {
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Requested lock: "
                      << (*res.first)->toString()
                      << " inserted");
        return requestedLock;
    }

    if ((*(res.first))->getLockMode() < requestedLock->getLockMode()) {
        _requestedLocks.erase(res.first);
        res = _requestedLocks.insert(requestedLock);
        assert(res.second);
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Promoted lock: " << (*res.first)->toString() << " inserted");
    }
    return (*(res.first));
}

void Query::addPhysicalPlan(std::shared_ptr<PhysicalPlan> physicalPlan)
{
    _physicalPlans.push_back(physicalPlan);
}

std::shared_ptr<PhysicalPlan> Query::getCurrentPhysicalPlan()
{
    return _physicalPlans.empty()
        ? std::shared_ptr<PhysicalPlan>()
        : _physicalPlans.back();
}

void Query::handleError(const std::shared_ptr<Exception>& unwindException)
{
    assert(arena::Arena::getArenaTLS() == _arena);
    assert(unwindException);
    assert(unwindException->getLongErrorCode() != SCIDB_E_NO_ERROR);
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_I);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
}

void Query::invokeFinalizers(Finalizers& finalizers)
{
    assert(arena::Arena::getArenaTLS() == _arena);

    assert(finalizers.empty() || checkFinalState());
    for (Finalizers::reverse_iterator riter = finalizers.rbegin();
         riter != finalizers.rend(); ++riter)
    {
        Finalizer& fin = *riter;
        if (!fin) {
           continue;
        }
        try {
           fin(shared_from_this());
        } catch (const std::exception& e) {
            std::string ewhat = e.what();
           LOG4CXX_FATAL(_logger, "Query (" << _queryID
                         << ") finalizer failed:"
                         << ewhat
                         << "Aborting!");
           abort();
        }
    }
}

void Query::executeCompletionFinalizers()
{
    SCIDB_ASSERT(arena::Arena::getArenaTLS() == _arena);
    SCIDB_ASSERT(isCoordinator());

    Finalizers completionFinalizers;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_K);
        bool installedQuery = false;
        {
            ScopedMutexLock cs(_execStage, PTW_SML_QUERY_ERROR_K);
            installedQuery = _installedQuery;
        }

        if (!installedQuery) {
            LOG4CXX_DEBUG(_logger, "Query " << getQueryID()
                          << " was not installed before invoking completion finalizers");
            if (SCIDB_E_NO_ERROR == _error->getLongErrorCode()) {
                LOG4CXX_DEBUG(_logger, "Query " << getQueryID()
                              << " did not have an error installed, most likely due to a "
                              "liveness change on one or more workers");
                // The query isn't installed everywhere, yet we're
                // executing completion finalizers, most likely because
                // of a liveness change on a worker immediately after
                // the query was installed, but before we received
                // notifications (during prepareQuery).  So, if there
                // isn't an error on the query object yet, put one there
                // to ensure that an exception is emitted from
                // prepareQuery, to the client, when Query::validate is
                // next called.
                _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED)
                          << getQueryID());
                _error->setQueryId(getQueryID());
                LOG4CXX_DEBUG(_logger, "Deferring completion finalizer execution for "
                              "query " << getQueryID() << " until the next call to validate");
                return;
            }
            else if (_commitState != ABORTED) {
                // The query isn't installed everywhere and there's an
                // error lodged on the query, but the caller of prepareQuery
                // hasn't gotten around to checking it yet, so we can't
                // finish completion finalizer execution.
                LOG4CXX_DEBUG(_logger, "Query not yet aborted, deferring completion finalizer "
                              "for query " << getQueryID() << " until the next call to validate");
                return;
            }
            // else the query wasn't installed, but there is an error lodged
            // on it and it's in the 'ABORTED' state, so proceed with
            // execution finalizers.
        }

        completionFinalizers.swap(_completionFinalizers);
    }

    LOG4CXX_DEBUG(_logger,
                  "Invoking (" << completionFinalizers.size()
                  << ") post-2PC completion finalizers for query "
                  << getQueryID());

    // The completion finalizers have already been swapped-out
    // of the query object by now.  We could assert that, but
    // that would require us taking the _errorMutex again.
    for (auto riter = completionFinalizers.rbegin();
         riter != completionFinalizers.rend(); ++riter) {
        Finalizer& fin = *riter;
        if (!fin) {
            continue;
        }
        try {
            fin(shared_from_this());
        } catch (const std::exception& e) {
            std::string ewhat = e.what();
            LOG4CXX_FATAL(_logger, "Query (" << _queryID
                          << ") 2PC completion finalizer failed:"
                          << ewhat
                          << "Aborting!");
            abort();
        }
    }
}

void Query::sentPhysPlan()
{
    // This should be called only once, when the physical plan
    // is broadcast to the workers.
    SCIDB_ASSERT(!_sentPhysPlan);

    _sentPhysPlan = true;
}

bool Query::hasSentPhysPlan() const
{
    return _sentPhysPlan;
}

void Query::installedQuery()
{
    // This should be called only once, when the query ID is
    // broadcast to all workers.
    ScopedMutexLock prot(_execStage, PTW_SML_QUERY_MUTEX_A);
    ASSERT_EXCEPTION(!_installedQuery,
                     "Query::installedQuery() called more than once for query "
                     << getQueryID());
    _installedQuery = true;
}

bool Query::checkFinalState()
{
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_J);

   if (!_votingFinalizers.empty()) {
       return false;
   }

   switch (_completionStatus) {
       case OK:
       case ERROR:
           return true;
       case INIT:
           return _error->getLongErrorCode() != SCIDB_E_NO_ERROR;
       case START:
           return false;
       default:
           SCIDB_ASSERT(false);
   }

   // dummy return
   return false;
}

void
Query::invokeErrorHandlers(std::deque<std::shared_ptr<ErrorHandler> >& errorHandlers)
{
    for (deque<std::shared_ptr<ErrorHandler> >::reverse_iterator riter = errorHandlers.rbegin();
         riter != errorHandlers.rend(); riter++) {
        std::shared_ptr<ErrorHandler>& eh = *riter;
        try {
            eh->handleError(shared_from_this());
        } catch (const std::exception& e) {
            LOG4CXX_FATAL(_logger, "Query (" << _queryID
                          << ") error handler failed:"
                          << e.what()
                          << "Aborting!");
            abort();
        }
    }
}

bool Query::handleAbort()
{
    assert(arena::Arena::getArenaTLS() == _arena);

    Finalizers finalizersOnStack;
    deque<std::shared_ptr<ErrorHandler> > errorHandlersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    bool alreadyAborted = false;
    size_t nWorkers = 0;
    bool hasInstalledQuery = false;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_K);

        if (_commitState == COMMITTED) {
            // We're likely here because the client disconnected before
            // the two-phase commit completion was... completed.  So
            // there's nothing to do because the query has committed.
            // Without this check, we can get into a state where abort
            // is called after a commit, simply because a client disconnect
            // is handled as an automatic abort.
            LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") already committed, nothing to abort");
            SCIDB_ASSERT(_votingFinalizers.empty());
            return false;
        }

        if (isMulti()) {
            auto subquery = getSubquery();

            // The subquery won't exist if *this is a fake query created
            // by _explain_physical() or _explain_logical().  The subquery
            // also won't exist if the query is a user-supplied query that
            // has a query failure during prepareQuery, as subqueries
            // are not derived until later in the multiquery lifetime.

            // If I'm the multiquery and I haven't yet aborted, then someone
            // is telling me to abort (likely via a 'cancel' or instance
            // failure).  Tell my subquery to abort if it's in-progress.  It'll
            // later tell me that it aborted as part of his abort broadcast handler
            // state machine (only on the coordinator, workers must continue)
            // which is great as it means that the subquery's
            // UpdateErrorHandlers will have executed and then
            // I can later come and execute my provisional array cleanup.
            // In general, this avoids maintaining some separate state
            // to avoid a cycle of the multiquery aborting the subquery which
            // aborts the multiquery.
            if (subquery
                && subquery->_commitState == UNKNOWN) {  // Not yet committed, not yet aborted.
                arena::ScopedArenaTLS arenaTLS(subquery->getArena());
                // Aborting subquery, not *this, so ignore subquery->handleAbort() return value.
                subquery->handleAbort();
                if (isCoordinator()) {
                    // On the coordinator, let the subquery notify the
                    // multiquery from the BroadcastAbortErrorHandler to
                    // avoid a potential cycle (as described above).
                    return false;
                }
                // Workers don't install a BroadcastAbortErrorHandler, so a
                // multiquery on the worker must continue to abort.
                SCIDB_ASSERT(!isCoordinator());
                LOG4CXX_DEBUG(_logger, "Multiquery " << _queryID
                              << " continuing with abort on worker.");
            }
        }

        if (isSub() && _commitState == COMMITTED) {
            // Occurs between subqueries, when the client disconnects or when the
            // user cancels the multiquery.
            auto multiquery = getMultiquery();
            arena::ScopedArenaTLS arenaTLS(multiquery->getArena());
            // Aborting subquery, not *this, so ignore subquery->handleAbort() return value.
            multiquery->handleAbort();
            return false;
        }

        // We get to this point in any one of the following cases:
        //   If the query is not committed, regardless of kind.
        //   isMulti() and not committed and the current subquery aborted or committed
        //   isSub() and not committed
        //   normal and not committed and not aborted

        LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is being aborted");

        if(_commitState == COMMITTED) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be aborted after commit."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        if(_isAutoCommit && _completionStatus == START) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be aborted when in autoCommit state."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        alreadyAborted = _commitState == ABORTED;
        _commitState = ABORTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            // If there's no error lodged with the query, then the only reason
            // we'd come through handleAbort() is if the user requested to cancel
            // the query (either via cancel() or a client disconnect).
            _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED)
                      << _queryID);
            _error->setQueryId(_queryID);
            msg = _error;
        }

        // If I'm the multiquery, then I'm in the START state because not
        // all of my sub-queries have executed yet.  A MULTI-type query doesn't go
        // from the START state to the OK (or ERROR) state until it finishes executing,
        // and it will not finish executing until all of its sub-queries have executed.
        // However, I need to abort because one of my sub-queries told me to abort,
        // and that abort needs to happen regardless of my current state (unless I'm
        // already committed, which will not happen).
        if (_completionStatus == START && !isMulti())
        {
            LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is still in progress");
            return false;
        }
        errorHandlersOnStack.swap(_errorHandlers);
        finalizersOnStack.swap(_votingFinalizers);

        {
            ScopedMutexLock prot(_execStage, PTW_SML_QUERY_ERROR_K);
            nWorkers = _liveWorkers.size();
            hasInstalledQuery = _installedQuery;
        }
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!errorHandlersOnStack.empty()) {
        LOG4CXX_ERROR(_logger, "Query (" << _queryID << ") error handlers ("
                     << errorHandlersOnStack.size() << ") are being executed");
        invokeErrorHandlers(errorHandlersOnStack);
        errorHandlersOnStack.clear();
    }
    else if (alreadyAborted) {
        LOG4CXX_DEBUG(_logger, "Query (" << _queryID <<
                      ") has no error handlers to run because it was already aborted");
    }
    LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") finalizers ("
                  << finalizersOnStack.size() << ") are being executed");
    invokeFinalizers(finalizersOnStack);
    if (isCoordinator()) {
        LOG4CXX_DEBUG(_logger, "Waiting for " << nWorkers
                      << " workers to finish the 2PC completion step for query "
                      << _queryID);
    }
    LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") abort complete on this instance");

    if (isCoordinator() && (!hasInstalledQuery || nWorkers == 0)) {
        // Either:
        //   1. The query wasn't installed on workers
        //  or
        //   2. The query was installed, but because the whole cluster's
        //      liveness wasn't available, this query is aborted.
        // Number 2 can happen just after a single instance reboots, because the
        // liveness hasn't settled-out.
        LOG4CXX_DEBUG(_logger, "Query (" << _queryID
                      << ") was aborted before the query was installed on "
                      "all workers, invoke 2PC completion finalizers now");
        executeCompletionFinalizers();
    }

    return true;
}

void Query::handleCommit()
{
    assert(arena::Arena::getArenaTLS() == _arena);

    Finalizers finalizersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    size_t nWorkers = 0;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_L);

        LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is being committed");

        if (_completionStatus != OK || _commitState == ABORTED) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "commit");
        }

        _errorHandlers.clear();

        _commitState = COMMITTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_ALREADY_COMMITED);
            (*static_cast<scidb::SystemException*>(_error.get())) << _queryID;
            _error->setQueryId(_queryID);
            msg = _error;
        }
        finalizersOnStack.swap(_votingFinalizers);
        nWorkers = getWorkersRemaining();
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    assert(_queryID != INVALID_QUERY_ID);
    invokeFinalizers(finalizersOnStack);
    if (isCoordinator()) {
        LOG4CXX_DEBUG(_logger, "Waiting for " << nWorkers
                      << " workers to finish the 2PC completion step for query "
                      << _queryID);
    }
}

void Query::handleComplete()
{
    assert(arena::Arena::getArenaTLS() == _arena);
    handleCommit();
#ifdef COVERAGE
    __gcov_flush();
#endif
}

void Query::handleCancel()
{
    assert(arena::Arena::getArenaTLS() == _arena);
    handleAbort();
}

void Query::handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& newLiveness)
{
    QueryID thisQueryId;
    InstanceID coordPhysId = INVALID_INSTANCE;
    std::shared_ptr<const scidb::Exception> msg;
    bool isAbort = false;  // May become true only on workers.  Always false on the coordinator.
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_M);

        assert(newLiveness->getVersion() >= _coordinatorLiveness->getVersion());

        if (newLiveness->getVersion() == _coordinatorLiveness->getVersion()) {
            assert(newLiveness->isEqual(*_coordinatorLiveness));
            return;
        }

        LOG4CXX_ERROR(_logger, "Query " << _queryID << " aborted on changed liveness, was "
                      << *_coordinatorLiveness << ", now " << *newLiveness);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_LIVENESS_CHANGED)
                << *_coordinatorLiveness << *newLiveness;
            _error->setQueryId(_queryID);
            msg = _error;
        }

        if (_coordinatorID != INVALID_INSTANCE) {
            // We're on a worker instance.
            coordPhysId = getPhysicalCoordinatorID();

            InstanceLiveness::InstancePtr newCoordState = newLiveness->find(coordPhysId);
            isAbort = newCoordState->isDead();
            if (!isAbort) {
                InstanceLiveness::InstancePtr oldCoordState = _coordinatorLiveness->find(coordPhysId);
                isAbort = (*newCoordState != *oldCoordState);
           }
        }
        // If the coordinator is dead (or newly restarted), we abort the query.
        // There is still a possibility that the coordinator actually has committed.
        // For read queries it does not matter.
        // For write queries UpdateErrorHandler::handleErrorOnWorker() will wait
        // (while holding its own array lock)
        // until the coordinator array lock is released and decide whether to really abort
        // based on the state of the catalog (i.e. if the new version is recorded).

        if (!_errorQueue) {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << _queryID << ")");
            isAbort = false;
        }
        thisQueryId = _queryID;
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!isAbort) {
        // Coordinator flow of execution always goes this way.
        return;
    }

    // Only workers make it this far.
    SCIDB_ASSERT(_coordinatorID != INVALID_INSTANCE);

    try {
        std::shared_ptr<MessageDesc> msg = makeAbortMessage(thisQueryId);

        // HACK (somewhat): set sourceid to coordinator, because only it can issue an abort
        assert(coordPhysId != INVALID_INSTANCE);
        msg->setSourceInstanceID(coordPhysId);

        std::shared_ptr<MessageHandleJob> job = std::make_shared<ServerMessageHandleJob>(msg);
        job->dispatch(NetworkManager::getInstance());

    } catch (const scidb::Exception& e) {
        LOG4CXX_WARN(_logger, "Failed to abort queryID=" << thisQueryId
                      << " on coordinator liveness change because: " << e.what());
        if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND) {
            SCIDB_ASSERT(false);
            e.raise();
        } else {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << thisQueryId << ")");
        }
    }
}

InstanceID Query::getPhysicalCoordinatorID(bool resolveLocalInstanceId)
{
    InstanceID coord = _coordinatorID;
    if (_coordinatorID == INVALID_INSTANCE) {
        if (!resolveLocalInstanceId) {
            return INVALID_INSTANCE;
        }
        coord = _instanceID;
    }
    assert(_liveInstances.size() > 0);
    assert(_liveInstances.size() > coord);
    return _liveInstances[coord];
}

InstanceID Query::mapLogicalToPhysical(InstanceID instance)
{
   if (instance == INVALID_INSTANCE) {
      return instance;
   }
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_N);  //XXX TODO: remove lock ?
   assert(_liveInstances.size() > 0);
   if (instance >= _liveInstances.size()) {
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instance;
   }
   checkNoError();
   instance = _liveInstances[instance]; // 物理id就是真实的id
   return instance;
}

InstanceID Query::mapPhysicalToLogical(InstanceID instanceID)
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_O); //XXX TODO: remove lock ?
    assert(_liveInstances.size() > 0);
    size_t index=0;
    bool found = bsearch(_liveInstances, instanceID, index);
    if (!found) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE)
            << Iid(instanceID);
    }
    return index; // 注意，逻辑id只是 _liveInstances这个vector里面的下标index，不是真正的存储在pg里面的ins id
}

bool Query::isPhysicalInstanceDead(InstanceID instance)
{
   ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_P);  //XXX TODO: remove lock ?
   checkNoError();
   InstanceLiveness::InstancePtr instEntry = _coordinatorLiveness->find(instance);
   // non-existent == dead
   return (instEntry==NULL || instEntry->isDead());
}

bool Query::isDistributionDegraded(const ArrayDesc& desc, size_t redundancy)
{
    // Arrays are allowed to exist on different instances.
    ArrayResPtr res = desc.getResidency();
    ArrayDistPtr dist = desc.getDistribution();

    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);
    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);

    const InstanceLiveness::LiveInstances& liveInstances =
       getCoordinatorLiveness()->getLiveInstances();

    size_t numLiveServers = 0;
    if (isDebug()) {
        // Count the total number of *live* servers.
        // A server is considered live if all of its instances are live
        // (i.e. not marked as dead in the query liveness set)
        ServerCounter scT;
        membership->visitInstances(scT);

        const InstanceLiveness::LiveInstances& deadInstances =
                getCoordinatorLiveness()->getDeadInstances();

        ServerCounter scD;
        for (InstanceLiveness::DeadInstances::const_iterator iter = deadInstances.begin();
             iter != deadInstances.end();
             ++iter) {
            const InstanceID iId = (*iter).getInstanceId();
            scD(iId);
        }
        SCIDB_ASSERT(scT.getCount() >= scD.getCount());
        // live = total - dead
        numLiveServers = scT.getCount() - scD.getCount();
    }

    std::set<InstanceID> liveSet; //XXX TODO: use _liveInstances instead !
    for (InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
         iter != liveInstances.end();
         ++iter) {
        const InstanceID iId = (*iter).getInstanceId();
        bool inserted = liveSet.insert(iId).second;
        SCIDB_ASSERT(inserted);
    }

    if (res->isEqual(liveSet.begin(),liveSet.end())) {
        // query liveness is the same as array residency
        return false;
    }

    const size_t residencySize = res->size();
    size_t numLiveInRes = 0;
    size_t numResidencyServers = 0;
    size_t numLiveServersInResidency = 0;
    {
        ServerCounter scR;
        ServerCounter scD;
        for (size_t i=0; i < residencySize; ++i) { //XXX TODO: can we make it O(n) ?
            const InstanceID id = res->getPhysicalInstanceAt(i);
            scR(id);
            const size_t nFound = liveSet.count(id);
            SCIDB_ASSERT(nFound<2);
            numLiveInRes += nFound;
            if (nFound==0) { scD(id); }
        }
        numResidencyServers = scR.getCount();
        numLiveServersInResidency = numResidencyServers - scD.getCount();
    }

    LOG4CXX_TRACE(_logger, "Query::isDistributionDegraded: "
                  << "residencySize=" << residencySize
                  << " numLiveInRes=" << numLiveInRes
                  << " numLiveServers(debug build only)=" << numLiveServers
                  << " numResidencyServers=" << numResidencyServers
                  << " numLiveServersInResidency=" << numLiveServersInResidency
                  << " queryID=" << _queryID );

    // The number of live servers in residency may be higher or lower
    // than the number of servers with dead instances because a particular
    // array residency may span an arbitrary set of instances
    // (and say not include the dead instances).
    // So, numLiveServers >=< numLiveServersInResidency

    if (numLiveInRes == residencySize) {
        // all instances in the residency are alive
        return false;
    }

    SCIDB_ASSERT(numLiveInRes < residencySize);

    if ((numLiveServersInResidency + redundancy) < numResidencyServers) {
        // not enough redundancy, too many servers are down
        LOG4CXX_WARN(_logger, "Query::isDistributionDegraded:"
                              << " numLiveServesInResidency " << numLiveServersInResidency
                              << " + redundancy " << redundancy
                              << " < numResidencyServers " << numResidencyServers);
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM)
            << "Not enough servers for redundancy";
    }

    return true;
}

bool Query::isForceCancelled()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_AK);
    SCIDB_ASSERT(_commitState == ABORTED);
    bool result = (_completionStatus == OK);
    return result;
}

bool Query::isDistributionDegradedForRead(const ArrayDesc& desc)
{
    const size_t redundancy = desc.getDistribution()->getRedundancy();
    return isDistributionDegraded(desc, redundancy);
}

bool Query::isDistributionDegradedForWrite(const ArrayDesc& desc)
{
    return isDistributionDegraded(desc, 0);
}

void Query::checkDistributionForRemove(const ArrayDesc& desc)
{
    // We need to make sure that the array residency was determined
    // strictly before the query membership. That will allow us to determine
    // if we have enough instances on-line to remove the array.
    InstMembershipPtr membership =
            Cluster::getInstance()->getInstanceMembership(MAX_MEMBERSHIP_ID);
    if (getCoordinatorLiveness()->getMembershipId() != membership->getId()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_LIVENESS_MISMATCH);
    }

    //XXX TODO: if the entire residency set is alive, it should be OK to remove as well
    if (getCoordinatorLiveness()->getNumDead()>0) {
        stringstream ss;
        ss << "Dead servers prevent array removal: " << *getCoordinatorLiveness();
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM)
            << ss.str();
    }
}

namespace
{
    class ResidencyConstructor
    {
    public:
        ResidencyConstructor(std::vector<InstanceID>& instances, bool isRandom=false)
        :  _instances(instances), _isRandom(isRandom)
        {
        }
        void operator() (const InstanceDesc& i)
        {
            if (isDebug()) {
                if (!_isRandom || Query::getRandom()%2 == 1) {
                    _instances.push_back(i.getInstanceId());
                }
            } else {
                // normal production case
                _instances.push_back(i.getInstanceId());
            }
        }
    private:
        std::vector<InstanceID>& _instances;
        bool _isRandom;
    };
}

ArrayResPtr Query::getDefaultArrayResidencyForWrite()
{
    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);

    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);
    SCIDB_ASSERT(membership->getNumInstances() == getCoordinatorLiveness()->getNumInstances());

    bool isRandomRes(false);
    if (isDebug()) {
        // for testing only
        isRandomRes = (Config::getInstance()->getOption<std::string>(CONFIG_PERTURB_ARR_RES) == "random");
    }

    ArrayResPtr res;
    if (!isRandomRes && membership->getNumInstances() == getCoordinatorLiveness()->getNumLive()) {
        SCIDB_ASSERT(getCoordinatorLiveness()->getNumDead()==0);
        return getDefaultArrayResidency();
    }

    std::vector<InstanceID> resInstances;
    resInstances.reserve(membership->getNumInstances());
    ResidencyConstructor rc(resInstances, isRandomRes);
    membership->visitInstances(rc);
    if (!isRandomRes) {
        SCIDB_ASSERT(resInstances.size() == membership->getNumInstances());
    } else if (resInstances.empty()) {
        resInstances.push_back(getPhysicalInstanceID());
    }
    res = std::make_shared<MapArrayResidency>(resInstances);

    return res;
}

ArrayResPtr Query::getDefaultArrayResidency()
{
    SCIDB_ASSERT(_defaultArrResidency);
    return _defaultArrResidency;
}

std::shared_ptr<Query> Query::getQueryByID(QueryID queryID, bool raise)
{
    std::shared_ptr<Query> query;

    // The active query map holds only valid query ids.
    if (queryID.isValid()) {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY_QUERIES_B);

        Queries::const_iterator q = _queries.find(queryID);
        if (q != _queries.end()) {
            return q->second;
        }
    }

    if (raise) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND)
            << queryID;
    }

    return {};
}

void Query::freeQueries(const Finalizer& onFreeQuery)
{
    Queries queries;
    {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY_QUERIES_C);
        queries.swap(_queries);
    }
    for (auto const& q : queries) {
        LOG4CXX_DEBUG(_logger, "Deallocating queries: (" << q.second->getQueryID() << ")");
        try {
            arena::ScopedArenaTLS arenaTLS(q.second->getArena());
            onFreeQuery(q.second);
            q.second->handleAbort();
        } catch (Exception&) {
        }
    }
}

void Query::freeQueriesWithWarning(const Warning& warn)
{
    freeQueries([&warn](const std::shared_ptr<Query>& q) {
        SCIDB_ASSERT(q);
        q->postWarning(warn);
    });
}

size_t Query::visitQueries(const Visitor& visit)
{
    size_t queriesSize = 0;
    {
        ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY_QUERIES_D);

        if (visit) {
            Query::Queries queries = _queries;
            for (auto& i : queries) {
                visit(i.second);
            }
        }
        queriesSize = _queries.size();
    }
    return queriesSize;
}

std::string Query::getCompletionStatusStr() const
{
    switch(_completionStatus)
    {
        case INIT:    return "0 - pending";
        case START:   return "1 - active";
        case OK:      return "2 - completed";
        case ERROR:   return "3 - errors";
    }

    std::stringstream ss;
    ss << "4 - unknown=" << _completionStatus;
    return ss.str();
}

QueryStats & Query::getStats(bool updateArena)
{
    if(updateArena)
    {
        // find a better place to do this
        _queryStats.setArenaInfo(_arena);
    }
    return _queryStats;
}


/**
 * @brief Collect @c OperatorContext smart pointers from a physical plan tree.
 *
 * @details The various OperatorContexts in the physical plan can hold
 * smart pointer references that should be cleaned up when the Query
 * is finalized in @c Query::destroy() below.  In particular, any
 * PullSGContext in the tree will hang on to intermediate input
 * arrays, which may be (or may contain) large materialized MemArrays.
 * Those ought to be released when the query is finalized.
 *
 * @p We walk the tree and collect the smart pointers, so that when
 * this @c OpContextReaper object goes out of scope, the smart pointer
 * references will go away.  As with other smart pointers cleared by
 * @c Query::destroy(), the pointers are gathered under @c errorMutex,
 * but actual destruction of pointed-at objects occurs at scope-exit,
 * when the lock is not held.  We don't want to hold the lock because
 * these destructors can be quite heavyweight---for example,
 * destroying a MemArray may involve discarding dirty buffers and
 * removing an on-disk DataStore.
 */
class OpContextReaper
    : public PhysicalQueryPlanNode::Visitor
{
    using OpCtxPtr = std::shared_ptr<OperatorContext>;
    vector<OpCtxPtr> _ptrs;
public:
    void operator()(PhysicalQueryPlanNode& node,  const PhysicalQueryPlanPath* path, size_t depth)
    {
        SCIDB_ASSERT(!path);
        SCIDB_ASSERT(node.getPhysicalOperator());
        PhysicalOperator& phyOp = *node.getPhysicalOperator();
        OpCtxPtr opCtx = phyOp.getOperatorContext();
        if (opCtx) {
            _ptrs.push_back(opCtx);
            phyOp.unsetOperatorContext();
        }
    }
};

void Query::destroy()
{
    freeQuery(getQueryID());

    // Any members explicitly destroyed in this method
    // should have an atomic query validating getter method
    // (and setter ?)
    std::shared_ptr<Array> resultArray;
    std::shared_ptr<RemoteMergedArray> mergedArray;
    std::shared_ptr<WorkQueue> bufferQueue;
    std::shared_ptr<WorkQueue> errQueue;
    std::shared_ptr<WorkQueue> opQueue;
    Continuation continuation;
    std::shared_ptr<ReplicationContext> replicationCtx;
    std::shared_ptr<OperatorContext> opCtx;
    OpContextReaper reaper;

    // Swap pointers into local copies under mutex.  Pointed-at
    // objects are destroyed on scope-exit.  (Destructors may involve
    // lots of processing and should not run with the lock held.)
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_Q);

        LOG4CXX_TRACE(_logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _bufferReceiveQueue.swap(bufferQueue);
        _errorQueue.swap(errQueue);
        _sgQueue.swap(opQueue);
        _replicationCtx.swap(replicationCtx);

        // One day there may be more than one physical plan, but at
        // this point there's just the current one (or none, if the
        // query failed before one could be generated).
        auto plan = getCurrentPhysicalPlan();
        if (plan) {
            plan->getRoot()->visitDepthFirstPostOrder(reaper);
        }

        // Unregister this query from liveness notifications
        if(_livenessSubscriberID) {
            Notification<InstanceLiveness>::unsubscribe(_livenessSubscriberID);
        }

        // The result array may also have references to this query
        _currentResultArray.swap(resultArray);

        _mergedArray.swap(mergedArray);
        _continuation.swap(continuation);
        _operatorContext.swap(opCtx);
    }
    if (bufferQueue) { bufferQueue->stop(); }
    if (errQueue)    { errQueue->stop(); }
    if (opQueue)     { opQueue->stop(); }

    scidb::monitor::Communicator::addQueryInfoToHistory(this);
}

void
BroadcastAbortErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    if (query->getQueryID().isFake()) {
        return;
    }
    if (!query->getQueryID().isValid()) {
        assert(false);
        return;
    }
    if (! query->isCoordinator()) {
        assert(false);
        return;
    }
    LOG4CXX_DEBUG(_logger, "Broadcast ABORT message to all instances for query "
                  << query->getQueryID());
    std::shared_ptr<MessageDesc> abortMessage = makeAbortMessage(query->getQueryID());
    // query may not have the instance map, so broadcast to all
    NetworkManager::getInstance()->broadcastPhysical(abortMessage);

    mst::forwardError(query);
}

void Query::freeQuery(const QueryID& queryID)
{
    ScopedMutexLock mutexLock(queriesMutex, PTW_SML_QUERY_QUERIES_E);
    Queries::iterator i = _queries.find(queryID);
    if (i != _queries.end()) {
        std::shared_ptr<Query>& q = i->second;
        LOG4CXX_DEBUG(_logger, "Deallocating query (" << q->getQueryID()
                      << ") with use_count=" << q.use_count());
        _queries.erase(i);
    }
}

bool Query::validate()
{
    bool isShutdown = NetworkManager::isShutdown();
    if (isShutdown) {
        arena::ScopedArenaTLS arenaTLS(_arena);
        handleAbort();
    }

    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_R);
    checkNoError();
    return true;
}

void Query::checkNoError() const
{
    SCIDB_ASSERT(const_cast<Mutex*>(&errorMutex)->isLockedByThisThread());

    if (_isAutoCommit && _commitState == COMMITTED &&
        _error->getLongErrorCode() == SCIDB_LE_QUERY_ALREADY_COMMITED) {
        SCIDB_ASSERT(!_currentResultArray);
        return;
    }

    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        // note: error code can be SCIDB_LE_QUERY_ALREADY_COMMITED
        //       because ParallelAccumulatorArray is started
        //       regardless of whether the client pulls the data
        //       (even in case of mutating queries).
        //       So, the client can request a commit before PAA is done.
        //       An exception will force the backgound PAA threads to exit.
        _error->raise();
    }
}

std::shared_ptr<Array> Query::getCurrentResultArray()
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_S);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !_currentResultArray),
                       "Auto-commit query cannot return data");
    return _currentResultArray;
}

void Query::setCurrentResultArray(const std::shared_ptr<Array>& array)
{
    ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_T);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !array),
                       "Auto-commit query cannot return data");
    _currentResultArray = array;
}

// TODO: change to PV (=0)and eliminate this implementation?
OperatorContext::~OperatorContext()
{
}

void Query::startSGQueue(std::shared_ptr<OperatorContext> const& opContext,
                         std::shared_ptr<JobQueue> const& jobQueue)
{
    SCIDB_ASSERT(opContext);
    SCIDB_ASSERT(_sgQueue);

    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY_ERROR_U);
    assert(validate());

    // NOTE: Allthough we want to stop using Query's _operatorContext,
    // it's still used in some cases (Distributed Sort?)
    // and for the merging of REMOTE_ARRAYS onto the coordinator
    _operatorContext = opContext;
    _sgQueue->start(jobQueue);
}

void Query::stopSGQueue()
{
    SCIDB_ASSERT(_operatorContext);
    SCIDB_ASSERT(_sgQueue);

    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY_ERROR_V);
    SCIDB_ASSERT(validate());

    if(false) { // TODO: we can't actually reset it here yet, it breaks sort()
        _operatorContext.reset();
    }

    if(false) { // TODO: we can't actually stop it ourselves yet.
        // we'd have to count how many times it was started
        _sgQueue->stop();
    }
}

void Query::postWarning(const Warning& warn)
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY_WARN_A);
    _warnings.push_back(warn);
}

std::vector<Warning> Query::getWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY_WARN_B);
    return _warnings;
}

void Query::clearWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTW_SML_QUERY_WARN_C);
    _warnings.clear();
}

void RemoveErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    std::function<bool()> work = std::bind(&RemoveErrorHandler::handleRemoveLock, _lock, true);
    Query::runRestartableWork<bool, Exception>(work);
}

bool RemoveErrorHandler::handleRemoveLock(const std::shared_ptr<LockDesc>& lock,
                                          bool forceLockCheck)
{
   assert(lock);
   assert(lock->getLockMode() == LockDesc::RM);
   SystemCatalog& sysCat = *SystemCatalog::getInstance();

   std::shared_ptr<LockDesc> coordLock;
   if (!forceLockCheck) {
      coordLock = lock;
   } else {
      coordLock = sysCat.checkForCoordinatorLock(
            lock->getNamespaceName(), lock->getArrayName(), lock->getQueryId());
   }
   if (!coordLock) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock does not exist. No action for query "
                     << lock->getQueryId());
       return false;
   }

   if (coordLock->getArrayId() == 0 ) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock is not initialized. No action for query "
                     << lock->getQueryId());
       return false;
   }

   bool rc;
   if (coordLock->getArrayVersion() == 0)
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName());
       rc = sysCat.deleteArray(coordLock->getNamespaceName(),
                               coordLock->getArrayName());
   }
   else
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName()
            << " lock arrayVersion="  << coordLock->getArrayVersion());
       rc = sysCat.deleteArrayVersions(coordLock->getNamespaceName(),
                                       coordLock->getArrayName(),
                                       coordLock->getArrayVersion());
   }
   return rc;
}

void UpdateErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    std::function<void()> work = std::bind(&UpdateErrorHandler::_handleError, this, query);
    Query::runRestartableWork<void, Exception>(work);
}

void UpdateErrorHandler::_handleError(const std::shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(_logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getInstanceId() == Cluster::getInstance()->getLocalInstanceId());
   assert( (_lock->getLockMode() == LockDesc::CRT)
           || (_lock->getLockMode() == LockDesc::WR) );
   assert(mst::getLockingQueryID(query) == _lock->getQueryId());

   LOG4CXX_DEBUG(_logger,
                 "Update error handler is invoked for query ("
                 << query->getQueryID() << ")");

   if (_lock->getInstanceRole() == LockDesc::COORD) {
      handleErrorOnCoordinator(_lock);
   } else {
      assert(_lock->getInstanceRole() == LockDesc::WORKER);
      handleErrorOnWorker(_lock, query->isForceCancelled());
   }
}

void UpdateErrorHandler::releaseLock(const std::shared_ptr<LockDesc>& lock,
                                     const std::shared_ptr<Query>& query)
{
   assert(lock);
   assert(query);
   std::function<bool()> work = std::bind(&SystemCatalog::unlockArray,
                                          SystemCatalog::getInstance(),
                                          lock);
   bool rc = Query::runRestartableWork<bool, Exception>(work);
   if (!rc) {
      LOG4CXX_WARN(_logger, "Failed to release the lock for query ("
                   << query->getQueryID() << ")");
   }
}

static bool isTransientArray(const std::shared_ptr<LockDesc> & lock)
{
    return ( lock->getArrayId() > 0 &&
             lock->getArrayId() == lock->getArrayVersionId() &&
             lock->getArrayVersion() == 0 );
}


void UpdateErrorHandler::handleErrorOnCoordinator(const std::shared_ptr<LockDesc> & lock)
{
   assert(lock);
   assert(lock->getInstanceRole() == LockDesc::COORD);

   string const& namespaceName = lock->getNamespaceName();
   string const& arrayName = lock->getArrayName();

   std::shared_ptr<LockDesc> coordLock =
      SystemCatalog::getInstance()->checkForCoordinatorLock(
            namespaceName, arrayName, lock->getQueryId());
   if (!coordLock) {
      LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " coordinator lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return;
   }

   if (isTransientArray(coordLock)) {
       SCIDB_ASSERT(false);
       // no rollback for transient arrays
       return;
   }

   const ArrayID unversionedArrayId  = coordLock->getArrayId();
   const VersionID newVersion      = coordLock->getArrayVersion();
   const ArrayID newArrayVersionId = coordLock->getArrayVersionId();

   if (unversionedArrayId == 0) {
       SCIDB_ASSERT(newVersion == 0);
       SCIDB_ASSERT(newArrayVersionId == 0);
       // the query has not done much progress, nothing to rollback
       return;
   }

   ASSERT_EXCEPTION(newVersion > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newVersion<=0"));
   ASSERT_EXCEPTION(unversionedArrayId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent unversionedArrayId<=0"));
   ASSERT_EXCEPTION(newArrayVersionId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newArrayVersionId<=0"));

   const VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(unversionedArrayId);

   if (lastVersion == newVersion) {
       // we are done, the version is committed
       return;
   }
   SCIDB_ASSERT(lastVersion < newVersion);

   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                 " the new version "<< newVersion
                 <<" of array " << makeQualifiedArrayName(namespaceName, arrayName)
                 <<" (arrId="<< newArrayVersionId <<")"
                 <<" is being rolled back for query ("
                 << lock->getQueryId() << ")");

   doRollback(lastVersion, unversionedArrayId, newArrayVersionId);
}

void UpdateErrorHandler::handleErrorOnWorker(const std::shared_ptr<LockDesc>& lock,
                                             bool queryCancelled)
{
   assert(lock);
   assert(lock->getInstanceRole() == LockDesc::WORKER);

   string const& namespaceName  = lock->getNamespaceName();
   string const& arrayName      = lock->getArrayName();
   VersionID newVersion         = lock->getArrayVersion();
   ArrayID newArrayVersionId    = lock->getArrayVersionId();

   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                 << " queryCancelled = "<< queryCancelled
                 << " arrayName = "<< makeQualifiedArrayName(namespaceName, arrayName)
                 << " newVersion = "<< newVersion
                 << " newArrayVersionId = "<< newArrayVersionId);

   if (newVersion != 0) {

       // With 2PC completion, the coordinator lock will always be
       // present at this point.  The coordinator releases the lock once
       // all workers have responded during the 2PC completion step.
       // queryCancelled is always true for transaction recovery on startup,
       // (because we assume that a query was cancelled on instance (re)start)
       // otherwise it depends on the result of Query::isForceCancelled().
       // Previously, checking the coordinator lock ensured no race between a query finishing
       // normally and a cancellation of that same query (which could happen if
       // a query was completing as the user was killing iquery with CTRL+C,
       // for example).  But that assumed that the cancel could happen locally
       // before it happened on the coordinator--which was the case before I
       // changed the implementation of cancel()--but now that cannot happen.
       // Of course a query failure on an instance will happen before the
       // coordinator finds out as the instance must notify the coordinator,
       // but that's another case where the coordinator lock will be in place.

       ArrayID arrayId = lock->getArrayId();
       if(arrayId == 0) {
           LOG4CXX_WARN(_logger, "Invalid update lock for query ("
                        << lock->getQueryId()
                        << ") Lock:" << lock->toString()
                        << " No rollback is possible.");
       }
       VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);
       assert(lastVersion <= newVersion);

       // If there was a failure, then the query must be rolled back.  With
       // 2PC completion, the coordinator won't be able to finish, and its
       // lock will persist, until all worker instances have first completed and responded.
       // Something failed locally and it should not be possible that the coordinator
       // committed---we should definitely rollback.
       assert(queryCancelled || lastVersion < newVersion);

       if (lastVersion < newVersion && newArrayVersionId > 0) {

           LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                         " the new version "<< newVersion
                         <<" of array " << makeQualifiedArrayName(namespaceName, arrayName)
                         <<" (arrId="<< newArrayVersionId <<")"
                         <<" is being rolled back for query ("
                         << lock->getQueryId() << ")");

           doRollback(lastVersion, arrayId, newArrayVersionId);
       }
   }
   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " exit");
}

void UpdateErrorHandler::doRollback(VersionID lastVersion,
                                    ArrayID baseArrayId,
                                    ArrayID newArrayId)
{

    LOG4CXX_TRACE(_logger, "UpdateErrorHandler::doRollback:"
                  << " baseArrayId = "<< baseArrayId
                  << " newArrayId = "<< newArrayId);

   // if a query stopped before the coordinator recorded the new array
   // version id there is no rollback to do
   assert(newArrayId>0);
   assert(baseArrayId>0);

   try {
       DBArray::rollbackVersion(lastVersion,
                                baseArrayId,
                                newArrayId);
   } catch (const scidb::Exception& e) {
       LOG4CXX_ERROR(_logger, "UpdateErrorHandler::doRollback:"
                     << " baseArrayId = "<< baseArrayId
                     << " newArrayId = "<< newArrayId
                     << ". Error: "<< e.what());
       e.raise();
   }
}

void Query::registerPhysicalOperator(const std::weak_ptr<PhysicalOperator>& phyOpWeak)
{
    auto phyOp = phyOpWeak.lock();
    OperatorID opID = phyOp->getOperatorID();

    if(opID.isValid()) {
        if (_physicalOperators.size() < opID.getValue()+1) {
            _physicalOperators.resize(opID.getValue());  // default-insert missing entries
            _physicalOperators.push_back(phyOpWeak); // add the specified one
        } else {
            _physicalOperators[opID.getValue()] = phyOpWeak;
        }
        LOG4CXX_TRACE(_logger, "Query::registerPhysOp(): operatorID valid, no change");
    } else {
        _physicalOperators.push_back(std::weak_ptr<PhysicalOperator>(phyOpWeak));
        phyOp->setOperatorID(OperatorID(_physicalOperators.size()-1));
        LOG4CXX_TRACE(_logger, "Query::registerPhysOp(): (coordinator case) new operaterID generated");
    }
}

std::shared_ptr<PhysicalOperator> Query::getPhysicalOperatorByID(const OperatorID& operatorID) const
{
    SCIDB_ASSERT(_physicalOperators.size() > 0);

    SCIDB_ASSERT(operatorID.isValid());                           // by looking only at the number
    SCIDB_ASSERT(operatorID.getValue() < _physicalOperators.size()); // that its contained in the table

    auto weakPtr =  _physicalOperators[operatorID.getValue()];
    auto result = weakPtr.lock();

    SCIDB_ASSERT(result->getOperatorID() == operatorID); // matches the original

    return result;
}
void Query::releaseLocks(const std::shared_ptr<Query>& q)
{
    assert(q);
    LOG4CXX_DEBUG(_logger, "Releasing locks for query " << q->getQueryID());

    std::function<uint32_t()> work = std::bind(&SystemCatalog::deleteArrayLocks,
                                               SystemCatalog::getInstance(),
                                               Cluster::getInstance()->getLocalInstanceId(),
                                               q->getQueryID(),
                                               LockDesc::INVALID_ROLE);
    runRestartableWork<uint32_t, Exception>(work);
}

void Query::acquireLocks()
{
    QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_W);
        validate();
        if (!isSub()) {
            // Sub-queries don't acquire their own coordinator locks.  Those
            // locks are acquired by the multiquery during its query preparation step.
            // As such, the subquery doesn't need to release any locks.
            if (isCoordinator()) {
                // Pushed only on the coordinator as completion finalizers are not
                // run on any instance except for the coordinator instance.
                Query::Finalizer f = std::bind(&Query::releaseLocks, std::placeholders::_1);
                pushCompletionFinalizer(f, Query::CompletionOrder::DEFER);
            }
        }
        locks = _requestedLocks;
    }
    acquireLocksInternal(locks);
}

void Query::retryAcquireLocks()
{
    QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTW_SML_QUERY_ERROR_X);
        validate();
        locks = _requestedLocks;
    }

    // Either the query is DDL and optionally has no locks,
    // or the query is not DDL and must have locks.
    if (locks.empty() &&
        !logicalPlan->isDdl()) {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "Query::retryAcquireLocks";
    }

    if (!locks.empty()) {
        acquireLocksInternal(locks);
    }
}

void Query::acquireLocksInternal(QueryLocks& locks)
{
    LOG4CXX_TRACE(_logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    // If we don't have quorum and we're requesting anything other than
    // a read-only lock, then that's an error.
    if (_coordinatorLiveness->getNumDead() > 0) {
        for (const auto& lock : locks) {
            // TODO: if the array residency does not include the dead
            // instance(s), we will still fail but should not.
            if (lock->getLockMode() > LockDesc::RD) {
                LOG4CXX_ERROR(_logger, "query::acquireLocksInternal: can't acquire more than a readlock");
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM)
                    << "Dead instances, only RD locks are allowed";
            }
        }
    }

    try {
        // Subqueries never own any locks, only their multiquery does.
        if (!isSub()) {
            SystemCatalog::ErrorChecker errorChecker = std::bind(&Query::validate, this);
            SystemCatalog::getInstance()->lockArrays(locks, errorChecker);
        }

        validate();

        // Get the array metadata catalog version, i.e. 'timestamp' the arrays in use by this query.
        // This will populate LockDesc::_arrayCatalogId for each entry in locks, which before this
        // call should be zero in each instance.
        if (!locks.empty()) {
            SystemCatalog::getInstance()->getCurrentVersion(locks);
        }
    } catch (const scidb::LockBusyException& e) {
        e.raise();
    } catch (std::exception&) {
        releaseLocks(shared_from_this());
        throw;
    }
    if (_logger->isDebugEnabled()) {
        LOG4CXX_DEBUG(_logger, "Acquired "<< locks.size() << " array locks for query " << _queryID);
        for (auto & lock : locks)
        {
            LOG4CXX_DEBUG(_logger, "Acquired lock: " << lock->toString());
        }
    }
}

ArrayID
Query::getCatalogVersion(
    const std::string& namespaceName,
    const std::string& arrayName,
    bool allowMissing) const
{
    // Currently synchronization is not used because this is called
    // either strictly before or strictly after the query array lock
    // acquisition on the coordinator.  -- tigor
    assert(isCoordinator());

    if (_requestedLocks.empty() ) {
        // we have not acquired the locks yet
        return SystemCatalog::ANY_VERSION;
    }
    const std::string* unversionedNamePtr(&arrayName);
    std::string unversionedName;
    if (!isNameUnversioned(arrayName) ) {
        unversionedName = makeUnversionedName(arrayName);
        unversionedNamePtr = &unversionedName;
    }

    // Look up lock for this array by name, and return its catalogVersion.  It's OK to use
    // INVALID_MODE because this LockDesc object is only for lookup purposes, and the mode isn't
    // used by comparator.
    std::shared_ptr<LockDesc> key(
        std::make_shared<LockDesc>(
            namespaceName,
            (*unversionedNamePtr),
            getQueryID(),
            Cluster::getInstance()->getLocalInstanceId(),
            LockDesc::COORD,
            LockDesc::INVALID_MODE));

    QueryLocks::const_iterator iter = _requestedLocks.find(key);
    if (iter == _requestedLocks.end() && allowMissing) {
        return SystemCatalog::ANY_VERSION;
    }
    ASSERT_EXCEPTION(iter!=_requestedLocks.end(),
                     string("Query::getCatalogVersion: unlocked array: ")+arrayName);
    const std::shared_ptr<LockDesc>& lock = (*iter);
    assert(lock->isLocked());
    return lock->getArrayCatalogId();
}

ArrayID Query::getCatalogVersion(
    const std::string&      arrayName,
    bool                    allowMissing) const
{
    string ns, arr;
    splitQualifiedArrayName(arrayName, ns, arr);
    if (ns.empty()) {
        ns = getNamespaceName();
    }
    return getCatalogVersion(ns, arr, allowMissing);
}


uint64_t Query::getLockTimeoutNanoSec()
{
    static const uint64_t WAIT_LOCK_TIMEOUT_MSEC = 2000;
    const uint64_t msec = _rng()%WAIT_LOCK_TIMEOUT_MSEC + 1;
    const uint64_t nanosec = msec*1000000;
    return nanosec;
}

void Query::waitForSystemCatalogLock()
{
    Thread::nanoSleep(getLockTimeoutNanoSec());
}

void Query::setQueryPerThread(const std::shared_ptr<Query>& query)
{
    _queryPerThread = query;
}

std::shared_ptr<Query> Query::getQueryPerThread()
{
    return _queryPerThread.lock();
}

void  Query::resetQueryPerThread()
{
    _queryPerThread.reset();
}

PerfTimeAggData& Query::refPerfTimeAggData() const
{
    SCIDB_ASSERT(_perfTimeAggData.get());
    return *_perfTimeAggData;
}

uint64_t Query::getActiveTimeMicroseconds() const
{
    return _perfTimeAggData->getActiveRealTimeMicroseconds();
}

Query::Kind Query::getKind() const
{
    return _kind;
}

bool Query::isMulti() const
{
    return _kind == Query::Kind::MULTI;
}

bool Query::isSub() const
{
    return _kind == Query::Kind::SUB;
}

bool Query::isNormal() const
{
    return _kind == Query::Kind::NORMAL;
}

void Query::setKind(Kind kind)
{
    _kind = kind;
}

QueryID Query::getMultiqueryID() const
{
    SCIDB_ASSERT(_kind == Query::Kind::SUB);
    SCIDB_ASSERT(_multiquery);
    return _multiquery->getQueryID();
}

std::shared_ptr<Query> Query::spawn(size_t subqueryIndex)
{

    // A query is a multiquery only if that query's outer-most
    // operator is mquery() as detected during logical
    // plan creation.  Only multiqueries may be spawned into
    // one or more subqueries.
    SCIDB_ASSERT(isMulti());

    // There should be no subquery attached to this multiquery
    // at this time because this is either the first call to
    // spawn or it's a subsequent call with the subquery
    // cleared.
    SCIDB_ASSERT(!hasSubquery());

    ScopedMutexLock lock(queriesMutex, PTW_SML_QUERY_QUERIES_E);

    // Generate a new query ID for the subquery that is related
    // to the multiquery ID and which particular subquery this
    // is out of the number of subqueries for this multiquery.
    auto subqueryID = Query::generateID();
    auto subquery = Query::create(subqueryID);
    subquery->setKind(Query::Kind::SUB);
    subquery->queryString = queryString;
    subquery->setSubqueryIndex(subqueryIndex);
    subquery->_session = _session;
    subquery->_instanceID = _instanceID;
    subquery->_physInstanceID = _physInstanceID;
    subquery->_coordinatorID = _coordinatorID;
    subquery->_requestedLocks = _requestedLocks;
    Continuation emptyContinuation;
    subquery->swapContinuation(emptyContinuation);  // NO continuation in the subquery!
    subquery->setMultiquery(shared_from_this());
    setSubquery(subquery);
    auto insertedQuery = Query::insert(subquery);
    SCIDB_ASSERT(insertedQuery == subquery);  // assert that the query ID is not a duplicate

    return subquery;
}

void Query::setSubqueryCount(size_t subqueryCount)
{
    _subqueryCount = subqueryCount;
}

size_t Query::getSubqueryCount() const
{
    return _subqueryCount;
}

size_t Query::getSubqueryIndex() const
{
    return _subqueryIndex;
}

void Query::setSubqueryIndex(size_t subqueryIndex)
{
    _subqueryIndex = subqueryIndex;
}

std::shared_ptr<Query> Query::getMultiquery()
{
    SCIDB_ASSERT(_multiquery);
    return _multiquery;
}

void Query::setMultiquery(std::shared_ptr<Query> multiquery)
{
    SCIDB_ASSERT(multiquery);
    _multiquery = multiquery;
}

std::shared_ptr<Query> Query::getSubquery()
{
    return _subquery;
}

void Query::setSubquery(std::shared_ptr<Query> subquery)
{
    SCIDB_ASSERT(subquery);
    _subquery = subquery;
}

bool Query::hasSubquery() const
{
    return _subquery != nullptr;
}

void Query::clearSubquery()
{
    // The subquery's destroyFinalizer completion handler will
    // have deallocated the query, setting _subquery to nullptr
    // here should invoke the destructor and free the memory.
    _subquery.reset();
}

const ProcGrid* Query::getProcGrid() const
{
    // locking to ensure a single allocation
    // XXX TODO: consider always calling Query::getProcGrid() in MpiManager::checkAndSetCtx
    //           that should guarantee an atomic creation of _procGrid
    ScopedMutexLock lock(const_cast<Mutex&>(errorMutex), PTW_SML_QUERY_ERROR_Y);
    // logically const, but we made _procGrid mutable to allow the caching
    // NOTE: Tigor may wish to push this down into the MPI context when
    //       that code is further along.  But for now, Query is a fine object
    //       on which to cache the generated procGrid
    if (!_procGrid) {
        _procGrid = new ProcGrid(safe_static_cast<procNum_t>(getInstancesCount()));
    }
    return _procGrid;
}

void Query::listLiveInstances(InstanceVisitor& func)
{
    assert(func);
    ScopedMutexLock lock(errorMutex, PTW_SML_QUERY_ERROR_Z);

    for (vector<InstanceID>::const_iterator iter = _liveInstances.begin();
         iter != _liveInstances.end(); ++iter) {
        std::shared_ptr<Query> thisQuery(shared_from_this());
        func(thisQuery, (*iter));
    }
}

bool Query::markWorkerDone(InstanceID instance,
                           size_t& liveWorkersSize,
                           bool& hasInstalledQuery)
{
    // The message is sent by the workers via the NetworkManager::sendPhysical
    // interface which itself relies on NetworkManager::_selfInstanceID.  That
    // is set during instance startup from the physical instance ID.  Using
    // the physical instance ID here is fine because the set of instances
    // participating in a given query is not allowed to change during the
    // query's lifetime.

    liveWorkersSize = 0;
    bool removedElement = false;
    {
        ScopedMutexLock lock(_execStage, PTW_SML_QUERY_ERROR_Z);
        removedElement = (_liveWorkers.find(instance) != _liveWorkers.end());
        _liveWorkers.erase(instance);
        liveWorkersSize = _liveWorkers.size();
        hasInstalledQuery = _installedQuery;
    }
    return removedElement;
}

size_t Query::getWorkersRemaining() const
{
    size_t liveWorkersSize = 0;
    {
        ScopedMutexLock lock(_execStage, PTW_SML_QUERY_ERROR_Z);
        liveWorkersSize = _liveWorkers.size();
    }
    return liveWorkersSize;
}

void Query::attachSession(const std::shared_ptr<Session> &session)
{
    SCIDB_ASSERT(session);
    _session = session;
}

string Query::getNamespaceName() const
{
    if (_session) {
        const string& ns = _session->getNamespace().getName();
        if (!ns.empty()) {
            return ns;
        }
    }
    return rbac::PUBLIC_NS_NAME;
}

void Query::getNamespaceArrayNames(
    const std::string &         qualifiedName,
    std::string &               namespaceName,
    std::string &               arrayName) const
{
    namespaceName = getNamespaceName();
    scidb::splitQualifiedArrayName(qualifiedName, namespaceName, arrayName);
}



ReplicationContext::ReplicationContext(const std::shared_ptr<Query>& query, size_t nInstances)
: _query(query)
#ifndef NDEBUG // for debugging
,_chunkReplicasReqs(nInstances)
#endif
{
    // ReplicatonManager singleton is initialized at startup time
    if (_replicationMngr == NULL) {
        _replicationMngr = ReplicationManager::getInstance();
    }
}

ReplicationContext::QueueInfoPtr ReplicationContext::getQueueInfo(ArrayID id)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    QueueInfoPtr& qInfo = _inboundQueues[id];
    if (!qInfo) {
        int size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
        assert(size>0);
        size = (size<1) ? 4 : size+4; // allow some minimal extra space to tolerate mild overflows
        NetworkManager& netMgr = *NetworkManager::getInstance();
        qInfo = std::make_shared<QueueInfo>(netMgr.createWorkQueue("ReplicatonContextWorkQueue",
                                                                   1, static_cast<uint64_t>(size)));
        assert(!qInfo->getArray());
        assert(qInfo->getQueue());
        qInfo->getQueue()->stop();
    }
    assert(qInfo->getQueue());
    return qInfo;
}

void ReplicationContext::enableInboundQueue(ArrayID aId, const std::shared_ptr<Array>& array)
{
    assert(array);
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_D);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    qInfo->setArray(array);
    wq->start();
}

std::shared_ptr<scidb::WorkQueue> ReplicationContext::getInboundQueue(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_E);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    return wq;
}

std::shared_ptr<scidb::Array> ReplicationContext::getPersistentArray(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_F);
    QueueInfoPtr qInfo = getQueueInfo(aId);
    assert(qInfo);
    std::shared_ptr<scidb::Array> array = qInfo->getArray();
    assert(array);
    assert(qInfo->getQueue());
    return array;
}

void ReplicationContext::removeInboundQueue(ArrayID aId)
{
    // tigor:
    // Currently, we dont remove the queue until the query is destroyed.
    // The reason for this was that we did not have a sync point, and
    // each instance was not waiting for the INCOMING replication to finish.
    // But we now have a sync point here, to coordinate the storage manager
    // fluhes.  So we may be able to implement queue removal in the future.

    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    syncBarrier(0, query);
    syncBarrier(1, query);
}

namespace {
void generateReplicationItems(std::shared_ptr<MessageDesc>& msg,
                              ReplicationManager::ItemVector* replicaVec,
                              const std::shared_ptr<Query>& query,
                              InstanceID physInstanceId)
{
    if (physInstanceId == query->getPhysicalInstanceID()) {
        return;
    }
    std::shared_ptr<ReplicationManager::Item> item(new ReplicationManager::Item(physInstanceId, msg, query));
    replicaVec->push_back(item);
}
}

void ReplicationContext::replicationSync(ArrayID arrId)
{
    assert(arrId > 0);
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtChunkReplica);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_array_id(arrId);
    // tell remote instances that we are done replicating
    chunkRecord->set_eof(true);

    assert(_replicationMngr);
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    msg->setQueryID(query->getQueryID());

    ReplicationManager::ItemVector replicasVec;
    Query::InstanceVisitor f = std::bind(&generateReplicationItems,
                                         msg,
                                         &replicasVec,
                                         std::placeholders::_1,
                                         std::placeholders::_2);
    query->listLiveInstances(f);

    assert(replicasVec.size() == (query->getInstancesCount()-1));
    for (auto const& item : replicasVec) {
        _replicationMngr->send(item);
    }
    for (auto const& item : replicasVec) {
        _replicationMngr->wait(item);
        assert(item->isDone());
        ASSERT_EXCEPTION(!item->hasError(), "Error sending replica sync!");
    }

    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_G);
        qInfo = getQueueInfo(arrId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // wait for all to ack our eof
    Semaphore::ErrorChecker ec = std::bind(&Query::validate, query);
    qInfo->getSemaphore().enter(replicasVec.size(), ec, PTW_SEM_REP);
}

void ReplicationContext::replicationAck(InstanceID sourceId, ArrayID arrId)
{
    assert(arrId > 0);
    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_H);
        qInfo = getQueueInfo(arrId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // sourceId acked our eof
    qInfo->getSemaphore().release();
}

/// cached pointer to the ReplicationManager singeton
ReplicationManager*  ReplicationContext::_replicationMngr;

void ReplicationContext::enqueueInbound(ArrayID arrId, std::shared_ptr<Job>& job)
{
    assert(job);
    assert(arrId>0);
    assert(job->getQuery());
    ScopedMutexLock cs(_mutex, PTW_SML_QUERY_MUTEX_I);

    std::shared_ptr<WorkQueue> queryQ(getInboundQueue(arrId));

    if (Query::_logger->isTraceEnabled()) {
        std::shared_ptr<Query> query(job->getQuery());
        LOG4CXX_TRACE(Query::_logger, "ReplicationContext::enqueueInbound"
                      <<" job="<<job.get()
                      <<", queue="<<queryQ.get()
                      <<", arrId="<<arrId
                      << ", queryID="<<query->getQueryID());
    }
    assert(_replicationMngr);
    try {
        WorkQueue::WorkItem item = _replicationMngr->getInboundReplicationItem(job);
        queryQ->enqueue(item);
    } catch (const WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(Query::_logger, "ReplicationContext::enqueueInbound"
                      << ": Overflow exception from the message queue (" << queryQ.get()
                      << "): "<<e.what());
        std::shared_ptr<Query> query(job->getQuery());
        assert(query);
        assert(false);
        arena::ScopedArenaTLS arenaTLS(query->getArena());
        query->handleError(e.clone());
        e.raise();
    }
}

} // namespace
