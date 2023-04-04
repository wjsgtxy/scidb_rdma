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

#include <query/Multiquery.h>

#include <array/ArrayDesc.h>
#include <array/DBArray.h>
#include <query/InstanceID.h>
#include <query/Query.h>
#include <storage/StorageMgr.h>
#include <system/LockDesc.h>
#include <system/SystemCatalog.h>

namespace scidb {

namespace mst {

void forwardError(const QueryPtr& subquery)
{
    SCIDB_ASSERT(subquery);
    SCIDB_ASSERT(subquery->isCoordinator());

    if (!subquery->isSub()) {
        return;
    }

    // Tell my multiquery that I'm aborting so that it
    // can abort, rollback any previous subquery provisional
    // arrays, and notify other instances of the abort.
    auto multiquery = subquery->getMultiquery();
    arena::ScopedArenaTLS arenaTLS(multiquery->getArena());
    SCIDB_ASSERT(multiquery->isCoordinator());
    multiquery->done(subquery->getError()->clone());
}

QueryPtr getActiveQuery(const QueryPtr& query)
{
    SCIDB_ASSERT(query);

    if (query->isMulti() &&
        query->hasSubquery()) {
        return query->getSubquery();
    }

    return query;
}

QueryID getLockingQueryID(const QueryPtr& query)
{
    SCIDB_ASSERT(query);
    QueryID queryID = query->isSub() ? query->getMultiqueryID() : query->getQueryID();
    return queryID;
}

static bool isLocalInstancePrimary(InstanceID instanceID)
{
    auto primaryInstanceID = Cluster::getInstance()->getPrimaryInstanceId();
    auto localInstanceID = Cluster::getInstance()->getLocalInstanceId();

    if (localInstanceID == INVALID_INSTANCE) {
        // This case occurs during startup.  The instance ID known to the network
        // manager is INVALID_INSTANCE until NetworkManager::run() is invoked, but
        // storage rollback happens before then.  The network manager sources the
        // instance ID from the storage manager, so we can confidently get it
        // from there ourselves.
        localInstanceID = StorageMgr::getInstance()->getInstanceId();
    }

    return localInstanceID == primaryInstanceID;
}

static void rollbackProvisionalArray(ArrayUAID uaid,
                                     ArrayID aid,
                                     VersionID vid,
                                     InstanceID instanceID)
{
    DBArray::rollbackVersion(vid, uaid, aid);
    SystemCatalog::getInstance()->removeProvisionalEntry(uaid, aid, vid, instanceID);
    if (isLocalInstancePrimary(instanceID)) {
        SystemCatalog::getInstance()->deleteArray(aid);
    }
}

struct MultiqueryErrorHandler : Query::ErrorHandler
{
    ArrayUAID _uaid;
    ArrayID _aid;
    VersionID _vid;
    InstanceID _instanceID;

    MultiqueryErrorHandler(ArrayUAID uaid,
                            ArrayID aid,
                            VersionID vid,
                            InstanceID instanceID)
        : _uaid(uaid)
        , _aid(aid)
        , _vid(vid)
        , _instanceID(instanceID)
    {
    }

    void handleError(const std::shared_ptr<Query>& query) override
    {
        SCIDB_ASSERT(query);
        SCIDB_ASSERT(query->isMulti());
        rollbackProvisionalArray(_uaid, _aid, _vid, _instanceID);
    }
};

void installFinalizers(const QueryPtr& subquery,
                       const LockPtr& lock)
{
    SCIDB_ASSERT(lock);
    SCIDB_ASSERT(subquery);

    if (!subquery->isSub()) {
        return;
    }

    // Remember the unversioned array ID, the versioned array ID,
    // the array's particular version (X in a@X) and this instance's ID.
    // These four pieces of information are critical for remembering the
    // provisional array in the case of a roll-back on abort or instance
    // failure.

    ArrayUAID uaid = lock->getArrayId();
    ArrayID aid = lock->getArrayVersionId();
    VersionID vid = lock->getArrayVersion();
    InstanceID instanceID = lock->getInstanceId();

    // This finalizer is installed on the subquery and records the
    // provisional array information.  It is only invoked when the
    // subquery itself commits (put another way, only during Query::handleCommit
    // and not during Query::handleAbort).
    auto provArrayBookkeeper =
        [uaid, aid, vid, instanceID] (const auto& subquery) {
            SCIDB_ASSERT(subquery);
            SCIDB_ASSERT(subquery->isSub());

            // If this subquery is the last subquery in the compound query
            // (e.g., if it is query C in mquery(A,B,C)), then it is possible
            // that this finalizer will run after the multiquery's commit finalizer
            // has begun due to the fact that SciDB doesn't support proper two-phase
            // commit notifications.  So, to avoid attempting to install a finalizer
            // that ultimately won't be needed as it's the last Sub and it's committing
            // successfully, acquire a lock on the multiquery.
            auto multiquery = subquery->getMultiquery();
            SCIDB_ASSERT(multiquery);
            ScopedMutexLock cs(multiquery->getErrorMutex(), PTW_SML_QUERY_ERROR_B);

            // If this query was not committed successfully, then this finalizer is
            // being run during Query::handleAbort() and shouldn't execute.  Why?
            // Because the multiquery already has all of the rollback error handlers
            // installed for previous subqueries and since we won't be recording
            // a provisional array version, we can just rely on our normal
            // UpdateErrorHandler to clean up the mess for us, just like any other
            // NORMAL query would.
            if (!subquery->wasCommitted()) {
                return;
            }

            // If the multiquery was already committed, then we know all of the
            // subqueries are done and since we're in this finalizer and we already
            // established that we're not running from Query::handleAbort(), we don't
            // need to record any provisional array information.
            if (multiquery->wasCommitted()) {
                return;
            }

            // We're a subquery and we need to record our provisional array to
            // the catalog so that the multiquery can roll it back should another later.
            SystemCatalog::getInstance()->recordProvisionalEntry(uaid,
                                                                 aid,
                                                                 vid,
                                                                 instanceID,
                                                                 subquery->getMultiqueryID());

            // If the multiquery commits, then remove the provisional array entry
            // from the catalog via this finalizer which will be installed on the
            // multiquery.
            arena::ScopedArenaTLS arenaTLS(multiquery->getArena());
            auto provCommitFinalizer =
                [uaid, aid, vid, instanceID] (const auto& multiquery) {
                    SCIDB_ASSERT(multiquery);
                    SCIDB_ASSERT(multiquery->isMulti());

                    // If the multiquery failed to commit, then don't remove the
                    // provisional array from the catalog; let the MultiqueryErrorHandler
                    // do that for us, as well as rollback the local instance array data.
                    if (!multiquery->wasCommitted()) {
                        return;
                    }

                    // Remove the provisional array from the provisional_arrays table
                    // because we successfully committed all of the subqueries as
                    // well as the multiquery and won't roll anything back.
                    SystemCatalog::getInstance()->removeProvisionalEntry(uaid, aid, vid, instanceID);
                };
            multiquery->pushVotingFinalizer(provCommitFinalizer);

            // If the multiquery doesn't commit (aborts, cancels or sub does the same), then be
            // sure to remove the provisional array entry from the catalog and roll back the
            // provisional array.
            auto errorHandler = std::make_shared<MultiqueryErrorHandler>(uaid, aid, vid, instanceID);
            multiquery->pushErrorHandler(errorHandler);
        };
    subquery->pushVotingFinalizer(provArrayBookkeeper);
}

void rollbackOnStartup(const InstanceID& instanceID)
{
    ProvArrays provArrs;
    SystemCatalog::getInstance()->readProvisionalEntries(instanceID, provArrs);
    for (const auto& entry : provArrs) {
        ArrayUAID uaid = std::get<0>(entry);
        ArrayID aid = std::get<1>(entry);
        VersionID vid = std::get<2>(entry);
        rollbackProvisionalArray(uaid, aid, vid, instanceID);
    }
}

void setupQueryOnWorker(const QueryPtr& query,
                        const PhysicalPlanRecordPtr& record)
{
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(record);

    auto recordKind = static_cast<Query::Kind>(record->query().query_kind());
    query->setKind(record->query().has_query_kind() ? recordKind : Query::Kind::NORMAL);

    if (!query->isSub()) {
        return;
    }

    ASSERT_EXCEPTION(record->query().has_multi_query_id(),
                     "Subquery missing multiquery ID");
    auto pQIDRec = record->query().multi_query_id();
    SCIDB_ASSERT(!pQIDRec.empty());
    auto qID = QueryID::fromString(pQIDRec);
    SCIDB_ASSERT(qID.isValid());
    auto multiquery = Query::getQueryByID(qID);
    SCIDB_ASSERT(multiquery);
    query->setMultiquery(multiquery);
    auto subqueryIndex = record->query().sub_query_index();
    query->setSubqueryIndex(subqueryIndex);
    multiquery->setSubquery(query);
}

void updatePhysPlanMessage(const QueryPtr& query,
                           const PhysicalPlanRecordPtr& record)
{
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(record);

    if (query->isNormal()) {
        return;
    }

    record->mutable_query()->set_query(query->queryString);
    record->mutable_query()->set_query_kind(query->getKind());
    record->mutable_query()->set_afl(true);  // required by proto but ignored by worker
    if (query->isSub()) {
        auto qID = query->getMultiqueryID().toString();
        record->mutable_query()->set_multi_query_id(qID);
        record->mutable_query()->set_sub_query_index(query->getSubqueryIndex());
    }
}

}  // namespace mst
}  // namespace scidb
