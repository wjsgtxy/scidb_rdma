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

#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

#include <chrono>
#include <thread>

namespace scidb {

struct PhysicalLockArrays : PhysicalOperator
{
    PhysicalLockArrays(std::string const& logicalName,
                       std::string const& physicalName,
                       Parameters const& parameters,
                       ArrayDesc const& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

private:
    // If the query invoking lock_arrays is aborted by the user, then ensure that we
    // remove the global array lock.  User cancelations are handled by the query
    // abort logic.
    struct GlobalArrayLockRemover : Query::ErrorHandler
    {
        void handleError(const std::shared_ptr<Query>& query) override
        {
            PhysicalLockArrays::uninstallGlobalArrayLock(query);
        }
    };

    // @return true if lock_arrays(true), false if lock_arrays(false).
    bool lockingArrays() const
    {
        return paramToBool(_parameters[0]);
    }

    // Installs the global array lock in the catalog.
    static void installGlobalArrayLock(const std::shared_ptr<Query>& query)
    {
        auto globalLock = SystemCatalog::getInstance()->createGlobalArrayLock();
        SystemCatalog::ErrorChecker errorChecker = std::bind(&Query::validate, query.get());
        if (!SystemCatalog::getInstance()->lockArray(globalLock, errorChecker)) {
            // We could not acquire the global array lock because it already exists,
            // held by another user.
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                    << "global lock already held by another user";
        }
    }

    // Removes the global array lock from the catalog.
    static void uninstallGlobalArrayLock(const std::shared_ptr<Query>& query)
    {
        auto globalLock = SystemCatalog::getInstance()->createGlobalArrayLock();
        SystemCatalog::getInstance()->unlockArray(globalLock);
    }

    // The global array lock has been installed by this point, and it is
    // an exclusive lock; if there are other locks with lock_mode >= WR,
    // then we have to wait for them to finish.  No new non-read-only locks can be
    // added by anyone at this point, so the number of non-read-only locks
    // seen can only decrease over time.
    static void waitForHigherOrderLocks(const std::shared_ptr<Query>& query)
    {
        query->pushErrorHandler(std::make_shared<GlobalArrayLockRemover>());
        while (query->validate() &&
               SystemCatalog::getInstance()->countLocksAtLeast(LockDesc::WR) > 1) {
            // Not as good as getting a notification, yet it's basically what
            // MessageHandleJob (via WorkQueue) does and Query::runRestartableWork does.
            // We're interruptible via query->validate(), so I guess this is the way for now.
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void preSingleExecute(std::shared_ptr<Query> query) override
    {
        if (lockingArrays()) {
            // Install the global array lock.  If this method doesn't throw, then
            // from this point forward, no arrays may be created/removed/updated by
            // new queries.
            installGlobalArrayLock(query);

            // If there are any ongoing non-read array operations, then wait for
            // them to finish, unless the user aborts this query.
            waitForHigherOrderLocks(query);
        }
        else {
            // Remove the global lock.
            uninstallGlobalArrayLock(query);
        }
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(inputArrays.empty());
        return std::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLockArrays,
                                  "lock_arrays",
                                  "PhysicalLockArrays")

}  // namespace scidb
