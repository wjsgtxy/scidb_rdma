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
 * PhysicalRemoveVersions.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: sfridella
 */

#include <array/DBArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_remove"));

class PhysicalRemoveVersions: public PhysicalOperator
{
public:
    PhysicalRemoveVersions(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
       const string &arrayNameOrig =
           ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       VersionID targetVersion =
           ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[1])->getExpression()->evaluate().getInt64();

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

       _lock = std::shared_ptr<LockDesc>(
            new LockDesc(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                LockDesc::RM)
           );
       std::shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);

       // From this point on _schema is used to describe the array to be removed rather than the output array
       // somewhat hacky ... but getOutputDistribution() and other optimizer manipulations should be done by now

       SystemCatalog::GetArrayDescArgs args;
       args.result = &_schema;
       args.nsName = namespaceName;
       args.arrayName = arrayName;
       args.catalogVersion = query->getCatalogVersion(namespaceName, arrayName);
       args.versionId = targetVersion;
       args.throwIfNotFound = false;
       args.ignoreOrphanAttrs = true; // don't care if UDTs are missing
       bool found = SystemCatalog::getInstance()->getArrayDesc(args);
       if (!found) {
           // We know the array exists because Translator checked it,
           // so version must be missing.
           throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST)
               << arrayNameOrig;
       }

       SCIDB_ASSERT(_schema.getVersionId() == targetVersion);
       SCIDB_ASSERT(_schema.getUAId()>0);

       //XXX TODO: for now just check that all the instances in the residency are alive
       //XXX TODO: once we allow writes in a degraded mode, this call might have more semantics
       query->isDistributionDegradedForWrite(_schema);

       // Until the lock is updated with UAID, the query can be rolled back.
       _lock->setArrayId(_schema.getUAId());
       _lock->setArrayVersion(targetVersion);
       query->setAutoCommit();
       bool rc = SystemCatalog::getInstance()->updateArrayLock(_lock);
       SCIDB_ASSERT(rc);
   }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        std::shared_ptr<Array> target = DBArray::createDBArray(_schema, query);

        getInjectedErrorListener().throwif(__LINE__, __FILE__);

        /* Remove target versions from storage
         */
        target->removeVersions(query, _schema.getId());
        return std::shared_ptr<Array>();
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
       RemoveErrorHandler::handleRemoveLock(_lock, true);
    }

private:


   std::shared_ptr<LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemoveVersions, "remove_versions", "physicalRemoveVersions")

}  // namespace ops
