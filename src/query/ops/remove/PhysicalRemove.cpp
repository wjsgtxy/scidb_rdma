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
 * PhysicalRemove.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <array/DBArray.h>
#include <array/TransientCache.h>
#include <deque>
#include <query/PhysicalOperator.h>
#include <query/QueryProcessor.h>
#include <system/SystemCatalog.h>


using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_remove"));

class PhysicalRemove: public PhysicalOperator
{
public:
    PhysicalRemove(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        const std::string & arrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

       _lock = std::shared_ptr<LockDesc>(
            new LockDesc(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                LockDesc::RM));
       std::shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);

       SystemCatalog* catalog = SystemCatalog::getInstance();

       // From this point on _schema is used to describe the array to be removed rather than the output array
       // somewhat hacky ... but getOutputDistribution() and other optimizer manipulations should be done by now
       SystemCatalog::GetArrayDescArgs args;
       args.result = &_schema;
       args.nsName = namespaceName;
       args.arrayName = arrayName;
       args.catalogVersion = query->getCatalogVersion(namespaceName, arrayName);
       args.throwIfNotFound = true;
       args.ignoreOrphanAttrs = true; // don't care if UDTs are missing
       catalog->getArrayDesc(args);
       SCIDB_ASSERT(_schema.getName() == arrayName);
       SCIDB_ASSERT(_schema.getUAId() > 0);

       query->checkDistributionForRemove(_schema);

       // Until the lock is updated with UAID, the query can be rolled back.
       _lock->setArrayId(_schema.getUAId());
       query->setAutoCommit();
       bool rc = catalog->updateArrayLock(_lock);
       SCIDB_ASSERT(rc);
   }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        std::shared_ptr<Array> target = DBArray::createDBArray(_schema, query);

        getInjectedErrorListener().throwif(__LINE__, __FILE__);

        assert(_schema.getUAId() != 0);
        target->removeVersions(query, 0);
        transient::remove(_schema);
        return std::shared_ptr<Array>();
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
        bool rc = RemoveErrorHandler::handleRemoveLock(_lock, true);
        SCIDB_ASSERT(rc);
    }

private:

   std::shared_ptr<LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemove, "remove", "physicalRemove")

}  // namespace ops
