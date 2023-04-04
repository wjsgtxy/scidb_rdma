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
 *  @file LogicalDelete.cpp
 *  @date January 19, 2017
 *  @author Donghui Zhang
 */

#include <query/LogicalOperator.h>

#include <array/ArrayName.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: delete().
 *
 * @par Synopsis:
 *   delete( arrayName, expression )
 *
 * @par Summary:
 *   The delete operator deletes the array cells for which the expression evaluates true.
 *
 * @par Input:
 *   - arrayName: the array from which selected cells are to be removed.
 *   - expression: an expression which takes an cell as input and evaluates to either True or False.
 *
 * @par Output array:
 *   - the same named array with selected cells removed.
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalDelete : public LogicalOperator
{
public:

    LogicalDelete(const string& logicalName, const string& alias)
      : LogicalOperator(logicalName, alias)
    {
        _properties.tile = false;
        _properties.updater = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 // Use setMustExist(false) to save a catalog query
                 // since we must query Postgres below anyway.
                 RE(PP(PLACEHOLDER_ARRAY_NAME).setMustExist(false)),
                 RE(PP(PLACEHOLDER_EXPRESSION, TID_BOOL))
              }
            )},
            {"_fetch", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))} // undocumented
        };
        return &argSpec;
    }

    /**
     * Request a lock for all arrays that will be accessed by this operator.
     * Calls requestLock with the write lock over the target array (array deleted from)
     * @param query the query context
     */
    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        SCIDB_ASSERT(_parameters.size() == 2);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayNameOrig = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(arrayNameOrig));

        string arrayName;
        string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

        ArrayDesc srcDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &srcDesc;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        const LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? LockDesc::XCL : LockDesc::WR;

        std::shared_ptr<LockDesc>  lock(
            make_shared<LockDesc>(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                lockMode));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::WR);

        // Need update rights on the namespace.
        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName, rbac::P_NS_UPDATE);
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(schemas.empty());
        SCIDB_ASSERT(_parameters.size() == 2);

        string const& objName =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(objName));

        string arrayName;
        string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        ArrayDesc dstDesc;
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = &dstDesc;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.catalogVersion = arrayId;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        // The array exists in the catalog, ensure that we have the latest
        // version of its schema as it can change from version-to-version.
        ArrayDesc latestVersionDesc =
            SystemCatalog::getInstance()->getLatestVersion(namespaceName,
                                                           arrayName,
                                                           arrayId);
        cloneAttributes(latestVersionDesc, dstDesc);

        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId());
        SCIDB_ASSERT(dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        return dstDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDelete, "delete")

}  // namespace scidb
