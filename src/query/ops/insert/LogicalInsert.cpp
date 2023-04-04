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
 * @file LogicalInsert.cpp
 * @date Aug 30, 2012
 * @author poliocough@gmail.com
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
 * @brief The operator: insert().
 *
 * @par Synopsis:
 *   insert( sourceArray, targetArrayName )
 *
 * @par Summary:
 *   Inserts all data from left array into the persistent
 *   targetArray.  targetArray must exist with matching dimensions and
 *   attributes.  targetArray must also be mutable. The operator shall
 *   create a new version of targetArray that contains all data of the
 *   array that would have been received by merge(sourceArray,
 *   targetArrayName). In other words, new data is inserted between
 *   old data and overwrites any overlapping old values.  The
 *   resulting array is then returned.
 *
 * @par Input:
 *   - sourceArray the array or query that provides inserted data
 *   - targetArrayName: the name of the persistent array inserted into
 *
 * @par Output array:
 *   - the result of insertion
 *   - same schema as targetArray
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   Some might wonder - if this returns the same result as
 *   merge(sourceArray, targetArrayName), then why not use
 *   store(merge())? The answer is that
 *   1. this runs a lot faster - it does not perform a full scan of targetArray
 *   2. this also generates less chunk headers
 */
class LogicalInsert : public LogicalOperator
{
public:

    /**
     * Default conforming to the operator factory mechanism
     * @param[in] logicalName "insert"
     * @param[in] alias not used by this operator
     */
    LogicalInsert(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.updater = true;
        _properties.dataframe = true; // BUT only for isAppend mode, see below.
    }

    static PlistSpec const* makePlistSpec()
    {
        // Here we use setMustExist(false) to save a catalog query
        // from the Translator, since we must query the catalog below
        // anyway.

        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_ARRAY_NAME).setMustExist(false))
              })
            },

            // keywords
            { "_append", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            { "_fetch", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
        };
        return &argSpec;
    }


    /**
     * Request a lock for all arrays that will be accessed by this operator.
     * Calls requestLock with the write lock over the target array (array inserted into)
     * @param query the query context
     */
    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);
        SCIDB_ASSERT(_parameters.size() > 0);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayNameOrig = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(arrayNameOrig));

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

        ArrayDesc dstDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &dstDesc;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        const LockDesc::LockMode lockMode =
            dstDesc.isTransient() ? LockDesc::XCL : LockDesc::WR;

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

        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName, rbac::P_NS_UPDATE);
    }

    /**
     * Perform operator-specific checks of input and return the shape of the output. Currently,
     * the output array must exist.
     * @param schemas the shapes of the input arrays
     * @param query the query context
     */
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(schemas.size() == 1);
        SCIDB_ASSERT(_parameters.size() == 1);

        string const& objName =
			((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(objName));

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);

        ArrayDesc const& srcDesc = schemas[0];
        Parameter isAppend = findKeyword("_append");
        if (srcDesc.isDataframe() && !isAppend) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DATAFRAMES_NOT_SUPPORTED)
                << getLogicalName();
        }

        ArrayDesc dstDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &dstDesc;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.catalogVersion = arrayId;
        bool found = SystemCatalog::getInstance()->getArrayDesc(args);
        if (!found) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
        }

        // The array exists in the catalog, ensure that we have the latest
        // version of its schema as it can change from version-to-version.
        ArrayDesc latestVersionDesc =
            SystemCatalog::getInstance()->getLatestVersion(namespaceName,
                                                           arrayName,
                                                           arrayId);
        cloneAttributes(latestVersionDesc, dstDesc);

        if (dstDesc.isDataframe() && !isAppend) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DATAFRAMES_NOT_SUPPORTED)
                << getLogicalName();
        }

        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_OVERLAP |
                                   ArrayDesc::IGNORE_INTERVAL |  // allows auto-repart()
                                   ArrayDesc::SHORT_OK_IF_EBM);

        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId());
        SCIDB_ASSERT(dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        return dstDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInsert, "insert")

}  // namespace scidb
