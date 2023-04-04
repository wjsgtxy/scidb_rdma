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

#include "LogicalAddAttributes.h"

#include <array/ArrayName.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <string>

namespace scidb {

LogicalAddAttributes::LogicalAddAttributes(const std::string& logicalName,
                                           const std::string& aliasName)
    : LogicalOperator(logicalName, aliasName)
{
    _properties.ddl = true;
    _properties.updater = true;
}

PlistSpec const* LogicalAddAttributes::makePlistSpec()
{
    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
              RE(PP(PLACEHOLDER_ARRAY_NAME)),
              RE(PP(PLACEHOLDER_SCHEMA))
          })
        }
    };
    return &argSpec;
}

void LogicalAddAttributes::inferAccess(const std::shared_ptr<Query>& query)
{
    LogicalOperator::inferAccess(query);
    SCIDB_ASSERT(_parameters.size() > 0);
    const auto& arrayNameOrig = ((std::shared_ptr<OperatorParamArrayReference>&)_parameters[0])->getObjectName();

    std::string arrayName;
    std::string namespaceName;
    query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

    SystemCatalog::GetArrayDescArgs args;
    ArrayDesc targetDesc;
    args.result = &targetDesc;
    args.nsName = namespaceName;
    args.arrayName = arrayName;
    args.throwIfNotFound = true;
    SystemCatalog::getInstance()->getArrayDesc(args);

    // I would save targetDesc to *this to reuse again during the call to inferSchema
    // but the engine destroys *this between now and constructs a new instance of
    // this logical operator before calling inferSchema.

    const LockDesc::LockMode lockMode =
        targetDesc.isTransient() ? LockDesc::XCL : LockDesc::WR;

    std::shared_ptr<LockDesc> lock(
        std::make_shared<LockDesc>(namespaceName,
                                   arrayName,
                                   query->getQueryID(),
                                   Cluster::getInstance()->getLocalInstanceId(),
                                   LockDesc::COORD,
                                   lockMode));
    std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
    SCIDB_ASSERT(resLock);
    SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::WR);

    // Need update rights on the namespace in the same manner as
    // the other 'update' operator descendants.
    query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName, rbac::P_NS_UPDATE);
}

ArrayDesc LogicalAddAttributes::inferSchema(std::vector<ArrayDesc> schemas,
                                            std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(schemas.empty());
    SCIDB_ASSERT(_parameters.size() == 2);

    const auto& plist = getParameters();
    const auto& arrayNameOrig = ((std::shared_ptr<OperatorParamArrayReference>&)plist[0])->getObjectName();

    std::string arrayName;
    std::string namespaceName;
    query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

    SystemCatalog::GetArrayDescArgs args;
    ArrayDesc targetDesc;
    args.result = &targetDesc;
    args.nsName = namespaceName;
    args.arrayName = arrayName;
    args.throwIfNotFound = true;
    SystemCatalog::getInstance()->getArrayDesc(args);

    SystemCatalog::GetArrayDescArgs latestArgs;
    ArrayDesc latestDesc;
    latestArgs.result = &latestDesc;
    latestArgs.nsName = namespaceName;
    latestArgs.arrayName = arrayName;
    latestArgs.throwIfNotFound = true;
    latestArgs.versionId = LAST_VERSION;
    SystemCatalog::getInstance()->getArrayDesc(latestArgs);

    // Verify that no existing attributes of the array are named again in
    // the new attributes that the user desires to add.  Verify that no
    // new attribute exists as an alias on the array already.
    // Take the existing schema and create a new one with the new
    // attributes appended.
    const auto& existingAttrs = latestDesc.getAttributes(true);
    Attributes expanded = existingAttrs;
    auto partialSchema = ((std::shared_ptr<OperatorParamSchema>&)plist[1])->getSchema();
    const auto& newAttrs = partialSchema.getAttributes(true);
    for (const auto& newAttr : newAttrs) {
        for (const auto& existingAttr : existingAttrs) {
            if (newAttr.getName() == existingAttr.getName() ||
                existingAttr.hasAlias(newAttr.getName()) ||
                newAttr.hasAlias(existingAttr.getName())) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                     SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttr.getName();
            }
        }
        expanded.push_back(newAttr);
    }

    expanded.addEmptyTagAttribute();

    targetDesc.setAttributes(expanded);
    return targetDesc;
}

}  // namespace scidb
