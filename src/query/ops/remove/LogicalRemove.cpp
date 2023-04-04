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
 * LogicalRemove.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayName.h>
#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/LockDesc.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: remove().
 *
 * @par Synopsis:
 *   remove( arrayToRemove )
 *
 * @par Summary:
 *   Drops an array.
 *
 * @par Input:
 *   - arrayToRemove: the array to drop.
 *
 * @par Output array:
 *   NULL
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
class LogicalRemove: public LogicalOperator
{
public:
    LogicalRemove(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.exclusive = true;
        _properties.ddl = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_ARRAY_NAME))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, std::shared_ptr< Query> query) override
    {
        SCIDB_ASSERT(schemas.empty());

        return ddlArrayDesc(query);
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);
        assert(_parameters.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        string const& objName =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(objName));

        string arrayName;
        string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        std::shared_ptr<LockDesc>  lock(
            new LockDesc(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                LockDesc::RM));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= LockDesc::RM);

        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName, rbac::P_NS_DELETE);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRemove, "remove")


}  // namespace scidb
