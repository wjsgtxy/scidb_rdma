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

#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <rbac/Rights.h>

namespace scidb {

/**
 * @brief The operator: lock_arrays().
 *
 * @par Synopsis:
 *   lock_arrays( boolean )
 *
 * @par Summary:
 *   lock_arrays(true)
 *     Installs a global array lock, preventing the creation, removal,
 *     or update of any array.
 *   lock_arrays(false)
 *     Removes the global array lock.
 */
struct LogicalLockArrays : LogicalOperator
{
    LogicalLockArrays(const std::string& logicalName,
                      const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.ddl = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) }
        };
        return &argSpec;
    }

private:
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(query);
        SCIDB_ASSERT(schemas.empty());

        return ddlArrayDesc(query);
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        SCIDB_ASSERT(query);

        // Must be operator to freeze array updates.
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLockArrays, "lock_arrays")

}  // namespace scidb
