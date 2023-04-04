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
 * @file LogicalUnLoadLibrary.cpp
 * @brief Logical DDL operator which unloads user defined library
 * @author roman.simakov@gmail.com
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>

namespace scidb
{

/**
 * @brief The operator: unload_library().
 *
 * @par Synopsis:
 *   unload_library( library )
 *
 * @par Summary:
 *   Unloads a SciDB plugin.
 *
 * @par Input:
 *   - library: the name of the library to unload.
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - This operator is the reverse of load_library().
 *
 */
class LogicalUnLoadLibrary: public LogicalOperator
{
public:
    LogicalUnLoadLibrary(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.ddl = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
            }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        // Need database administrative powers.
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        return ddlArrayDesc(query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnLoadLibrary, "unload_library")

} //namespace
