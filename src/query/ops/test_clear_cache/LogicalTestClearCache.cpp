/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2019-2019 SciDB, Inc.
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

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;
using namespace scidb;

namespace scidb
{

/**
 * @brief The operator: test_clear_cache().
 *
 * @par Synopsis:
 *   test_clear_cache()
 *
 * @par Summary:
 *   This is an operator for invoking BufferMgr::clearCache()
 *   The operator does not accept array arguments nor can it
 *   return an array containing any chunks.
 *   It is not intended (at this time) for end-user use.
 *   It merely invokes pre-existing test code inside BufferMgr
 *
 * @par Input:
 *   - None
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   test_clear_cache()
 *
 * @par Errors:
 *   Any runtime error should result in a generic exception.
 *
 * @par Notes:
 *   A test operator meant only to invoke BufferMgr::clearCache()
 */
class LogicalTestClearCache: public LogicalOperator
{
public:
    LogicalTestClearCache(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        // Not really ddl, see inferSchema()
        _properties.ddl = true ; // so that inferSchema can return "ddlArrayDesc()"
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        ASSERT_EXCEPTION(schemas.empty(), "test_clear_cache operator does not accept any array arguments");

        // Not really ddl, but this avoids returning an array
        // (that will ParallelAccumulator would insist has at least one attribute).
        return ddlArrayDesc(query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalTestClearCache, "test_clear_cache")

}  // namespace scidb
