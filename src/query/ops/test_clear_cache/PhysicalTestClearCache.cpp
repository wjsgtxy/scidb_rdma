/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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

#include <array/MemArray.h>
#include <query/PhysicalOperator.h>
#include <log4cxx/logger.h>

using namespace std;
using namespace scidb;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("test_clear_cache"));

namespace scidb
{

/// for the overall documentation of this operator, see the LogicalTestClearCache
class PhysicalTestClearCache: public PhysicalOperator
{
  public:
    PhysicalTestClearCache(const std::string& logicalName, const std::string& physicalName,
                         const Parameters& parameters, const ArrayDesc& schema)
    :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays, std::shared_ptr<Query> query)
    {
        ASSERT_EXCEPTION(inputArrays.empty(), "test_clear_cache does not accept any input arrays");
        ASSERT_EXCEPTION(_parameters.empty(), "test_clear_cache takes no parameterss");

        BufferMgr::getInstance()->clearCache();

        return std::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalTestClearCache, "test_clear_cache", "test_clear_cache_impl")

} // namespace scidb
