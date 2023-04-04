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
 * @file OperatorProfiling.cpp
 * @brief Stub for profiling the execution of Operators
 *
 *        executeWrapper is in a separate file so it is easy to replace this
 *        stub with profiling code, without comitting a particular set of
 *        profiling code to the trunk.
 *
 *        This stub version merely calls execute().
 */

#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <util/logVmSizes.h>

#include <sstream>


using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("operatorProfiling"));

namespace scidb
{

std::shared_ptr< Array> PhysicalOperator::executeWrapper(std::vector< std::shared_ptr< Array> >& arrays,
                                                           std::shared_ptr<Query> query)
{
    auto result = execute(arrays, query);                   // happens here on operator "list('queries')" ::execute

    std::stringstream ss;
    ss << "operator: " << this->getPhysicalName();
    ss << " in query: " << query->getQueryString();
    SDB_LOG4_VM_SIZES(logger, ss.str());

    return result;
}

} // namespace scidb
