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
 * @file
 *
 * @brief Routines for serializing physical plans to strings.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <query/Serialize.h>

#include <query/Expression.h>
#include <query/PhysicalQueryPlan.h>

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <sstream>

#include <log4cxx/logger.h>

using namespace std;
using boost::archive::text_iarchive;
using boost::archive::text_oarchive;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.serialize"));
}

namespace scidb
{

string serializePhysicalPlan(const std::shared_ptr<PhysicalPlan> &plan)
{
    stringstream ss;
    text_oarchive oa(ss);

    try {
        const std::shared_ptr<PhysicalQueryPlanNode> &queryRoot = plan->getRoot();

        registerLeafDerivedOperatorParams<text_oarchive>(oa);

        PhysicalQueryPlanNode* n = queryRoot.get();
        oa & n;
    }
    catch (std::exception& ex) {
        string whatmsg(ex.what());
        ASSERT_EXCEPTION_FALSE("Cannot serialize physical plan: " << ex.what());
    }

    return ss.str();
}

//
// There is no deserializePhysicalPlan here.  Deserialization of
// physical plans happens in QueryProcessor::parsePhysical(), by way
// of an mtPreparePhysicalPlan message and
// ServerMessageHandleJob::handlePreparePhysicalPlan().
//

string serializePhysicalExpression(const Expression &expr)
{
    stringstream ss;
    text_oarchive oa(ss);

    oa & expr;

    return ss.str();
}

Expression deserializePhysicalExpression(const string &str)
{
    Expression expr;
    stringstream ss;
    ss << str;
    text_iarchive ia(ss);
    ia & expr;

    return expr;
}


} //namespace scidb
