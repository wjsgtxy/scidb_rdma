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

/// @file LogicalPlan.cpp
// taken from QueryPlan.cpp

#include <query/LogicalQueryPlan.h>

#include <memory>

#include <log4cxx/logger.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/Query.h>
#include <util/Indent.h>

using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("LogicalPlan"));

LogicalPlan::LogicalPlan(const std::shared_ptr<LogicalQueryPlanNode>& root):
        _root(root)
{

}

void LogicalPlan::inferAccess(const std::shared_ptr<Query>& query)
{
    return _root->inferAccess(query);
}

/**
 * Detects if this logical plan is one for a compound_query() AFL statement.
 * @param lqpn A logical query plan node to inspect.
 * @return True if this logical query plan node is for a compound_query()
 * statement, false if not.
 */
bool isCompoundQuery(const std::shared_ptr<LogicalQueryPlanNode>& lqpn)
{
    SCIDB_ASSERT(lqpn);
    auto op = lqpn->getLogicalOperator();
    SCIDB_ASSERT(op);
    auto opName = op->getOperatorName();
    SCIDB_ASSERT(!opName.empty());
    auto isCompound = (opName == "mquery");
    return isCompound;
}

const ArrayDesc& LogicalPlan::inferTypes(const std::shared_ptr<Query>& query)
{
    // the root's inherited type is set to the default, which needs to be
    // a partitioning distribution (for good performance).
    _root->getLogicalOperator()->setInheritedDistType(defaultDistType());
    auto isCompound = isCompoundQuery(_root);
    if (isCompound) {
        // This is a grouped query, mark it as the parent and record
        // how many children it will have.  The number of children directly
        // corresponds to the number of arguments to the grouped query as
        // each argument on its own is a complete AFL statement to execute
        // in its own query.
        query->setKind(Query::Kind::MULTI);
        query->setSubqueryCount(_root->getChildren().size());
    }
    return _root->inferTypes(query);
}

void LogicalPlan::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lPlan]:\n";
    _root->toString(out, indent+1, children);
}

} // namespace
