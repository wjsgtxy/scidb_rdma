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

/// @file PhysicalPlan.cpp
// taken from QueryPlan.cpp

#include <query/PhysicalQueryPlan.h>

#include <memory>

#include <log4cxx/logger.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <system/Utils.h>
#include <util/Indent.h>

using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("PhysicalPlan"));

PhysicalPlan::PhysicalPlan(const std::shared_ptr<PhysicalQueryPlanNode>& root):
        _root(root)
{
}

void PhysicalPlan::inferDistType(const std::shared_ptr<Query>& query)
{
    // This is the only place this should be done.  Do not duplicate.
    // the optimizer will check this was done before optimizing.
    // Currently, must be done on each instance prior to execution, so
    // it is called by the QueryExecutor, but see SDB-6466
    auto rootOp = _root->getPhysicalOperator();
    if (rootOp->getSchema().isDataframe()) {
        rootOp->setInheritedDistType(dtDataframe);
    } else {
        rootOp->setInheritedDistType(defaultDistTypeRoot());
    }
    checkRootDistType();

    _root->inferDistType(query, 0);
    LOG4CXX_TRACE(logger, "PhysicalPlan::inferDistType() done.");
}

void PhysicalPlan::checkRootDistType()
{
    // on resolving SDB-6466, reconsider where this code lives
    auto rootOp = _root->getPhysicalOperator();
    auto rootDistType = rootOp->getInheritedDistType();

    SCIDB_ASSERT(isPartition(rootDistType) ||
                 (isReplicated(rootDistType) && isReplicated(defaultDistTypeRoot())));

    if (isReplicated(rootDistType)) {
        LOG4CXX_ERROR(logger, "PhysicalPlan::checkRootDistType(): rootDistType is Replicated,"
                              << " unsupported for production use, for test purposes only.");
    }
    LOG4CXX_TRACE(logger, "PhysicalPlan::checkRootDistType(): rootDistType: " << distTypeToStr(rootDistType));
}

void PhysicalPlan::setQuery(const std::shared_ptr<Query>& query)
{
    _root->getPhysicalOperator()->setQuery(query);
}

void PhysicalPlan::toString(std::ostream &out, int const indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[pPlan]:";
    if (_root.get() != NULL)
    {
        out << "\n";
        _root->toString(out, indent+1, children);
    }
    else
    {
        out << "[NULL]\n";
    }
}

size_t PhysicalPlan::countSgNodes() const
{
    struct CountSGVisitor : public PhysicalQueryPlanNode::Visitor
    {
        size_t count ;

        CountSGVisitor()
        :   count(0)
        {}

        void operator()(PhysicalQueryPlanNode& node, const PhysicalQueryPlanPath* descPath, size_t depth)
        {
            auto isSG = node.isSgNode();
            LOG4CXX_TRACE(logger, "CountSGVisitor, isSG " << isSG << " at depth: " << depth);
            count += static_cast<size_t>(isSG);
        }
    };

    CountSGVisitor visitor;
    _root->visitDepthFirstPostOrder(visitor, /*depth*/0);
    return visitor.count;
}


} // namespace
