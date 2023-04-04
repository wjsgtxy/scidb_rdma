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


/// @file LogicalQueryPlanNode.cpp
// taken from QueryPlan.cpp

#include <query/LogicalQueryPlan.h>
#include <query/Query.h>

#include <memory>

#include <log4cxx/logger.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <util/Indent.h>

using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("LogicalNode"));

//
// LogicalQueryPlanNode
//

LogicalQueryPlanNode::LogicalQueryPlanNode(
    const std::shared_ptr<ParsingContext>& parsingContext,
    const std::shared_ptr<LogicalOperator>& logicalOperator):
    _logicalOperator(logicalOperator),
    _parsingContext(parsingContext)
{
}

LogicalQueryPlanNode::LogicalQueryPlanNode(
    const std::shared_ptr<ParsingContext>& parsingContext,
    const std::shared_ptr<LogicalOperator>& logicalOperator,
    const std::vector<std::shared_ptr<LogicalQueryPlanNode>>& childNodes):
    _logicalOperator(logicalOperator),
    _childNodes(childNodes),
    _parsingContext(parsingContext)
{
}

const ArrayDesc& LogicalQueryPlanNode::inferTypes(const std::shared_ptr<Query>& query, bool forceSchemaUpdate)
{
    // For Query::Kind::NORMAL queries, the cached answer is sufficient.  For Query::Kind::SUB
    // queries, we need to re-infer the schemas to ensure that the correct array versions are used.
    // Re-inference is required in, for example, this case:
    //    create array f <v:int64>[i=1:10];
    //    mquery(insert(build(<v:int64>[i=1:10],i),f),
    //           insert(project(apply(f, newv, 2*v), newv), f));
    if (query->isNormal() &&
        _logicalOperator->hasSchema() &&
        !forceSchemaUpdate) {
        return _logicalOperator->getSchema(); // the cached answer will work, avoids excessive recursion
    }

    // recurse on the child nodes to determine their ArrayDescriptors, prior to
    // computing this node's ArrayDescriptor as derived from the children's descriptors.
    bool allowDf = _logicalOperator->getProperties().dataframe;
    std::vector<ArrayDesc> inputSchemas;
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        inputSchemas.push_back(_childNodes[i]->inferTypes(query));
        if (!allowDf && inputSchemas.back().isDataframe()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DATAFRAMES_NOT_SUPPORTED)
                << _logicalOperator->getLogicalName();
        }
    }

    ArrayDesc outputSchema = _logicalOperator->inferSchema(inputSchemas, query);
    LOG4CXX_TRACE(logger, "LogicalQueryPlanNode::inferTypes produced PS: " <<
                  outputSchema.getDistribution()->getDistType());

    // TODO: Consider wrapping LogicalOperator::inferSchema with a method which does the following
    //       addAlias() logic rather than having this method take responsibility for it.
    if (!_logicalOperator->getAliasName().empty())
    {
        outputSchema.addAlias(_logicalOperator->getAliasName());  // TBD: investigate addAlias vs setAliasName
    }

    _logicalOperator->setSchema(outputSchema);  // cache the result

    return _logicalOperator->getSchema();
}

void LogicalQueryPlanNode::inferAccess(const std::shared_ptr<Query>& query)
{
    //XXX TODO: consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        _childNodes[i]->inferAccess(query);
    }

    assert(_logicalOperator);
    _logicalOperator->inferAccess(query);

    if (!_logicalOperator->hasRightsForAllNamespaces(query)) {
        // One or more of the namespaces referred to, explicitly or
        // implicitly, by this operator's array parameters does not
        // appear in the query's "rights I need" map.  That's a bug on
        // the part of the operator's author.
        //
        // It's better to throw this error than to allow the query to
        // run without access control enforcement.
        stringstream ss;
        ss << "Operator '" << _logicalOperator->getLogicalName()
           << "' is missing rights for one or more namespaces it uses,"
           << " see log file for details";
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
}

bool LogicalQueryPlanNode::isSelective() const
{
    // By definition DDL does not return any result array.
    if (isDdl()) {
        return false;
    }

    // Operators that do not directly modify persistent data are
    // always going to return a non-null Array (even if it is empty).

    if (!_logicalOperator->getProperties().updater) {
        return true;
    }

    // Whether data-modifying operators return result arrays or not is
    // determined by the "fetch" setting.  This is either an explicit
    // named parameter, or a system-wide default.

    Parameter p = _logicalOperator->findKeyword("_fetch");
    if (p) {
        auto lexp = dynamic_cast<OperatorParamLogicalExpression*>(p.get());
        return evaluate(lexp->getExpression(), TID_BOOL).getBool();
    }

    // No _fetch parameter, answer based on configured default behavior.
    return DEFAULT_FETCH_UPDATE_RESULTS;
}


void LogicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lInstance] children "<<_childNodes.size()<<"\n";
    _logicalOperator->toString(out,indent+1);

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

} // namespace
