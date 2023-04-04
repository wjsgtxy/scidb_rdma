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
 * @file QueryProcessor.cpp
 *
 * @author pavel.velikhov@gmail.com, roman.simakov@gmail.com
 *
 * @brief The interface to the Query Processor in SciDB
 *
 * The QueryProcessor provides the interface to create and execute queries
 * in SciDB.
 * The class that handles all major query processing tasks is QueryProcessor, which
 * is a stateless, reentrant class. The client of the QueryProcessor however uses the
 * Query and QueryResult interfaces instead of the QueryProcessor interface.
 */

#include <query/QueryProcessor.h>

#include <array/AccumulatorArray.h>
#include <array/ParallelAccumulatorArray.h>
#include <network/MessageUtils.h>
#include <query/Expression.h>
#include <query/Parser.h>
#include <query/PhysicalQueryPlan.h>
#include <query/RemoteMergedArray.h>
#include <query/optimizer/Optimizer.h>
#include <rbac/Session.h>

#include <log4cxx/logger.h>
#include <sstream>

using std::string;
using std::stringstream;
using std::vector;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// Basic QueryProcessor implementation
class QueryProcessorImpl : public QueryProcessor
{
private:
    // Recursive method for executing physical plan
    std::shared_ptr<Array> execute(const std::shared_ptr<PhysicalQueryPlanNode>& node,
                                   const std::shared_ptr<Query>& query,
                                   int depth);

    void preSingleExecute(const std::shared_ptr<PhysicalQueryPlanNode>& node,
                          const std::shared_ptr<Query>& query);

    void postSingleExecute(const std::shared_ptr<PhysicalQueryPlanNode>& node,
                           const std::shared_ptr<Query>& query);

    // Synchronization methods
    /**
     * Worker notifies coordinator about its state.
     * Coordinator waits for worker notifications.
     */
    void notify(const std::shared_ptr<Query>& query,
                uint64_t timeoutNanoSec = 0);

    /**
     * Worker waits for a notification from coordinator.
     * Coordinator sends out notifications to all workers.
     */
    void wait(const std::shared_ptr<Query>& query);

public:
    std::shared_ptr<Query> createQuery(
        string                              queryString,
        QueryID                             queryId,
        const std::shared_ptr<Session> &    session);

    void parseLogical(const std::shared_ptr<Query>& query, bool afl) override;
    void parsePhysical(const string& plan, const std::shared_ptr<Query>& query) override;
    const ArrayDesc& inferTypes(const std::shared_ptr<Query>& query) override;
    void createPhysicalPlan(const std::shared_ptr<Optimizer>& optimizer,
                            const std::shared_ptr<Query>& query) override;
    void physicalSetQuery(const std::shared_ptr<Query>& query) override;
    void optimize(const std::shared_ptr<Optimizer>& optimizer, const std::shared_ptr<Query>& query,
                  bool isDdl) override;
    void preSingleExecute(const std::shared_ptr<Query>& query) override;
    void execute(const std::shared_ptr<Query>& query) override;
    void postSingleExecute(const std::shared_ptr<Query>& query) override;
    void inferAccess(const std::shared_ptr<Query>& query) override;
};


std::shared_ptr<Query> QueryProcessorImpl::createQuery(
    string                              queryString,
    QueryID                             queryID,
    const std::shared_ptr<Session> &    session)
{
    SCIDB_ASSERT(queryID.isValid());
    assert(session);

    std::shared_ptr<Query> query = Query::create(queryID);

    LOG4CXX_DEBUG(logger, "QueryProcessorImpl::createQuery("
    << "_qry=" << this << ","
    << "id=" << queryID << ","
    << "session=" << session.get()
    << ")" );

    query->attachSession(session);
    query->queryString = queryString;
    return query;
}


void QueryProcessorImpl::parseLogical(const std::shared_ptr<Query>& query, bool afl)
{
    query->logicalPlan = std::make_shared<LogicalPlan>(parseStatement(query,afl));
}


void QueryProcessorImpl::parsePhysical(const std::string& plan, const std::shared_ptr<Query>& query)
{
    assert(!plan.empty());

    stringstream ss;
    ss << plan;
    TextIArchiveQueryPlan ia(ss);
    registerLeafDerivedOperatorParams<TextIArchiveQueryPlan>(ia);

    PhysicalQueryPlanNode* n;
    std::shared_ptr<PhysicalQueryPlanNode> node;
    try {
        ia & n;
        node = ia._helper._nodes.getSharedPtr(n);
    }
    catch (std::exception& ex) {
        string whatmsg(ex.what());
        ASSERT_EXCEPTION_FALSE("Cannot parse physical plan: " << ex.what());
    }

    query->addPhysicalPlan(std::make_shared<PhysicalPlan>(node));
}

const ArrayDesc& QueryProcessorImpl::inferTypes(const std::shared_ptr<Query>& query)
{
    return query->logicalPlan->inferTypes(query);
}

void QueryProcessorImpl::inferAccess(const std::shared_ptr<Query>& query)
{
    return query->logicalPlan->inferAccess(query);
}

void QueryProcessorImpl::createPhysicalPlan(const std::shared_ptr<Optimizer>& optimizer, const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(!(query->logicalPlan->empty()));
    query->addPhysicalPlan(optimizer->createPhysicalPlan(query->logicalPlan, query));
}

void QueryProcessorImpl::physicalSetQuery(const std::shared_ptr<Query>& query)
{
    query->getCurrentPhysicalPlan()->setQuery(query);  //  what about the 0, false? ... probably needed
}

// TODO: need to add ::createPhysicalPlanFromMessage(), and factor that aspect
//       out of handlePhysicalPreparePlan ???

void QueryProcessorImpl::optimize(const std::shared_ptr<Optimizer>& optimizer, const std::shared_ptr<Query>& query, bool isDdl)
{
    SCIDB_ASSERT(query);
    SCIDB_ASSERT(!query->getCurrentPhysicalPlan()->empty());

    query->addPhysicalPlan(optimizer->optimize(query, query->getCurrentPhysicalPlan(), isDdl));
}


// Recursive method for single executing physical plan
void QueryProcessorImpl::preSingleExecute(const std::shared_ptr<PhysicalQueryPlanNode>& node,
                                          const std::shared_ptr<Query>& query)
{
    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    if (query->isNormal() || query->isSub()) {
        vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
        for (size_t i = 0; i < childs.size(); i++) {
            preSingleExecute(childs[i], query);
        }
    }

    physicalOperator->preSingleExecute(query);
}


void QueryProcessorImpl::preSingleExecute(const std::shared_ptr<Query>& query)
{
    LOG4CXX_DEBUG(logger, "(Pre)Single executing queryID: " << query->getQueryID())

    auto physicalPlan = query->getCurrentPhysicalPlan();
    SCIDB_ASSERT(physicalPlan && !physicalPlan->empty());

    preSingleExecute(physicalPlan->getRoot(), query);
}

void QueryProcessorImpl::postSingleExecute(const std::shared_ptr<PhysicalQueryPlanNode>& node, const std::shared_ptr<Query>& query)
{
   Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    if (query->isNormal() || query->isSub()) {
        vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
        for (size_t i = 0; i < childs.size(); i++) {
            postSingleExecute(childs[i], query);
        }
    }

    physicalOperator->postSingleExecute(query);
}


void QueryProcessorImpl::postSingleExecute(const std::shared_ptr<Query>& query)
{
    LOG4CXX_DEBUG(logger, "(Post)Single executing queryID: " << query->getQueryID())

    postSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

// Recursive method for executing physical plan
std::shared_ptr<Array> QueryProcessorImpl::execute(const std::shared_ptr<PhysicalQueryPlanNode>& node,
                                                   const std::shared_ptr<Query>& query,
                                                   int depth)
{
    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();
    physicalOperator->setQuery(query);

    vector<std::shared_ptr<Array> > operatorArguments;
    vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();

    if (node->isDdl())
    {
        physicalOperator->executeWrapper(operatorArguments, query);
        return std::shared_ptr<Array>();
    }
    else
    {
        for (size_t i = 0; i < childs.size(); i++)
        {
            std::shared_ptr<Array> arg = execute(childs[i], query, depth+1);
            if (!arg)
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
            SCIDB_ASSERT(!arg->getArrayDesc().isAutochunked()); // Should have been caught below at depth+1!
            operatorArguments.push_back(arg);
        }
        std::shared_ptr<Array> result(physicalOperator->executeWrapper(operatorArguments, query));
        if (result.get() && result->getArrayDesc().isAutochunked()) {
            // Possibly a user-defined operator has not been adapted for autochunking.  (If it's a
            // built-in operator, this is an internal error, *sigh*.)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_AUTOCHUNKED_EXECUTE_RESULT)
                << physicalOperator->getLogicalName() << result->getArrayDesc().getDimensions();
        }
        return result;
    }
}

namespace {
    // For ease of auditing, best to log the query string on one line
    // and squeeze out extraneous whitespace.  SDB-5754.
    string prettyQuery(string const& queryString)
    {
        string result;
        result.reserve(queryString.size());
        bool spacing = false;
        for (auto const& ch : queryString) {
            if (::isspace(ch)) {
                if (!spacing) {
                    spacing = true;
                    result += ' ';
                }
            } else {
                spacing = false;
                result += ch;
            }
        }
        return result;
    }
}

void QueryProcessorImpl::execute(const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(query->getSession());
    LOG4CXX_INFO(logger, "Executing query(id:" << query->getQueryID()
                 << ", user:" << query->getSession()->getUser().getName()
                 << ", ns:" << query->getSession()->getNamespace().getName()
                 << "): " << prettyQuery(query->queryString)
                 << "; from program: " << query->programOptions << ";");

    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalQueryPlanNode> rootNode = query->getCurrentPhysicalPlan()->getRoot();
    std::shared_ptr<Array> currentResultArray = execute(rootNode, query, 0);

    Query::validateQueryPtr(query);

    if (currentResultArray)
    {
        if (Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
            if (typeid(*currentResultArray) != typeid(ParallelAccumulatorArray)) {
               std::shared_ptr<ParallelAccumulatorArray> paa = std::make_shared<ParallelAccumulatorArray>(currentResultArray);
               currentResultArray = paa;
               std::shared_ptr<PhysicalOperator> physicalOperator = rootNode->getPhysicalOperator();
               paa->start(query);
            }
        } else {
            if (typeid(*currentResultArray) != typeid(AccumulatorArray)) {
                currentResultArray = std::make_shared<AccumulatorArray>(currentResultArray,query);
            }
        }
        if (query->getInstancesCount() > 1 &&
            query->isCoordinator() &&
            !rootNode->isDdl())
        {
            // RemoteMergedArray uses the Query::_currentResultArray as its local (stream) array
            // so make sure to set it in advance
            query->setCurrentResultArray(currentResultArray);
            currentResultArray = RemoteMergedArray::create(currentResultArray->getArrayDesc(),
                    query->getQueryID());
        }
    }
    query->setCurrentResultArray(currentResultArray);
}

/**
 * QueryProcessor static method implementation
 */

std::shared_ptr<QueryProcessor> QueryProcessor::create()
{
    return std::shared_ptr<QueryProcessor>(new QueryProcessorImpl());
}


} // namespace
