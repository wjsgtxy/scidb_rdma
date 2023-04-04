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
 * @brief Dummy optimizer which producing physical plan from logical without any
 *                complicated transformations.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <memory>
#include <log4cxx/logger.h>

#include "query/optimizer/Optimizer.h"
#include "query/OperatorLibrary.h"
#include "query/ParsingContext.h"
#include "system/SystemCatalog.h"

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.optimizer"));

class L2POptimizer : public Optimizer
{
public:
    std::shared_ptr<PhysicalPlan> optimize(const std::shared_ptr<Query>& query,
                                             std::shared_ptr<LogicalPlan>& logicalPlan);

private:
    std::shared_ptr<PhysicalQueryPlanNode> traverse(const std::shared_ptr<Query>& query,
                                                      std::shared_ptr<LogicalQueryPlanNode> node);
};

std::shared_ptr<PhysicalPlan> L2POptimizer::optimize(const std::shared_ptr<Query>& query,
                                                       std::shared_ptr<LogicalPlan>& logicalPlan)
{
    std::shared_ptr<LogicalQueryPlanNode> root = logicalPlan->getRoot();

    if (!root) {
        return std::make_shared<PhysicalPlan>(std::shared_ptr<PhysicalQueryPlanNode>());
    }

    std::shared_ptr<PhysicalPlan> physicalPlan = std::make_shared<PhysicalPlan>(traverse(query, root));
    // null out the root
    logicalPlan->setRoot( std::shared_ptr<LogicalQueryPlanNode>());

    return physicalPlan;
}

std::shared_ptr<PhysicalQueryPlanNode> L2POptimizer::traverse(const std::shared_ptr<Query>& query,
                                                                std::shared_ptr<LogicalQueryPlanNode> node)
{
    node = logicalRewriteIfNeeded(query, node);
    std::shared_ptr<LogicalOperator> logicalOp = node->getLogicalOperator();

    vector<string> physicalOperatorsNames;
    OperatorLibrary::getInstance()->getPhysicalNames(logicalOp->getLogicalName(), physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];

    // Collection of input schemas of operator for resolving references
    vector< ArrayDesc> inputSchemas;

    // Adding children schemas
    const vector<std::shared_ptr<LogicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t ch = 0; ch < childs.size(); ch++)
    {
        inputSchemas.push_back(childs[ch]->getLogicalOperator()->getSchema());
    }

    const Parameters& lParams = logicalOp->getParameters();
    Parameters phParams;
    for (size_t i = 0; i < lParams.size(); i++)
    {
        if (lParams[i]->getParamType() == PARAM_LOGICAL_EXPRESSION)
        {
            std::shared_ptr<Expression> phParam = std::make_shared<Expression>();

            std::shared_ptr<OperatorParamLogicalExpression>& lParam = (std::shared_ptr<OperatorParamLogicalExpression>&)lParams[i];

            try
            {
                if (lParam->isConstant())
                {
                   phParam->compile(lParam->getExpression(), false, lParam->getExpectedType().typeId());
                }
                else
                {
                   phParam->compile(lParam->getExpression(), false, lParam->getExpectedType().typeId(), inputSchemas, logicalOp->getSchema());
                }
            }
            catch (Exception &e)
            {
                if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR2)
                {
                    throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_PARAMETER_TYPE_ERROR, lParam->getParsingContext())
                        << lParam->getExpectedType().name() << TypeLibrary::getType(phParam->getType()).name();
                }
                else
                {
                    e.raise();
                }
            }

            phParams.push_back(std::shared_ptr<OperatorParam>(
                    new OperatorParamPhysicalExpression(lParams[i]->getParsingContext(), phParam, lParam->isConstant())));
        }
        else
        {
            phParams.push_back(lParams[i]);
        }
    }

    std::shared_ptr<PhysicalOperator> physicalOp = OperatorLibrary::getInstance()->
            createPhysicalOperator(logicalOp->getLogicalName(), physicalName, phParams, logicalOp->getSchema());
    physicalOp->setQuery(query);

    std::shared_ptr<PhysicalQueryPlanNode> result(new PhysicalQueryPlanNode(physicalOp, node->isDdl(), node->supportsTileMode()));

    for (auto const& node_child, node->getChildren())
    {
       std::shared_ptr <PhysicalQueryPlanNode> pChild = traverse(query, node_child);
       result->addChild(pChild);
    }

    if (result->changesDistribution() || !result->outputFullChunks())
    {
        // Dumbest thing we can do: if we're not ABSOLUTELY SURE the operator is distribution-preserving, insert an SG,
        // and redistribute data exactly as in the output schema of the operator

        ArrayDesc sgSchema = result->getPhysicalOperator()->getSchema();

        Parameters sgParams;
        std::shared_ptr<Expression> distributeExpression = std::make_shared<Expression>();
        Value distributeType(TypeLibrary::getType(TID_INT32));
        distributeType.setInt32(createDistribution(getSynthesizedDistType()));
        distributeExpression->compileConstant(false, TID_INT32, distributeType);
        sgParams.push_back(std::make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                                             distributeExpression,
                                                                             true));

        std::shared_ptr<PhysicalOperator> sgOperator = OperatorLibrary::getInstance()->createPhysicalOperator("_sg",
                                                                                                              "impl_sg",
                                                                                                              sgParams,
                                                                                                              sgSchema);
        sgOperator->setQuery(query);

        std::shared_ptr<PhysicalQueryPlanNode> sgNode(new PhysicalQueryPlanNode(sgOperator,
                                                                                  false,
                                                                                  false));
        sgNode->addChild(result);
        result = sgNode;
    }

    return result;
}

std::shared_ptr<Optimizer> Optimizer::createL2POptimizer()
{
    LOG4CXX_DEBUG(logger, "Creating L2P optimizer instance")
    return std::shared_ptr<Optimizer>(new L2POptimizer());
}

} // namespace
