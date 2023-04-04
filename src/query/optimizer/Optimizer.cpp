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
 * @brief Basic class for all optimizers
 *
 * @author knizhnik@garret.ru
 */
#include <query/optimizer/Optimizer.h>

#include <query/Aggregate.h>
#include <query/OperatorLibrary.h>
#include <query/LogicalQueryPlan.h>

namespace scidb
{
    std::shared_ptr< LogicalQueryPlanNode> Optimizer::logicalRewriteIfNeeded(const std::shared_ptr<Query>& query,
                                                                               std::shared_ptr< LogicalQueryPlanNode> const node)
    {
        using std::make_shared;
        //Note: this rewrite mechanism should be
        //  1. generic
        //  2. user-extensible

        //Note: optimizer also performs rewrites like "sum" -> "sum2(sum)" but we can't do these here because:
        //  1. they are physical; not logical
        //  2. they are recursive. We don't want logical rewrites to be recursive.

        OperatorLibrary *olib =  OperatorLibrary::getInstance();
        std::shared_ptr<ParsingContext> const context = node->getParsingContext();

        if (AggregateLibrary::getInstance()->hasAggregate(node->getLogicalOperator()->getLogicalName()))
        {
           std::shared_ptr< LogicalOperator> oldStyleOperator = node->getLogicalOperator();
           std::shared_ptr< LogicalOperator> aggOperator = olib->createLogicalOperator("aggregate");
           aggOperator->setSchema(oldStyleOperator->getSchema());
           Parameters oldStyleParams = oldStyleOperator->getParameters();

           if (node->getLogicalOperator()->getLogicalName()=="count")
           {
               std::shared_ptr<OperatorParam> asterisk = std::make_shared<OperatorParamAsterisk>(context);

               std::shared_ptr<OperatorParam> aggCall =
                   std::make_shared<OperatorParamAggregateCall>(context,
                                                                node->getLogicalOperator()->getLogicalName(),
                                                                asterisk,
                                                                "");
               aggOperator->addParameter(aggCall);

           }
           else if (oldStyleParams.empty())
           {
               ArrayDesc const& inputSchema = node->getChildren()[0]->getLogicalOperator()->getSchema();
               std::shared_ptr<OperatorParamReference> attRef =
                   std::make_shared<OperatorParamAttributeReference>(context,
                                                                     inputSchema.getName(),
                                                                     inputSchema.getAttributes().firstDataAttribute().getName(),
                                                                     true);
               attRef->setInputNo(0);
               attRef->setObjectNo(0);

               std::shared_ptr<OperatorParam> aggCall =
                   std::make_shared<OperatorParamAggregateCall>(context,
                                                                node->getLogicalOperator()->getLogicalName(),
                                                                attRef,
                                                                "");
               aggOperator->addParameter(aggCall);
           }

           if (!oldStyleParams.empty())
           {
               ((LogicalOperator::Properties&)aggOperator->getProperties()).tile = false;
           }

           for (size_t i =0; i<oldStyleParams.size(); i++)
           {
               if (oldStyleParams[i]->getParamType() == PARAM_ATTRIBUTE_REF)
               {
                   std::shared_ptr<OperatorParam> aggCall =
                       std::make_shared<OperatorParamAggregateCall>(oldStyleParams[i]->getParsingContext(),
                                                                    node->getLogicalOperator()->getLogicalName(),
                                                                    oldStyleParams[i],
                                                                    "");
                   aggOperator->addParameter(aggCall);
               }
               else if (oldStyleParams[i]->getParamType() == PARAM_DIMENSION_REF)
               {
                   aggOperator->addParameter(oldStyleParams[i]);
               }
           }

           std::shared_ptr< LogicalQueryPlanNode> aggInstance =
               std::make_shared<LogicalQueryPlanNode>(context,
                                                      aggOperator);
           assert(node->getChildren().size() == 1);
           aggInstance->addChild(node->getChildren()[0]);
           return aggInstance;
        }
        else
        {
           return node;
        }
    }
}
