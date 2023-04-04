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
 * Optimizer.h
 *
 *  Created on: Dec 24, 2009
 *      Author: Emad, roman.simakov@gmail.com
 */

#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <boost/noncopyable.hpp>
#include <query/LogicalQueryPlan.h>

namespace scidb
{
class LogicalQueryPlanNode;
class Query;
class LogicalPlan;
class PhysicalPlan;

/**
 * The abstract class for optimization. Inherit this class to
 * implement some optimization.
 * To use optimizer you must get pointer to its interface. This should be done
 * by calling  create() method. It selects implementation of optimizer
 * according to parameter in configuration.
 */
class Optimizer : boost::noncopyable
{
protected:
    /**
     *  Helper function for constructing optimizer implementation without
     *  including headers. Each function must be implemented in related .cpp file
     *  with optimizer implementation.
     */
    virtual std::shared_ptr<LogicalQueryPlanNode> logicalRewriteIfNeeded(const std::shared_ptr<Query>& query,
                                                                         std::shared_ptr< LogicalQueryPlanNode> node);

  public:
    virtual ~Optimizer() {}

    /// Create a physical plan from the logical plan.
    /// Does not add all the necessary sg nodes - output may not be runnable.
    ///  @param logicalPlan plan to translate.
    ///  @return physicalPlan
    virtual std::shared_ptr<PhysicalPlan> createPhysicalPlan(std::shared_ptr<LogicalPlan> logicalPlan,
                                                             const std::shared_ptr<Query>& query) = 0;
    /**
     * This method get logical plan and generate pair of physical plan to be executed
     * and the rest of logical plan that must be optimized only after execution
     * returned physical plan. Note: we should not return a vector of physical fragments
     * immediately.
     *
     * @param query the query whose plan to be optimized
     * @param[in out] logicalPlan is a logical plan to be optimized. After optimization it contains
     * the rest of logical plan to be optimized later.
     * @return physical plan to be executed.
     *
     */
    virtual std::shared_ptr<PhysicalPlan>
    optimize(const std::shared_ptr<Query>& query,
             std::shared_ptr<PhysicalPlan> physicalPlan, bool isDdl) = 0;
    static std::shared_ptr<Optimizer> create();
};

} // namespace
#endif /* OPTIMIZER_H_ */
