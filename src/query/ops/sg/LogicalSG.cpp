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
 * @file LogicalSG.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator SCATTER/GATHER
 */

#include "SGParams.h"

#include <query/LogicalOperator.h>

#include <array/ArrayDistribution.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: sg().
 *
 * @par Synopsis:
 *   sg( srcArray, partitionSchema, instanceId=-1, outputArray="", isStrict=false, isPullSG=false)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *   It is the only operator that uses the network manager.
 *   Typically this operator is inserted by the optimizer into the physical plan.
 *
 * @par Input:
 *   -[0] srcArray: the source array, with srcAttrs and srcDims.
 *   -[1] distribution:<br>
 *     see ArrayDistributionInterface.h
 *   -[2] instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *     [TODO: The usage of instanceId, in calculating which instance a chunk should go to, requires further documentation.]
 *   -[3] isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false. <br>
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalSG: public LogicalOperator
{
public:
    LogicalSG(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_UINT32)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)),  // instanceID
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)), // isStrict
                       RE(RE::QMARK, {
                          RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))  // isPull
                       })
                    })
                 })
              })
            }
        };
        return &argSpec;
    }

private:

    /// @return  validated DistType specified by the user
    /// @throws scidb::SystemException if the scheme is unsupported
    /// @todo XXX TODO: change DistType to DistributionId
    DistType getDistType(const std::shared_ptr<Query>& query) const
    {
        ASSERT_EXCEPTION(_parameters[0], "Partitioning schema is not specified by the user");
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[0].get());
        const DistType dt = static_cast<DistType>(
            evaluate(lExp->getExpression(), TID_INT32).getInt32());
        if (! isValidDistType(dt, false)  && not isLocal(dt))
        {
            // Do not allow optional data associated with the DistType
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        return dt;
    }

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(
                evaluate(lExp->getExpression(), TID_INT64).getInt64());
        }
        instanceId = (instanceId==COORDINATOR_INSTANCE_MASK) ? query->getInstanceID() : instanceId;
        return instanceId;
    }

    public:

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query) override
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];

        //validate the DistType
        const DistType dt = getDistType(query) ;
        InstanceID localInstance = getInstanceId(query);

        if (isDebug()) {
            if (_parameters.size() > SGPARAM_IS_STRICT) { // isStrict present
                assert(_parameters[SGPARAM_IS_STRICT]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[SGPARAM_IS_STRICT].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
            if (_parameters.size() > SGPARAM_IS_PULL) { // isPull present
                assert(_parameters[SGPARAM_IS_PULL]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[SGPARAM_IS_PULL].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        }

        string distCtx;
        if (isLocal(dt)) {
            ASSERT_EXCEPTION((localInstance < query->getInstancesCount()),
                             "The specified instance is larger than total number of instances");
            stringstream ss;
            ss<<localInstance;
            distCtx = ss.str();
        }

        ArrayDistPtr arrDist = ArrayDistributionFactory::getInstance()->construct(dt,
                                                                                  DEFAULT_REDUNDANCY,
                                                                                  distCtx,
                                                                                  0);
        return ArrayDesc(desc.getName(),
                         desc.getAttributes(),
                         desc.getDimensions(),
                         arrDist,
                         query->getDefaultArrayResidency()); // use the query live set because we dont know better
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSG, "_sg")

} //namespace
