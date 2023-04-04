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
 * @file LogicalTestSG.cpp
 *
 * @author
 *
 *
 */

#include <log4cxx/logger.h>

#include <query/LogicalOperator.h>

#include <array/ArrayDistribution.h>
#include <query/Expression.h>
#include <query/Query.h>

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("test_sg"));

namespace scidb
{
/**
 * @brief The operator: test_sg().
 *
 * @par Synopsis:
 *   test_sg( srcArray, partitionSchema, instanceId=-1, sgMode, isStrict=false)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *
 * @par Input:
 *   - srcArray: the source array, with srcAttrs and srcDims.
 *   - partitionSchema:<br>
 *     see ArrayDistributionInterface.h
 *   - instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *   - sgMode: <br>
 *     'serial'-all attributes are redistributed in order; <br>
 *     'parallel' - all attributes are redistributed concurrently; <br>
 *     'randomRes' - some kind of random residency test <br>
 *     ''='parallel'<br>
 *   - isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false. <br>
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
class LogicalTestSG: public LogicalOperator
{
public:
    LogicalTestSG(const std::string& logicalName, const std::string& alias):
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
                    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)),
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),
                       RE(RE::QMARK, {
                          RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)),
                          RE(RE::STAR, {
                             RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))
                          })
                       })
                    })
                 })
              })
           }
        };
        return &argSpec;
    }

    private:

    DistType getDistType(const std::shared_ptr<Query>& query) const
    {
        ASSERT_EXCEPTION(_parameters[0], "Partitioning schema is not specified by the user");
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[0].get());
        const DistType dt = static_cast<DistType>(
            evaluate(lExp->getExpression(), TID_INT32).getInt32());
        if (! isValidDistType(dt, false) && not isLocal(dt)) // false = not allow optional data associated with the DistType
        {
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
            instanceId = static_cast<InstanceID>(evaluate(lExp->getExpression(), TID_INT64).getInt64());
        }
        instanceId = (instanceId==COORDINATOR_INSTANCE_MASK) ? query->getInstanceID() : instanceId;
        return instanceId;
    }

    string getSGMode(const std::shared_ptr<Query>& query)
    {
        if (_parameters.size() < 3) {
            return string();
        }
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[2].get());
        std::string sgmode(evaluate(lExp->getExpression(), TID_STRING).getString());
        return sgmode;
    }

    public:

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];

        // sanity check: #parameters is at least 1 (i.e. partitionSchema), but no more than #dims+4.
        if (_parameters.size() > inputSchemas[0].getDimensions().size() + 4) {
            LOG4CXX_ERROR(logger, "LogicalTestSG: _parameters.size(): " << _parameters.size());
            LOG4CXX_ERROR(logger, "LogicalTestSG: schemas[0].getDimensions().size()" << inputSchemas[0].getDimensions().size());
            ASSERT_EXCEPTION_FALSE("Parameter size must be no more than four plus # of dimensions.");
        }

        if (_parameters.size() > 4 && inputSchemas[0].getDimensions().size() + 4 != _parameters.size()) {
            LOG4CXX_ERROR(logger, "LogicalTestSG: offset vector: " << _parameters.size() - 4);
            ASSERT_EXCEPTION_FALSE("Number of parameters size must be no more than four plus # of dimensions and no less than four.");
        }

        //validate the DistType
        const DistType dt = getDistType(query) ;
        InstanceID localInstance = getInstanceId(query);
        std::string sgMode = getSGMode(query);

        const std::string& resultArrayName = desc.getName();
        if (isDebug()) {
            if (_parameters.size() >= 4) {
                assert(_parameters[3]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[3].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        }
        std::string distCtx;
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
        ArrayResPtr arrRes = query->getDefaultArrayResidency(); // use the query live set because we dont know better
        if (sgMode == "randomRes") {
            if (_randomRes) {
                arrRes = _randomRes;
            } else {
                std::vector<InstanceID> someInstances;
                someInstances.reserve(arrRes->size());
                for (size_t i=0; i < arrRes->size(); ++i) {

                    if (isLocal(dt) && i<=localInstance) {
                        someInstances.push_back(arrRes->getPhysicalInstanceAt(i));
                    } else if ((rand() % (i+1)) == 0) {
                        someInstances.push_back(arrRes->getPhysicalInstanceAt(i));
                    }
                }
                if (someInstances.size() == 0) {
                    someInstances.push_back(arrRes->getPhysicalInstanceAt(0));
                }
                arrRes = createDefaultResidency(PointerRange<InstanceID>(someInstances));
                _randomRes = arrRes;
            }
        }

        return ArrayDesc(resultArrayName, desc.getAttributes(), desc.getDimensions(), arrDist, arrRes);
    }
    private:
    ArrayResPtr _randomRes;

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTestSG, "test_sg");

} //namespace
