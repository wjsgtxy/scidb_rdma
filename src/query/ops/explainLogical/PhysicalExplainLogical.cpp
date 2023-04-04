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
 * @file LogicalExplainPhysical.cpp
 *
 * @author poliocough@gmail.com
 *
 * explain_logical operator / Physical implementation.
 */

#include <array/TupleArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <query/LogicalQueryPlan.h>
#include <query/QueryProcessor.h>
#include <util/OnScopeExit.h>


using std::string;

namespace scidb
{

class PhysicalExplainLogical: public PhysicalOperator
{
public:
    PhysicalExplainLogical(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t depth) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext
        getOutputDistribution(const std::vector<RedistributeContext> & /*inputDist*/,
                              const std::vector< ArrayDesc> & /*inputSchemas*/) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        bool afl = false;

        assert (_parameters.size()==1 || _parameters.size()==2);
        string queryString = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

        if (_parameters.size() == 2)
        {
            string languageSpec = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getString();
            afl = languageSpec == "afl";
        }

        std::ostringstream planString;

        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
                         query->getPhysicalCoordinatorID(),
                         query->mapLogicalToPhysical(query->getInstanceID()),
                         query->getCoordinatorLiveness());
        {
            arena::ScopedArenaTLS arenaTLS(innerQuery->getArena());

            OnScopeExit fqd([&innerQuery] () { Query::destroyFakeQuery(innerQuery.get()); });

            innerQuery->queryString = queryString;

            queryProcessor->parseLogical(innerQuery, afl);
            queryProcessor->inferTypes(innerQuery);

            innerQuery->logicalPlan->toString(planString);
        }

        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema, _arena);
        Value tuple[1];
        tuple[0].setData(planString.str().c_str(), planString.str().length() + 1);
        tuples->appendTuple(tuple);

        _result = tuples;
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        if (!_result)
        {
            _result = std::make_shared<TupleArray>(_schema, _arena);
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalExplainLogical, "_explain_logical", "physicalExplainLogical")

} //namespace
