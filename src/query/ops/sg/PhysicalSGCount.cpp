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

///
/// @file PhysicalSGCount.cpp
///

#include <array/MemArray.h>
#include <array/TupleArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/LogicalQueryPlan.h>
#include <query/PhysicalQueryPlan.h>
#include <query/Query.h>
#include <query/QueryProcessor.h>
#include <query/optimizer/Optimizer.h>
#include <util/OnScopeExit.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sgcount"));

class PhysicalSGCount: public PhysicalOperator
{
public:
    PhysicalSGCount(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(_parameters.size()==1);
        auto expression = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression();
        string queryString = expression->evaluate().getString();

        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
                                                query->getPhysicalCoordinatorID(),
                                                query->mapLogicalToPhysical(query->getInstanceID()),
                                                query->getCoordinatorLiveness());

        size_t sgCount(0);
        {
            arena::ScopedArenaTLS arenaTLS(innerQuery->getArena());
            OnScopeExit fqd([&innerQuery] () { Query::destroyFakeQuery(innerQuery.get()); });
            innerQuery->queryString = queryString;

            queryProcessor->parseLogical(innerQuery, true); // for internal testing only, all afl
            queryProcessor->inferTypes(innerQuery);
            const bool isDdl = innerQuery->logicalPlan->isDdl();

            std::shared_ptr< Optimizer> optimizer =  Optimizer::create();

            queryProcessor->createPhysicalPlan(optimizer, innerQuery);
            queryProcessor->optimize(optimizer, innerQuery, isDdl);

            sgCount = innerQuery->getCurrentPhysicalPlan()->countSgNodes();
            LOG4CXX_TRACE(logger, "PhysicalSGCount::preSingleExecute(): sg count = "  << sgCount);
        }

        Value tuple[1]; tuple[0].setUint64(sgCount);

        std::shared_ptr<TupleArray> tupleArray = std::make_shared<TupleArray>(_schema, _arena);
        tupleArray->appendTuple(tuple);
        _result = tupleArray;
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        if (!_result) {
           _result = std::make_shared<MemArray>(_schema, query);
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSGCount, "_sgcount", "_sgcount_impl")

} //namespace
