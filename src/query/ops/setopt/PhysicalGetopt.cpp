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
 * @file PhysicalGetopt.cpp
 *
 * Physical implementation of GETOPT operator for getopting data from text files
 */

#include <array/TupleArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/Config.h>

#include <log4cxx/logger.h>

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(
    log4cxx::Logger::getLogger("scidb.query.ops.getopt"));

using namespace std;

namespace scidb
{

class PhysicalGetopt: public PhysicalOperator
{
  public:
    PhysicalGetopt(const string& logicalName,
                   const string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        string oldValue;
        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema, _arena, query->getInstanceID());

        std::shared_ptr<OperatorParamPhysicalExpression> p0 =
            (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0];
        string name = p0->getExpression()->evaluate().getString();

        SCIDB_ASSERT(_parameters.size() == 1);

        oldValue = Config::getInstance()->getOptionValue(name);
        Value tuple[2];
        tuple[0].setString(oldValue.c_str());  // Inst value
        tuple[1].setBool(true);                // Empty bitmap value
        tuples->appendTuple(tuple);

        return tuples;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalGetopt, "_getopt", "physicalGetopt")

} //namespace
