/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file PhysicalShowArrays.cpp
 */

#include "FilterArrays.h"

#include <array/TupleArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

using namespace std;


namespace scidb
{

/**
 * Physical _show_arrays operator.
 */
class PhysicalShowArrays: public PhysicalOperator
{
  public:
    PhysicalShowArrays(
        const string &      logicalName,
        const string &      physicalName,
        const Parameters &  parameters,
        const ArrayDesc &   schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    // Execute runs on all instances and we only want verify_user to
    // run on the coordinator.  So the code is moved to the
    // postSingleExecute, since preSingleExecute and postSingleExecute
    // only run on the coordinator.
    std::shared_ptr<Array> execute(
        vector< std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema,
                                                                          _arena);

        string regExNamespace = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[0])->getExpression()->evaluate().getString();

        string regExArray = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[1])->getExpression()->evaluate().getString();

        bool ignoreOrphanAttributes = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[2])->getExpression()->evaluate().getBool();

        bool ignoreVersions = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[3])->getExpression()->evaluate().getBool();

        const bool orderByName = true;

        std::vector<ArrayDesc> arrayDescs;
        SystemCatalog::getInstance()->getArrays(
            "", arrayDescs, ignoreOrphanAttributes, ignoreVersions, orderByName);
        filterArrays(arrayDescs, regExNamespace, regExArray);

        if (query->isCoordinator())
        {
            Value tuple[3];
            for(const auto & arrayDesc : arrayDescs)
            {
                tuple[0].setString(arrayDesc.getNamespaceName());
                tuple[1].setString(arrayDesc.getName());
                tuple[2].setBool(true);                // Empty bitmap value  // DJG TODO
                tuples->appendTuple(tuple);
            }
        }
        return tuples;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalShowArrays, "_show_arrays", "PhysicalShowArrays");

}  // namespace scidb
