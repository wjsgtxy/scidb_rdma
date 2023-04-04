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
 * @file PhysicalMaterialize.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical set_temp operator
 * to save pointers to array and get in on the next iteration.
 */

#include <array/DelegateArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/QueryProcessor.h>

using namespace std;

namespace scidb
{

class PhysicalMaterialize: public PhysicalOperator
{
public:
    PhysicalMaterialize(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        MaterializedArray::MaterializeFormat format = (MaterializedArray::MaterializeFormat)((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getUint32();
        return std::shared_ptr<Array>(new MaterializedArray(inputArrays[0], query, format));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMaterialize, "_materialize", "impl_materialize")

} //namespace
