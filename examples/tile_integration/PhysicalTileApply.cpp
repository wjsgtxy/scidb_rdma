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
#include <vector>

#include <query/PhysicalOperator.h>
#include <query/FunctionLibrary.h>
#include <query/FunctionDescription.h>
#include <query/TypeSystem.h>
#include <system/ErrorsLibrary.h>

#include <array/Array.h>
#include "TileApplyArray.h"

namespace scidb {

using namespace std;

class PhysicalTileApply: public PhysicalOperator
{
  public:
    PhysicalTileApply(const string& logicalName,
                      const string& physicalName,
                      const Parameters& parameters,
                      const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size()%2 == 0);

        size_t numAttrs = _schema.getAttributes().size();
        assert(numAttrs >0);
        std::shared_ptr< vector<std::shared_ptr<Expression> > > expPtr = std::make_shared< vector< std::shared_ptr< Expression> > >(numAttrs);
        vector<std::shared_ptr<Expression> >& expressions = *expPtr;
        assert(expressions.size() == numAttrs);

        size_t currentParam = 0;
        auto attrIter = _schema.getAttributes().begin();
        for(size_t i =0; i<numAttrs; ++i)
        {
            assert(_parameters.size() > currentParam+1);
            assert(_parameters[currentParam]->getParamType() == PARAM_ATTRIBUTE_REF);
            assert(_parameters[currentParam+1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);

            string const& schemaAttName = attrIter->getName();
            ++attrIter;
            string const& paramAttName  = ((std::shared_ptr<OperatorParamReference>&)_parameters[currentParam])->getObjectName();

            if(schemaAttName == paramAttName)
            {
                expressions[i] = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[currentParam+1])->getExpression();
                currentParam+=2;
            }
            if(currentParam == _parameters.size())
            {
                break;
            }
        }

        assert(currentParam == _parameters.size());
        assert(expressions.size() == _schema.getAttributes().size());

        assert(!_tileMode); // we dont run in old tile mode
        std::shared_ptr<Array> input = inputArrays[0];
        return std::make_shared<TileApplyArray>(_schema, input, expPtr, query);
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTileApply, "tile_apply", "PhysicalTileApply");

}  // namespace scidb
