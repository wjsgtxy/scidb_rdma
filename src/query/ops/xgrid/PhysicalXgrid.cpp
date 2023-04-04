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
 * PhysicalXgrid.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include "LogicalXgrid.h"
#include "XgridArray.h"

#include <query/PhysicalOperator.h>
#include <query/Expression.h>

namespace scidb {

using namespace std;

class PhysicalXgrid : public  PhysicalOperator
{
    LogicalXgrid::ScaleFactors  _scaleFactors;

  public:
    PhysicalXgrid(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty()) {
            return PhysicalBoundaries::createEmpty(
                        _schema.getDimensions().size());
        }

        const Coordinates& inStart = inputBoundaries[0].getStartCoords();
        const Coordinates& inEnd = inputBoundaries[0].getEndCoords();
        const Dimensions& inDims = inputSchemas[0].getDimensions();

        Coordinates outStart, outEnd;
        for (size_t i =0; i<inDims.size(); i++)
        {
            int32_t grid = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt32();
            outStart.push_back( inDims[i].getStartMin() + grid * (inStart[i] - inDims[i].getStartMin()) );
            outEnd.push_back( inDims[i].getStartMin() + grid * (inEnd[i] - inDims[i].getStartMin()) + grid - 1 );
        }

        return PhysicalBoundaries(outStart, outEnd, inputBoundaries[0].getDensity());
    }

    virtual void inspectLogicalOp(LogicalOperator const& lop)
    {
        // Extract scale factors for use here on the coordinator.
        LogicalXgrid const& lox = dynamic_cast<LogicalXgrid const&>(lop);
        _scaleFactors = lox.getScaleFactors();

        // Encode the scale factors into my "control cookie" so that
        // non-coordinator instances can also know them.
        if (!_scaleFactors.empty()) {
            stringstream ss;
            for (int64_t i : _scaleFactors) {
                ss << ' ' << i;
            }
            setControlCookie(ss.str());
        }
    }

    //TODO:OPTAPI check negative case
    /***
     * Xgrid is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(
        vector< std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        // On workers, decode scale factors from control cookie.
        if (!query->isCoordinator() && !getControlCookie().empty()) {
            SCIDB_ASSERT(_scaleFactors.empty());
            istringstream iss(getControlCookie());
            string token;
            while (iss >> token) {
                _scaleFactors.push_back(boost::lexical_cast<LogicalXgrid::ScaleFactor>(token));
            }
        }
            
        assert(inputArrays.size() == 1);
        assert(_parameters.size() == _schema.getDimensions().size());
        assert(_scaleFactors.empty() || _scaleFactors.size() == _schema.getDimensions().size());
        return XgridArray::create(_schema, _scaleFactors, inputArrays[0]);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalXgrid, "xgrid", "physicalXgrid")

}  // namespace scidb
