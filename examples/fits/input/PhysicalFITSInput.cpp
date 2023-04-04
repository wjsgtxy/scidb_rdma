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
 * @author miguel@spacebase.org
 *
 * @see LogicalFITSInput.cpp
 */

#include "FITSInputArray.h"

#include <query/PhysicalOperator.h>
#include <array/DelegateArray.h>
#include <query/Expression.h>

namespace scidb
{
using namespace std;


class PhysicalFITSInput: public PhysicalOperator
{
public:
    PhysicalFITSInput(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    uint32_t getHDU() const
    {
        if (_parameters.size() >= 3) {  // Arguments include HDU number
            std::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            return paramExpr->getExpression()->evaluate().getUint32();
        }
        return 0;                       // Otherwise, assume primary HDU
    }

    InstanceID getFileInstanceID(std::shared_ptr<Query>& query) const
    {
        if (_parameters.size() == 4) {      // Arguments include instance ID
            std::shared_ptr<OperatorParamPhysicalExpression> paramExpr = (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3];
            return paramExpr->getExpression()->evaluate().getUint64();
        }
        return query->getInstanceID();          // Otherwise, use current instance ID
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const&,
            std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        const string filePath = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getString();
        uint32_t hdu = getHDU();
        InstanceID fileInstanceID = getFileInstanceID(query);
        InstanceID myInstanceID = query->getInstanceID();

        std::shared_ptr<Array> result;
        if (fileInstanceID == myInstanceID) {   // This is the instance containing the file
            result = std::shared_ptr<Array>(new FITSInputArray(_schema, filePath, hdu, query));
            if (_schema.getEmptyBitmapAttribute() != NULL) {
                result = std::shared_ptr<Array>(new NonEmptyableArray(result));
            }
        } else {                        // Otherwise, return empty array
            result = std::shared_ptr<Array>(new MemArray(_schema,query));
        }

        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalFITSInput, "fits_input", "impl_fits_input");

}
