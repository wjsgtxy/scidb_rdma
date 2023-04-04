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
 * @file PhysicalAttributes.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of ATTRIBUTES operator for attributesing data from text files
 */


#include <array/MemArray.h>
#include <array/TupleArray.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

class PhysicalAttributes: public PhysicalOperator
{
public:
    PhysicalAttributes(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        assert(!_schema.isAutochunked());
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see OperatorDist
    virtual RedistributeContext getOutputDistribution(const vector<RedistributeContext> & /*inputDistrib*/,
                                                      const vector< ArrayDesc> & /*inputSchema*/) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);
        const string &arrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayDesc arrayDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.catalogVersion = query->getCatalogVersion(namespaceName, arrayName);
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        Attributes const& attrs = arrayDesc.getAttributes(true);
        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema, _arena);
        for (size_t i = 0; i < attrs.size(); i++) {
            Value tuple[3];
            tuple[0].setString(attrs.findattr(i).getName().c_str());
            tuple[1] = Value(TypeLibrary::getType(TID_STRING));
            tuple[1].setString(attrs.findattr(i).getType().c_str());
            tuple[2] = Value(TypeLibrary::getType(TID_BOOL));
            tuple[2].setBool(attrs.findattr(i).isNullable());  // DJG TODO update, this corresponds to EBM

            tuples->appendTuple(tuple);
        }
        _result = tuples;
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result)
        {
            _result = std::make_shared<MemArray>(_schema, query);
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAttributes, "attributes", "physicalAttributes")

} //namespace
