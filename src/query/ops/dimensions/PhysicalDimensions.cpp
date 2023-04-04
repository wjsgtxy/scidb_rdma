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
 * @file PhysicalDimensions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of DIMENSIONS operator for dimensioning data from text files
 */

#include <array/MemArray.h>
#include <array/TupleArray.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>


using namespace std;

namespace scidb
{

class PhysicalDimensions: public PhysicalOperator
{
public:
    PhysicalDimensions(std::string const& logicalName,
                       std::string const& physicalName,
                       Parameters const& parameters,
                       ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& inDist, size_t /*depth*/) const override
    {
        SCIDB_ASSERT(inDist.size() == 0);

        return _schema.getDistribution()->getDistType();
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDistrib,
                                                      std::vector< ArrayDesc> const& inputSchema) const
    {
        SCIDB_ASSERT(inputDistrib.empty());
        SCIDB_ASSERT(inputSchema.empty());
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);

        string const& objName =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::GetArrayDescArgs args;
        query->getNamespaceArrayNames(objName, args.nsName, args.arrayName);
        args.catalogVersion = query->getCatalogVersion(args.nsName, args.arrayName);
        args.result = &arrayDesc;
        args.versionId = LAST_VERSION;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        Coordinates lowBoundary = arrayDesc.getLowBoundary();
        Coordinates highBoundary = arrayDesc.getHighBoundary();
        Dimensions const& dims = arrayDesc.getDimensions();
        assert(dims.size() == size_t(_schema.getDimensions()[0].getChunkInterval()));

        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema, _arena);
        for (size_t i = 0, size = dims.size(); i < size; i++)
        {
            Value tuple[8];
            tuple[0].setData(dims[i].getBaseName().c_str(), dims[i].getBaseName().length() + 1);
            tuple[1] = Value(TypeLibrary::getType(TID_INT64));
            tuple[1].setInt64(dims[i].getStartMin());
            tuple[2] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[2].setUint64(dims[i].getLength());
            tuple[3] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[3].setInt64(dims[i].getRawChunkInterval());
            tuple[4] = Value(TypeLibrary::getType(TID_UINT64));
            tuple[4].setUint64(dims[i].getChunkOverlap());
            tuple[5] = Value(TypeLibrary::getType(TID_INT64));
            tuple[5].setInt64(lowBoundary[i]);
            tuple[6] = Value(TypeLibrary::getType(TID_INT64));
            tuple[6].setInt64(highBoundary[i]);
            tuple[7].setString(TID_INT64); //TODO-3667: remove type from dimensions output. NOTE: requires a lot of test changes

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

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDimensions, "dimensions", "physicalDimensions")

} //namespace
