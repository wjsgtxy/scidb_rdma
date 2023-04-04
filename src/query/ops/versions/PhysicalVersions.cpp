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
 * @file PhysicalVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of VERSIONS operator for versionsing data from text files
 */

#include <array/MemArray.h>
#include <array/TupleArray.h>
#include <array/VersionDesc.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>


using namespace std;

namespace scidb
{

class PhysicalVersions: public PhysicalOperator
{
public:
    PhysicalVersions(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & /*inputDist*/,
                                                      const std::vector< ArrayDesc> & /*inSchemas*/) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);

        const string &arrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SystemCatalog& sysCat = *SystemCatalog::getInstance();
        ArrayDesc arrayDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        query->getNamespaceArrayNames(arrayNameOrig, args.nsName, args.arrayName);
        const ArrayID catalogVersion = query->getCatalogVersion(args.nsName, args.arrayName);
        args.catalogVersion = catalogVersion;
        args.throwIfNotFound = true;
        sysCat.getArrayDesc(args);

        std::vector<VersionDesc> versions = sysCat.getArrayVersions(arrayDesc.getId());

        std::shared_ptr<TupleArray> tuples = std::make_shared<TupleArray>(_schema, _arena);
        for (size_t i = 0; i < versions.size(); ++i) {
            const VersionDesc& verDesc = versions[i];
            if (verDesc.getArrayID() > catalogVersion) {
                //XXX tigor: this is a HACK to allow concurrent readers & writers
                // instead, we should either remove this op or make getArrayVersions()
                // respect the catalog version (or smthn like that)
                break;
            }
            Value tuple[2];
            tuple[0] = Value(TypeLibrary::getType(TID_INT64));
            tuple[0].setInt64(verDesc.getVersionID());
            tuple[1] = Value(TypeLibrary::getType(TID_DATETIME));
            tuple[1].setDateTime(verDesc.getTimeStamp());

            tuples->appendTuple(tuple);
        }
        _result = tuples;
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
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

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalVersions, "versions", "physicalVersions")

} //namespace
