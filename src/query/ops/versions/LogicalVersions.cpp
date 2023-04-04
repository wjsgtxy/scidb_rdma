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

/**
 * @file LogicalVersions.cpp
 * @author knizhnik@garret.ru
 * @brief Get list of updatable array versions
 */

#include <query/LogicalOperator.h>

#include <array/VersionDesc.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{
/**
 * @brief The operator: versions().
 *
 * @par Synopsis:
 *   versions( srcArray )
 *
 * @par Summary:
 *   Lists all versions of an array in the database.
 *
 * @par Input:
 *   - srcArray: a source array.
 *
 * @par Output array:
 *        <
 *   <br>   version_id
 *   <br>   timestamp: a string describing the creation time of the version
 *   <br> >
 *   <br> [
 *   <br>   VersionNo: start=1, end=#versions, chunk interval=#versions
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalVersions: public LogicalOperator
{
public:
    LogicalVersions(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_ARRAY_NAME))
            }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        // Need catalog read lock and namespace read rights.
        LogicalOperator::inferAccess(query);
        string ns, ary;
        string objName(param<OperatorParamReference>(0)->getObjectName());
        query->getNamespaceArrayNames(objName, ns, ary);
        query->getRights()->upsert(rbac::ET_NAMESPACE, ns, rbac::P_NS_READ);
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query) override
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog& sysCat = *SystemCatalog::getInstance();
        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        query->getNamespaceArrayNames(arrayNameOrig, args.nsName, args.arrayName);
        const ArrayID catalogVersion = query->getCatalogVersion(args.nsName, args.arrayName);
        args.catalogVersion = catalogVersion;
        args.throwIfNotFound = true;
        sysCat.getArrayDesc(args);

        std::vector<VersionDesc> versions = sysCat.getArrayVersions(arrayDesc.getId());

        size_t nVersions = versions.size();
        for (size_t i = 0; i < versions.size(); ++i) {
            const VersionDesc& verDesc = versions[i];
            if (verDesc.getArrayID() > catalogVersion) {
                //XXX tigor: this is a HACK to allow concurrent readers & writers
                // instead, we should either remove this op or make getArrayVersions()
                // respect the catalog version (or smthn like that)
                nVersions = i;
                break;
            }
        }

        Attributes attributes;
        attributes.push_back(AttributeDesc("version_id", TID_INT64, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("timestamp", TID_DATETIME, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);

        if (nVersions == 0)
        {
            nVersions = 1; // clamp at 1 for one based indexing
        }

        dimensions[0] = DimensionDesc("VersionNo", 1, 1, nVersions, nVersions, nVersions, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(
            dtLocalInstance, DEFAULT_REDUNDANCY, ss.str());
        return ArrayDesc("Versions", attributes, dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalVersions, "versions")

} //namespace
