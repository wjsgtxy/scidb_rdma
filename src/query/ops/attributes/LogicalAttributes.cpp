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
 * @file LogicalAttributes.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Get list of persistent array attributes
 */

#include <query/LogicalOperator.h>

#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: attributes().
 *
 * @par Synopsis:
 *   attributes( srcArray )
 *
 * @par Summary:
 *   Produces a 1D result array where each cell describes one attribute of the source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   name: string
 *   <br>   type_id: string
 *   <br>   nullable: bool
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#attributes less 1, chunk interval=#attributes.
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - attributes(A) <name:string, type_id:string, nullable:bool> [No] =
 *     <br> No,   name,    type_id, nullable
 *     <br> 0, "quantity", "uint64", false
 *     <br> 1,   "sales",  "double", false
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalAttributes: public LogicalOperator
{
public:
    LogicalAttributes(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_ARRAY_NAME).setAllowVersions(true))
            }
        };
        return &argSpec;
    }

    void inferAccess(const shared_ptr<Query>& query) override
    {
        // Need catalog read lock.
        LogicalOperator::inferAccess(query);

        // Need to read (the metadata of) the array in the namespace.
        string ns, ary;
        string const& objName = param<OperatorParamReference>(0)->getObjectName();
        query->getNamespaceArrayNames(objName, ns, ary);
        query->getRights()->upsert(rbac::ET_NAMESPACE, ns, rbac::P_NS_READ);
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, shared_ptr<Query> query) override
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);

        const string &arrayNameOrig = param<OperatorParamReference>(0)->getObjectName();

        string arrayName;
        string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

        ArrayDesc arrayDesc;
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        args.arrayName = arrayName;
        args.nsName = namespaceName;
        args.catalogVersion = arrayId;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        Attributes attributes;
        attributes.push_back(AttributeDesc("name", TID_STRING, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("type_id", TID_STRING, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("nullable", TID_BOOL, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);
        size_t nAttrs = arrayDesc.getAttributes(true).size();
        size_t end    = nAttrs>0 ? nAttrs-1 : 0;
        dimensions[0] = DimensionDesc("No", 0, 0, end, end, nAttrs, 0);

        stringstream ss;
        ss << query->getInstanceID(); // coordinator instance
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("Attributes", attributes, dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAttributes, "attributes")


} //namespace
