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
 * LogicalScan.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 */
#include <query/LogicalOperator.h>

#include <array/ArrayName.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logical_scan"));

/**
 * @brief The operator: scan().
 *
 * @par Synopsis:
 *   scan( srcArray [, ifTrim] )
 *
 * @par Summary:
 *   Produces a result array that is equivalent to a stored array.
 *
 * @par Input:
 *   - srcArray: the array to scan, with srcAttrs and srcDims.
 *   - ifTrim: whether to turn an unbounded array to a bounded array. Default value is false.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims (ifTrim=false), or trimmed srcDims (ifTrim=true).
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
class LogicalScan: public  LogicalOperator
{
public:
    LogicalScan(const std::string& logicalName, const std::string& alias):
                    LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_ARRAY_NAME).setAllowVersions(true)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))
                 })
              })
            }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);

        assert(!_parameters.empty());
        assert(_parameters.front()->getParamType() == PARAM_ARRAY_REF);

        const string& arrayNameOrig =
            ((std::shared_ptr<OperatorParamReference>&)_parameters.front())->getObjectName();
        SCIDB_ASSERT(isNameUnversioned(arrayNameOrig));

        ArrayDesc srcDesc;
        SystemCatalog::GetArrayDescArgs args;
        query->getNamespaceArrayNames(arrayNameOrig, args.nsName, args.arrayName);
        args.result = &srcDesc;
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        if (srcDesc.isTransient())
        {
            std::shared_ptr<LockDesc> lock(
                make_shared<LockDesc>(
                    args.nsName,
                    args.arrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    LockDesc::COORD,
                    LockDesc::XCL));
            std::shared_ptr<LockDesc> resLock(query->requestLock(lock));

            SCIDB_ASSERT(resLock);
            SCIDB_ASSERT(resLock->getLockMode() == LockDesc::XCL);
        }

        query->getRights()->upsert(rbac::ET_NAMESPACE, args.nsName, rbac::P_NS_READ);
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1 || _parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        std::shared_ptr<OperatorParamArrayReference>& arrayRef = (std::shared_ptr<OperatorParamArrayReference>&)_parameters[0];
        assert(arrayRef->getArrayName().empty() || isNameUnversioned(arrayRef->getArrayName()));
        assert(isNameUnversioned(arrayRef->getObjectName()));

        if (arrayRef->getVersion() == ALL_VERSIONS) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ASTERISK_USAGE2, _parameters[0]->getParsingContext());
        }

        ArrayDesc schema;
        const std::string &arrayNameOrig = arrayRef->getObjectName();

        SystemCatalog::GetArrayDescArgs args;
        query->getNamespaceArrayNames(arrayNameOrig, args.nsName, args.arrayName);
        args.catalogVersion = query->getCatalogVersion(args.nsName, args.arrayName);
        args.versionId = arrayRef->getVersion();
        args.throwIfNotFound = true;
        args.result = &schema;
        SystemCatalog::getInstance()->getArrayDesc(args);
        schema.addAlias(arrayNameOrig);
        schema.setNamespaceName(args.nsName);

        // Trim if the user wishes to.
        if (_parameters.size() == 2 // the user provided a true/false clause
            &&                       // and it's true
            evaluate(
                    ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                    TID_BOOL
                    ).getBool()
            )
        {
            schema.trim();

            // Without this change, harness test other.between_sub2 may fail.
            //
            // Once you trim the schema, the array is not the original array anymore.
            // Some operators, such as concat(), may go to the system catalog to find schema for input arrays if named.
            // We should make sure they do not succeed.
            schema.setName("");
        }

        SCIDB_ASSERT(not isUninitialized(schema.getDistribution()->getDistType()));
        SCIDB_ASSERT(not isUndefined(schema.getDistribution()->getDistType()));

        LOG4CXX_TRACE(logger, "LogicalScan::inferSchema: returning schema with ps = " << schema.getDistribution()->getDistType());
        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalScan, "scan")

} //namespace scidb
