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
 * @brief Input operator for loading data from an external FITS file.
 *        The operator syntax is:
 *        FITS_INPUT(<Array>, <File Path>, [ <HDU Number> [ , <Instance ID> ]] )
 */
#include <query/LogicalOperator.h>

#include <array/ArrayName.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <system/Cluster.h>
#include <system/SystemCatalog.h>

namespace scidb
{
using namespace std;


class LogicalFITSInput: public LogicalOperator
{
public:
    LogicalFITSInput(const string& logicalName, const string& alias) :
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_ARRAY_NAME)), // Array
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)), // File path
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_UINT32)), //HDU Number
                    RE(RE::QMARK, {
                         RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) // Instance ID
                    })
                 })
               })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        if (_parameters.size() == 4) {  // Check for valid instance ID
            InstanceID instanceID = evaluate(
                dynamic_pointer_cast<OperatorParamLogicalExpression>(_parameters[3])->getExpression(),
                TID_UINT64).getUint64();
            if (instanceID >= query->getInstancesCount()) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_INSTANCE_ID, _parameters[3]->getParsingContext()) << instanceID;
            }
        }

        const string& arrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &arrayDesc;
        splitQualifiedArrayName(arrayName, args.nsName, args.arrayName);
        if (args.nsName.empty()) {
            args.nsName = query->getNamespaceName();
        }
        args.catalogVersion = query->getCatalogVersion(args.nsName, args.arrayName);
        args.throwIfNotFound = true;
        SystemCatalog::getInstance()->getArrayDesc(args);

        stringstream ss;
        ss << query->getInstanceID(); // coordinator instance
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        arrayDesc.setDistribution(localDist);
        arrayDesc.setResidency(query->getDefaultArrayResidency());

        return arrayDesc;
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFITSInput, "fits_input");

}
