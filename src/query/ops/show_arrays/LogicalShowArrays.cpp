/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file LogicalShowArrays.cpp
 */

#include "FilterArrays.h"

#include <query/LogicalOperator.h>

#include <query/Expression.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <sstream>
#include <vector>


namespace scidb
{

/**
 * @brief The operator: _show_arrays().
 *
 * @par Synopsis:
 *   _show_arrays(regularExpression, ignoreOrphanAttributes, ignoreVersions)
 *   requires namespaces root privileges
 *
 * @returns Summary:
 *   Returns an attribute of a table based on a regular expression
 *
 */
class LogicalShowArrays : public LogicalOperator
{
public:
    /**
     * see LogicalOperator
     */
    LogicalShowArrays(
        const std::string& logicalName,
        const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),  // regExNamespace
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),  // regExArray
                 RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)),  // ignoreOrphanAttributes
                 RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))  // ignoreVersions
              })
            }
        };
        return &argSpec;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        // Need database administrative powers.
        query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
    }

    /**
     * see LogicalOperator
     */
    ArrayDesc inferSchema(
            std::vector<ArrayDesc> schemas,
            std::shared_ptr< Query> query)
    {
        std::string regExNamespace = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[0])->getExpression(),
            TID_STRING).getString();

        std::string regExArray = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[1])->getExpression(),
            TID_STRING).getString();

        bool ignoreOrphanAttributes = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[2])->getExpression(),
            TID_BOOL).getBool();

        bool ignoreVersions = evaluate(
            ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[3])->getExpression(),
            TID_BOOL).getBool();

        const bool orderByName = true;

        std::vector<ArrayDesc> arrayDescs;
        SystemCatalog::getInstance()->getArrays(
            "", arrayDescs, ignoreOrphanAttributes, ignoreVersions, orderByName);
        filterArrays(arrayDescs, regExNamespace, regExArray);

        Attributes attributes;
        attributes.push_back(AttributeDesc("namespace", TID_STRING,
                                           AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        attributes.push_back(AttributeDesc("array", TID_STRING,
                                           AttributeDesc::IS_NULLABLE, CompressorType::NONE));

        int size = static_cast<int>(arrayDescs.size());
        int end  = size>0 ? size-1 : 0;

        std::stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(
            dtLocalInstance, DEFAULT_REDUNDANCY, ss.str());
        Dimensions dimensions(1,DimensionDesc("i", 0, 0, end, end, size, 0));
        // ArrayDesc consumes the new copy, source is discarded.
        return ArrayDesc("showArrays", attributes.addEmptyTagAttribute(), dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalShowArrays, "_show_arrays");

} // emd namespace p4
