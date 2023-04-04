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
 * @file LogicalShow.cpp
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @brief Shows object, e.g. schema of array.
 */

#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/UserQueryException.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: show().
 *
 * @par Synopsis:
 *   show( schemaArray | schema | queryString [, 'aql' | 'afl'] )
 *
 * @par Summary:
 *   Shows the schema of an array.
 *
 * @par Input:
 *   - schemaArray | schema | queryString: an array where the schema is used, the schema itself or arbitrary query string
 *   - 'aql' | 'afl': Language specifier for query string
 * @par Output array:
 *        <
 *   <br>   schema: string
 *   <br> >
 *   <br> [
 *   <br>   i: start=end=0, chunk interval=1
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
class LogicalShow: public LogicalOperator
{
public:
    LogicalShow(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _usage = "show(<array name | anonymous schema | query string [, 'aql' | 'afl']>)";
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(RE::OR, {
                    RE(PP(PLACEHOLDER_SCHEMA)), // array name or schema definition
                    RE(RE::LIST, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),  // query string
                       RE(RE::QMARK, {
                          RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))  // afl or aql
                       })
                    })
                 })
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query) override
    {
        assert(inputSchemas.size() == 0);

        if (_parameters.size() == 2)
        {
            const std::shared_ptr<scidb::LogicalExpression> &queryString =
                ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[1])->getExpression();

            string lang = evaluate(queryString, TID_STRING).getString();
            std::transform(lang.begin(), lang.end(), lang.begin(), ::tolower);
            if (lang != "aql" && lang != "afl")
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_LANGUAGE_STRING,
                    _parameters[1]->getParsingContext());
            }
        }

        Attributes atts;
        atts.push_back(AttributeDesc("schema", TID_STRING, 0, CompressorType::NONE));
        atts.push_back(AttributeDesc("distribution", TID_STRING, AttributeDesc::IS_NULLABLE, CompressorType::NONE));

        // If the empty tag doesn't exist because the array doesn't
        // have one, then the attribute will be null.
        atts.push_back(AttributeDesc("etcomp",
                                     TID_STRING,
                                     AttributeDesc::IS_NULLABLE,
                                     CompressorType::NONE));

        Dimensions dims(1);
        dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("", atts, dims,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalShow, "show")

} //namespace
