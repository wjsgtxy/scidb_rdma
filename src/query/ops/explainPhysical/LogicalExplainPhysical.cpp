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
 * @file LogicalExplainPhysical.cpp
 *
 * @author poliocough@gmail.com
 *
 * explain_physical operator / Logical implementation.
 */

#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/ParsingContext.h>
#include <query/Query.h>
#include <query/UserQueryException.h>

namespace scidb
{

using namespace std;

/**
 * @brief The operator: explain_physical().
 *
 * @par Synopsis:
 *   explain_physical( query , language = 'aql' )
 *
 * @par Summary:
 *   Produces a single-element array containing the physical query plan.
 *
 * @par Input:
 *   - query: a query string.
 *   - language: the language string; either 'aql' or 'afl'; default is 'aql'
 *
 * @par Output array:
 *        <
 *   <br>   physical_plan: string
 *   <br> >
 *   <br> [
 *   <br>   No: start=end=0, chunk interval=1.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For internal usage.
 *
 */
class LogicalExplainPhysical: public LogicalOperator
{
public:
    LogicalExplainPhysical(const std::string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
        _usage = "explain_physical(<querystring> [,language]) language := 'afl'|'aql'";
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", RE(RE::LIST, {
                     RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),
                     RE(RE::QMARK, {
                        RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                     })
                  })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);

        Attributes attributes(1);
        attributes.push_back(AttributeDesc("physical_plan",  TID_STRING, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);

        if ( _parameters.size() != 1 && _parameters.size() != 2 )
        {
                std::shared_ptr< ParsingContext> pc;
                if (_parameters.size()==0) //need a parsing context for exception!
                {       pc = std::make_shared<ParsingContext>(); }
                else
                {       pc = _parameters[0]->getParsingContext(); }

                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_EXPLAIN_ERROR1,
                          pc);
        }

        string queryString =  evaluate(
                        ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                        TID_STRING).getString();
        // TODO: queryString is not used!

        if (_parameters.size() == 2)
        {
                string languageSpec =  evaluate(
                                ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                TID_STRING).getString();

                        if (languageSpec != "aql" && languageSpec != "afl")
                        {
                                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_EXPLAIN_ERROR2,
                                                _parameters[1]->getParsingContext());
                        }
        }

        dimensions[0] = DimensionDesc("No", 0, 0, 0, 0, 1, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("physical_plan", attributes, dimensions,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalExplainPhysical, "_explain_physical")

} //namespace
