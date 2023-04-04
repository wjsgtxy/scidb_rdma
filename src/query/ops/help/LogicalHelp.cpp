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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief This operator shows parameters of other operator
 */

#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/OperatorLibrary.h>
#include <query/Query.h>
#include <query/UserQueryException.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: help().
 *
 * @par Synopsis:
 *   help( operator )
 *
 * @par Summary:
 *   Produces a single-element array containing the help information for an operator.
 *
 * @par Input:
 *   - operator: the name of an operator.
 *
 * @par Output array:
 *        <
 *   <br>   help: string
 *   <br> >
 *   <br> [
 *   <br>   i: start=end=0, chunk interval=1.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalHelp: public LogicalOperator
{
public:
    LogicalHelp(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", RE(RE::QMARK, { RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) }) }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 0 || _parameters.size() == 1);

        if (_parameters.size() == 1)
        {
            const string &opName =
                evaluate(
                    ((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[0])->getExpression(),
                    TID_STRING).getString();

            try
            {
                OperatorLibrary::getInstance()->createLogicalOperator(opName);
            }
            catch (Exception &e)
            {
                throw CONV_TO_USER_QUERY_EXCEPTION(e, _parameters[0]->getParsingContext());
            }
        }

        Attributes atts;
        atts.push_back(AttributeDesc("help",  TID_STRING, 0, CompressorType::NONE));

        Dimensions dims(1);
        dims[0] = DimensionDesc("i", 0, 0, 0, 0, 1, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("Help",atts,dims,
                         localDist,
                         query->getDefaultArrayResidency());
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalHelp, "help")


} //namespace
