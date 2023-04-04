/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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


#include <query/LogicalOperator.h>
#include <query/Query.h>

using namespace std;

namespace scidb
{

class LogicalLimit: public LogicalOperator
{
public:
    LogicalLimit(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // count
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))  // offset
                    })
                 })
              })
            },
            {"count", RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) },
            {"offset", RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) }
        };
        return &argSpec;
    }

    /**
     * output schema of limit is same as the input schema
     */
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query)
    {
        Parameter countParam = findKeyword("count");
        if (_parameters.size() >= 1)
        {
            ASSERT_EXCEPTION(!countParam, "Conflicting positional and keyword count parameters!");
        }
        Parameter offsetParam = findKeyword("offset");
        if (_parameters.size() >= 2) {
            ASSERT_EXCEPTION(!offsetParam, "Conflicting positional and keyword offset parameters!");
        }

        if (_parameters.size() == 0 && !countParam)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "limit() operator requires at least one parameter.";
        }

        ArrayDesc res(
            schemas[0].getName(),
            schemas[0].getAttributes(),
            schemas[0].getDimensions(),
            schemas[0].getDistribution(),
            query->getDefaultArrayResidency());
        return res;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLimit, "limit")

}
// end namespace scidb
