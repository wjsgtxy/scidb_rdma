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

#include <query/LogicalOperator.h>

namespace scidb
{

struct LogicalTrace : LogicalOperator
{
    LogicalTrace(const std::string& logicalName,
                 const std::string& alias)
            : LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.dataframe = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                     RE(PP(PLACEHOLDER_INPUT)),                 // input array
                     RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),  // filename
                 })
            }
        };
        return &argSpec;
     }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query)
    {
        return schemas[0];
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalTrace, "_trace")

}  // namespace scidb
