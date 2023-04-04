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

///
/// @file PhysicalSGCount.cpp
///

#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/ParsingContext.h>
#include <query/Query.h>
#include <system/Exceptions.h>

namespace scidb
{

using namespace std;

class LogicalSGCount: public LogicalOperator
{
public:
    LogicalSGCount(const std::string& logicalName, const std::string& alias)
    :   LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", RE(RE::LIST, {
                     RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),
                  })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        SCIDB_ASSERT(inputSchemas.size() == 0);

        Attributes attributes(1);
        attributes.push_back(AttributeDesc("$count",  TID_UINT64, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(1);

        dimensions[0] = DimensionDesc("$dummy", 0, 0, 0, 0, 1, 0);

        stringstream ss;
        ss << query->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        return ArrayDesc("sg_count", attributes, dimensions,
                         localDist, query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSGCount, "_sgcount")

} //namespace
