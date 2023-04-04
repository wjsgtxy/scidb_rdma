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

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb {

class LogicalSummarize: public LogicalOperator
{
public:
    LogicalSummarize(const std::string& logicalName, const std::string& alias) :
            LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)),
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))
                    })
                 })
              })
            },
            {"by_instance", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            {"by_attribute", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) }
        };
        return &argSpec;
    }

    ArrayDesc getSchema(std::shared_ptr<Query>& query, size_t numInputAttributes)
    {
        size_t numInstances = query->getInstancesCount();
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("inst",  0, 0, numInstances-1,       numInstances-1,       1,                   0);
        dimensions[1] = DimensionDesc("attid", 0, 0, numInputAttributes-1, numInputAttributes-1, numInputAttributes, 0);

        Attributes attributes;
        attributes.push_back(AttributeDesc("att",
                                           TID_STRING,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("chunks",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("min_count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("avg_count",
                                           TID_DOUBLE,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("max_count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("min_bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("avg_bytes",
                                           TID_DOUBLE,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("max_bytes",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));

        // ArrayDesc consumes the new copy, source is discarded.
        return ArrayDesc("summarize",
                         attributes.addEmptyTagAttribute(),
                         dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
   }


    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        size_t numInputAttributes = inputSchema.getAttributes().size();
        return getSchema(query, numInputAttributes);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSummarize, "summarize")

} // end namespace scidb
