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
 * @brief Shows object. E.g. schema of array.
 */

#include <array/MemArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/QueryProcessor.h>
#include <util/OnScopeExit.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_show"));

class PhysicalShow: public PhysicalOperator
{
public:
    PhysicalShow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /// @see PhysicalOperator
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext>& /*inputDistrib*/,
                                                      const std::vector< ArrayDesc>& /*inSchemas*/) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    std::shared_ptr<ChunkIterator> getOutputChunkIter(const AttributeDesc& attr,
                                                      std::shared_ptr<Query> query)
    {
        std::shared_ptr<ArrayIterator> arrayIter = _result->getIterator(attr);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrayIter->newChunk(coords);
        std::shared_ptr<ChunkIterator> chunkIter = chunk.getIterator(query);
        return chunkIter;
    }

    void addResultSchema(const AttributeDesc& schemaAttr,
                         std::shared_ptr<Query> query,
                         const std::string& resultSchema)
    {
        auto chunkIt = getOutputChunkIter(schemaAttr, query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(resultSchema.c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    void addResultDistribution(const AttributeDesc& distributionAttr,
                               std::shared_ptr<Query> query,
                               DistType resultDistType)
    {
        auto chunkIt = getOutputChunkIter(distributionAttr, query);
        Value v(TypeLibrary::getType(TID_STRING));
        if (isStorable(resultDistType)) {
            // at this time, we are only exposing storable distributions
            v.setString(distTypeToStr(resultDistType));
        } else {
            // Exposing dynamically-used distributions ( !isStorable())
            // has no use for the user at this time, so it is hidden
            // to reduce the surface area of the query API.
            LOG4CXX_TRACE(logger, "PhysicalShow: null returned for distribution "
                          << distTypeToStr(resultDistType));
            v.setNull();
        }
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    void addEmptyTagCompression(const AttributeDesc& etCompAttr,
                                std::shared_ptr<Query> query,
                                CompressorType emptyTagCompression)
    {
        auto chunkIt = getOutputChunkIter(etCompAttr, query);
        Value v;
        if (emptyTagCompression != CompressorType::UNKNOWN) {
            auto etcStr = compressorTypeToString(emptyTagCompression);
            v.setString(etcStr.c_str());
        }
        // else the empty tag doesn't exist because the array doesn't
        // have one:  the attribute will be null for this position in the output.

        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        stringstream resultSchema;
        DistType resultDistType = dtUninitialized;
        CompressorType emptyTagCompression = CompressorType::UNKNOWN;

        if (_parameters[0]->getParamType() == PARAM_SCHEMA)
        {
            auto desc = ((const std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
            printSchema(resultSchema, desc);
            resultDistType = desc.getDistribution()->getDistType();
            auto emptyTag = desc.getEmptyBitmapAttribute();
            if (emptyTag) {
                emptyTagCompression = emptyTag->getDefaultCompressionMethod();
            }
        }
        else if (_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION)
        {
            string queryString =
                ((const std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])
                ->getExpression()->evaluate().getString();
            bool afl = false;
            if (_parameters.size() == 2)
            {
                string lang = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])
                        ->getExpression()->evaluate().getString();
                std::transform(lang.begin(), lang.end(), lang.begin(), ::tolower);
                afl = lang == "afl";
            }

            std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
            std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
                             query->getPhysicalCoordinatorID(),
                             query->mapLogicalToPhysical(query->getInstanceID()),
                             query->getCoordinatorLiveness());

            {
                arena::ScopedArenaTLS arenaTLS(innerQuery->getArena());

                innerQuery->attachSession(query->getSession());

                OnScopeExit fqd([&innerQuery] () { Query::destroyFakeQuery(innerQuery.get()); });

                innerQuery->queryString = queryString;

                queryProcessor->parseLogical(innerQuery, afl);
                auto desc = queryProcessor->inferTypes(innerQuery);
                printSchema(resultSchema, desc);
                resultDistType = desc.getDistribution()->getDistType();
            }
        }

        _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        const auto& attrs = _result->getArrayDesc().getAttributes();
        auto attrIter = attrs.begin();

        // place resultSchema into schema chunk
        addResultSchema(*attrIter, query, resultSchema.str());

        // place resultDistribution or null into distribution chunk
        addResultDistribution(*(++attrIter), query, resultDistType);

        // Include the empty tag compression.
        addEmptyTagCompression(*(++attrIter), query, emptyTagCompression);
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result) {
            _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalShow, "show", "impl_show")

} //namespace
