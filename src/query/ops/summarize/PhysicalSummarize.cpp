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

#include <array/MemArray.h>
#include <network/Network.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/TypeSystem.h>

#include <log4cxx/logger.h>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>


using namespace std;

namespace scidb
{

    struct SummaryTuple
    {
    private:
        friend class boost::serialization::access;
        template<class Archive>

        void serialize(Archive & ar, const unsigned int /*version*/)
        {
            ar & attName;
            ar & totalCount;
            ar & totalBytes;
            ar & numChunks;
            ar & minChunkCount;
            ar & maxChunkCount;
            ar & minChunkBytes;
            ar & maxChunkBytes;
        }

    public:
        string attName;
        size_t totalCount;
        size_t totalBytes;
        size_t numChunks;
        size_t minChunkCount;
        size_t maxChunkCount;
        size_t minChunkBytes;
        size_t maxChunkBytes;

        explicit SummaryTuple(string att = ""):
            attName(att),
            totalCount(0),
            totalBytes(0),
            numChunks(0),
            minChunkCount(std::numeric_limits<size_t>::max()),
            maxChunkCount(std::numeric_limits<size_t>::min()),
            minChunkBytes(std::numeric_limits<size_t>::max()),
            maxChunkBytes(std::numeric_limits<size_t>::min())
        {}
    };

    struct InstanceSummary
    {
        InstanceID myInstanceId;
        vector<SummaryTuple> summaryData;

        InstanceSummary(InstanceID iid,
                        vector<string> const& attNames):
            myInstanceId(iid),
            summaryData(attNames.size())
        {
            size_t i = 0;
            for (const auto& attName : attNames)
            {
                if (!attName.empty()) {
                    summaryData[i].attName = attName;
                    ++i;
                }
            }
        }
    };

    void addChunkData(AttributeID attId, size_t chunkBytes, size_t chunkCount, vector<SummaryTuple>& summaryData)
    {
        SummaryTuple& tuple = summaryData[attId];
        tuple.totalBytes += chunkBytes;
        tuple.totalCount += chunkCount;
        tuple.numChunks++;
        if(chunkCount < tuple.minChunkCount)
        {
            tuple.minChunkCount = chunkCount;
        }
        if(chunkCount > tuple.maxChunkCount)
        {
            tuple.maxChunkCount = chunkCount;
        }
        if(chunkBytes < tuple.minChunkBytes)
        {
            tuple.minChunkBytes = chunkBytes;
        }
        if(chunkBytes > tuple.maxChunkBytes)
        {
            tuple.maxChunkBytes = chunkBytes;
        }
    }

    shared_ptr<Array> toArray(ArrayDesc const& schema, shared_ptr<Query>& query, InstanceSummary const& summary, bool by_attr, size_t numInputAttr)
    {
        shared_ptr<Array> outputArray(new MemArray(schema, query));
        if (summary.summaryData.empty())
        {
            return outputArray;
        }

        Coordinates position(2,0);
        position[0] = summary.myInstanceId;
        static const size_t NUM_OUTPUT_ATTRIBUTES = schema.getAttributes(/*excludeEbm:*/true).size();
        vector<shared_ptr<ArrayIterator> > oaiters(NUM_OUTPUT_ATTRIBUTES);
        vector<shared_ptr<ChunkIterator> > ociters(NUM_OUTPUT_ATTRIBUTES);

        for (const auto& attr : schema.getAttributes(/*excludeEbm:*/true))
        {
            oaiters[attr.getId()] = outputArray->getIterator(attr);

            //populate empty tag from attr 0 implicitly
            int mode = ChunkIterator::SEQUENTIAL_WRITE;
            if (attr.getId()) {
                mode |= ChunkIterator::NO_EMPTY_CHECK;
            }

            ociters[attr.getId()] = oaiters[attr.getId()]->newChunk(position).getIterator(query, mode);
        }

        Value strbuf;
        Value buf;
        for(size_t i = 0; i < summary.summaryData.size(); ++i)
        {
            SummaryTuple const& t = summary.summaryData[i];
            strbuf.setString(t.attName);
            ociters[0]->setPosition(position);
            ociters[0]->writeItem(strbuf);
            buf.reset<uint64_t>(t.totalCount);
            ociters[1]->setPosition(position);
            ociters[1]->writeItem(buf);
            buf.setUint64(t.totalBytes);
            ociters[2]->setPosition(position);
            ociters[2]->writeItem(buf);
            buf.setUint64(t.numChunks);
            ociters[3]->setPosition(position);
            ociters[3]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                buf.setUint64(t.minChunkCount);
            }
            ociters[4]->setPosition(position);
            ociters[4]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                double avgChunkCount = static_cast<double>(t.totalCount) * 1.0 / static_cast<double>(t.numChunks);
                if(!by_attr)
                {
                    avgChunkCount = avgChunkCount * static_cast<double>(numInputAttr);
                }
                buf.setDouble(avgChunkCount);
            }
            ociters[5]->setPosition(position);
            ociters[5]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                buf.setUint64(t.maxChunkCount);
            }
            ociters[6]->setPosition(position);
            ociters[6]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                buf.setUint64(t.minChunkBytes);
            }
            ociters[7]->setPosition(position);
            ociters[7]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                buf.setDouble(static_cast<double>(t.totalBytes) * 1.0 / static_cast<double>(t.numChunks));
            }
            ociters[8]->setPosition(position);
            ociters[8]->writeItem(buf);

            if(t.numChunks == 0)
            {
                buf.setNull();
            }
            else
            {
                buf.setUint64(t.maxChunkBytes);
            }
            ociters[9]->setPosition(position);
            ociters[9]->writeItem(buf);

            position[1]++;
        }

        for(size_t oatt = 0; oatt < NUM_OUTPUT_ATTRIBUTES; ++oatt)
        {
            ociters[oatt]->flush();
        }

        return outputArray;
    }

    void sendSummaryData(shared_ptr<Query>& query, InstanceID const coordId, vector<SummaryTuple>& summaryData)
    {
        stringstream out;
        boost::archive::binary_oarchive oa(out);
        oa << summaryData;
        out.seekg(0, std::ios::end);
        size_t bufSize = out.tellg();
        out.seekg(0,std::ios::beg);
        string tmp = out.str();
        const char* cstr = tmp.c_str();
        shared_ptr<SharedBuffer> bufsend(new MemoryBuffer(cstr, bufSize));
        BufSend(coordId, bufsend, query);
        summaryData.clear();
    }

        void accumulate(SummaryTuple& outSummary, SummaryTuple const& inSummary, bool doCounts)
        {
            outSummary.numChunks += inSummary.numChunks;
            outSummary.totalBytes += inSummary.totalBytes;
            if(doCounts)
            {
                outSummary.totalCount += inSummary.totalCount;
                if(outSummary.maxChunkCount < inSummary.maxChunkCount)
                {
                    outSummary.maxChunkCount = inSummary.maxChunkCount;
                }
                if(outSummary.minChunkCount > inSummary.minChunkCount)
                {
                    outSummary.minChunkCount = inSummary.minChunkCount;
                }
            }
            if(outSummary.minChunkBytes > inSummary.minChunkBytes)
            {
                outSummary.minChunkBytes = inSummary.minChunkBytes;
            }
            if(outSummary.maxChunkBytes < inSummary.maxChunkBytes)
            {
                outSummary.maxChunkBytes = inSummary.maxChunkBytes;
            }
        }

    void accumulateChunkMetrics(SummaryTuple &summary, vector<SummaryTuple>const& summaryData)
    {
        for (size_t att = 0; att < summaryData.size(); ++att)
        {
            bool doCounts = (att == 0);
            accumulate(summary, summaryData[att], doCounts);
        }
    }

    void accumulateChunkMetricsByAtt(vector<SummaryTuple>& outSummaryData, vector<SummaryTuple>const& inSummaryData)
    {
        SCIDB_ASSERT(inSummaryData.size() == outSummaryData.size());
        for (unsigned att = 0; att < inSummaryData.size(); att++)
        {
            outSummaryData[att].attName = inSummaryData[att].attName;
            accumulate(outSummaryData[att], inSummaryData[att], true);
        }
    }

    std::vector<SummaryTuple> receiveSummaryData(InstanceID index, shared_ptr<Query>& query)
    {
        shared_ptr<SharedBuffer> buf = BufReceive(index, query);
        std::string bufstring((const char *)buf->getConstData(), buf->getSize());
        std::stringstream ssout;
        ssout << bufstring;
        boost::archive::binary_iarchive ia(ssout);
        std::vector<SummaryTuple> newlist;
        ia >> newlist;

        return newlist;
    }

    bool makeFinalSummary(ArrayDesc const& schema, shared_ptr<Query>& query, vector<SummaryTuple>& summaryDataVector, bool by_attr, bool by_inst)
    {
        InstanceID const myId    = query->getInstanceID();
        InstanceID const coordId = query->getCoordinatorID() == INVALID_INSTANCE ? myId : query->getCoordinatorID();
        size_t const numInstances = query->getInstancesCount();
        SummaryTuple outputSummaryData("all");


        if (by_inst)
        {
            if (by_attr)
            {
                // This case is covered by the default data setup in the calling execute() method.
            }
            else
            {
                accumulateChunkMetrics(outputSummaryData, summaryDataVector);
            }
        }
        else
        {
            if(myId != coordId)
            {
                sendSummaryData(query, coordId, summaryDataVector);
                return true;
            }

            accumulateChunkMetrics(outputSummaryData, summaryDataVector);
            for(InstanceID i = 0; i < numInstances; ++i)
            {
                if (i == myId)
                {
                    continue;
                }

                if (by_attr)
                {
                    std::vector<SummaryTuple> const recSummaryDataVector = receiveSummaryData(i, query);
                    accumulateChunkMetricsByAtt(summaryDataVector, recSummaryDataVector);
                }
                else
                {
                    std::vector<SummaryTuple> recSummaryDataVector = receiveSummaryData(i, query);
                    accumulateChunkMetrics(outputSummaryData, recSummaryDataVector);
                }
            }
        }
        if (!by_attr)
        {
            summaryDataVector.clear();
            summaryDataVector.push_back(outputSummaryData);
        }
        return true;
    }

    class PhysicalSummarize : public PhysicalOperator
    {
    private:
        bool _byAttribute;
        bool _byInstance;
        size_t _numInputAttributes;

    public:
        PhysicalSummarize(std::string const& logicalName,
                          std::string const& physicalName,
                          Parameters const& parameters,
                          ArrayDesc const& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema),
            _byAttribute(false),
            _byInstance(false),
            _numInputAttributes(0)
        {}

        // required to allow replicated input
        std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
        {
            SCIDB_ASSERT(numChildren == 1);
            vector<uint8_t> result(numChildren, true);
            return result;
        }

        /// @see OperatorDist
        DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
        {
            _schema.setDistribution(createDistribution(dtUndefined));
            return _schema.getDistribution()->getDistType();
        }

        /// @see PhysicalOperator
        RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& /*inputDistrib*/,
                                                  std::vector< ArrayDesc> const& /*inputSchemas*/) const override
        {
            _schema.setDistribution(createDistribution(dtUndefined));
            return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
        }

        std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
        {
            shared_ptr<Array>& inputArray = inputArrays[0];
            ArrayDesc const& inputSchema = inputArray->getArrayDesc();

            Parameter instanceParam = findKeyword("by_instance");

            if (instanceParam)
            {
                Value const& instanceVal = ((std::shared_ptr<OperatorParamPhysicalExpression>&)instanceParam)->getExpression()->evaluate();
                if (!instanceVal.isNull())
                {
                    _byInstance = instanceVal.getBool();
                }
            }

            Parameter attributeParam = findKeyword("by_attribute");
            if (attributeParam)
            {
                Value const& attributeVal = ((std::shared_ptr<OperatorParamPhysicalExpression>&)attributeParam)->getExpression()->evaluate();
                if (!attributeVal.isNull())
                {
                    _byAttribute = attributeVal.getBool();
                }
            }

            const auto& inputSchemaAttrs = inputSchema.getAttributes();
            _numInputAttributes = inputSchemaAttrs.size();
            vector<string> attNames(_numInputAttributes);
            vector<shared_ptr<ConstArrayIterator> > iaiters(_numInputAttributes);
            for (const auto& attr : inputSchemaAttrs)
            {
                attNames[attr.getId()] = attr.getName();
                iaiters[attr.getId()] = inputArray->getConstIterator(attr);
            }

            InstanceSummary summary(query->getInstanceID(), attNames);
            for (const auto& attr : inputSchemaAttrs)
            {
                while(!iaiters[attr.getId()]->end())
                {
                    ConstChunk const& chunk = iaiters[attr.getId()]->getChunk();
                    addChunkData(attr.getId(), chunk.getSize(), chunk.count(), summary.summaryData);
                    ++(*iaiters[attr.getId()]);
                }
            }

            makeFinalSummary(_schema, query, summary.summaryData, _byAttribute, _byInstance);
            return toArray(_schema, query, summary, _byAttribute, _numInputAttributes);
        }
    };

    // FRAGILE:
    // if you change 3rd arg, must update PhysicalQueryPlanNode::isSummarizeNode()
    DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSummarize, "summarize", "PhysicalSummarize")


} // end namespace scidb
