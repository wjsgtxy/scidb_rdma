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
 * AggregatePartitioningOperator.cpp     // TODO: change to PhysicalAggregatePartitioningOperator.ccp
 */

#include <boost/scope_exit.hpp>
#include <log4cxx/logger.h>
#include <unordered_map>

#include <query/PhysicalOperator.h>
#include <query/AutochunkFixer.h>
#include <util/arena/Vector.h>

#include <array/MemArray.h>
#include <array/TileIteratorAdaptors.h>
#include <query/Aggregate.h>
#include <array/DelegateArray.h>
#include <system/Sysinfo.h>

#include "Aggregator.h"     // TODO: change to PhysicalAggregatePartitioningOperator.h

namespace scidb {

using namespace arena;
static log4cxx::LoggerPtr aggLogger(log4cxx::Logger::getLogger("scidb.qproc.aggregator"));


FinalResultChunkIterator::FinalResultChunkIterator (DelegateChunk const* sourceChunk,
                                                    int iterationMode,
                                                    AggregatePtr const& agg)
:
    DelegateChunkIterator(sourceChunk, iterationMode),
    _agg(agg->clone()),
    _outputValue(_agg->getResultType())
{}

Value & FinalResultChunkIterator::getItem()
{
    Value input = inputIterator->getItem();
    _agg->finalResult(_outputValue, input);
    return _outputValue;
}

FinalResultMapCreator::FinalResultMapCreator(DelegateChunk const* sourceChunk, int iterationMode)
:
    DelegateChunkIterator(sourceChunk, iterationMode),
    CoordinatesMapper(*sourceChunk),
    _bm(NULL, 0)
{
    ConstChunk const& srcChunk = sourceChunk->getInputChunk();
    PinBuffer scope(srcChunk);
    //       NOTE: SDB-5853
    //       must use getConstData() not getData(), to read a Payload,
    //       otherwise getData() violates pincount preconditions.
    //       repo case was distinct_count4.test and mem-array-threshold=1
    ConstRLEPayload payload(static_cast<const char*>(srcChunk.getConstData()));
    ConstRLEPayload::iterator iter = payload.getIterator();
    while (!iter.end())
    {
        if(iter.isNull() && iter.getMissingReason()==0)
        {}
        else
        {
            RLEEmptyBitmap::Segment seg;
            seg._lPosition=iter.getPPos();
            seg._pPosition=iter.getPPos();
            seg._length = iter.getSegLength();
            _bm.addSegment(seg);
        }
        iter.toNextSegment();
    }
    _iter = _bm.getIterator();
    _boolValue.setBool(true);
    _coords.resize(sourceChunk->getArrayDesc().getDimensions().size());
    restart();
}

Value& FinalResultMapCreator::getItem()
{
    if(_iter.end())
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    return _boolValue;
}

bool FinalResultMapCreator::isEmpty()
{
    if(_iter.end())
    {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }

    return false;
}

Coordinates const& FinalResultMapCreator::getPosition()
{
    pos2coord(_iter.getLPos(), _coords);
    return _coords;
}

bool FinalResultMapCreator::setPosition(Coordinates const& pos)
{
    position_t p = coord2pos(pos);
    return _iter.setPosition(p);
}

EmptyFinalResultChunkIterator::EmptyFinalResultChunkIterator(DelegateChunk const* sourceChunk,
                                                             int iterationMode,
                                                             AggregatePtr const& agg)
:
    FinalResultMapCreator(sourceChunk,iterationMode),
    _agg(agg->clone()), _outputValue(_agg->getResultType())
{}

Value &EmptyFinalResultChunkIterator::EmptyFinalResultChunkIterator::getItem()
{
    inputIterator->setPosition(getPosition());
    Value input = inputIterator->getItem();
    _agg->finalResult(_outputValue, input);
    return _outputValue;
}

FinalResultArray::FinalResultArray (ArrayDesc const& desc, std::shared_ptr<Array> const& stateArray,
                                    std::vector<AggregatePtr> const& aggs, bool createEmptyMap)
:
    DelegateArray(desc, stateArray),
    _aggs(aggs),
    _createEmptyMap(createEmptyMap),
    _emptyMapScapegoat(desc.getAttributes().firstDataAttribute().getId())
{
    if(_createEmptyMap)
    {
        if(!desc.getEmptyBitmapAttribute())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "improper use of FinalResultArray";
        }

        for(AttributeID i =0, n=safe_static_cast<AttributeID>(desc.getAttributes(true).size());
            i<n;
            i++)
        {
            if (_aggs[i].get())
            {
                _emptyMapScapegoat=i;
                break;
            }
        }
    }
}

DelegateChunk* FinalResultArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
{
    return new DelegateChunk(*this, *iterator, attrID, false);
}

DelegateArrayIterator* FinalResultArray::createArrayIterator(const AttributeDesc& attrID) const
{
    if(_createEmptyMap && attrID.getId() == desc.getEmptyBitmapAttribute()->getId())
    {
        const auto& iaAttrs = getPipe(0)->getArrayDesc().getAttributes();
        auto emsIter = iaAttrs.find(_emptyMapScapegoat);
        SCIDB_ASSERT(emsIter != iaAttrs.end());
        return new DelegateArrayIterator(*this, attrID, getPipe(0)->getConstIterator(*emsIter));
    }
    return new DelegateArrayIterator(*this, attrID, getPipe(0)->getConstIterator(attrID));
}

DelegateChunkIterator* FinalResultArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
{
    AggregatePtr agg = _aggs[chunk->getAttributeDesc().getId()];
    if (agg.get())
    {
        if(_createEmptyMap)
        {
            return new EmptyFinalResultChunkIterator(chunk, iterationMode, agg);
        }
        else
        {
            return new FinalResultChunkIterator(chunk, iterationMode, agg);
        }
    }
    else if(_createEmptyMap && chunk->getAttributeDesc().isEmptyIndicator())
    {
        return new FinalResultMapCreator(chunk, iterationMode);
    }
    else
    {
        return new DelegateChunkIterator(chunk, iterationMode);
    }
}

AggregatePartitioningOperator::StateMap AggregatePartitioningOperator::newStateMap(Arena& a)
{
    // Note from Donghui Zhang:
    // The minimum number of buckets was chosen by Jonathon Bell in commit 99482bb with no comment.
    constexpr size_t MIN_NUM_BUCKETS = 11;
    using VT = typename StateMap::value_type;
    return StateMap(MIN_NUM_BUCKETS, StateMap::hasher(_outDims), StateMap::key_equal(_outDims),
                    arena::ScopedAllocatorAdaptor<VT>(arena::Allocator<VT>(&a)));
}

AggregatePartitioningOperator::AggregatePartitioningOperator(const std::string& logicalName,
                                                             const std::string& physicalName,
                                                             const Parameters& parameters,
                                                             const ArrayDesc& schema)
:
    PhysicalOperator(logicalName, physicalName, parameters, schema),
    _inDims (0),                           // set in iniitializeOp()
    _outDims(schema.getDimensions().size())
{
}

//
// inheritance
//
std::vector<DistType> AggregatePartitioningOperator::inferChildInheritances(DistType inherited, size_t numChildren) const
{
    SCIDB_ASSERT(numChildren==1);

    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::inferChildInheritances: calling base");
    auto result = PhysicalOperator::inferChildInheritances(inherited, numChildren);


    ASSERT_EXCEPTION(isPartition(result[0]),   // otherwise result too large: prevent silent incorrect result
                     "aggregate cannot be computed on non-partitioned data");
    SCIDB_ASSERT(isParameterless(result[0]));  // TODO: see if this can be relaxed
    SCIDB_ASSERT(not isDataframe(result[0]));    // otherwise ASSERT_EXCEPTION in
                                               // DataframeDistribution::getPrimaryChunkLocation

    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::inferChildInheritances: returns: "
                             << distTypeToStr(result[0]));
    return result;
}

//
// synthesis
//

bool AggregatePartitioningOperator::isOkForOutput(DistType distType) const
{
   if (not isPartition(distType)) { return false; }    // otherwise aggregates are too large
   if (isDataframe(distType)) { return false; }        // otherwise ASSERT_EXCEPTION in
                                                       // DataframeDistribution::getPrimaryChunkLocation
   return true;
}

DistType AggregatePartitioningOperator::inferSynthesizedDistType(std::vector<DistType> const& inDist,
                                                                 size_t depth) const
{
    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::inferSynthesizedDistType: "
                             << "depth "<< depth
                             << "inherited dist"      << distTypeToStr(getInheritedDistType())
                             << "_schema dist" << distTypeToStr(_schema.getDistribution()->getDistType()));

    // Try to use what the child synthesized.  If that's not acceptable, change to something that is
    auto result = inDist[0];
    if (not isPartition(result) ||       // otherwise aggregates are too large
        isDataframe(result)     ||       // otherwise ASSERT_EXCEPTION in
                                         // DataframeDistribution::getPrimaryChunkLocation
        not isValidForDistType(result, _schema.getDimensions())) { // otherwise ASSERT_EXCEPTION in
                                         // e.g. ColCylicDistribution::getPrimaryChunkLocation

        result = defaultDistType();   // a better name would be defaultDistTypePartition()
    }
    SCIDB_ASSERT(isPartition(result));      // check: it is a partition

    // update schema if it is no longer valid
    if (_schema.getDistribution()->getDistType() != result) {
        // changing requires creation, which requires parameterless
        if ( !isParameterless(result)) {
            result = defaultDistType();
        }
        _schema.setDistribution(createDistribution(result));
    }
    // postconditions
    SCIDB_ASSERT(isPartition(result));      // check: it is a partition
    SCIDB_ASSERT(result == _schema.getDistribution()->getDistType());
    SCIDB_ASSERT(not isDataframe(result));    // otherwise ASSERT_EXCEPTION in
                                            // DataframeDistribution::getPrimaryChunkLocation

    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::inferSynthesizedDistType: returns: "
                              << distTypeToStr(result));
    return result;  // retrieveable later with getSynthesizedDistType();
}

DistributionRequirement
AggregatePartitioningOperator::getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
{
    SCIDB_ASSERT(inputSchemas.size()==1);
    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::getDistributionRequirement()"
                             << " inputSchemas DistType"
                             << distTypeToStr(inputSchemas[0].getDistribution()->getDistType()));

    auto requiredDistType = getSynthesizedDistType();   // what we promised to produce
    // preconditions
    SCIDB_ASSERT(requiredDistType == _schema.getDistribution()->getDistType());
    SCIDB_ASSERT(isPartition(requiredDistType));   // othewise aggregates are too large
    SCIDB_ASSERT(not isDataframe(requiredDistType)); // if input is a dataframe, ASSERT_EXCEPTION in
                                                   // DataframeDistribution::getPrimaryChunkLocation

    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::getDistributionRequirement()"
                             << " result: " << distTypeToStr(requiredDistType)
                             << " SpecificAnyOrder");

    std::vector<RedistributeContext> redistContexts;
    redistContexts.push_back(RedistributeContext(createDistribution(requiredDistType),
                                                 _schema.getResidency()));
    return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, redistContexts);
}

RedistributeContext AggregatePartitioningOperator::getOutputDistribution(std::vector<RedistributeContext> const&,
                                                                         std::vector<ArrayDesc> const&) const
{
    std::shared_ptr<Query> query(_query);
    SCIDB_ASSERT(query);

    SCIDB_ASSERT(query->getDefaultArrayResidency()->isEqual(_schema.getResidency()));

    LOG4CXX_TRACE(aggLogger, "AggregatePartitioningOperator::getOutputDistribution "
                             " getSynthesizedDistType() " << distTypeToStr(getSynthesizedDistType())
                             << " _schema.getDistribution()->getDistType() "
                             << distTypeToStr(_schema.getDistribution()->getDistType()));

    // Aggregates would over-count unless the data is a partition (vs a replication)
    auto synthType = getSynthesizedDistType();
    SCIDB_ASSERT(isPartition(synthType));      // check: it is a partition
    SCIDB_ASSERT(isParameterless(synthType));  // check: it is parameter-free
    _schema.setDistribution(createDistribution(synthType));

    return RedistributeContext(_schema.getDistribution(),
                               _schema.getResidency());
}

// a bit too long, eh?
void AggregatePartitioningOperator::initializeOperator(ArrayDesc const& inputSchema)
{
    assert(_aggs.empty());
    const_cast<size_t&>(_inDims) = inputSchema.getDimensions().size();
    _aggs.resize(_schema.getAttributes().size());
    AggIOMapping countMapping;

    bool countStar = false;
    AttributeID attID = 0;
    for (size_t i =0, n=_parameters.size(); i<n; i++) {
        if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL) {
            std::shared_ptr <OperatorParamAggregateCall>const& ac =
                (std::shared_ptr <OperatorParamAggregateCall> const&) _parameters[i];
            AttributeDesc inAttributeId;
            AggregatePtr agg = resolveAggregate(ac, inputSchema.getAttributes(), &inAttributeId);
            _aggs[attID] = agg;

            if (inAttributeId == AttributeDesc()) {
                //this is for count(*) - set it aside in the countMapping pile
                countStar = true;
                countMapping.push_back(attID, agg);
            } else {
                //is anyone else scanning inAttributeId?
                size_t k, kn;
                for(k=0, kn=_ioMappings.size(); k<kn; k++) {
                    if (inAttributeId.getId() == _ioMappings[k].getInputAttributeId()) {
                        _ioMappings[k].push_back(attID, agg);
                        break;
                    }
                }

                if (k == _ioMappings.size()) {
                    _ioMappings.push_back(AggIOMapping(inAttributeId.getId(), attID, agg));
                }
            }
            attID++;
        }
    }

    if (countStar) {
        //We have things in the countMapping pile - find an input for it
        int64_t minSize = -1;
        size_t j=0;
        if (!_ioMappings.empty()) {
            //We're scanning other attributes - let's piggyback on one of them (the smallest)
            for (size_t i=0, n=_ioMappings.size(); i<n; i++) {
                size_t attributeSize = inputSchema.getAttributes().findattr(_ioMappings[i].getInputAttributeId()).getSize();
                if (attributeSize > 0) {
                    if (minSize == -1 || minSize > (int64_t) attributeSize) {
                        minSize = attributeSize;
                        j = i;
                    }
                }
            }
            _ioMappings[j].merge(countMapping);
        } else {
            //We're not scanning other attributes - let'pick the smallest attribute out of the input
            int64_t minSize = -1;
            for (size_t i =0, n=inputSchema.getAttributes().size(); i<n; i++) {
                size_t attributeSize = inputSchema.getAttributes().findattr(i).getSize();
                if (attributeSize > 0 && !inputSchema.getAttributes().findattr(i).isEmptyIndicator()) {
                    if (minSize == -1 || minSize > (int64_t) attributeSize) {
                        minSize = attributeSize;
                        j = i;
                    }
                }
            }
            countMapping.setInputAttributeId(safe_static_cast<AttributeID>(j));
            _ioMappings.push_back(countMapping);
        }
    }
}

ArrayDesc AggregatePartitioningOperator::createStateDesc()
{
    Attributes outAttrs;
    for (AttributeID i=0, n=safe_static_cast<AttributeID>(_schema.getAttributes().size()); i < n; i++) {
        if (_schema.getEmptyBitmapAttribute() == NULL
            || _schema.getEmptyBitmapAttribute()->getId() != i) {
            Value defaultNull;
            defaultNull.setNull(0);
            outAttrs.push_back(AttributeDesc(_schema.getAttributes().findattr(i).getName(),
                                             _aggs[i]->getStateType().typeId(),
                                             AttributeDesc::IS_NULLABLE,
                                             CompressorType::NONE,
                                             std::set<std::string>(),
                                             &defaultNull, ""));
        }
    }

    return ArrayDesc(_schema.getName(), outAttrs, _schema.getDimensions(), _schema.getDistribution(),
                                                  _schema.getResidency(), _schema.getFlags());
}

void AggregatePartitioningOperator::initializeOutput(std::shared_ptr<ArrayIterator>& stateArrayIterator,
                                                     std::shared_ptr<ChunkIterator>& stateChunkIterator,
                                                     Coordinates const& outPos)
{
    Chunk& stateChunk = stateArrayIterator->newChunk(outPos);
    std::shared_ptr<Query> query(stateArrayIterator->getQuery());
    stateChunkIterator = stateChunk.getIterator(query);
}

void AggregatePartitioningOperator::setOutputPosition(std::shared_ptr<ArrayIterator>& stateArrayIterator,
                                                      std::shared_ptr<ChunkIterator>& stateChunkIterator,
                                                      Coordinates const& outPos)
{
    if (stateChunkIterator.get() == nullptr) {
        initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
    }

    if (!stateChunkIterator->setPosition(outPos)) {
        stateChunkIterator->flush();
        if (!stateArrayIterator->setPosition(outPos)) {
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
        } else {
            Chunk& stateChunk = stateArrayIterator->updateChunk();
            std::shared_ptr<Query> query(stateArrayIterator->getQuery());
            stateChunkIterator = stateChunk.getIterator(query, ChunkIterator::APPEND_CHUNK);
        }
        if (!stateChunkIterator->setPosition(outPos)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }
}

AggregationFlags AggregatePartitioningOperator::composeFlags(std::shared_ptr<Array> const& inputArray, AggIOMapping const& mapping)
{
    AttributeID inAttID = mapping.getInputAttributeId();
    AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes().findattr(inAttID);

    bool arrayEmptyable = (inputArray->getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool attributeNullable = inputAttributeDesc.isNullable();

    bool countOnly = true;
    bool readZeroes = false;
    bool readNulls = false;

    size_t const nAggs = mapping.size();

    //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
    for(size_t i =0; i<nAggs; i++) {
        AggregatePtr agg = mapping.getAggregate(i);
        if (agg->isCounting() == false) {
            countOnly = false;
            if(agg->ignoreZeroes() == false) {
                readZeroes = true;
            }
            if(agg->ignoreNulls() == false && attributeNullable) {
                readNulls = true;
            }
        } else {
            CountingAggregate* cagg = (CountingAggregate*) agg.get();
            if (cagg->needsAccumulate()) {
                countOnly = false;
            }
            if (arrayEmptyable) { //we can't infer count from shape
                readZeroes = true;
                if (cagg->ignoreNulls()==false && attributeNullable) { //nulls must be included in count
                    readNulls = true;
                }
            } else if (attributeNullable && cagg->ignoreNulls()) {
                readNulls=true; readZeroes = true;
            }
        }
    }

    std::vector<uint8_t> shapeCountOverride (nAggs,false);
    std::vector<uint8_t> nullBarrier(nAggs,false);

    for(size_t i =0; i<nAggs; i++) {
        AggregatePtr agg = mapping.getAggregate(i);
        if(readNulls && agg->ignoreNulls()) {
            nullBarrier[i] = true;
        }
        if (agg->isCounting()) {
            CountingAggregate* cagg = (CountingAggregate*) agg.get();
            if(!arrayEmptyable &&
               ((attributeNullable && cagg->ignoreNulls() == false) || !attributeNullable) ) {
                shapeCountOverride[i] = true;
            }
        }
    }

    AggregationFlags result;
    result.countOnly = countOnly;
    result.iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;
    if (!readNulls) {
        result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
    }
    if (!readZeroes && isDefaultFor(inputAttributeDesc.getDefaultValue(),inputAttributeDesc.getType())) {
        result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
    }
    result.nullBarrier=nullBarrier;
    result.shapeCountOverride=shapeCountOverride;
    return result;
}

AggregationFlags AggregatePartitioningOperator::composeGroupedFlags(std::shared_ptr<Array> const& inputArray,
                                                                    AggIOMapping const& mapping)
{
    AttributeID inAttID = mapping.getInputAttributeId();
    AttributeDesc const& inputAttributeDesc = inputArray->getArrayDesc().getAttributes().findattr(inAttID);

    bool attributeNullable = inputAttributeDesc.isNullable();

    bool countOnly = false;
    bool readZeroes = false;
    bool readNulls = false;

    size_t const nAggs = mapping.size();

    //first pass: set countOnly, iterateWithoutZeroes, iterateWithoutNulls
    for(size_t i =0; i<nAggs; i++) {
        AggregatePtr agg = mapping.getAggregate(i);
        if(agg->ignoreZeroes() == false)
        {   readZeroes = true; }
        if(agg->ignoreNulls() == false && attributeNullable)
        {   readNulls = true;  }
    }

    std::vector<uint8_t> shapeCountOverride (nAggs,false);
    std::vector<uint8_t> nullBarrier(nAggs,false);

    for(size_t i =0; i<nAggs; i++) {
        AggregatePtr agg = mapping.getAggregate(i);
        if(readNulls && agg->ignoreNulls())
        {   nullBarrier[i] = true;    }
    }

    AggregationFlags result;
    result.countOnly = countOnly;
    result.iterationMode = ConstChunkIterator::IGNORE_OVERLAPS;
    if (!readNulls) {
        result.iterationMode |= ConstChunkIterator::IGNORE_NULL_VALUES;
    }
    if (!readZeroes && isDefaultFor(inputAttributeDesc.getDefaultValue(),inputAttributeDesc.getType())) {
        result.iterationMode |= ConstChunkIterator::IGNORE_DEFAULT_VALUES;
    }
    result.nullBarrier=nullBarrier;
    result.shapeCountOverride=shapeCountOverride;
    return result;
}

void AggregatePartitioningOperator::grandCount(Array* stateArray, std::shared_ptr<Array> & inputArray,
                                               AggIOMapping const& mapping, AggregationFlags const& aggFlags)
{
    const auto& iaAttrs = inputArray->getArrayDesc().getAttributes();
    auto mappedAttrIDIter = iaAttrs.find(mapping.getInputAttributeId());
    SCIDB_ASSERT(mappedAttrIDIter != iaAttrs.end());
    std::shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(*mappedAttrIDIter);
    size_t nAggs = mapping.size();

    std::vector<uint64_t> counts(nAggs,0);
    bool dimBasedCount = true;
    for(size_t i=0; i<nAggs; i++) {
        if(aggFlags.shapeCountOverride[i] == false) {
            dimBasedCount = false;
            break;
        }
    }

    if (dimBasedCount) {
        while (!inArrayIterator->end()) {
            {
                ConstChunk const& chunk = inArrayIterator->getChunk();
                uint64_t chunkCount = chunk.getNumberOfElements(false);
                for (size_t i=0; i<nAggs; i++) {
                        counts[i]+=chunkCount;
                }
            }
            ++(*inArrayIterator);
        }
    } else {
        while (!inArrayIterator->end()) {
            {
                ConstChunk const& chunk = inArrayIterator->getChunk();
                uint64_t itemCount = 0;
                uint64_t noNullCount = 0;

                uint64_t chunkCount = chunk.getNumberOfElements(false);
                std::shared_ptr <ConstChunkIterator> inChunkIterator =
                    chunk.getConstIterator(aggFlags.iterationMode);
                while(!inChunkIterator->end()) {
                    Value const& v = inChunkIterator->getItem();
                    if(!v.isNull()) {
                            noNullCount++;
                    }
                    itemCount++;
                    ++(*inChunkIterator);
                }
                for (size_t i=0; i<nAggs; i++) {
                    if (aggFlags.shapeCountOverride[i]) {
                        counts[i]+=chunkCount;
                    }
                    else if (aggFlags.nullBarrier[i]) {
                        counts[i]+=noNullCount;
                    }
                    else {
                        counts[i]+=itemCount;
                    }
                }
            }
            ++(*inArrayIterator);
        }
    }

    Coordinates outPos(_outDims);
    for(size_t i =0; i<_outDims; i++) {
        outPos[i]=_schema.getDimensions()[i].getStartMin();
    }

    const auto& stateArrayAttrs = stateArray->getArrayDesc().getAttributes();
    for(size_t i =0; i<nAggs; i++) {
        auto mappedAttrIter = stateArrayAttrs.find(mapping.getOutputAttributeId(i));
        SCIDB_ASSERT(mappedAttrIter != stateArrayAttrs.end());
        std::shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(*mappedAttrIter);
        std::shared_ptr<ChunkIterator> stateChunkIterator;
        initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
        stateChunkIterator->setPosition(outPos);
        Value state;
        AggregatePtr agg = mapping.getAggregate(i);
        agg->initializeState(state);
        ((CountingAggregate*)agg.get())->overrideCount(state,counts[i]);
        stateChunkIterator->writeItem(state);
        stateChunkIterator->flush();
    }
}

void AggregatePartitioningOperator::grandTileAggregate(Array* stateArray,
                                                       std::shared_ptr<Array> & inputArray,
                                                       AggIOMapping const& mapping,
                                                       AggregationFlags const& aggFlags)
{
    const auto& iaAttrs = inputArray->getArrayDesc().getAttributes();
    auto mappedAttrIter = iaAttrs.find(mapping.getInputAttributeId());
    SCIDB_ASSERT(mappedAttrIter != iaAttrs.end());
    std::shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(*mappedAttrIter);
    size_t nAggs = mapping.size();
    std::vector<Value> states(nAggs);

    while (!inArrayIterator->end()) {
        {
            ConstChunk const& inChunk = inArrayIterator->getChunk();
            std::shared_ptr <ConstChunkIterator> inChunkIterator = inChunk.getConstIterator(
                ChunkIterator::TILE_MODE|aggFlags.iterationMode);
            while (!inChunkIterator->end()) {
                Value const &v = inChunkIterator->getItem();
                const RLEPayload *tile = v.getTile();
                if (tile->count()) {
                    for (size_t i=0; i<nAggs; i++) {
                        AggregatePtr agg = mapping.getAggregate(i);
                        agg->accumulateIfNeeded(states[i], tile);
                    }
                }

                ++(*inChunkIterator);
            }
        }
        ++(*inArrayIterator);
    }

    Coordinates outPos(_outDims);
    for(size_t i =0; i<_outDims; i++) {
        outPos[i]=_schema.getDimensions()[i].getStartMin();
    }

    const auto& stateArrayAttrs = stateArray->getArrayDesc().getAttributes();
    for(size_t i =0; i<nAggs; i++) {
        auto mappedAttrIter = stateArrayAttrs.find(mapping.getOutputAttributeId(i));
        SCIDB_ASSERT(mappedAttrIter != stateArrayAttrs.end());
        std::shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(*mappedAttrIter);
        std::shared_ptr<ChunkIterator> stateChunkIterator;
        initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
        stateChunkIterator->setPosition(outPos);
        stateChunkIterator->writeItem(states[i]);
        stateChunkIterator->flush();
    }
}

size_t AggregatePartitioningOperator::findEndOfRun(PointerRange<Coordinate* const> cv,size_t i)
{
    assert(i <= cv.size());

    const Coordinate* const runValue = cv[i];

    for (size_t n=cv.size(); i!=n; ++i) {
        if (!std::equal(cv[i],cv[i]+_outDims,runValue)) {
            return i;
        }
    }

    return cv.size();
}

/**
 * For each position in tile, compute corresponding output coordinates.
 */
void AggregatePartitioningOperator::computeOutputCoordinates(std::shared_ptr<BaseTile> const& tile,
                                                             PointerRange<Coordinate* const>     range)
{
    assert(range.size() == tile->size());

    // The positions tile returned from ...::getData() uses ArrayEncoding.
    Tile<Coordinates, ArrayEncoding>* cTile =
        safe_dynamic_cast<Tile<Coordinates, ArrayEncoding>* >(tile.get());

    Coordinates in(_inDims);

    for (size_t i=0,N=range.size(); i!=N; ++i) {
        cTile->at           (i, CoordinateRange(in));
        transformCoordinates(in,CoordinateRange(_outDims,range[i]));
    }
}

void AggregatePartitioningOperator::groupedTileFixedSizeAggregate(Array* stateArray,
                                                                  std::shared_ptr<Array> & inputArray,
                                                                  AggIOMapping const& mapping,
                                                                  AggregationFlags const& aggFlags,
                                                                  size_t attSize)
{
    const size_t VALUES_PER_TILE =
        Sysinfo::getCPUCacheSize(Sysinfo::CPU_CACHE_L1) / attSize;

    // Each aggregate will have its own array and chunk iterator.
    // (Note that the index into the AggIOMapping is *not*
    // necessarily equal to the outAttributeID... that's only true
    // if no count() function is involved!  I.e. you cannot assume
    // that mapping.getOutputAttributeId(x) == x.)
    //
    const size_t N_AGGS = mapping.size();
    mgd::vector <std::shared_ptr<ArrayIterator> > stateArrayIters(_arena,N_AGGS);
    mgd::vector <std::shared_ptr<ChunkIterator> > stateChunkIters(_arena,N_AGGS);
    const auto& stateArrayAttrs = stateArray->getArrayDesc().getAttributes();
    for (size_t i = 0; i < N_AGGS; ++i) {
        auto mappedAttrIter = stateArrayAttrs.find(mapping.getOutputAttributeId(i));
        SCIDB_ASSERT(mappedAttrIter != stateArrayAttrs.end());
        stateArrayIters[i] = stateArray->getIterator(*mappedAttrIter);
    }

    // Tiles to hold the input data, the input positions that
    // correspond to each of these data values, and a tile's worth
    // of positions in the OUTPUT, which correspond to each
    // position in the INPUT.
    //
    std::shared_ptr<BaseTile> dataTile;
    std::shared_ptr<BaseTile> inPositionsTile;

    // Input phase.  For each input chunk...
    const auto& inputArrayAttrs = inputArray->getArrayDesc().getAttributes();
    auto mappedAttrIter = inputArrayAttrs.find(mapping.getInputAttributeId());
    SCIDB_ASSERT(mappedAttrIter != inputArrayAttrs.end());
    std::shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(*mappedAttrIter);

    // Build a local 'scoped' arena from which to allocate all the storage
    // for our local data structures; we flush at the end of processing a
    // chunk..
    ArenaPtr localArena(newArena(Options("Aggregator").scoped(_arena,64*MiB)// ...adjust to taste
                                                      .threading(0)));      // ...single threaded

    while (!inArrayIterator->end()) {
        // Obtain tile mode input chunk iterator.
        ConstChunk const& chunk = inArrayIterator->getChunk();
        std::shared_ptr<ConstChunkIterator> rawInChunkIterator =
            chunk.getConstIterator(aggFlags.iterationMode);
        // Wrap the ordinary chunk iterator with a tile mode iterator.
        std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        std::shared_ptr<ConstChunkIterator> inChunkIterator =
            std::make_shared<TileConstChunkIterator<std::shared_ptr<ConstChunkIterator> > >(
                rawInChunkIterator, query);

        // Empty chunk?  Next!
        if (inChunkIterator->end()) {
            ++(*inArrayIterator);
            continue;
        }

        // For each tile in the chunk...
        Coordinates cursor = inChunkIterator->getPosition();

        // place the state map in its own nested scope to ensure that:
        // a) it is destroyed before we reset the 'local' arena
        // b) the boost scope exit mechanism can destroy each of the state
        //    values that it holds
        {
            StateMap outStateMap(newStateMap(*localArena));

            BOOST_SCOPE_EXIT(&outStateMap,&localArena,N_AGGS) {
                for(const StateMap::value_type& v : outStateMap) {
                    destroy(*localArena,v.second,N_AGGS);
                }
            } BOOST_SCOPE_EXIT_END

            while (!cursor.empty()) {
                // Get tile data and positions, and compute output positions.
                cursor = inChunkIterator->getData(cursor, VALUES_PER_TILE, dataTile, inPositionsTile);

                if (!dataTile || dataTile->empty()) {
                    assert(cursor.empty());
                    break;
                }

                const size_t TILE_SIZE = dataTile->size();
                mgd::vector<Coordinate*> outCoordinates(_arena,TILE_SIZE);

                for (size_t i=0; i!=TILE_SIZE; ++i) {
                    outCoordinates[i] = newOutCoords(*localArena);
                }

                computeOutputCoordinates(inPositionsTile,outCoordinates);

                // For each run of identical output coordinates...
                size_t runIndex = 0;
                size_t endOfRun = 0;
                while (endOfRun < TILE_SIZE) {
                    // Next run.
                    runIndex = endOfRun;
                    endOfRun = findEndOfRun(outCoordinates,runIndex);

                    // Find the States vector for this output position:
                    const Coordinate* const outCoords(outCoordinates[endOfRun - 1]);
                    Coordinates       const outCoordsV(outCoords,outCoords+_outDims); // because set Postion needs a vector

                    StateMap::iterator states = outStateMap.find(outCoords);

                    if (states == outStateMap.end()) {
                        // Need a new States vector with one entry per aggregate.
                        states = outStateMap.insert(std::make_pair(outCoords, newStateVector(*localArena,N_AGGS))).first;

                        // We also need to initialize each state entry from the state chunk iterator,
                        // since prior calls might have placed intermediate state there.
                        for (size_t ag = 0; ag < N_AGGS; ++ag) {
                            if (stateArrayIters[ag]) {
                                setOutputPosition(stateArrayIters[ag], stateChunkIters[ag], outCoordsV);
                                Value& state = states->second[ag];
                                state = stateChunkIters[ag]->getItem();
                            }
                        }
                    }

                    // Aggregate this run of data into the States vector.
                    for (size_t i=runIndex; i < endOfRun; ++i) {
                        Value v;
                        dataTile->at(i, v);

                        for (size_t ag = 0; ag < N_AGGS; ++ag) {
                            Value& state = states->second[ag];
                            mapping.getAggregate(ag)->accumulateIfNeeded(state, v);
                        }
                    }
                }
            }

            // Output phase.  Write out chunk's accumulated aggregate results.

            Coordinates coords(_outDims); // <-because SetPosition() still needs a vector...

            for (const StateMap::value_type& kv : outStateMap) {
                coords.assign(kv.first,kv.first + _outDims);

                for (size_t ag=0; ag != N_AGGS; ++ag) {
                    if (stateArrayIters[ag]) {
                        setOutputPosition(stateArrayIters[ag],
                                        stateChunkIters[ag],
                                        coords);
                        stateChunkIters[ag]->writeItem(kv.second[ag]);
                    }
                }
            }
        }

        localArena->reset();             // toss memory backing the state map

        ++(*inArrayIterator);
    }

    // Finally, for each aggregate, flush its chunk iterator:
    for (size_t i=0; i!=N_AGGS; ++i) {
        if (ChunkIterator* cIter = stateChunkIters[i].get()) {
            cIter->flush();
        }
    }
}

void AggregatePartitioningOperator::grandAggregate(Array* stateArray, std::shared_ptr<Array> & inputArray,
                                                   AggIOMapping const& mapping, AggregationFlags const& aggFlags)
{
        const auto& inputArrayAttrs = inputArray->getArrayDesc().getAttributes();
        auto mappedAttrIter = inputArrayAttrs.find(mapping.getInputAttributeId());
        SCIDB_ASSERT(mappedAttrIter != inputArrayAttrs.end());
        std::shared_ptr<ConstArrayIterator> inArrayIterator = inputArray->getConstIterator(*mappedAttrIter);
        size_t const nAggs = mapping.size();
        Value null;
        null.setNull(0);
        std::vector<Value> states(nAggs,null);
        int64_t chunkCount = 0;

        while (!inArrayIterator->end())
        {
            {
                ConstChunk const& inChunk = inArrayIterator->getChunk();
                chunkCount += inChunk.getNumberOfElements(false);
                std::shared_ptr <ConstChunkIterator> inChunkIterator =
                    inChunk.getConstIterator(aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    Value const &v = inChunkIterator->getItem();

                    for (size_t i =0; i<nAggs; i++)
                    {
                        AggregatePtr agg = mapping.getAggregate(i);
                        agg->accumulateIfNeeded(states[i], v);
                    }
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        Coordinates outPos(_outDims);
        for(size_t i =0, n=outPos.size(); i<n; i++)
        {
            outPos[i]=_schema.getDimensions()[i].getStartMin();
        }

        const auto& stateArrayAttrs = stateArray->getArrayDesc().getAttributes();
        for(size_t i =0; i<nAggs; i++)
        {
            auto mappedAttrIter = stateArrayAttrs.find(mapping.getOutputAttributeId(i));
            SCIDB_ASSERT(mappedAttrIter != stateArrayAttrs.end());
            std::shared_ptr<ArrayIterator> stateArrayIterator = stateArray->getIterator(*mappedAttrIter);
            std::shared_ptr<ChunkIterator> stateChunkIterator;
            initializeOutput(stateArrayIterator, stateChunkIterator, outPos);
            stateChunkIterator->setPosition(outPos);
            if(aggFlags.shapeCountOverride[i])
            {
                AggregatePtr agg = mapping.getAggregate(i);
                ((CountingAggregate*)agg.get())->overrideCount(states[i], chunkCount);
            }
            stateChunkIterator->writeItem(states[i]);
            stateChunkIterator->flush();
        }
}

void AggregatePartitioningOperator::groupedAggregate(Array* stateArray,
                          std::shared_ptr<Array> & inputArray,
                          AggIOMapping const& mapping,
                          AggregationFlags const& aggFlags)
{
        const auto& inputArrayAttrs = inputArray->getArrayDesc().getAttributes();
        auto mappedAttrIter = inputArrayAttrs.find(mapping.getInputAttributeId());
        SCIDB_ASSERT(mappedAttrIter != inputArrayAttrs.end());
        std::shared_ptr<ConstArrayIterator> inArrayIterator =
            inputArray->getConstIterator(*mappedAttrIter);
        size_t const nAggs = mapping.size();

        std::vector <std::shared_ptr<ArrayIterator> > stateArrayIterators(nAggs);
        const auto& stateArrayAttrs = stateArray->getArrayDesc().getAttributes();
        for (size_t i =0; i<nAggs; i++)
        {
            auto mappedAttrIter = stateArrayAttrs.find(mapping.getOutputAttributeId(i));
            SCIDB_ASSERT(mappedAttrIter != stateArrayAttrs.end());
            stateArrayIterators[i] = stateArray->getIterator(*mappedAttrIter);
        }
        std::vector <std::shared_ptr<ChunkIterator> > stateChunkIterators(nAggs);
        Coordinates outPos(_schema.getDimensions().size());
        while (!inArrayIterator->end())
        {
            {
                std::shared_ptr <ConstChunkIterator> inChunkIterator =
                    inArrayIterator->getChunk().getConstIterator( aggFlags.iterationMode);
                while (!inChunkIterator->end())
                {
                    transformCoordinates(inChunkIterator->getPosition(), outPos);
                    Value const &v = inChunkIterator->getItem();
                    //XXX: Yes this whole thing is over-engineered and needs to be simplified and
                    //adapted to new tile mode next release we hope...
                    for (size_t i =0; i<nAggs; i++)
                    {
                        size_t const aggNum = mapping.getOutputAttributeId(i);
                        setOutputPosition(stateArrayIterators[i], stateChunkIterators[i], outPos);
                        Value& state = const_cast<Value&>(stateChunkIterators[i]->getItem());
                        _aggs[aggNum]->accumulateIfNeeded(state, v);
                        stateChunkIterators[i]->writeItem(state);
                    }
                    ++(*inChunkIterator);
                }
            }
            ++(*inArrayIterator);
        }

        for (size_t i = 0; i <nAggs; i++)
        {
            if (stateChunkIterators[i].get())
            {
                stateChunkIterators[i]->flush();
            }
        }
}

void AggregatePartitioningOperator::logMapping(AggIOMapping const& mapping, AggregationFlags const& flags)
{
        LOG4CXX_DEBUG(aggLogger, "AggIOMapping input " << mapping.getInputAttributeId()
                      << " countOnly " << flags.countOnly
                      << " iterMode " << flags.iterationMode);

        for (size_t i=0, n=mapping.size(); i<n; i++)
        {
            LOG4CXX_DEBUG(aggLogger, ">>aggregate " << mapping.getAggregate(i)->getName()
                          << " outputatt " << mapping.getOutputAttributeId(i)
                          << " nullbarrier " << static_cast<bool>(flags.nullBarrier[i])
                          << " sco " << static_cast<bool>(flags.shapeCountOverride[i]));
        }
}

std::shared_ptr<Array> AggregatePartitioningOperator::execute(std::vector< std::shared_ptr<Array> >& inputArrays,
                                                              std::shared_ptr<Query> query)
{
        AutochunkFixer af(getControlCookie());
        af.fix(_schema, inputArrays);

        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        ArrayDesc const& inArrayDesc = inputArray->getArrayDesc();
        initializeOperator(inArrayDesc);

        ArrayDesc stateDesc = createStateDesc();
        std::shared_ptr<MemArray> stateArray = std::make_shared<MemArray>(stateDesc,query);


        if (_schema.getSize()==1)
        {
            for (size_t i=0; i<_ioMappings.size(); i++)
            {
                AggregationFlags aggFlags = composeFlags(inputArray, _ioMappings[i]);
                logMapping(_ioMappings[i],aggFlags);

                if (_tileMode)
                {
                    grandTileAggregate(stateArray.get(),inputArray, _ioMappings[i], aggFlags);
                }
                else
                {
                    if(aggFlags.countOnly)
                    {
                        grandCount(stateArray.get(), inputArray, _ioMappings[i], aggFlags);
                    }
                    else
                    {
                        grandAggregate(stateArray.get(),inputArray, _ioMappings[i], aggFlags);
                    }
                }
            }
        }
        else
        {
            for (size_t i=0, n=_ioMappings.size(); i<n; i++)
            {
                AggregationFlags aggFlags = composeGroupedFlags( inputArray, _ioMappings[i]);
                logMapping(_ioMappings[i], aggFlags);

                size_t attributeSize = inArrayDesc.getAttributes().findattr(_ioMappings[i].getInputAttributeId()).getSize();
                if (inArrayDesc.getAttributes().findattr(_ioMappings[i].getInputAttributeId()).getType() != TID_BOOL
                    && attributeSize > 0)
                {
                    groupedTileFixedSizeAggregate(stateArray.get(), inputArray,
                                                  _ioMappings[i], aggFlags, attributeSize);
                }
                else
                {
                    groupedAggregate(stateArray.get(), inputArray, _ioMappings[i], aggFlags);
                }
            }
        }

        // psLocal inputs will not work without a re-distribution to the desired output distribution
        // here they are disallowed ...
        auto synthType = getSynthesizedDistType();
        if (!isPartition(synthType) || !isParameterless(synthType)) {
            synthType = defaultDistType();
            SCIDB_ASSERT(isPartition(synthType));      // check: it is a partition
            SCIDB_ASSERT(isParameterless(synthType));  // check: it is parameter-free
        }
        std::shared_ptr<Array> input(stateArray);

        // TODO: redistributing to RandomAccess does not seem to be necessary
        // even though a redistribute is required to perform the aggregation.
        std::shared_ptr<Array> mergedArray = redistributeToRandomAccess(input,
                                                                        createDistribution(synthType),
                                                                        ArrayResPtr(), //default query residency
                                                                        query,
                                                                        shared_from_this(),
                                                                        _aggs);
        stateArray.reset();

        _schema.setDistribution(mergedArray->getArrayDesc().getDistribution());
        _schema.setResidency(mergedArray->getArrayDesc().getResidency());

        SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(createDistribution(synthType)));
        SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

        std::shared_ptr<Array> finalResultArray =
            std::make_shared<FinalResultArray>(_schema,
                                               mergedArray,
                                               _aggs,
                                               _schema.getEmptyBitmapAttribute());
        if (_tileMode)
        {
            return std::make_shared<MaterializedArray>(finalResultArray, query, MaterializedArray::RLEFormat);
        }
        return finalResultArray;
}

Coordinate* AggregatePartitioningOperator::newOutCoords(Arena& a)
{
    return newVector<Coordinate>(a,_outDims,manual);
}

Value* AggregatePartitioningOperator::newStateVector(Arena& a,count_t c)
{
    return newVector<Value>(a,c,manual);
}

} // namespace
