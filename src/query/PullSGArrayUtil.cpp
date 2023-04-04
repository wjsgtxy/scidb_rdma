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
 * @file PullSGArrayUtil.cpp
 *
 * @brief Helper class and routine implementations used in pull-SG
 */

#include "PullSGArrayUtil.h"

#include <array/MemArray.h>
#include <array/SinglePassAdaptor.h>
#include <array/UnPinner.h>

using namespace std;

namespace scidb
{

namespace sg
{

WriteChunkToArrayFunc::WriteChunkToArrayFunc(const std::shared_ptr<Array>& outputArray,
                                             std::set<Coordinates, CoordinatesLess>* newChunkCoords,
                                             bool enforceDataIntegrity)
: _outputArray(outputArray),
  _newChunkCoords(newChunkCoords),
  _outputIters(outputArray->getArrayDesc().getAttributes().size()),
  _enforceDataIntegrity(enforceDataIntegrity),
  _hasDataIntegrityIssue(false)
{ }

void WriteChunkToArrayFunc::operator() (const AttributeID attId,
                                        const ConstChunk& chunk,
                                        const std::shared_ptr<Query>& query)
{
        static const char* funcName = "WriteChunkToArrayFunc: ";

        LOG4CXX_TRACE(PullSGArray::_logger,  funcName
                      << "trying to consume chunk for attId="<<attId);

        ASSERT_EXCEPTION((attId == chunk.getAttributeDesc().getId()), funcName);

        if (!_outputIters[attId]) {

            assert(_outputIters.size() == chunk.getArrayDesc().getAttributes().size());
            const auto& attrs = chunk.getArrayDesc().getAttributes();
            auto attrDesc = attrs.find(attId);
            SCIDB_ASSERT(attrDesc != attrs.end());

            _outputIters[attrDesc->getId()] = _outputArray->getIterator(*attrDesc);
        }

        static const bool withoutOverlap = false;
        const Coordinates& chunkPosition = chunk.getFirstPosition(withoutOverlap);

        if(_newChunkCoords && attId == 0) {
            _newChunkCoords->insert(chunkPosition);
        }

        LOG4CXX_TRACE(PullSGArray::_logger,  funcName << "writing chunk of attId="<<attId
                      << " at pos="<<chunkPosition);

        // chunk position must be unique, so setPosition() must fail
        // except for MemArray, which creates an empty emptyBitmap chunk
        // when any attribute chunk is constructed
        // sigh ...
        if (_outputIters[attId]->setPosition(chunkPosition)) {

            // TODO references ebm relative to size()
            if (attId != (chunk.getArrayDesc().getAttributes(true).size())) {
                // not an emptyBitmapChunk
                if (_enforceDataIntegrity) {
                    throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_DUPLICATE_CHUNK_ADDR)
                    << CoordsToStr(chunkPosition);
                }
                if (!_hasDataIntegrityIssue) {
                    LOG4CXX_WARN(PullSGArray::_logger, funcName
                                 << "Received data chunk at position "
                                 << CoordsToStr(chunkPosition)
                                 << " for attribute ID = " << attId
                                 << " is duplicate or out of (row-major) order"
                                 << ". Add log4j.logger.scidb.qproc.pullsgarray=TRACE to the log4cxx config file for more");
                    _hasDataIntegrityIssue=true;
                } else {
                    LOG4CXX_TRACE(PullSGArray::_logger, funcName
                                  << "Received data chunk at position "
                                  << CoordsToStr(chunkPosition)
                                  << " for attribute ID = " << attId
                                  << " is duplicate or out of (row-major) order");
                }
            }

            if (!_enforceDataIntegrity) {
                Chunk& dstChunk = _outputIters[attId]->updateChunk();

                assert((chunk.getArrayDesc().getEmptyBitmapAttribute() == NULL)  ||
                       (chunk.getArrayDesc().getEmptyBitmapAttribute()->getId() == attId) ||
                       chunk.getBitmapSize()>0);

                assert((dstChunk.getArrayDesc().getEmptyBitmapAttribute() == NULL)  ||
                       (dstChunk.getArrayDesc().getEmptyBitmapAttribute()->getId() == attId) ||
                       dstChunk.getBitmapSize()>0);

                dstChunk.merge(chunk, query);
                LOG4CXX_TRACE(PullSGArray::_logger,  funcName
                              << "merged chunk of attId="<<attId
                              << " at pos="<<chunkPosition);
                return;
            }
        }

        if ( isDebug() &&
            (chunk.getArrayDesc().getEmptyBitmapAttribute() != NULL)  &&
            (chunk.getArrayDesc().getEmptyBitmapAttribute()->getId() == attId) ) {

            if (_outputIters[0] &&
                _outputIters[0]->setPosition(chunkPosition) &&
                _outputIters[0]->getChunk().getSize()>0 ) {
                verifyBitmap(_outputIters[0]->getChunk(), chunk);
            }
        }

        std::shared_ptr<ConstRLEEmptyBitmap> nullEmptyBitmap;
        size_t ebmSize(0);
        if (!_enforceDataIntegrity &&
            (ebmSize = chunk.getBitmapSize())>0 ) {
            // XXX tigor TODO:
            // This whole hacky business with the empty bitmap is to support the old behavior of redistribute()
            // which would just "merge" any colliding data.
            // The dstChunk.merge() call above would not work without sticking the emptybitmap into copyChunk() below.
            // This will also augment each (Mem)chunk by the size of the empty bitmap.
            // Once we make _enforceDataIntegrity==true by default, we should just stop supporing the old behavior,
            // and let the users shoot themselves in the foot if they so choose (by setting _enforceDataIntegrity=false).
            size_t off = chunk.getSize()-ebmSize;
            nullEmptyBitmap =
                std::make_shared<ConstRLEEmptyBitmap>(
                    reinterpret_cast<const char*>(chunk.getConstData()) + off);
        }
        _outputIters[attId]->copyChunk(chunk, nullEmptyBitmap);

        LOG4CXX_TRACE(PullSGArray::_logger,  funcName << "wrote chunk of attId="<<attId
                      << " of size="<<chunk.getSize()
                      << " at pos="<<chunkPosition
                      << " with desc="<<chunk.getArrayDesc());
}

void WriteChunkToArrayFunc::verifyBitmap(ConstChunk const& dataChunk, ConstChunk const& ebmChunk)
{
    assert(ebmChunk.getAttributeDesc().isEmptyIndicator());

    dataChunk.pin();
    UnPinner dataUP(static_cast<Chunk*>(const_cast<ConstChunk*>(&dataChunk)));

    std::unique_ptr<ConstRLEPayload> payload(new ConstRLEPayload(static_cast<const char*>(dataChunk.getConstData())));
    std::unique_ptr<ConstRLEEmptyBitmap> emptyBitmap(new ConstRLEEmptyBitmap(static_cast<const char*>(ebmChunk.getConstData())));
    assert(emptyBitmap->count()>0);
    assert(emptyBitmap->count() == payload->count());
}

const char* SerializedArray::SERIALIZED_DIM_NAME = "sErIaLaTtRiD";
const char* SerializedArray::SERIALIZED_ATTR_NAME = "SeRiAlVoIdAtTr";

SerializedArray::SerializedArray(std::shared_ptr<Array> const& array,
                                 AttributeOrdering const& attributeOrdering)
: StreamArray(getSingleAttributeDesc(array->getArrayDesc()), false),
  _inputArray(array),
  _attributeOrdering(attributeOrdering),
  _currAttribute(_attributeOrdering.begin()),
  _inputIterators(array->getArrayDesc().getAttributes().size())
{
    SCIDB_ASSERT(_inputIterators.size() >= _attributeOrdering.size());
}

ConstChunk const*
SerializedArray::nextChunk(const AttributeDesc& attId_in, MemChunk& chunk)
{
    auto attId = attId_in.getId();
    SCIDB_ASSERT(attId == 0);
    const auto currInputAttr = *_currAttribute;

    if (!_inputIterators[currInputAttr.getId()]) {
        _inputIterators[currInputAttr.getId()] = _inputArray->getConstIterator(currInputAttr);
    } else {
        ++(*_inputIterators[currInputAttr.getId()]);
    }

    if (_inputIterators[currInputAttr.getId()]->end()) {
        SCIDB_ASSERT(_currAttribute == _attributeOrdering.begin());
        return NULL;
    }

    ConstChunk const* inputChunk = &_inputIterators[currInputAttr.getId()]->getChunk();

    // adjust chunk coordinates
    _currChunk.initialize(this, inputChunk);

    // advance attribute iterator
    if (++_currAttribute == _attributeOrdering.end()) {
        _currAttribute = _attributeOrdering.begin();
    }

    // If the _inputArray is emptyable, and this currInputAttr is the largest attribute
    // id, then this must be the empty bitmap chunk.  Set the flag, so we'll know not to
    // make "closures" for this chunk.
    // TODO this is problematic because, when the empty indicator is renumbered to zero,
    // we'll be unable to tell if currInputAttr is the empty bitmap chunk.  I suppose we
    // can check to see if it is zero rather than the size of the attributes.
    if (_inputArray->getArrayDesc().getAttributes().hasEmptyIndicator()
        && ((_inputArray->getArrayDesc().getAttributes(true).size()) == currInputAttr.getId()))
    {
        _currChunk.setEmptyBitmapChunk(true);
    }

    return &_currChunk;
}

ArrayDesc
SerializedArray::getSingleAttributeDesc(const ArrayDesc& desc)
{
    ArrayDesc outDesc(desc);
    Dimensions outDims(desc.getDimensions());
    Attributes outAttributes;
    // TODO references ebm relative to size()
    outDims.push_back(DimensionDesc(SERIALIZED_DIM_NAME,
                                    0, desc.getAttributes().size()-1, 1, 0));
    outAttributes.push_back(AttributeDesc(SERIALIZED_ATTR_NAME,TID_VOID,0,CompressorType::NONE));

    outDesc.setDimensions(outDims);
    outDesc.setAttributes(desc.getEmptyBitmapAttribute() == NULL ?
                          outAttributes :
                          outAttributes.addEmptyTagAttribute());
    return outDesc;
}

void SerializedArray::toMultiAttribute(const Array* multiArray,
                                       const ConstChunk* singleAttrChunk,
                                       const ArrayDesc& multiDesc,
                                       MemChunk* multiAttrChunk,
                                       Address& tmpMemChunkAddr)
{
    SCIDB_ASSERT(singleAttrChunk);
    SCIDB_ASSERT(multiAttrChunk);

    LOG4CXX_TRACE(PullSGArray::_logger, "SerializedArray::toMultiAttribute(): "
                  << "attId=" << singleAttrChunk->getAttributeDesc().getId()
                  <<" chunk desc= " << singleAttrChunk->getArrayDesc()
                  <<" multi-attribute desc=" << multiDesc
                  <<" chunk pos=" << singleAttrChunk->getFirstPosition(false));

    SCIDB_ASSERT(singleAttrChunk->getAttributeDesc().getId() == 0);


    const Coordinates& firstPos = singleAttrChunk->getFirstPosition(false);

    SCIDB_ASSERT(firstPos.size() == multiDesc.getDimensions().size()+1);

    tmpMemChunkAddr.coords.clear();
    tmpMemChunkAddr.coords.reserve(multiDesc.getDimensions().size());

    // Ignore the last dimension (that is, the attributeId dimension)
    tmpMemChunkAddr.attId = AttributeID(firstPos.back());
    tmpMemChunkAddr.coords.insert(tmpMemChunkAddr.coords.begin(),
                                  firstPos.begin(),
                                  firstPos.begin()+multiDesc.getDimensions().size());

    multiAttrChunk->initialize(multiArray,
                               &multiDesc,
                               tmpMemChunkAddr,
                               singleAttrChunk->getCompressionMethod());
}

InstanceID
SerializedArray::instanceForChunk(const std::shared_ptr<Query>& query,
                                  const Coordinates& chunkPosition,
                                  const ArrayDesc& arrayDesc,
                                  const ArrayDesc& multiAttrDesc,
                                  Coordinates& multiAttrChunkPos,
                                  SGInstanceLocator& inputInstLocator)
{
    const size_t numMultiDims = multiAttrDesc.getDimensions().size();
    SCIDB_ASSERT(numMultiDims+1 == chunkPosition.size());
    multiAttrChunkPos.clear();
    multiAttrChunkPos.reserve(numMultiDims);
    multiAttrChunkPos.insert(multiAttrChunkPos.begin(),
                             chunkPosition.begin(),
                             chunkPosition.begin()+numMultiDims);
    const InstanceID destInstance = inputInstLocator(query,
                                                     multiAttrChunkPos,
                                                     multiAttrDesc);
    return destInstance;
}


// class SerializedChunkMerger

SerializedArray::SerializedChunkMerger::SerializedChunkMerger(const ArrayDesc& multiDesc,
                                                  const ArrayDesc& singleDesc,
                                                  PartialChunkMergerList* mergers,
                                                  PullSGArrayBlocking::ChunkHandler& handler,
                                                  bool enforceDataIntegrity)
: _multiDesc(multiDesc),
  _singleDesc(singleDesc),
  _multiAttId(INVALID_ATTRIBUTE_ID)
{
    SCIDB_ASSERT(handler);
    _handler.swap(handler);

    const size_t attNum = multiDesc.getAttributes().size();
    _chunkMergers.resize(attNum);
    SCIDB_ASSERT(attNum > 0);
    SCIDB_ASSERT(mergers == NULL ||  // It's okay not to provide mergers.
                 mergers->size() >= attNum);

    for (AttributeID attId=0; attId < attNum; ++attId) {
        // If there's a merger for a given attribute, then use it; otherwise, use
        // a default chunk merger for that attribute.
        if (mergers!=NULL && (*mergers)[attId]) {
            _chunkMergers[attId].swap((*mergers)[attId]);
        } else {
            _chunkMergers[attId] =
               std::make_shared<MultiStreamArray::DefaultChunkMerger>(enforceDataIntegrity);
        }
    }
    SCIDB_ASSERT(!isInitialized(_finalMultiDesc));
    SCIDB_ASSERT(isInitialized(_singleDesc));
    SCIDB_ASSERT(isInitialized(_multiDesc));
}

bool
SerializedArray::SerializedChunkMerger::mergePartialChunk(size_t stream,
                                                    AttributeID attId,
                                                    std::shared_ptr<MemChunk>& partialChunk,
                                                    const std::shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(PullSGArray::_logger, __PRETTY_FUNCTION__ << " start ");

    SCIDB_ASSERT(attId == 0);
    SerializedChunkMerger::toMultiAttribute(attId,
                                      _multiDesc,
                                      partialChunk.get(),
                                      _tmpMemChunkAddr,
                                      query);

    SCIDB_ASSERT(_multiAttId == INVALID_ATTRIBUTE_ID ||
                 _multiAttId == partialChunk->getAttributeDesc().getId());
    _multiAttId = partialChunk->getAttributeDesc().getId();

    SCIDB_ASSERT(_multiAttId < _chunkMergers.size());
    SCIDB_ASSERT(_chunkMergers[_multiAttId]);

    return _chunkMergers[_multiAttId]->mergePartialChunk(stream,
                                                         _multiAttId,
                                                         partialChunk,
                                                         query);
}

void
SerializedArray::SerializedChunkMerger::mergePartialChunks(AttributeID attId,
                                                           std::vector<std::shared_ptr<MemChunk>> chunksIn,
                                                           std::vector<size_t> streams,
                                                           std::shared_ptr<Query> const& query)
{
    LOG4CXX_TRACE(PullSGArray::_logger, __PRETTY_FUNCTION__);
    SCIDB_ASSERT(attId == 0);   // special to this class

    SCIDB_ASSERT(streams.size());
    SCIDB_ASSERT(chunksIn.size() == streams.size());


    // check attId and type of chunk match across all partials
    for(size_t iii=0; iii <chunksIn.size(); iii++) {
        SCIDB_ASSERT(chunksIn[iii].get());
        SCIDB_ASSERT(attId == chunksIn[iii]->getAttributeDesc().getId());
    }

    for(size_t iii=0; iii <chunksIn.size(); iii++) {
        SerializedChunkMerger::toMultiAttribute(attId,
                                                _multiDesc,
                                                chunksIn[iii].get(),
                                                _tmpMemChunkAddr,
                                                query);

        SCIDB_ASSERT(_multiAttId == INVALID_ATTRIBUTE_ID ||
                     _multiAttId == chunksIn[iii]->getAttributeDesc().getId());
        _multiAttId = chunksIn[iii]->getAttributeDesc().getId();

        SCIDB_ASSERT(_multiAttId < _chunkMergers.size());
        SCIDB_ASSERT(_chunkMergers[_multiAttId]);

        size_t partialCount = chunksIn[iii]->count();

        _chunkMergers[_multiAttId]->mergePartialChunk(streams[iii],
                                                      _multiAttId,
                                                      chunksIn[iii],
                                                      query);

        LOG4CXX_TRACE(PullSGArray::_logger, __func__ << "[" << iii << "]"
                                                     << " stream " << streams[iii] << " attId=" << attId );
        LOG4CXX_TRACE(PullSGArray::_logger, __func__
                                           << "+partialeCount"<< partialCount);
    }
}

std::shared_ptr<MemChunk>
SerializedArray::SerializedChunkMerger::getMergedChunk(AttributeID attId,
                                                 const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(attId == 0);
    SCIDB_ASSERT(_multiAttId < _chunkMergers.size());
    SCIDB_ASSERT(_chunkMergers[_multiAttId]);

    std::shared_ptr<MemChunk> chunk =
       _chunkMergers[_multiAttId]->getMergedChunk(_multiAttId, query);

    SCIDB_ASSERT(chunk->getAttributeDesc().getId() == _multiAttId);

    if (_multiDesc != chunk->getArrayDesc()) {
        if (isInitialized(_finalMultiDesc)) {
            ASSERT_EXCEPTION(_finalMultiDesc == chunk->getArrayDesc(),
                             "Chunk ArrayDescs dont match");
        } else {
            _finalMultiDesc = chunk->getArrayDesc();

            ArrayDistPtr outputArrayDist = _multiDesc.getDistribution();
            ArrayResPtr outputArrayRes = _multiDesc.getResidency();

            _finalMultiDesc.setDistribution(outputArrayDist);
            _finalMultiDesc.setResidency(outputArrayRes);

            SCIDB_ASSERT(isInitialized(_finalMultiDesc));
        }
    }

    toSingleAttribute(chunk->getAttributeDesc().getId(),
                      chunk.get(),
                      _singleDesc,
                      _tmpMemChunkAddr,
                      query);

    _multiAttId = INVALID_ATTRIBUTE_ID;
    return chunk;
}

void
SerializedArray::SerializedChunkMerger::handleChunk (const AttributeID attId,
                                               const ConstChunk& chunk,
                                               const std::shared_ptr<Query>& query)
{
    const char* funcName = "ChunkHandlerImpl::op():";
    SCIDB_ASSERT(attId == 0);
    MemChunk* memChunk = safe_dynamic_cast<MemChunk*>(const_cast<ConstChunk*>(&chunk));

    SerializedChunkMerger::toMultiAttribute(attId,
                                            (isInitialized(_finalMultiDesc) ? _finalMultiDesc : _multiDesc),
                                            memChunk,
                                            _tmpMemChunkAddr,
                                            query);
    const AttributeID multiAttId = chunk.getAttributeDesc().getId();

    LOG4CXX_TRACE(PullSGArray::_logger, funcName
                  << " delivering chunk attr="<<chunk.getAttributeDesc()
                  <<", pos="<< CoordsToStr(chunk.getFirstPosition(false))
                  <<", isEmpty="<< chunk.isEmpty());

    _handler(multiAttId, chunk, query);
    SerializedChunkMerger::toSingleAttribute(multiAttId,
                                       memChunk,
                                       _singleDesc,
                                       _tmpMemChunkAddr,
                                       query);
}

void
SerializedArray::SerializedChunkMerger::toSingleAttribute(AttributeID attId,
                                                    MemChunk* chunk,
                                                    const ArrayDesc& singleDesc,
                                                    Address& tmpMemChunkAddr,
                                                    const std::shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(PullSGArray::_logger,
                  "SerializedArray::SerializedChunkMerger::toSingleAttribute(): "
                  << "attId=" << attId
                  <<" chunk desc= " << chunk->getArrayDesc()
                  <<" single-attribute desc=" << singleDesc
                  <<" chunk pos=" << chunk->getFirstPosition(false));

    tmpMemChunkAddr.coords.clear();
    tmpMemChunkAddr.coords.reserve(singleDesc.getDimensions().size());

    const Coordinates& firstPos = chunk->getFirstPosition(false);

    SCIDB_ASSERT(firstPos.size() == singleDesc.getDimensions().size()-1);

    tmpMemChunkAddr.attId = 0;
    tmpMemChunkAddr.coords.insert(tmpMemChunkAddr.coords.begin(),
                                  firstPos.begin(),
                                  firstPos.end());
    tmpMemChunkAddr.coords.push_back(attId);

    chunk->initialize(&chunk->getArray(), //array pointer is unused
                      &singleDesc,
                      tmpMemChunkAddr,
                      chunk->getCompressionMethod());
}

void
SerializedArray::SerializedChunkMerger::toMultiAttribute(AttributeID attId,
                                                   const ArrayDesc& multiDesc,
                                                   MemChunk* chunk,
                                                   Address& tmpMemChunkAddr,
                                                   const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(attId == 0);

    SerializedArray::toMultiAttribute(&chunk->getArray(),
                                      chunk,
                                      multiDesc,
                                      chunk,
                                      tmpMemChunkAddr);
}

// class ChunkImpl

Coordinates const&
SerializedArray::ChunkImpl::getFirstPosition(bool withOverlap) const
{
    if (withOverlap) {
        SCIDB_ASSERT(_outputFirstPositionWOverlap.size() ==
                     getArrayDesc().getDimensions().size());
        return _outputFirstPositionWOverlap;
    }
    SCIDB_ASSERT(_outputFirstPosition.size() ==
                 getArrayDesc().getDimensions().size());
    return _outputFirstPosition;
}

Coordinates const&
SerializedArray::ChunkImpl::getLastPosition(bool withOverlap) const
{
    if (withOverlap) {
        SCIDB_ASSERT(_outputLastPositionWOverlap.size() ==
                     getArrayDesc().getDimensions().size());
        return _outputLastPositionWOverlap;
    }
    SCIDB_ASSERT(_outputLastPosition.size() ==
                 getArrayDesc().getDimensions().size());
    return _outputLastPosition;
}

void
SerializedArray::ChunkImpl::initialize(const Array* outputArray,
                                       const ConstChunk* inputChunk)
{
    SCIDB_ASSERT(outputArray);
    SCIDB_ASSERT(inputChunk);

    _emptyBitmapChunk = false;
    _outputArray = outputArray;
    _inputChunk = inputChunk;

    const AttributeID attrId = _inputChunk->getAttributeDesc().getId();

    _outputLastPosition = _inputChunk->getLastPosition(false);
    _outputLastPosition.push_back(attrId);

    SCIDB_ASSERT(_outputLastPosition.size() ==
                 getArrayDesc().getDimensions().size());

    _outputFirstPosition = _inputChunk->getFirstPosition(false);
    _outputFirstPosition.push_back(attrId);

    SCIDB_ASSERT(_outputFirstPosition.size() ==
                 getArrayDesc().getDimensions().size());

    _outputLastPositionWOverlap = _inputChunk->getLastPosition(true);
    _outputLastPositionWOverlap.push_back(attrId);

    SCIDB_ASSERT(_outputLastPositionWOverlap.size() ==
                 getArrayDesc().getDimensions().size());

    _outputFirstPositionWOverlap = _inputChunk->getFirstPosition(true);
    _outputFirstPositionWOverlap.push_back(attrId);

    SCIDB_ASSERT(_outputFirstPositionWOverlap.size() ==
                 getArrayDesc().getDimensions().size());
    SCIDB_ASSERT(_inputChunk->getFirstPosition(true).size()+1 ==
                 getArrayDesc().getDimensions().size());
}

ConstChunk const*
DeserializedArray::nextChunk(const AttributeDesc& attId, MemChunk& chunk)
{
    const auto currAttr = *_currAttribute;

    if (attId.getId() != currAttr.getId()) {
        LOG4CXX_ERROR(PullSGArray::_logger, "DeserializedArray::"<<__func__<< " got attId " << attId.getId() << " but expected cur= " << currAttr.getId());
    }
    ASSERT_EXCEPTION(attId == currAttr, "DeserializedArray::nextChunk(): Redistributed chunk consumed out of order");

    if (!_inputIterator) {
        const auto& fda = _inputArray->getArrayDesc().getAttributes().firstDataAttribute();
        _inputIterator = _inputArray->getConstIterator(fda.getId()); // start with first attribute
    } else {
        ++(*_inputIterator);
    }

    // advance attribute iterator
    if (++_currAttribute == _attributeOrdering.end()) {
        _currAttribute = _attributeOrdering.begin();
    }

    if (_inputIterator->end()) {
        return NULL;
    }

    ConstChunk const* inputChunk = &_inputIterator->getChunk();

    // adjust chunk coordinates
    deserialize(inputChunk, chunk);

    return &chunk;
}

void
DeserializedArray::deserialize(ConstChunk const* inputChunk, MemChunk& chunk)
{
    SerializedArray::toMultiAttribute(this,
                                      inputChunk,
                                      getArrayDesc(),
                                      &chunk,
                                      _tmpMemChunkAddr);
    chunk.setPayload(inputChunk);
}

// a/k/a getSerializedSinglePass array?
// why not make the caller cast to SinglePass before supplying, so that it is the
// caller that meets the contract?
std::shared_ptr<Array>
getSerializedArray(std::shared_ptr<Array>& inputArray,
                   AttributeOrdering& attributeOrdering,
                   const ArrayDistPtr& outputArrayDist,
                   const ArrayResPtr& outputArrayRes,
                   const std::shared_ptr<Query>& query,
                   const std::shared_ptr<PhysicalOperator>& phyOp,
                   bool enforceDataIntegrity)
{
    auto access = inputArray->getSupportedAccess();
    if (access == Array::SINGLE_PASS) {
        SinglePassArray* singlePass = dynamic_cast<SinglePassArray*>(inputArray.get());
        if (not singlePass) {
            // XXX The framework really wants SINGLE_PASS arrays to be
            // derived from SinglePassArray.  If not, somebody makes a
            // setPosition() call that throws because there's no
            // override (see SDB-6365).  For example, the
            // df_input_opaque test will fail: you cannot
            // save(project(flatten(...))) without it.  This needs
            // more investigation.  In the meantime, use the adaptor.
            inputArray = make_shared<SinglePassAdaptor>(inputArray, query);
            singlePass = dynamic_cast<SinglePassArray*>(inputArray.get());
            ASSERT_EXCEPTION(singlePass, "SinglePassAdaptor not derived from SinglePassArray?!");
        }
        singlePass->setEnforceHorizontalIteration(true);
    }

    const ArrayDesc& multiAttrDesc = inputArray->getArrayDesc();

    // Create a single-attribute array with an extra dimension.
    // The last dimension coordinate corresponds to the attribute ID in the input array.
    // attributeOrdering maps the input attribute ID
    // to the sequential coordinate in the extra dimension.

    SCIDB_ASSERT(!attributeOrdering.empty());
    SCIDB_ASSERT(attributeOrdering.size() <= multiAttrDesc.getAttributes().size());
    std::shared_ptr<Array> serializedArray = std::make_shared<SerializedArray>(inputArray, attributeOrdering);

    // create an instance locator that will use the non-serialized
    // (i.e. the input array) dimensions
    std::shared_ptr<Array> tmp;

    // Use type alias to disambiguate overloaded method when using std::bind.
    // @see https://stackoverflow.com/questions/10555566/difference-between-c11-stdbind-and-boostbind
    using Func1 = InstanceID (*)(const std::shared_ptr<Query>&,
                                 const Coordinates&,
                                 const ArrayDesc&,
                                 const ArrayDistPtr&,
                                 const ArrayResPtr&);
    using Func2 = InstanceID (*)(const std::shared_ptr<Query>&,
                                const Coordinates&,
                                const ArrayDesc&,
                                const ArrayDistPtr&);
    if (outputArrayRes &&
        !outputArrayRes->isEqual(query->getDefaultArrayResidency())) {
        // The desired residency is different from the query liveness set.
        // We need to do some extra work in order to figure out the logical
        // instance IDs wrt the query based on the desired residency.
        SGInstanceLocator serializedInstLocator(
            std::bind(&SerializedArray::instanceForChunk,
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3,
                      multiAttrDesc,
                      Coordinates(),
                      SGInstanceLocator(std::bind(
                                            static_cast<Func1>(&PullSGContext::instanceForChunk),
                                            std::placeholders::_1,
                                            std::placeholders::_2,
                                            std::placeholders::_3,
                                            outputArrayDist,
                                            outputArrayRes))));
        tmp = pullRedistribute(serializedArray,
                               outputArrayDist,
                               outputArrayRes,
                               query,
                               phyOp,
                               serializedInstLocator,
                               enforceDataIntegrity);
    } else {

        SGInstanceLocator serializedInstLocator(
            std::bind(&SerializedArray::instanceForChunk,
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3,
                      multiAttrDesc,
                      Coordinates(),
                      SGInstanceLocator(std::bind(static_cast<Func2>(&PullSGContext::instanceForChunk),
                                                  std::placeholders::_1,
                                                  std::placeholders::_2,
                                                  std::placeholders::_3,
                                                  outputArrayDist))));
        tmp = pullRedistribute(serializedArray,
                               outputArrayDist,
                               ArrayResPtr(), // default query residency
                               query,
                               phyOp,
                               serializedInstLocator,
                               enforceDataIntegrity);
    }

    if (tmp == serializedArray ) {
        SCIDB_ASSERT(!phyOp->getOperatorContext());
        return inputArray;
    }
    return tmp;
}

std::shared_ptr<Array>
redistributeWithCallbackInAttributeOrder(std::shared_ptr<Array>& inputArray,
                                         PullSGArrayBlocking::ChunkHandler& chunkHandler,
                                         PartialChunkMergerList* mergers,
                                         const ArrayDistPtr& outputArrayDist,
                                         const ArrayResPtr& outputArrayRes,
                                         const std::shared_ptr<Query>& query,
                                         const std::shared_ptr<PhysicalOperator>& phyOp,
                                         bool enforceDataIntegrity)
{
    LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " begin");

    const ArrayDesc& multiAttrDesc = inputArray->getArrayDesc();

    // Create a single-attribute array with an extra dimension.
    // The last coordinate corresponds to an attribute ID in the input array.
    // attributeOrdering maps the input attribute ID
    // to the sequential coordinate in the extra dimension.
    AttributeOrdering attributeOrdering;
    for (const auto& attr : multiAttrDesc.getAttributes()) {
        attributeOrdering.insert(attr);
    }
    SCIDB_ASSERT(!attributeOrdering.empty());

    std::shared_ptr<Array> serializedArray = getSerializedArray(inputArray,
                                                                attributeOrdering,
                                                                outputArrayDist,
                                                                outputArrayRes,
                                                                query,
                                                                phyOp,
                                                                enforceDataIntegrity);
    if (serializedArray == inputArray) {
        return inputArray;
    }

    PullSGArrayBlocking *arrayToPull = safe_dynamic_cast<PullSGArrayBlocking*>(serializedArray.get());
    SCIDB_ASSERT(arrayToPull);
    SCIDB_ASSERT(arrayToPull->getSupportedAccess()==Array::SINGLE_PASS);
    // TODO: are there any additional assertions possible?

    const ArrayDesc& singleAttrDesc = arrayToPull->getArrayDesc();
    SCIDB_ASSERT(singleAttrDesc.getAttributes().size()<=2);
    SCIDB_ASSERT(singleAttrDesc.getAttributes().size()>0);

    std::unordered_set<AttributeID> attributesToPull;
    attributesToPull.insert(0); // only one pseudo attribute

    ArrayDesc outputArrayDesc(multiAttrDesc);
    outputArrayDesc.setDistribution(outputArrayDist);
    if (outputArrayRes) {
        outputArrayDesc.setResidency(outputArrayRes);
    } else {
        outputArrayDesc.setResidency(query->getDefaultArrayResidency());
    }

    // create a chunk merger that will convert chunk coordinates
    // from the serialized to the input dimensions
    std::shared_ptr<SerializedArray::SerializedChunkMerger >
       handler(new SerializedArray::SerializedChunkMerger(outputArrayDesc,
                                                          singleAttrDesc,
                                                          mergers,
                                                          chunkHandler,
                                                          enforceDataIntegrity));
    std::shared_ptr<scidb::MultiStreamArray::PartialChunkMerger> merger(handler);
    arrayToPull->setPartialChunkMerger(0, merger);
    SCIDB_ASSERT(!merger);
    SCIDB_ASSERT(handler);

    PullSGArrayBlocking::ChunkHandler chunkDeserializer(
        std::bind(&SerializedArray::SerializedChunkMerger::handleChunk,
                  handler,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3));

    // the chunk handler will also to convert the coordinates
    // from the serialized to the input dimensions
    {
        arrayToPull->pullAttributes(attributesToPull, chunkDeserializer);
        arrayToPull->sync();
    }

    return std::shared_ptr<Array>();
}

std::shared_ptr<Array>
redistributeWithCallback(std::shared_ptr<Array>& inputArray,
                         PullSGArrayBlocking::ChunkHandler& chunkHandler,
                         PartialChunkMergerList* mergers,
                         const ArrayDistPtr& outputArrayDist,
                         const ArrayResPtr& outputArrayRes,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity)
{
    LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " begin");

    if (inputArray->getSupportedAccess() == Array::SINGLE_PASS) {       // can do non-materializing pullSG
        LOG4CXX_DEBUG(PullSGArray::_logger, __func__ << " SINGLE_PASS case for input: " << typeid(*inputArray.get()).name());

        auto result= redistributeWithCallbackInAttributeOrder(inputArray,
                                                        chunkHandler,
                                                        mergers,
                                                        outputArrayDist,
                                                        outputArrayRes,
                                                        query,
                                                        phyOp,
                                                        enforceDataIntegrity);

        LOG4CXX_DEBUG(PullSGArray::_logger, __func__ << " SINGLE_PASS early return");
        return result;
    }
    std::shared_ptr<Array> tmp = pullRedistribute(inputArray,
                                                  outputArrayDist,
                                                  outputArrayRes,
                                                  query,
                                                  phyOp,
                                                  enforceDataIntegrity);
    if (tmp == inputArray ) {
        SCIDB_ASSERT(!phyOp->getOperatorContext());

        LOG4CXX_DEBUG(PullSGArray::_logger, __func__ << " no redistribute required early return");
        return inputArray;
    }

    PullSGArrayBlocking *arrayToPull = safe_dynamic_cast<PullSGArrayBlocking*>(tmp.get());
    assert(arrayToPull->getSupportedAccess()==Array::SINGLE_PASS);

    const ArrayDesc& desc = arrayToPull->getArrayDesc();

    if (mergers) {
        LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " setting up mergers");
        for (AttributeID a=0, n=safe_static_cast<AttributeID>(desc.getAttributes().size());
             a < n;
             ++a) {
            SCIDB_ASSERT(a < mergers->size());

            std::shared_ptr<MultiStreamArray::PartialChunkMerger>& merger = (*mergers)[a];
            if (merger) {  // per-attribute merger is optional
                arrayToPull->setPartialChunkMerger(a, merger);
                assert(!merger);
            }
        }
    }

    {
        LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " pulling attributes");
        std::unordered_set<AttributeID> attributesToPull;
        for (AttributeID a=0, n=safe_static_cast<AttributeID>(desc.getAttributes().size());
             a < n;
             ++a) {
            attributesToPull.clear();
            attributesToPull.insert(a);

            LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " pulling attribute " << a);
            arrayToPull->pullAttributes(attributesToPull, chunkHandler);
        }
        LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " sync");
        arrayToPull->sync();
    }

    return tmp;
}

} // namespace

} // namespace
