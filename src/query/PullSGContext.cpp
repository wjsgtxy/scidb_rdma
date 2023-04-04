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
 * @file PullSGContext.cpp
 *
 * @brief Implementation of the pull-based SG context
 */

#include "PullSGContext.h"

#include <memory>

#include <log4cxx/logger.h>

#include <array/CompressedBuffer.h>

#include <system/Config.h>


using namespace std;

namespace scidb
{
namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.pullsgctx"));
}

PullSGContext::PullSGContext(const std::shared_ptr<Array>& source,
                             const std::shared_ptr<PullSGArray>& result,
                             const size_t instNum,
                             const SGInstanceLocator& instLocator,
                             size_t cacheSizePerAttribute)
  : _inputSGArray(source),
    _resultArray(result),
    _instanceLocator(instLocator),
    _instanceStates(result->getArrayDesc().getAttributes().size(),
                    std::vector<InstanceState>(instNum)),
    _attributeIterators(result->getArrayDesc().getAttributes().size()),
    _attributeStates(result->getArrayDesc().getAttributes().size()),
    _perAttributeMaxSize(64),
    _hasDataIntegrityIssue(false)
{
    assert(source);
    assert(result);
    assert(_instanceLocator);
    ArrayDesc const& descIn = _inputSGArray->getArrayDesc();
    ArrayDesc const& descOut = result->getArrayDesc();
    size_t attrNum = descIn.getAttributes().size();
    ASSERT_EXCEPTION((attrNum == descOut.getAttributes().size()), "PullSGContext::PullSGContext");
    _isEmptyable = (descIn.getEmptyBitmapAttribute() != NULL);
    int n = Config::getInstance()->getOption<int>(CONFIG_SG_SEND_QUEUE_SIZE);
    if (n>0) { _perAttributeMaxSize = n; }
    if (cacheSizePerAttribute>0) { _perAttributeMaxSize = cacheSizePerAttribute; }
}

bool
PullSGContext::hasValues(ConstChunk const& chunk)
{
    bool chunkHasVals = (!_isEmptyable) || (!chunk.isEmpty());
    return (chunkHasVals && (chunk.getSize() > 0));
}

void
PullSGContext::getNextChunks(const std::shared_ptr<Query>& query,
                             const InstanceID pullingInstance,
                             const AttributeID attrId,
                             const bool positionOnlyOK,
                             const uint64_t prefetchSize,
                             const uint64_t fetchId,
                             ChunksWithDestinations& chunksToSend)
{
    static const char* funcName="PullSGContext::getNextChunks: ";
    ASSERT_EXCEPTION((attrId < _attributeIterators.size()), funcName);
    ASSERT_EXCEPTION((pullingInstance < query->getInstancesCount()), funcName);

    InstanceState& state = _instanceStates[attrId][pullingInstance];

    // Make sure we record the request
    assert(state._lastFetchId < fetchId);
    state._lastFetchId = fetchId;
    state._requestedNum += prefetchSize;

    getNextChunksInternal(query,
                          pullingInstance,
                          attrId,
                          positionOnlyOK,
                          chunksToSend);

    // Check if there are attributes that previously ran into RetryException from _inputSGArray
    std::unordered_set<size_t> unavailable(_unavailableAttributes);
    for (std::unordered_set<size_t>::const_iterator iter = unavailable.begin();
         iter != unavailable.end(); ++iter) {
        const AttributeID currAttrId = safe_static_cast<AttributeID>(*iter);

        if (currAttrId == attrId) { continue; }

        static const bool positionOnly = false;

        LOG4CXX_TRACE(logger, funcName << "Getting previously unavailbale attID= "<<currAttrId
                      <<", # chunks sent "<< chunksToSend.size());

        getNextChunksInternal(query,
                              INVALID_INSTANCE,
                              currAttrId,
                              positionOnly,
                              chunksToSend);
    }
}

void
PullSGContext::produceChunksInBackground(const std::shared_ptr<Query>& query,
                                         const AttributeID attrId,
                                         ChunksWithDestinations& chunksToSend)
{
    static const char* funcName="PullSGContext::produceChunksInBackground: ";

    _attributeStates[attrId]._bgTask = false;

    ASSERT_EXCEPTION((attrId < _attributeIterators.size()), funcName);

    if (isCacheFull(attrId)) {
        LOG4CXX_TRACE(logger, funcName << "Cache already full for attID= " << attrId << ". Stopping.");
        return;
    }

    if (isInputEOF(attrId)) {
        LOG4CXX_TRACE(logger, funcName << "Input for attID= " << attrId << " @ EOF. Stopping.");
        return;
    }

    static const bool positionOnly = false;

    getNextChunksInternal(query,
                          INVALID_INSTANCE,
                          attrId,
                          positionOnly,
                          chunksToSend);

    // Check if there are attributes that previously ran into RetryException from _inputSGArray
    std::unordered_set<size_t> unavailable(_unavailableAttributes);
    for (std::unordered_set<size_t>::const_iterator iter = unavailable.begin();
         iter != unavailable.end(); ++iter) {
        const AttributeID currAttrId = safe_static_cast<AttributeID>(*iter);

        if (currAttrId == attrId) { continue; }

        LOG4CXX_TRACE(logger, funcName << "Getting previously unavailbale attID= "<<currAttrId
                      <<", # chunks sent "<< chunksToSend.size());

        getNextChunksInternal(query,
                              INVALID_INSTANCE,
                              currAttrId,
                              positionOnly,
                              chunksToSend);
    }
}

bool
PullSGContext::setupToRunInBackground(const AttributeID attrId)
{
    static const char* funcName="PullSGContext::setupToRunInBackground: ";
    if (isCacheFull(attrId)) {
        LOG4CXX_TRACE(logger, funcName << "Cache already full for attID= " << attrId << ". Stopping.");
        return false;
    }
    if (isInputEOF(attrId)) {
        LOG4CXX_TRACE(logger, funcName << "Input for attID= " << attrId << " @ EOF. Stopping.");
        return false;
    }
    if (_attributeStates[attrId]._bgTask) {
        LOG4CXX_TRACE(logger, funcName << "Already running in background for attID= " << attrId);
        return false;
    }
    LOG4CXX_TRACE(logger, funcName << "About to run in background for attID= " << attrId);
    _attributeStates[attrId]._bgTask = true;
    return true;
}

void
PullSGContext::getNextChunksInternal(const std::shared_ptr<Query>& query,
                                     const InstanceID pullingInstance,
                                     const AttributeID attrId,
                                     const bool positionOnlyOK,
                                     ChunksWithDestinations& chunksToSend)
{
    static const char* funcName="PullSGContext::getNextChunksInternal: ";
    ASSERT_EXCEPTION((attrId < _attributeIterators.size()), funcName);
    ASSERT_EXCEPTION((pullingInstance < query->getInstancesCount() ||
                      pullingInstance == INVALID_INSTANCE), funcName);

    // Get array iterator
    std::shared_ptr<ConstArrayIterator>& inputArrIter = _attributeIterators[attrId];
    if (!inputArrIter) {
        const auto& attrs = _inputSGArray->getArrayDesc().getAttributes();
        auto attr = attrs.find(attrId);
        SCIDB_ASSERT(attr != attrs.end());
        inputArrIter = _inputSGArray->getConstIterator(*attr);
    }
    assert(_attributeIterators[attrId]);

    if (!_attributeStates[attrId]._eof) {
        // Try to drain the array. If no more chunks are available,
        // an EOF message will be inserted into the cache for all instances.
        _attributeStates[attrId]._eof = drainInputArray(inputArrIter, query, attrId);
    }

    // Check the cache
    bool found = findCachedChunksToSend(inputArrIter,
                                        query,
                                        pullingInstance,
                                        attrId,
                                        positionOnlyOK,
                                        chunksToSend);

    if (positionOnlyOK && !found && !_attributeStates[attrId]._eof ) {
        // Nothing to send to pullingInstance, but we will send the current position.
        // The requests received after we have responded with EOF are ignored.
        assert(pullingInstance < query->getInstancesCount());
        assert(_attributeIterators[attrId]);
        std::shared_ptr<ConstArrayIterator>& inputArrIter = _attributeIterators[attrId];
        assert (inputArrIter);
        assert (!inputArrIter->end());
        const Coordinates& chunkPosition = inputArrIter->getPosition();

        InstanceID destInstance = _instanceLocator(query,
                                                   chunkPosition,
                                                   _inputSGArray->getArrayDesc());


        if (_unavailableAttributes.find(attrId) != _unavailableAttributes.end()) {
            destInstance = INVALID_INSTANCE;
        } else {
            if (destInstance == ALL_INSTANCE_MASK) {
                destInstance = pullingInstance;
            }
            assert(destInstance < query->getInstancesCount());
        }

        std::shared_ptr<MessageDesc> chunkMsg = getPositionMesg(query->getQueryID(), attrId, destInstance, chunkPosition);
        std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();

        assert(pullingInstance < query->getInstancesCount());
        InstanceState& state = _instanceStates[attrId][pullingInstance];
        assert(state._lastFetchId>0 && state._lastFetchId<uint64_t(~0));
        chunkRecord->set_fetch_id(state._lastFetchId);

        LOG4CXX_TRACE(logger, funcName << "Returning current position attID= "<<attrId
                      <<", pulling= "<< pullingInstance
                      <<", lastFetch= "<< chunkRecord->fetch_id()
                      << ", pos="<<  chunkPosition);
        assert(chunkMsg);
        chunksToSend.push_back(make_pair(pullingInstance, chunkMsg));
    }
    assert(!positionOnlyOK || _attributeStates[attrId]._eof || !chunksToSend.empty());
}


bool
PullSGContext::advanceInputIterator(const AttributeID attrId,
                                    std::shared_ptr<ConstArrayIterator>& inputArrIter)
{
    static const char* funcName="PullSGContext::advanceInputIterator: ";
    const Coordinates oldChunkPosition = inputArrIter->getPosition();
    try {
        ++(*inputArrIter);
        _unavailableAttributes.erase(attrId);
    } catch (const StreamArray::RetryException& re) {
        _unavailableAttributes.insert(attrId);
        assert(_inputSGArray->getSupportedAccess() == Array::SINGLE_PASS);
        LOG4CXX_TRACE(logger, funcName
                      << "Chunk not availbale attID= "<<attrId
                      << ", size="<< _attributeStates[attrId]._size);
        return false;
    }
    if (!inputArrIter->end()) {
        const Coordinates& newChunkPosition = inputArrIter->getPosition();
        CoordinatesLess comp;
        if ( !comp(oldChunkPosition, newChunkPosition) ) {
            if (_resultArray->isEnforceDataIntegrity()) {
                throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_CHUNK_POSITION_OUT_OF_ORDER)
                << CoordsToStr(newChunkPosition);
            }
            // chunks must arrive in row-major order
            // (or in whatever order(s) we later choose to store arrays)
            if (!_hasDataIntegrityIssue) {
                LOG4CXX_WARN(logger, funcName << "Input data chunk at position "
                             << CoordsToStr(newChunkPosition)
                             << " for attribute ID = " << attrId
                             << " is out of (row-major) order"
                             << ". Add log4j.logger.scidb.qproc.pullsgctx=TRACE to the log4cxx config file for more");
                _hasDataIntegrityIssue=true;
            } else {
                LOG4CXX_TRACE(logger, funcName << "Input data chunk at position "
                              << CoordsToStr(newChunkPosition)
                              << " for attribute ID = " << attrId
                              << " is out of (row-major) order");
            }
        }
    }
    return true;
}

bool
PullSGContext::drainInputArray(std::shared_ptr<ConstArrayIterator>& inputArrIter,
                               const std::shared_ptr<Query>& query,
                               const AttributeID attrId)

{
    static const char* funcName="PullSGContext::drainInputArray: ";

    while(true) {

        if (_unavailableAttributes.find(attrId) != _unavailableAttributes.end() &&
            !advanceInputIterator(attrId, inputArrIter)) {
            break; // loop end
        }

        if (inputArrIter->end()) {
            insertEOFChunks(query->getQueryID(), attrId);
            return true;
        }

        const ConstChunk& chunk = inputArrIter->getChunk();

        LOG4CXX_TRACE(logger, funcName
                      << " delivering chunk attr="<<chunk.getAttributeDesc()
                      <<", pos="<< CoordsToStr(chunk.getFirstPosition(false))
                      <<", hasValues="<< hasValues(chunk));

        if (!hasValues(chunk)) {
            // skip empty chunk
            if (!advanceInputIterator(attrId, inputArrIter)) {
                break; // loop end
            }
            continue;
        }

        if (_attributeStates[attrId]._size >= _perAttributeMaxSize ) {
            LOG4CXX_TRACE(logger, funcName
                          << "Cache size exceeded, attID= "<<attrId
                          << ", size="<< _attributeStates[attrId]._size);
            break; // loop end
        }

        const Coordinates& chunkPosition = inputArrIter->getPosition();

        InstanceID destInstance = _instanceLocator(query,
                                                   chunkPosition,
                                                   _inputSGArray->getArrayDesc());

        InstanceID maxDest = destInstance+1;
        if (destInstance==ALL_INSTANCE_MASK) {
            // chunk goes to all instances
            maxDest = query->getInstancesCount();
            destInstance = 0;
            _attributeStates[attrId]._shallowSize += (maxDest-1);
        }
        std::shared_ptr<CompressedBuffer> buffer;
        for (; destInstance < maxDest; ++destInstance) {
            // Cache the next chunk
            std::shared_ptr<MessageDesc> chunkMsg = getChunkMesg(query,
                                                            attrId, destInstance,
                                                            chunk, chunkPosition,
                                                            buffer);
            assert(buffer);
            assert(buffer->getConstData());
            InstanceState& destState = _instanceStates[attrId][destInstance];
            destState._chunks.push_back(chunkMsg);
        }
        ++_attributeStates[attrId]._size;
        if (!advanceInputIterator(attrId, inputArrIter)) {
            break; // loop end
        }
        LOG4CXX_TRACE(logger, funcName
                      << "Advancing iterator  attID= "<<attrId
                      << ", size="<< _attributeStates[attrId]._size);
    }
    return false;
}


void
PullSGContext::setNextPosition(std::shared_ptr<MessageDesc>& chunkMsg,
                               const InstanceID destInstance,
                               std::shared_ptr<ConstArrayIterator>& inputArrIter,
                               const std::shared_ptr<Query>& query)
{
    static const char* funcName="PullSGContext::setNextPosition: ";
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    if (chunkRecord->eof()) {
        assert(!chunkRecord->has_next());
        return;
    }

    assert(!inputArrIter->end());
    assert(chunkRecord->has_attribute_id());

    if (_unavailableAttributes.find(chunkRecord->attribute_id()) != _unavailableAttributes.end()) {
        // not adding position (because we dont know it yet)
        LOG4CXX_TRACE(logger, funcName
                      << "Not adding new position attID= "<< chunkRecord->attribute_id()
                      << ", destInstance="<< destInstance);
        return;
    }

    const Coordinates& nextChunkPosition = inputArrIter->getPosition();

    InstanceID nextDestInstance = _instanceLocator(query,
                                                   nextChunkPosition,
                                                   _inputSGArray->getArrayDesc());

    if (nextDestInstance == ALL_INSTANCE_MASK) {
        nextDestInstance = destInstance;
    }
    assert(nextDestInstance < query->getInstancesCount());
    setNextPosition(chunkMsg, nextDestInstance, nextChunkPosition);
}

bool
PullSGContext::findCachedChunksToSend(std::shared_ptr<ConstArrayIterator>& inputArrIter,
                                      const std::shared_ptr<Query>& query,
                                      const InstanceID pullingInstance,
                                      const AttributeID attrId,
                                      const bool positionOnlyOK,
                                      ChunksWithDestinations& chunksToSend)
{
    static const char* funcName="PullSGContext::findCachedChunksToSend: ";
    assert(attrId < _attributeIterators.size());

    LOG4CXX_TRACE(logger, funcName << "Start attID= "<<attrId
                  <<", pulling="<< pullingInstance
                  <<", posOnly="<< positionOnlyOK);

    std::vector<InstanceState>& states = _instanceStates[attrId];

    bool found = false;

    for (size_t destInstance=0, n=states.size(); destInstance < n; ++destInstance) {

        InstanceState& destState = states[destInstance];

        bool onlyPos = (pullingInstance==destInstance && positionOnlyOK && destState._requestedNum==0);
        if (onlyPos && !destState._chunks.empty()) {
            // cannot send the chunk, but can send the position
            std::shared_ptr<MessageDesc> msg = reapChunkMsg(query->getQueryID(), attrId, destState, destInstance, onlyPos);
            assert(msg);
            assert(!msg->getRecord<scidb_msg::Chunk>()->has_next());

            if (logger->isTraceEnabled()) {
                Coordinates coords;
                std::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk>();
                for (int i = 0, n= chunkRecord->coordinates_size(); i < n;  ++i) {
                    coords.push_back(chunkRecord->coordinates(i));
                }
                LOG4CXX_TRACE(logger, funcName << "Found position attID= "<<attrId
                              <<", pulling="<< pullingInstance
                              <<", dest="<< destInstance
                              <<", EOF="<< chunkRecord->eof()
                              <<", coords="<<coords);
            }
            chunksToSend.push_back(make_pair(destInstance, msg));
            found = true;
            continue;
        }

        while (!destState._chunks.empty() && destState._requestedNum>0) {
            // We can send this chunk to the destination
            assert(!onlyPos);

            std::shared_ptr<MessageDesc> msg = reapChunkMsg(query->getQueryID(), attrId,
                                                       destState, destInstance, false);
            // piggy-back the next position
            if (!destState._chunks.empty()) {
                // next chunk's position
                setNextPosition(msg, destState._chunks.front());
            } else {
                // last iterator position
                setNextPosition(msg, destInstance, inputArrIter, query);
            }

            if (logger->isTraceEnabled()) {
                Coordinates coords;
                std::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk>();
                int coordsSize = chunkRecord->coordinates_size();
                ASSERT_EXCEPTION (coordsSize>0 || (coordsSize==0 && chunkRecord->eof()), funcName);
                coords.reserve(coordsSize);
                for (int i = 0; i < coordsSize;  ++i) {
                    coords.push_back(chunkRecord->coordinates(i));
                }
                Coordinates nextCoords;
                InstanceID nextDest = INVALID_INSTANCE;
                if (chunkRecord->has_next()) {
                    ASSERT_EXCEPTION ((coordsSize == chunkRecord->next_coordinates_size()), funcName);
                    nextCoords.reserve(coordsSize);
                    for (int i = 0; i < coordsSize;  ++i) {
                        nextCoords.push_back(chunkRecord->next_coordinates(i));
                    }
                    nextDest = chunkRecord->next_dest_instance();
                }
                LOG4CXX_TRACE(logger, funcName << "Found chunk attID= "<<attrId
                              <<", pulling="<< pullingInstance
                              <<", dest="<< destInstance
                              <<", EOF="<< chunkRecord->eof()
                              <<", coords="<<coords
                              <<", nextCoords="<<nextCoords
                              <<", next_dest="<<nextDest);
            }

            chunksToSend.push_back(make_pair(destInstance, msg));

            if (pullingInstance==destInstance) {
                found = true;
            }
        }
    }
    return found;
}

void
PullSGContext::insertEOFChunks(const QueryID queryId,
                               const AttributeID attrId)
{
    static const char* funcName="PullSGContext::InsertEOFChunks: ";
    LOG4CXX_DEBUG(logger, funcName
                      << "Inserting EOFs into cache for  attID= "<<attrId
                      << ", cache size="<< _attributeStates[attrId]._size);
    vector< InstanceState>& instanceStatesPerAttribute = _instanceStates[attrId];
    size_t destInstanceNum = instanceStatesPerAttribute.size();
    for (InstanceID i = 0;  i < destInstanceNum; ++i) {
        InstanceState& destInstanceState = instanceStatesPerAttribute[i];
        std::shared_ptr<MessageDesc> chunkMsg = getEOFChunkMesg(queryId, attrId);
        destInstanceState._chunks.push_back(chunkMsg);
    }
    _attributeStates[attrId]._shallowSize += destInstanceNum;
}

void
PullSGContext::setNextPosition(std::shared_ptr<MessageDesc>& chunkMsg,
                               const InstanceID nextDestSGInstance,
                               const Coordinates& nextChunkPosition)
{

    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    assert(!chunkRecord->has_next());
    assert(!chunkRecord->eof());

    if (!nextChunkPosition.empty()) {
        assert(size_t(chunkRecord->coordinates_size()) == nextChunkPosition.size());
        // set next chunk coordinates
        for (size_t i = 0, n = nextChunkPosition.size(); i < n; ++i) {
            chunkRecord->add_next_coordinates(nextChunkPosition[i]);
        }
        chunkRecord->set_next_dest_instance(nextDestSGInstance);
        chunkRecord->set_has_next(true);
    } else {
        assert(false);
        chunkRecord->set_has_next(false);
    }
}

void
PullSGContext::setNextPosition(std::shared_ptr<MessageDesc>& chunkMsg,
                               std::shared_ptr<MessageDesc>& nextChunkMsg)
{

    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    std::shared_ptr<scidb_msg::Chunk> nextChunkRecord = nextChunkMsg->getRecord<scidb_msg::Chunk>();
    assert (!chunkRecord->eof());

    if (!nextChunkRecord->eof()) {

        assert(chunkRecord->coordinates_size() == nextChunkRecord->coordinates_size());
        assert(nextChunkRecord->coordinates_size()>0);

        for (int i = 0, n= nextChunkRecord->coordinates_size(); i < n;  ++i) {
            chunkRecord->add_next_coordinates(nextChunkRecord->coordinates(i));
        }
        chunkRecord->set_next_dest_instance(nextChunkRecord->dest_instance());
        chunkRecord->set_has_next(true);
    } else {
        chunkRecord->set_has_next(false);
    }
}

std::shared_ptr<MessageDesc>
PullSGContext::getPositionMesg(const QueryID queryId,
                               const AttributeID attributeId,
                               const InstanceID destSGInstance,
                               const Coordinates& chunkPosition)
{
    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();

    // set chunk coordinates
    for (size_t i = 0, n = chunkPosition.size(); i < n; ++i) {
        chunkRecord->add_coordinates(chunkPosition[i]);
    }
    chunkMsg->setQueryID(queryId);
    chunkRecord->set_eof(false);
    chunkRecord->set_obj_type(PullSGArray::SG_ARRAY_OBJ_TYPE);
    chunkRecord->set_attribute_id(attributeId);
    chunkRecord->set_dest_instance(destSGInstance);
    chunkRecord->set_has_next(false);
    return chunkMsg;
}

std::shared_ptr<MessageDesc>
PullSGContext::getPositionMesg(const std::shared_ptr<MessageDesc>& fullChunkMsg)
{
    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    std::shared_ptr<scidb_msg::Chunk> fullChunkRecord = fullChunkMsg->getRecord<scidb_msg::Chunk>();

    // set chunk coordinates
    for (int i = 0, n = fullChunkRecord->coordinates_size(); i < n; ++i) {
        chunkRecord->add_coordinates(fullChunkRecord->coordinates(i));
    }
    chunkMsg->setQueryID(fullChunkMsg->getQueryID());
    assert(!fullChunkRecord->eof());
    chunkRecord->set_eof(fullChunkRecord->eof());
    assert(fullChunkRecord->obj_type() == PullSGArray::SG_ARRAY_OBJ_TYPE);
    chunkRecord->set_obj_type(PullSGArray::SG_ARRAY_OBJ_TYPE);
    chunkRecord->set_attribute_id(fullChunkRecord->attribute_id());
    chunkRecord->set_dest_instance(fullChunkRecord->dest_instance());
    chunkRecord->set_has_next(false);
    return chunkMsg;
}

std::shared_ptr<MessageDesc>
PullSGContext::getEOFChunkMesg(const QueryID queryId,
                               const AttributeID attributeId)
{
    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();

    chunkMsg->setQueryID(queryId);
    chunkRecord->set_obj_type(PullSGArray::SG_ARRAY_OBJ_TYPE);
    chunkRecord->set_attribute_id(attributeId);
    chunkRecord->set_has_next(false);

    chunkRecord->set_eof(true);
    return chunkMsg;
}

std::shared_ptr<MessageDesc>
PullSGContext::getChunkMesg(const std::shared_ptr<Query>& query,
                            const AttributeID attributeId,
                            const InstanceID destSGInstance,
                            const ConstChunk& chunk,
                            const Coordinates& chunkPosition,
                            std::shared_ptr<CompressedBuffer>& buffer)
{

    if (!buffer) {
        buffer = std::make_shared<CompressedBuffer>();
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;

        if (_inputSGArray->getArrayDesc().getEmptyBitmapAttribute() != NULL &&
            !chunk.getAttributeDesc().isEmptyIndicator()) {
            emptyBitmap = chunk.getEmptyBitmap();
            if (isDebug() && _isEmptyable) {
                verifyPositions(chunk, emptyBitmap);
            }
        }
        // isSameServer: an optimization, no compression between instances on the same server
        ASSERT_EXCEPTION(isValidPhysicalInstance(destSGInstance), "getChunkMesg: destSGInstance is not physical");
        auto const thisInstanceId = query->getInstanceID();
        ASSERT_EXCEPTION(isValidPhysicalInstance(thisInstanceId), "getChunkMesg: thisInstanceId  is not physical");
        const bool isSameServer = getServerId(destSGInstance) == getServerId(thisInstanceId);

        chunk.compress(*buffer, emptyBitmap, isSameServer); //XXX TODO: avoid data copy
        emptyBitmap.reset(); // the bitmask must be cleared before the iterator is advanced (bug?)
    }
    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(mtRemoteChunk, buffer);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    chunkRecord->set_compression_method(static_cast<int32_t>(buffer->getCompressionMethod()));
    chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
    chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
    const Coordinates& coordinates = chunk.getFirstPosition(false);
    for (size_t i = 0, n = coordinates.size(); i< n; ++i) {
        chunkRecord->add_coordinates(coordinates[i]);
    }
    chunkMsg->setQueryID(query->getQueryID());
    chunkRecord->set_eof(false);
    chunkRecord->set_obj_type(PullSGArray::SG_ARRAY_OBJ_TYPE);
    chunkRecord->set_attribute_id(attributeId);
    chunkRecord->set_dest_instance(destSGInstance);
    chunkRecord->set_has_next(false);

    return chunkMsg;
}

void PullSGContext::verifyPositions(ConstChunk const& chunk,
                                    std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap)
{
    assert(!chunk.isEmpty());
    if (_resultArray->isSerialized()) {
        // Serialized arrays have no physical empty bitmap, only a fake one in the metadata
        SCIDB_ASSERT((!emptyBitmap) || (emptyBitmap->count()>0));
        return;
    }
    SCIDB_ASSERT(emptyBitmap && emptyBitmap->count()>0);
    std::shared_ptr<ConstChunkIterator> chunkIter =
       chunk.materialize()->getConstIterator(ConstChunkIterator::DEFAULT);
    ConstRLEEmptyBitmap::iterator ebmIter = emptyBitmap->getIterator();
    while(!chunkIter->end()) {
        assert(!ebmIter.end());
        position_t chunkPos = chunkIter->getLogicalPosition();
        position_t ebmPos   = ebmIter.getLPos();
        SCIDB_ASSERT(chunkPos==ebmPos);
        ++(*chunkIter);
        ++(ebmIter);
    }
    assert(ebmIter.end());
}

std::shared_ptr<MessageDesc>
PullSGContext::reapChunkMsg(const QueryID queryId,
                            const AttributeID attrId,
                            InstanceState& destState,
                            const InstanceID destInstance,
                            const bool positionOnly)
{
    assert(!destState._chunks.empty());

    std::shared_ptr<MessageDesc> headMsg  = destState._chunks.front();
    assert(headMsg);

    if (positionOnly) {
        std::shared_ptr<scidb_msg::Chunk> headRecord = headMsg->getRecord<scidb_msg::Chunk>();
        if (headRecord->eof()) {
            assert(!headRecord->has_next());
            destState._chunks.pop_front();
        } else {
            headMsg = getPositionMesg(headMsg);
        }
        assert(destState._requestedNum==0);
    } else {
        destState._chunks.pop_front();
        assert(destState._requestedNum>0);
        --destState._requestedNum;
        assert(_attributeStates[attrId]._size <= _perAttributeMaxSize);

        if (_attributeStates[attrId]._shallowSize > 0) {
            --_attributeStates[attrId]._shallowSize;
        } else {
            assert(_attributeStates[attrId]._size>0);
            --_attributeStates[attrId]._size;
        }
    }
    std::shared_ptr<scidb_msg::Chunk> headRecord = headMsg->getRecord<scidb_msg::Chunk>();
    assert(destState._lastFetchId>0 && destState._lastFetchId<uint64_t(~0));
    headRecord->set_fetch_id(destState._lastFetchId);
    return headMsg;
}

} //namespace scidb
