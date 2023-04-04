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
 * @file PullSGArray.cpp
 *
 * @brief mplementation of an array that returns redistributed chunks by the means of pull-based scater/gather
 */
#include "PullSGArray.h"

#include <array/CompressedBuffer.h>
#include <array/MemArray.h>
#include <array/SinglePassArray.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Aggregate.h>
#include <query/AggregateChunkMerger.h>
#include <query/PhysicalOperator.h>
#include <query/PullSGArrayUtil.h>
#include <query/PullSGContext.h>
#include <system/Config.h>
#include <system/Exceptions.h>

#include <unordered_set>


using namespace std;

namespace scidb
{

log4cxx::LoggerPtr PullSGArray::_logger(log4cxx::Logger::getLogger("scidb.qproc.pullsgarray"));

namespace {
template<typename T>
void logMatrix(std::vector<std::vector<T> >& matrix, const std::string& prefix)
{
    if (!PullSGArray::_logger->isTraceEnabled()) {
        return;
    }
    stringstream ss;
    for (size_t i=0; i<matrix.size(); ++i) {
        std::vector<T>& row = matrix[i];
        for (size_t j=0; j<row.size(); ++j) {
            ss << "["<<i<<","<<j<<"] = "<<row[j]<<",";
        }
        ss << " ; ";
    }
    LOG4CXX_TRACE(PullSGArray::_logger, prefix << ": " << ss.str());
}
}

PullSGArray::PullSGArray(const ArrayDesc& arrayDesc,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity,
                         uint32_t chunkPrefetchPerAttribute)
  : MultiStreamArray(query->getInstancesCount(), query->getInstanceID(), arrayDesc, enforceDataIntegrity, query),
    _queryId(query->getQueryID()),
    _phyOp(phyOp),
    _callbacks(arrayDesc.getAttributes().size()),
    _messages(arrayDesc.getAttributes().size(), vector< StreamState >(getStreamCount())),
    _commonChunks(arrayDesc.getAttributes().size(), 0),
    _maxChunksPerStream(0),
    _maxChunksPerAttribute(64)
{
    _query = query;
    SCIDB_ASSERT(phyOp->getOperatorID().isValid());

    if (isDebug()) {
        _cachedChunks.resize(arrayDesc.getAttributes().size(), 0);
        _requestedChunks.resize(arrayDesc.getAttributes().size(), 0);
        _numSent.resize(arrayDesc.getAttributes().size(), 0);
        _numRecvd.resize(arrayDesc.getAttributes().size(), 0);
    }

    static const size_t MAX_MUTEX_NUM = 100;
    _sMutexes.resize(std::min(getStreamCount(), MAX_MUTEX_NUM));
    _aMutexes.resize(std::min(arrayDesc.getAttributes().size(),
                              MAX_MUTEX_NUM));

    static const uint32_t DEFAULT_PREFETCH_CACHE_SIZE=64;
    _maxChunksPerAttribute = DEFAULT_PREFETCH_CACHE_SIZE;

    int n = Config::getInstance()->getOption<int>(CONFIG_SG_RECEIVE_QUEUE_SIZE);
    if (n>0) {
        _maxChunksPerAttribute = n;
    }
    if (chunkPrefetchPerAttribute > 0) {
        _maxChunksPerAttribute = chunkPrefetchPerAttribute;
    }
    uint32_t streamCount = safe_static_cast<uint32_t>(getStreamCount());
    _maxChunksPerStream = _maxChunksPerAttribute / streamCount / 2;
    _maxCommonChunks = _maxChunksPerAttribute - (_maxChunksPerStream * streamCount);
}

std::shared_ptr<PhysicalOperator> PullSGArray::getPhysicalOperator()
{
    return _phyOp.lock();
}

std::ostream& operator << (std::ostream& out,
                           PullSGArray::StreamState& state)
{
    out << "["
        << state.getLastPositionOnlyId() <<";"
        << state.getLastRemoteId() <<";"
        << state.getRequested()<<";"
        << state.cachedSize()<<";"
        << state.size()<<";" ;
    out << "]";
    return out;
}

uint32_t PullSGArray::getPrefetchSize(AttributeID attId, size_t stream, bool positionOnly)
{
    static const char* funcName = "PullSGArray::getPrefetchSize: ";
    assert((_messages[attId][stream].cachedSize() + _messages[attId][stream].getRequested())
           <= (_maxChunksPerStream+_commonChunks[attId]));
    assert(_requestedChunks[attId] +_cachedChunks[attId]
           <= (_maxChunksPerAttribute+getStreamCount()));

    uint32_t prefetchSize = 0;
    uint32_t outstanding = safe_static_cast<uint32_t>(
        _messages[attId][stream].cachedSize() + _messages[attId][stream].getRequested());
    if (_maxChunksPerStream > outstanding) {
        // there is space for more chunks
        prefetchSize = _maxChunksPerStream - outstanding;
    } else if (_commonChunks[attId] < _maxCommonChunks &&
               _messages[attId][stream].getRequested()<1) {
        // per-stream limit is reached, but the common pool can be used
        prefetchSize = (_maxCommonChunks - _commonChunks[attId]) / safe_static_cast<uint32_t>(getStreamCount());
        prefetchSize = prefetchSize < 1 ? 1 : prefetchSize;
        _commonChunks[attId] += prefetchSize;
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                          << ", commonChunks=" << _commonChunks[attId]
                          << ", stream=" << stream);
    } else if (!positionOnly && outstanding < 1 &&
               _messages[attId][stream].isEmpty()) {
        // if the cache size is smaller than the number of streams,
        // we are not going to do any prefetching
        // but we have to request at least one chunk to make progress
        prefetchSize = 1;
        ++_commonChunks[attId]; // billing against _commonChunks
    }

    assert((_requestedChunks[attId] +_cachedChunks[attId] + prefetchSize)
           <= (_maxChunksPerAttribute+getStreamCount()));
    assert(_commonChunks[attId] <= (_maxCommonChunks+getStreamCount()));

    LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                  << ", prefetchSize=" << prefetchSize
                  << ", stream=" << stream);

    return prefetchSize;
}

void
PullSGArray::requestNextChunk(size_t stream, AttributeID attId, bool positionOnly, const Coordinates& lastKnownPosition)
{
    static const char* funcName = "PullSGArray::requestNextChunk: ";
    uint32_t prefetchSize=0;
    uint64_t fetchId=~0;
    {
        ScopedMutexLock lock(_sMutexes[stream % _sMutexes.size()], PTW_SML_SG_PULL);

        logMatrix(_messages, "PullSGArray::requestNextChunk(): before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!positionOnly) {
            pruneRedundantPositions(stream, attId, lastKnownPosition);
        }

        if (!streamState.isEmpty()) {
            std::shared_ptr<scidb_msg::Chunk> chunkRecord =
               streamState.head()->getRecord<scidb_msg::Chunk>();
            if (chunkRecord->eof()) {
                // nothing to request
                LOG4CXX_TRACE(_logger, funcName << " already @ EOF attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", stream=" << stream);
                if (isDebug()) {
                    LOG4CXX_DEBUG(_logger, funcName << " stats attId=" << attId
                                  << ", stream=" << stream
                                  << ", numSent=" << _numSent[attId]
                                  << ", numRecvd=" << _numRecvd[attId]);
                }
                return;
            }
        }

        bool isPositionReqInFlight = (streamState.getLastPositionOnlyId() >
                                      streamState.getLastRemoteId());
        {
            ScopedMutexLock cLock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
            prefetchSize = getPrefetchSize(attId, stream, positionOnly);
        }
        if (prefetchSize <= 0) {
            if (!streamState.isEmpty() ) {
                // already received something, needs to be consumed first before prefetching
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already have data attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", stream=" << stream);
                return;
            }
            if (!positionOnly) {
                // cannot prefetch any more
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already requested data attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                                  << ", stream=" << stream);
                return;
            } else if (isPositionReqInFlight) {
                // already have an outstanding position request
                LOG4CXX_TRACE(_logger, funcName << "nothing to request, already requested position attId=" << attId
                              << (positionOnly? ", position only" : ", full")
                              << ", last PO request ID=" << streamState.getLastPositionOnlyId()
                              << ", last request ID from source=" << streamState.getLastRemoteId()
                              << ", stream=" << stream);
                return;
            }
        }

        if (!streamState.isEmpty() || isPositionReqInFlight) {
            assert(prefetchSize > 0);
            // no need to ask for a bare position unless we have to make progress
            positionOnly = false;
        }

        if (positionOnly) {
            fetchId = streamState.getNextMsgId();
            streamState.setLastPositionOnlyId(fetchId);

        } else if (streamState.getRequested()>0) {
            LOG4CXX_TRACE(_logger, funcName << "nothing to request, too many outstanding attId=" << attId
                          << (positionOnly? ", position only" : ", full")
                          << ", prefetch="<<prefetchSize
                          << ", requested="<<streamState.getRequested()
                          << ", stream=" << stream);
            return;
        } else {
            fetchId = streamState.getNextMsgId();
        }
        if (isDebug()) {
            ScopedMutexLock cLock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
            _requestedChunks[attId] += prefetchSize;
            ++_numSent[attId];
        }
        streamState.setRequested(prefetchSize + streamState.getRequested());

        logMatrix(_messages, "PullSGArray::requestNextChunk(): after _messages");
    }

    LOG4CXX_TRACE(_logger, funcName << " request next chunk attId=" << attId
                  << (positionOnly? ", position only" : ", full")
                  << ", stream=" << stream
                  << ", prefetch=" << prefetchSize);

    std::shared_ptr<MessageDesc> fetchDesc = std::make_shared<MessageDesc>(mtFetch);
    fetchDesc->setQueryID(_queryId);

    auto phyOp = _phyOp.lock();
    SCIDB_ASSERT(phyOp);
    SCIDB_ASSERT(phyOp->getOperatorID().isValid());
    fetchDesc->setPhysicalOperatorID(phyOp->getOperatorID());

    std::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();

    assert(fetchId != uint64_t(~0));
    fetchRecord->set_fetch_id(fetchId);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_prefetch_size(prefetchSize);
    fetchRecord->set_obj_type(SG_ARRAY_OBJ_TYPE);

    const InstanceID logicalId = stream;
    if (getLocalStream() == logicalId) {
        // local
        std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        NetworkManager::getInstance()->sendLocal(query, fetchDesc);
    } else {
        // remote
        NetworkManager::getInstance()->send(logicalId, fetchDesc);
    }
}

void
PullSGArray::handleChunkMsg(const std::shared_ptr<MessageDesc>& chunkDesc,
                            const InstanceID logicalSourceId)
{
    static const char* funcName = "PullSGArray::handleChunkMsg: ";
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    ASSERT_EXCEPTION((chunkDesc->getQueryID()==_queryId), funcName);

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    ASSERT_EXCEPTION((chunkMsg->has_attribute_id()), funcName);
    AttributeID attId = chunkMsg->attribute_id();
    ASSERT_EXCEPTION((chunkMsg->has_fetch_id()), funcName);
    uint64_t fetchId = chunkMsg->fetch_id();
    ASSERT_EXCEPTION((fetchId>0 && fetchId<uint64_t(~0)), funcName);

    size_t stream = logicalSourceId;

    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    RescheduleCallback cb;
    {
        ScopedMutexLock lock(_sMutexes[stream % _sMutexes.size()], PTW_SML_SG_PULL);
        LOG4CXX_TRACE(_logger,  funcName << "received next chunk message attId="<<attId
                      <<", stream="<<stream
                      <<", queryID="<<_queryId);
        logMatrix(_messages, "PullSGArray::handleChunkMsg: before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        streamState.push(chunkDesc);
        streamState.setLastRemoteId(fetchId);

        if (isDebug()) {
            ScopedMutexLock cLock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
            ++_numRecvd[attId];
        }
        if (chunkDesc->getBinary()) {
            assert(streamState.getRequested()>0);
            streamState.setRequested(streamState.getRequested()-1);
            if (isDebug()) {
                ScopedMutexLock cLock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
                assert(_requestedChunks[attId]>0);
                --_requestedChunks[attId];
                ++_cachedChunks[attId];
            }
            assert(streamState.cachedSize()>0);
        }
        assert(streamState.size()>0);

        if (streamState.isPending()) {
            cb = getCallback(attId);
            streamState.setPending(false);
        }
        logMatrix(_messages, "PullSGArray::handleChunkMsg: after _messages");
    }
    if (cb) {
        const Exception* error(NULL);
        cb(error);
    }
}

void
PullSGArray::pruneRedundantPositions(size_t stream, AttributeID attId,
                                     const Coordinates& lastKnownPosition)
{
    static const char* funcName = "PullSGArray::pruneRedundantPositions: ";
    PullSGArray::StreamState& streamState = _messages[attId][stream];

    while (!streamState.isEmpty()) {
        const std::shared_ptr<MessageDesc>& msg = streamState.head();
        if (msg->getBinary()) {
            break;
        }
        std::shared_ptr<scidb_msg::Chunk> record = msg->getRecord<scidb_msg::Chunk>();
        if (record->eof()) {
            break;
        }

        int n = record->coordinates_size();
        ASSERT_EXCEPTION((n==safe_static_cast<int>(lastKnownPosition.size())), funcName);

        for (int i = 0; i<n; ++i) {
            ASSERT_EXCEPTION((lastKnownPosition[i] == record->coordinates(i)),
                             funcName);
        }
        streamState.pop();
    }
}

bool
PullSGArray::getChunk(size_t stream, AttributeID attId, const Coordinates& position, MemChunk* chunk)
{
    static const char* funcName = "PullSGArray::getChunk: ";
    assert(chunk);

    std::shared_ptr<MessageDesc> chunkDesc;
    std::shared_ptr<CompressedBuffer> compressedBuffer;
    {
        ScopedMutexLock lock(_sMutexes[stream % _sMutexes.size()], PTW_SML_SG_PULL);

        logMatrix(_messages, "PullSGArray::getChunk: before _messages");

        pruneRedundantPositions(stream, attId, position);

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!streamState.isEmpty()) {
            LOG4CXX_TRACE(PullSGArray::_logger, funcName << "streamState not isEmpty()" );
            chunkDesc = streamState.pop();
            assert(chunkDesc);
            assert(!chunkDesc->getRecord<scidb_msg::Chunk>()->eof());

            compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
            assert(compressedBuffer);
            {
                ScopedMutexLock cLock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
                if (isDebug()) { --_cachedChunks[attId]; }
                if ((streamState.cachedSize() +
                     streamState.getRequested()) >= _maxChunksPerStream) {
                    assert(_commonChunks[attId]>0);
                    --_commonChunks[attId];
                    LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                                  << ", commonChunks=" << _commonChunks[attId]
                                  << ", stream=" << stream);
                }
            }
            std::shared_ptr<MessageDesc> nextPosMsgDesc;
            if (streamState.isEmpty()) {
                nextPosMsgDesc = toPositionMesg(chunkDesc);
            }
            if (nextPosMsgDesc) {
                streamState.push(nextPosMsgDesc);
            }
        } else {

            assert(streamState.getRequested()>0);
        }
        if (!chunkDesc) {
            streamState.setPending(true);
        }
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                     << ", stream=" << stream
                     << ", message queue size=" << streamState.size());

        logMatrix(_messages, "PullSGArray::getChunk: after _messages");
    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(_logger, funcName << "found next chunk message stream="<<stream<<", attId="<<attId);
        assert(chunk != NULL);
        ASSERT_EXCEPTION(compressedBuffer.get()!=nullptr, funcName);

        const CompressorType compMethod = static_cast<CompressorType>(chunkMsg->compression_method());
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk->initialize(this, &desc, firstElem, compMethod);
        chunk->setCount(chunkMsg->count());

        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk->decompress(*compressedBuffer); //XXX TODO: avoid data copy
        assert(chunkMsg->dest_instance() == getLocalStream());
        if (!isSerialized()) {
            // When the input array is "serialized",
            // the last attribute, which is usually the empty bitmap,
            // 'looks' like the zero'th atrribute at a different coordinate.
            // Thus, such a chunk would have the EBM magic number not the expected payload magic number.
            checkChunkMagic(*chunk, __PRETTY_FUNCTION__);
        }
        return true;
    }
    else
    {
        LOG4CXX_DEBUG(_logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

std::shared_ptr<MessageDesc>
PullSGArray::toPositionMesg(const std::shared_ptr<MessageDesc>& oldChunkMsg)
{
    if (!oldChunkMsg) {
        return std::shared_ptr<MessageDesc>();
    }

    std::shared_ptr<scidb_msg::Chunk> oldChunkRecord = oldChunkMsg->getRecord<scidb_msg::Chunk>();
    if (!oldChunkMsg->getBinary()) {
        // positon mesg should not have the next position
        assert(!oldChunkRecord->has_next());
        // we should not be calling this method
        assert(false);
        return std::shared_ptr<MessageDesc>();
    }

    if (!oldChunkRecord->has_next()) {
        return std::shared_ptr<MessageDesc>();
    }
    assert(oldChunkRecord->next_coordinates_size()>0);

    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(oldChunkMsg->getMessageType());
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();

    // set chunk coordinates
    for (int i = 0, n = oldChunkRecord->next_coordinates_size(); i < n; ++i) {
        chunkRecord->add_coordinates(oldChunkRecord->next_coordinates(i));
    }

    chunkRecord->set_dest_instance(oldChunkRecord->next_dest_instance());
    chunkRecord->set_has_next(false);

    assert(!oldChunkRecord->eof());
    chunkRecord->set_eof(oldChunkRecord->eof());
    assert(oldChunkRecord->obj_type() == SG_ARRAY_OBJ_TYPE);

    chunkRecord->set_obj_type     (oldChunkRecord->obj_type());
    chunkRecord->set_attribute_id (oldChunkRecord->attribute_id());

    chunkMsg->setQueryID(oldChunkMsg->getQueryID());
    chunkMsg->setSourceInstanceID(oldChunkMsg->getSourceInstanceID());
    return chunkMsg;
}

bool
PullSGArray::getPosition(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    static const char* funcName = "PullSGArray::getPosition: ";
    LOG4CXX_TRACE(PullSGArray::_logger, funcName);
    std::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_sMutexes[stream % _sMutexes.size()], PTW_SML_SG_PULL);

        logMatrix(_messages, "PullSGArray::getPosition: before _messages");

        PullSGArray::StreamState& streamState = _messages[attId][stream];

        if (!streamState.isEmpty()) {
            chunkDesc = streamState.head();
            assert(chunkDesc);
            if (!chunkDesc->getBinary()) {
                streamState.pop();
            }
        }

        if (!chunkDesc) {
            assert(streamState.getLastPositionOnlyId() >
                   streamState.getLastRemoteId());
            streamState.setPending(true);
        }
        LOG4CXX_TRACE(_logger, funcName << "attId=" << attId
                     << ", stream=" << stream
                     << ", stream queue size=" << streamState.size());

        logMatrix(_messages, "PullSGArray::getPosition: after _messages");
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << " after _messages, attId" << attId );
    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(_logger, funcName << "checking for position stream="<<stream<<", attId="<<attId);

        for (int i = 0, n= chunkMsg->coordinates_size(); i < n;  ++i) {
            pos.push_back(chunkMsg->coordinates(i));
        }
        const InstanceID logicalSGDestination = chunkMsg->dest_instance();
        destStream = logicalSGDestination;

        LOG4CXX_TRACE(_logger, funcName << "found next position stream="<<stream
                      <<", attId="<<attId<<", pos="<<pos);
        return true;
    } else {
        LOG4CXX_DEBUG(_logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

ConstChunk const*
PullSGArray::nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    static const bool positionOnly = true;
    requestNextChunk(stream, attId, !positionOnly, _currMinPos[attId]);

    bool result = getChunk(stream, attId,  _currMinPos[attId], &chunk);

    requestNextChunk(stream, attId, positionOnly, _currMinPos[attId]); // pre-fetching

    return (result ? &chunk : NULL);
}

bool
PullSGArray::nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    static const bool positionOnly = true;
    requestNextChunk(stream, attId, positionOnly, pos);

    bool result = getPosition(stream, attId, pos, destStream);
    return result;
}

PullSGArray::RescheduleCallback
PullSGArray::getCallback(AttributeID attId)
{
   assert(attId<_callbacks.size());
   ScopedMutexLock lock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
   return _callbacks[attId];
}

PullSGArray::RescheduleCallback
PullSGArray::resetCallback(AttributeID attId)
{
    PullSGArray::RescheduleCallback cb;
    return resetCallback(attId,cb);
}

PullSGArray::RescheduleCallback
PullSGArray::resetCallback(AttributeID attId,
                           const PullSGArray::RescheduleCallback& newCb)
{
    LOG4CXX_TRACE(PullSGArray::_logger, "PullSGArray::"<<__func__);
    assert(attId<_callbacks.size());
    PullSGArray::RescheduleCallback oldCb;
    {
        ScopedMutexLock lock(_aMutexes[attId % _aMutexes.size()], PTW_SML_SG_PULL);
        _callbacks[attId].swap(oldCb);
        _callbacks[attId] = newCb;
    }
    return oldCb;
}

std::shared_ptr<MessageDesc>
PullSGArray::StreamState::pop()
{
    std::shared_ptr<MessageDesc> msg;
    if (_msgs.empty()) {
        return msg;
    }
    msg = _msgs.front();
    _msgs.pop_front();
    if (msg->getBinary()) {
        assert(_cachedSize>0);
        --_cachedSize;
    }
    return msg;
}

std::shared_ptr<ConstArrayIterator>
PullSGArray::getConstIterator(AttributeID attId) const
{
    assert(attId < _messages.size());

    StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
    if (!_iterators[attId]) {
        std::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, attId));
        std::shared_ptr<ConstArrayIterator>& iter =
           const_cast<std::shared_ptr<ConstArrayIterator>&>(_iterators[attId]);
        iter = cai;
        LOG4CXX_TRACE(_logger, "PullSGArray::getConstIterator(): new iterator attId="<<attId);
    } else {
        if (!_iterators[attId]->end()) {
            LOG4CXX_TRACE(_logger, "PullSGArray::getConstIterator(): increment attId="<<attId);
            ++(*_iterators[attId]);
        }
    }
    return _iterators[attId];
}

// This 6-arg overload uses no aggregates or chunk mergers.
// TODO: it seems to have a flaw.  Callers test the result
//  to see if is the same as inputArray, assuming that
//  this function will not redistribute unnecessarily.
//  However, this method does not actually test whether
//  the array is already distributed correctly (and call
//  ensureRandomAccess) ... instead it relies on redistributeWithCallback
//  to indicate no redistribution, however redistributeWithCallback
//  does not do this as of the time of this note.
std::shared_ptr<Array>
redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                           const ArrayDistPtr& outputArrayDist,
                           const ArrayResPtr& outputArrayRes,
                           const std::shared_ptr<Query>& query,
                           const std::shared_ptr<PhysicalOperator>& phyOp,
                           bool enforceDataIntegrity)
{
    static const char * funcName = "redistributeToRandomAccess(6arg): ";

    ArrayDesc outputArrayDesc(inputArray->getArrayDesc());
    outputArrayDesc.setDistribution(outputArrayDist);
    if (outputArrayRes) {
        outputArrayDesc.setResidency(outputArrayRes);
    } else {
        outputArrayDesc.setResidency(query->getDefaultArrayResidency());
    }
    std::shared_ptr<Array> outputArray = std::make_shared<MemArray>(outputArrayDesc, query);

    LOG4CXX_DEBUG(PullSGArray::_logger, funcName << "Temporary array was opened");

    PullSGArrayBlocking::ChunkHandler chunkHandler =
        std::bind(sg::WriteChunkToArrayFunc(outputArray, NULL, enforceDataIntegrity),
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3);
    std::shared_ptr<Array> redistributed = sg::redistributeWithCallback(inputArray,
                                                                        chunkHandler,
                                                                        NULL,
                                                                        outputArrayDist,
                                                                        outputArrayRes,
                                                                        query,
                                                                        phyOp,
                                                                        enforceDataIntegrity);
    if (redistributed == inputArray) {
        SCIDB_ASSERT(redistributed->getArrayDesc().getResidency()->isEqual(outputArrayRes ?
                                                                           outputArrayRes :
                                                                           query->getDefaultArrayResidency()));
        SCIDB_ASSERT(outputArrayDist->checkCompatibility(redistributed->getArrayDesc().getDistribution()));

        return PhysicalOperator::ensureRandomAccess(redistributed, query);
    }
    return outputArray;
}

// This 7-arg overload uses aggregates.  It's a wrapper that sets up appropriate chunk mergers.
std::shared_ptr<Array>
redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                           const ArrayDistPtr& outputArrayDist,
                           const ArrayResPtr& outputArrayRes,
                           const std::shared_ptr<Query>& query,
                           const std::shared_ptr<PhysicalOperator>& phyOp,
                           const std::vector<AggregatePtr>& aggregates,
                           bool enforceDataIntegrity)
{
    static const char * funcName = "redistributeToRandomAccess(7arg, agg): ";
    LOG4CXX_TRACE(PullSGArray::_logger, funcName << " begin");

    ArrayDesc const& inputDesc = inputArray->getArrayDesc();
    const size_t nAttrs = inputDesc.getAttributes().size();
    const bool isEmptyable = (inputDesc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && (inputDesc.getEmptyBitmapAttribute()->getId() != nAttrs-1 || aggregates[nAttrs-1])) {
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_AGGREGATE_ERROR1);
    }
    PartialChunkMergerList mergers(nAttrs);

    for (AttributeID a=0; a < nAttrs; ++a) {
        assert(a<aggregates.size());
        if (aggregates[a]) {
            std::shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
            std::make_shared<AggregateChunkMerger>(aggregates[a],isEmptyable);
            mergers[a] = merger;
        }
    }
    return redistributeToRandomAccess(inputArray,
                                      outputArrayDist,
                                      outputArrayRes,
                                      query,
                                      phyOp,
                                      mergers,
                                      enforceDataIntegrity);
}

// This 7-arg overload takes a vector of chunk mergers.
std::shared_ptr<Array>
redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                           const ArrayDistPtr& outputArrayDist,
                           const ArrayResPtr& outputArrayRes,
                           const std::shared_ptr<Query>& query,
                           const std::shared_ptr<PhysicalOperator>& phyOp,
                           PartialChunkMergerList& mergers,
                           bool enforceDataIntegrity)
{
    static const char * funcName = "redistributeToRandomAccess(7arg, mergers): ";
    LOG4CXX_TRACE(PullSGArray::_logger, funcName << " begin");

    ArrayDesc outputArrayDesc(inputArray->getArrayDesc());
    outputArrayDesc.setDistribution(outputArrayDist);
    if (outputArrayRes) {
        outputArrayDesc.setResidency(outputArrayRes);
    } else {
        outputArrayDesc.setResidency(query->getDefaultArrayResidency());
    }
    std::shared_ptr<Array> outputArray = std::make_shared<MemArray>(outputArrayDesc, query);

    LOG4CXX_DEBUG(PullSGArray::_logger, funcName << "Temporary array was opened");
    PullSGArrayBlocking::ChunkHandler chunkHandler =
        std::bind<void>(sg::WriteChunkToArrayFunc(outputArray, NULL, enforceDataIntegrity),
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3);

    ASSERT_EXCEPTION((mergers.size() == inputArray->getArrayDesc().getAttributes().size()),
                     "Number of mergers != number of input array attributes");

    std::shared_ptr<Array> redistributed = sg::redistributeWithCallback(inputArray,
                                                                        chunkHandler,
                                                                        &mergers,
                                                                        outputArrayDist,
                                                                        outputArrayRes,
                                                                        query,
                                                                        phyOp,
                                                                        enforceDataIntegrity);
    if (redistributed == inputArray) {
        return PhysicalOperator::ensureRandomAccess(redistributed, query);
    }
    return outputArray;
}

void redistributeToArray(std::shared_ptr<Array>& inputArray,
                         std::shared_ptr<Array>& outputArray,
                         set<Coordinates, CoordinatesLess>* newChunkCoordinates,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity)
{
    LOG4CXX_TRACE(PullSGArray::_logger, __func__ << " begin");

    PullSGArrayBlocking::ChunkHandler chunkHandler =
        std::bind<void>(sg::WriteChunkToArrayFunc(outputArray,
                                                  newChunkCoordinates,
                                                  enforceDataIntegrity),
                        std::placeholders::_1,
                        std::placeholders::_2,
                        std::placeholders::_3);

    std::shared_ptr<Array> redistributed = sg::redistributeWithCallback(inputArray,
                                                                        chunkHandler,
                                                                        NULL,
                                                                        outputArray->getArrayDesc().getDistribution(),
                                                                        outputArray->getArrayDesc().getResidency(),
                                                                        query,
                                                                        phyOp,
                                                                        enforceDataIntegrity);
    if (redistributed == inputArray) {
        outputArray->appendHorizontal(redistributed, newChunkCoordinates);
    }
}

void
redistributeToArray(std::shared_ptr<Array>& inputArray,
                    std::shared_ptr<Array>& outputArray,
                    PartialChunkMergerList& mergers,
                    set<Coordinates, CoordinatesLess>* newChunkCoordinates,
                    const std::shared_ptr<Query>& query,
                    const std::shared_ptr<PhysicalOperator>& phyOp,
                    bool enforceDataIntegrity)
{
    PullSGArrayBlocking::ChunkHandler chunkHandler = std::bind<void>(sg::WriteChunkToArrayFunc(outputArray,
                                                                                               newChunkCoordinates,
                                                                                               enforceDataIntegrity),
                                                                     std::placeholders::_1,
                                                                     std::placeholders::_2,
                                                                     std::placeholders::_3);

    ASSERT_EXCEPTION((mergers.size() == inputArray->getArrayDesc().getAttributes().size()),
                     "Number of mergers != number of input array attributes");

    std::shared_ptr<Array> redistributed = sg::redistributeWithCallback(inputArray,
                                                                        chunkHandler,
                                                                        &mergers,
                                                                        outputArray->getArrayDesc().getDistribution(),
                                                                        outputArray->getArrayDesc().getResidency(),
                                                                        query,
                                                                        phyOp,
                                                                        enforceDataIntegrity);
    if (redistributed == inputArray) {
        outputArray->appendHorizontal(redistributed, newChunkCoordinates);
    }
}

std::shared_ptr<Array>
pullRedistributeInAttributeOrder(std::shared_ptr<Array>& inputArray,
                                 AttributeOrdering& attributeOrdering,
                                 const ArrayDistPtr& outputArrayDist,
                                 const ArrayResPtr& outputArrayRes,
                                 const std::shared_ptr<Query>& query,
                                 const std::shared_ptr<PhysicalOperator>& phyOp,
                                 bool enforceDataIntegrity)
{
    std::shared_ptr<Array> serializedArray = sg::getSerializedArray(inputArray,
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
    SCIDB_ASSERT(arrayToPull->getSupportedAccess()==Array::SINGLE_PASS);

    const ArrayDesc& multiAttrDesc = inputArray->getArrayDesc();
    const ArrayDesc& singleAttrDesc = arrayToPull->getArrayDesc();
    SCIDB_ASSERT(singleAttrDesc.getAttributes().size()<=2);
    SCIDB_ASSERT(singleAttrDesc.getAttributes().size()>0);

    // create a chunk merger that will convert chunk coordinates from the sereialized to the input dimensions

    PullSGArrayBlocking::ChunkHandler chunkHandler(std::bind<void>(sg::FailOnInvocation(),
                                                                   std::placeholders::_1,
                                                                   std::placeholders::_2,
                                                                   std::placeholders::_3));

    ArrayDesc outputArrayDesc(multiAttrDesc);
    outputArrayDesc.setDistribution(outputArrayDist);
    if (outputArrayRes) {
        outputArrayDesc.setResidency(outputArrayRes);
    } else {
        outputArrayDesc.setResidency(query->getDefaultArrayResidency());
    }

    std::shared_ptr<sg::SerializedArray::SerializedChunkMerger>
       handler(new sg::SerializedArray::SerializedChunkMerger(outputArrayDesc,
                                                              singleAttrDesc,
                                                              NULL,
                                                              chunkHandler,
                                                              enforceDataIntegrity));
    std::shared_ptr<scidb::MultiStreamArray::PartialChunkMerger> merger(handler);
    arrayToPull->setPartialChunkMerger(0, merger);
    SCIDB_ASSERT(!merger);
    SCIDB_ASSERT(handler);

    std::shared_ptr<PullSGArrayBlocking> sgArray(dynamic_pointer_cast<PullSGArrayBlocking>(serializedArray));

    std::shared_ptr<Array> resultArray = std::make_shared<sg::DeserializedArray>(sgArray,
                                                                                 outputArrayDesc,
                                                                                 attributeOrdering);
    return resultArray;
}


// This overload is a wrapper that provides a default SGInstanceLocator (and handles array residency
// if one is provided).
std::shared_ptr<Array>
pullRedistribute(std::shared_ptr<Array>& inputArray,
                 const ArrayDistPtr& outputArrayDist,
                 const ArrayResPtr& outputArrayRes,
                 const std::shared_ptr<Query>& query,
                 const std::shared_ptr<PhysicalOperator>& phyOp,
                 bool enforceDataIntegrity)
{
    static const char * funcName = "pullRedistribute(6arg): ";
    LOG4CXX_TRACE(PullSGArray::_logger, funcName << "begin");

    LOG4CXX_TRACE(PullSGArray::_logger, funcName << "distrib = " << outputArrayDist);
    SCIDB_ASSERT(outputArrayDist);

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
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "extra residency work");

        SGInstanceLocator instanceLocator(std::bind(static_cast<Func1>(&PullSGContext::instanceForChunk),
                                                    std::placeholders::_1,
                                                    std::placeholders::_2,
                                                    std::placeholders::_3,
                                                    outputArrayDist,
                                                    outputArrayRes));
        auto result = pullRedistribute(inputArray,
                                outputArrayDist,
                                outputArrayRes,
                                query,
                                phyOp,
                                instanceLocator,
                                enforceDataIntegrity);
        LOG4CXX_DEBUG(PullSGArray::_logger, funcName << "extra residency early return");
        return result;
    }

    SGInstanceLocator instanceLocator(std::bind(static_cast<Func2>(&PullSGContext::instanceForChunk),
                                                std::placeholders::_1,
                                                std::placeholders::_2,
                                                std::placeholders::_3,
                                                outputArrayDist));
    auto result = pullRedistribute(inputArray,
                            outputArrayDist,
                            ArrayResPtr(), // default query residency
                            query,
                            phyOp,
                            instanceLocator,
                            enforceDataIntegrity);
    LOG4CXX_DEBUG(PullSGArray::_logger, funcName << "end");
    return result;
}

std::shared_ptr<Array>
pullRedistribute(std::shared_ptr<Array>& inputArray,
                 const ArrayDistPtr& outputArrayDist,
                 const ArrayResPtr& outputArrayRes,
                 const std::shared_ptr<Query>& query,
                 const std::shared_ptr<PhysicalOperator>& phyOp,
                 SGInstanceLocator& instanceLocator,
                 bool enforceDataIntegrity)
{
    static const char * funcName = "pullRedistribute(7-arg il): ";
    const uint64_t instanceCount = query->getInstancesCount();

    ArrayDesc const& desc = inputArray->getArrayDesc();
    size_t nAttrs = desc.getAttributes().size();
    assert(nAttrs>0);
    bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);
    if (isEmptyable && desc.getEmptyBitmapAttribute()->getId() != nAttrs-1) {  // DJG
        throw USER_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_REDISTRIBUTE_ERROR1);
    }

    ArrayDesc outputArrayDesc(desc);
    outputArrayDesc.setDistribution(outputArrayDist);
    if (outputArrayRes) {
        outputArrayDesc.setResidency(outputArrayRes);
    } else {
        outputArrayDesc.setResidency(query->getDefaultArrayResidency());
    }
    SCIDB_ASSERT(phyOp->getOperatorID().isValid());

    LOG4CXX_TRACE(PullSGArray::_logger, funcName << " phyOp " << static_cast<void*>(phyOp.get())
                                                 << " opID " << phyOp->getOperatorID()
                                                 << " phyOp name " << phyOp->getPhysicalName() );

    ASSERT_EXCEPTION(!phyOp->getOperatorContext(), funcName); // cannot be used from two different places
                                                              // for the same operator, so must not be
                                                              // set yet

    LOG4CXX_TRACE(PullSGArray::_logger, funcName << "pre syncBarrier(0)");
    {
        ScopedWaitTimer timer(PTW_SWT_PULL_BARRIER_0, true); // debit PULL_BARRIER_0 specifically
        syncBarrier(0, query);                               // not whatever this debits
    }
    LOG4CXX_DEBUG(PullSGArray::_logger, funcName << "post syncBarrier(0)");

    // Creating result array with the same descriptor as the input one
    std::shared_ptr<PullSGArrayBlocking> pullArray =
       std::make_shared<PullSGArrayBlocking>(outputArrayDesc,
                                             query,
                                             phyOp,
                                             inputArray,
                                             enforceDataIntegrity);

    // Assigning result of this operation for current query and signal to concurrent handlers that they
    // can continue to work (after the barrier)
    SCIDB_ASSERT(instanceLocator);
    std::shared_ptr<PullSGContext> sgCtx =
       std::make_shared<PullSGContext>(inputArray,
                                       pullArray,
                                       instanceCount,
                                       instanceLocator);

    //
    // START THE SG QUEUE
    // TODO: currently handled by Query, hoping to transition to Operator soon.
    //
    SCIDB_ASSERT(sgCtx);

    // some instances can reach this point and start sending messages to us before we have
    // set our own operator context. so we need an additional barrier
    phyOp->setOperatorContext(sgCtx);   // future pathyway for starting queue
    {
        ScopedWaitTimer timer(PTW_SWT_PULL_BARRIER_1); // debit PULL_BARRIER_1 specifically
        syncBarrier(1, query);                         // not whatever this debits
    }

    LOG4CXX_DEBUG(PullSGArray::_logger, "pullRedistribute(): using query->startSGQueue()" );
    query->startSGQueue(sgCtx);
    return pullArray;
}

bool PullSGArray::isSerialized() const
{
    return (getArrayDesc().getAttributes().firstDataAttribute().getName() ==
            sg::SerializedArray::SERIALIZED_ATTR_NAME);
}

PullSGArrayBlocking::PullSGArrayBlocking(const ArrayDesc& arrayDesc,
                                         const std::shared_ptr<Query>& query,
                                         const std::shared_ptr<PhysicalOperator>& phyOp,
                                         const std::shared_ptr<Array>& inputSGArray,
                                         bool enforceDataIntegrity,
                                         uint32_t chunkPrefetchPerAttribute)
  : Array(inputSGArray),
    PullSGArray(arrayDesc, query, phyOp, enforceDataIntegrity, chunkPrefetchPerAttribute),
    _sgInputAccess(getPipe(0)->getSupportedAccess()),
    _nonBlockingMode(false)
{
    assert(_sgInputAccess>=Array::SINGLE_PASS &&
           _sgInputAccess<=Array::RANDOM);
}

std::shared_ptr<ConstArrayIterator>
PullSGArrayBlocking::getConstIterator(AttributeID attId) const
{
    const static char* funcName =  "PullSGArrayBlocking::getConstIterator: ";

    const size_t attrNum = _iterators.size(); // emptyBitmap included
    // Make sure that multiple attributes are NOT pulled simultaneously using this interface.
    // If the input to pullRedistribute() is a SINGLE_PASS array,
    // only a SINGLE attribute is allowed to be pulled by this interface.
    // To pull multiple attributes simultaneously, pullAttributes() must be used.
    // If the input to pullRedistribute() is a SINGLE_PASS array,
    // pullAttributes() must be used to pull ALL attributes only.
    for (size_t a=0; a < attrNum; ++a) {
        if (a!=attId && _iterators[a] &&
            (isInputSinglePass() || !_iterators[a]->end())) {
            ASSERT_EXCEPTION(false, string(funcName)+string("multiple attributes disallowed"));
        }
    }
    const auto& attrs = getArrayDesc().getAttributes();
    auto attr = attrs.findattr(attId);
    return PullSGArray::getConstIterator(attr.getId());  // kinda redundant, but stay with me
}

ConstChunk const*
PullSGArrayBlocking::nextChunk(const AttributeDesc& attId_in, MemChunk& memChunk)
{
    auto attId = attId_in.getId();
    static const char * funcName = "PullSGArrayBlocking::nextChunk: ";

    if (_nonBlockingMode) {
        return PullSGArray::nextChunk(attId_in, memChunk);
    }

    ConstChunk const* chunk(NULL);
    std::unordered_set<AttributeID> attributeSet;

    std::shared_ptr<SyncCtx> ctx = std::make_shared<SyncCtx>(_query);
    PullSGArray::RescheduleCallback cb = std::bind(&SyncCtx::signal,
                                                   ctx,
                                                   attId,
                                                   std::placeholders::_1);
    resetCallback(attId, cb);

    while (true) {
        try {
            chunk = PullSGArray::nextChunk(attId_in, memChunk);
            break;
        } catch (const scidb::StreamArray::RetryException& ) {
            LOG4CXX_TRACE(_logger,  funcName
                          << "caught RetryException while waiting for attId="<<attId);
            ctx->waitForActiveAttributes(attributeSet); // waiting here BLOCKS if this is running on mtFetch
                                                        // AND the input array is not SerialArray-dervied
            assert(attributeSet.size()==1);
            assert((*attributeSet.begin()) == attId);
        }
    }
    resetCallback(attId);
    validateIncomingChunk(chunk,attId);
    return chunk;
}

void
PullSGArrayBlocking::validateIncomingChunk(ConstChunk const* chunk,
                                           const AttributeID attId)
{
    if (isDebug() && chunk) {
        assert( isSerialized() ||
                (getArrayDesc().getEmptyBitmapAttribute() == NULL) ||
                (!chunk->isEmpty()));
        assert(chunk->getSize() > 0);

        assert(chunk->getAttributeDesc().getId() == attId);
        assert(attId < chunk->getArrayDesc().getAttributes().size());
    }
}

void PullSGArrayBlocking::sync()
{
    static const char * funcName = "PullSGArrayBlocking::sync: ";
    std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);

    std::shared_ptr<PhysicalOperator> phyOp = getPhysicalOperator();
    LOG4CXX_TRACE(_logger, funcName << " phyOp->getPhysicalName() " << phyOp->getPhysicalName()
                                                 << " phyOp->getOperatorID() " << phyOp->getOperatorID());
    std::shared_ptr<PullSGContext> sgCtx = dynamic_pointer_cast<PullSGContext>(phyOp->getOperatorContext());

    // check context sanity
    ASSERT_EXCEPTION((sgCtx && sgCtx->getResultArray().get() == this), funcName);
    ASSERT_EXCEPTION((PullSGArray::getConstIterator(0)->end()), funcName);

    syncSG(query); // make sure there are no outgoing messages in-flight
                   // TODO: wouldn't Query::syncSG() make more sense?
    syncBarrier(3, query); // because we are using an additional barrier
    LOG4CXX_DEBUG(_logger, funcName << "SG termination barrier reached.");

    // Reset SG Context to NULL
    phyOp->unsetOperatorContext(); // future plumbing

    LOG4CXX_DEBUG(_logger, funcName << "PullSG finished");
}

void
PullSGArrayBlocking::SyncCtx::signal(AttributeID attrId,
                                     const Exception* error)
{
    ScopedMutexLock cs(_mutex, PTW_SML_SG_PULL);
    _cond = true;
    if (error) {
        _error = error->clone();
    }
    _activeAttributes.insert(attrId);
    _ev.signal();
}

void
PullSGArrayBlocking::SyncCtx::waitForActiveAttributes(unordered_set<AttributeID>&
                                                      activeAttributes)
{
    ScopedMutexLock cs(_mutex, PTW_SML_SG_PULL);
    while(!_cond) {
        _ev.wait(_mutex, _ec, PTW_EVENT_SG_PULL);
    }
    if (_error) {
        _error->raise();
    }
    _cond = false;
    assert(!_activeAttributes.empty());
    activeAttributes.swap(_activeAttributes);
    assert(!activeAttributes.empty());
}


void PullSGArrayBlocking::pullAttributes(std::unordered_set<AttributeID>& attributesToPull,
                                         ChunkHandler& func)
{
    _nonBlockingMode = true;
    const static char* funcName =  "PullSGArrayBlocking::pullAttributes: ";
    if (isInputSinglePass()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << " isSinglePass() case for input: "
                      << typeid(*getPipe(0).get()).name());

        if (attributesToPull.size() != _iterators.size()) {
            stringstream ss; ss << funcName << " all attributes are required for SINGLE_PASS arrays";
            ASSERT_EXCEPTION(false, ss.str());
        }

        SinglePassArray* singlePass = dynamic_cast<SinglePassArray*>(getPipe(0).get());
        LOG4CXX_DEBUG(PullSGArray::_logger, funcName << " SinglePass singlePass "<< (void*)(singlePass));
        if(!singlePass) {
            stringstream ss; ss << funcName << "SinglePassArray required";
            ASSERT_EXCEPTION(false, ss.str());
        }
        if (!singlePass->isEnforceHorizontalIteration()) {
            stringstream ss; ss << funcName << "EnforceHorizontalIteration must be true";
            ASSERT_EXCEPTION(false, ss.str());
        }
    } else {
        LOG4CXX_DEBUG(PullSGArray::_logger, funcName << " NOT isSinglePass() case for input: "
                      << typeid(*getPipe(0).get()).name());
    }

    std::shared_ptr<SyncCtx> ctx = std::make_shared<SyncCtx>(_query);

    // set up per-attribute callbacks
    for (std::unordered_set<AttributeID>::const_iterator i = attributesToPull.begin();
         i != attributesToPull.end(); ++i) {
        const AttributeID attId = *i;
        SCIDB_ASSERT(attId<_iterators.size());
        if (_iterators[attId])  {
            stringstream ss; ss << funcName << "attribute "<< attId << " already pulled";
            ASSERT_EXCEPTION(false, ss.str());
        }
        PullSGArray::RescheduleCallback cb = std::bind(&SyncCtx::signal,
                                                       ctx,
                                                       attId,
                                                       std::placeholders::_1);
        resetCallback(attId, cb);
    }

    std::unordered_set<AttributeID> activeAttributes(attributesToPull);

    while (!attributesToPull.empty()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName
                      << " active attrs size="<<activeAttributes.size());
        for (std::unordered_set<AttributeID>::iterator iter = activeAttributes.begin();
             iter != activeAttributes.end(); ) {
            const AttributeID attId = *iter;
            bool eof = false;
            try {
                eof = pullChunk(func,attId);
            } catch (const scidb::MultiStreamArray::RetryException& ) {
                std::unordered_set<AttributeID>::iterator iterToErase = iter;
                ++iter;
                activeAttributes.erase(iterToErase);
                continue;
            }

            if (eof) {
                std::unordered_set<AttributeID>::iterator iterToErase = iter;
                ++iter;
                activeAttributes.erase(iterToErase);
                resetCallback(attId);
                attributesToPull.erase(attId);
                LOG4CXX_DEBUG(PullSGArray::_logger, funcName
                              << "EOF attId="<< attId
                              <<", remain="<<attributesToPull.size());
                continue;
            }
            ++iter;
        }

        // wait for a result
        if (!attributesToPull.empty() &&
            activeAttributes.empty()) {
            LOG4CXX_TRACE(PullSGArray::_logger,  funcName
                          << "waiting, active attrs size="<<activeAttributes.size());
            ctx->waitForActiveAttributes(activeAttributes);
        }
    }
    _nonBlockingMode = false;
}

bool PullSGArrayBlocking::pullChunk(ChunkHandler& chunkHandler,
                                    const AttributeID attId)
{
    const static char* funcName =  "PullSGArrayBlocking::pullChunk: ";
    if (isDebug()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "trying to consume chunk for attId="<<attId);
    }
    std::shared_ptr<ConstArrayIterator> arrIter = PullSGArray::getConstIterator(attId);
    if (arrIter->end()) {
        LOG4CXX_DEBUG(PullSGArray::_logger,  funcName << "EOF attId="<<attId);
        return true;
    }
    const ConstChunk& chunk = arrIter->getChunk();
    validateIncomingChunk(&chunk, attId);

    if (isDebug()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "trying to consume chunk for pos="
                      << CoordsToStr(chunk.getFirstPosition(false)));
    }

    std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
    chunkHandler(attId, chunk, query);

    if (isDebug()) {
        LOG4CXX_TRACE(PullSGArray::_logger, funcName << "advanced attId="<<attId);
    }
    return false;
}

} // namespace
