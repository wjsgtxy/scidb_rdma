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

#ifndef SCIDB_CLIENT

#include "RemoteMergedArray.h"

#include <array/CompressedBuffer.h>
#include <array/PinBuffer.h>
#include <network/MessageDesc.h>
#include <network/MessageHandleJob.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Query.h>
#include <system/Warnings.h>

namespace scidb {

using std::vector;
using std::stringstream;
using std::make_shared;
using std::dynamic_pointer_cast;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.RemoteMergedArray"));

/* R E M O T E   M E R G E D   A R R A Y */

RemoteMergedArray::RemoteMergedArray(const ArrayDesc& arrayDesc,
                                     const std::shared_ptr<Query>& query)
 : Array(query->getCurrentResultArray()),
   MultiStreamArray(query->getInstancesCount(), query->getInstanceID(), arrayDesc, false, query),
  _callbacks(arrayDesc.getAttributes().size()),
  _query(query),
  _messages(arrayDesc.getAttributes().size(), vector< MessageState >(getStreamCount()))
{
    static const size_t MAX_MUTEX_NUM = 100;
    const size_t nMutexes = std::min(arrayDesc.getAttributes().size(), MAX_MUTEX_NUM);
    _mutexes.resize(nMutexes);
}

std::shared_ptr<RemoteMergedArray>
RemoteMergedArray::create(const ArrayDesc& arrayDesc,
                          QueryID queryId)
{
    std::shared_ptr<Query> query = Query::getQueryByID(queryId);
    std::shared_ptr<RemoteMergedArray> array =
       std::shared_ptr<RemoteMergedArray>(new RemoteMergedArray(arrayDesc, query));
    query->setMergedArray(array);
    return array;
}



namespace {
template<typename T>
void logMatrix(std::vector<std::vector<T> >& matrix, const std::string& prefix)
{
    if (!logger->isTraceEnabled()) {
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
    LOG4CXX_TRACE(logger, prefix << ": " << ss.str());
}
}

void
RemoteMergedArray::requestNextChunk(size_t stream, AttributeID attId, bool positionOnly)
{
    static const char* funcName = "RemoteMergedArray::requestNextChunk: ";
    if (_query->getInstanceID() == stream) {
        return;
    }
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()], PTW_SML_RA);

        logMatrix(_messages, "RemoteMergedArray::requestNextChunk(): _messages");

        if (positionOnly && _messages[attId][stream]._message) {
            // we must already have the position
            assert(_messages[attId][stream]._hasPosition);
            return;
        }

        if (!_messages[attId][stream]._hasPosition) {
            // already requested
            assert(!_messages[attId][stream]._message);
            return;
        }

        LOG4CXX_TRACE(logger, funcName << " request next chunk attId=" << attId
                     << (positionOnly? ", position only" : ", full")
                     << ", stream #" << stream);

        std::shared_ptr<MessageDesc>  chunkDesc = _messages[attId][stream]._message;
        if (chunkDesc) {
            std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
            if (!chunkMsg->has_next() || chunkMsg->eof()) {
                // nothing to request
                return;
            }
        }

        _messages[attId][stream]._hasPosition = false;
        _messages[attId][stream]._message.reset();
    }
    std::shared_ptr<MessageDesc> fetchDesc = std::make_shared<MessageDesc>(mtFetch);
    std::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_query->getQueryID());
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(positionOnly);
    fetchRecord->set_obj_type(MERGED_ARRAY_OBJ_TYPE);

    int priority = static_cast<MessageHandleJob*>(
        Job::getCurrentJobPerThread().get())->getPriority();
    fetchRecord->mutable_session_info()->set_job_priority(priority);

    NetworkManager::getInstance()->send(stream, fetchDesc);
}

void
RemoteMergedArray::handleChunkMsg(std::shared_ptr< MessageDesc>& chunkDesc)
{
    static const char* funcName = "RemoteMergedArray::handleChunkMsg: ";
    assert(chunkDesc->getMessageType() == mtRemoteChunk);

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    size_t stream = size_t(_query->mapPhysicalToLogical(chunkDesc->getSourceInstanceID()));

    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    if (chunkMsg->warnings_size())
    {
        for (int i = 0; i < chunkMsg->warnings_size(); ++i)
        {
            const ::scidb_msg::Chunk_Warning& w = chunkMsg->warnings(i);
            _query->postWarning(
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }
    }
    RescheduleCallback cb;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()], PTW_SML_RA);
        LOG4CXX_TRACE(logger,  funcName << "received next chunk message attId="<<attId
                     <<", stream="<<stream
                     <<", queryID="<<_query->getQueryID());
        logMatrix(_messages, "RemoteMergedArray::handleChunkMsg: _messages");

        assert(!_messages[attId][stream]._message);
        assert(!_messages[attId][stream]._hasPosition);

        _messages[attId][stream]._message = chunkDesc;
        _messages[attId][stream]._hasPosition = true;

        assert(_messages[attId][stream]._message);
        assert(_messages[attId][stream]._hasPosition);
        cb = _callbacks[attId];
    }
    if (cb) {
        const Exception* error(NULL);
        cb(error);
    } else {
        _query->validate();
        LOG4CXX_TRACE(logger, funcName << "no callback is set attId="<<attId
                      <<", stream="<<stream
                      <<", queryID="<<_query->getQueryID());
    }
}

bool
RemoteMergedArray::getChunk(size_t stream, AttributeID attId, MemChunk* chunk)
{
    static const char* funcName = "RemoteMergedArray::getChunk: ";
    assert(chunk);
    std::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()], PTW_SML_RA);
        chunkDesc = _messages[attId][stream]._message;

        logMatrix(_messages, "RemoteMergedArray::getChunk: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, funcName << "found next chunk message stream="<<stream<<", attId="<<attId);
        assert(chunk != NULL);
        ASSERT_EXCEPTION(chunkDesc->getBinary().get()!=nullptr, funcName);

        const CompressorType compMethod = static_cast<CompressorType>(chunkMsg->compression_method());
        const size_t decompressedSize = chunkMsg->decompressed_size(); // dz 之前我在rdma 收到消息的解析里面，没有设置这两个参数

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk->initialize(this, &desc, firstElem, compMethod);
        chunk->setCount(chunkMsg->count());

        std::shared_ptr<CompressedBuffer> compressedBuffer =
           dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk->decompress(*compressedBuffer);
        checkChunkMagic(*chunk, __PRETTY_FUNCTION__); // dz 这里throw了
        return true;
    }
    else
    {
        LOG4CXX_TRACE(logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

bool
RemoteMergedArray::getPos(size_t stream, AttributeID attId, Coordinates& pos)
{
    static const char* funcName = "RemoteMergedArray::getPos: ";
    std::shared_ptr<MessageDesc> chunkDesc;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()], PTW_SML_RA);
        chunkDesc = _messages[attId][stream]._message;
        logMatrix(_messages, "RemoteMergedArray::getPos: _messages");

    }
    if (!chunkDesc) {
        throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
    }

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, funcName << "checking for position stream="<<stream<<", attId="<<attId);
        if (chunkMsg->has_next())
        {
            for (int i = 0; i < chunkMsg->next_coordinates_size(); i++) {
                pos.push_back(chunkMsg->next_coordinates(i));
            }
            LOG4CXX_TRACE(logger, funcName << "found next position stream="<<stream
                          <<", attId="<<attId<<", pos="<<pos);
            return true;
        }
        return false; //no position => eof
    }
    else
    {
        LOG4CXX_TRACE(logger, funcName << "EOF chunk stream="<<stream<<", attId="<<attId);
        return false;
    }
}

bool
RemoteMergedArray::fetchChunk(size_t stream, AttributeID attId, MemChunk* chunk)
{
    assert(stream < getStreamCount());
    assert(chunk);
    if (_query->getInstanceID() != stream) {

        return getChunk(stream, attId, chunk);

    } else {
        // We get chunk body from the current result array on local instance
        bool result = false;
        const auto& attr = _localArray()->getArrayDesc().getAttributes().findattr(attId);
        if (!_localArray()->getConstIterator(attr)->end()) {
            {   // PinBuffer scope
                const ConstChunk& srcChunk = _localArray()->getConstIterator(attr)->getChunk();
                PinBuffer buf(srcChunk);

                Address firstElem;
                firstElem.attId = attId;
                firstElem.coords = srcChunk.getFirstPosition(false);
                chunk->initialize(this, &desc, firstElem, srcChunk.getCompressionMethod());
                if (!srcChunk.getAttributeDesc().isEmptyIndicator() &&
                    getArrayDesc().getEmptyBitmapAttribute() != NULL &&
                    srcChunk.getBitmapSize() == 0) {
                    checkChunkMagic(srcChunk, __PRETTY_FUNCTION__);
                    srcChunk.makeClosure(*chunk, srcChunk.getEmptyBitmap());
                } else {
                    // MemChunk::allocate
                    chunk->allocate(srcChunk.getSize());
                    // Copy the MemChunk::data field bit-for-bit from one to the other
                    memcpy(chunk->getWriteData(), srcChunk.getConstData(), srcChunk.getSize());
                }
                // This is a no-op when chunk is a MemChunk
                chunk->write(_query);
            }
            ++(*_localArray()->getConstIterator(attr));
            result = true;
        }
        return result;
    }
}

bool
RemoteMergedArray::fetchPosition(size_t stream, AttributeID attId, Coordinates& position)
{
   if (_query->getInstanceID() != stream) {

       return getPos(stream, attId, position);

   } else {
       // We get chunk body from the current result array on local instance
       bool result = false;
       const auto& attr = _localArray()->getArrayDesc().getAttributes().findattr(attId);
       if (!_localArray()->getConstIterator(attr)->end()) {
           position = _localArray()->getConstIterator(attr)->getPosition();
           result = true;
       } else {
           result = false;
       }
       return result;
   }
}

ConstChunk const*
RemoteMergedArray::nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());

    bool result = fetchChunk(stream, attId, &chunk);
    return (result ? &chunk : NULL);
}

bool
RemoteMergedArray::nextChunkPos(size_t stream, AttributeID attId, Coordinates& pos, size_t& destStream)
{
    assert(stream < getStreamCount());
    assert(attId < _messages.size());
    static const bool positionOnly = true;
    requestNextChunk(stream, attId, positionOnly);

    bool result = fetchPosition(stream, attId, pos);

    requestNextChunk(stream, attId, false);
    destStream = _query->getInstanceID();
    return result;
}

std::shared_ptr<ConstArrayIterator>
RemoteMergedArray::getConstIterator(AttributeID attId) const
{
    assert(attId < _messages.size());

    const StreamArray* pself = this;
    StreamArray* self = const_cast<StreamArray*>(pself);

    if (!_iterators[attId]) {
        std::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, attId));
        std::shared_ptr<ConstArrayIterator>& iter =
           const_cast<std::shared_ptr<ConstArrayIterator>&>(_iterators[attId]);
        iter = cai;
        LOG4CXX_TRACE(logger,
                      "RemoteMergedArray::getConstIterator(): new iterator attId="<<attId);
    } else {
        if (!_iterators[attId]->end()) {
            LOG4CXX_TRACE(logger,
                          "RemoteMergedArray::getConstIterator(): increment attId="<<attId);
            ++(*_iterators[attId]);
        }
    }

    return _iterators[attId];
}

RemoteMergedArray::RescheduleCallback
RemoteMergedArray::resetCallback(AttributeID attId)
{
    RescheduleCallback cb;
    return resetCallback(attId, cb);
}

RemoteMergedArray::RescheduleCallback
RemoteMergedArray::resetCallback(AttributeID attId,
                                                    const RescheduleCallback& newCb)
{
    assert(attId < _callbacks.size());
    RescheduleCallback oldCb;
    {
        ScopedMutexLock lock(_mutexes[attId % _mutexes.size()], PTW_SML_RA);
        _callbacks[attId].swap(oldCb);
        _callbacks[attId] = newCb;
    }
    return oldCb;
}

RemoteMergedArray::MessageState::MessageState()
    : _hasPosition(true)
{}

std::ostream& operator<<(std::ostream& out, RemoteMergedArray::MessageState& state)
{
    out << "[" << state._hasPosition << "," << state._message.get() << "]";
    return out;
}

}  // scidb
#endif
