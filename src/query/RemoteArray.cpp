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
 * @file RemoteArray.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include "RemoteArray.h"

#include <memory>

#include <log4cxx/logger.h>

#include <array/CompressedBuffer.h>
#include <network/proto/scidb_msg.pb.h>
#include <network/NetworkManager.h>
#include <network/MessageDesc.h>
#include <query/OperatorID.h>

using namespace std;

namespace scidb
{

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.remotearray"));

RemoteArrayContext::RemoteArrayContext(size_t numInstances):
    _inboundArrays(numInstances), _outboundArrays(numInstances)
{}

std::shared_ptr<RemoteArray> RemoteArrayContext::getInboundArray(InstanceID logicalSrcInstanceID) const
{
    assert(!_inboundArrays.empty());
    assert(logicalSrcInstanceID < _inboundArrays.size());
    return _inboundArrays[logicalSrcInstanceID];
}

void RemoteArrayContext::setInboundArray(InstanceID logicalSrcInstanceID, const std::shared_ptr<RemoteArray>& array)
{
    assert(!_inboundArrays.empty());
    assert(logicalSrcInstanceID < _inboundArrays.size());
    _inboundArrays[logicalSrcInstanceID] = array;
}

std::shared_ptr<Array> RemoteArrayContext::getOutboundArray(const InstanceID& logicalDestInstanceID) const
{
    assert(!_outboundArrays.empty());
    assert(logicalDestInstanceID < _outboundArrays.size());
    return _outboundArrays[logicalDestInstanceID];
}

void RemoteArrayContext::setOutboundArray(const InstanceID& logicalDestInstanceID, const std::shared_ptr<Array>& array)
{
    assert(!_outboundArrays.empty());
    assert(logicalDestInstanceID < _outboundArrays.size());
    _outboundArrays[logicalDestInstanceID] = array;
}

std::shared_ptr<RemoteArray> RemoteArray::create(
        std::shared_ptr<RemoteArrayContext>& remoteArrayContext,
        const ArrayDesc& arrayDesc, QueryID queryId, OperatorID phyOpID, InstanceID logicalSrcInstanceID)
{
    // Note: if make_shared is used here, you will get a compilation error saying RemoteArray::RemoteArray is private.
    std::shared_ptr<RemoteArray> array = std::shared_ptr<RemoteArray>(new RemoteArray(arrayDesc, queryId, phyOpID, logicalSrcInstanceID));
    remoteArrayContext->setInboundArray(logicalSrcInstanceID, array);
    return array;
}

RemoteArray::RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, OperatorID phyOpID, InstanceID logicalSrcInstanceID)
: StreamArray(arrayDesc), _queryId(queryId), _phyOpID(phyOpID), _instanceID(logicalSrcInstanceID),
  _received(arrayDesc.getAttributes().size(), Semaphore()),
  _messages(arrayDesc.getAttributes().size()),
  _requested(arrayDesc.getAttributes().size())
{
}

std::shared_ptr<RemoteArrayContext> RemoteArray::getContext(std::shared_ptr<OperatorContext>& opContext)
{
    auto result = dynamic_pointer_cast<RemoteArrayContext>(opContext);
    ASSERT_EXCEPTION(result, "RemoteArray::getContext failed.");
    return result;
}

void RemoteArray::requestNextChunk(AttributeID attId)
{
    LOG4CXX_TRACE(logger, "RemoteArray fetches next chunk of " << attId << " attribute");
    std::shared_ptr<MessageDesc> fetchDesc = std::make_shared<MessageDesc>(mtFetch);
    std::shared_ptr<scidb_msg::Fetch> fetchRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDesc->setQueryID(_queryId);
    SCIDB_ASSERT(_phyOpID.isValid());
    fetchDesc->setPhysicalOperatorID(_phyOpID);
    fetchRecord->set_attribute_id(attId);
    fetchRecord->set_position_only(false);
    fetchRecord->set_obj_type(0);
    NetworkManager::getInstance()->send(_instanceID, fetchDesc);
}

void RemoteArray::handleChunkMsg(std::shared_ptr<MessageDesc>& chunkDesc)
{
    assert(chunkDesc->getMessageType() == mtRemoteChunk);
    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();
    AttributeID attId = chunkMsg->attribute_id();
    assert(attId < _messages.size());
    assert(attId < _received.size());
    _messages[attId] = chunkDesc;
    _received[attId].release();
}

bool RemoteArray::proceedChunkMsg(AttributeID attId, MemChunk& chunk)
{
    std::shared_ptr<MessageDesc>  chunkDesc = _messages[attId];
    _messages[attId].reset();

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "RemoteArray received next chunk message");
        assert(chunkDesc->getBinary());

        const CompressorType compMethod = static_cast<CompressorType>(chunkMsg->compression_method());
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk.initialize(this, &desc, firstElem, compMethod);
        chunk.setCount(chunkMsg->count());

        std::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk.decompress(*compressedBuffer);
        LOG4CXX_TRACE(logger, "RemoteArray initializes next chunk");

        requestNextChunk(attId);
        return true;
    }
    else
    {
        return false;
    }
}


ConstChunk const* RemoteArray::nextChunk(const AttributeDesc& attId_in, MemChunk& chunk)
{
    auto attId = attId_in.getId();
    if (!_requested[attId]) {
        requestNextChunk(attId);
    }
    std::shared_ptr<Query> query = Query::getQueryByID(_queryId);
    Semaphore::ErrorChecker errorChecker = std::bind(&Query::validateQueryPtr, query);
    _received[attId].enter(errorChecker, PTW_SEM_NET_RCV_R);
    _requested[attId] = true;
    return proceedChunkMsg(attId, chunk) ? &chunk : NULL;
}


} // namespace
