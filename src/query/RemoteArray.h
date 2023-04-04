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
 * @file RemoteArray.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef REMOTEARRAY_H_
#define REMOTEARRAY_H_

#include <memory>

#include <array/StreamArray.h>
#include <query/OperatorID.h>
#include <query/Query.h>
#include <util/Mutex.h>

namespace scidb
{
class MessageDesc;

/**
 * A sub-class of OperatorContext, to share data using RemoteArray among instances.
 * Even though multiple threads access the RemoteArrayContext, no synchronization is needed, for the following reasons.
 * There are two types of threads:
 *   - A "writer" thread which creates this context, and calls query->setOperatorContext/unsetOperatorContext;
 *   - and multiple "reader" threads on either side of the channel of RemoteArray:
 *     * A pullee responds to a mtFetch message, by returning a chunk from _outboundArrays.
 *     * A puller responds to a mtRemoteChunk message, by adding the received chunk to an array referenced in _inboundArrays.
 * The synchronization of the writer thread and the reader threads should be protected using syncBarrier in the writer thread itself as:
 *   1. syncBarrier(0)
 *   2. prepare a std::shared_ptr<RemoteArrayContext>
 *   3. query->setOperatorContext()
 *   4. NOW mtFetch/mtRemoteChunk MESSAGES MAY BE EXCHANGED.
 *   5. syncBarrier(1)
 *   6. query->unsetOperatorContext()
 */
class RemoteArrayContext: public OperatorContext
{
public:
    /**
     * @param numInstances  the number of SciDB instances.
     */
    RemoteArrayContext(size_t numInstances);

    /**
     * Given a source instance, get the remote array to pull data from the instance.
     * @param logicalSrcInstanceID  the logical source instance ID.
     * @return a RemoteArray to pull data from.
     */
    std::shared_ptr<RemoteArray> getInboundArray(InstanceID logicalSrcInstanceID) const;

    /**
     * Given a source instance, and an array, take a note that the array is meant to pull data from that instance.
     * @param logicalSrcInstanceID  the logical source instance ID.
     * @param array          a RemoteArray to pull data from.
     */
    void setInboundArray(InstanceID logicalSrcInstanceID, const std::shared_ptr<RemoteArray>& array);

    /**
     * Given a destination instance, get the outbound array to be sent to the instance.
     * @param logicalDestInstanceID  the logical destination instance ID.
     * @return an outbound array prepared for the instance.
     */
    std::shared_ptr<Array> getOutboundArray(const InstanceID& logicalDestInstanceID) const;

    /**
     * Given a destination instance, and an array, take a note that the array is meant to be sent to the instance.
     * @param logicalDestInstanceID  the logical destination instance ID.
     * @param array           an SciDB array to be sent to the instance.
     */
    void setOutboundArray(const InstanceID& logicalDestInstanceID, const std::shared_ptr<Array>& array);

private:
    /**
     * A vector of RemoteArrays, to pull data from each remote instance.
     */
    std::vector<std::shared_ptr<RemoteArray> > _inboundArrays;

    /**
     * A vector of outbound arrays, to send data to each remote instance.
     */
    std::vector<std::shared_ptr<Array> > _outboundArrays;
};

/**
 * Class implement fetching chunks from current result array of remote instance.
 */
class RemoteArray: public StreamArray
{
public:
     /// scidb_msg::Chunk/Fetch::obj_type
    static const uint32_t REMOTE_ARRAY_OBJ_TYPE = 0;

    void handleChunkMsg(std::shared_ptr< MessageDesc>& chunkDesc);

    /**
     * Create a RemoteArray object, store it in remoteArrayContext, and return it.
     * @param[inout] remoteArrayContext   an RemoteArrayContext object, whose _inboundArrays[logicalSrcInstanceID] will be set.
     * @param[in]    arrayDesc            the schema of the RemoteArray.
     * @param[in]    queryId              the ID of the query context.
     * @param[in]     phyOp               the PhysicalOperator for the SG
     * @param[in]    logicalSrcInstanceId the logical ID of the instance to pull data from.
     * @return a shared_ptr to the RemoteArray object (which is already stored in the RemoteArrayContext).
     */
    static std::shared_ptr<RemoteArray> create(
            std::shared_ptr<RemoteArrayContext>& remoteArrayContext,
            const ArrayDesc& arrayDesc, QueryID queryId, OperatorID phyOpID, InstanceID instanceID);

    static std::shared_ptr<RemoteArrayContext> getContext(std::shared_ptr<OperatorContext>&);

private:
    bool proceedChunkMsg(AttributeID attId, MemChunk& chunk);
    void requestNextChunk(AttributeID attId);

    /**
     * This is private because the caller is supposed to call RemoteArray::create to create a RemoteArray object.
     */
    RemoteArray(const ArrayDesc& arrayDesc, QueryID queryId, OperatorID phyOpID, InstanceID logicalSrcInstanceID);

    QueryID _queryId;
    OperatorID _phyOpID;
    InstanceID _instanceID;
    std::vector<Semaphore> _received;
    std::vector<std::shared_ptr<MessageDesc> > _messages;
    std::vector<uint8_t> _requested;

    // overloaded method
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;
};


} // namespace

#endif /* REMOTEARRAY_H_ */
