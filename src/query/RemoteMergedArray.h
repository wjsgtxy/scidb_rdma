#ifndef ___REMOTEMERGEDARRAY_H___
#define ___REMOTEMERGEDARRAY_H___
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

#include <array/MultiStreamArray.h>

namespace scidb {

class MessageDesc;

/**
 * RemoteMergedArray collects partial chunks from other instances and merges them to produce complete chunks.
 * Its nextChunk() is allowed to throw RetryException
 * indicating that nextChunk() should be called again to obtain the chunk.
 */
class RemoteMergedArray: public MultiStreamArray
{

public:

    /// scidb_msg::Chunk/Fetch::obj_type
    static const uint32_t MERGED_ARRAY_OBJ_TYPE = 1;

    /**
     * Handle a remote instance message containing a chunk and/or position
     * @param chunkDesc the message structure
     */
    void handleChunkMsg(std::shared_ptr<MessageDesc>& chunkDesc);

    /// Factory method
    static std::shared_ptr<RemoteMergedArray> create(const ArrayDesc& arrayDesc, QueryID queryId);

    /**
     * @see scidb::ConstArrayIterator
     * This implementation always returns the same iterator object.
     * It is created on the first invocation and incremented(operator++()) on the subsequent
     */
    std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attId) const ;

    /**
     * Callback to invoke when a remote chunk becomes available
     * @param error if not NULL, specifies an error preventing retrieval of the remote chunk
     */
    typedef std::function<void(const scidb::Exception* error)> RescheduleCallback;

    /**
     * Remove the callback for chunks of a given attribute
     * @param attId attribute ID
     * @return the removed callback
     */
    RescheduleCallback resetCallback(AttributeID attId);

    /**
     * Replace the callback for chunks of a given attribute
     * @param attId attribute ID
     * @param newCb the new callback
     * @return the old callback
     */
    RescheduleCallback resetCallback(AttributeID attId, const RescheduleCallback& newCb);

protected:

    /**
     * Get the next chunk from a given stream/instance
     * @param stream ID which corresponds to a source instance
     * @param attId attribute ID
     * @param chunk that can be used to copy the result
     * @return the requested chunk (may not be the same as the 'chunk' argument)
     * @throws scidb::MultiStreamArray::RetryException if the chunk is not yet ready
     */
    virtual ConstChunk const* nextChunkBody(size_t stream,
                                            AttributeID attId,
                                            MemChunk& chunk);

    /**
     * Get the next chunk position from a given stream/instance
     * @param stream ID which corresponds to a source instance
     * @param attId attribute ID
     * @param pos [out] requested position
     * @param destStream [out] destination for the chunk @ pos
     * @return false if no more positions are available (EOF); true otherwise
     * @throws scidb::MultiStreamArray::RetryException if the position is not yet ready
     */
    virtual bool nextChunkPos(size_t stream,
                              AttributeID attId,
                              Coordinates& pos,
                              size_t& destStream);
private:

    /**
     * Request the next chunk or position from a remote stream.
     * This implementation piggy-backs the position on the chunk message.
     * Every chunk message carries the position of the subsequent chunk.
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param positionOnly is set to true if only the position is requested
     * @return
     */
    void requestNextChunk(size_t stream, AttributeID attId, bool positionOnly);

    /**
     * Construct and return the current chunk from a given remote stream/instance
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param chunk [out] chunk to be populated
     * @return false if no more chunks are available (EOF), true otherwise
     */
    bool getChunk(size_t stream, AttributeID attId, MemChunk* chunk);

    /**
     * Get the next remote chunk position
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param pos [out] position to be populated
     * @return false if no more positions are available (EOF), true otherwise
     */
    bool getPos(size_t stream, AttributeID attId, Coordinates& pos);

    /**
     * Get the next chunk position (local or remote)
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param position [out] position to be populated
     * @return false if no more positions are available (EOF), true otherwise
     */
    bool fetchPosition(size_t stream, AttributeID attId, Coordinates& position);

    /**
     * Construct and return the current chunk (local or remote)
     * @param stream ID which corresponds to a remote instance
     * @param attId attribute ID
     * @param chunk [out] chunk to be populated
     * @return false if no more chunks are available (EOF), true otherwise
     */
    bool fetchChunk(size_t stream, AttributeID attId, MemChunk* chunk);

    /**
     * Constructor
     * @param arrayDesc array descriptor (aka schema)
     * @param query the query context
     */
    RemoteMergedArray(const ArrayDesc& arrayDesc,
                      const std::shared_ptr<Query>& query);

    std::vector<RescheduleCallback > _callbacks;
    std::shared_ptr<Query> _query;
    std::vector<Mutex> _mutexes;
    struct MessageState
    {
        MessageState();
        std::shared_ptr<MessageDesc> _message;
        bool _hasPosition; // false if position has been requested but not yet available (except for the very first time)
    };
    friend std::ostream& operator << (std::ostream& out,
                                      RemoteMergedArray::MessageState& state);

    std::vector< std::vector< MessageState > > _messages;
    std::shared_ptr<Array> _localArray() { return getPipe(0); }
};
}  // scidb
#endif  // ifndef SCIDB_CLIENT
#endif
