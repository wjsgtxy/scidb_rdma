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
 * @file ReplicationManager.cpp
 *
 * @details Simple replication manager based on the original version
 * that was included in the old storage manager code.  See the wiki
 * page DEV/How+Chunk+Replication+Works for an overview.
 */

#include <array/ReplicationMgr.h>

#include <array/ArrayDesc.h>
#include <array/ArrayDistribution.h>
#include <array/CompressedBuffer.h>
#include <system/Config.h>
#include <storage/StorageMgr.h>
#include <network/NetworkManager.h>
#include <network/MessageDesc.h>
#include <network/MessageUtils.h>

using namespace std;
namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.replication"));

const int MAX_INSTANCE_BITS = 10; // 2^MAX_INSTANCE_BITS = max number of instances

/**
 * Fibonacci hash for a 64 bit key
 * @param key to hash
 * @param fib_B = log2(max_num_of_buckets)
 * @return hash = bucket index
 */
static uint64_t fibHash64(const uint64_t key, const uint64_t fib_B)
{
    assert(fib_B < 64);
    const uint64_t fib_A64 = (uint64_t) 11400714819323198485U;
    return (key * fib_A64) >> (64 - fib_B);
}

ReplicationManager::ReplicationManager()
:
    _repEvent(),
    _lsnrId(0)
{}

void ReplicationManager::start(const std::shared_ptr<JobQueue>& jobQueue)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    assert(!_lsnrId);
    Notification<NetworkManager::ConnectionStatus>::Subscriber pListener =
        std::bind(&ReplicationManager::handleConnectionStatus,
                  this,  // bare pointer because RM should never go away
                  std::placeholders::_1);
    _lsnrId = Notification<NetworkManager::ConnectionStatus>::subscribe(pListener);
    assert(_lsnrId);

    // initialize inbound replication queue
    int size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
    assert(size>0);
    size = (size<1) ? 4 : size+4; // allow some minimal extra space to tolerate mild overflows
    // this queue is single-threaded because the order of replicas is important (per source)
    // and CachedStorage serializes everything anyway via THE mutex.
    _inboundReplicationQ = std::make_shared<WorkQueue>(jobQueue, "Replication Queue",
                                                       1, static_cast<uint64_t>(size));
}

void ReplicationManager::stop()
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    Notification<NetworkManager::ConnectionStatus>::unsubscribe(_lsnrId);
    _lsnrId = 0;
    clear();
    _repEvent.signal();
}

void ReplicationManager::send(const std::shared_ptr<Item>& item)
{
    assert(item);
    assert(!item->isDone());
    assert(_lsnrId);

    ScopedMutexLock cs(_repMutex, PTW_SML_REP);
    std::shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    if (!ri) {
        ri = std::shared_ptr<RepItems>(new RepItems);
    }
    ri->push_back(item);

    // If mine is the only item, or next-item-to-transmit got stalled
    // by Connection::Channel flow control, then try to transmit.
    if (ri->size() == 1 || ri->front()->isStalled()) {
        sendItem(*ri);
    }
}

// If this predicate returns false, the _repEvent.wait() call below
// will finish even if the event was not signalled.  See scidb::Event.
bool ReplicationManager::keepWaiting(const std::shared_ptr<Item>& item)
{
    assert(getInstance()->_repMutex.isLockedByThisThread());
    assert(item);
    if (item->isDone()) {
        return false;
    }
    return Query::isValidQueryPtr(item->getQuery());
}

void ReplicationManager::wait(const std::shared_ptr<Item>& item)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    assert(_lsnrId);

    if (item->isDone()) {
        item->throwIfError();
        return;
    }

    std::shared_ptr<RepItems>& ri = _repQueue[item->getInstanceId()];
    assert(ri);

    Event::ErrorChecker ec = std::bind(&ReplicationManager::keepWaiting, item);

    while (true) {

        LOG4CXX_TRACE(logger, "ReplicationManager::wait: about to wait for instance=" << Iid(item->getInstanceId())
                  << ", item @" << (void*)item.get()
                  << ", size=" << item->getChunkMsg()->getMessageSize()
                  << ", query (" << item->getChunkMsg()->getQueryID()<<")"
                  << ", queue size="<< ri->size());
        assert(!ri->empty());

        bool mine = (ri->front() == item);
        bool done = sendItem(*ri);
        if (mine && done) {
            assert(item->isDone());
            item->throwIfError();
            return;
        }
        if (!done) {
            // Nothing sent, likely because of flow control
            // back-pressure.  Wait for the _repEvent, which we hope is
            // a ConnectionStatus update from the channel's flow control
            // machinery.
            try {
                if (hasInjectedError(REPLICA_WAIT, __LINE__, __FILE__)) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
                }
                _repEvent.wait(_repMutex, ec, PTW_EVENT_REP);
            } catch (Exception& e) {
                item->setDone(e.clone());
                e.raise();
            }
        }
        if (item->isDone()) {
            item->throwIfError();
            return;
        }
    }
}

void ReplicationManager::handleConnectionStatus(
    Notification<NetworkManager::ConnectionStatus>::MessageTypePtr connStatus)
{
    assert(connStatus->getPhysicalInstanceId() != INVALID_INSTANCE);

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: notification for instance="
                  << connStatus->getPhysicalInstanceId()
                  << ", remote receive queue size="
                  << connStatus->getAvailableQueueSize());

    if (connStatus->getQueueType() != NetworkManager::mqtReplication)
    {
        return;
    }
    if (connStatus->getAvailableQueueSize() <= 0)
    {
        return;
    }
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    // No in-progress outbound replication to the reporting instance?  Nothing to do.
    RepQueue::iterator iter = _repQueue.find(connStatus->getPhysicalInstanceId());
    if (iter == _repQueue.end()) {
        return;
    }

    // Let wait() callers know there's room to transmit another
    // replica message.  For now we only have the one _repEvent
    // condition variable, so we cannot signal on a
    // per-remote-instance basis, oh well.

    LOG4CXX_TRACE(logger, "ReplicationManager::handleConnectionStatus: FlowCtl: iid="
                  << Iid(connStatus->getPhysicalInstanceId())
                  << ", local replication queue size="<< iter->second->size()
                  << ", remote receive queue size="<< connStatus->getAvailableQueueSize());

    _repEvent.signal();
}

void hasReplicaSendInjectedError(int line, const char* file)
{
    auto errorTriggered = hasInjectedError(REPLICA_SEND, __LINE__, __FILE__);
    if (errorTriggered) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INJECTED_ERROR, SCIDB_LE_INJECTED_ERROR);
    }
}

bool ReplicationManager::sendItem(RepItems& ri)
{
    ScopedMutexLock cs(_repMutex, PTW_SML_REP);

    const std::shared_ptr<Item>& item = ri.front();

    if (item->isDone()) {
        ri.pop_front();
        hasReplicaSendInjectedError(__LINE__, __FILE__);
        return true;
    }
    try {
        std::shared_ptr<Query> q(Query::getValidQueryPtr(item->getQuery()));

        std::shared_ptr<MessageDesc> chunkMsg(item->getChunkMsg());
        NetworkManager::getInstance()->sendPhysical(item->getInstanceId(), chunkMsg,
                                                   NetworkManager::mqtReplication);
        LOG4CXX_TRACE(logger, "ReplicationManager::sendItem: successful replica chunk send to instance="
                      << item->getInstanceId()
                      << ", size=" << item->getChunkMsg()->getMessageSize()
                      << ", query (" << q->getQueryID()<<")"
                      << ", queue size="<< ri.size());
        item->setDone();
    } catch (NetworkManager::OverflowException& e) {
        // Only the replication channels are flow controlled.
        assert(NetworkManager::getQueueType(e) == NetworkManager::mqtReplication);
        item->setStalled();     // ...and stays on front of RepItems queue
        return false;
    } catch (Exception& e) {
        item->setDone(e.clone());
    }
    assert(item->isDone());
    ri.pop_front();
    hasReplicaSendInjectedError(__LINE__, __FILE__);
    return true;
}

void ReplicationManager::clear()
{
    assert(_repMutex.isLockedByThisThread());
    for (RepQueue::iterator iter = _repQueue.begin(); iter != _repQueue.end(); ++iter) {
        std::shared_ptr<RepItems>& ri = iter->second;
        assert(ri);
        for (RepItems::iterator i=ri->begin(); i != ri->end(); ++i) {
            (*i)->setDone(SYSTEM_EXCEPTION_SPTR(SCIDB_SE_REPLICATION, SCIDB_LE_UNKNOWN_ERROR));
        }
    }
    _repQueue.clear();
    _repEvent.signal();
}

bool
ReplicationManager::isPrimaryReplica(CachedDBChunk const* chunk, size_t redundancy)
{
    assert(chunk);
    const InstanceID myInstanceId = StorageMgr::getInstance()->getInstanceId();
    bool mine = (myInstanceId == chunk->getChunkAuxMeta().primaryInstanceId);

    if (logger->isTraceEnabled()) {
        if ( not(mine || redundancy > 0) ) {
            LOG4CXX_TRACE(logger, "isPrimaryReplica: chunk primaryInstanceId "
                          << Iid(chunk->getChunkAuxMeta().primaryInstanceId)
                          << ", my instanceId " << Iid(myInstanceId));
        }
    }

    ASSERT_EXCEPTION((mine || (redundancy > 0)), "error: non-primary chunk when 0 redundancy");
    return mine;
}

InstanceID
ReplicationManager::getPrimaryInstanceIdPosition(
    ArrayDesc const& desc,
    Coordinates const& address)
{
    const size_t nInstances = desc.getResidency()->size();
    const InstanceID logicalInstancePos =
        desc.getPrimaryInstanceId(address, nInstances);
    return logicalInstancePos;
}

InstanceID
ReplicationManager::getPrimaryInstanceId(ArrayDesc const& desc, Coordinates const& address)
{
    if (isReplicated(desc.getDistribution()->getDistType())) {
        // the chunk is primary on every instance, so return our own instance
        return StorageMgr::getInstance()->getInstanceId();
    }

    InstanceID logicalInstancePos = getPrimaryInstanceIdPosition(desc, address);
    InstanceID physicalInstance = desc.getResidency()->getPhysicalInstanceAt(logicalInstancePos);
    return physicalInstance;
}

void
ReplicationManager::validateRangesInDebug(const IndexRange* ranges,
                                          const size_t nRanges,
                                          const size_t maxSize)
{
    if (isDebug()) {
        size_t used=0;
        for (size_t i=0; i < nRanges; ++i)  {
            SCIDB_ASSERT(ranges[i].second >= ranges[i].first);
            if (i>0) {
                SCIDB_ASSERT(ranges[i].first > ranges[i-1].second);
            }
            used += ranges[i].second-ranges[i].first+1;
        }
        SCIDB_ASSERT(nRanges < 1 || ranges[nRanges-1].second < maxSize);
        SCIDB_ASSERT(used < maxSize);
    }
}

void
ReplicationManager::insertLastInOrder(IndexRange* ranges, size_t curIndex)
{
    SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
    while (curIndex > 0) {
        if (ranges[curIndex].first < ranges[curIndex-1].first) {
            ranges[curIndex].swap(ranges[curIndex-1]);
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            --curIndex;
        } else {
            SCIDB_ASSERT(ranges[curIndex-1].second < ranges[curIndex].first);
            SCIDB_ASSERT(ranges[curIndex].first <= ranges[curIndex].second);
            break;
        }
    }
}

size_t
ReplicationManager::findIndexInResidency(const size_t index,
                                         const IndexRange* unavailableRanges,
                                         const size_t nRanges,
                                         const size_t maxSize)
{
    SCIDB_ASSERT(index<maxSize);
    validateRangesInDebug(unavailableRanges, nRanges, maxSize);

    size_t available=0; // running count of indeces not coverred by unavailableRanges
    size_t afterLastRange=0;
    size_t rIndex = maxSize;
    for (size_t i=0; i < nRanges; ++i)
    {
        size_t newAvailable =
            available + (unavailableRanges[i].first-afterLastRange);

        if (newAvailable >= index+1) {
            rIndex = afterLastRange + (index-available);
            ASSERT_EXCEPTION(rIndex < maxSize, "Unreachable code");
            return rIndex;
        } else {
            available = newAvailable;
            afterLastRange = unavailableRanges[i].second+1;
            SCIDB_ASSERT(afterLastRange < maxSize);
        }
    }
    SCIDB_ASSERT(available < index+1);
    SCIDB_ASSERT(afterLastRange < maxSize);

    rIndex = afterLastRange + (index - available);
    ASSERT_EXCEPTION(rIndex < maxSize, "Unreachable code");
    return rIndex;
}

size_t
ReplicationManager::computeNextReplicaTarget(const ArrayResPtr& res,
                                             const size_t targetIndex,
                                             IndexRange* unavailableRanges,
                                             InstanceID* replicas,
                                             const size_t nReplicas,
                                             const size_t resSize)
{
    SCIDB_ASSERT(resSize == res->size());
    size_t i = findIndexInResidency(targetIndex,
                                    unavailableRanges,
                                    nReplicas,
                                    resSize);
    SCIDB_ASSERT(i < resSize);

    // add a new replica target
    InstanceID iid = res->getPhysicalInstanceAt(i);
    const size_t serverId = getServerId(iid);
    replicas[nReplicas] = iid;

    // add all the instances on serverId to unavailableRanges
    size_t end=i+1;
    for (; end < resSize; ++end) {
        if (getServerId(res->getPhysicalInstanceAt(end)) != serverId) {
            break; /* residency must be sorted*/
        }
    }
    ssize_t begin=i-1;
    for (; begin >= 0; --begin) {
        if (getServerId(res->getPhysicalInstanceAt(begin)) != serverId) {
            break; /* residency must be sorted*/
        }
    }

    unavailableRanges[nReplicas] = IndexRange(begin+1,end-1);
    SCIDB_ASSERT(end>size_t(begin+1));
    insertLastInOrder(unavailableRanges, nReplicas);
    validateRangesInDebug(unavailableRanges, nReplicas+1, resSize+1);

    // the size of the newly used up range = #instances on serverId
    const size_t numInstancesOnServerId = end-(begin+1);
    return numInstancesOnServerId;
}

void
ReplicationManager::getReplicasInstanceId(InstanceID* replicas,
                                          ArrayDesc const& desc,
                                          PersistentAddress const& address)
{
    const size_t redundancy = desc.getDistribution()->getRedundancy();
    const size_t resSize = desc.getResidency()->size();

    if (redundancy > MAX_REDUNDANCY ||
        resSize <= redundancy) {
        ServerCounter sc;
        for (size_t i=0; i < resSize; ++i) {
            sc(desc.getResidency()->getPhysicalInstanceAt(i));
        }
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
              << redundancy << sc.getCount() << MAX_REDUNDANCY;
    }

    IndexRange unavailableRanges[MAX_REDUNDANCY + 1];
    const size_t nReplicas = (redundancy + 1);

    size_t primaryInstanceIndex = getPrimaryInstanceIdPosition(desc, address);
    size_t nUnavailable = computeNextReplicaTarget(desc.getResidency(), primaryInstanceIndex,
                                                   unavailableRanges, replicas,
                                                   0, resSize);
    SCIDB_ASSERT(nUnavailable <= resSize);
    size_t nInstancesRemaining = resSize - nUnavailable;

    for (size_t i = 0; i < redundancy; ++i)
    {
        const size_t curReplica = (i + 1);
        if (nInstancesRemaining <= 0) {
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
                    << redundancy << curReplica << MAX_REDUNDANCY;
        }

        // NOTICE: Currently, getHashedChunkNumber() hashes the chunk numbers along each dimension
        //         rather than the coordinates. Originally getHashedChunkNumber() returned
        //         the chunk number in the row-major ordering. After hashing was introduced
        //         (to accommodate unbound dimensions)
        //         the domain of fibHash64() changed potentially resulting in worse performance.
        //         Separately, the code has been improved to stop assuming that array
        //         distribution uses a hash, and to instead rely only on
        //         getPrimaryInstanceIdPosition() which delegates the mapping to the descriptor so
        //         So again the domain of fibHash64 has changed.  There's some evidence
        //         that the distribution of the redundant chunks is now much less level
        //         than the distribution of the primary chunks, so the TODO below is
        //         now perhaps even more important.
        const uint64_t chunkId = primaryInstanceIndex * nReplicas + curReplica;

        // XXX TODO: this can be replaced with CityHash without computing chunkId
        size_t instanceIndex = fibHash64(chunkId, MAX_INSTANCE_BITS) % nInstancesRemaining;

        nUnavailable = computeNextReplicaTarget(desc.getResidency(), instanceIndex,
                                                unavailableRanges, replicas,
                                                curReplica, resSize);
        SCIDB_ASSERT(nUnavailable <= nInstancesRemaining);
        nInstancesRemaining -= nUnavailable;

        LOG4CXX_TRACE(logger, "Coords="<< CoordsToStr(address.coords)
                      << " primaryInstanceIndex=" << primaryInstanceIndex
                      << " currReplica=" << curReplica
                      << " instanceIndex=" << instanceIndex
                      << " nInstancesRemaining=" << nInstancesRemaining
                      << " target instance ["<<curReplica<<"]="<< replicas[curReplica]);
    }

    // post-condition, checks that the result does not return
    // two instances from the same server.
    // note that primary instance is replicas[0], not a redundant chunk.
    // note that there's no check here on the uniformity of the distribution
    // from fibHash64 at this time.
    if (isDebug()) {
        for (size_t i = 0; i < redundancy; ++i) {
            for (size_t j = i+1; j < redundancy+1; ++j) {
                SCIDB_ASSERT(getServerId(replicas[i]) !=
                             getServerId(replicas[j]));
            }
        }
    }
}

void
ReplicationManager::replicate(ArrayDesc const& desc,
                              PersistentAddress const& addr,
                              CachedDBChunk const* chunk,
                              void const* data,
                              size_t compressedSize,
                              size_t decompressedSize,
                              std::shared_ptr<Query> const& query,
                              ReplicationManager::ItemVector& replicasVec)
{
    Query::validateQueryPtr(query);
    SCIDB_ASSERT(compressedSize <= decompressedSize);

    // Both or neither.
    assert((chunk == nullptr) == (data == nullptr));

    size_t redundancy = desc.getDistribution()->getRedundancy();

    /* If we are in recovery, or redundancy is set to 0, or this is
       itself a replica chunk, then there is nothing to do.
    */
    if (StorageMgr::getInstance()->inRecovery() ||
        redundancy <= 0 ||
        (chunk && !isPrimaryReplica(chunk, redundancy)))
    {
        return;
    }

    /* Create the replication message
     */
    replicasVec.reserve(redundancy);
    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, addr);

    QueryID queryId = query->getQueryID();
    SCIDB_ASSERT(queryId.isValid());

    std::shared_ptr<MessageDesc> chunkMsg;
    if (chunk)
    {
        std::shared_ptr<CompressedBuffer> buffer =
            std::make_shared<CompressedBuffer>();
        buffer->allocate(compressedSize);
        memcpy(buffer->getWriteData(), data, compressedSize);
        chunkMsg = std::make_shared<MessageDesc>(mtChunkReplica, buffer);
    }
    else
    {
        // Replicate a tombstone.
        chunkMsg = std::make_shared<MessageDesc>(mtChunkReplica);
    }
    chunkMsg->setQueryID(queryId);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord =
        chunkMsg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_attribute_id(addr.attId);
    chunkRecord->set_array_id(addr.arrVerId);
    for (size_t k = 0; k < addr.coords.size(); k++)
    {
        chunkRecord->add_coordinates(addr.coords[k]);
    }
    chunkRecord->set_eof(false);

    if(chunk)
    {
        chunkRecord->set_compression_method(chunk->getCompressionMethod());
        chunkRecord->set_decompressed_size(decompressedSize);
        chunkRecord->set_count(0);
        LOG4CXX_TRACE(logger, "Replicate chunk of array ID="
                      << addr.arrVerId << " attribute ID=" << addr.attId
                      << " coords=" << CoordsToStr(addr.coords));
    }
    else
    {
        chunkRecord->set_tombstone(true);
    }

    for (size_t i = 1; i <= redundancy; ++i)
    {
        std::shared_ptr<Item> item =
            make_shared <Item>(replicas[i], chunkMsg, query);
        send(item);
        replicasVec.push_back(item);
    }
}

void
ReplicationManager::abortReplicas(ReplicationManager::ItemVector const& replicasVec)
{
    for (auto const& item : replicasVec) {
        abort(item);
        assert(item->isDone());
    }
}

void
ReplicationManager::waitForReplicas(ReplicationManager::ItemVector & replicasVec)
{
    for (auto const& item : replicasVec) {
        wait(item);
        assert(item->isDone());
        assert(!item->hasError());
    }
}

//
// NOTE: chunkIid might be a "premature optimization" because
//       primaryInstanceID can be determined from desc and addr
//       Passing "bool isMine" here instead would allow
//       ChunkAuxMeta to replace _primaryInstanceID (8bytes)
//       with _isMine (1 bit)
bool
ReplicationManager::isResponsibleFor(ArrayDesc const& desc,
                                     PersistentAddress& addr,
                                     InstanceID chunkIid,
                                     std::shared_ptr<Query> const& query)
{
    Query::validateQueryPtr(query);

    if (isReplicated(desc.getDistribution()->getDistType()) || // primary on all instances
        StorageMgr::getInstance()->getInstanceId() == chunkIid) {           // primary is me
        return true;
    }

    if (not query->isPhysicalInstanceDead(chunkIid)) {
        // TODO: validate whether the optimization "chunkIid" is premature
        //       i.e. the time saved is negative or negligible
        //       vs using primaryInstanceId(desc, addr) while
        //       saving 64 bits of primaryInstanceId in ChunkAuxMeta
        return false;       // replica chunk and primary is alive, we aren't responsible
    }

    // physical instance of primary chunk is dead
    // is this the instance that should serve the replica?

    InstanceID replicas[MAX_REDUNDANCY + 1];
    getReplicasInstanceId(replicas, desc, addr);

    InstanceID serverIid = StorageMgr::getInstance()->getInstanceId();
    size_t redundancy = desc.getDistribution()->getRedundancy();

    for (size_t i = 1; i <= redundancy; ++i) {
        if (replicas[i] == serverIid) {
            return true;    // we are the I-th replica for this chunk and can serve it
        }
        if (not query->isPhysicalInstanceDead(replicas[i])) {
            return false;  // another instance is the I-th replica and can serve it
        }
    }
    return false; // we were not a replica (and no alive replica shortened the search first)
}

} // namespace
