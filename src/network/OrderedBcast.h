/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
#ifndef ORDEREDBCAST_H_
#define ORDEREDBCAST_H_

#include <deque>
#include <map>
#include <sstream>


#include <query/InstanceID.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <util/Singleton.h>
#include <util/WorkQueue.h>
#include <network/NetworkMessage.h>
#include <network/MessageDescription.h>
#include <util/Notification.h>

namespace scidb
{

class MessageDesc;
class NetworkManager;

class LivenessTracker
{
public:

    typedef std::map<InstanceID, uint64_t> VersionVector; //XXX use vector instead ?

    enum MsgDeliveryAction
    {
            DISCARD=0,
            DELIVER,
            DEFER
    };

    /// Instance ID getter from a VersionVector element
    InstanceID operator() (const VersionVector::value_type& elem) // VersionVector是一个map，map的value_type是一个pair
    {
        return elem.first;
    }

    /// Instance ID getter from a InstanceLiveness::LiveInstances element
    InstanceID operator() (const InstanceLiveness::LiveInstances::value_type& elem)
    {
        return elem.getInstanceId();
    }

    /// Constructor
    LivenessTracker();

    /// Destructor
    ~LivenessTracker() {}

    /// Generate Liveness GPB message struct
    scidb::MessagePtr createLiveness(scidb::MessageID msgId);

    /// Generate LivenessAck GPB message struct
    scidb::MessagePtr createLivenessAck(scidb::MessageID msgId);

    /// Handle a local or remote liveness change
    /// @return false if the liveness is ignored; true otherwise
    bool newLiveness(InstanceID iId, const InstLivenessPtr& liveInfo);

    /// Handle a remote reply to our liveness message
    /// @return false if the liveness info is ignored; true otherwise
    bool newLivenessAck(InstanceID remoteId,
                        uint64_t myLivenessVer,
                        const InstLivenessPtr& remoteLiveInfo);

    /// @return true if the cluster is stable and the messaging can proceed
    bool isInSync() const { return _isInSync; }

    /// @return a message delivery action based on the liveness state of the cluster
    MsgDeliveryAction getMsgDeliveryAction(const VersionVector& vv);

    /// @return the current version vector, empty if the one has not been formed in a stable cluster
    const VersionVector& getVersionVector() const { return _versionVector; }

private:

    typedef std::map<InstanceID, InstLivenessPtr> LivenessVector; //XXX unordered_map instead ?
    LivenessVector _liveVector;
    VersionVector _versionVector;
    InstanceID _selfId;
    bool _isInSync;
    NetworkManager* _nm;

    LivenessTracker(const LivenessTracker& ) = delete;
    LivenessTracker& operator=(const LivenessTracker& ) = delete;
    void checkInSync();
};

/// Implementation of a globally ordered broadcast.
/// The broadcasted messages can be lost in case of cluster instability (changing liveness).
/// However, if the liveness does not change in a cluster,
/// the messages are reliably delivered to all the live instances.
/// It does NOT guarantee the same message delivery to the surviving instances or
/// provide any other stronger guarantees.
/// It assumes a FIFO pair-wise (instance-to-instance) communication and
/// that an event like a TCP connection loss be reported
/// as a new local liveness (with an increased version).
/// The *same liveness* is defined by a liveness version vector,
/// which is a list of liveness versions for each of the live instances.
/// Each message is stamped with a version vector and
/// is considered SENDABLE/DELIVERABLE if the local instance agrees with
/// its version vector (according to LivenessTracker).
/// An agreed upon version vector is sometimes referred to as view.
class OrderedBcastManager : public Singleton<OrderedBcastManager>
{
public:
    typedef uint64_t Timestamp;

    virtual ~OrderedBcastManager();
    OrderedBcastManager();

    void init(Timestamp ts=0);
    void broadcast(const std::shared_ptr<MessageDesc>& messageDesc);

private: // classes

    /// Lamport clock structure
    class LogicalTimestamp
    {
    private:
        Timestamp _ts;
        InstanceID _iid; // 逻辑时钟和ins关联
        LogicalTimestamp() = delete;
    public:
        LogicalTimestamp ( Timestamp ts, InstanceID iid )
        : _ts(ts), _iid(iid)
        {
            SCIDB_ASSERT(isValidPhysicalInstance(_iid));
        }
        Timestamp getTimestamp()  const { return _ts; }
        InstanceID getInstanceId() const { return _iid; }

        bool operator<(const LogicalTimestamp& other) const
        {
            if (_ts == other._ts) {
                return (_iid < other._iid); // 时间相同的情况下，比较ins id大小
            }
            return (_ts < other._ts);
        }

        bool operator>(const LogicalTimestamp& other) const
        {
            if (_ts == other._ts) {
                return (_iid > other._iid);
            }
            return (_ts > other._ts);
        }

        bool operator==(const LogicalTimestamp& other) const
        {
            return (_iid == other._iid && _ts == other._ts);
        }

        bool operator!=(const LogicalTimestamp& other) const
        {
            return !operator==(other);
        }
    };
    friend std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::LogicalTimestamp& lts);

    /// Base class for an entry in the globally ordered queue corresponding to
    /// either broadcast request or a broadcast acknowledgement
    class Entry
    {
    public:
        /// @param vVec [in/out] the contents are swapped out
        Entry(LivenessTracker::VersionVector& vVec)
        {
            _versionVec.swap(vVec);
        }
        virtual ~Entry() {}
        const LivenessTracker::VersionVector& getVersionVector() const { return _versionVec; }
        void setVersionVector(LivenessTracker::VersionVector& vVec) { _versionVec.swap(vVec); }
    private:
        LivenessTracker::VersionVector _versionVec;
    };

    /// Broadcast request
    class RequestEntry : public Entry
    {
    public:
        /// @param vVec [in/out] the contents are swapped out
        RequestEntry(const std::shared_ptr<MessageDesc>& msg,
                     LivenessTracker::VersionVector& vVec)
        : Entry(vVec), _msg(msg)
        {
            SCIDB_ASSERT(msg);
        }
        virtual ~RequestEntry() {}
        std::shared_ptr<MessageDesc> getMessage() const { return _msg; }
    private:
        const std::shared_ptr<MessageDesc> _msg;
    };
    friend std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::RequestEntry& re);

    /// Broadcast reply (i.e. request acknowledgement)
    class ReplyEntry : public Entry
    {
    public:
        /// @param vVec [in/out] the contents are swapped out
        ReplyEntry(Timestamp reqTs, // 请求消息的逻辑时间戳
                   InstanceID reqSrc,
                   LivenessTracker::VersionVector& vVec)
        : Entry(vVec), _reqTs(reqTs), _reqSrc(reqSrc)
        {
            SCIDB_ASSERT(isValidPhysicalInstance(_reqSrc));
            SCIDB_ASSERT(_reqTs > 0);
        }
        virtual ~ReplyEntry() {}
        Timestamp getRequestTimestamp()   const { return _reqTs; }
        InstanceID getRequestInstanceId() const { return _reqSrc; }
    private:
        const Timestamp _reqTs; // 请求消息的逻辑时间戳
        const InstanceID _reqSrc;
    };
    friend std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::ReplyEntry& re);

    typedef std::map<LogicalTimestamp,
            std::shared_ptr<RequestEntry> > RequestQueue; //XXX priority_queue instead ? checking for duplicates ?

    typedef std::deque<std::pair<LogicalTimestamp, std::shared_ptr<Entry> > > DeferredQueue; // 双端队列，推迟的queue

    typedef std::map<InstanceID,Timestamp> InstanceClocks;  //XXX unordered_map instead ?

private: // fields

    /// Local Lamport clock // 分布式系统：Lamport 逻辑时钟 https://zhuanlan.zhihu.com/p/56146800
    Timestamp _ts;
    /// Last Lamport clock broadcasted to the current liveness set
    Timestamp _lastBroadcastTs;
    /// Currently known clocks of other instances
    InstanceClocks _remoteTimestamps;
    /// Message queue for the broadcast messages in the current liveness.
    /// It enforces the global order (within a view). // 有序的全局广播消息队列
    RequestQueue _queue;
    /// Broadcast message queue for the messages in the future livenesses
    DeferredQueue _deferredQueue;

    NetworkManager* _nm;
    Cluster* _cluster;
    std::shared_ptr<WorkQueue> _wq;
    /// A mechanism for identifying the current liveness
    LivenessTracker _livenessTracker;
    const InstanceID _selfInstanceId;
    Notification<InstanceLiveness>::SubscriberID _livenessSubscriberID;

private: // methods

    OrderedBcastManager(const OrderedBcastManager&) = delete;
    OrderedBcastManager& operator=(const OrderedBcastManager&) = delete;

    // NetworkMessageFactory creation hooks:
    /// @return ordered broadcast request message structure
    MessagePtr createRequest(MessageID msgId);
    /// @return ordered broadcast reply message structure
    MessagePtr createReply(MessageID msgId);

    // The handleXXX methods handle various events delivered by the networking thread
    // They perform initial parsing and enqueue further work onto a dedicated FIFO
    // WorkQueue (to guarantee the broadcast order etc.)

    /// Handle remote ordered broadcast request
    void handleRequest(const std::shared_ptr<MessageDescription>& obcastMsg);
    /// Handle remote ordered broadcast reply
    void handleReply(const std::shared_ptr<MessageDescription>& obcastMsg);
    /// Handle local liveness notification
    void handleLivenessNotification(const InstLivenessPtr& liveInfo);
    /// Handle remote liveness message
    void handleLiveness(const std::shared_ptr<MessageDescription>& liveMsg);
    /// Handle remote liveness ACK message to "my" liveness broadcast
    void handleLivenessAck(const std::shared_ptr<MessageDescription>& liveAckMsg);

    std::shared_ptr<WorkQueue> getFIFO() { return _wq; }
    /// Enqueue work on the dedicated FIFO queue
    void enqueue(WorkQueue::WorkItem& work);

    /// Process local/remote liveness event
    void livenessChange(InstanceID iId, const InstLivenessPtr& liveInfo);
    /// Process remote liveness ACK
    void livenessAck(InstanceID remoteId,
                     uint64_t myLivenessVer,
                     const InstLivenessPtr& remoteLiveInfo);
    /// Process local ordered broadcast request
    void localRequest(const std::shared_ptr<MessageDesc>& messageDesc);
    /// Process remote ordered broadcast request
    void remoteRequest(const std::shared_ptr<MessageDesc>& messageDesc,
                       const InstanceID reqSrc,
                       const Timestamp reqTs,
                       LivenessTracker::VersionVector& vVec) ; // not const!
    /// Process remote ordered broadcast reply
    void remoteReply(const InstanceID repSrc,
                     const Timestamp  repTs,
                     const InstanceID reqSrc,
                     const Timestamp  reqTs,
                     LivenessTracker::VersionVector& vVec); // not const!

    // The deliverXXX methods perform the final event processing
    // IF/WHEN the event does not need to be deferred due to unstable view.
    bool deliverRemoteRequest(const LogicalTimestamp& lts,
                              const std::shared_ptr<RequestEntry>& re,
                              const bool isReply = true);
    void deliverLocalRequest(const LogicalTimestamp& lts, const std::shared_ptr<RequestEntry>& re);
    void deliverRemoteReply(const InstanceID repSrc,
                            const Timestamp  repTs,
                            const InstanceID reqSrc,
                            const Timestamp  reqTs,
                            const LivenessTracker::VersionVector& vVec); // not const!

    /**
     * Remove old messages,
     * broadcast delayed own requests,
     * reply to remote requests,
     * release requests to the user.
     */
    void processQueueOnLivenessChange();

    void verifyNoDeferredInDebug(InstanceID iId);
    void clearQueueInDebug();
    /// Check if any requests can be released to the user and release them
    void release();
    bool isOkToRelease(const LogicalTimestamp& lts);
    /// Put reply message on the network
    void broadcastReply(InstanceID requestId,
                        uint64_t requestTs,
                        const LivenessTracker::VersionVector& vVec);
    /// Put request message on the network
    void broadcastRequest(const LogicalTimestamp& lts,
                          const std::shared_ptr<MessageDesc>& messageDesc,
                          const LivenessTracker::VersionVector& vVec);
    friend std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::DeferredQueue& dq);
    friend std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::RequestQueue& rq);
};

std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::LogicalTimestamp& lts);
std::ostream& operator<<(std::ostream& os, const std::map<InstanceID, uint64_t>& m);
std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::ReplyEntry& re);
std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::DeferredQueue& dq);
std::ostream& operator<<(std::ostream& os, const OrderedBcastManager::RequestQueue& rq);

}
#endif // ORDEREDBCAST_H_
