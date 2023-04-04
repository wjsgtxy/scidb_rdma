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
 * @file Cluster.h
 *
 * @brief Contains class for providing information about cluster
 *
 */

#ifndef CLUSTER_H
#define CLUSTER_H

#include <vector>
#include <set>
#include <memory>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <util/Singleton.h>
#include <query/InstanceID.h>
#include <system/Utils.h>

namespace scidb
{
typedef uint64_t MembershipID;
static const MembershipID MAX_MEMBERSHIP_ID = ~0;
static const uint64_t DEFAULT_LIVENESS_VER = 0;

/**
 * Descriptor of instance
 */
class InstanceDesc
{
public:

    static const double INFINITY_TS;
    static const MembershipID INVALID_MEMBERSHIP_ID = ~0;

    /**
     * Construct partial instance descriptor (without id, for adding to catalog)
     *
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param path to the binary
     */
    InstanceDesc(const std::string &host,
                 uint16_t port,
                 const std::string &basePath,
                 uint32_t serverId,
                 uint32_t serverInstanceId);


    /**
     * Construct full instance descriptor
     *
     * @param instance_id instance identifier
     * @param host ip or hostname where instance running
     * @param port listening port
     * @param online instance status (online or offline)
     */
    InstanceDesc(InstanceID instanceId,
                 MembershipID membershipId,
                 const std::string& host,
                 uint16_t port,
                 double onlineTs,
                 const std::string& basePath,
                 uint32_t serverId,
                 uint32_t serverInstanceId);


    /**
     * Get instance identifier
     * @return instance identifier
     */
    InstanceID getInstanceId() const
    {
        return _instanceId;
    }

    /**
     * Get the membership ID in which the instance was added
     * or INVALID_MEMBERSHIP_ID
     * @return membership identifier
     */
    MembershipID getMembershipId() const
    {
        return _membershipId;
    }

    /**
     * Get instance hostname or IP
     * @return instance host
     */
    const std::string& getHost() const
    {
        return _host;
    }

    /**
     * Get instance listening port number
     * @return port number
     */
    uint16_t getPort() const
    {
        return _port;
    }

    /**
     * Get server ID as specified in the SciDB configuration file
     * (e.g. server-ID=<host>,<server_instance_IDs>
     * @return server ID
     */
    uint32_t getServerId() const
    {
        return _serverId;
    }

    /**
     * Get server instance ID as specified in the SciDB configuration file
     * (e.g. server-ID=<host>,<server_instance_IDs>
     * @return server instance ID
     */
    uint32_t getServerInstanceId() const
    {
        return _serverInstanceId;
    }

    /**
     * @return time when the instance marked itself online
     */
    double getOnlineSince() const
    {
        return _online;
    }

    /**
     * Get instance binary path
     * @return path to the instance's binary
     */
    const std::string& getBasePath() const
    {
        return _basePath;
    }

    /**
     * Get instance binary path
     * @return path to the instance's binary
     */
    std::string getPath() const ;

private:
    InstanceID   _instanceId;
    MembershipID _membershipId;
    std::string  _host;
    uint16_t     _port;
    double       _online;
    std::string  _basePath;
    uint32_t     _serverId;
    uint32_t     _serverInstanceId;
};

std::ostream& operator<<(std::ostream&,const InstanceDesc&);

/**
 * Vector of InstanceDesc type
 */
typedef std::vector<InstanceDesc> Instances;

/**
 *  Determines the number of servers in a sorted list of physical instances.
 *  It is intended to be called for each in such a list.
 */
class ServerCounter
{
private:
    size_t _count;
    size_t _curServerId;
public:
    ServerCounter() : _count(0), _curServerId(~0)
    {
    }
    void operator() (const InstanceDesc& i)
    {
        operator()(i.getInstanceId());
    }
    void operator() (const InstanceID i)
    {
        const size_t newServerId = getServerId(i);
        if (newServerId != _curServerId) {
            _curServerId = newServerId;
            ++_count;
        }
    }
    size_t getCount() const { return _count; }
};

/**
 * Class that describes the cluster membership, i.e. all physical SciDB instances that can process queries.
 * The membership can change (due to administrative instance addition/removal).
 * Each new set of instances is called a membership identified by a monotonically increasing MembershipID.
 */
class InstanceMembership
{
 public:

    /**
     * Constructor
     * @param id membership ID
     */
    explicit InstanceMembership(MembershipID id)
        : _id(id)
    {}

    /**
     * Constructor
     * @param id membership ID;
     *        must be monotonically increasing for every new object // 单调递增的
     * @param [in,out] instances a vector of instance member descriptors,
     *        it is empty on return
     */
    InstanceMembership(MembershipID id, Instances& instances)
        : _id(id)
    {
        SCIDB_ASSERT(!instances.empty());
        _instanceConfigs.swap(instances);

        for (size_t i = 0, n = _instanceConfigs.size(); i < n; ++i) {
            const InstanceID iId = _instanceConfigs[i].getInstanceId();
            ASSERT_EXCEPTION(isValidPhysicalInstance(iId),
                             "Invalid instance ID: " << Iid(iId));
            _instances[iId] = i;
        }
    }

    ~InstanceMembership()
    {}

    /// @return number of instances in this membership
    size_t  getNumInstances() const { return _instanceConfigs.size(); }

    /// @return membership ID, a monotically increasing value
    MembershipID getId() const { return _id; }

    /// @throws InstanceMembership::NotFoundException if instance is not in this membership
    void confirmMembership(InstanceID iId) const
    {
        if (!isMember(iId)) {
            throw NotFoundException(REL_FILE, __FUNCTION__, __LINE__)
                << Iid(iId);
        }
    }

    /**
     * @param iId instance ID
     * @return true if instance is in this membership; else false
     */
    bool isMember(InstanceID iId) const
    {
        const bool res = (_instances.find(iId) != _instances.end());
        return res;
    }

   /**
    * Get the instance membership index
    * @param iId member instance ID
    * @return index into the ordered memberhip list of instances
    * @throw scidb::InstanceMembership::NotFoundException
    *        if the instance with iId is not a memeber
    */
    size_t getIndex(InstanceID iId) const
    {
        Instance2IndexMap::const_iterator iter = _instances.find(iId);
        if (iter == _instances.end()) {
            throw NotFoundException(REL_FILE, __FUNCTION__, __LINE__)
                << Iid(iId);
        }
        SCIDB_ASSERT(iter->first == iId);
        SCIDB_ASSERT(iter->second < _instanceConfigs.size());
        return iter->second;
    }

   /**
    * Get configuration information for a given instance
    * @param iId member instance ID
    * @return instance descriptor
    * @throw scidb::InstanceMembership::NotFoundException
    *        if the instance with iId is not a memeber
    */
   const InstanceDesc& getConfig(InstanceID iId) const
   {
       return _instanceConfigs[getIndex(iId)];
   }

   /**
    * Get configuration information for an instance at a given index
    * @param indx index into the ordered memberhip list of instances
    * @return instance descriptor
    * @throw scidb::InstanceMembership::NotFoundException
    *        if the instance at indx does not exist
    */
   const InstanceDesc& getConfigAt(size_t indx) const
   {
       if (indx >= _instanceConfigs.size()) {
           throw NotFoundException(REL_FILE, __FUNCTION__, __LINE__)
               << Iid(indx);
       }
       return _instanceConfigs[indx];
   }

   /**
    * For each instance in the membership invoke
    * the caller-specified visitor(scidb::InstanceDesc const&)
    */
   template<typename Visitor>
   void visitInstances(Visitor& visitor) const
   {
       for (Instance2IndexMap::const_iterator iter =_instances.begin();
            iter != _instances.end(); ++iter) {
           SCIDB_ASSERT(iter->second < _instanceConfigs.size());
           visitor(_instanceConfigs[iter->second]);
       }
   }

   /// @return if the other object describes an identical membership
   /// with the same ID and members
   bool isEqual(const InstanceMembership& other) const
   {
      return ((_id == other._id) &&
              (_instances==other._instances));
   }

    DECLARE_SYSTEM_EXCEPTION_SUBCLASS(NotFoundException,
                                      SCIDB_SE_INTERNAL,
                                      SCIDB_LE_INSTANCE_DOESNT_EXIST);

private:

    InstanceMembership(const InstanceMembership&) = delete;
    InstanceMembership& operator=(const InstanceMembership&) = delete;

    MembershipID _id;
    scidb::Instances _instanceConfigs;
    typedef std::map<InstanceID,size_t> Instance2IndexMap;
    Instance2IndexMap _instances;
};
typedef std::shared_ptr<const InstanceMembership> InstMembershipPtr;

/**
 * A helper class describing the liveness state of a SciDB instance
 */
 class InstanceLivenessEntry
 {
   public:
      InstanceLivenessEntry()
          : _generationId(0)
          , _instanceId(INVALID_INSTANCE)
          , _isDead(false)
      {}

      InstanceLivenessEntry(InstanceID instanceId, uint64_t genId, bool isDead)
          : _generationId(genId)
          , _instanceId(instanceId)
          , _isDead(isDead)
      {}

      ~InstanceLivenessEntry()
      {}

      uint64_t getInstanceId() const { return _instanceId; }

      void setInstanceId(InstanceID id) {_instanceId = id; }

      uint64_t getGenerationId() const { return _generationId; }

      void setGenerationId(uint64_t id) {_generationId = id; }

      bool isDead() const { return _isDead; }

      void setIsDead(bool state) {_isDead = state; }

      bool operator<(const InstanceLivenessEntry& other) const
      {
          assert(_instanceId != INVALID_INSTANCE);
          assert(other._instanceId != INVALID_INSTANCE);
          return (_instanceId < other._instanceId);
      }

      bool operator==(const InstanceLivenessEntry& other) const
      {
          assert(_instanceId != INVALID_INSTANCE);
          assert(other._instanceId != INVALID_INSTANCE);
          return ((_instanceId != INVALID_INSTANCE) &&
                  (_instanceId == other._instanceId) &&
                  (_generationId == other._generationId) &&
                  (_isDead == other._isDead));
      }

      bool operator!=(const InstanceLivenessEntry& other) const
      {
          return !operator==(other);
      }

      InstanceLivenessEntry(const InstanceLivenessEntry& other)
      {
          _generationId = other._generationId;
          _instanceId = other._instanceId;
          _isDead = other._isDead;
      }

      InstanceLivenessEntry& operator=(const InstanceLivenessEntry& other)
      {
          if (this != &other) {
              _generationId = other._generationId;
              _instanceId = other._instanceId;
              _isDead = other._isDead;
          }
          return *this;
      }

 private:

      uint64_t _generationId;
      InstanceID _instanceId;
      bool   _isDead;
 };

/**
 * Class that describes the cluster liveness, i.e. dead/live status of all physical SciDB instances.
 * The membership ID associated with a particular liveness must correspond to a particular membership.
 * For example, over the lifetime of a given membership with a given membership ID there might be many
 * livenesses corresponding to the membership (via the membership ID).
 */
class InstanceLiveness
{
 public:

   typedef InstanceLivenessEntry const* InstancePtr;
   typedef std::set<InstanceLivenessEntry> DeadInstances;
   typedef std::set<InstanceLivenessEntry> LiveInstances;

   explicit InstanceLiveness(MembershipID membId, uint64_t version)
   : _membershipId(membId), _version(version)
   {}
   ~InstanceLiveness() {}
   const LiveInstances& getLiveInstances() const { return _liveInstances; }
   const DeadInstances& getDeadInstances() const { return _deadInstances; }
   MembershipID   getMembershipId()  const { return _membershipId; }
   uint64_t getVersion() const { return _version; }
   bool     isDead(const InstanceID& id) const { return (find(_deadInstances, id) != NULL); }
   size_t   getNumDead()  const { return _deadInstances.size(); }
   size_t   getNumLive()  const { return _liveInstances.size(); }
   size_t   getNumInstances() const { return getNumDead()+getNumLive(); }

   bool insert(const InstancePtr& key)
   {
      assert(key);
      if (key->isDead()) {
         if (find(_liveInstances, key)) {
            assert(false);
            return false;
         }
         return _deadInstances.insert(*key).second;
      } else {
         if (find(_deadInstances, key)) {
            assert(false);
            return false;
         }
         return _liveInstances.insert(*key).second;
      }
      assert(false);
      return false;
   }

   InstancePtr find(const InstanceID& instanceId) const
   {
      InstanceLivenessEntry key(instanceId,0,false);
      InstancePtr val = find(_liveInstances, &key);
      if (val) {
         assert(!val->isDead());
         return val;
      }
      val = find(_deadInstances, &key);
      assert(!val || val->isDead());
      return val;
   }

   bool isEqual(const InstanceLiveness& other) const
   {
      return ((_membershipId == other._membershipId) &&
              (_deadInstances==other._deadInstances) &&
              (_liveInstances==other._liveInstances));
   }

 private:

   typedef std::set<InstanceLivenessEntry> InstanceEntries;
   InstanceLiveness(const InstanceLiveness&);
   InstanceLiveness& operator=(const InstanceLiveness&);
   InstancePtr find(const InstanceEntries& instances, const InstanceID& instanceId) const
   {
      InstanceLivenessEntry key(instanceId,0,false);
      return find(instances, &key);
   }
   InstancePtr find(const InstanceEntries& instances, const InstancePtr& key) const
   {
      InstancePtr found(NULL);
      InstanceEntries::const_iterator iter = instances.find(*key);
      if (iter != instances.end()) {
         found = &(*iter);
      }
      return found;
   }
   MembershipID _membershipId;
   uint64_t _version;
   InstanceEntries _liveInstances;
   InstanceEntries _deadInstances;
};

typedef std::shared_ptr<const InstanceLiveness> InstLivenessPtr;
std::ostream& operator<<(std::ostream&, InstanceLiveness const&);


/**
 * A singleton class which servers as an interface
 * to the cluster/instance state information including the membership and liveness.
 */
class Cluster: public Singleton<Cluster>
{

public:
   /**
    * Get cluster membership matching a specific ID
    * @param membId membership ID; 0 is the smallest possible value
    * @return current membership, whose membership ID == membId
    * @throw scidb::SystemException if the current cluster membership ID != membId
    */
    InstMembershipPtr getMatchingInstanceMembership(MembershipID membId)
    {
        const InstMembershipPtr m = getInstanceMembership(membId);
        if (m->getId() != membId) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_LIVENESS_MISMATCH);
        }
        return m;
    }

   /**
    * Get cluster membership
    * @param membId membership ID; 0 is the smallest possible value
    * @return current membership, whose membership ID may be larger than membId
    */
    InstMembershipPtr getInstanceMembership(MembershipID membId);

   /**
    * Get cluster liveness
    * @return current liveness
    */
   InstLivenessPtr getInstanceLiveness();

   /**
    * Get this instances' ID
    */
   InstanceID getLocalInstanceId();

   /**
    * @return the instance of lowest rank in the
    * collection of live instances.
    */
   InstanceID getPrimaryInstanceId();

   /// Get the (globally unique?) UUID of this cluster
   const std::string& getUuid() { return _uuid; }

protected:

   /// Constructor
   Cluster();

   /// Destructor
   ~Cluster() {}
   friend class Singleton<Cluster>;

private:

   void getInstances();

   InstMembershipPtr _lastMembership;
   std::string _uuid;
   Mutex _mutex;
};

} // namespace

#endif // CLUSTER_H
