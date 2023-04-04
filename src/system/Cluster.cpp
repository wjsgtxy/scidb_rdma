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
 * @author roman.simakov@gmail.com
 */
#include <system/Cluster.h>

#include <limits>
#include <string>
#include <memory>

#include <network/NetworkManager.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{
const double InstanceDesc::INFINITY_TS = std::numeric_limits<double>::infinity();

Cluster::Cluster()
: _uuid(SystemCatalog::getInstance()->getClusterUuid())
{
}

InstMembershipPtr
Cluster::getInstanceMembership(MembershipID id)
{
   ScopedMutexLock lock(_mutex, PTW_SML_CLUSTER);

   if (!_lastMembership || _lastMembership->getId() < id) {
       getInstances();
   }
   SCIDB_ASSERT(_lastMembership);

   return _lastMembership;
}

void Cluster::getInstances()
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    Instances instances; // Instances是一个vector
    MembershipID id = SystemCatalog::getInstance()->getInstances(instances); // 会连接pg数据库查询ins信息
    SCIDB_ASSERT(!instances.empty());
    _lastMembership = std::make_shared<InstanceMembership>(id, instances);
    SCIDB_ASSERT(instances.empty());
}

InstLivenessPtr Cluster::getInstanceLiveness()
{
    static bool netmgrHasLiveness = false;

    InstLivenessPtr liveness(NetworkManager::getInstance()->getInstanceLiveness());
    if (liveness) {
        netmgrHasLiveness = true;
        return liveness;
    }

    // NetworkManager doesn't have a liveness object yet, so we have to
    // create one based on the contents of the SystemCatalog.  See
    // getInstanceMembership() above.

    // Once the Network manager *does* have liveness information to
    // return, we never come through this code path again.
    SCIDB_ASSERT(not netmgrHasLiveness);

   InstMembershipPtr membership(getInstanceMembership(0));
   SCIDB_ASSERT(membership);

   std::shared_ptr<InstanceLiveness> newLiveness(new InstanceLiveness(membership->getId(),
                                                                      DEFAULT_LIVENESS_VER));
   auto addEntry = [newLiveness](InstanceDesc const& inst) {
       InstanceLivenessEntry entry(inst.getInstanceId(), 0, false);
       newLiveness->insert(&entry);
   };
   membership->visitInstances(addEntry);

   liveness = newLiveness;
   SCIDB_ASSERT(liveness->getNumLive() > 0);
   return liveness;
}

InstanceID Cluster::getLocalInstanceId()
{
   return NetworkManager::getInstance()->getPhysicalInstanceID();
}

InstanceID Cluster::getPrimaryInstanceId()
{
    auto myLiveness = getInstanceLiveness();
    SCIDB_ASSERT(myLiveness);
    auto liveInstances = myLiveness->getLiveInstances();
    SCIDB_ASSERT(!liveInstances.empty());
    const auto& firstLiveInst = *liveInstances.begin();
    return firstLiveInst.getInstanceId();
}

InstanceDesc::InstanceDesc(const std::string& host,
                           uint16_t port,
                           const std::string& basePath,
                           uint32_t serverId,
                           uint32_t serverInstanceId)
        : _instanceId(INVALID_INSTANCE),
          _membershipId(0),
          _host(host),
          _port(port),
          _online(INFINITY_TS),
          _basePath(basePath),
          _serverId(serverId),
          _serverInstanceId(serverInstanceId)
{
    boost::filesystem::path p(_basePath);
    _basePath = p.normalize().string();
    SCIDB_ASSERT(!_host.empty());
    SCIDB_ASSERT(!_basePath.empty());
}

InstanceDesc::InstanceDesc(InstanceID instanceId,
                           MembershipID membershipId,
                           const std::string& host,
                           uint16_t port,
                           double onlineTs,
                           const std::string& basePath,
                           uint32_t serverId,
                           uint32_t serverInstanceId)
        : _instanceId(instanceId),
          _membershipId(membershipId),
          _host(host),
          _port(port),
          _online(onlineTs),
          _basePath(basePath),
          _serverId(serverId),
          _serverInstanceId(serverInstanceId)
{
    boost::filesystem::path p(_basePath);
    _basePath = p.normalize().string();
    SCIDB_ASSERT(!_host.empty());
    SCIDB_ASSERT(!_basePath.empty());
}

std::string InstanceDesc::getPath() const
{
    std::stringstream ss;
    // XXX TODO: consider boost path utils
    ss << _basePath << "/" << _serverId << "/" << _serverInstanceId ;
    return ss.str();
}

std::ostream& operator<<(std::ostream& stream,const InstanceDesc& instance)
{
    stream << "instance { id = " << instance.getInstanceId()
           << ", membership_id = " << instance.getMembershipId()
           << ", host = " << instance.getHost()
           << ", port = " << instance.getPort()
           << ", member since " << instance.getOnlineSince()
           << ", base_path = " << instance.getPath()
           << ", server_id = " << instance.getServerId()
           << ", server_instance_id = " << instance.getServerInstanceId()
           <<" }";
    return stream;
}

std::ostream& operator<<(std::ostream& os, InstanceLiveness const& lv)
{
    os << "{m:" << lv.getMembershipId()
       << ",v:" << lv.getVersion()
       << ",a(" << lv.getNumLive() << "):[";
    for (auto const& ile : lv.getLiveInstances()) {
        os << Iid(ile.getInstanceId(), Iid::Terse())
           << "/g:" << ile.getGenerationId() << ',';
    }
    os << "],d(" << lv.getNumDead() << "):[";
    for (auto const& ile : lv.getDeadInstances()) {
        os << Iid(ile.getInstanceId(), Iid::Terse())
           << "/g:" << ile.getGenerationId() << ',';
    }
    os << "]}";
    return os;
}

} // namespace scidb
