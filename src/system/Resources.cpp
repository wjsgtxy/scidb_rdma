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
 * @file Resources.cpp
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @brief Transparent interface for examining cluster physical resources
 */

#include <system/Resources.h>

#include <network/MessageDesc.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Query.h>
#include <util/Semaphore.h>
#include <util/Mutex.h>
#include <util/Utility.h>

#include <boost/filesystem/operations.hpp>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.system.resources"));

using namespace std;

namespace scidb
{

constexpr char const * const Resources::cls;


class BaseResourcesCollector
{
public:
    BaseResourcesCollector() = default;
    virtual ~BaseResourcesCollector() = default;

protected:
    Semaphore _collectorSem;
    friend class Resources;
};

class FileExistsResourcesCollector: public BaseResourcesCollector
{
public:
    void collect(InstanceID piid, bool exists, bool release = true)
    {
        LOG4CXX_TRACE(logger, "FileExistsResourcesCollector::collect: Physical instance " << Iid(piid)
                      << " reports exists=" << exists);
        {
            ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
            _instancesMap[piid] = exists;
            if (release)
                _collectorSem.release();
        }
    }

    ~FileExistsResourcesCollector() = default;

    map<InstanceID, bool> _instancesMap;

private:
    Mutex _lock;
    friend class Resources;
};

void Resources::fileExists(const string &path,
                           map<InstanceID, bool> &instancesMap,
                           const std::shared_ptr<Query> &query)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": Checking everywhere for file '" << path << "'");
    NetworkManager* networkManager = NetworkManager::getInstance();

    BrcPtr baseCollector(new FileExistsResourcesCollector());
    uint64_t id = 0;
    {
        ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
        id = ++_lastResourceCollectorId;
        _resourcesCollectors[id] = baseCollector;

        // Local call need not mess with semaphore.
        auto collector = safe_dynamic_cast<FileExistsResourcesCollector*>(baseCollector.get());
        collector->collect(query->getPhysicalInstanceID(), checkFileExists(path), /*semRelease:*/false);
    }

    std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
    std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
        msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
    msg->setQueryID(QueryID::getFakeQueryId());
    request->set_resource_request_id(id);
    request->set_file_path(path);
    networkManager->broadcastPhysical(msg);

    LOG4CXX_TRACE(logger, cls << __func__ << ": Waiting while instances return result for collector " << id);

    try
    {
        Semaphore::ErrorChecker errorChecker = std::bind(&Query::validateQueryPtr, query);
       baseCollector->_collectorSem.enter(query->getInstancesCount() - 1, errorChecker, PTW_SEM_RESOURCES);
    }
    catch (...)
    {
        LOG4CXX_TRACE(logger, cls << __func__ << ": Waiting for result of collector " << id <<
            " interrupter by error");
        {
            ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
            _resourcesCollectors.erase(id);
        }
        throw;
    }

    LOG4CXX_TRACE(logger, cls << __func__ << ": Returning result of collector " << id);

    {
        ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
        auto collector = safe_dynamic_cast<FileExistsResourcesCollector*>(_resourcesCollectors[id].get());
        ASSERT_EXCEPTION(collector, "Some other kind of BaseResourcesCollector?!");
        instancesMap = collector->_instancesMap;
        _resourcesCollectors.erase(id);
    }
}

bool Resources::fileExists(const string &path, InstanceID liid, const std::shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": Checking for file '"
                  << path << "' on logical instance " << liid);
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (liid == query->getInstanceID())
    {
        LOG4CXX_TRACE(logger, cls << __func__ << ": Logical instance " << liid <<
                      " is local instance. Returning result.");
        return checkFileExists(path);
    }
    else
    {
        LOG4CXX_TRACE(logger, cls << __func__ << ": Logical instance " << liid
                      << " is remote instance. Requesting result.");
        BrcPtr baseCollector(new FileExistsResourcesCollector());
        uint64_t id = 0;
        {
            ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
            id = ++_lastResourceCollectorId;
            _resourcesCollectors[id] = baseCollector;
        }

        std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
        std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
            msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        msg->setQueryID(QueryID::getFakeQueryId());
        request->set_resource_request_id(id);
        request->set_file_path(path);
        InstanceID physIid = query->mapLogicalToPhysical(liid);
        networkManager->sendPhysical(physIid, msg);

        LOG4CXX_TRACE(logger, cls << __func__ << ": Waiting while instance return result for collector " << id);

        try
        {
            Semaphore::ErrorChecker errorChecker = std::bind(&Query::validateQueryPtr, query);
           baseCollector->_collectorSem.enter(1, errorChecker, PTW_SEM_RESOURCES);
        }
        catch (...)
        {
            LOG4CXX_TRACE(logger, cls << __func__ << ": Exception waiting for result of collector " << id);
            {
                ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
                _resourcesCollectors.erase(id);
            }
            throw;
        }

        bool result = false;
        {
            ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
            auto collector = safe_dynamic_cast<FileExistsResourcesCollector*>(_resourcesCollectors[id].get());
            auto entry = collector->_instancesMap.find(physIid);
            if (entry == collector->_instancesMap.end()) {
                LOG4CXX_ERROR(logger, cls << __func__ << ": Map entry for physical instance "
                              << Iid(physIid) << " is MISSING!")
            } else {
                result = collector->_instancesMap[physIid];
                _resourcesCollectors.erase(id);
            }
        }
        LOG4CXX_TRACE(logger, cls << __func__ << ": Collector " << id << " says file "
                      << (result ? "exists" : "does not exist")
                      << " on logical instance " << liid
                      << " (physical " << Iid(physIid) << ')');

        return result;
    }
}

void Resources::handleFileExists(const std::shared_ptr<MessageDesc>& messageDesc)
{
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (mtResourcesFileExistsRequest == messageDesc->getMessageType())
    {
        std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        const string& file = inMsgRecord->file_path();
        LOG4CXX_TRACE(logger, cls << __func__ <<
                      ": Message mtResourcesFileExistsRequest, checking file " << file);

        std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsResponse);
        msg->setQueryID(QueryID::getFakeQueryId());

        std::shared_ptr<scidb_msg::ResourcesFileExistsResponse> outMsgRecord =
            msg->getRecord<scidb_msg::ResourcesFileExistsResponse>();
        outMsgRecord->set_resource_request_id(inMsgRecord->resource_request_id());
        outMsgRecord->set_exist_flag(Resources::getInstance()->checkFileExists(file));

        networkManager->sendPhysical(messageDesc->getSourceInstanceID(), msg);
    }
    // mtResourcesFileExistsResponse
    else
    {
        std::shared_ptr<scidb_msg::ResourcesFileExistsResponse> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsResponse>();

        LOG4CXX_TRACE(logger, cls << __func__ <<
                      ": Got reply from physical instance " << Iid(messageDesc->getSourceInstanceID()));

        Resources::getInstance()->markFileExists(
            inMsgRecord->resource_request_id(),
            messageDesc->getSourceInstanceID(),
            inMsgRecord->exist_flag());
    }
}

bool Resources::checkFileExists(const std::string &path) const
{
    bool result = false;
    try
    {
        result = boost::filesystem::exists(path);
    }
    catch (std::exception const& e)
    {
        LOG4CXX_TRACE(logger, cls << __func__ << ": Exception: " << e.what());
        result = false;
    }
    catch (...)
    {
        LOG4CXX_TRACE(logger, cls << __func__ << ": Unhandled exception");
        result = false;
    }

    LOG4CXX_TRACE(logger, cls << __func__ << ": path=" << path << ", result=" << result);
    return result;
}

void Resources::markFileExists(uint64_t reqId, InstanceID piid, bool exists)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": requestId=" << reqId <<
                  " reply from physical instance " << Iid(piid) <<
                  ", exists=" << exists);

    BrcPtr baseCollector;
    {
        ScopedMutexLock lock(_lock, PTW_SML_RESOURCES);
        auto entry = _resourcesCollectors.find(reqId);
        if (entry != _resourcesCollectors.end())
        {
            baseCollector = entry->second;
        }
    }

    if (baseCollector)
    {
        auto collector = safe_dynamic_cast<FileExistsResourcesCollector*>(baseCollector.get());
        collector->collect(piid, exists);
    }
    else
    {
        LOG4CXX_WARN(logger, cls << __func__ << ": Cannot find collector for id " << reqId);
    }
}

}
