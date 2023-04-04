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

#include <network/Network.h>

#include "Connection.h"
#include <network/NetworkManager.h>
#include <network/NetworkMessage.h>
#include <network/ThrottledScheduler.h>

#include <array/MemoryBuffer.h>
#include <query/Query.h>
#include <system/UserException.h>
#include <google/protobuf/descriptor.h>

namespace
{
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));
}  // namespace

namespace scidb
{

void Send(void* ctx, InstanceID instance, void const* data, size_t size)
{
    std::shared_ptr<SharedBuffer> buf = std::make_shared<MemoryBuffer>(data, size);
    NetworkManager::getInstance()->send(instance, buf,
                                        *(std::shared_ptr<Query>*)ctx);
}

void Receive(void* ctx, InstanceID instance, void* data, size_t size)
{
    NetworkManager* nm = NetworkManager::getInstance();
    SCIDB_ASSERT(nm);
    std::shared_ptr<SharedBuffer> buf = nm->receive(instance,
                                               *(std::shared_ptr<Query>*)ctx);
    SCIDB_ASSERT(buf->getSize() == size);
    memcpy(data, buf->getConstData(), buf->getSize());
}

void BufSend(InstanceID target, std::shared_ptr<SharedBuffer> const& data, std::shared_ptr<Query>& query)
{
    NetworkManager::getInstance()->send(target, data, query);
}

std::shared_ptr<SharedBuffer> BufReceive(InstanceID source, std::shared_ptr<Query>& query)
{
    return NetworkManager::getInstance()->receive(source,query);
}

void BufBroadcast(std::shared_ptr<SharedBuffer> const& data, std::shared_ptr<Query>& query)
{
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtBufferSend, data);
    msg->setQueryID(query->getQueryID());
    NetworkManager::getInstance()->broadcastLogical(msg);
}

std::shared_ptr<NetworkMessageFactory> getNetworkMessageFactory()
{
   return NetworkManager::getInstance()->getNetworkMessageFactory();
}

boost::asio::io_service& getIOService()
{
   return NetworkManager::getInstance()->getIOService();
}

std::shared_ptr<MessageDesc> prepareMessage(MessageID msgID,
                                            MessagePtr record,
                                            boost::asio::const_buffer& binary)
{
   std::shared_ptr<SharedBuffer> payload;
   if (boost::asio::buffer_size(binary) > 0) {
      assert(boost::asio::buffer_cast<const void*>(binary));
      payload = std::shared_ptr<SharedBuffer>(new MemoryBuffer(boost::asio::buffer_cast<const void*>(binary),
                                                          boost::asio::buffer_size(binary)));
   }
   std::shared_ptr<MessageDesc> msgDesc =
           std::make_shared<Connection::ServerMessageDesc>(payload);

   msgDesc->initRecord(msgID);
   MessagePtr msgRecord = msgDesc->getRecord<Message>();
   const google::protobuf::Descriptor* d1 = msgRecord->GetDescriptor();
   assert(d1);
   const google::protobuf::Descriptor* d2 = record->GetDescriptor();
   assert(d2);
   if (d1->full_name().compare(d2->full_name()) != 0) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE);
   }
   msgRecord->CopyFrom(*record.get());

   return msgDesc;
}

void sendAsyncPhysical(InstanceID targetInstanceID,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
   std::shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
   assert(msgDesc);
   NetworkManager::getInstance()->sendPhysical(targetInstanceID, msgDesc);
}

void sendAsyncClient(ClientContext::Ptr& clientCtx,
               MessageID msgID,
               MessagePtr record,
               boost::asio::const_buffer& binary)
{
    Connection* conn = dynamic_cast<Connection*>(clientCtx.get());
    if (conn == NULL) {
        throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_CTX)
               << typeid(*clientCtx).name());
    }
    std::shared_ptr<MessageDesc> msgDesc = prepareMessage(msgID,record,binary);
    assert(msgDesc);
    conn->sendMessage(msgDesc);
}

std::shared_ptr<WorkQueue> getWorkQueue()
{
    return NetworkManager::getInstance()->getWorkQueue();
}

uint32_t getLivenessTimeout()
{
   return Config::getInstance()->getOption<int>(CONFIG_LIVENESS_TIMEOUT);
}

std::shared_ptr<Scheduler> getScheduler(Scheduler::Work& workItem, time_t period)
{
   if (!workItem) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_WORK_ITEM);
   }
   if (period < 1) {
      throw USER_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_INVALID_SHEDULER_PERIOD);
   }
   std::shared_ptr<scidb::Scheduler> scheduler(new ThrottledScheduler(period, workItem,
                                          NetworkManager::getInstance()->getIOService()));
   return scheduler;
}

void resolveComplete(std::shared_ptr<boost::asio::ip::tcp::resolver>& resolver,
                     std::shared_ptr<boost::asio::ip::tcp::resolver::query>& query,
                     ResolverFunc& cb,
                     const boost::system::error_code& error,
                     boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
{
    try {
        cb(error, endpoint_iterator);
    } catch (const scidb::Exception& e) {
        LOG4CXX_ERROR(logger, "Name resolution callback failed with: "<<e.what());
        assert(false);
    }
}

void resolveAsync(const std::string& address, const std::string& service, ResolverFunc& cb)
{
    std::shared_ptr<boost::asio::ip::tcp::resolver> resolver(
        new boost::asio::ip::tcp::resolver(NetworkManager::getInstance()->getIOService()));
    std::shared_ptr<boost::asio::ip::tcp::resolver::query> query =
        std::make_shared<boost::asio::ip::tcp::resolver::query>(address, service);
    resolver->async_resolve(*query,
                            std::bind(&scidb::resolveComplete,
                                      resolver,
                                      query,
                                      cb,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
}

}  // namespace scidb
