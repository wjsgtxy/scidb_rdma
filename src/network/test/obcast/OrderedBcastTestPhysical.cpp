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

#include <array/MemArray.h>
#include <network/Connection.h>
#include <network/Network.h>
#include <network/OrderedBcast.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/Event.h>
#include <util/Mutex.h>

#include <log4cxx/logger.h>

namespace scidb
{

using namespace std;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.obcast.test"));

class OrderedBcastTestPhysical: public PhysicalOperator
{
private:
    Mutex _mutex;
    Event _event;
    std::string _error;
    InstanceID _selfInstanceId;
    size_t _numInstances;
    std::vector<std::pair<InstanceID, uint64_t> > _messageList;
    const static MessageID MSG_ID;
    size_t _MSG_NUM;

public:

    OrderedBcastTestPhysical(const string& logicalName, const string& physicalName,
                             const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    ~OrderedBcastTestPhysical()
    {
        scidb::getNetworkMessageFactory()->removeMessageType(MSG_ID);
    }

    void preSingleExecute(std::shared_ptr<Query> query) override
    {
    }
    void setQuery(const std::shared_ptr<Query>& query) override
    {
        PhysicalOperator::setQuery(query);
    }

    scidb::MessagePtr createRequest(scidb::MessageID msgId)
    {
        ASSERT_EXCEPTION(msgId == MSG_ID, "BAD MSG_ID");
        return MessagePtr(std::make_shared<scidb_msg::DummyQuery>());
    }

    void handleRequest(const std::shared_ptr<MessageDescription>& dummyMsg)
    {
        const InstanceID sourceInstanceId = dummyMsg->getSourceInstanceID();

        std::shared_ptr<scidb_msg::DummyQuery> repPtr =
                dynamic_pointer_cast<scidb_msg::DummyQuery>(dummyMsg->getRecord());

        if (!repPtr ||
            !repPtr->IsInitialized() ||
            !repPtr->has_payload_id() ||
            !repPtr->has_cluster_uuid() ||
            repPtr->cluster_uuid() != Cluster::getInstance()->getUuid()) {
            ScopedMutexLock cs(_mutex, PTW_SML_ORDERED_BCAST_TEST);
            if (_error.empty()) { _error = "XXXX Bug in OBCAST message validation"; }
            _event.signal();
            return;
        }
        const uint64_t payload = repPtr->payload_id();

        ScopedMutexLock cs(_mutex, PTW_SML_ORDERED_BCAST_TEST);
        _messageList.push_back(std::make_pair(sourceInstanceId, payload));
        if (_messageList.size() == _MSG_NUM*_numInstances) { _event.signal(); }
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override
    {
        Cluster* cluster = Cluster::getInstance();
        SCIDB_ASSERT(cluster);
        _selfInstanceId = Cluster::getInstance()->getLocalInstanceId();
        _numInstances = query->getCoordinatorLiveness()->getNumLive();
        _messageList.reserve(_numInstances);

        ASSERT_EXCEPTION(_parameters.size() == 1, "XXXX");
        ASSERT_EXCEPTION(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION, "XXXX");
        std::shared_ptr<OperatorParamPhysicalExpression> paramExpr =
           (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0];
        ASSERT_EXCEPTION(paramExpr->isConstant(), "XXXX");
        _MSG_NUM = paramExpr->getExpression()->evaluate().getUint64();

        // register our message types
        std::shared_ptr<scidb::NetworkMessageFactory> factory = scidb::getNetworkMessageFactory();
        NetworkMessageFactory::MessageCreator msgCreator =
            std::bind(&OrderedBcastTestPhysical::createRequest,
                      this,
                      std::placeholders::_1);
        NetworkMessageFactory::MessageHandler msgHandler =
            std::bind(&OrderedBcastTestPhysical::handleRequest,
                      this,
                      std::placeholders::_1);
        bool installed = factory->addMessageType(MSG_ID, msgCreator, msgHandler);
        if (!installed) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_SEND_RECEIVE);
        }
        msgCreator = nullptr;
        msgHandler = nullptr;

        // wait for everybody to get here
        syncBarrier(0, query);
        syncBarrier(1, query);

        // broadcast _MSG_NUM number of messages
        for (size_t i=0; i < _MSG_NUM; ++i) {

            std::shared_ptr<MessageDesc> dummyMsg = std::make_shared<Connection::ServerMessageDesc>();

            dummyMsg->initRecord(MSG_ID);
            dummyMsg->setQueryID(query->getQueryID());
            dummyMsg->setSourceInstanceID(_selfInstanceId);

            std::shared_ptr<scidb_msg::DummyQuery> dummyRecord = dummyMsg->getRecord<scidb_msg::DummyQuery>();
            dummyRecord->set_payload_id(i);
            dummyRecord->set_cluster_uuid(cluster->getUuid());

            OrderedBcastManager::getInstance()->broadcast(dummyMsg);
        }

        // wait for the arrival of all the messages
        {
            ScopedMutexLock cs(_mutex, PTW_SML_ORDERED_BCAST_TEST);
            Event::ErrorChecker errorChecker = std::bind(&Query::validateQueryPtr, query);
            while (_messageList.size() != _MSG_NUM*_numInstances) {
                _event.wait(_mutex, errorChecker, PTW_EVENT_SG_PULL);
            }

            if (!_error.empty()) {
                factory->removeMessageType(MSG_ID);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << _error);
            }

            SCIDB_ASSERT(_messageList.size() == _MSG_NUM*_numInstances);
        }

        // wait for everybody to get here
        syncBarrier(0, query);
        syncBarrier(1, query);

        if (logger->isTraceEnabled()) {
            for (size_t j=0; j < _messageList.size(); ++j)
            {
                LOG4CXX_TRACE(logger, "XXXX Local messages: "
                              << " local["<<j<<"]=(" << _messageList[j].first
                              <<","<< _messageList[j].second<<")" );
            }
        }

        // broadcast the list of received messages to every one else
        size_t sizeOfMsgs = _messageList.size()*sizeof(std::pair<InstanceID, uint64_t>);
        for (size_t i=0; i < _numInstances; ++i) {
            if (i == query->getInstanceID()) { continue; }

            scidb::Send(&query, i, &_messageList[0], sizeOfMsgs);
        }

        // receive the list of messages and compare it with the own (in order)
        for (size_t i=0; i < _numInstances; ++i) {
            if (i == query->getInstanceID()) { continue; }

            std::shared_ptr<SharedBuffer> buf = scidb::BufReceive(i, query);
            if (sizeOfMsgs != buf->getSize()) {
                stringstream s;
                s << "XXXX Bug in OBCAST messaging "<< " numBytesRecvd=" << buf->getSize()
                  << " from=" << i << " numBytesSent=" << sizeOfMsgs ;
                factory->removeMessageType(MSG_ID);
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << s.str());
            }
            auto recvdMsgs =
                    reinterpret_cast<const std::pair<InstanceID, uint64_t>* >(buf->getConstData());
            for (size_t j=0; j < _messageList.size(); ++j) {
                if (_messageList[j] != recvdMsgs[j]) {
                    stringstream s;
                    s << "XXXX Bug in OBCAST message ordering from=" << i
                      << " recvd["<<j<<"]=(" << recvdMsgs[j].first    <<","<< recvdMsgs[j].second << ")"
                      << " local["<<j<<"]=(" << _messageList[j].first <<","<< _messageList[j].second<<")" ;
                    factory->removeMessageType(MSG_ID);
                    throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << s.str());
                }
            }
        }
        // remove our message type handlers
        factory->removeMessageType(MSG_ID);
        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }

    void postSingleExecute(std::shared_ptr<Query> query) override
    {
    }
};
const MessageID OrderedBcastTestPhysical::MSG_ID = 888;
REGISTER_PHYSICAL_OPERATOR_FACTORY(OrderedBcastTestPhysical, "_obcast_test", "OrderedBcastTestPhysical");

}
