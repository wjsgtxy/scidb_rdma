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

/*
 * @file SciDBRemote.cpp
 *
 * @author roman.simakov@gmail.com
 * @author smirnoffjr@gmail.com
 *
 * @brief SciDB API implementation to communicate with instance by network // dz 这是对之前那个对外提供的 SciDBAPI的一个实现，实现客户端的方法去和instance连接，执行查询等
 */

#include "ClientAuthenticator.h"
#include "SciDBAPI.h"

#include <array/StreamArray.h>
#include <array/CompressedBuffer.h>
#include <network/Connection.h>
#include <network/MessageUtils.h>
#include <network/BaseConnection.h>
#include <network/MessageDesc.h>
#include <system/Exceptions.h>
#include <system/ErrorCodes.h>
#include <util/PluginManager.h>
#include <util/Singleton.h>
#include <rbac/SessionProperties.h>
#include <array/ArrayDistributionInterface.h> // For typedef ArrayDistPtr;
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/logger.h>

#include <fstream>
#include <memory>
#include <regex>
#include <stdlib.h>
#include <string>

using namespace std;

namespace scidb
{
/**
 * Initialize instance
 */
class __Init
{
public:
    __Init()
    {
        log4cxx::BasicConfigurator::configure();
        log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
        rootLogger->setLevel(log4cxx::Level::toLevel("ERROR"));
    }
} __init;

/**
 * Class which associating active queries with warnings queues on client, so it easy to add new
 * warning from anywhere when it received from server
 */
class SciDBWarnings: public Singleton<SciDBWarnings>
{
public:
    void postWarning(QueryID queryId, const Warning &warning)
    {
#ifdef SCIDB_CLIENT
    	ScopedMutexLock _lock(_mapLock, PTW_SML_SR);
        assert(_resultsMap.find(queryId) != _resultsMap.end());
        _resultsMap[queryId]->_postWarning(warning);
#endif
    }

    void associateWarnings(QueryID queryId, QueryResult *res)
    {
#ifdef SCIDB_CLIENT
    	ScopedMutexLock _lock(_mapLock, PTW_SML_SR);
    	_resultsMap[queryId] = res;
#endif
    }

    void unassociateWarnings(QueryID queryId)
    {
#ifdef SCIDB_CLIENT
    	ScopedMutexLock _lock(_mapLock, PTW_SML_SR);
    	_resultsMap.erase(queryId);
#endif
    }

private:
    std::map<QueryID, QueryResult*> _resultsMap;
    Mutex _mapLock;
};

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

boost::asio::io_service ioService;


/**
 * ClientArray inherits StreamArray and implement nextChunk method by requesting network
 */
class ClientArray: public StreamArray // dz 客户端的array，用于存储结果集
{
public:
    ClientArray( BaseConnection* connection, const ArrayDesc& arrayDesc, QueryID queryID, QueryResult& queryResult):
    StreamArray(arrayDesc), _connection(connection), _queryID(queryID), _queryResult(queryResult)
    {
    }

protected:
    // overloaded method
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

private:
    BaseConnection* _connection;
    QueryID _queryID;
    QueryResult& _queryResult;
};

std::string getModuleFileName()
{
    // read the full name, e.g. '/tmp/dir/myfile'
    const size_t maxLength = PATH_MAX+NAME_MAX;
    char exepath[maxLength+1] = {0};
    ssize_t len = readlink("/proc/self/exe", exepath, maxLength);
    if (len==-1 || len==0) return exepath;
    exepath[len]=0;
    return exepath;
}

std::string getCommandLineOptions()
{
    stringstream cmdline;
    FILE *f = fopen("/proc/self/cmdline", "rb");
    char *arg = NULL;
    size_t size = 0;
    bool first = true;
    while(getdelim(&arg, &size, 0, f) != -1) {
        if (!first) {
            cmdline << arg << " ";
        } else {
            first = false;
        }
    }
    free(arg);
    fclose(f);
    return cmdline.str();
}

void fillProgramOptions(std::string& programOptions)
{
    programOptions = getModuleFileName() + " " + getCommandLineOptions();
}


/**
 * Remote implementation of the SciDBAPI interface // dz scidb接口的一个实现，iquery就是调用的这个接口，其实最后是调用的这里的实现
 */
class SciDBRemote : public scidb::SciDBClient // dz scidbremote->scidbclient->scidb 父子关系
{
public:
    virtual ~SciDBRemote() {}

    void* connect(SessionProperties const& props,
                  const std::string& host,
                  uint16_t port) override
    {
        // Capturing the BaseConnection in a unique pointer ensures that
        // it is deleted should an exception be thrown between when it's
        // constructed and when it's returned.  At the point of return, the
        // unique pointer's claim on the object is released.
        auto conn = std::make_unique<BaseConnection>(ioService); // 直接创建的baseconn，我记得用的是同步的方法
        conn->connect(host, port);
        unique_ptr<ClientAuthenticator> ca(ClientAuthenticator::create(props));
        ca->authenticate(conn.get());
        return conn.release(); // dz 释放了智能指针对该Conn的管理，返回原始conn指针
    }

    void disconnect(void* connection_handle)
    {
        BaseConnection* connection =
            static_cast<BaseConnection*>(connection_handle);
        if (connection) {
            connection->disconnect();
            delete connection;
        }
    }

    // 整个过程就是同步的发送消息然后同步接收消息，返回的消息里面带有一个query id，将这个query id赋值给引用参数 queryResult
    void prepareQuery(const std::string& queryString, bool afl,
                      const std::string&, QueryResult& queryResult,
                      void* connection_handle)
    {
        BaseConnection* connection = static_cast<BaseConnection*>(connection_handle);

        std::shared_ptr<MessageDesc> queryMessage = std::make_shared<MessageDesc>(mtPrepareQuery); // dz 创建msg，prepare创建的就是scidb_msg::Query类型的消息
        queryMessage->getRecord<scidb_msg::Query>()->set_query(queryString);
        queryMessage->getRecord<scidb_msg::Query>()->set_afl(afl);

        std::string programOptions;
        fillProgramOptions(programOptions);
        queryMessage->getRecord<scidb_msg::Query>()->set_program_options(programOptions);

        LOG4CXX_TRACE(logger, "Send " << (afl ? "AFL" : "AQL") << " for preparation " << queryString);

        ASSERT_EXCEPTION(connection!=NULL,"NULL connection");
        std::shared_ptr<MessageDesc> resultMessage =
            connection->sendAndReadMessage<MessageDesc>(queryMessage); // dz 这是一个同步的模板方法，先同步发送client消息，然后接收消息

        if (resultMessage->getMessageType() != mtQueryResult) {
            assert(resultMessage->getMessageType() == mtError);

            makeExceptionFromErrorMessageAndThrowOnClient(resultMessage);
        }

        std::shared_ptr<scidb_msg::QueryResult> queryResultRecord =
            resultMessage->getRecord<scidb_msg::QueryResult>(); // 获取的查询结果集，在scidb_msg.proto定义，AttributeDesc，DimensionDesc，查询的一些基本信息，查询花费时间等

        SciDBWarnings::getInstance()->associateWarnings(resultMessage->getQueryID(), &queryResult);
        for (int i = 0; i < queryResultRecord->warnings_size(); i++)
        {
            const ::scidb_msg::QueryResult_Warning& w = queryResultRecord->warnings(i);
            SciDBWarnings::getInstance()->postWarning(
                        resultMessage->getQueryID(),
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }

         // Processing result message
        queryResult.queryID = resultMessage->getQueryID();
        if (queryResultRecord->has_exclusive_array_access()) {
            queryResult.requiresExclusiveArrayAccess = queryResultRecord->exclusive_array_access();
        }

        LOG4CXX_TRACE(logger, "Result for query " << queryResult.queryID);
    }

    // 执行真正的查询，之前已经通过上面的 prepareQuery 准备好了query id了
    void executeQuery(const std::string& queryString, bool afl,
                      QueryResult& queryResult,
                      void* connection_handle)
    {
        BaseConnection* connection = static_cast<BaseConnection*>(connection_handle);

        std::shared_ptr<MessageDesc> queryMessage = std::make_shared<MessageDesc>(mtExecuteQuery);
        queryMessage->getRecord<scidb_msg::Query>()->set_query(queryString);
        queryMessage->getRecord<scidb_msg::Query>()->set_afl(afl);
        std::string programOptions;
        fillProgramOptions(programOptions);
        queryMessage->getRecord<scidb_msg::Query>()->set_program_options(programOptions);
        queryMessage->getRecord<scidb_msg::Query>()->set_fetch(queryResult.fetch);
        queryMessage->setQueryID(queryResult.queryID);

        if (!queryResult.queryID.isValid()) {
            LOG4CXX_TRACE(logger, "Send " << (afl ? "AFL" : "AQL") << " for execution " << queryString);
        }
        else {
            LOG4CXX_TRACE(logger, "Send prepared query " << queryResult.queryID << " for execution");
        }

        ASSERT_EXCEPTION(connection!=NULL,"NULL connection");
        std::shared_ptr<MessageDesc> resultMessage =
            connection->sendAndReadMessage<MessageDesc>(queryMessage);

        if (resultMessage->getMessageType() != mtQueryResult) {
            assert(resultMessage->getMessageType() == mtError);

            makeExceptionFromErrorMessageAndThrowOnClient(resultMessage);
        }

        // Processing result message
        std::shared_ptr<scidb_msg::QueryResult> queryResultRecord =
            resultMessage->getRecord<scidb_msg::QueryResult>();

        queryResult.queryID = resultMessage->getQueryID();

        LOG4CXX_TRACE(logger, "Result for query " << queryResult.queryID);

        queryResult.autoCommit = queryResultRecord->auto_commit();
        queryResult.selective = queryResultRecord->selective();
        if (queryResult.selective) // 查询类的消息，返回的结果集中有 attrs和dims等
        {
            Attributes attributes; // 是一个vector，存的是AttributeDesc
            for (int i = 0; i < queryResultRecord->attributes_size(); i++)
            {
                Value defaultValue;
                if (queryResultRecord->attributes(i).default_missing_reason() >= 0) {
                    defaultValue.setNull(safe_static_cast<Value::reason>(
                                             queryResultRecord->attributes(i).default_missing_reason()));
                } else {
                    defaultValue.setData(
                        queryResultRecord->attributes(i).default_value().data(),
                        queryResultRecord->attributes(i).default_value().size());
                }
                attributes.push_back(AttributeDesc(
                        queryResultRecord->attributes(i).name(),
                        queryResultRecord->attributes(i).type(),
                        safe_static_cast<int16_t>(queryResultRecord->attributes(i).flags()),
                        static_cast<CompressorType>(
                            queryResultRecord->attributes(i).default_compression_method()),
                        std::set<std::string>(),
                        0, &defaultValue)
                );
            }

            Dimensions dimensions;
            for (int i = 0; i < queryResultRecord->dimensions_size(); i++)
            {
                DimensionDesc dim(
                        queryResultRecord->dimensions(i).name(),
                        queryResultRecord->dimensions(i).start_min(),
                        queryResultRecord->dimensions(i).curr_start(),
                        queryResultRecord->dimensions(i).curr_end(),
                        queryResultRecord->dimensions(i).end_max(),
                        queryResultRecord->dimensions(i).chunk_interval(),
                        queryResultRecord->dimensions(i).chunk_overlap());

                dimensions.push_back(dim);
            }

            queryResult.executionTime = queryResultRecord->execution_time(); // 执行时间，TODO，不知道为什么这个一直是0？
            queryResult.explainLogical = queryResultRecord->explain_logical();
            queryResult.explainPhysical = queryResultRecord->explain_physical();
            // 结果集的array，是一个新的arraydesc
            const ArrayDesc arrayDesc(queryResultRecord->array_name(),
                                      attributes,
                                      dimensions,
                                      ArrayDistPtr(),
                                      ArrayResPtr(),
                                      /*flags*/ 0,
                                      /*isCcmClient*/ true);

            queryResult.array = std::shared_ptr<Array>(
                new ClientArray(connection, arrayDesc, queryResult.queryID, queryResult)); // 将结果集存储在了queryResult的array字段了
        }

        // Post the warnings.
        SciDBWarnings::getInstance()->associateWarnings(resultMessage->getQueryID(), &queryResult);
        for (int i = 0; i < queryResultRecord->warnings_size(); i++)
        {
            const ::scidb_msg::QueryResult_Warning& w = queryResultRecord->warnings(i);
            SciDBWarnings::getInstance()->postWarning(
                        resultMessage->getQueryID(),
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }
    }

    void cancelQuery(QueryID queryID, void* connection_handle)
    {
        BaseConnection* connection = static_cast<BaseConnection*>(connection_handle);

        std::shared_ptr<MessageDesc> cancelQueryMessage = std::make_shared<MessageDesc>(mtCancelQuery);
        cancelQueryMessage->setQueryID(queryID);

        LOG4CXX_TRACE(logger, "Canceling query for execution " << queryID);

        ASSERT_EXCEPTION(connection!=NULL,"NULL connection");
        std::shared_ptr<MessageDesc> resultMessage =
            connection->sendAndReadMessage<MessageDesc>(cancelQueryMessage);

        if (resultMessage->getMessageType() == mtError) {
            std::shared_ptr<scidb_msg::Error> error = resultMessage->getRecord<scidb_msg::Error>();
            if (error->short_error_code() != SCIDB_E_NO_ERROR)
                makeExceptionFromErrorMessageAndThrowOnClient(resultMessage);
        } else {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE2)
                << resultMessage->getMessageType();
        }
    }

    // commit提交
    void completeQuery(QueryID queryID, void* connection_handle)
    {
        BaseConnection* connection = static_cast<BaseConnection*>(connection_handle);

        std::shared_ptr<MessageDesc> completeQueryMessage = std::make_shared<MessageDesc>(mtCompleteQuery);
        completeQueryMessage->setQueryID(queryID);

        LOG4CXX_TRACE(logger, "Completing query for execution " << queryID);

        ASSERT_EXCEPTION(connection!=NULL,"NULL connection");
        std::shared_ptr<MessageDesc> resultMessage =
            connection->sendAndReadMessage<MessageDesc>(completeQueryMessage);

        if (resultMessage->getMessageType() == mtError) {
            std::shared_ptr<scidb_msg::Error> error = resultMessage->getRecord<scidb_msg::Error>();
            if (error->short_error_code() != SCIDB_E_NO_ERROR)
                makeExceptionFromErrorMessageAndThrowOnClient(resultMessage);
        } else {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_MESSAGE_TYPE2)
                << resultMessage->getMessageType();
        }
    }

} _sciDB;


/**
 * C L I E N T   A R R A Y
 * 获取client array的next chunk
 */
ConstChunk const* ClientArray::nextChunk(const AttributeDesc& attId_in, MemChunk& chunk)
{
    auto attId = attId_in.getId();
    LOG4CXX_TRACE(logger, "Fetching next chunk of " << attId << " attribute");
    std::shared_ptr<MessageDesc> fetchDesc = std::make_shared<MessageDesc>(mtFetch);
    fetchDesc->setQueryID(_queryID);
    std::shared_ptr<scidb_msg::Fetch> fetchDescRecord = fetchDesc->getRecord<scidb_msg::Fetch>();
    fetchDescRecord->set_attribute_id(attId);
    fetchDescRecord->set_array_name(getArrayDesc().getName());

    std::shared_ptr<MessageDesc> chunkDesc = _connection->sendAndReadMessage<MessageDesc>(fetchDesc); // 同步获取nextchunk消息

    if (chunkDesc->getMessageType() != mtChunk) {
        assert(chunkDesc->getMessageType() == mtError);

        makeExceptionFromErrorMessageAndThrowOnClient(chunkDesc);
    }

    std::shared_ptr<scidb_msg::Chunk> chunkMsg = chunkDesc->getRecord<scidb_msg::Chunk>();

    if (!chunkMsg->eof())
    {
        LOG4CXX_TRACE(logger, "Next chunk message was received");
        const CompressorType compMethod = static_cast<CompressorType>(chunkMsg->compression_method());
        const size_t decompressedSize = chunkMsg->decompressed_size();

        Address firstElem;
        firstElem.attId = attId;
        for (int i = 0; i < chunkMsg->coordinates_size(); i++) {
            firstElem.coords.push_back(chunkMsg->coordinates(i));
        }

        chunk.initialize(this, &desc, firstElem, compMethod);
        std::shared_ptr<CompressedBuffer> compressedBuffer = dynamic_pointer_cast<CompressedBuffer>(chunkDesc->getBinary());
        compressedBuffer->setCompressionMethod(compMethod);
        compressedBuffer->setDecompressedSize(decompressedSize);
        chunk.decompress(*compressedBuffer);

        for (int i = 0; i < chunkMsg->warnings_size(); i++)
        {
            const ::scidb_msg::Chunk_Warning& w = chunkMsg->warnings(i);
            SciDBWarnings::getInstance()->postWarning(
                        _queryID,
                        Warning(
                            w.file().c_str(),
                            w.function().c_str(),
                            w.line(),
                            w.strings_namespace().c_str(),
                            w.code(),
                            w.what_str().c_str(),
                            w.stringified_code().c_str())
                        );
        }

        LOG4CXX_TRACE(logger, "Next chunk was initialized");
        return &chunk;
    }
    else
    {
        LOG4CXX_TRACE(logger, "There is no new chunks");
        return NULL;
    }
}

#ifdef SCIDB_CLIENT
QueryResult::~QueryResult()
{
    SciDBWarnings::getInstance()->unassociateWarnings(queryID);
}

bool QueryResult::hasWarnings()
{
	ScopedMutexLock lock(_warningsLock, PTW_SML_SR);
	return !_warnings.empty();
}

Warning QueryResult::nextWarning()
{
	ScopedMutexLock lock(_warningsLock, PTW_SML_SR);
	Warning res = _warnings.front();
	_warnings.pop();
	return res;
}

void QueryResult::_postWarning(const Warning &warning)
{
	ScopedMutexLock lock(_warningsLock, PTW_SML_SR);
	_warnings.push(warning);
}
#endif

/**
 * E X P O R T E D   F U N C T I O N
 */
EXPORTED_FUNCTION SciDBClient& getSciDB()
{
    return _sciDB;
}

}
