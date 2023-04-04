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

// The header file for the implementation details in this file
#include "CcmBridge.h"
// header files from the ccm module
#include "Uuid.h"
// SciDB modules
#include <SciDBAPI.h>
#include <array/ArrayWriter.h>
#include <query/QueryID.h>
#include <rbac/Credential.h>
#include <system/Config.h>
#include <system/UserException.h>
// c++ standard headers
#include <memory>
#include <string>
#include <unordered_map>

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ccm.CcmBridge"));
// clang-format off
}
// clang-format on

namespace scidb { namespace ccm { namespace bridge {

using SciDbConnHandler = void*;
using SciDbConnMap = std::unordered_map<Uuid, SciDbConnHandler>;

// A mapping of Ccm session identifiers to the SciDB connection used for
// processing queries for that Ccm session identifier.
SciDbConnMap scidbConns;

IngotProducer::IngotProducer(const scidb::QueryID& queryID,
                             const std::string& resultPayload,
                             bool success)
    : _queryID(queryID)
    , _resultPayload(resultPayload)
    , _success(success)
{}

IngotProducer::iterator IngotProducer::begin()
{
    return {_resultPayload};
}

IngotProducer::iterator IngotProducer::end()
{
    return {""};
}

IngotProducer::iterator::iterator(const std::string& queryResultBuffer)
    : _queryResultBuffer(queryResultBuffer)
{}

const std::string& IngotProducer::iterator::operator*() const
{
    return _queryResultBuffer;
}

IngotProducer::iterator& IngotProducer::iterator::operator++()
{
    _queryResultBuffer.clear();
    return *this;
}

// Pilfered and hacked from iquery.cpp
scidb::QueryID _executeSciDBQuery_internal(const std::string& queryString,
                                           scidb::QueryResult& queryResult,
                                           const std::string& format,
                                           SciDbConnHandler handle,
                                           FILE* resultStream,
                                           bool& success)
{
    scidb::SciDBClient& sciDB = scidb::getSciDB();
    success = false;
    try {
        sciDB.prepareQuery(queryString, true, "", queryResult, handle);
        sciDB.executeQuery(queryString, true, queryResult, handle);
    } catch (...) {
        LOG4CXX_INFO(logger, " - Query Failed: " << queryString);
        return INVALID_QUERY_ID;
    }
    success = true;
    // Output "maximum precision" from ArrayWriter, TODO See SDB-5545
    scidb::ArrayWriter::setPrecision(-1);
    std::shared_ptr<scidb::Query> emptyQuery;
    if (queryResult.array) {
        scidb::ArrayWriter::save(*queryResult.array,
                                 "",
                                 emptyQuery,
                                 format,
                                 0,  // flags
                                 resultStream);
    }

    auto currentQueryID = scidb::QueryID();

    if (queryResult.queryID.isValid() && !queryResult.autoCommit && handle) {
        sciDB.completeQuery(queryResult.queryID, handle);
        currentQueryID = queryResult.queryID;
    }

    return currentQueryID;
}

bool authenticate(const std::string& userName,
                  const std::string& challengeResponse,
                  const Uuid& ccmSessionId,
                  int sessionPriority)
{
    scidb::Credential cred;
    cred.setUsername(userName);
    cred.setPassword(challengeResponse);
    scidb::SessionProperties sessionProperties(cred);
    sessionProperties.setPriority(sessionPriority);
    scidb::SciDBClient& sciDB = scidb::getSciDB();
    std::string connectionString = "localhost";
    uint16_t port = static_cast<uint16_t>(
        scidb::Config::getInstance()->getOption<int>(scidb::CONFIG_PORT));
    try {
        auto sciDbConnection = sciDB.connect(sessionProperties, connectionString, port);
        if (sciDbConnection) {
            auto inserted = scidbConns.insert(std::make_pair(ccmSessionId, sciDbConnection));
            return inserted.second;
        }
    } catch (scidb::UserException& e) {
        if (e.getShortErrorCode() != scidb::SCIDB_SE_AUTHENTICATION) {
            LOG4CXX_ERROR(logger,
                          "authenticate system error: " << e.getStringifiedErrorId() << "," << e.what());
            e.raise();
        }
    }
    return false;
}

std::shared_ptr<IngotProducer> executeQuery(const std::string& afl,
                                            const std::string& format,
                                            const Uuid& ccmSessionId)
{
    auto sciDbConnectionIter = scidbConns.find(ccmSessionId);
    bool success = false;
    if (sciDbConnectionIter != scidbConns.end()) {
        scidb::QueryResult queryResult;
        char* resultBuffer = nullptr;
        size_t resultBufferSize = 0;
        FILE* stream = open_memstream(&resultBuffer, &resultBufferSize);
        scidb::QueryID queryID;

        auto sciDbConnection = sciDbConnectionIter->second;
        queryID = _executeSciDBQuery_internal(afl,
                                              queryResult,
                                              format,
                                              sciDbConnection,
                                              stream,
                                              success);
        std::string queryResultBuffer(resultBuffer, resultBufferSize);
        if (!queryResult.array) {
            // ArrayWriter::save(..) calls flush or close on (FILE*) resultBuff.
            // Since we are not calling ArrayWriter nothing is going into resultBuff
            ::fclose(stream);
        }
        free(resultBuffer);
        return std::make_shared<IngotProducer>(queryID, queryResultBuffer, success);
    }

    return std::make_shared<IngotProducer>();  // empty query result, begin() == end()
}

bool destroy(const Uuid& ccmSessionId) noexcept
{
    try {
        auto southboundIter = scidbConns.find(ccmSessionId);
        if (southboundIter != scidbConns.end()) {
            scidb::SciDBClient& sciDB = scidb::getSciDB();
            sciDB.disconnect(southboundIter->second);
            scidbConns.erase(southboundIter);
            return true;
        }
    } catch (std::exception const& e) {
        LOG4CXX_ERROR(logger, "Destroying of southboundConnections exception: " << e.what());
    }
    return false;
}

bool operator==(const IngotProducer::iterator& lhs, const IngotProducer::iterator& rhs)
{
    // TODO: This isn't a long-term solution, but preserves the blocking behavior
    // as-implemented on the client API branch.
    return *lhs == *rhs;
}

bool operator!=(const IngotProducer::iterator& lhs, const IngotProducer::iterator& rhs)
{
    return !(lhs == rhs);
}

}}}  // namespace scidb::ccm::bridge
