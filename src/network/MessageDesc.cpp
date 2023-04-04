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

#include "MessageDesc.h"

#include <log4cxx/logger.h>

#include <array/CompressedBuffer.h>
#include <array/SharedBuffer.h>
#include <network/proto/scidb_msg.pb.h>
#include <system/Exceptions.h> // for ASSERT_EXCEPTION
#include <system/Utils.h>      // for SCIDB_ASSERT
#include <util/Utility.h>      // for setToZeroInDebug()

using namespace std;
namespace asio = boost::asio;

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.network.MessageDesc"));

// Table translates MessageID to message name and is also used for
// validating non-plugin MessageIDs.  File-scoped to guarantee early
// initialization.

class MessageTable
{

    vector<string> _entries;

public:
    MessageTable()
    {
        _entries.resize(100); // Probably big enough.
        size_t biggest = 0;

        // clang-format off
#       define X(_name, _code, _desc)                   \
        do {                                            \
            size_t c = static_cast<size_t>(_code);      \
            if (c >= _entries.size()) {                 \
                _entries.resize(c + 1);                 \
            }                                           \
            SCIDB_ASSERT(_entries[c].empty());          \
            _entries[c] = # _name ;                     \
            if (c > biggest) {                          \
                biggest = c;                            \
            }                                           \
        } while (0);

#       include <util/MessageTypes.inc>
#       undef X
        // clang-format on

        if (biggest + 1 < _entries.size()) {
            _entries.resize(biggest + 1);
        }
    }

    size_t size() const { return _entries.size(); }

    const string& operator[](size_t index) const { return _entries[index]; }
};

MessageTable msgTable;

} // anonymous namespace

namespace scidb {

string strMsgType(MessageID msgId)
{
    if (msgId < msgTable.size() && !msgTable[msgId].empty()) {
        return msgTable[msgId];
    }
    stringstream ss;
    ss << "mtUnknown_" << msgId;
    return ss.str();
}

string MessageHeader::str(const string& strPrefix) const
{
    stringstream ss;
    ss << strPrefix << "netProtocolVersion=" << static_cast<uint32_t>(_netProtocolVersion) << '\n'
       << strPrefix << "recordSize=" << _recordSize << '\n'
       << strPrefix << "binarySize=" << _binarySize << '\n'
       << strPrefix << "sourceInstanceID=" << _sourceInstanceID << '\n'
       << strPrefix << "queryID=" << _queryID << '\n'
       << strPrefix << "type=" << strMsgType(_messageType) << endl;
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const MessageHeader& messsageHeader)
{
    out << "netProtocolVersion=" << static_cast<uint32_t>(messsageHeader.getNetProtocolVersion())
        << '\n'
        << "recordSize=" << messsageHeader.getRecordSize() << '\n'
        << "binarySize=" << messsageHeader.getBinarySize() << '\n'
        << "sourceInstanceID=" << messsageHeader.getSourceInstanceID() << '\n'
        << "queryID=" << messsageHeader.getQueryID() << '\n'
        << "messageType=" << static_cast<uint32_t>(messsageHeader.getMessageType()) << std::endl;
    return out;
}

MessageDesc::MessageDesc(MessageID messageType, const std::shared_ptr<SharedBuffer>& binary)
    : _binary(binary)
{
    init(messageType);
}

MessageDesc::MessageDesc() { init(mtNone); }

MessageDesc::MessageDesc(MessageID messageType) { init(messageType); }

MessageDesc::MessageDesc(const std::shared_ptr<SharedBuffer>& binary)
:
    _messageHeader(),
    _binary(binary)
{
    init(mtNone);
    SCIDB_ASSERT(! _messageHeader.getPhysicalOperatorID().isValid());
}

void MessageDesc::init(MessageID messageType)
{
    // all _messageHeader fields must always be set (including _zeroPad)
    _messageHeader.setNetProtocolVersion( scidb_msg::NET_PROTOCOL_CURRENT_VER );
    _messageHeader.setMessageType( static_cast<uint16_t>(messageType) );
    _messageHeader.setZeroPad();
    _messageHeader.setRecordSize( 0 );
    _messageHeader.setBinarySize( 0 );
    _messageHeader.setSourceInstanceID( CLIENT_INSTANCE );
    _messageHeader.setQueryID(INVALID_QUERY_ID);
    _messageHeader.setPhysicalOperatorID();

    if (messageType != mtNone) {
        _record = createRecordByType(messageType); // dz：只要不是mtNone类型的消息，都有对应的结构化部分 _record,看下面的创建record的代码，不同类型消息可能是同一个结构化类型        
    }

    SCIDB_ASSERT(! _messageHeader.getPhysicalOperatorID().isValid());
}

void MessageDesc::writeConstBuffers(std::vector<asio::const_buffer>& constBuffers)
{
    if (_messageHeader.getRecordSize() == 0) {
        ostream out(&_recordStream);
        _record->SerializeToOstream(&out); // dz：序列化成流
        _messageHeader.setRecordSize(_recordStream.size());
    }
    const bool haveBinary = _binary && _binary->getSize();
    if (haveBinary) {
        _messageHeader.setBinarySize(_binary->getSize());
    }
    // dz: 有3个部分，都push进去了，打包一起发送，至少有header和record两个部分，可能有binary
    constBuffers.push_back(asio::buffer(&_messageHeader, sizeof(_messageHeader)));
    constBuffers.push_back(asio::buffer(_recordStream.data()));
    if (haveBinary) {
        constBuffers.push_back(asio::buffer(_binary->getConstData(), _binary->getSize()));
    }

    // dz add
    if(_messageHeader.getRecordSize() == 0){
        LOG4CXX_DEBUG(logger, "dz record size is 0");
        LOG4CXX_DEBUG(logger, "writeConstBuffers: " << strMsgType(_messageHeader.getMessageType())
                                            << " ; recordSize=" << _messageHeader.getRecordSize()
                                            << " ; binarySize=" << _messageHeader.getBinarySize());
        if(haveBinary){
            // recordsize=0, 但是binary size不为0，看有没有这种情况
            LOG4CXX_DEBUG(logger, "record size is 0 and binary is not");
        }
    }

    LOG4CXX_TRACE(logger,
                  "writeConstBuffers: " << strMsgType(_messageHeader.getMessageType())
                  << " ; recordSize=" << _messageHeader.getRecordSize()
                  << " ; binarySize=" << _messageHeader.getBinarySize());
}

bool MessageDesc::parseRecord(size_t bufferSize)
{
    _recordStream.commit(bufferSize);

    _record = createRecord(static_cast<MessageID>(_messageHeader.getMessageType()));

    istream inStream(&_recordStream);
    bool rc = _record->ParseFromIstream(&inStream);
    return (rc && _record->IsInitialized());
}

void MessageDesc::prepareBinaryBuffer()
{
    if (_messageHeader.getBinarySize()) {
        if (_binary) {
            _binary->reallocate(_messageHeader.getBinarySize());
        }
        else {
            // For chunks it's correct but for other data it can required other buffers
            _binary = std::shared_ptr<SharedBuffer>(new CompressedBuffer()); // 注意，这里分配的还是不一样的buffer
            _binary->allocate(_messageHeader.getBinarySize());
        }
    }
}

MessagePtr MessageDesc::createRecordByType(MessageID messageType)
{
    switch (messageType) {
    case mtPrepareQuery:
    case mtExecuteQuery:
        return MessagePtr(new scidb_msg::Query());
    case mtPreparePhysicalPlan:
        return MessagePtr(new scidb_msg::PhysicalPlan());
    case mtHangup:
        return MessagePtr(new scidb_msg::Hangup());
    case mtFetch:
        return MessagePtr(new scidb_msg::Fetch());
    case mtChunk:
    case mtChunkReplica:
    case mtRecoverChunk:
    case mtRemoteChunk:
        return MessagePtr(new scidb_msg::Chunk());
    case mtQueryResult:
    case mtUpdateQueryResult:
        return MessagePtr(new scidb_msg::QueryResult());
    case mtAuthError:
    case mtError:
        return MessagePtr(new scidb_msg::Error());
    case mtNotify:
        return MessagePtr(new scidb_msg::Liveness());
    case mtSyncRequest:
    case mtSyncResponse:
    case mtCancelQuery:
    case mtWait:
    case mtBarrier:
    case mtBufferSend:
    case mtAlive:
    case mtReplicaSyncRequest:
    case mtReplicaSyncResponse:
    case mtAbortRequest:
    case mtAbortResponse:
    case mtCommitRequest:
    case mtCommitResponse:
    case mtCompleteQuery:
        return MessagePtr(new scidb_msg::DummyQuery());
    case mtAuthLogon:
        return MessagePtr(new scidb_msg::AuthLogon());
    case mtAuthChallenge:
        return MessagePtr(new scidb_msg::AuthChallenge());
    case mtAuthResponse:
        return MessagePtr(new scidb_msg::AuthResponse());
    case mtAuthComplete:
        return MessagePtr(new scidb_msg::AuthComplete());
    case mtResourcesFileExistsRequest:
        return MessagePtr(new scidb_msg::ResourcesFileExistsRequest());
    case mtResourcesFileExistsResponse:
        return MessagePtr(new scidb_msg::ResourcesFileExistsResponse());
    case mtControl:
        return MessagePtr(new scidb_msg::Control());

    default:
        LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
    }
}

bool MessageDesc::validate()
{
    if (_messageHeader.getNetProtocolVersion() != scidb_msg::NET_PROTOCOL_CURRENT_VER) {
        LOG4CXX_ERROR(logger,
                      "Invalid protocol version: " << _messageHeader.getNetProtocolVersion());
        return false;
    }

    // Check the msgType against the msgTable.
    static_assert(is_unsigned<MessageID>::value, "Signed MessageID?  Must test for negative here!");
    MessageID msgType = _messageHeader.getMessageType();
    if (msgType == mtNone || !isScidbMessage(msgType)) {
        return false;
    }
    SCIDB_ASSERT(msgType < msgTable.size());
    string const& name = msgTable[msgType];
    if (name.empty() || !::strncmp(name.c_str(), "mtUnused", 7)) {
        return false;
    }
    return true;
}

std::string MessageDesc::str(const std::string& strPrefix) const
{
    std::stringstream ss;
    ss << strPrefix << '\n'
       << _messageHeader.str("  _messageDesc._messageHeader.");

    if (_record) {
        if (_messageHeader.getRecordSize()) {
            ss << " _record {" << '\n'
               << _record->DebugString() << '\n'
               << "}" << std::endl;
        }
    }
    else {
        ss << " _record is NULL" << std::endl;
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const MessageDesc& messsageDesc)
{
    const MessageHeader& header = messsageDesc.getMessageHeader();

    out << header;
    if (messsageDesc.getRecord()) {
        if (header.getRecordSize()) {
            out << " _record {" << std::endl;
            out << messsageDesc.getRecord()->DebugString() << std::endl;
            out << "}" << std::endl;
        }
    }
    else {
        out << " _record is NULL" << std::endl;
    }

    return out;
}

size_t MessageDesc::getMessageSize() const
{
    return _messageHeader.getRecordSize() + _messageHeader.getBinarySize() + sizeof(MessageHeader);
}

QueryID MessageDesc::getQueryID() const { return _messageHeader.getQueryID(); }

void MessageDesc::setQueryID(QueryID queryID) { _messageHeader.setQueryID(queryID); }

void MessageDesc::setPhysicalOperatorID(OperatorID operatorID)
{
    _messageHeader.setPhysicalOperatorID(operatorID);
}

OperatorID MessageDesc::getPhysicalOperatorID() const
{
    return _messageHeader.getPhysicalOperatorID();
}

void MessageDesc::initRecord(MessageID messageType)
{
    assert(_messageHeader.getMessageType() == mtNone);
    _record = createRecord(messageType);
    _messageHeader.setMessageType(static_cast<uint16_t>(messageType));
}

MessageHeader& MessageDesc::getMessageHeader() { return _messageHeader; }
const MessageHeader& MessageDesc::getMessageHeader() const { return _messageHeader; }

const MessagePtr& MessageDesc::getRecord() const { return _record; }

MessagePtr MessageDesc::createRecord(MessageID messageType)
{
    return createRecordByType(static_cast<MessageID>(messageType));
}

InstanceID MessageDesc::getSourceInstanceID() { return _messageHeader.getSourceInstanceID(); }

void MessageDesc::setSourceInstanceID(const InstanceID& instanceId)
{
    _messageHeader.setSourceInstanceID(instanceId);
}

MessageID MessageDesc::getMessageType()
{
    return static_cast<MessageID>(_messageHeader.getMessageType());
}

void MessageDesc::setMessageType(MessageID messageType)
{
    // Very dangerous, so assert that this is the one legitimate
    // case we know of.  The two message types MUST have the same
    // underlying record type, else overwriting the messageType in
    // the header will likely segfault or worse.
    ASSERT_EXCEPTION(_messageHeader.getMessageType() == mtError && messageType == mtAuthError,
                     "Internal error, illegal setMessageType() call");
    _messageHeader.setMessageType(static_cast<uint16_t>(messageType));
}

std::shared_ptr<SharedBuffer> MessageDesc::getBinary() { return _binary; }

} // namespace scidb
