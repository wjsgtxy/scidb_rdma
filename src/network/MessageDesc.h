#ifndef MESSAGEDESC_H_
#define MESSAGEDESC_H_
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
#include <vector>

#include <boost/asio.hpp>

#include <query/InstanceID.h>
#include <query/QueryID.h>
#include <query/OperatorID.h>
#include <system/Utils.h>        // for SCIDB_ASSERT
#include <network/NetworkMessage.h> // for MessageID

namespace scidb {

class SharedBuffer;

// 消息头
class MessageHeader //dz：sizeof一共7个64位，56字节
{
private:
    uint16_t _netProtocolVersion;   ///< Version of network protocol
    uint16_t _messageType;          ///< Type of message 
    uint32_t _zeroPad;              ///< gcc would pad here, which causes valgrind unintiialized complaints
                                    ///  so we make explicit padding instead
    uint64_t _recordSize;           ///< The size of structured part of message 结构化数据
    uint64_t _binarySize;           ///< The size of unstructured part of message 非结构化数据 二进制数据
    InstanceID _sourceInstanceID;   ///< The source instance number dz：64bit
    QueryID _queryID;               ///< Query ID dz：包含2个64的id
    OperatorID _physicalOperatorID; ///< Physical Operator ID dz：只有一个uint64_t _id的成员变量

public:
    MessageHeader()
        : _netProtocolVersion(0)
        , _messageType(0)
        , _zeroPad(0)
        , _recordSize(0)
        , _binarySize(0)
        , _sourceInstanceID(0)  // perhaps should be INVALID_INSTANCE?
        , _queryID()
        , _physicalOperatorID()
    {}

    uint16_t getNetProtocolVersion() const { return _netProtocolVersion; }
    uint16_t getMessageType() const { return _messageType; }
    uint64_t getRecordSize() const { return _recordSize; }
    uint64_t getBinarySize() const { return _binarySize; }
    InstanceID getSourceInstanceID() const { return _sourceInstanceID; }
    const QueryID& getQueryID() const { return _queryID; }
    const OperatorID& getPhysicalOperatorID() const { return _physicalOperatorID; }

    void setNetProtocolVersion(uint16_t v) { _netProtocolVersion = v; }
    void setMessageType(uint16_t v) { _messageType = v; }
    void setZeroPad() { _zeroPad = 0; }
    void setRecordSize(uint64_t v) { _recordSize = v; }
    void setBinarySize(uint64_t v) { _binarySize = v; }
    void setSourceInstanceID(InstanceID v) { _sourceInstanceID = v; }
    void setQueryID(const QueryID& v) { _queryID = v; }
    void setPhysicalOperatorID() { _physicalOperatorID = OperatorID(); }
    void setPhysicalOperatorID(const OperatorID& v) { _physicalOperatorID = v; }

    /**
     * Convert the MessageHeader to a string optionally prefixing a
     * string prior to the converted MessageHeader
     *
     * @param strPrefix - the prefix string to use (optional)
     */
    std::string str(const std::string& strPrefix = "") const;
};

std::ostream& operator<<(std::ostream& out, const MessageHeader& messsageHeader);

/**
 * Message descriptor with all necessary parts
 */
class MessageDesc
{
public:
    /**
     * Message descriptor
     * @param messageType provides related google protocol buffer message
     * @param binary a pointer to buffer that will be used for reading or writing
     * binary data. Can be ommited when the message has no binary data
     */
    MessageDesc();
    MessageDesc(MessageID messageType);
    MessageDesc(const std::shared_ptr<SharedBuffer>& binary);
    MessageDesc(MessageID messageType, const std::shared_ptr<SharedBuffer>& binary);
    virtual ~MessageDesc() {}
    void writeConstBuffers(std::vector<boost::asio::const_buffer>& constBuffers);
    bool parseRecord(size_t bufferSize);
    void prepareBinaryBuffer();

    InstanceID getSourceInstanceID();

    /**
     * This method is not part of the public API
     */
    void setSourceInstanceID(const InstanceID& instanceId);

    template <class Derived>
    std::shared_ptr<Derived> getRecord()
    {
        SCIDB_ASSERT(std::dynamic_pointer_cast<Derived>(_record) != nullptr);
        return std::static_pointer_cast<Derived>(_record);
    }

    MessageID getMessageType();

    void setMessageType(MessageID messageType);

    std::shared_ptr<SharedBuffer> getBinary();

    virtual bool validate();

    size_t getMessageSize() const;

    QueryID getQueryID() const;

    void setQueryID(QueryID queryID);

    /// Set an id on the operator sufficiently
    /// unique that it can be looked up from a an
    /// inter-instance message by using it as an index
    /// into a vector maintained on Query.
    /// Used by the query planner and the
    /// optimizer, not for general use
    /// @param operatorID - the ID
    void setPhysicalOperatorID(OperatorID operatorID);

    /// obtain the ID set by setPhysicalOperatorID()
    /// @return the ID
    OperatorID getPhysicalOperatorID() const;

    void initRecord(MessageID messageType);

    /**
     * Convert the MessageDesc to a string optionally prefixing a
     * string prior to the converted MessageDesc.
     *
     * @param strPrefix - the prefix string to use (optional)
     */
    std::string str(const std::string& strPrefix = "") const;

    /**
     * primarily for serialization purposes
     */
    MessageHeader& getMessageHeader();
    const MessageHeader& getMessageHeader() const;

    /**
     * primarily for serialization purposes
     */
    const MessagePtr& getRecord() const;

protected:
    virtual MessagePtr createRecord(MessageID messageType);

private:
    void init(MessageID messageType);
    MessageHeader _messageHeader;          /** < Message header */
    MessagePtr _record;                    /** < Structured part of message */ //dz：Google protobuf::Message ptr    
    std::shared_ptr<SharedBuffer> _binary; /** < Buffer for binary data to be transfered */ // 有不同的buffer类型，realloc会不一样
    boost::asio::streambuf
        _recordStream; /** < Buffer for serializing Google Protocol Buffers objects */ // dz：通过流转换的结构化数据，转换后的数据放到_record中

    static MessagePtr createRecordByType(MessageID messageType);

    friend class BaseConnection;
    friend class Connection;
    friend class RdmaConnection; // dz add
    friend class RdmaCommManager;
};

std::ostream& operator<<(std::ostream& out, MessageDesc& messsageDesc);

typedef std::shared_ptr<MessageDesc> MessageDescPtr;
} // namespace scidb

#endif
