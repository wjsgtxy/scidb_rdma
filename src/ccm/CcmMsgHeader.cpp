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
#include "CcmMsgHeader.h"
// header files from the ccm module
#include "CcmMsgType.h"
#include "Uuid.h"
// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
// c++ standard libraries
#include <cstring>
#include <iostream>
// c and system headers
#include <arpa/inet.h>  // for htonl, htons, etc.

namespace {
constexpr int bytesIn128BitUuId = 16;
}  // namespace

namespace scidb { namespace ccm {

bool CcmMsgHeader::isValid() const
{
    // TODO (Phase 2): This constant for Version (1) eventually needs to be available from a single
    //       source of truth by many different locations:
    //       1. Ccm.proto file
    //       2. the SciDBMessage.py file,
    //       3. HERE
    //       4. Other programming languages extensions (such as R)
    //       5. .... others as well.
    // see also VERSION in CcmMsgHeader.h
    bool isGoodCcmVersion = (_version == VERSION);
    bool isGoodCcmMsgType = (_msgType < static_cast<uint8_t>(CcmMsgType::MSGMAX));
    bool isGoodCcmReserve = (*(reinterpret_cast<const int16_t*>(_reserve)) == 0);
    return (isGoodCcmVersion && isGoodCcmMsgType && isGoodCcmReserve);
}

uint8_t CcmMsgHeader::getVersion() const
{
    SCIDB_ASSERT(isValid());
    return _version;
}

CcmMsgType CcmMsgHeader::getType() const
{
    SCIDB_ASSERT(isValid());
    return static_cast<CcmMsgType>(_msgType);
}

const uint8_t* CcmMsgHeader::getSession() const
{
    SCIDB_ASSERT(isValid());
    return _session;
}

uint32_t CcmMsgHeader::getRecordSize() const
{
    SCIDB_ASSERT(isValid());
    // Do not use global namespace resolver (::ntohl) since optimized builds use
    // a macro for ntohl which causes a problem with older versions of <arpa/inet.h>.
    return ntohl(_recordSize);
}

void CcmMsgHeader::setVersion(uint8_t ver)
{
    SCIDB_ASSERT(isValid());
    _version = ver;
}

void CcmMsgHeader::setType(CcmMsgType type)
{
    _msgType = static_cast<uint8_t>(type);
    SCIDB_ASSERT(isValid());
}

void CcmMsgHeader::setSession(const unsigned char* uuid)
{
    std::memcpy(&_session, uuid, bytesIn128BitUuId);
    SCIDB_ASSERT(isValid());
}

void CcmMsgHeader::setRecordSize(uint32_t nativeSize)
{
    // Do not use global namespace resolver (::htonl) since optimized builds use
    // a macro for htonl which causes a problem with older versions of <arpa/inet.h>.
    _recordSize = htonl(nativeSize);
    SCIDB_ASSERT(isValid());
}

std::ostream& operator<<(std::ostream& os, const CcmMsgHeader& cmh)
{
    Uuid session(cmh.getSession());
    os << "{"
       << " \"CcmMsgHeader\": "
       << "{  "
       << "\"version\": " << static_cast<uint32_t>(cmh.getVersion()) << ", \"type:\": \""
       << cmh.getType() << "\""
       << ", \"session\": \"" << session << "\""
       << ", \"recordSize\": " << cmh.getRecordSize() << "}"
       << "}";
    return os;
}
}}  // namespace scidb::ccm
