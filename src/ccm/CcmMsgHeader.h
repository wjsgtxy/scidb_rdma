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

#ifndef CCM_MSG_HEADER_H_
#define CCM_MSG_HEADER_H_

// c++ standard libraries
#include <cstdint>
#include <iosfwd>

namespace scidb { namespace ccm {

enum class CcmMsgType;

/**
 *
 * @ingroup Ccm
 */
class CcmMsgHeader
{
    static const uint8_t VERSION = 1;  // TODO This belongs elsewhere

    friend std::ostream& operator<<(std::ostream& os, const CcmMsgHeader& cmh);

  public:
    bool isValid() const;

    uint8_t getVersion() const;
    CcmMsgType getType() const;
    const uint8_t* getSession() const;
    uint32_t getRecordSize() const;

    void setVersion(uint8_t ver);
    void setType(CcmMsgType type);
    void setSession(const unsigned char* uuid);
    void setRecordSize(uint32_t nativeSize);

  private:
    // These bytes are stored just like they came off the wire (network byte
    // order) and remain as such so they can go back onto the wire when we need
    // them..  Use accessor/mutators functions to get the recordSize,
    // session(big-endian UUID), and msgType.
    uint8_t _version{VERSION};
    uint8_t _msgType{0};
    uint8_t _reserve[2]{0, 0};
    uint8_t _session[16]{
        0,
    };
    uint32_t _recordSize{0};
    // We need the length of the protocol buffer (record size) because the fields in the
    // proto Message are optional which affects the overall size of each message sent.

} __attribute__((packed));
}}  // namespace scidb::ccm
#endif
