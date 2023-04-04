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
 * @file InstanceID.h
 *
 *
 * @brief Instance ID
 */

#ifndef INSTANCE_ID_H_
#define INSTANCE_ID_H_

#include <cassert>
#include <limits>
#include <string>
#include <sstream>

namespace scidb
{
/**
 * Instance identifier
 */
typedef uint64_t InstanceID;

// XXX TODO: change _MASK -> _VALUE
const InstanceID  CLIENT_INSTANCE  = ~0;  // Connection with this instance id is client connection
const InstanceID  INVALID_INSTANCE = ~0;  // Invalid instanceID
const InstanceID ALL_INSTANCE_MASK = -1;
const InstanceID COORDINATOR_INSTANCE_MASK = -2;

///@return true if iId can be a valid instance ID, i.e. it is in the expected value range
inline bool isValidPhysicalInstance(InstanceID iId)
{
    return (iId != CLIENT_INSTANCE &&
            iId != INVALID_INSTANCE &&
            iId != ALL_INSTANCE_MASK &&
            iId != COORDINATOR_INSTANCE_MASK &&
            /* 0x7FFFFFFFFFFFFFFF = max value of PG bigint */
            iId <= std::numeric_limits<int64_t>::max()
            );
}

inline size_t getServerId(InstanceID iId)
{
    assert(isValidPhysicalInstance(iId));
    return iId >> 32;
}

inline size_t getServerInstanceId(InstanceID iId)
{
    assert(isValidPhysicalInstance(iId));
    return iId & 0x0FFFFFFFF;
}

// Helper for clearer InstanceID logging with operator<< .
//
// Beware: the physical instance corresponding to a (server_id,
// server_instance_id) pair may *not* be the same as the packed
// ((server_id << 32) | server_instance_id) value.  We should probably
// log them differently, but for now we don't.
// @see SystemCatalog::addInstance().
//
struct Iid {
    struct Terse {};
    explicit Iid(InstanceID i) : iid(i) {}
    Iid(InstanceID i, Terse const&) : iid(i), compat(false) {}
    Iid(uint32_t sid, uint32_t siid)
    {
        iid = sid;
        iid <<= 32;
        iid |= siid;
    }
    std::string str() const;
    InstanceID iid;
    bool compat { true };
};

inline std::ostream& operator<< (std::ostream& os, Iid const& id)
{
    switch (id.iid) {
    case CLIENT_INSTANCE:
        os << "CLIENT(-1)";
        break;
    case COORDINATOR_INSTANCE_MASK:
        os << "COORDINATOR(-2)";
        break;
    default:
        os << 's' << getServerId(id.iid)
           << "-i" << getServerInstanceId(id.iid);
        if (id.compat) {
            os << " (" << id.iid << ')'; // XXX Temporary until Iid used everywhere.
        }
        break;
    }
    return os;
}

inline std::string Iid::str() const
{
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

} // scidb namespace

#endif /* INSTANCE_ID_H_ */
