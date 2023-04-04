/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file Rights.cpp
 * @brief Support for Rights, Permissions, and RightsMaps.
 */

#include <rbac/Rights.h>

#include <system/UserException.h>
#include <system/Utils.h>

#include <log4cxx/logger.h>

#include <sys/param.h>
#include <vector>

using namespace std;

namespace {
    log4cxx::LoggerPtr logger = log4cxx::Logger::getLogger("scidb.rbac");
}

namespace scidb { namespace rbac {

// One day we may have enough entity types to warrant a .inc file, but
// for now this suffices.  This is the ONE TRUE PLACE for translating
// strings to entity types.
EntityType strToEntityType(string const& s)
{
    if (s == "db" || s == "database") {
        return ET_DB;
    } else if (s == "ns" || s == "namespace") {
        return ET_NAMESPACE;
    } else {
        return ET_UNDEFINED;
    }
}


string entityTypeToStr(EntityType t, bool abbrev)
{
    switch (t) {
    case ET_NAMESPACE:
        return abbrev ? "ns" : "namespace";
    case ET_DB:
        return abbrev ? "db" : "database";
    default:
        return abbrev ? "undef" : "undefined";
    }
}

/**
 * @brief Support mapping Permissions code letters to bit masks and back again.
 *
 * @note Since this is a class, we know that the static instance in
 * getPermToBitMapper() below will be initialized before main() starts.
 */
class PermToBitMapper
{
    Permissions::value_type _ch2mask[128];
    vector<char> _bit2ch;       // don't slur the name...
public:
    PermToBitMapper()
    {
        static_assert(P_BITS_MAX <= (1UL << 63),
                      "Too many permission bits is a _bit2ch of a problem");

        ::memset(&_ch2mask[0], 0, sizeof(_ch2mask));
        _bit2ch.resize(sizeof(Permissions::value_type) * NBBY);
#       define X(_name, _bit, _ch, _desc)               \
        do {                                            \
            SCIDB_ASSERT(_ch < sizeof(_ch2mask));       \
            SCIDB_ASSERT(_bit < _bit2ch.size());        \
            _ch2mask[_ch] = ( 1 << _bit );              \
            _bit2ch[_bit] = _ch ;                       \
        } while (0);
#       include <rbac/Permissions.inc>
#       undef X
    }

    Permissions::value_type charToMask(char ch) const
    {
        size_t index = static_cast<size_t>(ch);
        SCIDB_ASSERT(index < sizeof(_ch2mask));
        return _ch2mask[index];
    }

    char bitToChar(size_t bit) const
    {
        SCIDB_ASSERT(bit < _bit2ch.size());
        return _bit2ch[bit];
    }
};


static PermToBitMapper& getPermToBitMapper()
{
    static PermToBitMapper p2bm;
    return p2bm;
}


static bool fromString(string const& s, Permissions::value_type& out)
{
    Permissions::value_type bits;
    out = 0;
    for (char const& ch : s) {
        bits = getPermToBitMapper().charToMask(ch);
        if (!bits || bits >= P_BITS_MAX) {
            // ch didn't map to a bitmask, string is bogus
            return false;
        }
        out |= bits;
    }
    return true;
}


Permissions::Permissions(string const& s)
{
    if (!fromString(s, _bits)) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_PERMISSIONS)
            << s;
    }
}


Permissions& Permissions::operator= (string const& s)
{
    if (!fromString(s, _bits)) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_PERMISSIONS)
            << s;
    }
    return *this;
}


bool Permissions::assign(string const& s)
{
    return fromString(s, _bits);
}


string Permissions::str() const
{
    string result;
    value_type mask = 1;
    for (int bit = 0; mask < P_BITS_MAX; mask <<= 1, ++bit) {
        if (_bits & mask) {
            result += getPermToBitMapper().bitToChar(bit);
        }
    }
    return result;
}


bool RightsMap::contains(RightsMap const& subset) const
{
    for (Right const& want : subset) {
        auto got = find(want.first);
        if (got == end()) {
            LOG4CXX_TRACE(logger,
                          "No rights for " << want.first << ", access denied");
            return false;
        }
        if ((got->second & want.second) != want.second) {
            LOG4CXX_TRACE(logger, "Access denied to " << want.first
                          << ", got " << got->second
                          << " but need " << want.second);
            return false;
        }
    }
    return true;
}


void RightsMap::upsert(EntityType type,
                       string const& name,
                       Permissions::value_type rightsMask)
{
    SCIDB_ASSERT(rightsMask && rightsMask < P_BITS_MAX);
    upsert(type, name, Permissions(rightsMask));
}


void RightsMap::upsert(EntityType type,
                       string const& name,
                       Permissions const& perms)
{
    // The type must be in-range, and we're only naming namespaces (so far).
    SCIDB_ASSERT(type < ET_MAX);
    SCIDB_ASSERT((!name.empty() && type == ET_NAMESPACE) ||
                 (name.empty()  && type != ET_NAMESPACE));

    EntityTag tag(type, name);
    auto r = insert(make_pair(tag, perms));
    auto& pos = r.first;
    if (!r.second) {
        // Entry was already there, add the newly requested rights.
        LOG4CXX_TRACE(logger, "Add " << perms << " rights to " << pos->second
                      << " for " << pos->first);
        pos->second += perms;
    } else {
        LOG4CXX_TRACE(logger, "Need " << pos->second << " rights for "
                      << pos->first);
    }
}

} } // namespaces
