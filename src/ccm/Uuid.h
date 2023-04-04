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
#ifndef UUID_H_
#define UUID_H_

// header files from the ccm module
#include <boost/uuid/uuid.hpp>
// c++ standard libraries
#include <iosfwd>
#include <string>

namespace scidb { namespace ccm {

/**
 * @class Uuid
 *
 * @ingroup Ccm
 *
 * A universally unique identifier (UUID) generator class
 *
 * Uuid is a wrapper around a standards compliant universally unique identifier (UUID), a
 * 128 bit number (encoded entirely in big-endian format), with convenience member
 * functions for converting to/from memory pointers (for use in the CcmMsgHeader) and
 * strings. (This wrapper uses a boost::uuids::uuid which encodes the UUID entirely in
 * big-endian format, stored in a uint8_t[16] byte array).
 *
 * According to wikipedia (fetched 2019-03-16):
 *    UUIDs are standardized by the Open Software Foundation (OSF) as part of the
 *    Distributed Computing Environment (DCE).
 *
 *    UUIDs are documented as part of ISO/IEC 11578:1996 "Information technology – Open
 *    Systems Interconnection – Remote Procedure Call (RPC)" and more recently in ITU-T
 *    Rec. X.667 | ISO/IEC 9834-8:2005."
 *
 */
class Uuid
{
  public:
    /**
     * Generate a random 128-bit Uuid.
     */
    static Uuid create();

    /**
     * The default constructor which generates new nil UUID, that is
     * '00000000-0000-0000-0000-000000000000'.
     */
    Uuid();

    /**
     * Create a Uuid using the 16 byte big-endian UUID (128-bit number)
     * representation.
     *
     * @param mem pointer to 16 byte big-endian representation of a UUID.
     *
     * @note The calling function must create and maintains ownership to the
     *       memory.
     *
     * @see https://en.wikipedia.org/wiki/Universally_unique_identifier
     *
     */
    explicit Uuid(const uint8_t* mem);

    /**
     * Return a pointer to the 16 byte big-endian UUID representation.
     */
    uint8_t const* data() const { return _id.data; }

    /**
     * Explicit conversion to the 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' string
     * representation of the UUID.
     */
    operator std::string() const;
    friend std::ostream& operator<<(std::ostream& out, const Uuid& id);

    // Provide hash function to allow Uuids to be used as keys in unordered collections
    std::size_t hash() const;
    bool operator==(const Uuid& other) const { return _id == other._id; }

  private:
    boost::uuids::uuid _id;
};

}}  // namespace scidb::ccm

/**
 * Hash function needed for std::unordered_map/std::unordered_set
 */
namespace std {
template <>
struct hash<scidb::ccm::Uuid>
{
    std::size_t operator()(const scidb::ccm::Uuid& k) const { return k.hash(); }
};
}  // namespace std
#endif
