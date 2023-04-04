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
 * @file Address.h
 *
 * @brief Classes which represent temporary and persistent chunk Addresses
 */

#ifndef ADDRESS_H_
#define ADDRESS_H_

#include <array/ArrayID.h>
#include <array/AttributeID.h>
#include <array/Coordinate.h>
#include <sstream>

namespace scidb {

/**
 * An Address is used to specify the location of a chunk inside an array.
 */
struct Address
{
    /*
     * Attribute identifier
     */
    AttributeID attId;
    /**
     * Chunk coordinates
     */
    Coordinates coords;

    /**
     * Default constructor
     */
    Address()
        : attId(INVALID_ATTRIBUTE_ID)
    {}

    /**
     * Constructor
     * @param attId attribute identifier
     * @param coords element coordinates
     */
    Address(AttributeID attId, Coordinates const& coords)
        : attId(attId)
        , coords(coords)
    {}

    /**
     * Copy constructor
     * @param addr the object to copy from
     */
    Address(const Address& addr)
    {
        this->attId = addr.attId;
        this->coords = addr.coords;
    }

    /**
     * Partial comparison function, used to implement std::map
     * @param other another aorgument of comparison
     * @return true if "this" preceeds "other" in partial order
     */
    bool operator<(const Address& other) const
    {
        if (attId != other.attId) {
            return attId < other.attId;
        }
        if (coords.size() != other.coords.size()) {
            return coords.size() < other.coords.size();
        }
        for (size_t i = 0, n = coords.size(); i < n; i++) {
            if (coords[i] != other.coords[i]) {
                return coords[i] < other.coords[i];
            }
        }
        return false;
    }

    /**
     * Equality comparison
     * @param other another aorgument of comparison
     * @return true if "this" equals to "other"
     */
    bool operator==(const Address& other) const
    {
        if (attId != other.attId) {
            return false;
        }
        assert(coords.size() == other.coords.size());
        for (size_t i = 0, n = coords.size(); i < n; i++) {
            if (coords[i] != other.coords[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Inequality comparison
     * @param other another aorgument of comparison
     * @return true if "this" not equals to "other"
     */
    bool operator!=(const Address& other) const { return !(*this == other); }

    /**
     * Compute a hash of coordiantes
     * @return a 64-bit hash of the chunk coordinates.
     */
    uint64_t hash() const
    {
        uint64_t h = 0;
        for (int i = safe_static_cast<int>(coords.size()); --i >= 0;) {
            h ^= coords[i];
        }
        return h;
    }

    /**
     * @return a string representation of the address
     */
    std::string toString() const
    {
        std::stringstream ss;
        ss << "{ attid = " << attId << ", coords = " << coords << "}";
        return ss.str();
    }
};

/**
 * A PersistantAddress is used to specify the location of a chunk inside
 * a persistent array.
 */
class PersistentAddress : public Address
{
public:
    /**
     * Array id for the Versioned Array ID wherein this chunk
     * first appeared.
     */
    ArrayID arrVerId;

    /**
     * Default constructor
     */
    PersistentAddress()
        : Address(0, Coordinates())
        , arrVerId(0)
    {}

    /**
     * Constructor
     * @param arrVerId array identifier
     * @param attId attribute identifier
     * @param coords element coordinates
     */
    PersistentAddress(ArrayID arrVerId, AttributeID attId, Coordinates const& coords)
        : Address(attId, coords)
        , arrVerId(arrVerId)
    {}

    /**
     * Copy constructor
     * @param addr the object to copy from
     */
    PersistentAddress(PersistentAddress const& addr)
        : Address(addr.attId, addr.coords)
        , arrVerId(addr.arrVerId)
    {}

    /**
     * Comparison function, used to implement std::map
     * @param other another argument of comparison
     * @return true if "this" preceeds "other" in partial order
     */
    inline bool operator<(PersistentAddress const& other) const
    {
        if (attId != other.attId) {
            return attId < other.attId;
        }
        if (coords.size() != other.coords.size()) {
            return coords.size() < other.coords.size();
        }
        for (size_t i = 0, n = coords.size(); i < n; i++) {
            if (coords[i] != other.coords[i]) {
                return coords[i] < other.coords[i];
            }
        }
        if (arrVerId != other.arrVerId) {
            /* note: reverse ordering to keep most-recent versions at
               the front of the map
            */
            return arrVerId > other.arrVerId;
        }
        return false;
    }

    /**
     * Equality comparison
     * @param other another argument of comparison
     * @return true if "this" equals to "other"
     */
    inline bool operator==(PersistentAddress const& other) const
    {
        if (arrVerId != other.arrVerId) {
            return false;
        }

        return Address::operator==(static_cast<Address const&>(other));
    }

    /**
     * Inequality comparison
     * @param other another argument of comparison
     * @return true if "this" not equals to "other"
     */
    inline bool operator!=(PersistentAddress const& other) const
    {
        return !(*this == other);
    }

    /**
     * Check for same base Addr
     * @param other another argument of comparison
     * @return true if "this" is equal to "other" notwithstanding
     *         different arrId (version)
     */
    inline bool sameBaseAddr(PersistentAddress const& other) const
    {
        if (attId != other.attId) {
            return false;
        }
        return (coords == other.coords);
    }

    /**
     * @return a string representation of the address
     */
    std::string toString() const
    {
        std::stringstream ss;
        ss << "{ attid = " << attId
           << ", coords = " << coords
           << ", vaid = " << arrVerId
           << "}";
        return ss.str();
    }
};

}  // namespace scidb

#endif  // ADDRESS_META_H_
