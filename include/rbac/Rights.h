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
 * @file Rights.h
 * @brief A right identifies an object and an associated set of permissions.
 */

#ifndef RIGHTS_H
#define RIGHTS_H

#include <cstdint>
#include <map>
#include <ostream>
#include <string>

namespace scidb { namespace rbac {

/// Kinds of "things" we control access to.
enum EntityType {
    ET_UNDEFINED,
    ET_NAMESPACE,
    ET_DB,
    ET_MAX                      // MUST BE LAST
};

/// Translate string to corresponding EntityType, or ET_UNDEFINED.
EntityType strToEntityType(std::string const& s);

/// Translate EntityType to display string.
/// @param t input entity type
/// @param abbrev if true use shorter name, e.g. "db" not "database"
std::string entityTypeToStr(EntityType t, bool abbrev = false);

/// Identifies an individual "thing" we control access to.
struct EntityTag {
    EntityTag() : type(ET_UNDEFINED) {}
    EntityTag(EntityType t, std::string const& n) : type(t), name(n) {}

    EntityType type;
    std::string name;
};

inline bool operator< (EntityTag const& a, EntityTag const& b)
{
    if (a.type < b.type) {
        return true;
    } else if (a.type > b.type) {
        return false;
    } else {
        return a.name < b.name;
    }
}

inline std::ostream& operator<< (std::ostream& os, EntityTag const& otag)
{
    os << '(';
    if (otag.type < ET_MAX) {
        os << entityTypeToStr(otag.type, /*abbrev:*/ true);
    } else {
        os << int(otag.type);
    }
    os << ':' << otag.name << ')';
    return os;
}

/// Permission bitmask definitions.
enum PermMasks {
#   define X(_name, _bit, _ch, _desc) P_ ## _name = (1ULL << _bit),
#   include "Permissions.inc"
#   undef X
};

/// Value class for storing permission bits.
class Permissions
{
public:
    typedef uint64_t value_type;

    Permissions() : _bits(0ULL) {}
    explicit Permissions(uint64_t b) : _bits(b) {}

    /// Construct from string.
    /// @throw USER_EXCEPTION if string is bogus
    explicit Permissions(std::string const& s);

    /// @brief Interpret string as permissions mask.
    /// @throw USER_EXCEPTION if string is bogus
    Permissions& operator= (std::string const& s);

    /// @brief Interpret string as permissions mask.
    /// @return true iff string is valid
    bool assign(std::string const& s);

    // Forgive the Java style but it makes these kinds of operators
    // easy to read without wasting much vertical space.
    Permissions& operator=  (Permissions const& rhs) {
        if (&rhs != this) {
            _bits = rhs._bits;
        }
        return *this;
    }
    Permissions& operator|= (Permissions const& rhs) {
        _bits |= rhs._bits;
        return *this;
    }
    Permissions& operator+= (Permissions const& rhs) {
        return *this |= rhs;
    }
    Permissions& operator&= (Permissions const& rhs) {
        _bits &= rhs._bits;
        return *this;
    }
    Permissions& operator-= (Permissions const& rhs) {
        _bits &= ~rhs._bits;
        return *this;
    }

    operator bool() const { return _bits != 0ULL; }

    std::string str() const;

    value_type bits() const { return _bits; }

private:
    value_type  _bits;
};

inline std::ostream& operator<< (std::ostream& os, Permissions const& perms)
{
    os << perms.str();
    return os;
}

inline bool operator== (Permissions const &a, Permissions const &b)
{
    return a.bits() == b.bits();
}

inline bool operator!= (Permissions const &a, Permissions const &b)
{
    return a.bits() != b.bits();
}

inline Permissions operator& (Permissions const &a, Permissions const &b)
{
    Permissions tmp(a);
    tmp &= b;
    return tmp;
}

inline Permissions operator| (Permissions const &a, Permissions const &b)
{
    Permissions tmp(a);
    tmp |= b;
    return tmp;
}

inline Permissions operator~ (Permissions const& rhs)
{
    return Permissions(~rhs.bits());
}

/// A right is a set of permissions on a particular object.
typedef std::pair<EntityTag, Permissions> Right;

inline std::ostream& operator<< (std::ostream& os, Right const& p)
{
    os << '[' << p.first << p.second << ']';
    return os;
}

/// Collection of rights required by a query.
struct RightsMap : public std::map<EntityTag, Permissions>
{
    /// @brief True iff this map contains all rights in 'subset'.
    bool contains(RightsMap const& subset) const;

    /**
     * @brief Collect access rights for the named, typed object.
     * @param objType kind of object to be accessed
     * @param objName name of the object to be accessed
     * @param rightsMask kind(s) of access to be granted
     * @note Idempotent, multiple calls from prepareQuery() are OK.
     */
    void upsert(EntityType objType,
                std::string const& objName,
                Permissions::value_type rightsMask);

    /** Another way to add rights. */
    void upsert(EntityType objType,
                std::string const& objName,
                Permissions const& perms);
};

inline std::ostream& operator<< (std::ostream& os, RightsMap const& rMap)
{
    os << '{';
    for (auto const& right : rMap) {
        os << right << ',';
    }
    os << '}';
    return os;
}

} } // namespaces

#endif /* ! RIGHTS_H */
