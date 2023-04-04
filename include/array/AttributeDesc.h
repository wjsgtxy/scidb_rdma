#ifndef ATTRIBUTEDESC_H_
#define ATTRIBUTEDESC_H_
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
#include <array/AttributeID.h>
#include <util/compression/CompressorType.h>
#include <query/Value.h>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/set.hpp>

#include <cstdint>
#include <set>
#include <string>

namespace scidb {
class Value;

constexpr char const * const DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME = "EmptyTag";

/**
 * Attribute descriptor
 */
class AttributeDesc
{
public:
    enum AttributeFlags
    {
        IS_NULLABLE        = 1,
        IS_EMPTY_INDICATOR = 2
    };

    /**
     * Construct empty attribute descriptor (for receiving metadata)
     */
    AttributeDesc();
    ~AttributeDesc() {}         // value class, non-virtual d'tor

    /**
     * Construct attribute descriptor with default space reservation
     *
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param defaultValue default attribute value (if NULL, then use predefined default
     *                     value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     */
    AttributeDesc(const std::string &name,
                  TypeId type,
                  int16_t flags,
                  CompressorType defaultCompressionMethod,
                  const std::set<std::string> &aliases = std::set<std::string>(),
                  Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0);


    /**
     * Construct full attribute descriptor with explicit space reservation
     *
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param reserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default
     *                     value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     *
     * @note DO NOT USE.  The "reserve" value was to be used to implement
     *       "delta chunks", but these were never fully implemented.  This
     *       code will likely go away.
     */
    AttributeDesc(const std::string &name,
                  TypeId type,
                  int16_t flags,
                  CompressorType defaultCompressionMethod,
                  const std::set<std::string> &aliases,
                  int16_t reserve, Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0)
        : AttributeDesc(name, type, flags, defaultCompressionMethod,
                        aliases, defaultValue, defaultValueExpr, varSize)
    {
        _reserve = reserve;
    }

    bool operator == (AttributeDesc const& other) const;
    bool operator != (AttributeDesc const& other) const
    {
        return !(*this == other);
    }

    /**
     * Get attribute identifier
     * @return attribute identifier
     */
    AttributeID getId() const;
    void setId(AttributeID id);

    /**
     * Get attribute name
     * @return attribute name
     */
    const std::string& getName() const;

    /**
     * Get attribute aliases
     * @return attribute aliases
     */
    const std::set<std::string>& getAliases() const;

    /**
     * Assign new alias to attribute
     * @alias alias name
     */
    void addAlias(const std::string& alias);

    /**
     * Check if such alias present in aliases
     * @alias alias name
     * @return true if such alias present
     */
    bool hasAlias(const std::string& alias) const;

    /**
     * Get chunk reserved space percent
     * @return reserved percent of chunk size
     */
    int16_t getReserve() const;

    /**
     * Get attribute type
     * @return attribute type
     */
    TypeId getType() const;

    /**
     * Check if this attribute can have NULL values
     */
    bool isNullable() const;

    /**
     * Check if this arttribute is empty cell indicator
     */
    bool isEmptyIndicator() const;

    /**
     * Get default compression method for this attribute: it is possible to specify explicitly different
     * compression methods for each chunk, but by default one returned by this method is used
     */
    CompressorType getDefaultCompressionMethod() const;

    /**
     * Get default attribute value
     */
    Value const& getDefaultValue() const;

    /**
     * Get attribute flags
     * @return attribute flags
     */
    int16_t getFlags() const;

    /**
     * Return type size or var size (in bytes) or 0 for truly variable size.
     */
    size_t getSize() const;

    /**
     * Get the optional variable size.v
     */
    size_t getVarSize() const;

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] output stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostream&, int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _id;
        ar & _name;
        ar & _aliases;
        ar & _type;
        ar & _flags;
        ar & _defaultCompressionMethod;
        ar & _reserve;
        ar & _defaultValue;
        ar & _varSize;
        ar & _defaultValueExpr;
    }

    /**
     * Return expression string which used for default value.
     *
     * @return expression string
     */
    const std::string& getDefaultValueExpr() const;

    // @return true if the target is a reserved attribute name.
    static bool isReservedName(const std::string& target);

private:
    AttributeID _id;
    std::string _name;
    std::set<std::string> _aliases;
    TypeId _type;
    int16_t _flags;
    CompressorType _defaultCompressionMethod;
    int16_t _reserve;
    Value _defaultValue;
    size_t _varSize;

    /**
     * Compiled and serialized expression for evaluating default value. Used only for storing/retrieving
     * to/from system catalog. Default value evaluated once after fetching metadata or during schema
     * construction in parser. Later only Value field passed between schemas.
     *
     * We not using Expression object because this class used on client.
     * actual value.
     */
    //TODO: May be good to have separate Metadata interface for client library
    std::string _defaultValueExpr;
};

std::ostream& operator<<(std::ostream&,const AttributeDesc&);

struct AOComparator {
    bool operator() (const AttributeDesc& lhs,
                     const AttributeDesc& rhs) const {
        return lhs.getId() < rhs.getId();
    }
};
using AttributeOrdering = std::set<AttributeDesc, AOComparator>;

}  // namespace scidb
#endif
