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

#include <array/AttributeDesc.h>

#include <array/ArrayName.h>
#include <query/TypeSystem.h>
#include <rbac/Rbac.h>
#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif
#include <system/Utils.h>
#include <util/BitManip.h>
#include <util/compression/Compressor.h>

#include <boost/algorithm/string.hpp>
#include <log4cxx/logger.h>

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.metadata"));
}

using namespace std;

namespace scidb {

AttributeDesc::AttributeDesc() :
    _id(INVALID_ATTRIBUTE_ID),
    _type( TypeId( TID_VOID)),
    _flags(0),
    _defaultCompressionMethod(CompressorType::NONE),
    _reserve(
#ifndef SCIDB_CLIENT
        static_cast<int16_t>(Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE))
#else
        0
#endif
     ),
    _varSize(0)
{}

AttributeDesc::AttributeDesc(const std::string &name,
                             TypeId type,
                             int16_t flags,
                             CompressorType defaultCompressionMethod,
                             const std::set<std::string> &aliases,
                             Value const* defaultValue,
                             const string &defaultValueExpr,
                             size_t varSize) :
    _id(INVALID_ATTRIBUTE_ID),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(static_cast<int16_t>(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0))),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(
#ifndef SCIDB_CLIENT
        safe_static_cast<int16_t>(Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE))
#else
        0
#endif
    ),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            _defaultValue = TypeLibrary::getDefaultValue(type);
        }
    }
    if(name == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME &&  (_flags & AttributeDesc::IS_EMPTY_INDICATOR) == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute name misuse";
    }
    if(name != DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME && (_flags & AttributeDesc::IS_EMPTY_INDICATOR))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute not named properly";
    }
}

bool AttributeDesc::operator==(AttributeDesc const& other) const
{
    return
        _id == other._id &&
        _name == other._name &&
        _aliases == other._aliases &&
        _type == other._type &&
        _flags == other._flags &&
        _defaultCompressionMethod == other._defaultCompressionMethod &&
        _reserve == other._reserve &&
        _defaultValue == other._defaultValue &&
        _varSize == other._varSize &&
        _defaultValueExpr == other._defaultValueExpr;
}

AttributeID AttributeDesc::getId() const
{
    return _id;
}

void AttributeDesc::setId(AttributeID id)
{
    SCIDB_ASSERT(id != INVALID_ATTRIBUTE_ID);
    _id = id;
}

const std::string& AttributeDesc::getName() const
{
    return _name;
}

const std::set<std::string>& AttributeDesc::getAliases() const
{
    return _aliases;
}

void AttributeDesc::addAlias(const string& alias)
{
    string trimmedAlias = alias;
    boost::algorithm::trim(trimmedAlias);
    _aliases.insert(trimmedAlias);
}

bool AttributeDesc::hasAlias(const std::string& alias) const
{
    if (alias.empty())
        return true;
    else {
        std::string qualifiedAlias;
        if(!isQualifiedArrayName(alias))
        {
            qualifiedAlias = makeQualifiedArrayName(rbac::PUBLIC_NS_NAME, alias);
        }

        return
            (_aliases.find(alias)          != _aliases.end()) ||
            (_aliases.find(qualifiedAlias) != _aliases.end());
    }
}

TypeId AttributeDesc::getType() const
{
    return _type;
}

int16_t AttributeDesc::getFlags() const
{
    return _flags;
}

bool AttributeDesc::isNullable() const
{
    return (_flags & IS_NULLABLE) != 0;
}

bool AttributeDesc::isEmptyIndicator() const
{
    return (_flags & IS_EMPTY_INDICATOR) != 0;
}

CompressorType AttributeDesc::getDefaultCompressionMethod() const
{
    return _defaultCompressionMethod;
}

Value const& AttributeDesc::getDefaultValue() const
{
    return _defaultValue;
}

int16_t AttributeDesc::getReserve() const
{
    return _reserve;
}

size_t AttributeDesc::getSize() const
{
    Type const& type = TypeLibrary::getType(_type);
    return type.byteSize() > 0 ? type.byteSize() : getVarSize();
}

size_t AttributeDesc::getVarSize() const
{
    return _varSize;
}

const std::string& AttributeDesc::getDefaultValueExpr() const
{
    return _defaultValueExpr;
}

bool AttributeDesc::isReservedName(const std::string& target)
{
    SCIDB_ASSERT(!target.empty());
    return target[0] == '$';
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] output stream to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void AttributeDesc::toString(std::ostream &os, int indent) const
{
    if (indent > 0)
    {
        os << std::string(indent,' ');
    }

    os << "[attDesc] id " << _id
       << " name " << _name
       << " aliases {";

    for (const std::string& alias : _aliases)
    {
        os << alias << '.' << _name << ", ";
    }

    os << "} type " << _type
       << " flags " << _flags
       << " compression " << static_cast<uint32_t>(_defaultCompressionMethod)
       << " reserve " << _reserve
       << " default " << _defaultValue.toString(_type);
}

std::ostream& operator<<(std::ostream& stream, const AttributeDesc& att)
{
    //Don't print NULL because nullable is the default behavior.
    stream << att.getName() << ':' << att.getType()
           << (isAnyOn<int>(att.getFlags(), AttributeDesc::IS_NULLABLE) ? "" : " NOT NULL");
    try
    {
        if (!isAnyOn<int>(att.getFlags(), AttributeDesc::IS_NULLABLE) &&  // not nullable, and
            !isDefaultFor(att.getDefaultValue(),att.getType()))           // not equal to the type's default value
        {
            stream << " DEFAULT " << att.getDefaultValue().toString(att.getType());
        }
    }
    catch (const SystemException &e)
    {
        if (e.getLongErrorCode() != SCIDB_LE_TYPE_NOT_REGISTERED)
        {
            e.raise();
        }

        stream << " DEFAULT UNKNOWN";
    }
    if (att.getDefaultCompressionMethod() != CompressorType::NONE)
    {
        stream << " COMPRESSION '"
               << CompressorFactory::getInstance().getCompressor(att.getDefaultCompressionMethod())->getName()
               << "'";
    }
    return stream;
}


}  // namespace scidb
