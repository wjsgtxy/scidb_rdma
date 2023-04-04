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


#include <array/Attributes.h>

#include <query/TypeSystem.h>   // for TID_INDICATOR
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/PointerRange.h>

namespace scidb {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wtype-limits"

// DJG TODO custom iterator to treat the collection of attributes as a ring
// to handle challenging cases like PrettySortArray.

size_t Attributes::_emptyIndicatorPosition() const
{
    return _attributes.size() - 1;
}

void Attributes::_verifyAttributesContainerIntegrity() const
{
    if (isDebug()) {
        AttributeID lastId = STARTING_ATTRIBUTE_ID-1;
        for (const auto& attr : _attributes) {
            _affirmInRange(attr.getId());
            SCIDB_ASSERT(++lastId == attr.getId());
        }
    }
}

void Attributes::_computeNextAttributeID()
{
    // There's no need treat the _nextAttributeID to serialization (and hence
    // no need to change the opaque storage header or version in the
    // TemplateParser code) when we can trivially determine it after we have
    // deserialized this structure.
    _verifyAttributesContainerIntegrity();
    _nextAttributeID = _attributes.empty() ? STARTING_ATTRIBUTE_ID : _lastAttributeID() + 1;
}

void Attributes::_affirmInRange(AttributeID attrID) const
{
    SCIDB_ASSERT(attrID >= STARTING_ATTRIBUTE_ID);

    // I would like to have this kind of enforcement in place but there
    // is code (like index_lookup) that depends on this returning an
    // iterator at _attributes::end() when the attribute ID is not found.
    //SCIDB_ASSERT(_attributes.empty() ? true : attrID <= _lastAttributeID());

    // PhysicalJoin.cpp:97 depends on this not being true, but we could
    // probably rewrite that code to ask the container if it has the attr
    // before trying to get it.
    //SCIDB_ASSERT(attrID != INVALID_ATTRIBUTE_ID);
}

AttributeID Attributes::_lastAttributeID() const
{
    SCIDB_ASSERT(!_attributes.empty());
    const auto lastAttrIter = _attributes.end()-1;
    const auto& lastAttr = *lastAttrIter;
    const auto lastAttrID = lastAttr.getId();
    return lastAttrID;
}

// DJG We don't reserve space eagerly now, but support callsites that request
// that behavior for now until they're updated.
Attributes::Attributes(size_t)
{
}

Attributes::Attributes()
{
}

Attributes::Attributes(const Attributes& rhs)
    : _nextAttributeID(rhs._nextAttributeID)
    , _emptyTagCompression(rhs._emptyTagCompression)
    , _attributes(rhs._attributes)
{
    _verifyAttributesContainerIntegrity();
}

Attributes::Attributes(Attributes&& rhs)
    : _nextAttributeID(rhs._nextAttributeID)
    , _emptyTagCompression(rhs._emptyTagCompression)
    , _attributes(rhs._attributes)
{
    _verifyAttributesContainerIntegrity();
}

Attributes& Attributes::operator= (const Attributes& rhs)
{
    _emptyTagCompression = rhs._emptyTagCompression;
    _attributes = rhs._attributes;
    _verifyAttributesContainerIntegrity();
    _nextAttributeID = rhs._nextAttributeID;
    return *this;
}

Attributes& Attributes::operator= (Attributes&& rhs)
{
    _emptyTagCompression = rhs._emptyTagCompression;
    _attributes = rhs._attributes;
    _verifyAttributesContainerIntegrity();
    _nextAttributeID = rhs._nextAttributeID;
    return *this;
}

Attributes::~Attributes()
{
}

bool Attributes::hasEmptyIndicator() const
{
    return !_attributes.empty() && _attributes[_emptyIndicatorPosition()].isEmptyIndicator();
}

AttributeDesc const* Attributes::getEmptyBitmapAttribute() const
{
    // DJG TODO assert that the empty bitmap attribute exists.  Force callers to check
    // for the presence of the empty bitmap by calling hasEmptyIndicator first, rather
    // than relying on this method returning nullptr.
    return ((!_attributes.empty() && _attributes[_emptyIndicatorPosition()].isEmptyIndicator()) ?
            &_attributes[_emptyIndicatorPosition()] : nullptr);
}

Attributes& Attributes::addEmptyTagAttribute()
{
    if (!hasEmptyIndicator()) {
        _verifyAttributesContainerIntegrity();
        AttributeDesc etag(DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME, TID_INDICATOR,
                           AttributeDesc::IS_EMPTY_INDICATOR, _emptyTagCompression);
        etag.setId(_nextAttributeID);
        ++_nextAttributeID;
        _attributes.insert(_attributes.end(), etag);
        _verifyAttributesContainerIntegrity();
    }
    return *this;
}

void Attributes::deleteEmptyIndicator()
{
    if (hasEmptyIndicator()) {
        _attributes.erase(_attributes.begin() + _emptyIndicatorPosition());
        _computeNextAttributeID();
    }
}

void Attributes::replace(AttributeID aid, const AttributeDesc& ad)
{
    _affirmInRange(aid);
    _affirmInRange(ad.getId());
    const_cast<AttributeDesc&>(ad).setId(aid);
    _attributes[aid] = ad;
}

Attributes::const_iterator Attributes::begin() const noexcept
{
    return _attributes.begin();
}

Attributes::const_iterator Attributes::end() const noexcept
{
    return _attributes.end();
}

AttributeDesc& Attributes::firstDataAttribute()
{
    const auto& rthis = *this;
    const auto& fda = rthis.firstDataAttribute();
    return const_cast<AttributeDesc&>(fda);
}

const AttributeDesc& Attributes::firstDataAttribute() const
{
#if 0  // DJG
    if (hasEmptyIndicator()) {
        SCIDB_ASSERT(_attributes.size() >= 2);
        return _attributes[1];
    }
#endif
    SCIDB_ASSERT(!_attributes.empty());
    auto& attr = _attributes[0];  // this location should contain attribute ID 1 for now
    _affirmInRange(attr.getId());
    return attr;
}

Attributes::const_iterator Attributes::find(const AttributeDesc& attributeDesc) const
{
    _affirmInRange(attributeDesc.getId());
    auto iter = std::find_if(_attributes.begin(), _attributes.end(),
        [&attributeDesc] (const auto& elem) { return elem.getId() == attributeDesc.getId(); });
    return iter;
}

Attributes::const_iterator Attributes::find(AttributeID attrID) const
{
    _affirmInRange(attrID);
    auto iter = std::find_if(_attributes.begin(), _attributes.end(),
        [&attrID] (const auto& elem) { return elem.getId() == attrID; });
    return iter;
}

const AttributeDesc& Attributes::findattr(AttributeID attrID) const
{
    _affirmInRange(attrID);
    auto iter = find(attrID);
    ASSERT_EXCEPTION(iter != end(), "Attribute ID " << attrID << " not found");
    return *iter;
}

const AttributeDesc& Attributes::findattr(size_t attrID_) const
{
    auto attrID = static_cast<AttributeID>(attrID_);
    auto iter = find(attrID);
    ASSERT_EXCEPTION(iter != end(), "Attribute ID " << attrID << " not found");
    return *iter;
}

const AttributeDesc& Attributes::findattr(int attrID_) const
{
    auto attrID = static_cast<AttributeID>(attrID_);
    auto iter = find(attrID);
    ASSERT_EXCEPTION(iter != end(), "Attribute ID " << attrID << " not found");
    return *iter;
}

bool Attributes::hasAttribute(AttributeID attrID) const
{
    auto iter = find(attrID);
    return iter != end();
}

void Attributes::setEmptyTagCompression(CompressorType etCompression)
{
    // Remember desired empty tag compression as the empty bitmap may (or may not!)
    // later be created, depending on whether the array is emptyable.
    _emptyTagCompression =
        etCompression == CompressorType::UNKNOWN ? CompressorType::NONE : etCompression;

    // Update the empty tag attribute descriptor, if it exists, with the
    // desired compression.
    if (hasEmptyIndicator()) {
        deleteEmptyIndicator();  // Remove outdated EBM attribute descriptor.
        addEmptyTagAttribute();  // Recreate EBM attribute descriptor with compression.
    }
}

void Attributes::push_back(const AttributeDesc& ad)
{
    if (ad.isEmptyIndicator()) {
        _emptyTagCompression = ad.getDefaultCompressionMethod();
        addEmptyTagAttribute();
    }
    else {
        _verifyAttributesContainerIntegrity();
        SCIDB_ASSERT(_nextAttributeID >= STARTING_ATTRIBUTE_ID);
        const_cast<AttributeDesc&>(ad).setId(_nextAttributeID);
        ++_nextAttributeID;
        _attributes.push_back(ad);
        _verifyAttributesContainerIntegrity();
    }
}

size_t Attributes::size() const
{
    // Always report the size of the attributes container as though
    // it has the empty bitmap.  Anyone creating auxiliary containers
    // that map to attributes but use the attribute IDs rather than
    // their container positions will always get the right element
    // in their respective container and it will be within the bounds
    // of that container.
    return _attributes.size() + STARTING_ATTRIBUTE_ID;
}

bool Attributes::empty() const
{
    return _attributes.empty();
}

bool Attributes::operator== (const Attributes& rhs) const
{
    return _attributes == rhs._attributes;
}

std::ostream& Attributes::stream_out(std::ostream& os) const
{
    return insertRange(os,_attributes,',');
}

std::ostream& operator<<(std::ostream& stream,const Attributes& atts)
{
    return atts.stream_out(stream);
}

#pragma GCC diagnostic pop

}  // namespace scidb
