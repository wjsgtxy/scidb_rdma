#ifndef ATTRIBUTES_H_
#define ATTRIBUTES_H_
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

#include <vector>
#include <iosfwd>

namespace scidb {

class AttributeDesc;

/**
 * Vector of AttributeDesc type
 */
class Attributes
{
    using ContainerType = std::vector<AttributeDesc>;

    const AttributeID STARTING_ATTRIBUTE_ID = 0;
    AttributeID _nextAttributeID{STARTING_ATTRIBUTE_ID};
    CompressorType _emptyTagCompression{CompressorType::NONE};
    ContainerType _attributes;
    size_t _emptyIndicatorPosition() const;
    void _verifyAttributesContainerIntegrity() const;
    void _computeNextAttributeID();
    void _affirmInRange(AttributeID attrID) const;
    AttributeID _lastAttributeID() const;

public:
    using const_iterator = ContainerType::const_iterator;

    Attributes(size_t r);
    Attributes();
    Attributes(const Attributes& rhs);
    Attributes(Attributes&& rhs);
    Attributes& operator= (const Attributes& rhs);
    Attributes& operator= (Attributes&& rhs);
    ~Attributes();

    bool hasEmptyIndicator() const;
    AttributeDesc const* getEmptyBitmapAttribute() const;
    Attributes& addEmptyTagAttribute();

    // TODO These two shouldn't be necessary, but I'm using them
    // as a work-around until the rest of the API settles-out.
    void deleteEmptyIndicator();
    void replace(AttributeID aid, const AttributeDesc& ad);  // for cast() only
    const_iterator begin() const noexcept;
    const_iterator end() const noexcept;
    AttributeDesc& firstDataAttribute();
    const AttributeDesc& firstDataAttribute() const;
    const_iterator find(const AttributeDesc& attributeDesc) const;
    const_iterator find(AttributeID attrID) const;
    const AttributeDesc& findattr(AttributeID attrID) const;
    const AttributeDesc& findattr(size_t attrID) const;
    const AttributeDesc& findattr(int attrID) const;
    bool hasAttribute(AttributeID attrID) const;

    /**
     * Specify the empty tag compression; default is CompressorType::NONE.
     *
     * This method should be called by ArrayDesc only as empty tag compression
     * is configurable at the array level.
     *
     * @param etCompression One of the compression algorithms from the
     * CompressorType enum.  If this is specified as CompressorType::UNKNOWN, then
     * it is interpreted as CompressorType::NONE.
     */
    void setEmptyTagCompression(CompressorType etCompression);

    template<typename Archive>
    void serializer(Archive& ar)
    {
        ar & _attributes;
        if (hasEmptyIndicator()) {
            _emptyTagCompression = getEmptyBitmapAttribute()->getDefaultCompressionMethod();
        }
        _computeNextAttributeID();
    }

    void push_back(const AttributeDesc& ad);
    size_t size() const;
    bool empty() const;
    bool operator== (const Attributes& rhs) const;
    std::ostream& stream_out(std::ostream& os) const;
};

std::ostream& operator<<(std::ostream&,const Attributes&);


}  // namespace scidb
#endif
