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


#include <array/ArrayDesc.h>

#include <array/ArrayDistribution.h>
#include <array/ArrayName.h>
#include <array/AttributeDesc.h>
#include <query/TypeSystem.h>
#include <rbac/Rbac.h>
#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

namespace {
log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.metadata"));
}

using namespace std;

namespace scidb {

/*
 * Class ArrayDesc
 */

ArrayDesc::ArrayDesc() :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _flags(0)
{}


ArrayDesc::ArrayDesc(const std::string &namespaceName,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _namespaceName(namespaceName),
    _arrayName(arrayName),
    _attributes(attributes),
    _attributesWithoutBitmap(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidDistType(arrDist->getDistType())); // temporary restriction while scaffolding erected for #4546
#endif

        // Copy the collection of attributes and remove the empty bitmap from
        // that copy if it's present.  The loop responsible for erasing the empty
        // bitmap is consolidated to a function in the next commit.
    _attributesWithoutBitmap.deleteEmptyIndicator();

    initializeDimensions();
}

ArrayDesc::ArrayDesc(const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags,
                     bool isCcmClient) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _namespaceName(rbac::PUBLIC_NS_NAME),
    _arrayName(arrayName),
    _attributes(attributes),
    _attributesWithoutBitmap(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

    if (!isCcmClient) {
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidDistType(arrDist->getDistType())); // temporary restriction while scaffolding erected for #4546
    }
        // Copy the collection of attributes and remove the empty bitmap from
        // that copy if it's present.  The loop responsible for erasing the empty
        // bitmap is consolidated to a function in the next commit.
    _attributesWithoutBitmap.deleteEmptyIndicator();

    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &namespaceName,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags)
:
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _namespaceName(namespaceName),
    _arrayName(arrayName),
    _attributes(attributes),
    _attributesWithoutBitmap(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidDistType(arrDist->getDistType())); // temporary restriction while scaffolding erected for #4546
#endif
    // either both 0 or not...
    assert(arrId == 0 || uAId != 0);

        // Copy the collection of attributes and remove the empty bitmap from
        // that copy if it's present.  The loop responsible for erasing the empty
        // bitmap is consolidated to a function in the next commit.
    _attributesWithoutBitmap.deleteEmptyIndicator();

    initializeDimensions();
}


ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags)
:
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _namespaceName(rbac::PUBLIC_NS_NAME),
    _arrayName(arrayName),
    _attributes(attributes),
    _attributesWithoutBitmap(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidDistType(arrDist->getDistType())); // temporary restriction while scaffolding erected for #4546
#endif
    // either both 0 or not...
    assert(arrId == 0 || uAId != 0);

        // Copy the collection of attributes and remove the empty bitmap from
        // that copy if it's present.  The loop responsible for erasing the empty
        // bitmap is consolidated to a function in the next commit.
    _attributesWithoutBitmap.deleteEmptyIndicator();

    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayDesc const& other) :
    _arrId(other._arrId),
    _uAId(other._uAId),
    _versionId(other._versionId),
    _namespaceName(other._namespaceName),
    _arrayName(other._arrayName),
    _attributes(other._attributes),
    _attributesWithoutBitmap(other._attributesWithoutBitmap),
    _dimensions(other._dimensions),
    _flags(other._flags),
    _distribution(other._distribution),
    _residency(other._residency)
{
    splitQualifiedArrayName(other._arrayName, _namespaceName, _arrayName);
    initializeDimensions();
}

string ArrayDesc::getQualifiedArrayName(bool includeVersion) const
{
    auto arrayName = includeVersion ? _arrayName : makeUnversionedName(_arrayName);

    if (isQualifiedArrayName(arrayName) || (_namespaceName==""))
    {
        return arrayName;
    }

    return makeQualifiedArrayName(_namespaceName, arrayName);
}

bool ArrayDesc::operator==(ArrayDesc const& other) const
{
    bool same =
       _namespaceName == other._namespaceName &&
       _arrayName == other._arrayName &&
       _attributes == other._attributes &&
       _dimensions == other._dimensions &&
       _flags == other._flags;

    if (!same) {
        return false;
    }
    if (_distribution && other._distribution) {
        same = _distribution->checkCompatibility(other._distribution);
    } else if (_distribution || other._distribution) {
        return false;
    }
    if (!same) {
        return false;
    }
    if (_residency && other._residency) {
        same = _residency->isEqual(other._residency);
    } else if (_residency || other._residency) {
        return false;
    }
    return same;
}

ArrayDesc& ArrayDesc::operator=(ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _namespaceName = other._namespaceName;
    _arrayName = other._arrayName;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _flags = other._flags;
    initializeDimensions();
    _distribution = other._distribution;
    _residency = other._residency;
    return *this;
}

void ArrayDesc::initializeDimensions()
{
    Coordinate logicalChunkSize = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._array = this;
        // If unknown (autochunked) interval, use 1 so the calculation is as close as it can be.
        Coordinate chunkLength = _dimensions[i].getChunkIntervalIfAutoUse(1);
        Coordinate t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength ) //overflow check
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        chunkLength = t;
        t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength) //overflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }

        t = logicalChunkSize * chunkLength;
        if (chunkLength != 0 && t / chunkLength != logicalChunkSize) //overflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        logicalChunkSize = t;
    }
}

void ArrayDesc::trim()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        DimensionDesc& dim = _dimensions[i];
        SCIDB_ASSERT(dim._chunkInterval > 0);
        if (dim._startMin == CoordinateBounds::getMin() && dim._currStart != CoordinateBounds::getMax()) {
            dim._startMin = dim._currStart;
        }
        if (dim._endMax == CoordinateBounds::getMax() && dim._currEnd != CoordinateBounds::getMin()) {
            dim._endMax = (dim._startMin + (dim._currEnd - dim._startMin + dim._chunkInterval) / dim._chunkInterval * dim._chunkInterval + dim._chunkOverlap - 1);
        }
    }
}

Coordinates ArrayDesc::getLowBoundary() const
{
    assert(!_dimensions.empty());
    Coordinates low(_dimensions.size());
    for (size_t i = 0, n = _dimensions.size(); i < n; ++i) {
        const DimensionDesc& dim = _dimensions[i];
        low[i] = dim.getCurrStart();
    }
    return low;
}

Coordinates ArrayDesc::getHighBoundary() const
{
    assert(!_dimensions.empty());
    Coordinates high(_dimensions.size());
    for (size_t i = 0, n = _dimensions.size(); i < n; ++i) {
        const DimensionDesc& dim = _dimensions[i];
        high[i] = dim.getCurrEnd();
    }
    return high;
}

InstanceID ArrayDesc::getPrimaryInstanceId(Coordinates const& pos,
                                           size_t instanceCount) const
{
    SCIDB_ASSERT(_distribution);
    return _distribution->getPrimaryChunkLocation(pos, _dimensions, instanceCount);
}

ssize_t ArrayDesc::findDimension(const std::string& name, const std::string& alias) const
{
    const ssize_t N_DIMS = _dimensions.size();
    for (ssize_t i = 0; i < N_DIMS; ++i) {
        if (_dimensions[i].hasNameAndAlias(name, alias)) {
            return i;
        }
    }
    return -1;
}

bool ArrayDesc::contains(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if (pos[i] < dims[i].getStartMin() || pos[i] > dims[i].getEndMax()) {
            return false;
        }
    }
    return true;
}

void ArrayDesc::getChunkPositionFor(Coordinates& pos) const
{
    getChunkPositionFor(_dimensions, pos);
}

void ArrayDesc::getChunkPositionFor(Dimensions const& dims, Coordinates& pos)
{
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if ( dims[i].getRawChunkInterval() > 0) {
            Coordinate diff = (pos[i] - dims[i].getStartMin()) % dims[i].getChunkInterval();

            // The code below ensures the correctness of this code, in case pos[i] < dims[i].getStartMin().
            // Example:
            //   - Suppose dimStart=0, chunkInterval=5. All chunkPos should be a multiple of 5.
            //   - Given pos[i]=-9, we desire to reduce it to -10.
            //   - The calculated diff = -4.
            //   - The step below changes diff to a non-negative number of 1, bedore using it to decrease pos[i].
            if (diff < 0) {
                diff += dims[i].getChunkInterval();
            }

            pos[i] -= diff;
        }
    }
}

bool ArrayDesc::isAChunkPosition(Coordinates const& pos) const
{
    Coordinates chunkPos = pos;
    getChunkPositionFor(chunkPos);
    return coordinatesCompare(pos, chunkPos) == 0;
}

bool ArrayDesc::isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const
{
    Coordinates chunkPosForCell = cellPos;
    getChunkPositionFor(chunkPosForCell);
    return coordinatesCompare(chunkPosForCell, chunkPos) == 0;
}

void ArrayDesc::getChunkBoundaries(Coordinates const& chunkPosition,
                                   bool withOverlap,
                                   Coordinates& lowerBound,
                                   Coordinates& upperBound) const
{
#ifndef NDEBUG
    do
    {
        Coordinates alignedChunkPosition = chunkPosition;
        getChunkPositionFor(alignedChunkPosition);
        SCIDB_ASSERT(alignedChunkPosition == chunkPosition);
    }
    while(false);
#endif /* NDEBUG */
    Dimensions const& d = getDimensions();
    Dimensions::size_type const n = d.size();
    SCIDB_ASSERT(n == chunkPosition.size());
    lowerBound = chunkPosition;
    upperBound = chunkPosition;
    for (size_t i = 0; i < n; i++) {
        upperBound[i] += d[i].getChunkInterval() - 1;
    }
    if (withOverlap) {
        for (size_t i = 0; i < n; i++) {
            lowerBound[i] -= d[i].getChunkOverlap();
            upperBound[i] += d[i].getChunkOverlap();
        }
    }
    for (size_t i = 0; i < n; ++i) {
        lowerBound[i] = std::max(lowerBound[i], d[i].getStartMin());
        upperBound[i] = std::min(upperBound[i], d[i].getEndMax());
    }
}

uint64_t ArrayDesc::getSize() const
{
    uint64_t size = 1;
    //uint64_t max = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0, n = _dimensions.size(); i < n; i++)
    {
        uint64_t length = _dimensions[i].getLength();
        //check for uint64_t overflow

        // As soon as we encounter one dimension with a '*' give up
        // and return maxLength.
        // Or, when length * size > getMaxLength() return getMaxLength()
        if (_dimensions[i].isMaxStar() || length > CoordinateBounds::getMaxLength() / size)
        {
            return CoordinateBounds::getMaxLength();
        }
        size *= length;
    }
    return size;
}

AttributeDesc const* ArrayDesc::getEmptyBitmapAttribute() const
{
    return _attributes.getEmptyBitmapAttribute();
}

Attributes const& ArrayDesc::getAttributes(bool excludeEbm) const
{
    return excludeEbm ? _attributesWithoutBitmap : _attributes;
}

ArrayDesc& ArrayDesc::setAttributes(Attributes const& attributes)
{
    _attributes = attributes;
    _attributesWithoutBitmap = attributes;
    _attributesWithoutBitmap.deleteEmptyIndicator();
    return *this;
}

ArrayDesc& ArrayDesc::addEmptyTagAttribute()
{
    _attributes.addEmptyTagAttribute();
    return *this;
}

void ArrayDesc::cutOverlap()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._chunkOverlap = 0;
    }
}

Dimensions ArrayDesc::grabDimensions(VersionID version) const
{
    Dimensions dims(_dimensions.size());
    for (size_t i = 0; i < dims.size(); i++) {
        DimensionDesc const& dim = _dimensions[i];
        dims[i] = dim;
    }
    return dims;
}

void ArrayDesc::addAlias(const std::string& alias)
{
    for (const auto& attr : _attributes)
    {
        const_cast<AttributeDesc&>(attr).addAlias(alias);
    }

    for (const auto& attr : _attributesWithoutBitmap)
    {
        const_cast<AttributeDesc&>(attr).addAlias(alias);
    }

    for (DimensionDesc& dim : _dimensions)
    {
        dim.addAlias(alias);
    }
}

bool ArrayDesc::coordsAreAtChunkStart(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
        if ( coords[i] < _dimensions[i].getStartMin() ||
             coords[i] > _dimensions[i].getEndMax() )
        {
            return false;
        }

        if (_dimensions[i].isAutochunked())
        {
            if (coords[i] != _dimensions[i].getStartMin())
            {
                // We have no way of knowing, unless the coordinate is
                // at the very beginning of the dimension range.
                return false;
            }
        }
        else
        {
            if ( (coords[i] - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 )
            {
                return false;
            }
        }
    }
    return true;
}

bool ArrayDesc::coordsAreAtChunkEnd(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
        if ( coords[i] != _dimensions[i].getEndMax() &&
             (coords[i] < _dimensions[i].getStartMin() ||
              coords[i] > _dimensions[i].getEndMax() ||
              (coords[i] + 1 - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 ))
        {
            return false;
        }
    }
    return true;
}

void ArrayDesc::addAttribute(AttributeDesc const& newAttribute)
{
    for (size_t i = 0; i< _dimensions.size(); i++)
    {
        if (_dimensions[i].getBaseName() == newAttribute.getName() || newAttribute.hasAlias(_dimensions[i].getBaseName()))
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }

    for (const auto& attr : _attributes)
    {
        if (attr.getName() == newAttribute.getName())
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }
    _attributes.push_back(newAttribute);
    if (!newAttribute.isEmptyIndicator())
    {
        _attributesWithoutBitmap.push_back(newAttribute);
    }
}

double ArrayDesc::getNumChunksAlongDimension(size_t dimension, Coordinate start, Coordinate end) const
{
    assert(dimension < _dimensions.size());
    if(start==CoordinateBounds::getMax() && end ==CoordinateBounds::getMin())
    {
        start = _dimensions[dimension].getStartMin();
        end = _dimensions[dimension].getEndMax();
    }
    return ceil(
        (static_cast<double>(end) - static_cast<double>(start) + 1.0)
        / static_cast<double>(_dimensions[dimension].getChunkInterval()));
}

// Can src array be stored/inserted into dst array?
void ArrayDesc::checkConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc, unsigned options)
{
    if (!(options & IGNORE_PSCHEME) &&
        !(srcDesc.getDistribution()->checkCompatibility(dstDesc.getDistribution()) &&
          srcDesc.getResidency()->isEqual(dstDesc.getResidency())))
    {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
           << "Target of INSERT/STORE must have same distribution as the source";
    }

    Dimensions const& srcDims = srcDesc.getDimensions();
    Dimensions const& dstDims = dstDesc.getDimensions();

    if (srcDims.size() != dstDims.size())
    {
        //TODO: this will get lifted when we allow redimension+insert in the same op
        //and when we DO implement redimension+insert - we will need to match
        // attributes/dimensions by name, not position.
        LOG4CXX_DEBUG(logger, "Source and target array descriptors are not conformant:"
                      << srcDesc.toString() << ", " << dstDesc.toString());
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
              << "INSERT/STORE" << srcDims.size() << dstDims.size();
    }

    const bool checkOverlap = !(options & IGNORE_OVERLAP);
    const bool checkInterval = !(options & IGNORE_INTERVAL);
    for (size_t i = 0, n = srcDims.size(); i < n; i++)
    {
        if( (checkInterval && srcDims[i].getChunkInterval() != dstDims[i].getChunkInterval()) ||
            (checkOverlap  && srcDims[i].getChunkOverlap()  != dstDims[i].getChunkOverlap()) )
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant"
                          <<" in chunk interval or overlap:"
                          << srcDesc.toString() << ", " << dstDesc.toString());

            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                  << srcDims[i].getBaseName() << dstDims[i].getBaseName();
        }
        if (srcDims[i].getStartMin() != dstDims[i].getStartMin()) {
            ostringstream oss;
            oss << '[' << srcDims[i] << "] != [" << dstDims[i] << ']';
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH)
                  << oss.str();
        }
        if (srcDims[i].getEndMax() == dstDims[i].getEndMax()) {
            // Dimension matches, all is cool.
            continue;
        }
        else if (srcDims[i].getEndMax() > dstDims[i].getEndMax()) {
            if (srcDims[i].getEndMax() == CoordinateBounds::getMax()) {
                // Go ahead and try to inject S[i=0:*,...] into D[j=0:N,...] on the expectation
                // that S doesn't actually have any data beyond N.  (If it does, fail when you
                // know that for sure, not now.)
                continue;
            }
            LOG4CXX_ERROR(logger, "ArrayDesc::checkConformity: INCORRECT DIM BOUNDARY");
            LOG4CXX_ERROR(logger, "ArrayDesc::checkConformity: srcDims["<<i<<"].getEndMax():"
                                   << srcDims[i].getEndMax());
            LOG4CXX_ERROR(logger, "ArrayDesc::checkConformity: dstDims["<<i<<"].getEndMax():"
                                   << dstDims[i].getEndMax());
            LOG4CXX_ERROR(logger, "ArrayDesc::checkConformity: CoordinateBounds::getMax():"
                                   << CoordinateBounds::getMax());
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY)
                << dstDims[i].getStartMin() << dstDims[i].getEndMax();
        }
        else if ((options & SHORT_OK_IF_EBM) && srcDesc.getEmptyBitmapAttribute() != NULL) {
            // The source dimension can be shorter if the source array has an empty bitmap.
            continue;

            // One day all arrays will have empty bitmaps.  Then the question becomes, is the
            // check for partial source chunks (below) ever useful?
        }
        else if (srcDims[i].getLength() % srcDims[i].getChunkInterval() != 0) {
            // The source dimension can be shorter if there are no partial source chunks.
            ostringstream oss;
            oss << "Source array for INSERT/STORE must have uniform chunk sizes";
            if (options & SHORT_OK_IF_EBM) {
                oss << " (for non-emptyable arrays)";
            }
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << oss.str();
        }
    }

    checkAttributeConformity(srcDesc, dstDesc);
}

// Can source array cells be stored in destination array?
void ArrayDesc::checkAttributeConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc)
{
    Attributes const& srcAttrs = srcDesc.getAttributes(true);
    Attributes const& dstAttrs = dstDesc.getAttributes(true);

    if (srcAttrs.size() != dstAttrs.size())
    {
        if (srcDesc.getName() != dstDesc.getName() ||
            srcDesc.getNamespaceName() != dstDesc.getNamespaceName() ||
            srcAttrs.size() > dstAttrs.size()) {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                          << srcDesc.toString() << ", " << dstDesc.toString());

            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << "Target of INSERT/STORE must have same attributes as the source";
        }

        // Otherwise, the src and target are the same array, yet at different versions
        // due to add_attributes.
    }
    auto srcAttrsIter = srcAttrs.begin();
    auto dstAttrsIter = dstAttrs.begin();
    while (srcAttrsIter != srcAttrs.end())
    {
        if(srcAttrsIter->getType() != dstAttrsIter->getType())
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                          << srcDesc.toString() << ", " << dstDesc.toString());

            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
            << srcAttrsIter->getName() << srcAttrsIter->getType() << dstAttrsIter->getType();
        }

        //can't store nulls into a non-nullable attribute
        if(!dstAttrsIter->isNullable() && srcAttrsIter->isNullable())
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                          << srcDesc.toString() << ", " << dstDesc.toString());
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
            << srcAttrsIter->getName();
        }

        ++srcAttrsIter;
        ++dstAttrsIter;

        if (srcAttrsIter == srcAttrs.end() &&
            dstAttrsIter != dstAttrs.end()) {
            // dstAttrs has attributes from a previous add_attributes call
            break;
        }
    }
}

bool ArrayDesc::areNamesUnique() const
{
    set<string> names;
    for (const auto& attr : _attributes) {
        auto iter_and_inserted = names.insert(attr.getName());
        if (!iter_and_inserted.second) {
            return false;
        }
    }
    for (auto const& dim : _dimensions) {
        auto iter_and_inserted = names.insert(dim.getBaseName());
        if (!iter_and_inserted.second) {
            return false;
        }
    }
    return true;
}

// Determine if 'this' and the 'other' schema match based on the
// selection critera.  So far, only dimension-based criteria are implemented.
bool ArrayDesc::sameSchema(ArrayDesc const& other, SchemaFieldSelector const &sel) const
{
    Dimensions::const_iterator itOther, itThis;
    Dimensions const& dimsOther = other.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    ASSERT_EXCEPTION(dimsOther.size() == _dimensions.size(), "Dimension count mismatch");

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end());
        ++itOther,                        ++itThis)
    {
        assert(itThis != _dimensions.end());
        if(sel.chunkOverlap()  && ((*itOther).getChunkOverlap()     != (*itThis).getChunkOverlap()))     return false;
        if(sel.chunkInterval()) {
            if (itOther->getRawChunkInterval() != itThis->getRawChunkInterval()) {
                if (itThis->isIntervalResolved() || !sel.wildcardInterval()) {
                    return false;
                }
            }
        }
        if(sel.startMin()      && ((*itOther).getStartMin()         != (*itThis).getStartMin()))         return false;
        if(sel.endMax()        && ((*itOther).getEndMax()           != (*itThis).getEndMax()))           return false;
    }

    return true;
}

// Replace this schema's dimension values with those from another schema
void ArrayDesc::replaceDimensionValues(ArrayDesc const& other)
{
    Dimensions::iterator        itThis;
    Dimensions::const_iterator  itOther;
    Dimensions const& dimsOther = other.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    ASSERT_EXCEPTION(dimsOther.size() == _dimensions.size(), "Dimension count mismatch");

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end())  &&  (itThis != _dimensions.end());
        ++itOther,                        ++itThis)
    {
        (*itThis).replaceValues((*itOther));
    }
}

bool ArrayDesc::hasReservedNames() const
{
    for (const auto& attr : _attributes) {
        if (AttributeDesc::isReservedName(attr.getName())) {
            return true;
        }
    }

    if (!isDataframe()) {
        for (const auto& dim : _dimensions) {
            if (DimensionDesc::isReservedName(dim.getBaseName())) {
                return true;
            }
        }
    }

    return false;
}

void ArrayDesc::setEmptyTagCompression(CompressorType etCompression)
{
    // While the empty tag compression is configurable only at the
    // array-level, we leverage the fact that the empty tag is
    // in fact an attribute for handling its compression.
    _attributes.setEmptyTagCompression(etCompression);
}

void printSchema(std::ostream& stream,
                 const ArrayDesc& ob,
                 bool includeDimensions)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << ">";
    if (includeDimensions) {
        stream << " [";
        printSchema(stream, ob.getDimensions());
        stream << ']';
    }
}

std::ostream& operator<<(std::ostream& stream,const ArrayDesc& ob)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif

    stream << ob.getQualifiedArrayName()
           << '<' << ob.getAttributes(true)
           << "> [" << ob.getDimensions() << ']';

#ifndef SCIDB_CLIENT
    stream << " ArrayId: " << ob.getId();
    stream << " UnversionedArrayId: " << ob.getUAId();
    stream << " Version: " << ob.getVersionId();
    stream << " Flags: " << ob.getFlags();
    stream << " Distrib: " << ob.getDistribution();
    stream << " Residency: " << ob.getResidency();
    stream << " <" << ob.getAttributes(false) << ">" ;
#endif

    return stream;
}

void cloneAttributes(ArrayDesc const& source, ArrayDesc& dest)
{
    dest.setAttributes(source.getAttributes(true));
    if (source.getAttributes(false).hasEmptyIndicator()) {
        dest.addEmptyTagAttribute();
    }
}

std::string ArrayDesc::toString () const
{
    stringstream ss;
    ss << (*this);
    return ss.str();
}

}  // namespace scidb
