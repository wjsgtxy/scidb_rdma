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


#include <array/DimensionDesc.h>

#include <array/ArrayDesc.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb {
/*
 * Class DimensionDesc
 */

DimensionDesc::DimensionDesc() :
    ObjectNames(),

    _startMin(0),
    _currStart(0),
    _currEnd(0),
    _endMax(0),

    _chunkInterval(0),
    _chunkOverlap(0),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate start, Coordinate end, int64_t chunkInterval,
                             int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(start),
    _currStart(CoordinateBounds::getMax()),
    _currEnd(CoordinateBounds::getMin()),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate start, Coordinate end,
                             int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(start),
    _currStart(CoordinateBounds::getMax()),
    _currEnd(CoordinateBounds::getMin()),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate startMin, Coordinate currStart, Coordinate currEnd,
                             Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate startMin,
                             Coordinate currStart, Coordinate currEnd, Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

bool DimensionDesc::operator==(DimensionDesc const& other) const
{
    return
        _names == other._names &&
        _startMin == other._startMin &&
        _endMax == other._endMax &&
        _chunkInterval == other._chunkInterval &&
        _chunkOverlap == other._chunkOverlap;
}

uint64_t DimensionDesc::getCurrLength() const
{
    Coordinate low = _startMin;
    Coordinate high = _endMax;
#ifndef SCIDB_CLIENT
    if (_startMin == CoordinateBounds::getMin() || _endMax == CoordinateBounds::getMax()) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            if (_startMin == CoordinateBounds::getMin()) {
                low = SystemCatalog::getInstance()->getLowBoundary(_array->getId())[index];
            }
            if (_endMax == CoordinateBounds::getMax()) {
                high = SystemCatalog::getInstance()->getHighBoundary(_array->getId())[index];
            }
        } else {
            low = _currStart;
            high = _currEnd;
        }
    }
#endif
    /*
     * Check for empty array.  According to informal agreement, high
     * boundary for empty array is CoordinateBounds::getMin() and low
     * is ...::getMax().
     */
    if (low == CoordinateBounds::getMax() || high == CoordinateBounds::getMin()) {
        return 0;
    } else {
        return high - low + 1;
    }
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] os output stream to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionDesc::toString (std::ostream& os, int indent) const
{
    if (indent > 0)
    {
        os << std::string(indent, ' ');
    }

    os << "[dimDesc] names " << _names
       << " startMin " << _startMin
       << " currStart " << _currStart
       << " currEnd " << _currEnd
       << " endMax " << _endMax
       << " chnkInterval " << _chunkInterval
       << " chnkOverlap " << _chunkOverlap
       << '\n';
}

bool DimensionDesc::isReservedName(const std::string& target)
{
    SCIDB_ASSERT(!target.empty());
    return target[0] == '$';
}

void DimensionDesc::validate() const
{
    if (_startMin > _endMax)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW);
    }

    if(_startMin < CoordinateBounds::getMin())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_MIN_TOO_SMALL)
            << _startMin
            << CoordinateBounds::getMin();
    }

    if(_endMax > CoordinateBounds::getMax())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_MAX_TOO_LARGE)
            << _endMax
            << CoordinateBounds::getMax();
    }
}

void printSchema(std::ostream& stream,const Dimensions& dims)
{
    for (size_t i=0,n=dims.size(); i<n; i++)
    {
        printSchema(stream, dims[i]);
        if (i != n-1)
        {
            stream << "; ";
        }
    }
}

std::ostream& operator<<(std::ostream& os, const DimensionDesc& dim)
{
    printSchema(os, dim, /*verbose:*/true);
    return os;
}

void printSchema(std::ostream& os, const DimensionDesc& dim, bool verbose)
{
    // Stringify numbers that may need special representation like '*'.
    // (Low bounds do not use any such notation, by design.)

    stringstream end;
    if (dim.getEndMax() == CoordinateBounds::getMax()) {
        end << '*';
    } else {
        end << dim.getEndMax();
    }

    stringstream currEnd;
    if (verbose) {
        if (dim.getCurrEnd() == CoordinateBounds::getMax()) {
            currEnd << '*';
        } else {
            currEnd << dim.getCurrEnd();
        }
    }

    // Emit the dimension name.  In verbose mode we want the aliases too.
    if (verbose) {
        os << dim.getNamesAndAliases();
    } else {
        printNames(os, dim.getNamesAndAliases());
    }

    os << '=' << dim.getStartMin() << ':' << end.str();
    if (verbose) {
        os << " (" << dim.getCurrStart() << ':' << currEnd.str() << ')';
    }
    os << ':' << dim.getChunkOverlap() << ':';

    int64_t interval = dim.getRawChunkInterval();
    if (interval == DimensionDesc::AUTOCHUNKED) {
        os << '*';
    } else {
        os << interval;
    }
}



}  // namespace scidb
