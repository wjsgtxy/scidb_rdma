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
 * @file PhysicalBoundaries.cpp
 * @author Alex Poliakov
 */

#include <query/PhysicalBoundaries.h>

#include <array/Array.h>
#include <array/ArrayDesc.h>
#include <array/ConstChunk.h>
#include <array/MemoryBuffer.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <util/SpatialType.h>

#include <log4cxx/logger.h>
#include <unordered_set>

using namespace std;

namespace scidb
{

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.bounds"));
}

PhysicalBoundaries::PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density)
    : _startCoords(start)
    , _endCoords(end)
    , _density(density)
{
    if (_startCoords.size() != _endCoords.size()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }

    if (density < 0.0 || density > 1.0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "density not between 0.0 and 1.0";
    }

    for (size_t i = 0; i< _startCoords.size(); i++)
    {
        if (_startCoords[i] < CoordinateBounds::getMin())
        {
            _startCoords[i] = CoordinateBounds::getMin();
        }
        else if (_startCoords[i] > CoordinateBounds::getMax())
        {
            _startCoords[i] = CoordinateBounds::getMax();
        }

        if (_endCoords[i] < CoordinateBounds::getMin())
        {
            _endCoords[i] = CoordinateBounds::getMin();
        }
        else if (_endCoords[i] > CoordinateBounds::getMax())
        {
            _endCoords[i] = CoordinateBounds::getMax();
        }
    }
}


PhysicalBoundaries PhysicalBoundaries::createFromFullSchema(ArrayDesc const& schema )
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<schema.getDimensions().size(); i++)
    {
        resultStart.push_back(schema.getDimensions()[i].getStartMin());
        resultEnd.push_back(schema.getDimensions()[i].getEndMax());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}


PhysicalBoundaries PhysicalBoundaries::createEmpty(size_t numDimensions)
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<numDimensions; i++)
    {
        resultStart.push_back(CoordinateBounds::getMax());
        resultEnd.push_back(CoordinateBounds::getMin());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}


bool PhysicalBoundaries::isEmpty() const
{
    if (_startCoords.size() == 0)
    {
        return true;
    }

    for (size_t i = 0; i<_startCoords.size(); i++)
    {
        if (_startCoords[i] > _endCoords[i])
        {
            return true;
        }
    }
    return false;
}


uint64_t PhysicalBoundaries::getCellNumber (Coordinates const& coords, Dimensions const& dims)
{
    if (dims.size() != coords.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }
    uint64_t result = 0;
    for ( size_t i = 0, n = dims.size(); i < n; i++)
    {
        if (dims[i].getLength()==0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << "dimension has zero length";
        }
        uint64_t t = result * dims[i].getLength();
        if (t / dims[i].getLength() != result) //overflow check multiplication
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
        t = result + coords[i] - dims[i].getStartMin();
        if (t < result) //overflow check addition
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
    }
    if (!CoordinateBounds::isValidLength(result))
    {
        return CoordinateBounds::getMaxLength();
    }
    return result;
}


Coordinates PhysicalBoundaries::getCoordinates(uint64_t origCellNum, Dimensions const& dims, bool throwOnOverflow)
{
    uint64_t cellNum = origCellNum;  // working copy
    if(cellNum >= CoordinateBounds::getMaxLength())
    {
        return Coordinates(dims.size(), CoordinateBounds::getMax());
    }
    Coordinates result (dims.size(), 0);
    for (int i = safe_static_cast<int>(dims.size()); --i >= 0;)
    {
        result[i] = dims[i].getStartMin() + (cellNum % dims[i].getLength());
        cellNum /= dims[i].getLength();
    }
    if (cellNum != 0)
    {
        if (throwOnOverflow)
        {
            stringstream ss;
            ss << "Non-zero remainder " << cellNum << " for cell number " << origCellNum << " in " << dims;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << ss.str();
        }
        else
        {
            LOG4CXX_DEBUG(logger, "Non-zero remainder " << cellNum << " for cell number " << origCellNum
                          << " in " << dims << ", using {*,...}");
            return Coordinates(dims.size(), CoordinateBounds::getMax());
        }
    }
    return result;
}


Coordinates PhysicalBoundaries::reshapeCoordinates (Coordinates const& inCoords,
                                                    Dimensions const& currentDims,
                                                    Dimensions const& newDims,
                                                    bool strict)
{
    uint64_t cellNum = getCellNumber(inCoords, currentDims);
    if ( cellNum >= CoordinateBounds::getMaxLength() )
    {
        return Coordinates(newDims.size(), CoordinateBounds::getMax());
    }
    return getCoordinates(cellNum, newDims, strict);
}


uint64_t PhysicalBoundaries::getNumCells (Coordinates const& start, Coordinates const& end)
{
    if (PhysicalBoundaries(start,end).isEmpty())
    {
        return 0;
    }
    uint64_t result = 1;
    for ( size_t i = 0; i < end.size(); i++)
    {
        ASSERT_EXCEPTION(start[i] >= CoordinateBounds::getMin(),
                         "Invalid start[i], smaller than minimum coordinate");
        ASSERT_EXCEPTION(end[i] <= CoordinateBounds::getMax(),
                         "Invalid end[i], bigger than maximum coordinate");
        if(end[i] >= start[i])
        {
            uint64_t t = result * (end[i] - start[i] + 1);
            if ( t / (end[i] - start[i] + 1) != result) //overflow check multiplication
            {
                return CoordinateBounds::getMaxLength();
            }
            result = t;
        }
        else
        {
            result *= 0;
        }
    }
    if (!CoordinateBounds::isValidLength(result))
    {
        return CoordinateBounds::getMaxLength();
    }
    return result;
}


uint64_t PhysicalBoundaries::getNumCells() const
{
    return getNumCells(_startCoords, _endCoords);
}


uint64_t PhysicalBoundaries::getNumChunks(Dimensions const& dims) const
{
    if (_startCoords.size() != dims.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }

    if (isEmpty())
    {
        return 0;
    }

    uint64_t result = 1;
    for (size_t i =0; i < _endCoords.size(); i++)
    {
        if (_startCoords[i]<=CoordinateBounds::getMin() || _endCoords[i]>=CoordinateBounds::getMax())
        {
            return CoordinateBounds::getMaxLength();
        }

        DimensionDesc const& dim = dims[i];
        if (!dim.isIntervalResolved() )
        {
            throw SYSTEM_EXCEPTION_SUBCLASS(UnknownChunkIntervalException)
                << "Dimension interval not resolved";
        }

        if (_startCoords[i] < dim.getStartMin() || _endCoords[i] > dim.getEndMax())
        {
            // Note: *not* an internal error, this can happen for example if we insert(unbounded, bounded)
            // and the data in the unbounded (i.e. <...>[i=0:*,...]) array does not fit inside the bounded one.
            // See ticket #2016.

            stringstream ss;
            ss << "At dimension " << i << ", bounding box [" << CoordsToStr(_startCoords)
               << ", " << CoordsToStr(_endCoords) << "] falls outside of dimensions " << dims;
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << ss.str();
        }

        Coordinate arrayStart = dim.getStartMin();
        int64_t chunkInterval = dim.getChunkInterval();
        Coordinate physStart = _startCoords[i]; //TODO:OPTAPI (- overlap) ?
        Coordinate physEnd = _endCoords[i];
        int64_t numChunks = chunkInterval == 0 ? 0 :
            ((physEnd - arrayStart + chunkInterval) / chunkInterval) - ((physStart - arrayStart) / chunkInterval);

        uint64_t t = result * numChunks;
        if ( numChunks && t / numChunks != result) //overflow check multiplication
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
    }
    return result;
}


std::ostream& operator<< (std::ostream& stream, const PhysicalBoundaries& bounds)
{
    stream<<"start "<<bounds._startCoords<<" end "<<bounds._endCoords<<" density "<<bounds._density;
    return stream;
}


bool PhysicalBoundaries::isInsideBox (Coordinate const& in, size_t const& dimensionNum) const
{
    if (dimensionNum >= _startCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    Coordinate start = _startCoords[dimensionNum];
    Coordinate end = _endCoords[dimensionNum];

    return in >= start && in <= end;
}


bool PhysicalBoundaries::isInsideBox (Coordinates const& in) const
{
    if ( in.size() > _startCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    for ( size_t i = 0, len = in.size(); i < len; ++i )
    {
        if (! isInsideBox ( in[i], i ) )
        {
          return false;
        }
    }

    return true;
}


bool PhysicalBoundaries::contains (PhysicalBoundaries const& other) const
{
    return isInsideBox(other._startCoords)
        && isInsideBox(other._endCoords);
}


PhysicalBoundaries& PhysicalBoundaries::intersectWith (PhysicalBoundaries const& other)
{
    if (&other == this) {
        return *this;
    }

    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty() || other.isEmpty())
    {
        *this = createEmpty(_startCoords.size());
        return *this;
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart < otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd > otherEnd ? otherEnd : myEnd);
    }

    double intersectionCells = static_cast<double>(getNumCells(start,end));

    double resultDensity = 1.0;
    if (intersectionCells > 0)
    {
        double myCells = static_cast<double>(getNumCells());
        double otherCells = static_cast<double>(other.getNumCells());
        double maxMyDensity = min( _density * myCells / intersectionCells, 1.0 );
        double maxOtherDensity = min ( other._density * otherCells / intersectionCells, 1.0);
        resultDensity = min (maxMyDensity, maxOtherDensity);
    }

    _startCoords = start;
    _endCoords = end;
    _density = resultDensity;

    return *this;
}


PhysicalBoundaries intersectWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right)
{
    PhysicalBoundaries result(left);
    result.intersectWith(right);
    return result;
}


PhysicalBoundaries& PhysicalBoundaries::unionWith (PhysicalBoundaries const& other)
{
    if (&other == this) {
        return *this;
    }

    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty()) {
        *this = other;
        return *this;
    }

    if (other.isEmpty()) {
        return *this;
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart > otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd < otherEnd ? otherEnd : myEnd);
    }

    double myCells = static_cast<double>(getNumCells());
    double otherCells = static_cast<double>(other.getNumCells());
    double resultCells = static_cast<double>(getNumCells(start, end));
    double maxDensity = min ( (myCells * _density + otherCells * other._density ) / resultCells, 1.0);

    _startCoords = start;
    _endCoords = end;
    _density = maxDensity;

    return *this;
}


PhysicalBoundaries unionWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right)
{
    PhysicalBoundaries result(left);
    result.unionWith(right);
    return result;
}


PhysicalBoundaries& PhysicalBoundaries::crossWith (PhysicalBoundaries const& other)
{
    // This code works for the (seldom seen in practice I admit)
    // "this == &other" case, let's keep it that way!

    if (isEmpty() || other.isEmpty())
    {
        *this = createEmpty(_startCoords.size()+other._startCoords.size());
        return *this;
    }

    Coordinates start, end;
    for (size_t i=0; i<_startCoords.size(); i++)
    {
        start.push_back(_startCoords[i]);
        end.push_back(_endCoords[i]);
    }
    for (size_t i=0; i<other.getStartCoords().size(); i++)
    {
        start.push_back(other.getStartCoords()[i]);
        end.push_back(other.getEndCoords()[i]);
    }

    _startCoords = start;
    _endCoords = end;
    _density *= other._density;

    return *this;
}


PhysicalBoundaries crossWith (PhysicalBoundaries const& left, PhysicalBoundaries const& right)
{
    PhysicalBoundaries result(left);
    result.crossWith(right);
    return result;
}


PhysicalBoundaries& PhysicalBoundaries::shiftBy (Coordinates const& offset)
{
    SCIDB_ASSERT(offset.size() == _startCoords.size());
    SCIDB_ASSERT(offset.size() == _endCoords.size());

    for (size_t i = 0; i < offset.size(); ++i) {
        _startCoords[i] += offset[i];
        _endCoords[i] += offset[i];
    }

    return *this;
}

PhysicalBoundaries shiftBy (PhysicalBoundaries const& bounds, Coordinates const& offset)
{
    PhysicalBoundaries result(bounds);
    result.shiftBy(offset);
    return result;
}


namespace {
    bool hasMaxStar(Coordinates const& coords)
    {
        for (Coordinate const& c : coords) {
            if (CoordinateBounds::isMaxStar(c)) {
                return true;
            }
        }
        return false;
    }
}


PhysicalBoundaries PhysicalBoundaries::reshape(Dimensions const& oldDims, Dimensions const& newDims) const
{
    if (isEmpty())
    {
        return createEmpty(newDims.size());
    }

    // If *this* bounding box is already unbounded (hasMaxStar), don't complain if it won't fit into
    // the new shape.  That wouldn't be surprising.
    ASSERT_EXCEPTION(!hasMaxStar(_startCoords), "Starting coordinate somehow got set to '*'");
    const bool strict = !hasMaxStar(_endCoords);

    Coordinates start = reshapeCoordinates(_startCoords, oldDims, newDims, strict);
    Coordinates end = reshapeCoordinates(_endCoords, oldDims, newDims, strict);

    if (newDims.size() > oldDims.size())
    {
        bool dimensionFull = false;

        for (size_t i = 0; i < start.size(); i++)
        {
            if (dimensionFull)
            {
                if (newDims[i].getStartMin() <= CoordinateBounds::getMin()
                    || newDims[i].getEndMax() >= CoordinateBounds::getMax())
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY);
                }

                start[i] = newDims[i].getStartMin();
                end[i] = newDims[i].getEndMax();
            }
            else if(end[i] > start[i])
            {
                dimensionFull = true;
            }
        }
    }

    // Cell counts will be wildly wrong when bounding coordinates use '*' (MAX_COORDINATE), so use
    // std::min() to ensure that the density computation stays sane, in range [0.0 .. 1.0].
    double startingCells = static_cast<double>(getNumCells());
    double resultCells = static_cast<double>(getNumCells(start, end));
    double newDensity = std::min(1.0, _density * startingCells / resultCells);
    return PhysicalBoundaries(start, end, newDensity);
}


std::shared_ptr<SharedBuffer> PhysicalBoundaries::serialize() const
{
    SCIDB_ASSERT(_startCoords.size() == _endCoords.size());
    size_t const N_DIMS = _startCoords.size();

    size_t totalSize = sizeof(size_t) + sizeof(double)
        + 2 * N_DIMS * sizeof(Coordinate);

    MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);

    auto sizePtr = reinterpret_cast<size_t*>(buf->getWriteData());
    auto startingPoint = sizePtr;
    *sizePtr = _startCoords.size();
    sizePtr++;

    double* densityPtr = (double*) sizePtr;
    *densityPtr = _density;
    densityPtr++;

    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i = 0; i < N_DIMS; i++)
    {
        *coordPtr = _startCoords[i];
        coordPtr++;
    }

    for(size_t i = 0; i < N_DIMS; i++)
    {
        *coordPtr = _endCoords[i];
        coordPtr++;
    }

    SCIDB_ASSERT(reinterpret_cast<char*>(coordPtr) == reinterpret_cast<char*>(startingPoint) + buf->getSize());
    return std::shared_ptr<SharedBuffer> (buf);
}


PhysicalBoundaries PhysicalBoundaries::deSerialize(std::shared_ptr<SharedBuffer> const& buf)
{
    auto numCoordsPtr = reinterpret_cast<const size_t*>(buf->getConstData());
    size_t numCoords = *numCoordsPtr;
    numCoordsPtr++;

    auto densityPtr = reinterpret_cast<const double*>(numCoordsPtr);
    double density = *densityPtr;
    densityPtr++;

    Coordinates start, end;
    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i =0; i<numCoords; i++)
    {
        start.push_back(*coordPtr);
        coordPtr++;
    }

    for(size_t i =0; i<numCoords; i++)
    {
        end.push_back(*coordPtr);
        coordPtr++;
    }

    return PhysicalBoundaries(start,end,density);
}


uint32_t PhysicalBoundaries::getCellSizeBytes(const Attributes& attrs )
{
    uint32_t totalBitSize = 0;
    Config* cfg = Config::getInstance();

    for (const auto& attr : attrs)
    {
        Type cellType = TypeLibrary::getType(attr.getType());
        uint32_t bitSize = cellType.bitSize();

        if (bitSize == 0)
        {
            bitSize =  cfg->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION) * 8;
        }
        if (attr.isNullable())
        {
            bitSize += 1;
        }
        totalBitSize += bitSize;
    }
    return (totalBitSize + 7)/8;
}


double PhysicalBoundaries::getSizeEstimateBytes(const ArrayDesc& schema) const
{
    uint64_t numCells = getNumCells();
    size_t numDimensions = schema.getDimensions().size();
    size_t numAttributes = schema.getAttributes().size();

    size_t cellSize = getCellSizeBytes(schema.getAttributes());

    //we assume that every cell is part of sparse chunk
    cellSize += numAttributes * (numDimensions  * sizeof(Coordinate) + sizeof(int));
    double size = static_cast<double>(numCells) * static_cast<double>(cellSize) ;

    // Assume all chunks are sparse and add header (if we can).
    uint64_t numChunks = 0;
    try {
        numChunks = getNumChunks(schema.getDimensions());
    } catch (UnknownChunkIntervalException&) {
        numChunks = 0;
    }
    size += static_cast<double>(numChunks) *
        static_cast<double>(numAttributes);

    return size * _density;
}


void PhysicalBoundaries::updateFromChunk(ConstChunk const* chunk, bool chunkShapeOnly)
{
    size_t nDims = _startCoords.size();
    if (chunk == NULL)
    {   return; }

    //chunk iteration is expensive - only perform if needed
    Coordinates const& chunkFirstPos = chunk->getFirstPosition(false);
    Coordinates const& chunkLastPos = chunk->getLastPosition(false);
    bool updateLowBound = false, updateHiBound = false;
    for (size_t i = 0; i < nDims; i++)
    {
       if (chunkFirstPos[i] < _startCoords[i])
       {
           if ( chunkShapeOnly )
           {    _startCoords[i] = chunkFirstPos[i]; }
           else
           {   updateLowBound = true; }
       }

       if (chunkLastPos[i] > _endCoords[i])
       {
           if ( chunkShapeOnly)
           {    _endCoords[i] = chunkLastPos[i]; }
           else
           {    updateHiBound = true; }
       }
    }

    //The chunk is inside the box and cannot expand the bounds. Early exit.
    if (!updateLowBound && !updateHiBound) {
        return;
    }

    //TODO: there is a further optimization opportunity here. The given chunk *should* always be a bitmap
    //chunk. Once we verify that, we can iterate over bitmap segments and compute coordinates. Committing
    //this due to timing constraints - should revisit and optimize this when possible.
    std::shared_ptr<ConstChunkIterator> citer = chunk->materialize()->getConstIterator();
    while (!citer->end() && (updateLowBound || updateHiBound))
    {
        Coordinates const& pos = citer->getPosition();
        bool updated = false;
        for (size_t j = 0; j < nDims; j++)
        {
            if (updateHiBound && pos[j] > _endCoords[j])
            {
                _endCoords[j] = pos[j];
                updated = true;
            }
            if (updateLowBound && pos[j] < _startCoords[j])
            {
                _startCoords[j] = pos[j];
                updated = true;
            }
        }
        if(updated) //it's likely that no further update can come from this chunk
        {
            if(updateHiBound)
            {
                size_t k=0;
                while (k<nDims && _endCoords[k]>=chunkLastPos[k])
                { k++; }
                if (k==nDims) //no more useful data for hi bound could come from this chunk!
                {   updateHiBound=false; }
            }
            if(updateLowBound)
            {
                size_t k=0;
                while (k<nDims && _startCoords[k]<=chunkFirstPos[k])
                { k++; }
                if (k==nDims) //no more useful data for lo bound could come from this chunk!
                {   updateLowBound=false; }
            }
        }
        ++(*citer);
    }
}


void PhysicalBoundaries::updateFromRange(SpatialRange const& range)
{
    unionWith(PhysicalBoundaries(range._low, range._high));
}


PhysicalBoundaries PhysicalBoundaries::trimToDims(Dimensions const& dims) const
{
    SCIDB_ASSERT(_startCoords.size() == dims.size());
    size_t nDims = dims.size();

    Coordinates resStart(nDims);
    Coordinates resEnd(nDims);

    for(size_t i=0; i<nDims; i++)
    {
        resStart[i] = std::max<Coordinate> (dims[i].getStartMin(), _startCoords[i]);
        resEnd[i] = std::min<Coordinate> (dims[i].getEndMax(), _endCoords[i]);
    }

    return PhysicalBoundaries(resStart, resEnd, _density);
}


PhysicalBoundaries
PhysicalBoundaries::createFromChunkList(std::shared_ptr<Array>& inputArray,
                                        const std::set<Coordinates, CoordinatesLess>& chunkCoordinates)
{
    ASSERT_EXCEPTION((inputArray->getSupportedAccess() == Array::RANDOM),
                     "PhysicalBoundaries::createFromChunkList: ");

    const ArrayDesc& arrayDesc = inputArray->getArrayDesc();
    const size_t nDims = arrayDesc.getDimensions().size();

    if (chunkCoordinates.empty()) {
        return PhysicalBoundaries::createEmpty(nDims);
    }

    typedef set<Coordinates, CoordinatesLess> ChunkCoordinates;
    typedef std::unordered_set<const Coordinates*> CoordHashSet;
    CoordHashSet chunksToExamine;

    if (nDims>1) {

        typedef std::vector<std::pair<int64_t, std::deque<const Coordinates*> > > ChunkListByExtremeDim;
        ChunkListByExtremeDim chunksWithMinDim(nDims,
                                               make_pair(CoordinateBounds::getMax(),
                                                         std::deque<const Coordinates*>()));
        SCIDB_ASSERT(nDims == chunksWithMinDim.size());
        ChunkListByExtremeDim chunksWithMaxDim(nDims,
                                               make_pair(CoordinateBounds::getMin(),
                                                         std::deque<const Coordinates*>()));
        SCIDB_ASSERT(nDims == chunksWithMaxDim.size());

        // find all boundary chunks
        for (ChunkCoordinates::const_iterator iter = chunkCoordinates.begin();
             iter != chunkCoordinates.end(); ++iter) {
            const Coordinates& chunkCoords = *iter;
            SCIDB_ASSERT(chunkCoords.size() == nDims);
            for (size_t d=0; d < nDims; ++d) {
                pair<int64_t, std::deque<const Coordinates*> >& minChunks = chunksWithMinDim[d];
                if (chunkCoords[d] < minChunks.first) {
                    minChunks.second.clear();
                    minChunks.first = chunkCoords[d];
                    minChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == minChunks.first) {
                    minChunks.second.push_back(&chunkCoords);
                } else { }

                pair<int64_t, std::deque<const Coordinates*> >& maxChunks = chunksWithMaxDim[d];
                if (chunkCoords[d] > maxChunks.first) {
                    maxChunks.second.clear();
                    maxChunks.first = chunkCoords[d];
                    maxChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == maxChunks.first) {
                    maxChunks.second.push_back(&chunkCoords);
                } else { }
            }
        }

        // throw away duplicates
        for (size_t d=0; d < nDims; ++d) {
            std::deque<const Coordinates*>& minList = chunksWithMinDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = minList.begin();
                 iter != minList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }

            std::deque<const Coordinates*>& maxList = chunksWithMaxDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = maxList.begin();
                 iter != maxList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }
        }
    } else {
        SCIDB_ASSERT(nDims == 1);
        const Coordinates& firstCoord = *chunkCoordinates.begin();
        chunksToExamine.insert(&firstCoord);
        const Coordinates& lastCoord = *(--chunkCoordinates.end());
        chunksToExamine.insert(&lastCoord);
    }

    // update bounds using the boundary chunks
    PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);
    SCIDB_ASSERT(!arrayDesc.getAttributes().empty());
    const auto& lastAttribute = *(arrayDesc.getAttributes().end()-1);
    const bool isNoEmptyTag = arrayDesc.getAttributes().hasEmptyIndicator();
    std::shared_ptr<ConstArrayIterator> arrayIter = inputArray->getConstIterator(lastAttribute);
    for (CoordHashSet::const_iterator iter = chunksToExamine.begin();
         iter != chunksToExamine.end(); ++iter) {
        const Coordinates* chunkCoords = *iter;
        bool rc = arrayIter->setPosition(*chunkCoords);
        SCIDB_ASSERT(rc);
        const ConstChunk& chunk = arrayIter->getChunk();
        bounds.updateFromChunk(&chunk, isNoEmptyTag);
    }
    return bounds;
}

} //namespace
