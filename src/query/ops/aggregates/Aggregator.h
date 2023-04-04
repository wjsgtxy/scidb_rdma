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

/*
 * AggPartitioningOperator.h
 *
 *  Created on: Jul 25, 2011
 *      Author: poliocough@gmail.com, genius
 */

#ifndef _AGGREGATOR_H
#define _AGGREGATOR_H

#include <log4cxx/logger.h>
#include <unordered_map>

#include <query/PhysicalOperator.h>
#include <query/AutochunkFixer.h>
#include <util/arena/Vector.h>
#include <array/MemArray.h>
#include <array/TileIteratorAdaptors.h>
#include <query/Aggregate.h>
#include <array/DelegateArray.h>
#include <system/Sysinfo.h>

#include <array/PinBuffer.h>        // grr. due to implementation-in-header

/****************************************************************************/
namespace scidb {


////////////////////////////////////////////////////////////////////////////////

class FinalResultChunkIterator : public DelegateChunkIterator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    FinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg);
    virtual Value &getItem();
};

class FinalResultMapCreator : public DelegateChunkIterator, protected CoordinatesMapper
{
private:
    RLEEmptyBitmap _bm;
    RLEEmptyBitmap::iterator _iter;
    Value _boolValue;
    Coordinates _coords;

public:
    FinalResultMapCreator(DelegateChunk const* sourceChunk, int iterationMode);
    Value& getItem() override;
    bool isEmpty() ;

    bool end() override { return _iter.end(); }
    void operator ++() override { ++_iter; }

    Coordinates const& getPosition() override;
    bool setPosition(Coordinates const& pos);

    void restart() override { _iter.restart(); }
};

class EmptyFinalResultChunkIterator : public FinalResultMapCreator
{
private:
    AggregatePtr _agg;
    Value _outputValue;

public:
    EmptyFinalResultChunkIterator (DelegateChunk const* sourceChunk, int iterationMode, AggregatePtr const& agg);
    virtual Value &getItem();
};

class FinalResultArray : public DelegateArray
{
private:
    std::vector <AggregatePtr> _aggs;
    bool _createEmptyMap;
    AttributeID _emptyMapScapegoat;

public:
    FinalResultArray (ArrayDesc const& desc,
                      std::shared_ptr<Array> const& stateArray,
                      std::vector<AggregatePtr> const& aggs,
                      bool createEmptyMap = false);

    virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;
    DelegateArrayIterator* createArrayIterator(const AttributeDesc& attrID) const override;
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const;
};

struct AggregationFlags
{
    int               iterationMode;
    bool              countOnly;
    std::vector<uint8_t> shapeCountOverride;
    std::vector<uint8_t> nullBarrier;
};

/**
 * The aggregator computes a distributed aggregation to the input array, based
 * on several parameters. The pieces of the puzzle are:
 *  - one or more AGGREGATE_CALLs in the given parameters
 *  - input schema
 *  - output schema
 *  - the transformCoordinates() function
 */
// TODO: rename PhysicalAggregatePartitioningOperator
class AggregatePartitioningOperator : public PhysicalOperator
{
  protected:
     std::vector <AggIOMapping> _ioMappings;
     std::vector <AggregatePtr> _aggs;
     size_t              const  _inDims;
     size_t              const  _outDims;

  protected:
    struct hash_t
    {
                      hash_t(size_t n) : size(n)                               {}
        size_t        operator()(const Coordinate* p)                    const {return boost::hash_range(p,p + size);}
        bool          operator()(const Coordinate* p,const Coordinate* q)const {return std::equal(p,p+size,q);}
        size_t  const size; // The common coordinate vector length
    };

    using StateMap = std::unordered_map<
        const Coordinate*, Value*, hash_t,
        hash_t, arena::ScopedAllocatorAdaptor<std::pair<const Coordinate*, Value*>>>;

    StateMap newStateMap(arena::Arena& a);
    Coordinate* newOutCoords(arena::Arena& a);
    Value* newStateVector(arena::Arena& a, arena::count_t c);
    // TODO: explicit dtor call?  WTF?
    void destroyState(Value& v) { v.~Value(); }

  public:
    AggregatePartitioningOperator(const std::string& logicalName,
                                  const std::string& physicalName,
                                  const Parameters& parameters,
                                  const ArrayDesc& schema);

    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren) const override;
    DistType inferSynthesizedDistType(std::vector<DistType> const& inDist, size_t depth) const override;
    virtual DistributionRequirement
            getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const override;
    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const&,
            std::vector<ArrayDesc> const&) const ;
    bool isOkForOutput(DistType distType) const ;

    virtual void initializeOperator(ArrayDesc const& inputSchema);
    virtual void transformCoordinates(CoordinateCRange,CoordinateRange) = 0;
    ArrayDesc createStateDesc();
    void initializeOutput(std::shared_ptr<ArrayIterator>& stateArrayIterator,
                          std::shared_ptr<ChunkIterator>& stateChunkIterator,
                          Coordinates const& outPos);
    void setOutputPosition(std::shared_ptr<ArrayIterator>& stateArrayIterator,
                           std::shared_ptr<ChunkIterator>& stateChunkIterator,
                           Coordinates const& outPos);
    AggregationFlags composeFlags(std::shared_ptr<Array> const& inputArray, AggIOMapping const& mapping);
    AggregationFlags composeGroupedFlags(std::shared_ptr<Array> const& inputArray, AggIOMapping const& mapping);
    void grandCount(Array* stateArray,
                    std::shared_ptr<Array> & inputArray,
                    AggIOMapping const& mapping,
                    AggregationFlags const& aggFlags);
    void grandTileAggregate(Array* stateArray,
                            std::shared_ptr<Array> & inputArray,
                            AggIOMapping const& mapping,
                            AggregationFlags const& aggFlags);

    /**
     *  Search within the interval [start, end) of the given range of points
     *  for the first entry that doesn't match *i .
     */
    size_t findEndOfRun(PointerRange<Coordinate* const> cv,size_t i);

    /**
     * For each position in tile, compute corresponding output coordinates.
     */
    void computeOutputCoordinates(
            std::shared_ptr<BaseTile> const& tile,
            PointerRange<Coordinate* const>     range);

    void groupedTileFixedSizeAggregate(
        Array* stateArray,
        std::shared_ptr<Array> & inputArray,
        AggIOMapping const& mapping,
        AggregationFlags const& aggFlags,
        size_t attSize);

    void grandAggregate(Array* stateArray,
                        std::shared_ptr<Array> & inputArray,
                        AggIOMapping const& mapping,
                        AggregationFlags const& aggFlags);

    void groupedAggregate(Array* stateArray,
                          std::shared_ptr<Array> & inputArray,
                          AggIOMapping const& mapping,
                          AggregationFlags const& aggFlags);

    void logMapping(AggIOMapping const& mapping, AggregationFlags const& flags);

    std::shared_ptr<Array>
    execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query);
};

} // namespace

#endif /* include guard */
