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

#include "IndexLookupSettings.h"
#include <query/PhysicalOperator.h>
#include <array/DelegateArray.h>
#include <array/SortArray.h>
#include <util/arena/Vector.h>
#include <util/Arena.h>
#include <query/AttributeComparator.h>
#include <array/ProjectArray.h>
#include "../sort/DistributedSort.h"

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.toy_operators.index_lookup"));
using arena::ArenaPtr;

/**
 * @par Algorithm:
 * <br>
 * <br>
 * It is assumed that the index_array (second argument) is small enough to fit entirely on disk on any of the instances.
 * Our first step is to call redistribute() to make a copy of the index on every instance.
 *
 * We then create a sorted vector that contains some of the values from the index_array. The vector contains some of the
 * values from the index and their corresponding coordinate. The vector always contains the first and the last value of
 * each chunk. In addition, we insert a random sampling of values from the index into the vector, not to exceed
 * MEMORY_LIMIT bytes of memory used. The vector is ordered based on the "<" comparison operator for the particular
 * datatype, allowing for binary search.
 *
 * Having built the vector, we create a virtual array that is computed as it is iterated over. Every time the client
 * requests for data from the output attribute, we first obtain the corresponding value from the input attribute. We
 * try to find the matching value in the vector. If not successful, we find the position of the next largest value and
 * the next smallest value in the vector. We use those coordinates to select a chunk in the index array. We then use
 * binary search over the chunk to find the value.
 *
 * @author apoliakov@paradigm4.com
 */
class PhysicalIndexLookup : public PhysicalOperator
{
public:
    PhysicalIndexLookup(string const& logicalName,
                        string const& physicalName,
                        Parameters const& parameters,
                        ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    bool isSingleThreaded() const override
    {
        return false;
    }

private:
    template<class T>
    using Vector = std::vector<T, arena::ScopedAllocatorAdaptor<T>>;

    /**
     * A (sorted) vector of values that can be used to look up their coordinates with binary search.
     */
    class LookupVector
    {
    private:
        /**
         * To create this vector, the index array is first sorted. We record the coordinates of the values in the
         * sorted array (so we our client can find them) and in the input (so we can return them as part of the lookup).
         * However sometimes the index array is sorted when passed to the operator. In that case, we don't need the
         * extra indirection layer.
         */
        Vector<Value> _values;
        Vector<Coordinate> _positionsInOriginalArray;
        Vector<Coordinate> _positionsInSortedArray;   //NOT used if index array was initially sorted
        AttributeComparator _lessThan;
        bool const _indexPreSortedAndNotNullable;

    public:
        LookupVector(TypeId const& tid, size_t initialSize, bool indexPreSortedAndNotNullable, ArenaPtr const& arena):
            _values(arena::Allocator<Value>(arena)),
            _positionsInOriginalArray(arena::Allocator<Coordinate>(arena)),
            _positionsInSortedArray(arena::Allocator<Coordinate>(arena)),
            _lessThan(tid),
            _indexPreSortedAndNotNullable(indexPreSortedAndNotNullable)
        {
            _values.reserve(initialSize);
            _positionsInOriginalArray.reserve(initialSize);
            if(_indexPreSortedAndNotNullable)
            {
                return;
            }
            _positionsInSortedArray.reserve(initialSize);
        }

        /**
         * Add v and its positions to the vector. Must be called in sorted order but not enforced!
         */
        void addElement(Value const& v,
                        Coordinate const positionInOriginalArray,
                        Coordinate const positionInSortedArray)
        {
            _values.push_back(v);
            _positionsInOriginalArray.push_back(positionInOriginalArray);
            if(_indexPreSortedAndNotNullable)
            {
                return;
            }
            _positionsInSortedArray.push_back(positionInSortedArray);
        }

        /**
         * Find an element in the vector, or find the coordinates of two elements it could be between.
         * If found - return true and set lb = ub = positionInOriginalArray.
         * If not found but out of range, return false set lb to previous positionInSortedArray, ub to next
         * positionInSortedArray. If out of range, return false, set lb to 0, set ub to -1
         * @param v the value to find
         * @param[out] lb the lower bound coordinate placeholder
         * @param[out] ub the upper bound coordinate placeholder
         * @return true if an exact match for v is found, false otherwise.
         */
        bool findElement( Value const& v, Coordinate& lb, Coordinate& ub) const
        {
#           define VALUE_OUT_OF_RANGE lb = 0; ub = -1; return false;
            Vector<Value>::const_iterator iter;
            iter = std::lower_bound(_values.begin(), _values.end(), v, _lessThan);
            if (iter == _values.end())
            {
                VALUE_OUT_OF_RANGE;
            }
            size_t index = iter - _values.begin();
            if (*iter == v)
            {
                lb = _positionsInOriginalArray[index];
                ub = _positionsInOriginalArray[index];
                return true;
            }
            if (iter == _values.begin())
            {
                VALUE_OUT_OF_RANGE;
            }
            if (_indexPreSortedAndNotNullable)
            {
                ub = _positionsInOriginalArray[index];
                lb = _positionsInOriginalArray[index-1];
            }
            else
            {
                ub = _positionsInSortedArray[index];
                lb = _positionsInSortedArray[index-1];
            }
            return false;
#           undef VALUE_OUT_OF_RANGE
        }
    };

    /**
     * An object that contains a pointer to the LookupVector and a pointer to the index array, and can be used
     * to look up the coordinate of a particular value.
     */
    class ValueIndex
    {
    private:
        std::shared_ptr<Array> _indexArray;
        AttributeComparator _lessThan;
        //Important: the map stays constant throughout the process and the ValueIndex may not mutate it.
        std::shared_ptr<LookupVector const> _lookupVector;
        std::shared_ptr<ConstArrayIterator> _valueArrayIter;
        std::shared_ptr<ConstChunkIterator> _valueChunkIter;
        Coordinates _currentChunkPosition; //the position of the currently opened chunk
        std::shared_ptr<ConstArrayIterator> _positionArrayIter;
        std::shared_ptr<ConstChunkIterator> _positionChunkIter; //we keep one chunk open at any particular time to save RAM
        bool const _indexPreSortedAndNotNullable;

        //move our iterators to a new chunk position; close current chunk if any
        void repositionIterators(Coordinates const& desiredChunkPos)
        {
            _valueChunkIter.reset();
            _valueArrayIter->setPosition(desiredChunkPos);
            _valueChunkIter = _valueArrayIter->getChunk().getConstIterator();
            _currentChunkPosition = desiredChunkPos;
            if (_indexPreSortedAndNotNullable)
            {
                return;
            }
            _positionChunkIter.reset();
            _positionArrayIter->setPosition(desiredChunkPos);
            _positionChunkIter = _positionArrayIter->getChunk().getConstIterator();
        }

        /**
         * Find the position of input in _indexArray, searching between the coordinates start and end. The
         * coordinates start and end must be in the same chunk.
         * @param input the value to look for
         * @param start the starting coordinate in the array at which to look for
         * @param end the ending coordinate in the array
         * @param[out] result set to the position of input if it is found
         * @return true if input was found, false otherwise
         */
        bool findPositionInArray(Value const&input, Coordinate start, Coordinate end, Coordinate& result)
        {
            //convert start to the chunk position and reposition the iterator if necessary
            Coordinates chunkPos(1,start);
            _indexArray->getArrayDesc().getChunkPositionFor(chunkPos);
            if (_currentChunkPosition.size() == 0 || _currentChunkPosition[0] != chunkPos[0])
            {
                repositionIterators(chunkPos);
            }
            while (start < end) //binary search
            {
                //XXX: must code binary search myself because chunk iterators are not STL-compatible
                Coordinates midPoint (1, (start+end) / 2);
                _valueChunkIter->setPosition(midPoint);
                Value const& item = _valueChunkIter->getItem();
                if (item == input)
                {
                    if(_indexPreSortedAndNotNullable)
                    {
                        result = midPoint[0];
                    }
                    else
                    {
                        _positionChunkIter->setPosition(midPoint);
                        result = _positionChunkIter->getItem().getInt64();
                    }
                    return true;
                }
                else if(_lessThan(input, item)) //input < item
                {
                    end = midPoint[0];
                }
                else //input > item
                {
                    start = midPoint[0] + 1;
                }
            }
            return false;
        }

    public:
        ValueIndex(std::shared_ptr<Array> const& indexArray, std::shared_ptr<LookupVector const> const& partialVector,
                   bool indexPreSortedAndNotNullable):
            _indexArray(indexArray),
            _lessThan(),
            _lookupVector(partialVector),
            _valueArrayIter(),
            _positionArrayIter(),
            _indexPreSortedAndNotNullable(indexPreSortedAndNotNullable)
        {
            auto attrIter = indexArray->getArrayDesc().getAttributes(true).begin();
            _lessThan = attrIter->getType();
            _valueArrayIter = indexArray->getConstIterator(*attrIter);
            _positionArrayIter = indexArray->getConstIterator(*(++attrIter));
        }

        /**
         * Find the position of input in the index, first looking at the vector, then at the array chunks.
         * @param input the value to look for
         * @param[out] result set to the position of input if found
         * @return true if the value was found, false otherwise
         */
        bool findPosition(Value const& input, Coordinate& result)
        {
            Coordinate lb, ub;
            bool ret = _lookupVector->findElement(input,lb,ub);
            if (ret)
            {
                result = lb;
                return true;
            }
            if (ub < lb)
            {
                return false;
            }
            return findPositionInArray(input, lb, ub, result);
        }
    };

    /**
     * A special ChunkIterator used to lookup the coordinates of values.
     * The DelegateArray class, with its friends DelegateArrayIterator and DelegateChunkIterator provide facilities
     * for returning a slightly modified version of the input array in a streaming on-demand fashion. The returned data
     * is not materialized until it is requested by the client of the array.
     */
    class IndexLookupChunkIterator : public DelegateChunkIterator
    {
    private:
        /**
         * The index object. It is important to note that multiple threads may create multiple iterators to the same
         * array, which is why this cannot be a pointer to a shared object. All indeces however do contain a pointer to
         * the same shared map and are very cerful not to alter it.
         */
        ValueIndex _index;

        /**
         * A placeholder for the returned value.
         */
        Value _buffer;

    public:
        IndexLookupChunkIterator(DelegateChunk const* chunk,
                                 int iterationMode,
                                 std::shared_ptr<Array> const& indexArray,
                                 std::shared_ptr<LookupVector const> const& partialMap,
                                 bool indexPreSortedAndNotNullable):
            DelegateChunkIterator(chunk, iterationMode),
            _index(indexArray, partialMap, indexPreSortedAndNotNullable)
        {}

        virtual Value& getItem()
        {
            //The inputIterator is constructed by the DelegateChunkIterator and happens to be an iterator to the
            //corresponding chunk of the input attribute.
            Value const& input = inputIterator->getItem();
            Coordinate output;
            //Perform the index lookup
            if (!input.isNull() && _index.findPosition(input, output))
            {
                _buffer.setInt64(output);
            }
            else
            {
                _buffer.setNull();
            }
            return _buffer;
        }

        //Note: all of the other ConstChunkIterator methods - getPosition, setPosition, end, ... do not need to be
        //overwritten for this case
    };

    /**
     * The virtual array that simply returns the underlying iterators to all the data, unless the client asks for
     * the new index attribute, in which case the IndexLookupChunkIterator is returned.
     */
    class IndexLookupArray : public DelegateArray
    {
    private:
        /**
         * The id of the looked-up attribute.
         */
        AttributeID const _sourceAttributeId;

        /**
         * The id of the output attribute that contains the looked-up coordinates.
         */
        AttributeID const _dstAttributeId;

        /**
         * A pointer to the index array.
         */
        std::shared_ptr<Array>const _indexArray;

        /**
         * A pointer to the partial map.
         */
        std::shared_ptr<LookupVector const> const _partialMap;

        /**
         * True if the index array was pre-sorted. False otherwise.
         */
        bool const _indexPreSortedAndNotNullable;

    public:
        IndexLookupArray(ArrayDesc const& desc,
                         std::shared_ptr<Array>& input,
                         AttributeID const sourceAttribute,
                         std::shared_ptr<Array> indexArray,
                         std::shared_ptr<LookupVector const> partialMap,
                         bool indexPreSortedAndNotNullable):
            DelegateArray(desc, input, true),
            _sourceAttributeId(sourceAttribute),
            _dstAttributeId(safe_static_cast<AttributeID>(desc.getAttributes(true).size() -1)),
            _indexArray(indexArray),
            _partialMap(partialMap),
            _indexPreSortedAndNotNullable(indexPreSortedAndNotNullable)
        {}

        virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
        {
            if (id == _dstAttributeId)
            {   //pass "false" to the "clone" field indicating that this chunk is NOT a copy of the underlying chunk
                return new DelegateChunk(*this, *iterator, id, false);
            }
            return DelegateArray::createChunk(iterator, id);
        }

        DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override
        {
            if (id.getId() == _dstAttributeId)
            {
                //pass an iterator to the source attribute so the chunk iterator can have access to the looked up data
                const auto& attr = getPipe(0)->getArrayDesc().getAttributes().findattr(_sourceAttributeId);
                return new DelegateArrayIterator(*this, id, getPipe(0)->getConstIterator(attr));
            }
            else if (id.getId() == _dstAttributeId+1)
            {
                //client must be asking for the empty tag, whose id is now shifted up by one
                auto attrToTheLeft = getPipe(0)->getArrayDesc().getAttributes().find(id);
                --attrToTheLeft;
                return new DelegateArrayIterator(*this, id, getPipe(0)->getConstIterator(*attrToTheLeft));
            }
            return DelegateArray::createArrayIterator(id);
        }

        virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
        {
            if (chunk->getAttributeDesc().getId() == _dstAttributeId)
            {
                return new IndexLookupChunkIterator(chunk, iterationMode, _indexArray, _partialMap, _indexPreSortedAndNotNullable);
            }
            return DelegateArray::createChunkIterator(chunk, iterationMode);
        }
    };

    /**
     * A very simple array that just adds the inputs (only) dimension as a new attribute. Used so that we can record
     * the original position of cells in the index array as we sort it.
     */
    class AddDimensionArray : public DelegateArray
    {
    private:
        class AddDimensionChunkIterator : public DelegateChunkIterator
        {
        private:
            Value _buffer;

        public:
            AddDimensionChunkIterator(DelegateChunk const* chunk,
                                     int iterationMode):
                DelegateChunkIterator(chunk, iterationMode)
            {}

            virtual Value& getItem()
            {
                _buffer.setInt64 ( inputIterator->getPosition()[0] );
                return _buffer;
            }
        };

        static ArrayDesc createDescriptor( ArrayDesc const& inputDesc)
        {
            //assert-like checks:
            if( inputDesc.getAttributes(true).size() != 1 ||
                inputDesc.getDimensions().size() != 1)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
            }
            Attributes newAttributes = inputDesc.getAttributes(true);
            newAttributes.push_back(AttributeDesc(" ",
                                                  TID_INT64,
                                                  0,
                                                  CompressorType::NONE));
            // ArrayDesc consumes the new copy, source is discarded.
            return ArrayDesc(inputDesc.getName(),
                             newAttributes.addEmptyTagAttribute(),
                             inputDesc.getDimensions(),
                             inputDesc.getDistribution(),
                             inputDesc.getResidency());

        }

    public:
        AddDimensionArray(std::shared_ptr<Array>& input):
            DelegateArray(createDescriptor(input->getArrayDesc()), input, true)
        {}

        virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
        {
            if (id == 1)
            {
                //pass "false" to the "clone" field indicating that this chunk is NOT a copy of the underlying chunk
                return new DelegateChunk(*this, *iterator, id, false);
            }
            return DelegateArray::createChunk(iterator, id);
        }

        DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override
        {
            if (id.getId() == 2)
            {
                //client must be asking for the empty tag, whose id is now shifted up by one
                auto attrToTheLeft = getPipe(0)->getArrayDesc().getAttributes().find(id);
                --attrToTheLeft;
                return new DelegateArrayIterator(*this, id, getPipe(0)->getConstIterator(*attrToTheLeft));
            }
            return DelegateArray::createArrayIterator(id);
        }

        virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
        {
            if (chunk->getAttributeDesc().getId() == 1)
            {
                return new AddDimensionChunkIterator(chunk, iterationMode);
            }
            return DelegateArray::createChunkIterator(chunk, iterationMode);
        }
    };

    /**
     * A guide to tell us how many values may be placed into the lookup vector.
     */
    struct MemoryLimits
    {
        /**
         * The probability that any value is inserted (the fraction of the total number of values), between 0 and 1.
         */
        double insertionProbability;

        /**
         * The upper limit on the number of inserted values, in addition to the required two values for each chunk.
         */
        size_t numOptionalValues;

        /**
         * The number of chunks in the array
         */
        size_t chunkCount;

        MemoryLimits():
            insertionProbability(0),
            numOptionalValues(0),
            chunkCount(0)
        {}
    };

    /**
     * Compute a MemoryLimits object based on the memory limit and some information about index data.
     */
    MemoryLimits computeVectorLimits(std::shared_ptr<Array>& indexArray, double memLimit, bool indexPreSortedAndNotNullable)
    {
        MemoryLimits result;
        const auto& fda = indexArray->getArrayDesc().getAttributes().firstDataAttribute();
        size_t declaredValueSize = fda.getSize(); //0 means variable size
        bool isIntegralType = (declaredValueSize > 0 && declaredValueSize<=8);
        ssize_t cellCount = 0, totalSize = 0;
        for(std::shared_ptr<ConstArrayIterator> indexArrayIter = indexArray->getConstIterator(fda);
            !indexArrayIter->end();
            ++(*indexArrayIter))
        {
            //just iterate over the chunks
            ++result.chunkCount;
            ConstChunk const& chunk = indexArrayIter->getChunk();
            //we know the array has been distributed, therefore it is very likely a MemArray, in which case the methods
            //count and getSize run in constant time.
            cellCount += chunk.count();
            totalSize += chunk.getSize();
        }
        double averageValueSize = static_cast<double>(totalSize) / static_cast<double>(cellCount);
        double averageVectorMemberSize;
        size_t numCoordinatesNeeded = indexPreSortedAndNotNullable ? 1 : 2;
        if (isIntegralType) //if it is fixed-size and under 8 bytes, it is stored inside the Value class
        {
            averageVectorMemberSize = static_cast<double>(
                numCoordinatesNeeded * sizeof(Coordinate) + sizeof(Value));
        }
        else //otherwise Value points to it
        {
            //averageValueSize includes some chunk overhead, so it is a slight over-estimate; err on the side of caution
            averageVectorMemberSize = static_cast<double>(
                numCoordinatesNeeded * sizeof(Coordinate) + sizeof(Value)) + averageValueSize;
        }
        ssize_t valuesThatFitInLimit = static_cast<ssize_t>(floor( memLimit / averageVectorMemberSize));
        valuesThatFitInLimit -= (2* result.chunkCount);
        if (valuesThatFitInLimit <= 0)
        {}
        else if (valuesThatFitInLimit > cellCount) //the good case
        {
            result.insertionProbability = 1.0;
            result.numOptionalValues = valuesThatFitInLimit;
        }
        else
        {
            result.insertionProbability =
                static_cast<double>(valuesThatFitInLimit) / static_cast<double>(cellCount);
            result.numOptionalValues = valuesThatFitInLimit;
        }
        LOG4CXX_DEBUG(logger, "Vector Limits: cellCount "<<cellCount
                              <<" chunkCount "<<result.chunkCount
                              <<" compulsory values "<<result.chunkCount * 2
                              <<" avgValueSize "<<averageValueSize
                              <<" avgMapMemberSize "<<averageVectorMemberSize
                              <<" optValuesLimit "<<result.numOptionalValues
                              <<" insertionProb "<<result.insertionProbability);
        return result;
    }

    /**
     * Scan the data from the index array and insert a portion of it into the vector.
     */
    std::shared_ptr<LookupVector const> buildLookupVector(std::shared_ptr<Array>& indexArray, MemoryLimits const& limits,
                                                     bool indexPreSortedAndNotNullable, const std::shared_ptr<Query>& query)
    {
        size_t mapSize = limits.numOptionalValues + 2 * limits.chunkCount;
        auto attrIter = indexArray->getArrayDesc().getAttributes(true).begin();
        std::shared_ptr<LookupVector> result = std::make_shared<LookupVector>(
                                     attrIter->getType(), mapSize, indexPreSortedAndNotNullable,
                                     query->getArena());
        size_t optionalValuesInserted = 0;
        std::shared_ptr<ConstArrayIterator> valueArrayIter = indexArray->getConstIterator(*attrIter);
        //note: if indexPreSortedAndNotNullable is true, this is just an iterator over the empty tag; harmless
        std::shared_ptr<ConstArrayIterator> positionArrayIter = indexArray->getConstIterator(*(++attrIter));
        Value indexValueToAdd;
        Coordinate positionInSortedArray(-1), positionInOriginalArray(-1);
        bool currentValIsAdded = true, newChunk = true;
        while (!valueArrayIter->end())
        {
            newChunk = true;
            std::shared_ptr<ConstChunkIterator> valueChunkIter = valueArrayIter->getChunk().getConstIterator();
            std::shared_ptr<ConstChunkIterator> positionChunkIter = positionArrayIter->getChunk().getConstIterator();
            while (!valueChunkIter->end())
            {
                if (newChunk && !currentValIsAdded)
                {   //add the last element of the prev chunk
                    result->addElement(indexValueToAdd, positionInOriginalArray, positionInSortedArray);
                }
                indexValueToAdd = valueChunkIter->getItem();
                positionInSortedArray = valueChunkIter->getPosition()[0];
                if(indexPreSortedAndNotNullable)
                {
                    positionInOriginalArray = positionInSortedArray;
                }
                else
                {
                    positionInOriginalArray = positionChunkIter->getItem().getInt64();
                }
                if(newChunk)
                {   //add the first element of this chunk
                    result->addElement(indexValueToAdd, positionInOriginalArray, positionInSortedArray);
                    currentValIsAdded = true;
                }
                else if (limits.insertionProbability >= 1.0 ||
                         (optionalValuesInserted < limits.numOptionalValues &&
                         (rand() / (RAND_MAX + 1.0)) < limits.insertionProbability))
                {   //add randomly chosen elements if limits allow
                    result->addElement(indexValueToAdd, positionInOriginalArray, positionInSortedArray);
                    ++ optionalValuesInserted;
                    currentValIsAdded = true;
                }
                else
                {
                    currentValIsAdded = false;
                }
                newChunk = false;
                ++(*valueChunkIter);
                ++(*positionChunkIter);
            }
            ++(*valueArrayIter);
            ++(*positionArrayIter);
        }
        if (!currentValIsAdded)
        {   //add the last element in the array if we haven't already
            result->addElement(indexValueToAdd, positionInOriginalArray, positionInSortedArray);
        }
        LOG4CXX_DEBUG(logger, "Lookup vector built. Inserted "<<optionalValuesInserted<<" optional values");
        return result;
    }

    std::shared_ptr<Array> prepareIndexArray(std::shared_ptr<Array> & inputIndex,
                                             std::shared_ptr<Query>& query,
                                             const std::shared_ptr<PhysicalOperator>& phyOp,
                                             bool const indexPreSortedAndNotNullable)
    {
        // Add an int64-typed attribute whose value is equal to the dimension coordinate.
        std::shared_ptr<Array> dimApplied = std::make_shared<AddDimensionArray>(inputIndex);

        // Sort the array, unless it was pre-sorted AND the attribute is not nullable.
        if(! indexPreSortedAndNotNullable)
        {
            LOG4CXX_TRACE(logger, "PhysicalIndexLookup::prepareIndexArray local sort skipping nulls");
            // Local sort, skipping nulls.
            ArrayDesc const& schemaDimApplied = dimApplied->getArrayDesc();

            SortingAttributeInfos sortingAttributeInfos(3);
            sortingAttributeInfos[0].columnNo = 0; // the original attribute
            sortingAttributeInfos[0].ascent = true;
            sortingAttributeInfos[1].columnNo = 2; // chunk_pos
            sortingAttributeInfos[1].ascent = true;
            sortingAttributeInfos[2].columnNo = 3; // cell_pos
            sortingAttributeInfos[2].ascent = true;

            SortArray sorter(schemaDimApplied, _arena);
            sorter.setPreservePositions(true) // expanding the schema with chunk_pos and cell_pos attributes.
                  .setChunkSize(schemaDimApplied.getDimensions()[0].getChunkInterval());

            ArrayDesc const& sorterOutputSchemaExpanded = sorter.getExpandedOutputSchema();
            ArrayDesc const& sorterOutputSchemaNonExpanded = sorter.getOutputSchema();
            std::shared_ptr<TupleComparator> tcomp = std::make_shared<TupleComparator>(
                    sortingAttributeInfos, sorterOutputSchemaExpanded);
            TupleSkipper tupleSkipper(tupleSkipperIfAnyAttributeIsMissing, &sortingAttributeInfos);

            std::shared_ptr<MemArray> sorted = sorter.getSortedArray(dimApplied, query, phyOp, tcomp, &tupleSkipper);

            // Global sort.
            if (query->getInstancesCount() > 1) {
                ElapsedMilliSeconds timing;
                DistributedSort ds(query, phyOp, sorted, sorterOutputSchemaExpanded, _arena, sortingAttributeInfos, timing);
                sorted = ds.sort();
            }

            // Project off the chunk_pos and cell_pos attributes.
            assert(sorted->getArrayDesc().getAttributes().size()==5); // input attribute, dim, chunk_pos, cell_pos, bitmap.
            vector<AttributeID> projection(3);
            projection[0] = 0;  // the input attribute.
            projection[1] = 1;  // dim
            projection[2] = 4;  // the bitmap.
            dimApplied = std::make_shared<ProjectArray>(sorterOutputSchemaNonExpanded, sorted, projection);
        }

        // Ensure replicated
        bool isReplicated = dimApplied->getArrayDesc().getDistribution()->checkCompatibility(createDistribution(dtReplication));
        shared_ptr<Array> rightArray;
        if (!isReplicated) {
            LOG4CXX_TRACE(logger, "PhysicalIndexLookup::prepareIndexArray redistributing RHS");
            rightArray = redistributeToRandomAccess(dimApplied,
                                                    createDistribution(dtReplication),
                                                    ArrayResPtr(),
                                                    query,
                                                    shared_from_this());
        } else {
            // already replicated, ensure RandomAccess
            LOG4CXX_TRACE(logger, "PhysicalIndexLookup::prepareIndexArray RHS already replicated, ensuring random");
            rightArray = PhysicalOperator::ensureRandomAccess(dimApplied, query);
        }
        return rightArray;
    }

public:
    /**
     * @see PhysicalOperator::getOutputBoundaries
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
       return inputBoundaries[0];
    }

    // required to allow replicated input
    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        vector<uint8_t> result(numChildren, false);
        SCIDB_ASSERT(numChildren == 2);
        result[1] = true;   // permitted on the right-hand input
        return result;
    }

    /// see OperatorDist
    void checkInputDistAgreement(std::vector<DistType> const& inputDistrib, size_t /*depth*/) const
    {
        SCIDB_ASSERT(inputDistrib.size() == 2);
        LOG4CXX_DEBUG(logger, "PhysicalIndexLookup::checkInputDistAgreement: no agreement required");
        // no restriction on inputDistrib values
    }

    /**
     * @see PhysicalOperator::execute
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        ArrayDesc const& inputSchema = inputArrays[0]->getArrayDesc();
        ArrayDesc const& indexSchema = inputArrays[1]->getArrayDesc();
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(inputSchema.getDistribution()));
        SCIDB_ASSERT(_schema.getResidency()->isEqual(inputSchema.getResidency()));

        IndexLookupSettings settings(inputSchema, indexSchema, _parameters, _kwParameters, false, query);
        bool indexPreSortedAndNotNullable = settings.isIndexPreSorted() &&
                (indexSchema.getAttributes().firstDataAttribute().getFlags() & AttributeDesc::IS_NULLABLE) == 0;
        std::shared_ptr<Array> preparedIndex =
            prepareIndexArray(inputArrays[1], query, shared_from_this(), indexPreSortedAndNotNullable);
        MemoryLimits vectorLimits = computeVectorLimits(
            preparedIndex,
            static_cast<double>(settings.getMemoryLimit()),
            indexPreSortedAndNotNullable);
        std::shared_ptr<LookupVector const> partialVector = buildLookupVector(preparedIndex, vectorLimits, indexPreSortedAndNotNullable, query);
        return std::shared_ptr<Array>(new IndexLookupArray(_schema, inputArrays[0], settings.getInputAttributeId(),
                                                           preparedIndex, partialVector, indexPreSortedAndNotNullable));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalIndexLookup, "index_lookup", "PhysicalIndexLookup")

} //namespace scidb
