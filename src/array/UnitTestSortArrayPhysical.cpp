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
#include <array/MemArray.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>  // for SortingAttributeInfos;
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/Exceptions.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestSortArrayPhysical: public PhysicalOperator
{
    typedef map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
public:

    UnitTestSortArrayPhysical(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
    }

    bool isSingleThreaded() const override
    {
        return false;
    }

    /**
     * Generate a random value.
     * The function should be extended to cover all types and all special values such as NaN, and then be moved to a public header file.
     * @param[in]    type        the type of the value
     * @param[inout] value       the value to be filled
     * @param[in]    percentNull a number from 0 to 100, where 0 means never generate null, and 100 means always generate null
     * @return       the value from the parameter
     */
    Value& genRandomValue(TypeId const& type, Value& value, int percentNull, Value::reason nullReason)
    {
        assert(percentNull>=0 && percentNull<=100);

        if (percentNull>0 && rand()%100<percentNull) {
            value.setNull(nullReason);
        } else if (type==TID_INT64) {
            value.setInt64(rand());
        } else if (type==TID_BOOL) {
            value.setBool(rand()%100<50);
        } else if (type==TID_STRING) {
            vector<char> str;
            const size_t maxLength = 300;
            const size_t minLength = 1;
            assert(minLength>0);
            size_t length = rand()%(maxLength-minLength) + minLength;
            str.resize(length + 1);
            for (size_t i=0; i<length; ++i) {
                int c;
                do {
                    c = rand()%128;
                } while (! isalnum(c));
                str[i] = (char)c;
            }
            str[length-1] = 0;
            value.setString(&str[0]);
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestSortArrayPhysical" << "genRandomValue";
        }
        return value;
    }

    /**
     * Given a value, return a human-readable string for its value.
     * @note This should eventually be factored out to the include/ directory.
     * @see ArrayWriter
     */
    string valueToString(Value const& value, TypeId const& type)
    {
        std::stringstream ss;

        if (value.isNull()) {
            ss << "?(" << value.getMissingReason() << ")";
        } else if (type==TID_INT64) {
            ss << value.getInt64();
        } else if (type==TID_BOOL) {
            ss << (value.getBool() ? "true" : "false");
        } else if (type==TID_STRING) {
            ss << value.getString();
        } else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestSortArrayPhysical" << "value2string";
        }
        return ss.str();
    }

    /**
     * Insert data from a map to an array.
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    m      the map of Coordinate --> Value
     */
    void insertMapDataIntoArray(std::shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m)
    {
        Coordinates coord(1);
        coord[0] = 0;
        const auto& arrayAttrs = array.getArrayDesc().getAttributes(true);
        vector< std::shared_ptr<ArrayIterator> > arrayIters(arrayAttrs.size());
        vector< std::shared_ptr<ChunkIterator> > chunkIters(arrayAttrs.size());

        for (const auto& attr : arrayAttrs)
        {
            arrayIters[attr.getId()] = array.getIterator(attr);
            chunkIters[attr.getId()] =
                ((MemChunk&)arrayIters[attr.getId()]->newChunk(coord)).getIterator(query,
                                                                                   ChunkIterator::SEQUENTIAL_WRITE);
        }

        for (auto const& p : m) {
            coord[0] = p.first;
            for (const auto& attr : arrayAttrs)
            {
                if (arrayIters[attr.getId()] && !chunkIters[attr.getId()]->setPosition(coord))
                {
                    chunkIters[attr.getId()]->flush();
                    chunkIters[attr.getId()].reset();
                    chunkIters[attr.getId()] =
                        ((MemChunk&)arrayIters[attr.getId()]->newChunk(coord)).getIterator(query,
                                                                                ChunkIterator::SEQUENTIAL_WRITE);
                    chunkIters[attr.getId()]->setPosition(coord);
                }
                chunkIters[attr.getId()]->writeItem(p.second);
            }
        }

        for (const auto& attr : arrayAttrs)
        {
            chunkIters[attr.getId()]->flush();
        }
    }

    /**
     * Test sort array once.
     * The method generates a large 1-d array of random values.
     * It then tries to sort the array by the first attributes.
     * For each cell, there is 0% possibility that it is empty.
     * For each value, there is 0% possibility that it is null.
     * For each cell, the value in each attribute is the same
     * so that we can be sure the sort does not scramble cells.
     *
     * @param[in]   query
     * @param[in]   type     the value type
     * @param[in]   start    the start coordinate of the dim
     * @param[in]   end      the end coordinate of the dim
     * @param[in]   chunkInterval  the chunk interval
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void testOnce_SortArray(std::shared_ptr<Query>& query,
                            const std::shared_ptr<PhysicalOperator>& phyOp,
                            TypeId const& type,
                            Coordinate start,
                            Coordinate end,
                            size_t nattrs,
                            bool ascent,
                            uint32_t chunkInterval)
    {
        const int percentEmpty = 20;
        const int percentNullValue = 10;
        const int missingReason = 0;

        LOG4CXX_DEBUG(logger, "SortArray UnitTest Attempt [type=" << type << "][start=" << start << "][end=" << end <<
                      "][nattrs=" << nattrs << "][ascent=" << ascent << "]");

        // Array schema
        Attributes attributes;
        for (size_t i = 0; i < nattrs; i++)
        {
            std::stringstream ss;
            ss << "X" << i;
            attributes.push_back(AttributeDesc(
                ss.str(), type,
                AttributeDesc::IS_NULLABLE, CompressorType::NONE));
        }
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("dummy_dimension"), start, end, chunkInterval, 0);
        // ArrayDesc consumes the new copy, source is discarded.
        ArrayDesc schema("dummy_array", attributes.addEmptyTagAttribute(), dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());

        // Sort Keys
        SortingAttributeInfos sortingAttributeInfos;
        SortingAttributeInfo  k;
        k.columnNo = 0;
        k.ascent = ascent;
        sortingAttributeInfos.push_back(k);

        // Define the array to sort
        std::shared_ptr<MemArray> arrayInst(new MemArray(schema,query));
        std::shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);

        // Generate source data
        CoordValueMap mapInst;
        Value value;
        for (Coordinate i=start; i<end+1; ++i) {
            if (! rand()%100<percentEmpty) {
                mapInst[i] = genRandomValue(type, value, percentNullValue, missingReason);
            }
        }

        // Insert the map data into the array.
        insertMapDataIntoArray(query, *arrayInst, mapInst);

        // Sort
        SortArray sorter(schema, _arena);
        sorter.setPreservePositions(false);
        std::shared_ptr<TupleComparator> tcomp(new TupleComparator(sortingAttributeInfos, schema));
        std::shared_ptr<MemArray> sortedArray = sorter.getSortedArray(baseArrayInst, query, phyOp, tcomp);

        // Check correctness.
        // - Retrieve all data from the array. Ensure results are sorted.
        for (const auto& attr : schema.getAttributes(true))
        {
            Value t1[1];
            Value t2[1];
            size_t itemCount = 0;
            std::shared_ptr<ConstArrayIterator> constArrayIter =
                sortedArray->getConstIterator(attr);
            constArrayIter->restart();
            while (!constArrayIter->end())
            {
                std::shared_ptr<ConstChunkIterator> constChunkIter =
                    constArrayIter->getChunk().getConstIterator(ConstChunkIterator::DEFAULT);
                while (!constChunkIter->end())
                {
                    itemCount++;
                    Value const& v = constChunkIter->getItem();
                    t1[0] = v;
                    ++(*constChunkIter);
                    if (!constChunkIter->end())
                    {
                        Value const& next = constChunkIter->getItem();
                        t2[0] = next;
                        if (tcomp->compare(t1, t2) > 0)
                        {
                            stringstream ss;

                            ss << "elements in attr " << attr.getId() << " are out of order";
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                                "UnitTestSortArray" << ss.str();
                        }
                    }
                }
                ++(*constArrayIter);
            }
            if (itemCount != mapInst.size())
            {
                stringstream ss;

                ss << "wrong # of elements in attr " << attr.getId() << " expected: " << mapInst.size() <<
                    " got: " << itemCount;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestSortArray" << ss.str();
            }
        }

        LOG4CXX_DEBUG(logger, "SortArray UnitTest Success [type=" << type << "][start=" << start << "][end=" << end <<
                      "][nattrs=" << nattrs << "][ascent=" << ascent << "]");
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        srand(static_cast<unsigned int>(time(NULL)));

        auto phyOp = shared_from_this();

        testOnce_SortArray(query, phyOp, TID_INT64, 0, 1000, 1, true, 100);
        testOnce_SortArray(query, phyOp, TID_INT64, 0, 1000, 1, false, 100);
        testOnce_SortArray(query, phyOp, TID_INT64, 0, 1000, 3, true, 100);
        testOnce_SortArray(query, phyOp, TID_STRING, 0, 1000, 1, true, 100);
        testOnce_SortArray(query, phyOp, TID_STRING, 0, 1000, 1, false, 100);
        testOnce_SortArray(query, phyOp, TID_STRING, 0, 1000, 3, true, 100);
        testOnce_SortArray(query, phyOp, TID_INT64, 0, 5000000, 3, true, 10000);

        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestSortArrayPhysical, "test_sort_array", "UnitTestSortArrayPhysical");
}
