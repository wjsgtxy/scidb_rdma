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

#include <query/PhysicalOperator.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <memory>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <array/RLE.h>
#include <array/MemArray.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestMemArrayPhysical: public PhysicalOperator
{
    typedef map<Coordinate, Value> CoordValueMap;
    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
public:

    UnitTestMemArrayPhysical(const string& logicalName, const string& physicalName,
                    const Parameters& parameters, const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
    }

    /**
     * Generate a random value.
     * The function should be extended to cover all types and all special values such as NaN, and then be moved to a public header file.
     * @param[in]      type        the type of the value
     * @param[in, out] value       the value to be filled
     * @param[in]      percentNull a number from 0 to 100, where 0 means never generate null, and 100 means always generate null
     * @return         the value from the parameter
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
                << "UnitTestMemArrayPhysical" << "genRandomValue";
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
                << "UnitTestMemArrayPhysical" << "value2string";
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
        const auto& attrs = array.getArrayDesc().getAttributes(true);
        vector< std::shared_ptr<ArrayIterator> > arrayIters(attrs.size());
        vector< std::shared_ptr<ChunkIterator> > chunkIters(attrs.size());

        for (const auto& attr : attrs)
        {
            arrayIters[attr.getId()] = array.getIterator(attr);
            chunkIters[attr.getId()] =
                ((MemChunk&)arrayIters[attr.getId()]->newChunk(coord)).getIterator(query,
                                                                                   ChunkIterator::SEQUENTIAL_WRITE);
        }

        for (CoordValueMapEntry const& p : m) {
            coord[0] = p.first;
            for (const auto& attr : attrs)
            {
                if (!chunkIters[attr.getId()]->setPosition(coord))
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

        for (const auto& attr : attrs)
        {
            chunkIters[attr.getId()]->flush();
        }
    }

    /**
     * Append data from a map to an array.
     * @param[in]    query
     * @param[inout] array  the array to receive data
     * @param[in]    m      the map of Coordinate --> Value
     */
    void appendMapDataToArray(std::shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m)
    {
        Coordinates coord(1);
        coord[0] = 0;
        const auto& attrs = array.getArrayDesc().getAttributes(true);
        vector< std::shared_ptr<ArrayIterator> > arrayIters(attrs.size());
        vector< std::shared_ptr<ChunkIterator> > chunkIters(attrs.size());

        for (const auto& attr : attrs)
        {
            arrayIters[attr.getId()] = array.getIterator(attr);
            arrayIters[attr.getId()]->setPosition(coord);
            chunkIters[attr.getId()] =
                ((MemChunk&)arrayIters[attr.getId()]->updateChunk()).getIterator(query,
                                                                      ChunkIterator::SEQUENTIAL_WRITE |
                                                                      ChunkIterator::APPEND_CHUNK);
        }

        for (CoordValueMapEntry const& p : m) {
            coord[0] = p.first;
            for (const auto& attr : attrs)
            {
                if (!chunkIters[attr.getId()]->setPosition(coord))
                {
                    chunkIters[attr.getId()]->flush();
                    chunkIters[attr.getId()].reset();
                    arrayIters[attr.getId()]->setPosition(coord);
                    chunkIters[attr.getId()] =
                        ((MemChunk&)arrayIters[attr.getId()]->updateChunk()).getIterator(query,
                                                                              ChunkIterator::SEQUENTIAL_WRITE |
                                                                              ChunkIterator::APPEND_CHUNK);
                    chunkIters[attr.getId()]->setPosition(coord);
                }
                chunkIters[attr.getId()]->writeItem(p.second);
            }
        }

        for (const auto& attr : attrs)
        {
            chunkIters[attr.getId()]->flush();
        }
    }

    /**
     * Test memarray append behavior.
     * First this method generates a large 1-d array of
     * random values, with each chunk only half full.
     * Next it re-opens each chunk and appends another
     * group of random values to it.
     *
     * @param[in]   query
     * @param[in]   type     the value type
     * @param[in]   start    the start coordinate of the dim
     * @param[in]   end      the end coordinate of the dim
     * @param[in]   chunkInterval  the chunk interval
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void
    testAppend_MemArray(std::shared_ptr<Query>& query,
                        TypeId const& type,
                        Coordinate start,
                        Coordinate end,
                        uint32_t chunkInterval)
    {
        const int percentNullValue = 0;
        const int missingReason = 0;

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Append Test [type=" <<
                      type << "][start=" << start << "][end=" << end <<
                      "][chunkInterval=" << chunkInterval << "]");

        try
        {

            // Array schema
            Attributes attributes;
            attributes.push_back(AttributeDesc(
                "X",  type, AttributeDesc::IS_NULLABLE, CompressorType::NONE));

            vector<DimensionDesc> dimensions(1);
            dimensions[0] = DimensionDesc("dummy_dimension", start, end, chunkInterval, 0);
            // ArrayDesc consumes the new copy, source is discarded.
            ArrayDesc schema("dummy_array", attributes.addEmptyTagAttribute(), dimensions,
                             createDistribution(getSynthesizedDistType()),
                             query->getDefaultArrayResidency());

            // Create the array
            std::shared_ptr<MemArray> arrayInst(new MemArray(schema,query));
            std::shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);

            // Generate source data --- half chunks
            CoordValueMap mapInst1;
            Value value;
            uint32_t halfChunk = chunkInterval / 2;
            for (Coordinate i=start; i<end; ++i)
            {
                mapInst1[i] = genRandomValue(type, value, percentNullValue, missingReason);
                if ((i+1) % halfChunk == 0)
                {
                    i += halfChunk;
                }
            }

            // Insert the map data into the array.
            insertMapDataIntoArray(query, *arrayInst, mapInst1);

            // Generate the other half of all the chunks.
            CoordValueMap mapInst2;
            for (Coordinate i=start + halfChunk; i<end; ++i)
            {
                mapInst2[i] = genRandomValue(type, value, percentNullValue, missingReason);
                if ((i+1) % halfChunk == 0)
                {
                    i += halfChunk;
                }
            }

            // Append the map data to the array chunks.
            appendMapDataToArray(query, *arrayInst, mapInst2);

            // Scan the array
            // - Retrieve all data from the array.
            Value t;
            size_t itemCount = 0;
            const auto& attrs = arrayInst->getArrayDesc().getAttributes();
            std::shared_ptr<ConstArrayIterator> constArrayIter =
                arrayInst->getConstIterator(attrs.firstDataAttribute());
            while (!constArrayIter->end())
            {
                std::shared_ptr<ConstChunkIterator> constChunkIter =
                    constArrayIter->getChunk().getConstIterator(ConstChunkIterator::DEFAULT);
                while (!constChunkIter->end())
                {
                    itemCount++;
                    Value const& v = constChunkIter->getItem();
                    t = v;
                    ++(*constChunkIter);
                }
                ++(*constArrayIter);
            }
            if (itemCount != (mapInst1.size() + mapInst2.size()))
            {
                stringstream ss;

                ss << "wrong # of elements in array after append, expected: " <<
                    (mapInst1.size() + mapInst2.size()) << " got: " << itemCount;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestMemArray" << ss.str();
            }
        }
        catch (...)
        {
            throw;
        }

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Append Success [type=" << type << "][start=" <<
                      start << "][end=" << end << "][chunkInterval=" << chunkInterval << "]");
    }

    /**
     * Test memarray once.
     * First this method sets the mem array threshold to
     * something small.  Then it generates a large 1-d array of
     * random values.  Finally it scans the values once.
     * If the number of swapouts is not equal to the number
     * of reads, we assert.
     *
     * @param[in]   query
     * @param[in]   type     the value type
     * @param[in]   start    the start coordinate of the dim
     * @param[in]   end      the end coordinate of the dim
     * @param[in]   chunkInterval  the chunk interval
     * @param[in]   threshold the mem-array threshold in mb
     *
     * @throw SCIDB_SE_INTERNAL::SCIDB_LE_UNITTEST_FAILED
     */
    void testOnce_MemArray(std::shared_ptr<Query>& query,
                           TypeId const& type,
                           Coordinate start,
                           Coordinate end,
                           uint32_t chunkInterval,
                           uint64_t threshold)
    {
        const int percentNullValue = 0;
        const int missingReason = 0;

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Attempt [type=" << type << "][start=" << start << "][end=" << end <<
                      "][chunkInterval=" << chunkInterval << "][threshold=" << threshold << "]");

        try
        {

            // Array schema
            Attributes attributes;
            attributes.push_back(AttributeDesc(
                "X",  type, AttributeDesc::IS_NULLABLE, CompressorType::NONE));

            vector<DimensionDesc> dimensions(1);
            dimensions[0] = DimensionDesc(string("dummy_dimension"), start, end, chunkInterval, 0);
            // ArrayDesc consumes the new copy, source is discarded.
            ArrayDesc schema("dummy_array", attributes.addEmptyTagAttribute(), dimensions,
                             createDistribution(getSynthesizedDistType()),
                             query->getDefaultArrayResidency());

            // Define the array
            std::shared_ptr<MemArray> arrayInst(new MemArray(schema,query));
            std::shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);

            // Generate source data
            CoordValueMap mapInst;
            Value value;
            for (Coordinate i=start; i<end+1; ++i)
            {
                mapInst[i] = genRandomValue(type, value, percentNullValue, missingReason);
            }

            // Insert the map data into the array.
            insertMapDataIntoArray(query, *arrayInst, mapInst);

            // Scan the array
            // - Retrieve all data from the array.
            Value t;
            size_t itemCount = 0;
            const auto& attrs = arrayInst->getArrayDesc().getAttributes();
            std::shared_ptr<ConstArrayIterator> constArrayIter =
                arrayInst->getConstIterator(attrs.firstDataAttribute());
            constArrayIter->restart();
            while (!constArrayIter->end())
            {
                std::shared_ptr<ConstChunkIterator> constChunkIter =
                    constArrayIter->getChunk().getConstIterator(ConstChunkIterator::DEFAULT);
                while (!constChunkIter->end())
                {
                    itemCount++;
                    Value const& v = constChunkIter->getItem();
                    t = v;
                    ++(*constChunkIter);
                }
                ++(*constArrayIter);
            }
            if (itemCount != mapInst.size())
            {
                stringstream ss;

                ss << "wrong # of elements in array, expected: " << mapInst.size() <<
                    " got: " << itemCount;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED) <<
                    "UnitTestMemArray" << ss.str();
            }
        }
        catch (...)
        {
            throw;
        }

        LOG4CXX_DEBUG(logger, "MemArray UnitTest Success [type=" << type << "][start=" <<
                      start << "][end=" << end << "][chunkInterval=" << chunkInterval <<
                      "][threshold=" << threshold << "]");
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        if (query->isCoordinator())
        {
            srand(static_cast<unsigned int>(time(NULL)));

            testOnce_MemArray(query, TID_INT64, 0, 500000, 10000, 2);
            testAppend_MemArray(query, TID_INT64, 0, 500000, 10000);
        }
        return std::shared_ptr<Array>(new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestMemArrayPhysical, "test_memarray", "UnitTestMemArrayPhysical");
}
