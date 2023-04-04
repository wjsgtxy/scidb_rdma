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
 * @file PhysicalInstanceStats.cpp
 * @brief The physical implementation of the instance_stats operator.
 * @see LogicalInstanceStats.cpp
 * @author apoliakov@paradigm4.com
 */

#include <array/MemArray.h>
#include <network/Network.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>

using namespace std;

namespace scidb
{

/*
 * A static-linkage logger object we can use to write data to scidb.log.
 * Lookup the log4cxx package for more information.
 */
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.toy_operators.instance_stats"));

class PhysicalInstanceStats : public PhysicalOperator
{
public:
    /**
     * An inner struct used to gather the information we need to output. To facilitate the "global" option, this struct
     * may be marshalled and un-marshalled into a memory buffer.
     */
    struct Stats
    {
        /* Self-explanatory; see LogicalInstanceStats.cpp for more info. */
        size_t chunkCount;
        size_t cellCount;
        size_t minCellsPerChunk;
        size_t maxCellsPerChunk;

        Stats():
            chunkCount(0),
            cellCount(0),
            minCellsPerChunk(std::numeric_limits<size_t>::max()),
            maxCellsPerChunk(0)
        {}

        /**
         * Unmarshall stats from a flat buffer.
         * Note: the scidb::SharedBuffer is a thin wrapper over a block of allocated memory.
         * @param statData the buffer of data; must be exactly getMarshalledSize() bytes.
         */
        Stats(std::shared_ptr<SharedBuffer>& statData)
        {
            /* assert-like defensive exception */
            if (statData->getSize() != getMarshalledSize())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                      << "Received a statistics data buffer of incorrect size";
            }
            auto ptr = reinterpret_cast<const size_t*> (statData->getConstData());
            chunkCount = *ptr;
            ++ptr;
            cellCount = *ptr;
            ++ptr;
            minCellsPerChunk = *ptr;
            ++ptr;
            maxCellsPerChunk = *ptr;
            ++ptr;
        }

        /**
         * Marshall stats into a buffer
         * @return a buffer of size getMarshalledSize() containing the data.
         */
        std::shared_ptr<SharedBuffer> marshall()
        {
            std::shared_ptr <SharedBuffer> result (new MemoryBuffer(NULL, getMarshalledSize()));
            auto ptr = reinterpret_cast<size_t*> (result->getWriteData());
            *ptr = chunkCount;
            ++ptr;
            *ptr = cellCount;
            ++ptr;
            *ptr = minCellsPerChunk;
            ++ptr;
            *ptr = maxCellsPerChunk;
            ++ptr;
            return result;
        }

        /**
         * @return the marshalled size of the struct, in bytes.
         */
        static size_t getMarshalledSize()
        {
            return 4*sizeof(size_t);
        }

        /**
         * Add data from another Stats object to this.
         * @param other a set of stats collected on a different instance.
         */
        void merge(Stats const& other)
        {
            chunkCount += other.chunkCount;
            cellCount += other.cellCount;
            if (other.minCellsPerChunk < minCellsPerChunk)
            {
                minCellsPerChunk = other.minCellsPerChunk;
            }
            if (other.maxCellsPerChunk > maxCellsPerChunk)
            {
                maxCellsPerChunk = other.maxCellsPerChunk;
            }
        }
    };

    PhysicalInstanceStats(string const& logicalName,
                          string const& physicalName,
                          Parameters const& parameters,
                          ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Read data from inputArray, compute and return a set of statistics on it; also, log the data values if
     * settings.dumpToLog() is set. This routine provides the simple example of reading data from an input array.
     * @param inputArray the array to read
     * @return an object that describes the data
     */
    Stats computeLocalStats(std::shared_ptr<Array>& inputArray)
    {
        Stats result;

        /* The ConstArrayIterator allows one to read the array data, one attribute at a time.
         * We obtain the iterator for attribute 0.
         */
        const auto& attr = inputArray->getArrayDesc().getAttributes().firstDataAttribute();
        std::shared_ptr<ConstArrayIterator> arrayIter = inputArray->getConstIterator(attr);
        while (!arrayIter->end())
        {
            /* The ConstArrayIterator will iterate once for every chunk in the array, in row-major order */
            ++result.chunkCount;
            size_t const cellsInChunk  = arrayIter->getChunk().count();
            result.cellCount += cellsInChunk;
            if (cellsInChunk > result.maxCellsPerChunk)
            {
                result.maxCellsPerChunk = cellsInChunk;
            }
            if (cellsInChunk < result.minCellsPerChunk)
            {
                result.minCellsPerChunk = cellsInChunk;
            }
            ++(*arrayIter); /* advance array iterator */

            /* Note: implementations of this function in previous releases had an example of iterating over values
             * inside a chunk and checking for nulls. See previous versions of the source tree.
             */
        }

        /* Note: both ConstArrayIterator and ConstChunkIterator support a setPosition() method for random-access reading
         * See PhysicalIndexLookup for example usage.
         */
        return result;
    }

    /**
     * Record a set of statistics into a MemArray.
     * @param stats the statistics to record
     * @param query the query context
     */
    std::shared_ptr<Array> writeStatsToMemArray(Stats const& stats, std::shared_ptr<Query>& query)
    {
        /* This is very similar to the write code seen in PhysicalHelloInstances, except we are writing multiple
         * attributes - all at the same position.
         */
        std::shared_ptr<Array> outputArray(new MemArray(_schema, query));
        auto attrIter = outputArray->getArrayDesc().getAttributes(true).begin();
        std::shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(*attrIter);
        Coordinates position(1, query->getInstanceID());

        /* The first attribute is opened with only SEQUENTIAL_WRITE. Other attributes are also opened with
         * NO_EMPTY_CHECK. So the empty tag is populated implicitly from the first attribute.
         *
         * Note: since there's only one cell to write, SEQUENTIAL_WRITE is not so relevant, though it is faster.
         */

        //chunk count
        std::shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query,
                                                                                       ChunkIterator::SEQUENTIAL_WRITE);
        outputChunkIter->setPosition(position);
        Value value;
        value.setUint64(stats.chunkCount);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        //cell count
        outputArrayIter = outputArray->getIterator(*++attrIter);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        value.setUint64(stats.cellCount);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        //min cells per chunk
        outputArrayIter = outputArray->getIterator(*++attrIter);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        if (stats.cellCount > 0)
        {
            value.setUint64(stats.minCellsPerChunk);
        }
        else
        {
            value.setNull();
        }
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        //max cells per chunk
        outputArrayIter = outputArray->getIterator(*++attrIter);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        if (stats.cellCount > 0)
        {
            value.setUint64(stats.maxCellsPerChunk);
        }
        else
        {
            value.setNull();
        }
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        //avg cells per chunk
        outputArrayIter = outputArray->getIterator(*++attrIter);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        if (stats.cellCount > 0)
        {
            value.setDouble(static_cast<double>(stats.cellCount) / static_cast<double>(stats.chunkCount));
        }
        else
        {
            value.setNull();
        }
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        return outputArray;
    }

    /**
     * Exchange the statistics between instances.
     * @param[in|out] myStats starts with the local information and is populated with the aggregation of the global
     *                        information on instance 0. Not changed on other instances.
     * @param query the query context
     */
    void exchangeStats(Stats& myStats, std::shared_ptr<Query>& query)
    {
        if (query->getInstanceID() != 0)
        {
            /* I am not instance 0, so send my stuff to instance 0 */
            std::shared_ptr<SharedBuffer> buf = myStats.marshall();
            /* Non-blocking send. Must be matched by a BufReceive call on the recipient */
            BufSend(0, buf, query);
        }
        else
        {
            /*I am instance 0, receive stuff rom all other instances */
            for (InstanceID i = 1; i<query->getInstancesCount(); ++i)
            {
                /* Blocking receive. */
                std::shared_ptr<SharedBuffer> buf = BufReceive(i, query);
                Stats otherInstanceStats(buf);
                /* add data to myStats */
                myStats.merge(otherInstanceStats);
            }
        }

        /* Note: at the moment instance 0 IS synonymous with "coordinator". In the future we may move to a more
         * advanced multiple-coordinator scheme.
         */
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        bool global = false;
        Parameter globalParam = findKeyword("global");
        if (globalParam) {
            global = ((std::shared_ptr<OperatorParamPhysicalExpression>&) globalParam)->
                getExpression()->evaluate().getBool();
        }

        bool log = false;
        Parameter logParam = findKeyword("log");
        if (logParam) {
            log = ((std::shared_ptr<OperatorParamPhysicalExpression>&) globalParam)->
                getExpression()->evaluate().getBool();
        }

        std::shared_ptr<Array> inputArray = inputArrays[0];
        if (log)
        {
            /* Most arrays in the system allow the user to iterate over them multiple times, and in arbitrary order.
             * However, some arrays do not. This function will, if necessary, convert our input array to an object
             * that does.
             */
            inputArray = ensureRandomAccess(inputArray, query);

            /* A useful helper for debugging
             */
            dumpArrayToLog(inputArray, logger);
        }

        Stats stats = computeLocalStats(inputArray);
        if (global)
        {
            /* Exchange data between instances */
            exchangeStats(stats, query);
            if (query->getInstanceID() == 0)
            {
                return writeStatsToMemArray(stats, query);
            }
            else
            {
                /* Just return an empty array if I am not instance 0*/
                return std::shared_ptr<Array>(new MemArray(_schema, query));
            }
        }
        else
        {
            /* Just return local stats*/
            return writeStatsToMemArray(stats, query);
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalInstanceStats, "instance_stats", "PhysicalInstanceStats");

} //namespace scidb
