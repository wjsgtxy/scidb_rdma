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
 * @file QueryStats.h
 *
 *
 * @brief Statistics related to a query's usage
 */
#ifndef QUERY_STATS_H_
#define QUERY_STATS_H_

#include <atomic>
#include <util/Arena.h>

namespace scidb
{
    class Chunk;

    /**
     * Stores/maintains counters related to an query's usage of resources.
     */
    class QueryStats
    {
    private:
        /// The pointer to the current query owning these statistics
        class Query *               _query;

        /// The timestamp of when the query started
        uint64_t                    _startTimeMicroseconds;

        /// The timestamp of last QueryStats reset
        uint64_t                    _resetTimeMicroseconds;

        /// The query active time from the start of the query to the QueryStats reset.
        uint64_t                    _activeTimeStartToResetMicroseconds;

        /// The number of bytes that were sent to network
        std::atomic_uint_fast64_t   _netSendBytes;

        /// The number of bytes that were received from network
        std::atomic_uint_fast64_t   _netRecvBytes;

        /// The amount of memory available for allocation
        std::atomic_uint_fast64_t   _memAvailable;

        /// The amount of memory currently allocated
        std::atomic_uint_fast64_t   _memAllocated;

        /// The number of memory allocations currently outstanding
        std::atomic_uint_fast64_t   _memAllocations;

        /// The amount of memory peak that has been used
        std::atomic_uint_fast64_t   _memPeakUsage;

        /// The number of cache bytes that were swapped out.
        uint64_t                    _cacheSwapBytes;

        ///The number of times the cache swapped bytes
        uint64_t                    _cacheSwapNum;

        /// The number of cache bytes that were loaded
        uint64_t                    _cacheLoadBytes;

        ///The number of times the cache loaded bytes
        uint64_t                    _cacheLoadNum;

        /// The number of cached bytes that were dropped
        uint64_t                    _cacheDropBytes;

        ///The number of times the cache dropped bytes
        uint64_t                    _cacheDropNum;

        /// The number of cache memory bytes that are being used
        uint64_t                    _cacheUsedMemSize;

        ///the amount of time the cache spends calling writeData(...)
        uint64_t                    _timeCacheWriteMicroseconds;

        ///the amount of time the cache spends calling readData(...)
        uint64_t                    _timeCacheReadMicroseconds;

        /// The time spent accessing postgress
        uint64_t                    _timeAccessingPostgressMicroseconds;

        /// Number of DBChunks written
        uint64_t                    _dbChunkNumWrites;

        /// Size of compressed DBChunks written
        uint64_t                    _dbChunkCompressedWriteBytes;

        /// Size of uncompressed DBChunks written
        uint64_t                    _dbChunkUncompressedWriteBytes;

        /// Number of DBChunks read
        uint64_t                    _dbChunkNumReads;

        /// Size of the compressed DBChunks read
        uint64_t                    _dbChunkCompressedReadBytes;

        /// Size of uncompressed DBChunks read
        uint64_t                    _dbChunkUncompressedReadBytes;

    public:
        /**
         * Constructor
         */
        QueryStats(class Query *query);

        /**
         * Provide the ability to reset the counters
         */
        void reset();

        /**
         * Add the provided amount to the network send bytes counter
         * @param amountBytes - the amount to add
         */
        void addToNetSend(uint64_t recvBytes);

        /**
         * Add the provided amount to the network receive bytes counter
         * @param amountBytes - the amount to add
         */
        void addToNetRecv(uint64_t recvBytes);


        /**
         * Set the counters with current information from the specified arena
         * @param arena - the arena to gather the information from
         */
        void setArenaInfo(const scidb::arena::ArenaPtr arena);

        /**
         * Add to the number of cached bytes swapped
         * @param newValue - The updated value for the number of bytes swapped out of the cache
         */
        void addToCacheSwapBytes(uint64_t newValue);

        /**
         * Add to the number of cached load bytes
         * @param newValue - The updated value for the number of bytes loaded into the cache
         */
        void addToCacheLoadBytes(uint64_t newValue);

        /**
         * Add to the number of cached drop bytes
         * @param newValue - The updated value for the number of bytes dropped from the cache
         */
        void addToCacheDropBytes(uint64_t newValue);

        /**
         * Stores the cache used mem size
         * @param newValue - The number of bytes being used by the cache
         */
        void setCacheUsedMemSize(uint64_t newValue);

        /**
         * Add to the time the cache spends calling writeData(...)
         * @param elapsedTimeMicroseconds - the elapsed time
         */
        void addToCacheWriteTime(uint64_t elapsedTimeMicroseconds);

        /**
         * Add to the time the cache spends calling readData(...)
         * @param elapsedTimeMicroseconds - the elapsed time
         */
        void addToCacheReadTime(uint64_t elapsedTimeMicroseconds);

        /**
         * Add to the time spent communicating with postgress
         * @param elapsedTimeMicroseconds - the elapsed time
         */
        void addToPostgressTime(uint64_t elapsedTimeMicroseconds);

        /**
         * Update DBChunk write size statistics
         * @param compressedBytes - Size of the data after it has been compressed.
         * @param uncompressedBytes - Size of the data prior to any compression.
         */
        void addToDBChunkWrite(uint64_t compressedBytes, uint64_t uncompressedBytes);

        /**
         * Update DBChunk read size statistics
         * @param compressedBytes - Size of the data after it has been compressed.
         * @param uncompressedBytes - Size of the data prior to any compression.
         */
        void addToDBChunkRead(uint64_t compressedBytes, uint64_t uncompressedBytes);

        struct MonitorInformation
        {
            uint64_t   startTimeMicroseconds;  /// the timestamp for when the query started
            uint64_t   resetTimeMicroseconds;  /// the timestamp for when the counters where last reset
            uint64_t   activeTimeStartToResetMicroseconds;  /// time the cpu was active between the start and reset time
            uint64_t   timeAccessingPostgressMicroseconds; /// amount of time spent accessing postgress

            struct {
                uint64_t   sendBytes;  /// the network send bytes counter value
                uint64_t   recvBytes;  /// the network receive bytes counter value
            } net;

            struct {
                uint64_t   available;   /// the memory available bytes counter value
                uint64_t   allocated;   /// the memory allocated bytes counter value
                uint64_t   peakUsage;   /// the memory peak usage bytes counter value
                uint64_t   allocations; /// the number of memory allocations counter value
            } mem;

            struct {
                uint64_t   swapBytes;   /// the number of cache bytes that were swapped out.
                uint64_t   swapNum;     /// the number of times the cache swapped bytes
                uint64_t   loadBytes;   /// the number of cache bytes that were loaded
                uint64_t   loadNum;     /// the number of times the cache loaded bytes
                uint64_t   dropBytes;   /// the number of cache bytes that were dropped
                uint64_t   dropNum;     /// the number of times the cache dropped bytes
                uint64_t   usedMemSize; /// the number of cache memory bytes that are being used
                uint64_t   timeWriteMicroseconds;   /// the amount of time the cache spends calling writeData(...)
                uint64_t   timeReadMicroseconds;    /// amount of time the cache spends calling readData(...)
            } cache;

            struct
            {
                uint64_t        numWrites;              /// The number of DBChunks written
                uint64_t        compressedWriteBytes;   /// The number of DBChunks compressed bytes written
                uint64_t        uncompressedWriteBytes; /// The number of DBChunks uncompressed (equivalent) bytes written

                uint64_t        numReads;               /// The number of DBChunks read
                uint64_t        compressedReadBytes;    /// The number of DBChunks compressed bytes read
                uint64_t        uncompressedReadBytes;  /// The number of DBChunks uncompressed (equivalent) bytes read
            } dbChunk;

            MonitorInformation()
                : startTimeMicroseconds(0)
                , resetTimeMicroseconds(0)
                , activeTimeStartToResetMicroseconds(0)
                , timeAccessingPostgressMicroseconds(0)
                , net({0, 0})
                , mem({0, 0, 0, 0})
                , cache({0, 0, 0, 0, 0, 0, 0, 0, 0})
                , dbChunk({0, 0, 0, 0, 0, 0})
            {

            }
        };

        /**
         * Retrieve the information captured by the class relating to the query
         */
        void getInfo(MonitorInformation &monitorInformation) const;


        /**
         * Verify that the specified counter does not have the most significant bit set.
         *
         * If the most significant bit is set then:
         *     Log the event
         *     Reset the counters
         */
        inline bool CheckMaxCounter(const std::atomic_uint_fast64_t &value);
    };  // class QueryStats
} // namespace scidb

#endif // #ifndef QUERY_STATS_H_
