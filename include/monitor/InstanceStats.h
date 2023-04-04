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
 * @file InstanceStats.h
 *
 *
 * @brief Statistics related to an instance's usage
 */
#ifndef INSTANCE_STATS_H_
#define INSTANCE_STATS_H_

#include <atomic>
#include <monitor/MonitorConfig.h>
#include <sys/time.h>
#include <system/Utils.h>
#include <util/Arena.h>
#include <util/PerfTime.h>
#include <util/Singleton.h>


namespace scidb
{
    /**
     * Keeps track of how much time the cpu has been active since instantiated or restarted.
     */
    class ActiveCpuTimer
    {
    private:
        uint64_t                _startTimeMicroSeconds;
        static ActiveCpuTimer   _activeCpuTimer;


        /**
         * Get the sum of all of the threads resource usage
         */
        uint64_t _getCpuTimeInMicroseconds() const noexcept
        {
            return perfTimeGetCpuInMicroseconds(RUSAGE_SELF);
        }

        /**
         * Initialize the start time with the current CPU time.
         */
        ActiveCpuTimer()
            : _startTimeMicroSeconds(_getCpuTimeInMicroseconds())
        {
        }

    public:
        /**
         * Retrieve the one an only instance of this object
         */
        static ActiveCpuTimer &getInstance()
        {
            return _activeCpuTimer;
        }

        /**
         * Reset the "start time" to the current time.
         */
        void restart()
        {
            _startTimeMicroSeconds = _getCpuTimeInMicroseconds();
        }

        /**
         * Determine how much time has elapsed since the "start time" in microseconds
         */
        uint64_t getElapsedTimeInMicroseconds() const
        {
            return _getCpuTimeInMicroseconds() - _startTimeMicroSeconds;
        }

        /**
         * Determine how much time has elapsed since the "start time" in milliseconds
         */
        uint64_t getElapsedTimeInMilliseconds() const
        {
            // Convert to milliseconds
            static const uint64_t milliseconds = 1000;

            return (_getCpuTimeInMicroseconds() - _startTimeMicroSeconds) / milliseconds;
        }
    };

    /**
     * Stores/maintains counters related to an instance's usage of resources. Also provides the
     * ability to reset such resource counters.
     */
    class InstanceStats : public Singleton<InstanceStats>
    {
    private:
        /// Used to convert seconds to microseconds
        static const double _microseconds;

        /// Number of bytes that this instance sent to the network
        std::atomic_uint_fast64_t   _netSendBytes;

        /// Number of bytes that this instance received from the network
        std::atomic_uint_fast64_t   _netRecvBytes;

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

        /// The timestamp that this instance started.
        static uint64_t             _timeStampStartMicroseconds;

        /// The timestamp that this instance was last reset.
        uint64_t                    _timeStampResetMicroseconds;

        /// The time spent accessing postgress
        uint64_t                    _timeAccessingPostgressMicroseconds;

        /// The number of DBChunks written
        uint64_t                    _dbChunkNumWrites;

        /// Size of the written DBChunk after it has been compressed.
        uint64_t                    _dbChunkCompressedWriteSize;

        /// Size of written DBChunk data prior to any compression.
        uint64_t                    _dbChunkUncompressedWriteSize;

        /// The number of DBChunks read
        uint64_t                    _dbChunkNumReads;

        /// Size of the read DBChunk after it has been compressed.
        uint64_t                    _dbChunkCompressedReadSize;

        /// Size of read DBChunk data prior to any compression.
        uint64_t                    _dbChunkUncompressedReadSize;


        /// Retrieve information relating to the specified arena (assumed to be the global arena).
        void retrieveArenaInfo(const arena::ArenaPtr& arena);

    public:
        /**
         * Constructor
         */
        InstanceStats();

        /**
         * Reset the statistics
         */
        void reset();

        /**
         * Add to the number of bytes this instance has sent to the network.
         * @param sendBytes - This amount will be added to the number fo bytes sent to the network
         */
        void addToNetSend(uint64_t sendBytes);

        /**
         * Add to the number of bytes this instance has received from the network.
         * @param sendBytes - This amount will be added to the number fo bytes received from the network
         */
        void addToNetRecv(uint64_t recvBytes);

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
            uint64_t        timeCpuActiveMicroseconds;   // The time (in milliseconds) the cpu has been active
            uint64_t        timeStampStartMicroseconds;  // The time (in microsseconds) that this instance started
            uint64_t        timeStampResetMicroseconds;  // The time (in microseconds)that this instance was last reset
            uint64_t        timeAccessingPostgressMicroseconds;  // The time spent (in microseconds)accessing postgress

            struct
            {
                uint64_t        sendBytes;      // The number of bytes sent to the network
                uint64_t        recvBytes;      // The number of bytes received from the network
                std::string     hostName;       // The name of the machine this instance is running on
                std::string     ipAddress;      // The ip address of the machine this instance is running on
            } net;

            struct
            {
                uint64_t        swapBytes;      // The number of cache bytes swapped out
                uint64_t        swapNum;        // The number of times the cache swapped bytes
                uint64_t        loadBytes;      // The number of cache bytes loaded
                uint64_t        loadNum;        // The number of times the cache loaded bytes
                uint64_t        dropBytes;      // The number of cache bytes dropped
                uint64_t        dropNum;        // The number of times the cache dropped bytes
                uint64_t        usedMemSize;    // The number of cache bytes currently in use
                uint64_t        timeWriteMicroseconds;  // The time (in microseconds) the cache spends calling writeData(...)
                uint64_t        timeReadMicroseconds;   // The time (in microseconds) the cache spends calling readData(...)
            } cache;

            struct
            {
                uint64_t        bytesAvailable;  // Number of bytes that are available on the instance
                uint64_t        bytesAllocated;  // Number of bytes that are allocated on the instance
                uint64_t        bytesPeakUsage;  // Number of bytes that were used during peak usage on the instance
                uint64_t        allocations;     // Number of allocations requested on this instance
            } mem;

            struct
            {
                uint64_t        numWrites;              // The number of DBChunks written
                uint64_t        compressedWriteSize;    // The number of DBChunks compressed bytes written
                uint64_t        uncompressedWriteSize;  // The number of DBChunks uncompressed (equivalent) bytes written

                uint64_t        numReads;               // The number of DBChunks read
                uint64_t        compressedReadSize;     // The number of DBChunks compressed bytes read
                uint64_t        uncompressedReadSize;   // The number of DBChunks uncompressed (equivalent) bytes read
            } dbChunk;
        } ;

        /**
         * Retreive the stored information
         * @param statsInfo - A structure containing the instance statistics
         */
        void getInfo(MonitorInformation &monitorInfo) const;


        /**
         * Checks to ensure that the most significant bit of a counter is not set and
         * asserts if it is.
         * @param value - A reference to the counter to check
         */
        bool CheckMaxCounter(const std::atomic_uint_fast64_t &value);
    };  // class InstanceStats

} // namespace scidb

#endif // #ifndef INSTANCE_STATS_H_
