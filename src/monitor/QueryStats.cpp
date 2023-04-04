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
 * @file InstanceStats.cpp
 *
 *
 * @brief Statistics related to an instance's usage
 */

#include <monitor/QueryStats.h>

#include <array/Array.h>
#include <log4cxx/logger.h>
#include <sstream>
#include <system/Utils.h>
#include <util/PerfTime.h>
#include <query/Query.h>


namespace scidb
{
    // Logger for network subsystem. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_monitor"));

    QueryStats::QueryStats(
        class Query *query)
        : _query(query)
        , _startTimeMicroseconds(perfTimeGetElapsedInMicroseconds())
        , _resetTimeMicroseconds(_startTimeMicroseconds)
        , _activeTimeStartToResetMicroseconds(0)
        , _netSendBytes(0)
        , _netRecvBytes(0)
        , _memAvailable(0)
        , _memAllocated(0)
        , _memPeakUsage(0)
        , _cacheSwapBytes(0)
        , _cacheSwapNum(0)
        , _cacheLoadBytes(0)
        , _cacheLoadNum(0)
        , _cacheDropBytes(0)
        , _cacheDropNum(0)
        , _cacheUsedMemSize(0)
        , _timeCacheWriteMicroseconds(0)
        , _timeCacheReadMicroseconds(0)
        , _timeAccessingPostgressMicroseconds(0)
        , _dbChunkNumWrites(0)
        , _dbChunkCompressedWriteBytes(0)
        , _dbChunkUncompressedWriteBytes(0)
        , _dbChunkNumReads(0)
        , _dbChunkCompressedReadBytes(0)
        , _dbChunkUncompressedReadBytes(0)
    {
        SCIDB_ASSERT(query);
    }

    void QueryStats::reset()
    {
        SCIDB_ASSERT(_query);
        _resetTimeMicroseconds = perfTimeGetElapsedInMicroseconds();
        _activeTimeStartToResetMicroseconds = _query->getActiveTimeMicroseconds();
        _netSendBytes       = 0;
        _netRecvBytes       = 0;
        _memAvailable       = 0;
        _memAllocated       = 0;
        _memPeakUsage       = 0;
        _cacheSwapBytes     = 0;
        _cacheSwapNum       = 0;
        _cacheLoadBytes     = 0;
        _cacheLoadNum       = 0;
        _cacheDropBytes     = 0;
        _cacheDropNum       = 0;
        _cacheUsedMemSize   = 0;
        _timeCacheWriteMicroseconds  = 0;
        _timeCacheReadMicroseconds   = 0;
        _timeAccessingPostgressMicroseconds  = 0;

        _dbChunkNumWrites               = 0;
        _dbChunkCompressedWriteBytes    = 0;
        _dbChunkUncompressedWriteBytes  = 0;
        _dbChunkNumReads                = 0;
        _dbChunkCompressedReadBytes     = 0;
        _dbChunkUncompressedReadBytes   = 0;
    }

    bool QueryStats::CheckMaxCounter(const std::atomic_uint_fast64_t &value)
    {
        if(value & (1ULL << 63))
        {
            std::stringstream ss;
            ss << "QueryStats counter overflow" << std::endl
               << "  _netSendBytes = " << _netSendBytes << std::endl
               << "  _netRecvBytes = " << _netRecvBytes << std::endl
               << "  _memAvailable = " << _memAvailable << std::endl
               << "  _memAllocated = " << _memAllocated << std::endl
               << "  _memPeakUsage = " << _memPeakUsage << std::endl;
            LOG4CXX_ERROR(logger, ss.str());
            reset();
            return false;
        }
        return true;
    }

    void QueryStats::addToNetSend(uint64_t recvBytes)
    {
        _netSendBytes += recvBytes;
        SCIDB_ASSERT(CheckMaxCounter(_netSendBytes));
    }

    void QueryStats::addToNetRecv(uint64_t recvBytes)
    {
        _netRecvBytes += recvBytes;
        SCIDB_ASSERT(CheckMaxCounter(_netRecvBytes));
    }

    void QueryStats::addToCacheSwapBytes(uint64_t amount)
    {
        _cacheSwapNum++;
        _cacheSwapBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheSwapBytes));
    }

    void QueryStats::addToCacheLoadBytes(uint64_t amount)
    {
        _cacheLoadNum++;
        _cacheLoadBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheLoadBytes));
    }

    void QueryStats::addToCacheDropBytes(uint64_t amount)
    {
        _cacheDropNum++;
        _cacheDropBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheDropBytes));
    }

    void QueryStats::setCacheUsedMemSize(uint64_t amount)
    {
        _cacheUsedMemSize = amount;
    }

    void QueryStats::addToCacheWriteTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeCacheWriteMicroseconds += elapsedTimeMicroseconds;
    }

    void QueryStats::addToCacheReadTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeCacheReadMicroseconds += elapsedTimeMicroseconds;
    }


    void QueryStats::setArenaInfo(const arena::ArenaPtr arena)
    {
        if(arena)
        {
            _memAvailable   = arena->available();
            _memAllocated   = arena->allocated();
            _memPeakUsage   = arena->peakusage();
            _memAllocations = arena->allocations();

            SCIDB_ASSERT(CheckMaxCounter(_memAvailable));
            SCIDB_ASSERT(CheckMaxCounter(_memAllocated));
            SCIDB_ASSERT(CheckMaxCounter(_memPeakUsage));
            SCIDB_ASSERT(CheckMaxCounter(_memAllocations));
        }
    }

    void QueryStats::addToPostgressTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeAccessingPostgressMicroseconds += elapsedTimeMicroseconds;
    }

    /**
     * The variables below are not automic because they are only updated in
     * CachedStorage::writeChunkToDataStore(...).  The CachedStorage::writeChunkToDataStore(...)
     * method is only called in CachedStorage::write(...) and is protected by the following:
     *     ScopedMutexLock cs(_mutex);
     */

    void QueryStats::addToDBChunkWrite(uint64_t compressedBytes, uint64_t uncompressedBytes)
    {
        _dbChunkNumWrites++;
        _dbChunkCompressedWriteBytes    += compressedBytes;
        _dbChunkUncompressedWriteBytes  += uncompressedBytes;
    }

    /**
     * The variables below are also not automic because they are protected by a mutex in
     * CachedStorage.  However, following this chain of methods is a bit more difficult than the
     * CachedStorage::writeChunkToDataStore(...). There are two locations in which
     * CachedStorage::readChunkFromDataStore(...) is called, as shown below, and each are protected
     * by a mutex.
     *
     * 1)
     *   CachedStorage::DBArrayChunk::compress(...)
     *       PersistenChunk::Pinner scope(dbChunk)           <<< mutex lock is here
     *       CachedStorage::compressChunk(...)
     *           CachedStorage::readChunkFromDataStore(...)
     *
     * 2)
     *   CachedStorage::DBArrayChunk::getConstIterator(...)
     *       dbChunk->pin()                                  <<< mutex lock is here
     *       CachedStorage::loadChunk(...)
     *           CachedStorage::fetchChunk(...)
     *               CachedStorage::readChunkFromDataStore(...)
     */
    void QueryStats::addToDBChunkRead(uint64_t compressedBytes, uint64_t uncompressedBytes)
    {
        _dbChunkNumReads++;
        _dbChunkCompressedReadBytes     += compressedBytes;
        _dbChunkUncompressedReadBytes   += uncompressedBytes;
    }

    void QueryStats::getInfo(MonitorInformation &monitorInfo) const
    {
        monitorInfo.startTimeMicroseconds   = _startTimeMicroseconds;
        monitorInfo.resetTimeMicroseconds   = _resetTimeMicroseconds;
        monitorInfo.activeTimeStartToResetMicroseconds    = _activeTimeStartToResetMicroseconds;
        monitorInfo.timeAccessingPostgressMicroseconds    = _timeAccessingPostgressMicroseconds;

        monitorInfo.net.sendBytes            = _netSendBytes;
        monitorInfo.net.recvBytes            = _netRecvBytes;

        monitorInfo.mem.available            = _memAvailable;
        monitorInfo.mem.allocated            = _memAllocated;
        monitorInfo.mem.peakUsage            = _memPeakUsage;
        monitorInfo.mem.allocations          = _memAllocations;

        monitorInfo.cache.swapBytes          = _cacheSwapBytes;
        monitorInfo.cache.swapNum            = _cacheSwapNum;
        monitorInfo.cache.loadBytes          = _cacheLoadBytes;
        monitorInfo.cache.loadNum            = _cacheLoadNum;
        monitorInfo.cache.dropBytes          = _cacheDropBytes;
        monitorInfo.cache.dropNum            = _cacheDropNum;
        monitorInfo.cache.usedMemSize        = _cacheUsedMemSize;
        monitorInfo.cache.timeWriteMicroseconds   = _timeCacheWriteMicroseconds;
        monitorInfo.cache.timeReadMicroseconds    = _timeCacheReadMicroseconds;

        monitorInfo.dbChunk.numWrites                     = _dbChunkNumWrites;
        monitorInfo.dbChunk.compressedWriteBytes          = _dbChunkCompressedWriteBytes;
        monitorInfo.dbChunk.uncompressedWriteBytes        = _dbChunkUncompressedWriteBytes;

        monitorInfo.dbChunk.numReads                      = _dbChunkNumReads;
        monitorInfo.dbChunk.compressedReadBytes           = _dbChunkCompressedReadBytes;
        monitorInfo.dbChunk.uncompressedReadBytes         = _dbChunkUncompressedReadBytes;
    }
} // namespace scidb

