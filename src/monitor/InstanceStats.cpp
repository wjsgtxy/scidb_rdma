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
#include <monitor/InstanceStats.h>

#include <log4cxx/logger.h>
#include <monitor/MonitorConfig.h>
#include <network/NetworkManager.h>
#include <sstream>
#include <string>
#include <sys/utsname.h>
#include <system/Utils.h>




namespace scidb
{
    // Logger for network subsystem. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_monitor"));

    ActiveCpuTimer ActiveCpuTimer::_activeCpuTimer;

    uint64_t InstanceStats::_timeStampStartMicroseconds(perfTimeGetElapsedInMicroseconds());


    InstanceStats::InstanceStats()
        : _netSendBytes(0)
        , _netRecvBytes(0)
        , _cacheSwapBytes(0)
        , _cacheSwapNum(0)
        , _cacheLoadBytes(0)
        , _cacheLoadNum(0)
        , _cacheDropBytes(0)
        , _cacheDropNum(0)
        , _cacheUsedMemSize(0)
        , _timeCacheWriteMicroseconds(0)
        , _timeCacheReadMicroseconds(0)
        , _timeStampResetMicroseconds(_timeStampStartMicroseconds)
        , _timeAccessingPostgressMicroseconds(0)
        , _dbChunkNumWrites(0)
        , _dbChunkCompressedWriteSize(0)
        , _dbChunkUncompressedWriteSize(0)
        , _dbChunkNumReads(0)
        , _dbChunkCompressedReadSize(0)
        , _dbChunkUncompressedReadSize(0)
    {

    }

    void InstanceStats::reset()
    {
        _netSendBytes               = 0;
        _netRecvBytes               = 0;
        _cacheSwapBytes             = 0;
        _cacheSwapNum               = 0;
        _cacheLoadBytes             = 0;
        _cacheLoadNum               = 0;
        _cacheDropBytes             = 0;
        _cacheDropNum               = 0;
        _cacheUsedMemSize           = 0;
        _timeCacheWriteMicroseconds = 0;
        _timeCacheReadMicroseconds  = 0;
        _timeAccessingPostgressMicroseconds  = 0;
        _dbChunkNumWrites               = 0;
        _dbChunkCompressedWriteSize     = 0;
        _dbChunkUncompressedWriteSize   = 0;
        _dbChunkNumReads                = 0;
        _dbChunkCompressedReadSize      = 0;
        _dbChunkUncompressedReadSize    = 0;

        ActiveCpuTimer::getInstance().restart();

        _timeStampResetMicroseconds = perfTimeGetElapsedInMicroseconds();
    }

    void InstanceStats::addToPostgressTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeAccessingPostgressMicroseconds += elapsedTimeMicroseconds;
    }

    /**
     * The variables below are not automic because they are only updated in
     * CachedStorage::writeChunkToDataStore(...).  The CachedStorage::writeChunkToDataStore(...)
     * method is only called in CachedStorage::write(...) and is protected by the following:
     *     ScopedMutexLock cs(_mutex);
     */
    void InstanceStats::addToDBChunkWrite(uint64_t compressedSize, uint64_t uncompressedSize)
    {
        _dbChunkNumWrites++;
        _dbChunkCompressedWriteSize     += compressedSize;
        _dbChunkUncompressedWriteSize   += uncompressedSize;
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
    void InstanceStats::addToDBChunkRead(uint64_t compressedSize, uint64_t uncompressedSize)
    {
        _dbChunkNumReads++;
        _dbChunkCompressedReadSize      += compressedSize;
        _dbChunkUncompressedReadSize    += uncompressedSize;
    }

    void InstanceStats::getInfo(MonitorInformation &monitorInfo) const
    {
        if(MonitorConfig::getInstance()->isEnabled())
        {
            struct utsname  sysInfo;
            if(uname(&sysInfo) == 0)
            {
                monitorInfo.net.hostName = sysInfo.nodename;
            } else {
                monitorInfo.net.hostName = "";
            }

            NetworkManager::getInstance()->getIpAddress(monitorInfo.net.ipAddress);

            const arena::ArenaPtr& arena = arena::getArena();
            SCIDB_ASSERT(arena);

            monitorInfo.mem.bytesPeakUsage  = arena->peakusage();
            monitorInfo.mem.bytesAllocated  = arena->allocated();
            monitorInfo.mem.bytesAvailable  = arena->available();
            monitorInfo.mem.allocations     = arena->allocations();
        } else {
            monitorInfo.net.hostName = "";
            monitorInfo.net.ipAddress = "";
            monitorInfo.mem.bytesPeakUsage  = 0;
            monitorInfo.mem.bytesAllocated  = 0;
            monitorInfo.mem.bytesAvailable  = 0;
            monitorInfo.mem.allocations     = 0;
        }


        monitorInfo.net.sendBytes                 = _netSendBytes;
        monitorInfo.net.recvBytes                 = _netRecvBytes;
        monitorInfo.cache.swapBytes               = _cacheSwapBytes;
        monitorInfo.cache.swapNum                 = _cacheSwapNum;
        monitorInfo.cache.loadBytes               = _cacheLoadBytes;
        monitorInfo.cache.loadNum                 = _cacheLoadNum;
        monitorInfo.cache.dropBytes               = _cacheDropBytes;
        monitorInfo.cache.dropNum                 = _cacheDropNum;
        monitorInfo.cache.usedMemSize             = _cacheUsedMemSize;
        monitorInfo.cache.timeWriteMicroseconds   = _timeCacheWriteMicroseconds;
        monitorInfo.cache.timeReadMicroseconds    = _timeCacheReadMicroseconds;


        monitorInfo.timeCpuActiveMicroseconds   =
            ActiveCpuTimer::getInstance().getElapsedTimeInMicroseconds();

        monitorInfo.timeStampStartMicroseconds            = _timeStampStartMicroseconds;
        monitorInfo.timeStampResetMicroseconds            = _timeStampResetMicroseconds;
        monitorInfo.timeAccessingPostgressMicroseconds    = _timeAccessingPostgressMicroseconds;

        monitorInfo.dbChunk.numWrites                     = _dbChunkNumWrites;
        monitorInfo.dbChunk.compressedWriteSize           = _dbChunkCompressedWriteSize;
        monitorInfo.dbChunk.uncompressedWriteSize         = _dbChunkUncompressedWriteSize;

        monitorInfo.dbChunk.numReads                      = _dbChunkNumReads;
        monitorInfo.dbChunk.compressedReadSize            = _dbChunkCompressedReadSize;
        monitorInfo.dbChunk.uncompressedReadSize          = _dbChunkUncompressedReadSize;
    }

    bool InstanceStats::CheckMaxCounter(const std::atomic_uint_fast64_t &value)
    {
        if(value & (1ULL << 63))
        {
            std::stringstream ss;
            ss << "InstanceStats counter overflow" << std::endl
               << "  _netSendBytes = " << _netSendBytes << std::endl
               << "  _netRecvBytes = " << _netRecvBytes << std::endl;
            LOG4CXX_ERROR(logger, ss.str());
            return false;
        }
        return true;
    }

    void InstanceStats::addToNetSend(uint64_t amount)
    {
        _netSendBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_netSendBytes));
    }

    void InstanceStats::addToNetRecv(uint64_t amount)
    {
        _netRecvBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(amount));
    }

    void InstanceStats::addToCacheSwapBytes(uint64_t amount)
    {
        _cacheSwapNum++;
        _cacheSwapBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheSwapBytes));
    }

    void InstanceStats::addToCacheLoadBytes(uint64_t amount)
    {
        _cacheLoadNum++;
        _cacheLoadBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheLoadBytes));
    }

    void InstanceStats::addToCacheDropBytes(uint64_t amount)
    {
        _cacheDropNum++;
        _cacheDropBytes += amount;
        SCIDB_ASSERT(CheckMaxCounter(_cacheDropBytes));
    }

    void InstanceStats::setCacheUsedMemSize(uint64_t amount)
    {
        _cacheUsedMemSize = amount;
    }

    void InstanceStats::addToCacheWriteTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeCacheWriteMicroseconds += elapsedTimeMicroseconds;
    }

    void InstanceStats::addToCacheReadTime(uint64_t elapsedTimeMicroseconds)
    {
        _timeCacheReadMicroseconds += elapsedTimeMicroseconds;
    }

}  // namespace scidb
