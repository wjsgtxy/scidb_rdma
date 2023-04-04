########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2019 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

add_subdirectory("compression")

set (util_src
    arena/Arena.cpp
    arena/ArenaCheckpoint.cpp
    arena/ArenaDetails.cpp
    arena/ArenaHeader.cpp
    arena/ArenaMonitor.cpp
    arena/ArenaOptions.cpp
    arena/RootArena.cpp
    arena/LimitedArena.cpp
    arena/ScopedArena.cpp
    arena/LeaArena.cpp
    arena/DebugArena.cpp
    arena/ThreadedArena.cpp
    isnumber.cpp
    safebuf.cpp
    Backtrace.cpp
    CsvParser.cpp
    DFA.cpp
    TsvParser.cpp
    Job.cpp
    JobQueue.cpp
    PerfTime.cpp
    PerfTimeScope.cpp
    PerfTimeAggData.cpp
    Semaphore.cpp
    Event.cpp
    Thread.cpp
    ThreadPool.cpp
    PluginManager.cpp
    FileIO.cpp
    PathUtils.cpp
    PluginObjects.cpp
    require.cpp
    InjectedError.cpp
    IoFormats.cpp
    BufferedFileInput.cpp
    shm/SharedMemoryIpc.cpp
    MultiConstIterators.cpp
    WorkQueue.cpp
    DataStore.cpp
    SpatialType.cpp
    StringUtil.cpp
    RtreeOfSpatialRange.cpp
    CoordinatesMapper.cpp
    Counter.cpp
    Utility.cpp
    CryptoUtils.cpp
    RTLog.cpp
    logVmSizes.cpp
)

install(FILES log4cxx.properties
        #  # DESTINATION should probably be 'etc' (${CMAKE_INSTALL_FULL_SYSCONFDIR},
        #  # not 'share' ${CMAKE_INSTALL_DATAROOTDIR}/scidb (see also related SDB-6657)
        DESTINATION share/scidb
        COMPONENT scidb
  )

find_package(Libcsv REQUIRED)
include_directories(${libcsv_INCLUDE_DIR})

add_library(util_lib STATIC ${util_src})
target_link_libraries(util_lib
  PUBLIC json_lib
         Boost::filesystem
         Boost::program_options
         Boost::regex
         Boost::serialization
         Boost::system
         ${LOG4CXX_LIBRARIES}
         ${LIBRT_LIBRARIES}
         ${libcsv_LIBRARY}
         OpenSSL::Crypto
  )

#
# plugin containing test functions used by a .test to validate PerfTime... in the library above
#
set (perf_time_test_src
     perfTimeTestFuncs.cpp
)

scidb_add_library(perf_time_test SHARED ${perf_time_test_src})
target_link_libraries(perf_time_test PUBLIC Boost::boost)

scidb_install(TARGETS perf_time_test
              DESTINATION lib/scidb/plugins
              COMPONENT scidb-plugins
  )

#
# end plugin
#
