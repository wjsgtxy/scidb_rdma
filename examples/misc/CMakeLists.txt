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

# shared library for user defined objects
set (misc_src
    misc.cpp
    ../../src/array/UnitTestDeepChunkMergeLogical.cpp
    ../../src/array/UnitTestDeepChunkMergePhysical.cpp
    ../../src/array/UnitTestSortArrayLogical.cpp
    ../../src/array/UnitTestSortArrayPhysical.cpp
    ../../src/util/UnitTestFileIOLogical.cpp
    ../../src/util/UnitTestFileIOPhysical.cpp
    ../../src/util/UnitTestDataStoreLogical.cpp
    ../../src/util/UnitTestDataStorePhysical.cpp
    ../../src/util/UnitTestRootArena.cpp
    ../../src/array/UnitTestChunkLimitLogical.cpp
    ../../src/array/UnitTestChunkLimitPhysical.cpp
    ../../src/array/UnitTestMemArrayLogical.cpp
    ../../src/array/UnitTestMemArrayPhysical.cpp
    ../../src/query/ops/redimension/UnitTestChunkIdMapsLogical.cpp
    ../../src/query/ops/redimension/UnitTestChunkIdMapsPhysical.cpp
    ../../src/query/UnitTestBuiltinAggregatesLogical.cpp
    ../../src/query/UnitTestBuiltinAggregatesPhysical.cpp
    ../../src/query/ops/sg/test/LogicalTestSG.cpp
    ../../src/query/ops/sg/test/PhysicalTestSG.cpp
    ../../src/network/test/obcast/OrderedBcastTestLogical.cpp
    ../../src/network/test/obcast/OrderedBcastTestPhysical.cpp
    ../../src/storage/UnitTestStorageEngineLogical.cpp
    ../../src/storage/UnitTestStorageEnginePhysical.cpp
    ../../src/query/ops/remove/TestLogicalAbortRemove.cpp
    ../../src/query/ops/remove/TestPhysicalAbortRemove.cpp
    ../../src/query/ops/remove/TestLogicalCallbackRemoveVersions.cpp
    ../../src/query/ops/remove/TestPhysicalCallbackRemoveVersions.cpp
    ../../src/query/ops/show_arrays/LogicalShowArrays.cpp
    ../../src/query/ops/show_arrays/PhysicalShowArrays.cpp
    ../../src/query/ops/show_arrays/FilterArrays.cpp
    ../../src/query/ops/consume/LogicalBadReadWrite.cpp
    ../../src/query/ops/consume/PhysicalBadReadWrite.cpp
)

scidb_add_library(misc SHARED ${misc_src})
target_link_libraries(misc PUBLIC Boost::boost)

scidb_install(TARGETS misc
        # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins # see SDB-6657
        DESTINATION lib/scidb/plugins
        COMPONENT scidb-plugins
  )
