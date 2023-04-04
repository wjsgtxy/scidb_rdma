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

# Build upgradeChunkIndex as a plugin
set(upgrade_chunk_index_src
    io/PersistentChunk.cpp
    io/Storage.cpp
    LogicalUpgradeChunkIndex.cpp
    PhysicalUpgradeChunkIndex.cpp
    plugin.cpp
    UpgradeChunkIndex.cpp
)

scidb_add_library(upgrade_chunk_index SHARED ${upgrade_chunk_index_src})
target_link_libraries(upgrade_chunk_index PUBLIC Boost::boost)

scidb_install(TARGETS upgrade_chunk_index
        # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins # see SDB-6657
        DESTINATION lib/scidb/plugins
        COMPONENT scidb-plugins
  )
