########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2019 SciDB, Inc.
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

message(STATUS "**BEGIN linear_algebra/CMakeLists.txt --------------------------------")

# shared library for user defined objects
set (linear_algebra_src
    plugin.cpp
    spgemm/LogicalSpgemm.cpp
    spgemm/PhysicalSpgemm.cpp
    stats/HypergeometricFunctions.cpp
)

#
# complete the linar_algebra library settings
#
scidb_add_library(linear_algebra SHARED ${linear_algebra_src})
target_link_libraries(linear_algebra PUBLIC Boost::boost)

scidb_install(TARGETS linear_algebra
              DESTINATION  lib/scidb/plugins
              COMPONENT scidb-plugins
              RENAME_SUFFIX scidb
  )
# Update alternatives for library
install(CODE "execute_process(COMMAND \
${CMAKE_SOURCE_DIR}/utils/update_alternatives.sh \
 ${CMAKE_INSTALL_PREFIX} \
 lib/scidb/plugins \
 linear_algebra \
 ${CMAKE_SHARED_LIBRARY_SUFFIX} \
 ${SCIDB_VERSION_MAJOR} \
 ${SCIDB_VERSION_MINOR} \
 scidb \
)"
  )

message(STATUS "**END linear_algebra/CMakeLists.txt --------------------------------")
