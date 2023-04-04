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
if (NOT WITHOUT_SERVER)
    add_subdirectory("catalog")
endif()

# Prepare Constants.cpp
set(CONFIGURE_SCIDB_SRC_DIR "${CMAKE_SOURCE_DIR}/src/")
set(CONFIGURE_SCIDB_VERSION "${SCIDB_VERSION}")
set(CONFIGURE_SCIDB_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
set(CONFIGURE_SCIDB_VERSION_MAJOR "${SCIDB_VERSION_MAJOR}")
set(CONFIGURE_SCIDB_VERSION_MINOR "${SCIDB_VERSION_MINOR}")
set(CONFIGURE_SCIDB_VERSION_PATCH "${SCIDB_VERSION_PATCH}")
set(CONFIGURE_SCIDB_VERSION_BUILD "${SCIDB_BUILD_REVISION}")

set(CONFIGURE_DEFAULT_MPI_DIR "${MPI_INST_DIR}")
set(CONFIGURE_DEFAULT_MPI_TYPE "${MPI_FLAVOR}")

file(STRINGS CMakeLists.txt COPYRIGHT REGEX "^# Copyright .*")
string(REPLACE "# " "" CONFIGURE_COPYRIGHT ${COPYRIGHT})

set(CONSTANTS_CPP_IN "${CMAKE_CURRENT_SOURCE_DIR}/Constants.cpp.in")
set(CONSTANTS_CPP "${CMAKE_CURRENT_BINARY_DIR}/Constants.cpp")
message(STATUS "Configuring ${CONSTANTS_CPP_IN} as ${CONSTANTS_CPP}")
configure_file(${CONSTANTS_CPP_IN} ${CONSTANTS_CPP} @ONLY)


set(system_src
    Auth.cpp
    System.cpp
    Config.cpp
    Exceptions.cpp
    UserException.cpp
    Cluster.cpp
    Sysinfo.cpp
    ${CONSTANTS_CPP}
    SciDBConfigOptions.cpp
    Resources.cpp
    Warnings.cpp
    ErrorsLibrary.cpp
    BlockCyclic.cpp
)

add_library(system_lib STATIC ${system_src})
target_link_libraries(system_lib
  PUBLIC json_lib
         Boost::filesystem
         Boost::program_options
         Boost::regex
         Boost::serialization
         Boost::system
         ${LOG4CXX_LIBRARIES}
         Threads::Threads
  )
