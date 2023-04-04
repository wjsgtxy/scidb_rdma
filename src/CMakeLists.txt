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
# dz add 忽略编译告警
remove_warnings()

add_subdirectory("capi")
add_subdirectory("network")
add_subdirectory("system")
add_subdirectory("rbac")
add_subdirectory("mkl")
add_subdirectory("monitor")
add_subdirectory("mpi")
add_subdirectory("dense_linear_algebra")
add_subdirectory("linear_algebra")
# add_subdirectory("jdbc") # see SDB-6665: Move jdbc to github
add_subdirectory("storage")
add_subdirectory("ccm")

if (NOT WITHOUT_SERVER)
    add_subdirectory("array")
    add_subdirectory("query")

    # $include_src is not necessary. It is a convenience for the qtcreator and xcode IDEs
    set(CMAKE_VERBOSE_MAKEFILE ON)
    scidb_add_executable(scidb
                   network/entry.cpp network/GlobalNew.cpp dense_linear_algebra/blas/initMathLibs.cpp
                   $<TARGET_OBJECTS:array_lib>
                   $<TARGET_OBJECTS:mpi>)

    target_link_libraries(scidb
      PUBLIC network_lib
             util_lib
             system_lib
             qproc_lib
             storage_lib
             ${LIBRT_LIBRARIES}
             Threads::Threads
             ${CMAKE_DL_LIBS}
             ${BLAS_LIBRARIES}
             ${LAPACK_LIBRARIES}
             ccm_lib
      )

    scidb_install(TARGETS scidb
            DESTINATION ${CMAKE_INSTALL_BINDIR}
            COMPONENT scidb
      )
endif()

add_subdirectory("util")
