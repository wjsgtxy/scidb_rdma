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

message(STATUS "****************** BEGIN src/mpi/CMakeLists.txt ******************")

if(SCALAPACK_TIMING)
  add_compile_definitions(SCALAPACK_TIMING)
endif()

if(SCALAPACK_DEBUG)
  add_compile_definitions(SCALAPACK_DEBUG)
endif()

    ###########################
    # ScaLAPACK
    ###########################
    find_package(ScaLAPACK)

    if (SCALAPACK_FOUND)
        message(STATUS "Debug: SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
        message(STATUS "Debug: SCALAPACK_INC_DIR is ${SCALAPACK_INC_DIR}")
    else (SCALAPACK_FOUND)
        message(WARNING "ScaLAPACK not found. ScaLAPACK-based functionality will NOT be built.")
        message(WARNING "Ubuntu Hint: $ sudo apt-get install libscalapack-mpi1")
    endif (SCALAPACK_FOUND)

if(NOT MPI_CXX_FOUND OR NOT MPI_Fortran_FOUND)
    message(WARNING "MPI not present. ScaLAPACK-based functionality will NOT be built")
    message(WARNING "Debug: MPI_C_FOUND is ${MPI_C_FOUND}, MPI_CXX_FOUND is ${MPI_CXX_FOUND}, MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "Ubuntu Hint: $ sudo apt-get install libopenmpi-dev")
endif()


if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    message(STATUS "Debug: Building MPI Slave")

    #
    # mpi_common needed by scidb and its slaves
    #
    set (mpi_common_src
        MPIUtils.cpp
    )

    #
    # the slave obtains it from this library
    #
    add_library(mpi_common STATIC ${mpi_common_src})
    target_link_libraries(mpi_common PUBLIC  util_lib)

    #
    # mpi management and operator base classes for scidb
    #
    set (mpi_src
        MPIManager.cpp
        MPILauncher.cpp
        MPISlaveProxy.cpp
        MPIPhysical.cpp
    )

    # mpi test operator goes into a plugin
    set (mpi_test_src
        test/MPITestLogical.cpp
        test/MPITestPhysical.cpp
        # this operator force-initializes mpi, spawns mpi processes, and cleans up
        # it needs to be renamed and/or refactored
        # (e.g. shouldn't the cleanup happen on scidb restarts without user intervention?)
        MPIInitLogical.cpp
        MPIInitPhysical.cpp
    )

    #
    # build the mpi-slave program
    #
    add_subdirectory("slaving")

else (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
      MPI_CXX_FOUND AND MPI_Fortran_FOUND)

    message(WARNING "Can't build MPI/ScaLAPACK functionality")
    message(WARNING "because one of the following is missing:")
    message(WARNING "BLAS_FOUND        is ${BLAS_FOUND}")
    message(WARNING "LAPACK_FOUND      is ${LAPACK_FOUND}")
    message(WARNING "SCALAPACK_FOUND   is ${SCALAPACK_FOUND}")
    message(WARNING "MPI_CXX_FOUND     is ${MPI_CXX_FOUND}")
    message(WARNING "MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "")

     set (mpi_test_src
          pluginNoMpi.cpp
     )

    message(WARNING "Can't build ScaLAPACK extensions")
    message(WARNING "without ScaLAPACK, MPI, LAPACK, and BLAS")
    message(WARNING "This is only a warning, SciDB will build but without")
    message(WARNING "the libdense_linear_algebra.so plugin.")
    message(WARNING "If that is not desired, please make sure the following are correctly resolved:")
    message(WARNING "----------------------------------------------------")
    message(WARNING "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_Fortran_COMPILER is ${CMAKE_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
    message(WARNING "Debug: SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
    message(WARNING "Debug: LAPACK_FOUND is    ${LAPACK_FOUND}")
    message(WARNING "Debug: BLAS_FOUND is      ${BLAS_FOUND}")
    message(WARNING "Debug: MPI_CXX_FOUND is   ${MPI_CXX_FOUND}")
    message(WARNING "Debug: MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "----------------------------------------------------")

endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

#
# complete the mpi_test library settings
#
add_library(mpi OBJECT ${mpi_src} ${mpi_common_src})
target_link_libraries(mpi PUBLIC scidb_msg_lib Boost::boost)

scidb_add_library(mpi_test SHARED ${mpi_test_src})
target_link_libraries(mpi_test PUBLIC Boost::boost)

scidb_install(TARGETS mpi_test
        # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins # see SDB-6657
        DESTINATION lib/scidb/plugins
        COMPONENT scidb-plugins
  )

message(STATUS "****************** END src/mpi/CMakeLists.txt ******************")
