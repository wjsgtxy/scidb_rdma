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

#
# The program in this directory is not just for mpi.
# Ideally, we would give the mpi directory its own test-slave program, but
# at the moment, SciDB can only manage one slave program.
# So for a while we will build it here, but include slave modules in it from elsewhere.
# Until that is cleaned up, we will extend the include searching to include the root
# directory of all such "elsewhere" slave modules.
# TODO: once dla-scalapack is organized and stable in SciDB, reverse this dependency.
#       and have the dla-scalapack slave include the mpi testing.  This is more legitimate
#       because that code already depends on the mpi directory tree, and the reverse is
#       not desireable (general thing: mpi depending on specific thing: dla-scalapack).
include_directories("${CMAKE_SOURCE_DIR}/src/dense_linear_algebra")


message(STATUS "****************** BEGIN mpi/slaving/CMakeLists.txt ******************")

if (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    MPI_CXX_FOUND AND MPI_Fortran_FOUND)
    #
    # build the mpi-slave program
    #
    set (mpi_slave_common_src
    #    NOTE: the following items are to be moved back into their
    #    own slave in the dla area
         ../../dense_linear_algebra/scalapackUtil/test/slaving/mpiCopySlave.cpp
         ../../dense_linear_algebra/scalapackUtil/test/slaving/mpiRankSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgemmSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgesvdSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/pdgesvdMasterSlave.cpp
         ../../dense_linear_algebra/dlaScaLA/slaving/slaveTools.cpp
         ../../dense_linear_algebra/scalapackUtil/reformat.cpp
    )

    add_library(mpi_slave_common STATIC ${mpi_slave_common_src})

    message(STATUS "Debug:src/mpi/slaving BLAS_LIBRARIES  is ${BLAS_LIBRARIES}")
    message(STATUS "Debug:src/mpi/slaving LAPACK_LIBRARIES is ${LAPACK_LIBRARIES}")
    message(STATUS "Debug:src/mpi/slaving SCALAPACK_LIBRARIES is ${SCALAPACK_LIBRARIES}")
    message(STATUS "Debug:src/mpi/slaving MPI_LIBRARIES is ${MPI_LIBRARIES}")

    # Because SCALAPACK_LIBRARIES has a .so with linkage to platform libblas and liblapack,
    # we link in {BLAS,LAPACK}_LIBRARIES first.  When those are set to MKL versions,
    # then they can resolve to MKL before SCALAPACK_LIBRARIES resolves to platform.
    # Uncomment the next line to verify the order.
    set(CMAKE_VERBOSE_MAKEFILE ON)
    target_link_libraries(mpi_slave_common
      PUBLIC ${BLAS_LIBRARIES}
             ${LAPACK_LIBRARIES}
             ${SCALAPACK_LIBRARIES}
             MPI::MPI_CXX
             mpi_common
      )

    # to use a 64-bit scalapack,
    # make the changes listed in src/dense_linear_algebra/scalapackUtil/scalapackTypes.hpp
    # including change the linkage of mpi_slave_common (as the following 8 lines do in a local build)
    #
    # add_library(scalapack STATIC IMPORTED GLOBAL)
    # set_target_properties(scalapack PROPERTIES IMPORTED_LOCATION /home/<user>/work/gt.scalapack-svn/lib/libscalapack.a)
    # add_library(lapack STATIC IMPORTED GLOBAL)
    # set_target_properties(lapack PROPERTIES IMPORTED_LOCATION /home/<user>/work/gt.Reference-LAPACK.lapack/liblapack.a)
    # add_library(refblas STATIC IMPORTED GLOBAL)
    # set_target_properties(refblas PROPERTIES IMPORTED_LOCATION /home/<user>/work/gt.Reference-LAPACK.lapack/librefblas.a)
    # this example will be removed once we have completely switched over to 64 bit only
    # target_link_libraries(mpi_slave_common  scalapack lapack refblas mpi_common ${MPI_LIBRARIES})


    #
    # first one is a stripped-down version that is a pure mpi/scalapack program
    # used as a unit test that avoids all scidb-isms, useful for 1st-level
    # divide-and-conquer if slave problems occur when driven by scidb
    #

    scidb_add_executable(mpi_slave_direct mpi_slave_direct.cpp ../../dense_linear_algebra/blas/initMathLibs.cpp)
    scidb_add_executable(mpi_slave_scidb  mpi_slave_scidb.cpp  ../../dense_linear_algebra/blas/initMathLibs.cpp)

    scidb_install(TARGETS mpi_slave_scidb
        # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins # see SDB-6657
        DESTINATION lib/scidb/plugins
        COMPONENT scidb-plugins
    )

    # Temporary until separate dla-scalapack slave
    target_link_libraries(mpi_slave_direct
      PUBLIC mpi_slave_common
             blacs-openmpi # dz add 2022-11-11 解决dso missing问题
             scidbclient
             scidb_msg_lib
      )

    target_link_libraries(mpi_slave_scidb
      PUBLIC mpi_slave_common
             scidbclient
             blacs-openmpi # dz add 2022-11-11 解决dso missing问题
             scidb_msg_lib
             ${LOG4CXX_LIBRARIES}
             protobuf::libprotobuf
             ${LIBRT_LIBRARIES}
             Boost::filesystem
             Boost::program_options
             Boost::regex
             Boost::serialization
             Boost::system
      )


endif (SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
       MPI_CXX_FOUND AND MPI_Fortran_FOUND)

message(STATUS "****************** END mpi/slaving/CMakeLists.txt ******************")
