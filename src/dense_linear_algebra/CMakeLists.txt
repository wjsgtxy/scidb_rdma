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
message(STATUS "*****BEGIN dense_linear_algebra/CMakeLists.txt ***********************")

#
# The library in this directory is just for dense linear algebra
# based on scalapack.  The files form dlaScaLA should be moved up into
# here.  To protect us from constant re-writing of includes, we will
# add to the default include paths
#
include_directories("${CMAKE_SOURCE_DIR}/src/dense_linear_algebra")  # include our own root

# the above is necessary to keep compatibility with the use of code in here within the
# "mpi" directory owning the "only" slave, but needing it to be build-able from here too,
# because that is where it will be shortly.

# TODO: JHM .... rename scalapackUtil to scalapack
# TODO: JHM .... move all dlaScaLA files to this "dense_linear_algebra" directory

if(SCALAPACK_TIMING)
  add_compile_definitions(SCALAPACK_TIMING)
endif()

if(SCALAPACK_DEBUG)
  add_compile_definitions(SCALAPACK_DEBUG)
endif()

if (USE_LOCAL_MPI)
  set(MPI_INCLUDE "-I. -I /home/james/packages/ompi/openmpi-1.4.5/build/ompi/include -I /home/james/packages/ompi/openmpi-1.4.5/build/opal/include")
else (USE_LOCAL_MPI)
  MESSAGE(STATUS "MPI_Fortran_COMPILER=${MPI_Fortran_COMPILER}")
endif(USE_LOCAL_MPI)

# shared library for user defined objects
#set (dense_linear_algebra_src
#    #OpsRegistrator.cpp
#)

#
# DLA is built, by default, using the platform-provided set of:
# ScaLAPACK, BLACS, Open MPI, LAPACK, AND BLAS
# (On most, but not all platforms, ScaLAPACK is a recent version that
#  contains BLACS internally, AND is not a separate package)
#
# We will call this list of 4-5 packages "the ScaLAPACK stack"
#

if (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND
    message(STATUS "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")

    #
    # build the common mpi framework code
    #
    # TODO: this will return
#    set (dense_linear_algebra_common_src mpi/MPIUtils.cpp
#                                         sharedMem/SharedMemoryIpc.cpp
#    )
#   add_library(dense_linear_algebra_common STATIC ${dense_linear_algebra_common_src})

    # NOTE: library compiles with regular compilers, not MPI compilers
    # only the slave needs MPI compilation AND ScaLAPACK+MPI libraries
    

    #
    # dense_linear_algebra library
    #

    #
    # to use a 64-bit scalapack, lapack, blas
    # make the changes listed in src/dense_linear_algebra/scalapackUtil/scalapackTypes.hpp
    # including enabling the following line
    # set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -fdefault-integer-8")

    set (dense_linear_algebra_src ${dense_linear_algebra_src}
        plugin.cpp
        # scalapack emulation:
        dlaScaLA/scalapackEmulation/numroc.f       # called from Physical operators
        dlaScaLA/scalapackEmulation/descinit.f     # called from Physical operators
                dlaScaLA/scalapackEmulation/lsame.f     # compare two single letters!
            dlaScaLA/scalapackEmulation/pdelset.f       # pdelsetOp - phys op
            dlaScaLA/scalapackEmulation/pdelget.f       # pdelgetOp - phys op
                dlaScaLA/scalapackEmulation/infog2l.f   # local indices; referenced by elset/elget?
            dlaScaLA/scalapackEmulation/pxerbla.f  # error handler

        # BLACS emulation:
            dlaScaLA/scalapackEmulation/blacs_info_fake.cpp # blacs_grid_info() from Physical operators
        scalapackUtil/reformat.cpp
        scalapackUtil/test/MPICopyLogical.cpp
        scalapackUtil/test/MPICopyPhysical.cpp
        scalapackUtil/test/slaving/mpiCopyMaster.cpp
        scalapackUtil/test/MPIRankLogical.cpp
        scalapackUtil/test/MPIRankPhysical.cpp
        scalapackUtil/test/slaving/mpiRankMaster.cpp
        scalapackUtil/ScaLAPACKLogical.cpp
        scalapackUtil/ScaLAPACKPhysical.cpp
        dlaScaLA/GEMMLogical.cpp
        dlaScaLA/GEMMPhysical.cpp
        dlaScaLA/GEMMOptions.cpp
        dlaScaLA/SVDLogical.cpp
        dlaScaLA/SVDPhysical.cpp
        dlaScaLA/slaving/pdgemmMaster.cpp
        dlaScaLA/slaving/pdgesvdMaster.cpp
        dlaScaLA/slaving/pdgesvdMasterSlave.cpp
        # test direct use of blas from a scidb plugin
        blas/LogicalBLASFlopsTest.cpp
        blas/PhysicalBLASFlopsTest.cpp
        # test direct use of lapack from a scidb plugin
        lapack/LogicalLAPACKFlopsTest.cpp
        lapack/PhysicalLAPACKFlopsTest.cpp
    )

    #
    # build the mpi-slave program  (not currently linked here, but a version will return here, "soon")
    #
    set (mpi_slave_src
        scalapackUtil/test/slaving/mpiCopySlave.cpp
        scalapackUtil/test/slaving/mpiRankSlave.cpp
        dlaScaLA/slaving/pdgemmSlave.cpp
        dlaScaLA/slaving/pdgesvdSlave.cpp
        dlaScaLA/slaving/pdgesvdMasterSlave.cpp
        dlaScaLA/slaving/slaveTools.cpp
        scalapackUtil/reformat.cpp
    )

    # Temporarily, src/mpi/slaving produces mpi_slave_scidb, which is the "everything" slave
#    scidb_add_executable(mpi_slave ${mpi_slave_src})
#   target_link_libraries(mpi_slave ${MPI_LIBRARIES})
#   target_link_libraries(mpi_slave ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} ${SCALAPACK_LIBRARIES})
#   target_link_libraries(mpi_slave ${LOG4CXX_LIBRARIES} ${PROTOBUF_LIBRARIES} ${LIBRT_LIBRARIES} ${Boost_LIBRARIES})
#   TODO: restore this directory to  have its own mpi-slave


#
# AND the tools needed to test gemm AND gesvd
#
# These 'configure_file() files are needed in ${TEST_BIN_DIR} which is ${SCIDB_BUILD_PATH}/bin
# They should be moved and/or installed into a canonical tests/bin directory.
# TEST_BIN_DIR is set in tests/harness/scidbtestharness_env.sh and only used by the follwing tests:
#       daily/stable/by_op/gemm/64instance/10_gemm_driver_allstd_o1to288c32.test
#       daily/stable/by_op/gesvd/30_svd_doSvd_verySmall.test
#       daily/stable/by_op/gesvd/31_svd_doSvdWithMetrics_verySmall.test
#       daily/stable/by_op/gesvd/32_svd_driverXS.test
#       daily/stable/by_op/gesvd/64instance/20_svd_doSvd_o1to288c32.test
#       daily/stable/by_op/gesvd/64instance/31_svd_driver_random_t1to4_o1to1024c32.test
#       daily/stable/by_op/gesvd/64instance/35_svd_driver_allstd_o1to288c32.test
#       daily/stable/by_op/gesvd/perf/30_svd_doSvd_1_S.test
#       daily/unstable/mu/mu_update.test
#       weekly/by_op/gemm/10_gemm_driver_allstd_o1to96c32.test
#       weekly/by_op/gesvd/31_svd_driver_random_t1to4_o1to96c32.test
#       weekly/by_op/gesvd/35_svd_driver_allstd_o1to96c32.test
configure_file(doSvd.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvd.sh" COPYONLY)
configure_file(doSvdDirect.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdDirect.sh" COPYONLY)
configure_file(doSvdWithMetrics.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdWithMetrics.sh" COPYONLY)
configure_file(doSvdMetric.sh "${GENERAL_OUTPUT_DIRECTORY}/doSvdMetric.sh" COPYONLY)
configure_file(diag.sh "${GENERAL_OUTPUT_DIRECTORY}/diag.sh" COPYONLY)
configure_file(test/dla_driver.py "${GENERAL_OUTPUT_DIRECTORY}/dla_driver.py" COPYONLY)

else (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND

     set (dense_linear_algebra_src ${dense_linear_algebra_src}
          pluginNoSP.cpp
     )

    message(WARNING "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    message(WARNING "Can't build ScaLAPACK extensions")
    message(WARNING "without ScaLAPACK, Open MPI, LAPACK, or a BLAS")
    message(WARNING "or without MPI_CXX")
    message(WARNING "or without MPI_Fortran interfaces.")
    message(WARNING "This is only a warning AND is not a problem unless")
    message(WARNING "you want the libdense_linear_algebra.so plugin to be built.")
    message(WARNING "")
    message(WARNING "If you intended to built it, the following will help you debug what is missing.")
    message(WARNING "-------------------------------------------------")
    message(WARNING "Debug: MPI_Fortran_COMPILER is ${MPI_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_Fortran_COMPILER is ${CMAKE_Fortran_COMPILER}")
    message(WARNING "Debug: CMAKE_CXX_COMPILER is ${CMAKE_CXX_COMPILER}")
    message(WARNING "Debug: SCALAPACK_FOUND is ${SCALAPACK_FOUND}")
    message(WARNING "Debug: LAPACK_FOUND is    ${LAPACK_FOUND}")
    message(WARNING "Debug: BLAS_FOUND is      ${BLAS_FOUND}")
    message(WARNING "Debug: MPI_CXX_FOUND is   ${MPI_CXX_FOUND}")
    message(WARNING "Debug: MPI_Fortran_FOUND is ${MPI_Fortran_FOUND}")
    message(WARNING "----------------------------------------------------")

endif (MPI_CXX_FOUND AND MPI_Fortran_FOUND) # SCALAPACK_FOUND AND LAPACK_FOUND AND BLAS_FOUND AND


#
# complete the dense_linear_algebra plugin settings
#
scidb_add_library(dense_linear_algebra SHARED ${dense_linear_algebra_src})
target_link_libraries(dense_linear_algebra PUBLIC Boost::date_time)

scidb_install(TARGETS dense_linear_algebra
              # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins  # see SDB-6657
              DESTINATION lib/scidb/plugins
              COMPONENT scidb-plugins
              RENAME_SUFFIX scidb
  )
# Update alternatives for library
install(CODE "execute_process(COMMAND \
${CMAKE_SOURCE_DIR}/utils/update_alternatives.sh \
 ${CMAKE_INSTALL_PREFIX} \
 lib/scidb/plugins \
 dense_linear_algebra \
 ${CMAKE_SHARED_LIBRARY_SUFFIX} \
 ${SCIDB_VERSION_MAJOR} \
 ${SCIDB_VERSION_MINOR} \
 scidb \
)"
  )

message(STATUS "*****END linear_algebra/CMakeLists.txt ***********************")
