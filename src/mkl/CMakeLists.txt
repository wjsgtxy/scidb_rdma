########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2016-2019 SciDB, Inc.
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
message(STATUS "*****BEGIN src/mkl/CMakeLists.txt *******************************")

# the purpose of this file is to create soft links that
# override the platform's blas and lapack libraries with mkl
# even when other packages (e.g. ScaLAPACK) may have been built with the
# platform versions and ld.so sees that (can check with ldd)

# if (Intel MKL is installed)
#     set up links in /opt/scidb/<rel>/bin which will be found preferentially
#     to the ones in /usr/lib64 due to how we configure rpath
# endif

if(MKL_BLAS_FOUND)
    message(DEBUG, "adding overriding softlinks from platform blas and lapack to mkl")

    # add the soft links that will override plaform's BLAS and LAPACK with libmkl_rt.so

    # TODO: find a better way to name the platform versions
    set(PLATFORM_BLAS_NAME "libblas.so.3")
    set(PLATFORM_LAPACK_NAME "liblapack.so.3")

    # create the directory
    execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory  ${LIB_LINKS_DIRECTORY}/)
    # create the soft links
    message(STATUS "creating symlink from ${LIB_LINKS_DIRECTORY}/${PLATFORM_BLAS_NAME} to ${MKL_BLAS_LIBRARY}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${MKL_BLAS_LIBRARY} ${LIB_LINKS_DIRECTORY}/${PLATFORM_BLAS_NAME})
    message(STATUS "creating symlink from ${LIB_LINKS_DIRECTORY}/${PLATFORM_LAPACK_NAME} to ${MKL_BLAS_LIBRARY}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${MKL_BLAS_LIBRARY} ${LIB_LINKS_DIRECTORY}/${PLATFORM_LAPACK_NAME})
else()
    message(SEND_ERROR "Unsupported configuration, missing libblas, liblapack redirects to mkl")
endif()

message(STATUS "***** END src/mkl/CMakeLists.txt *******************************")
