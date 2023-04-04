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


# Try to find the installed MKL BLAS libraries
#
# Use as FIND_PACKAGE(MKL_BLAS)
#  <name> is MKL_BLAS
#  when successful the following are set:
#
#  <name>_FOUND - system has MKL BLAS installed
#                 - but there are no include files that should be referenced at this time
#  <name>_LIBRARIES - list of the libraries that must be linked
#

include(LibFindMacros)

# 0. This file follows
#    http://www.cmake.org/Wiki/CMake:How_To_Find_Libraries#Writing_find_modules
#    retrieved 6/05/2013 with some addtional changes.
#    Extra verbosity is explanatory or to serve as a template for future
#    limited-function FindXXXX.cmake implementations.
# ------------------------------------------------------------
# 1. Dependencies:
#        use find_package to detect any libraries these depend on
#        note that QUIETLY and REQUIRED are to be forwarded to dependency lookups
# 2. optional use of pkg-config to detect include and library paths, if pkg-config available
# 3. use find_path and find_library
#        results SAVED in <name>_INCLUDE_DIR and <name>_LIBRARY (singular) [???]
#        (paths supplied by pkg-config should only be used as hints, the cmake hardcoded lookups must also be allowed)
# 4. set <name>_INCLUDE_DIRS (plural) to "<name>_INCLUDE_DIR <dependencyN>_INCLUDE_DIRS [...]"
# 5. set <name>_PROCESS_LIBS to "<name>_LIBRARY <dependencyN>_LIBRARIES [...]"
#    NOTE that dependencies are in the plural forms
# 6. call libfind_process() to do checks and set the <name>_FOUND variable and print the
#         standardized success or failure message
#         NOTE: at one time a call to find_package_handle_standard_args() was recommended
#         but the libfind_process() wrapper is now recommended
# --------------------------------------------------------------


# NOTE: these are the only locations we want to look for these files.  Any other location is suspect,
#       so we will not use find_library() as in normal Find modules.  This isn't a normal find module,
#       it is being used for making and installing some particular licensed binaries in a particular way.
find_path(MKL_BLAS_LIB_DIR libmkl_rt.so
          NO_DEFAULT_PATH
          PATHS /opt/intel/compilers_and_libraries/linux/mkl/lib/intel64)
          # above path is location where installed by yum from intel repo
          # if this does not work for other distros, add additional locations

if(MKL_BLAS_LIB_DIR)

    # 1. N/A
    # 2. N/A
    # 3. use find_path and find_library to find libmkl_rt
    #        results SAVED in <name>_INCLUDE_DIR and <name>_LIBRARY (singular) [???]
    # note: libmkl_rt.so is the directly called library. (rt means runtime selection of optimization)
    find_library(MKL_BLAS_LIBRARY mkl_rt NO_DEFAULT_PATH PATHS ${MKL_BLAS_LIB_DIR})
    message(STATUS "DEBUG MKL_BLAS_LIBRARY=${MKL_BLAS_LIBRARY}")
endif()

# 4. N/A
# 5. set(<name>_LIBRARIES to <name>_LIBRARY <dependencyN>_LIBRARIES [...])
#
set(MKL_BLAS_PROCESS_LIBS "MKL_BLAS_LIBRARY")

#
# 6. call libfind_process() to set the <name>_FOUND variable and print standard messages
#
libfind_process(MKL_BLAS)

