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

# 3.4.3 provides generation of python files for protobuf files.
# 3.12  has add_compile_definitions.
# 3.13  allows install(TARGETS ...) from the root directory.
cmake_minimum_required(VERSION 3.13)

# TODO: Remove this. see SDB-6589
# This work-around to set the environment variables should REALLY not exist.
# CXX, CC, FC should be set by the user (for example, in run.py) which would
# decouple the concept completely and correctly.
#
# Use old-school gcc-4.9 if it exist on the system
# by setting the CXX, CC, FC environment variables (if not set)
# before calling the project() command.
# If the environment variables were not set and the 'special 4.9'
# doesn't exist then just let CMake do what it normally does.
#if (NOT DEFINED ENV{CXX})
#  if(EXISTS "/usr/bin/g++-4.9")
#    set(ENV{CXX} /usr/bin/g++-4.9)
#  elseif(EXISTS "/opt/rh/devtoolset-3/root/usr/bin/g++")
#    set(ENV{CXX} /opt/rh/devtoolset-3/root/usr/bin/g++)
#  endif()
#endif()
#if (NOT DEFINED ENV{CC})
#  if(EXISTS "/usr/bin/gcc-4.9")
#    set(ENV{CC} /usr/bin/gcc-4.9)
#  elseif(EXISTS "/opt/rh/devtoolset-3/root/usr/bin/gcc")
#    set(ENV{CC} /opt/rh/devtoolset-3/root/usr/bin/gcc)
#  endif()
#endif()
#if (NOT DEFINED ENV{FC})
#  if(EXISTS "/usr/bin/gfortran-4.9")
#    set(ENV{FC}  /usr/bin/gfortran-4.9)
#  elseif(EXISTS "/opt/rh/devtoolset-3/root/usr/bin/gfortran")
#    set(ENV{FC}  /opt/rh/devtoolset-3/root/usr/bin/gfortran)
#  endif()
#endif()

# dz test remote
set(ENV{CXX} /usr/bin/g++-7)
set(ENV{CC} /usr/bin/gcc-7)
#set(ENV{FC}  /usr/bin/gfortran-7)
# set(ENV{CXX} /usr/bin/g++-5)
# set(ENV{CC} /usr/bin/gcc-5)
set(ENV{FC}  /usr/bin/gfortran-5) # szu_lab ubuntu1604 dz test remotehahah

project(SciDB C CXX Fortran)

set(CMAKE_ENABLE_EXPORTS True)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")
include(CMakeBuildFlags.cmake) # configure CMAKE_BUILD_TYPES and default compilation/link flags
SET(CMAKE_BUILD_TYPE "Debug")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

include(SciDBFunctions)
set_version_from_file(SCIDB)
# Set the default CMAKE_INSTALL_PREFIX if not provided by the initial setup call of cmake.
#    CMake sets CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT variable to a TRUE
#    value when the CMAKE_INSTALL_PREFIX has just been initialized to its
#    default value, typically on the first run of CMake within a new build
#    tree. This can be used by project code to change the default without
#    overriding a user-provided value:
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  #set(CMAKE_INSTALL_PREFIX "/opt/scidb/${SCIDB_SHORT_VERSION}" CACHE STRING "Install path" FORCE)
  set(CMAKE_INSTALL_PREFIX "/home/dengzhong/scidb/install" CACHE STRING "Install path" FORCE)
  message(STATUS "CMAKE_INSTALL_PREFIX Initialized to default setting to: " ${CMAKE_INSTALL_PREFIX})
else(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  message(STATUS "CMAKE_INSTALL_PREFIX not Initialized to default keeping: " ${CMAKE_INSTALL_PREFIX})
endif(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# # GNUInstallDirs MUST be included AFTER setting CMAKE_INSTALL_PREFIX because GNUInstallDirs
# # uses that variable to set the CMAKE_INSTALL_<dir> values.
include(GNUInstallDirs)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_INSTALL_MESSAGE LAZY)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)   # -std=c++14 NOT -std=gnu++14
set(CMAKE_POSITION_INDEPENDENT_CODE ON) # set -fPIC/-fPIE everwhere
# defines
#   See NOTES section of https://en.cppreference.com/w/cpp/types/integer
#       The C committee recommended that PRI[iuxd]64 expand to nothing if
#       __STDC_FORMAT_MACROS, __STDC_LIMIT_MACROS are not defined before
#       including <cinttypes> (or <inttypes.h>), but the C++ committee rejected
#       that recommendation. Some implementers (like glibc 2.17) followed the C
#       committees recommendation.  For "safety". We add the defines everywhere
#       since we use these macros in printf, sscanf in several locations.
add_compile_definitions(__STDC_FORMAT_MACROS __STDC_LIMIT_MACROS)
# TODO: eliminate PROJECT_ROOT
#    This is used in include/util/StringUtil.h for use by "REL_FILE" macro so it
#    could be used via target_compile_definitions BUT WE WOULD HAVE TO make
#    certain that all of the target_link_libraries( X PUBLIC util_lib) were
#    correct throughout -- a much bigger project
#           target_compile_definitions(util_lib PUBLIC PROJECT_ROOT=\"${CMAKE_SOURCE_DIR}/\")
add_compile_definitions(PROJECT_ROOT=\"${CMAKE_SOURCE_DIR}/\")


set(GENERAL_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(LIB_LINKS_DIRECTORY "${CMAKE_BINARY_DIR}/softlinks")

# dz add 忽略编译告警 https://blog.csdn.net/motingqian/article/details/118752259
macro(remove_warnings) # dz 注意，这个位置很重要，要在include其他子文件夹的签名定义，否则子文件里面找不到这个
  add_definitions(
          -Wno-terminate
          #-Wno-unused-value
          #-Wno-unknown-pragmas
          #-Wno-sequence-point
          #-Wno-delete-non-virtual-dtor
          #-Wno-unused-but-set-variable
          #-Wno-sign-compare
          #-Wno-unused-variable
          #-Wno-return-local-addr
          #-Wno-unused-function
          #-Wno-deprecated-declarations
  )
endmacro()

################################################################
# Setting DT_RUNPATH in the elf header:
#
# A. review of library resolution steps
#
# From "man ld.so":
##The shared libraries needed by the program are searched for in the following order:
##
## 1. (ELF only) Using the directories specified in the DT_RPATH dynamic section attribute of the binary if
## present and DT_RUNPATH attribute does not exist. Use of DT_RPATH is deprecated.
##
## 2. Using the environment variable LD_LIBRARY_PATH. Except if the executable is a set-user-ID/set-group-ID
## binary, in which case it is ignored.
##
## 3. (ELF only) Using the directories specified in the DT_RUNPATH dynamic section attribute of the binary if
## present.
##
## 4. From the cache file /etc/ld.so.cache which contains a compiled list of candidate libraries previously
## found in the augmented library path. If, however, the binary was linked with the -z nodeflib linker
## option, libraries in the default library paths are skipped.
##
## 5. In the default path /lib, and then /usr/lib. If the binary was linked with the -z nodeflib linker
## option, this step is skipped.
#
# NOTE:
#  a. We set DT_RUNPATH so (1) does not apply.
#  b. If the user sets LD_LIBRARY_PATH, then (2) applies, and it is the user's repsonsibility.
#  c. Resolution is normally via step (3).
#  d. Only if that fails, (5) and (6) do apply [we do not use -z nodeflib at this time.]
#
# B. $ORIGIN
#
# From "man ld.so":
##RPATH TOKEN EXPANSION
## The runtime linker provides a number of tokens that can be used in an rpath specification (DT_RPATH or DT_RUNPATH).
##
## $ORIGIN
## ld.so understands the string $ORIGIN (or equivalently ${ORIGIN}) in an rpath specification to mean the directory
## containing the application executable. Thus, an application located in somedir/app could be compiled with
## gcc -Wl,-rpath,'$ORIGIN/../lib' so that it finds an associated shared library in somedir/lib no matter where
## somedir is located in the directory hierarchy.
#
# NOTE:
#   We use $ORIGIN to find the locations
#      ../scidb/<version>/lib and
#      ../scidb/<version>/lib/scidb/plugins
#   from
#        either location above or
#      ../scidb/<version>/bin.
#
# by setting it to "$ORIGIN/../lib:$ORIGIN/../.."
#
#   And we use $ORIGIN/../plugins to find plugins from plugins
#
# NOTE:
#  this also allows resolution to ../scidb/<version>.  This will be fixed at a later point.
#
# C. CMAKE macros and the linker
#
# It should be noted that there is no command to the linker to set RUNPATH directly.
# Instead we set RPATH and pass --enable-new-dtags to the linker.
# The linker then sets DT_RUNPATH rather than DT_RPATH.
#
#  Setting CMAKE_SHARED_LINKER_FLAGS to "-Wl,--enable-new-dtags"
#    means CMAKE macros that set RPATH for shared objects results in RUNPATH being set.
#  Setting CMAKE_EXE_LINKER_FLAGS to "-Wl,--enable-new-dtags"
#    means CMAKE macros that set RPATH for programs results in RUNPATH being set.
#  Setting CMAKE_SKIP_BUILD_RPATH to FALSE
#    assures us that CMAKE sets RPATH at build time.
#  Setting CMAKE_BUILD_WITH_INSTALL_RPATH to TRUE tells CMAKE the object is built with the install time RPATH
#    (which for our purpose is the same as build time) so it will not re-instrument at install time.
#  Setting CMAKE_INSTALL_RPATH_USE_LINK_PATH to TRUE tells CMAKE to add any linkages that were used at build time
#    as absolute paths. This applies to boost objects in /opt/scidb/<version>/3rdparty/boost/lib.
#
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
# SDB-6657: 'Cpp code should not hard-code library locations of files.'
#    We should use the distro/architecture library location
#    (CMAKE_INSTALL_LIBDIR) in the RPATH once that location in the cpp files are
#    no longer hard-coded
# set(CMAKE_INSTALL_RPATH "$ORIGIN/../plugins:$ORIGIN/../${CMAKE_INSTALL_LIBDIR}:$ORIGIN/../../")
set(CMAKE_INSTALL_RPATH "$ORIGIN/../plugins:$ORIGIN/../lib:$ORIGIN/../../")
set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--enable-new-dtags")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--enable-new-dtags")


#
# Find MPI flavor
#
# Order of the flavors IS IMPORTANT.
#   Searching for the string in the path occurs in the order specified, so putting mpich
#   before mpich2scidb would result in incorrect result.
#
# NOTE: We have a patched FindMPI from CMake 3.14.4 in cmake/Modules to use
#       MPI_EXECUTABLE_SUFFIX correctly. A patch was submitted to kitware/CMake
#       and is scheduled as a part of 3.16.0:
#       https://gitlab.kitware.com/cmake/cmake/merge_requests/3496
set(_SCIDB_MPI_FLAVORS "mpich2scidb"  "openmpi" "mpich")
# Look first to see if the flavor is appended to the executable. (debian distributions)
foreach (flavor IN ITEMS  ${_SCIDB_MPI_FLAVORS} )
  if (NOT "${MPI_CXX_FOUND}")
    set(MPI_EXECUTABLE_SUFFIX ".${flavor}")
    find_package(MPI)
  endif()
  unset(MPI_EXECUTABLE_SUFFIX)
endforeach()
# If We couldn't find an MPI flavor by appending the suffix, try one last time by just
# attempting to find MPI with no suffixes.
#   This is the case with non-debian distributions.  Unfortunately, we need to do this
#   after checking the suffixes first.
if (NOT "${MPI_CXX_FOUND}")
  find_package(MPI)
endif()
# If we found MPI, then we need to set the MPI_FLAVOR and MPI_INST_DIR which are
# substituted as "default" command line options in system/Constants.cpp
if(MPI_CXX_FOUND)
  execute_process(
    COMMAND readlink -f ${MPI_CXX_COMPILER}
    OUTPUT_VARIABLE MPI_CXX_COMPILER_FULL_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
  string(REGEX REPLACE "/bin/[^/]*$" "" MPI_INST_DIR "${MPI_CXX_COMPILER_FULL_PATH}")
  foreach(_FLAVOR IN ITEMS ${_SCIDB_MPI_FLAVORS})
    if (NOT MPI_FLAVOR AND "${MPI_CXX_INCLUDE_DIRS}"  MATCHES ${_FLAVOR})
      set(MPI_FLAVOR ${_FLAVOR})
    endif()
  endforeach()
endif()
message(STATUS " ------- MPI_INST_DIR = ${MPI_INST_DIR} ---")
message(STATUS " ------- MPI_FLAVOR = ${MPI_FLAVOR} ---")

find_package(LibRT REQUIRED)
# BOOST CONFIG
#set(BOOST_ROOT /opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/boost)
set( BOOST_ROOT "/home/dengzhong/boost_1_72_0" ) # dz szu_lab_ubuntu1604
set(Boost_NO_BOOST_CMAKE TRUE)
  find_package(Boost 1.72 REQUIRED
  # find_package(Boost 1.54 REQUIRED  
    COMPONENTS date_time
               filesystem
               program_options
               regex
               serialization
               system
               thread
               atomic
    )
find_package(Protobuf REQUIRED)
find_package(Log4CXX REQUIRED)
find_package(Doxygen)
find_package(LibPQXX REQUIRED)
find_package(LibPQ REQUIRED)
find_package(OpenSSL 0.9.8 REQUIRED)
#
# configure CPPUNIT
#
find_package(PkgConfig)
pkg_check_modules(CPPUNIT cppunit)

find_package(FLEX 2.5.35 REQUIRED)
find_package(BISON 2.4 REQUIRED)

find_package(SED REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(Threads REQUIRED)
find_package(PythonInterp REQUIRED)
find_package(EditLine REQUIRED)

#
# configure BLAS & LAPACK
#
find_package(MKL_BLAS) # dz mkl是intel实现的bals和lapack库，性能更好一些，现在szu_lab平台报错了，先屏蔽看看,后续：不行，还是dso missing 错误
if(MKL_BLAS_FOUND)
    # emulate what find_package(BLAS) and find_package(LAPACK)
    # would do if they supported MKL BLAS and LAPACK
    set(BLAS_LIBRARIES ${MKL_BLAS_LIBRARIES})
    set(LAPACK_LIBRARIES ${MKL_BLAS_LIBRARIES})
    set(BLAS_FOUND "TRUE")
    set(LAPACK_FOUND "TRUE")
else()
    message(SEND_ERROR "Can't configure scidb for MKL BLAS LAPACK. Not supported at this time.")
    message(FATAL_ERROR "To use platform libs at your own risk, try commenting out this fatal message.")
    find_package(BLAS REQUIRED)
    find_package(LAPACK REQUIRED)
endif()


#
# INCLUDE DIRECTORIES
#
include_directories("${CMAKE_SOURCE_DIR}/src")
include_directories("${CMAKE_SOURCE_DIR}/include")
include_directories("${CMAKE_SOURCE_DIR}/utils")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/src")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/utils")
include_directories(${LOG4CXX_INCLUDE_DIRS})
include_directories(${LIBPQXX_INCLUDE_DIRS})
include_directories(${LIBPQ_INCLUDE_DIRS})
include_directories("${CMAKE_SOURCE_DIR}/extern")

link_directories(${LIBPQXX_LIBRARY_DIRS})
link_directories(${LIBPQ_LIBRARY_DIRS})

#
# SUBDIRECTORIES
#
add_subdirectory("extern")
add_subdirectory("src")
add_subdirectory("utils")
if (NOT WITHOUT_SERVER)
    add_subdirectory("tests")  # dz todo 先不要test了
    add_subdirectory("examples")
endif()
add_subdirectory("scripts")

#
# TESTING: CTest, Dart, and Cdash
#
ENABLE_TESTING() # dz todo 屏蔽test？
INCLUDE(CTest)

if (CDASH_CONFIG)
  # Community Edition
  ADD_TEST(ScidbInstall "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/install.sh ${CDASH_CONFIG})
  ADD_TEST(ScidbTestFull "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbTestWeekly "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbResult "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "collect_cleanup")
  # Enterprise Edition, scidb repository
  ADD_TEST(ScidbTrustInstall "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/p4/install.sh ${CDASH_CONFIG})
  ADD_TEST(ScidbTrustTestFull "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbTrustTestWeekly "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbTrustResult "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "collect_cleanup")
  #
  ADD_TEST(ScidbNamespaceInstall "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/p4/install.sh ${CDASH_CONFIG})
  ADD_TEST(ScidbNamespaceTestFull "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbNamespaceTestWeekly "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "prepare_run")
  ADD_TEST(ScidbNamespaceResult "/bin/bash" ${CMAKE_BINARY_DIR}/cdash2/scidb/harness.sh ${CDASH_CONFIG} "collect_cleanup")
  #
  # TEST INSTALL TIMEOUT
  #
  if (NOT INSTALL_TIMEOUT)
    set(INSTALL_TIMEOUT 3600) # 1 hour.
  endif(NOT INSTALL_TIMEOUT)
  #
  # TEST TIMEOUT
  #
  if (NOT HARNESS_TEST_TIMEOUT)
    if("${CMAKE_BUILD_TYPE}" STREQUAL "Valgrind")
      set(HARNESS_TEST_TIMEOUT 86400) # 24 hours - for Valgrind build/tests.
    else()
      set(HARNESS_TEST_TIMEOUT 27000) # 7.5 hours - for regular build/tests.
    endif()
  endif(NOT HARNESS_TEST_TIMEOUT)
  #
  # TEST WEEKLY TIMEOUT
  #
  if (NOT HARNESS_TEST_WEEKLY_TIMEOUT)
    if("${CMAKE_BUILD_TYPE}" STREQUAL "Valgrind")
      set(HARNESS_TEST_WEEKLY_TIMEOUT 86400) # 24 hours - for Valgrind build/tests.
    else()
      set(HARNESS_TEST_WEEKLY_TIMEOUT 86400) # 24 hours - for weekly build/tests.
    endif()
  endif(NOT HARNESS_TEST_WEEKLY_TIMEOUT)
  #
  # TEST RESULTS COLLECTION TIMEOUT
  #
  if (NOT RESULTS_COLLECTION_TIMEOUT)
    set(RESULTS_COLLECTION_TIMEOUT 3600) # 1 hour.
  endif(NOT RESULTS_COLLECTION_TIMEOUT)

  set_tests_properties (ScidbInstall PROPERTIES TIMEOUT ${INSTALL_TIMEOUT})
  set_tests_properties (ScidbTestFull PROPERTIES TIMEOUT ${HARNESS_TEST_TIMEOUT})
  set_tests_properties (ScidbTestWeekly PROPERTIES TIMEOUT ${HARNESS_TEST_WEEKLY_TIMEOUT})
  set_tests_properties (ScidbResult PROPERTIES TIMEOUT ${RESULTS_COLLECTION_TIMEOUT})


  set_tests_properties (ScidbTrustInstall PROPERTIES TIMEOUT ${INSTALL_TIMEOUT})
  set_tests_properties (ScidbTrustTestFull PROPERTIES TIMEOUT ${HARNESS_TEST_TIMEOUT})
  set_tests_properties (ScidbTrustTestWeekly PROPERTIES TIMEOUT ${HARNESS_TEST_WEEKLY_TIMEOUT})
  set_tests_properties (ScidbTrustResult PROPERTIES TIMEOUT ${RESULTS_COLLECTION_TIMEOUT})

  set_tests_properties (ScidbNamespaceInstall PROPERTIES TIMEOUT ${INSTALL_TIMEOUT})
  set_tests_properties (ScidbNamespaceTestFull PROPERTIES TIMEOUT ${HARNESS_TEST_TIMEOUT})
  set_tests_properties (ScidbNamespaceTestWeekly PROPERTIES TIMEOUT ${HARNESS_TEST_WEEKLY_TIMEOUT})
  set_tests_properties (ScidbNamespaceResult PROPERTIES TIMEOUT ${RESULTS_COLLECTION_TIMEOUT})
endif()
#
# Custom uninstall target
#
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake"
    IMMEDIATE @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/uninstall.cmake)
#
# all outputs must go to CMAKE_BINARY_DIR to support out-of-tree build and test
#
# TODO: these should be done at make time, not cmake time, so a developer need
#       not do a full rebuild when only changing a test file.
#       (HINT add CMakeLists.txt files, with targets with ADD_CUSTOM_COMMAND()s intead of execute_process()
#
# COPY TESTS
#
message(STATUS "copying ${CMAKE_SOURCE_DIR}/tests to ${CMAKE_BINARY_DIR}/tests")
execute_process(COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/tests
                                                           ${CMAKE_BINARY_DIR}/tests)

set(ignoreMe "${RUN_PY}")
if (DEFINED RUN_PY)
  # Remove the actual test cases and expected results from the build area:
  # they will be accessed directly from the source tree.
  message(STATUS "removing ${CMAKE_SOURCE_DIR}/tests/harness/testcases/t from ${CMAKE_BINARY_DIR}/tests/harness/testcases/t")
  execute_process(COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/tests/harness/testcases/t)
  execute_process(COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/tests/harness/testcases/data)
endif (DEFINED RUN_PY)

#
# COPY VERSION
#
execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/version
                                                 ${CMAKE_BINARY_DIR}/version)

#
# INSTALL
#
include(install.cmake)

message(STATUS "****************** END CMakeLists.txt ******************")
