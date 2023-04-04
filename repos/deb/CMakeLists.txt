########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2019-2019 SciDB, Inc.
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
# CMake setup
#
cmake_minimum_required(VERSION 3.4.3)

if(EXISTS "/usr/bin/g++-4.9")
    set(CMAKE_C_COMPILER "/usr/bin/gcc-4.9")
    set(CMAKE_CXX_COMPILER "/usr/bin/g++-4.9")
elseif(EXISTS "/opt/rh/devtoolset-3/root/usr/bin/g++")
    set(CMAKE_C_COMPILER "/opt/rh/devtoolset-3/root/usr/bin/gcc")
    set(CMAKE_CXX_COMPILER "/opt/rh/devtoolset-3/root/usr/bin/g++")
else()
    message(FATAL_ERROR "CMakeLists.txt: cannot find g++ 4.9")
endif()

PROJECT(scidb C CXX)
#
# SciDB setup
#   Get SciDB's version
#
file(READ "${CMAKE_SOURCE_DIR}/../../version" _SCIDB_VERSION)
STRING(REGEX MATCH "^([0-9]*)\\.([0-9]*)\\.([0-9]*).*$" __SCIDB_VERSION "${_SCIDB_VERSION}")
if(NOT __SCIDB_VERSION)
    message(FATAL_ERROR "Can not parse 'version' file")
endif()
set(SCIDB_VERSION_MAJOR "${CMAKE_MATCH_1}")
set(SCIDB_VERSION_MINOR "${CMAKE_MATCH_2}")
set(SCIDB_VERSION_PATCH "${CMAKE_MATCH_3}")
set(SCIDB_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}.${SCIDB_VERSION_PATCH}")
set(SCIDB_SHORT_VERSION "${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}")

set(CPACK_PACKAGE_VERSION_MAJOR "${SCIDB_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${SCIDB_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${SCIDB_VERSION_PATCH}")

set(CMAKE_INSTALL_PREFIX "/opt/scidb/${SCIDB_SHORT_VERSION}")
#
# CMake build settings
#
configure_file(postinst.in postinst @ONLY)
# CE
configure_file(repos_common.sh.in repos_common.sh @ONLY)
execute_process(COMMAND sed "-i" "s/%/@/" "${CMAKE_CURRENT_BINARY_DIR}/repos_common.sh")
configure_file(repos_ce.sh.in repos_ce.sh @ONLY)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_common.sh DESTINATION repos COMPONENT community)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_ce.sh DESTINATION repos COMPONENT community)
# EE
configure_file(repos_common.sh.in repos_common.sh @ONLY)
execute_process(COMMAND sed "-i" "s/%/@/" "${CMAKE_CURRENT_BINARY_DIR}/repos_common.sh")
configure_file(repos_ee.sh.in repos_ee.sh @ONLY)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_common.sh DESTINATION repos COMPONENT enterprise)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_ee.sh DESTINATION repos COMPONENT enterprise)
# DEVEL
configure_file(repos_common.sh.in repos_common.sh @ONLY)
execute_process(COMMAND sed "-i" "s/%/@/" "${CMAKE_CURRENT_BINARY_DIR}/repos_common.sh")
configure_file(repos_devel.sh.in repos_devel.sh @ONLY)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_common.sh DESTINATION repos COMPONENT development)
install(PROGRAMS ${CMAKE_BINARY_DIR}/repos_devel.sh DESTINATION repos COMPONENT development)
#
# CPack directives
#   Package Names
SET(CPACK_DEBIAN_COMMUNITY_PACKAGE_NAME   "scidb-${SCIDB_VERSION}-repos")
SET(CPACK_DEBIAN_ENTERPRISE_PACKAGE_NAME  "paradigm4-${SCIDB_VERSION}-repos")
SET(CPACK_DEBIAN_DEVELOPMENT_PACKAGE_NAME "scidb-${SCIDB_VERSION}-dev-repos")
#   File Names
SET(CPACK_DEBIAN_COMMUNITY_FILE_NAME   scidb-${SCIDB_VERSION}-repos.deb)
SET(CPACK_DEBIAN_ENTERPRISE_FILE_NAME  paradigm4-${SCIDB_VERSION}-repos.deb)
SET(CPACK_DEBIAN_DEVELOPMENT_FILE_NAME scidb-${SCIDB_VERSION}-dev-repos.deb)
#   Package Information
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "Martin Resnick")
SET(CPACK_PACKAGE_VENDOR "Paradigm4")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Repos to install SciDB")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/../ReadMe.txt")
SET(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/../Copyright.txt")
#   Where to put them
SET(CPACK_PACKAGE_INSTALL_DIRECTORY "/opt/scidb/${SCIDB_SHORT_VERSION}")
set(CPACK_PACKAGING_INSTALL_PREFIX "/opt/scidb/${SCIDB_SHORT_VERSION}")
#  Here we go...
SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEB_COMPONENT_INSTALL ON)
SET(CPACK_DEBIAN_COMMUNITY_PACKAGE_CONTROL_EXTRA "${CMAKE_BINARY_DIR}/repos_ce.sh;${CMAKE_BINARY_DIR}/postinst")
SET(CPACK_DEBIAN_ENTERPRISE_PACKAGE_CONTROL_EXTRA "${CMAKE_BINARY_DIR}/repos_ee.sh;${CMAKE_BINARY_DIR}/postinst")
SET(CPACK_DEBIAN_DEVELOPMENT_PACKAGE_CONTROL_EXTRA "${CMAKE_BINARY_DIR}/repos_devel.sh;${CMAKE_BINARY_DIR}/postinst")

INCLUDE(CPack)

cpack_add_install_type(community DISPLAY_NAME "Community")
cpack_add_install_type(enterprise DISPLAY_NAME "Enterprise")
cpack_add_install_type(development DISPLAY_NAME "Development")
