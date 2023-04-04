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

# This file contains CMake convenience variables, functions, and macros used by the SciDB
# Project and other projects which depend upon the SciDB Project.

# Some of the FindXXXX.cmake files in the SciDB directory have not been updated to enforce
# that <PackageName> must match CMAKE_FIND_PACKAGE_NAME in find_package_handle_standard_args().
# This change was made in CMake 3.17.
# See https://cmake.org/cmake/help/v3.17/module/FindPackageHandleStandardArgs.html
# Known files and line numbers include:
#   * cmake/Modules/FindMPI.cmake:1654
#        The FindMPI.cmake module should be removed from SciDB repository in favor of the
#        one distributed with CMake and the CMake minimum version updated.
#         - See SDB-6600 and the commit 84ff6c2da51ba691ccea3ec68bfdebebe6bfec76.
#   * cmake/Modules/FindEditLine.cmake:30
#   * cmake/Modules/FindScaLAPACK.cmake:28
set(FPHSA_NAME_MISMATCHED ON)


# These variables are needed by cmake.install

#
# DEBUG symbols
function(strip_symbols targ)
  # NOTE:
  #      Symbols are ONLY stripped in RelWithDebInfo Builds
  #
  if("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
    set(DEBUG_SYMBOLS_DIR ".debug")
    set(DEBUG_SYMBOLS_EXT "${DEBUG_SYMBOLS_DIR}")
    add_custom_command(TARGET ${targ}
      POST_BUILD
      COMMAND mkdir -p $<TARGET_FILE_DIR:${targ}>/${DEBUG_SYMBOLS_DIR}
      COMMAND ${CMAKE_OBJCOPY}
              --only-keep-debug $<TARGET_FILE:${targ}>
              $<TARGET_FILE_DIR:${targ}>/${DEBUG_SYMBOLS_DIR}/$<TARGET_FILE_NAME:${targ}>${DEBUG_SYMBOLS_EXT}
      COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:${targ}>
      COMMAND ${CMAKE_OBJCOPY}
              --add-gnu-debuglink=${DEBUG_SYMBOLS_DIR}/$<TARGET_FILE_NAME:${targ}>${DEBUG_SYMBOLS_EXT}
              $<TARGET_FILE:${targ}>
      COMMAND chmod -x $<TARGET_FILE_DIR:${targ}>/${DEBUG_SYMBOLS_DIR}/$<TARGET_FILE_NAME:${targ}>${DEBUG_SYMBOLS_EXT}
      WORKING_DIRECTORY $<TARGET_FILE_DIR:${targ}>)
  endif()
endfunction()

function(scidb_add_executable targ)
  add_executable(${targ} ${ARGN})
  strip_symbols(${targ})
endfunction()

function(scidb_add_library targ)
  add_library(${targ} ${ARGN})
  strip_symbols(${targ})
endfunction()

set(DEBUG_SYMBOLS_DIRECTORY ".debug")
set(DEBUG_SYMBOLS_EXTENSION "${DEBUG_SYMBOLS_DIRECTORY}")

function(scidb_install_debugsymbols targ base_dir component)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
    install(FILES $<TARGET_FILE_DIR:${targ}>/${DEBUG_SYMBOLS_DIRECTORY}/$<TARGET_FILE_NAME:${targ}>${DEBUG_SYMBOLS_EXTENSION}
            DESTINATION ${base_dir}/${DEBUG_SYMBOLS_EXTENSION}
            COMPONENT ${component}
      )
  endif()
endfunction()

function(scidb_removekv lst key prefix)
  list(FIND ${lst} ${key} _index)
  if(_index GREATER -1)
    string(REPLACE "${key};${${prefix}_${key}}"
      "" ${lst} "${${lst}}")
  endif()
  set(${lst} "${${lst}}" PARENT_SCOPE)
endfunction()



function(scidb_install_debugsymbols_rename targ base_dir component suffix)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo")
    install(FILES $<TARGET_FILE_DIR:${targ}>/${DEBUG_SYMBOLS_DIRECTORY}/$<TARGET_FILE_NAME:${targ}>${DEBUG_SYMBOLS_EXTENSION}
            DESTINATION ${base_dir}/${DEBUG_SYMBOLS_EXTENSION}
            COMPONENT ${component}
            RENAME "${CMAKE_SHARED_LIBRARY_PREFIX}${targ}-${suffix}${CMAKE_SHARED_LIBRARY_SUFFIX}${DEBUG_SYMBOLS_EXTENSION}"
      )
  endif()
endfunction()

#
# scidb_install is a wrapper that alters CMake's install() in two ways.
#
# 1. It adds an extra install() target to install the .debug (symbols) for the
#    target. The COMPONENT of the installed debug files uses the COMPONENT of
#    the TARGETS and appends "-dbg" to that component.
#
# 2. It allows for a TARGET to be renamed on disk when it is installed which
#    facilitates using update-alternatives.  As an example, linear_algebra: The
#    linear_algebra library needs to be compiled with the ELF header "Library
#    soname" of liblinear_algebra.so, but the file on disk needs to be
#    liblinear_algebra-scidb.so and update-alternatives is used to link
#    liblinear_algebra.so to that file (or liblinear_algebra-p4.so in the case
#    of the enterprise edition).
#    - CMake does not easily allow this since install(TARGETS ...) does not
#      support the RENAME clause, and using CMake's set_target_properties to
#      change the suffix actually changes the "libray soname" in the compiled
#      library.
#      - e.g. set_target_properties(<lib> PROPERTIES SUFFIX "-scidb${CMAKE_SHARED_LIBRARY_SUFFIX}")
#        will alter the elf "Library soname" header to be lib<lib>-scidb.so] which causes
#        dlopen issues.
#    - To deal with this, the TARGETS form of CMake's install() command is altered to
#      be a FILES form of the CMake install() command which DOES support the RENAME clause.
#
# Another possible solution to this, would be to have this function add an additional
# install(CODE ) target which would execute a "mv" command to rename the file.
# This has the downside that the generated cmake.install file would be incorrect and would add
# more overhead to the "custom uninstall target"
#
# scidb_install() takes the same arguments as CMake's install() command but
# also may take an additional option with the 'TARGETS form' of install():
# RENAME_SUFFIX : The string value appended (along with a hyphen) to the file basename
#                 to define the output file name.
#
# Example. Assuming the following options are passed for the shared_library
#          target 'dense_linear_algebra'
#     TARGETS      dense_linear_algebra
#     DESTINATION  lib/scidb/plugins
#     COMPONENT scidb-plugins
#     RENAME_SUFFIX  scidb
# then the compiled file libdense_linear_algebra.so will be installed as libdense_linear_algebra-scidb.so
function(scidb_install)
  set(_install_args ${ARGN})
  set(options "")
  set(oneValueArgs DESTINATION COMPONENT RENAME_SUFFIX)
  set(multiValueArgs TARGETS)
  cmake_parse_arguments(SCIDB_INSTALL
    "${options}"
    "${oneValueArgs}"
    "${multiValueArgs}"
    ${ARGN} )

  if (NOT SCIDB_INSTALL_TARGETS)
    if (SCIDB_INSTALL_RENAME_SUFFIX)
      message(FATAL_ERROR
        "scidb_install() only allow RENAME_SUFFIX with 'TARGETS form' of install()"
        )
    endif()
    install(${_install_args})
  else()
    if (NOT SCIDB_INSTALL_COMPONENT)
      # TODO Blindly using 'COMPONENT scidb' for the  targets  and
      # the .debug files to 'COMPONENT scidb-dbg' is probably suboptimal.
      set(SCIDB_INSTALL_COMPONENT "scidb")
      list(APPEND _install_args COMPONENT "${SCIDB_INSTALL_COMPONENT}")
      message(WARNING "Assigning 'TARGETS ${SCIDB_INSTALL_TARGETS}' to the use 'COMPONENT ${SCIDB_INSTALL_COMPONENT}'")
    endif()

    if (SCIDB_INSTALL_RENAME_SUFFIX)
      scidb_removekv(_install_args RENAME_SUFFIX SCIDB_INSTALL)

      foreach(targ IN LISTS SCIDB_INSTALL_TARGETS)
        # We are actually going to use install(FILES...) for each of the SCIDB_INSTALL_TARGETS because
        # the target is being renamed and install(TARGET ...) doesn't allow for renaming....)
        set(_target_install_args ${_install_args})
        # remove the TARGETS from the passed arments and replace with a FILES <single_target>
        # We need to process each target individually since we are using RENAME with FILES
        scidb_removekv(_target_install_args TARGETS SCIDB_INSTALL)
        set(_target_install_args "FILES;$<TARGET_FILE:${targ}>;${_target_install_args}")
        # Now add the "RENAME <old_name>-<suffix>"
        # We need the CMAKE_X_PREFIX (like lib for SHARED_LIBRARY)
        #         and the CMAKE_X_SUFFIX (like .so for SHARED_LIBRARY) for the target name
        # We are renaming like <cmake_prefix><target_name>-<passed_suffix><cmake_suffix>:
        #    cmake_prefix:  ${CMAKE_${_target_type}_PREFIX}
        #    target_name:   ${targ}
        #    passed_suffix: ${SCIDB_INSTALL_RENAME_SUFFIX}
        #    cmake_suffix:  ${CMAKE_${_target_type}_SUFFIX}
        # (e.g. target linear_algebra is a shared libray, which gets renamed to liblinear_algebra)-${SUFFIX}.so)
        get_target_property(_target_type ${targ} TYPE)
        list(APPEND _target_install_args RENAME "${CMAKE_${_target_type}_PREFIX}${targ}-${SCIDB_INSTALL_RENAME_SUFFIX}${CMAKE_${_target_type}_SUFFIX}")
        # Now pass the altered arguments to install()
        install(${_target_install_args})
        # TODO SDB-6658: The installation of .debug files (with debug symbols) needs to be removed.
        scidb_install_debugsymbols_rename(
          ${targ}
          ${SCIDB_INSTALL_DESTINATION}
          ${SCIDB_INSTALL_COMPONENT}-dbg
          ${SCIDB_INSTALL_RENAME_SUFFIX}
          )
      endforeach()
    else()
      install(${_install_args})
      # TODO Allow passing "DEBUG_COMPONENT_SUFFIX" (or similar)?  May not be worth it
      #      since we:
      #  1. always use -dbg
      #  2. shouldn't be generating .debug files anyway (SDB-6658)
      # TODO... use foreach( targ IN LISTS ${SCIDB_INSTALL_TARGETS})
      foreach(targ IN LISTS SCIDB_INSTALL_TARGETS)
        scidb_install_debugsymbols(
          ${targ}
          ${SCIDB_INSTALL_DESTINATION}
          ${SCIDB_INSTALL_COMPONENT}-dbg
          )
      endforeach()
    endif()
  endif()
endfunction()

find_package(Git)
# Determine the Version of the project by examining the ${_SOURCE_DIR}/${_VERSION_FILE}
# and either using git or examining the ${_SOURCE_DIR}/${_REVISION_FILE} to obtain the
# revision wich is the git hash. If git is used to determine the revision, then the result
# of git rev-list is put into the ${_OUTDIR}/${_REVISION_FILE}.  The installed revision
# file (by way of ${_OUTDIR}/${_REVISION_FILE}) is used when building packages from a tar
# ball and the git hash is used (and added to the revision file) when building from a git
# repository.
function(set_version_from_file
    # _PREFIX
    # _SOURCE_DIR
    # _VERSION_FILE
    # _REVISION_FILE
    # _OUTDIR
    )
  if(ARGV0)
    set(_PREFIX ${ARGV0})
  else()
    string(TOUPPER ${PROJECT_NAME} _PREFIX)
  endif()

  if(ARGV1)
    set(_SOURCE_DIR "${ARGV1}")
  else()
    set(_SOURCE_DIR ${CMAKE_SOURCE_DIR})
  endif()

  if(ARGV2)
    set(_OUTDIR "${ARGV2}")
  else()
    set(_OUTDIR "${CMAKE_BINARY_DIR}")
  endif()

  if(ARGV3)
    set(_VERSION_FILE "${ARGV3}")
  else()
    set(_VERSION_FILE "version")
  endif()

  if(ARGV4)
    set(_REVISION_FILE "${ARGV4}")
  else()
    set(_REVISION_FILE "revision")
  endif()

  file(READ "${_SOURCE_DIR}/${_VERSION_FILE}" _FULL_VERSION)
  string(REGEX MATCH "^([0-9]*)\\.([0-9]*)\\.([0-9]*).*$" _RE_VERSION "${_FULL_VERSION}")
  if (NOT _RE_VERSION)
    message(FATAL_ERROR "Unable to parse version information from ${_VERSION_FILE}")
  endif()
  set(VERSION_MAJOR "${CMAKE_MATCH_1}")
  set(VERSION_MINOR "${CMAKE_MATCH_2}")
  set(VERSION_PATCH "${CMAKE_MATCH_3}")

  if(GIT_FOUND AND EXISTS "${_SOURCE_DIR}/.git")
    # Get revision from git working copy
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-list --abbrev=8  --abbrev-commit -1 HEAD
      OUTPUT_VARIABLE _REVISION
      RESULT_VARIABLE _result
      OUTPUT_STRIP_TRAILING_WHITESPACE
      WORKING_DIRECTORY ${_SOURCE_DIR})

    if(NOT "${_result}" STREQUAL "0")
      message(FATAL_ERROR "Can not invoke GIT ${GIT_EXECUTABLE}. Check binary!")
    endif()

    # message(STATUS "Obtained revision from git: ${_REVISION}; writing to ${_OUTDIR}/${_REVISION_FILE}")
    file(WRITE "${_OUTDIR}/${_REVISION_FILE}" ${_REVISION})
  elseif (EXISTS "${_SOURCE_DIR}/${_REVISION_FILE}")
    # Get version from plain source tarball/directory
    file(READ "${_SOURCE_DIR}/${_REVISION_FILE}" _REVISION)
    string(STRIP ${_REVISION} _REVISION)
    # message(STATUS "Read revision ${_REVISION} from file: ${_SOURCE_DIR}/${_REVISION_FILE}")
  else()
    message(FATAL_ERROR "Can not fetch working copy version and can't find revision file.")
  endif()

  set(${_PREFIX}_VERSION_MAJOR "${VERSION_MAJOR}" PARENT_SCOPE)
  set(${_PREFIX}_VERSION_MINOR "${VERSION_MINOR}" PARENT_SCOPE)
  set(${_PREFIX}_VERSION_PATCH "${VERSION_PATCH}" PARENT_SCOPE)
  set(${_PREFIX}_BUILD_REVISION "${_REVISION}"     PARENT_SCOPE)
  set(${_PREFIX}_SHORT_VERSION "${VERSION_MAJOR}.${VERSION_MINOR}" PARENT_SCOPE)
  set(${_PREFIX}_VERSION_MMP   "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}" PARENT_SCOPE)
  set(${_PREFIX}_VERSION       "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}.${_REVISION}" PARENT_SCOPE)

  message(STATUS "${_PREFIX}_VERSION_MAJOR : ${VERSION_MAJOR}")
  message(STATUS "${_PREFIX}_VERSION_MINOR : ${VERSION_MINOR}")
  message(STATUS "${_PREFIX}_VERSION_PATCH : ${VERSION_PATCH}")
  message(STATUS "${_PREFIX}_BUILD_REVISION: ${_REVISION}")
  message(STATUS "${_PREFIX}_SHORT_VERSION : ${VERSION_MAJOR}.${VERSION_MINOR}")
  message(STATUS "${_PREFIX}_VERSION_MMP   : ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
  message(STATUS "${_PREFIX}_VERSION       : ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}.${_REVISION}")
endfunction()
