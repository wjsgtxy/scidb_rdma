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


# Define the flags associated with each CMAKE_BUILD_TYPE for the compiler type.
# CMake usually set this via the Module/Compiler/XXXX.cmake files
# Here we redefine some of the flags for the standand CMake build types (RELWITHDEBINFO for example)
# and define a few new build types
#
# Currently we only update these flags and create build types for the GNU compiler.
#

set(_SCIDB_BUILD_TYPES
    "Debug"
    "RelWithDebInfo"
    "Release"
    "Assert"
    "CC"
    "DebugNoAssert"
    "Profile"
    "Valgrind"
  )

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  # DEBUG_INFO_FLAGS
  set(DEBUG_INFO_FLAGS "-g -ggdb3 -fno-omit-frame-pointer")
  # Note-0: -g may be unnecessary with -ggdb3, but the gcc info manual is unclear.
  #
  # Note-1: We only need -fno-omit-frame-pointer when the followiong is true:
  #  1. We want to to be able to debug (-g -ggdb3) (i.e. we have ${DEBUG_INFO_FLAGS})
  #  2. Optimization (-O, -O2, etc) is also desired
  #  3. NOTE: -fno-omit-frame-pointer is true on 64 bit versions of GCC even after GCC version 4.6
  #           unless -O, -O2, etc. are also given. (for more information see the man/info page for GCC)
  # Adding this flag for all but the -O0 case is more overhead than it's worth, so just add -fno-omit-frame-pointer
  # when adding the DEBUG_INFO_FLAGS.

  #
  # Overrides of standard CMake Build Types: DEBUG and RELWITHDEBINFO
  #
  #
  # # cmake default of CMAKE_<LANG>_FLAGS_DEBUG for GNU Compiler
  #            (see CMAKE_${lang}_FLAGS_DEBUG_INIT in cmake-X.Y/Modules/Compiler/GNU.cmake)
  # CMAKE_CXX_FLAGS_DEBUG      "-g"
  # CMAKE_C_FLAGS_DEBUG        "-g"
  # CMAKE_Fortran_FLAGS_DEBUG  "-g"
  #
  # CMAKE_<LANG>_DEBUG           -- asserted, unoptimized
  # NOTE: -O0 is not necessary. It is the same as no optimization option being given. James wants the
  #       default value of this particular option to be given regardless.
  set(CMAKE_CXX_FLAGS_DEBUG     "-O0 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_C_FLAGS_DEBUG       "-O0 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_Fortran_FLAGS_DEBUG "-O0 ${DEBUG_INFO_FLAGS}")

  # # CMake default of CMAKE_<LANG>_FLAGS_RELWITHDEBINFO for GNU Compiler
  #            (see CMAKE_${lang}_FLAGS_RELWITHDEBINFO_INIT in cmake-X.Y/Modules/Compiler/GNU.cmake)
  # CMAKE_CXX_FLAGS_RELWITHDEBINFO     "-O2 -g -DNDEBUG"
  # CMAKE_C_FLAGS_RELWITHDEBINFO       "-O2 -g -DNDEBUG"
  # CMAKE_Fortran_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG"
  #
  # CMAKE_<LANG>_RELWITHDEBINFO  -- not asserted, optimized (same as ASSERT with asserts disabled)
  # We use -O3 rather then -O2 for relwithdebinfo.
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO     "-O3 ${DEBUG_INFO_FLAGS} -DNDEBUG -frtti") # dz todo frtti是我加的
  set(CMAKE_C_FLAGS_RELWITHDEBINFO       "-O3 ${DEBUG_INFO_FLAGS} -DNDEBUG")
  set(CMAKE_Fortran_FLAGS_RELWITHDEBINFO "-O3 ${DEBUG_INFO_FLAGS} -DNDEBUG")

  #
  # User defined build types : Assert, CC, DebugNoAssert, Profile, Valgrind
  #
  # CMAKE_<LANG>_ASSERT          -- asserted, optimized
  set(CMAKE_CXX_FLAGS_ASSERT     "-O3 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_C_FLAGS_ASSERT       "-O3 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_Fortran_FLAGS_ASSERT "-O3 ${DEBUG_INFO_FLAGS}")

  mark_as_advanced(
    CMAKE_CXX_FLAGS_ASSERT
    CMAKE_C_FLAGS_ASSERT
    CMAKE_Fortran_FLAGS_ASSERT
    CMAKE_EXE_LINKER_FLAGS_ASSERT
    CMAKE_MODULE_LINKER_FLAGS_ASSERT
    CMAKE_SHARED_LINKER_FLAGS_ASSERT
    CMAKE_STATIC_LINKER_FLAGS_ASSERT
    )

  # Clean exit is not sufficient for getting code coverage data flushed, so we need an explicit define so the
  # function to flush the data can be explicitly called.  We are also calling it at the end of every successful
  # query, not just when the applications exits.
  # CMAKE_<LANG>_CC              -- asserted, unoptimized, code coverage
  set(CMAKE_CXX_FLAGS_CC          "-DCLEAN_EXIT -DCOVERAGE -O0 ${DEBUG_INFO_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_C_FLAGS_CC            "-DCLEAN_EXIT -DCOVERAGE -O0 ${DEBUG_INFO_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_Fortran_FLAGS_CC      "-DCLEAN_EXIT -DCOVERAGE -O0 ${DEBUG_INFO_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_EXE_LINKER_FLAGS_CC   "-fprofile-arcs -ftest-coverage")

  mark_as_advanced(
    CMAKE_CXX_FLAGS_CC
    CMAKE_C_FLAGS_CC
    CMAKE_Fortran_FLAGS_CC
    CMAKE_EXE_LINKER_FLAGS_CC
    CMAKE_MODULE_LINKER_FLAGS_CC
    CMAKE_SHARED_LINKER_FLAGS_CC
    CMAKE_STATIC_LINKER_FLAGS_CC
    )

  # CMAKE_<LANG>_DEBUGNOASSERT   -- not asserted, unoptimized
  set(CMAKE_CXX_FLAGS_DEBUGNOASSERT     "-O0 ${DEBUG_INFO_FLAGS} -DNDEBUG")
  set(CMAKE_C_FLAGS_DEBUGNOASSERT       "-O0 ${DEBUG_INFO_FLAGS} -DNDEBUG")
  set(CMAKE_Fortran_FLAGS_DEBUGNOASSERT "-O0 ${DEBUG_INFO_FLAGS} -DNDEBUG")

  mark_as_advanced(
    CMAKE_CXX_FLAGS_DEBUGNOASSERT
    CMAKE_C_FLAGS_DEBUGNOASSERT
    CMAKE_Fortran_FLAGS_DEBUGNOASSERT
    CMAKE_EXE_LINKER_FLAGS_DEBUGNOASSERT
    CMAKE_MODULE_LINKER_FLAGS_DEBUGNOASSERT
    CMAKE_SHARED_LINKER_FLAGS_DEBUGNOASSERT
    CMAKE_STATIC_LINKER_FLAGS_DEBUGNOASSERT
    )


  # CMAKE_<LANG>_PROFILE         -- not asserted, partial optimized, profiled
  # '-pg'
  #     Generate extra code to write profile information suitable for the
  #     analysis program 'gprof'.  You must use this option when compiling
  #     the source files you want data about, and you must also use it when
  #     linking.
  set(CMAKE_CXX_FLAGS_PROFILE     "-O2 ${DEBUG_INFO_FLAGS} -DNDEBUG -pg")
  set(CMAKE_C_FLAGS_PROFILE       "-O2 ${DEBUG_INFO_FLAGS} -DNDEBUG -pg")
  set(CMAKE_Fortran_FLAGS_PROFILE "-O2 ${DEBUG_INFO_FLAGS} -DNDEBUG -pg")
  set(CMAKE_EXE_LINKER_FLAGS_PROFILE "-pg")

  mark_as_advanced(
    CMAKE_CXX_FLAGS_PROFILE
    CMAKE_C_FLAGS_PROFILE
    CMAKE_Fortran_FLAGS_PROFILE
    CMAKE_EXE_LINKER_FLAGS_PROFILE
    CMAKE_MODULE_LINKER_FLAGS_PROFILE
    CMAKE_SHARED_LINKER_FLAGS_PROFILE
    CMAKE_STATIC_LINKER_FLAGS_PROFILE
    )


  # CMAKE_<LANG>_VALGRIND        -- asserted, unoptimized, interpreted (very very slow)
  set(CMAKE_CXX_FLAGS_VALGRIND     "-DCLEAN_EXIT -O0 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_C_FLAGS_VALGRIND       "-DCLEAN_EXIT -O0 ${DEBUG_INFO_FLAGS}")
  set(CMAKE_Fortran_FLAGS_VALGRIND "-DCLEAN_EXIT -O0 ${DEBUG_INFO_FLAGS}")

  mark_as_advanced(
    CMAKE_CXX_FLAGS_VALGRIND
    CMAKE_C_FLAGS_VALGRIND
    CMAKE_Fortran_FLAGS_VALGRIND
    CMAKE_EXE_LINKER_FLAGS_VALGRIND
    CMAKE_MODULE_LINKER_FLAGS_VALGRIND
    CMAKE_SHARED_LINKER_FLAGS_VALGRIND
    CMAKE_STATIC_LINKER_FLAGS_VALGRIND
    )

  # #########################################################
  # Set the default BUILD TYPE to "Assert" when calling CMake
  # #########################################################
  set(default_build_type "Assert")

  if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified")
    set(CMAKE_BUILD_TYPE "${default_build_type}"
      CACHE STRING "Choose the type  of build, options are: RelWithDebInfo Assert Debug CC Profile Release Valgrind DebugNoAssert"
      FORCE)
    # set the possible values for cmake UI
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
      ${_SCIDB_BUILD_TYPES}
      )
  endif()
  unset(DEBUG_INFO_FLAGS)
else()
  message(WARNING "Not all SciDB build types (${_SCIDB_BUILD_TYPES}) are defined for this compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()


include(CheckCXXCompilerFlag)
function(scidb_add_cxx_flag flag)
  check_cxx_compiler_flag("${flag}" SCIDB_CXX_HAS_FLAG)
  if (SCIDB_CXX_HAS_FLAG)
    # message("_have compiler ${flag} ${SCIDB_CXX_HAS_FLAG}")
    set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} ${flag}" PARENT_SCOPE)
  endif()
  unset(SCIDB_CXX_HAS_FLAG CACHE)
endfunction()

# TODO:
# These are the warning flags that need to be separated and passed to the
# invocation of cmake in the "setup/configuration step.
#
# They have been left here during the first refactoring work which begins to
# separate orthogonal concepts (CMake BUILD_TYPE flags, and user-desired warning
# flags).
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  ##
  ##  CLIKE_LANGUAGE_WARNINGS  for C/C++ but most don't apply to Fortran
  ##
  set(CLIKE_LANGUAGE_WARNINGS "")
  # -Wall :: enables all the warnings about constructions that some users
  #          consider questionable, and that are easy to avoid
  # -Wextra :: (-Wextra option used to be called '-W'.  The older name is still
  #            supported, but the newer name is more descriptive.)
  # -pedantic (-Wpedantic): Issue all the warnings demanded by strict ISO C and
  #                         ISO C++; reject all programs that use forbidden
  #                         extensions
  # -Wconversion :: Blindly converting floating point and integral numbers back
  #                 and forth is very dangerous in an application where correct
  #                 numeric values are imperative!
  # -Werror ::  Turn Warnings into Errors and cause compilation to fail.
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wall -Wextra")
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -pedantic")
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wconversion")
#  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Werror")  // dz todo 这个会把很多告警当做错误，先屏蔽了
  #
  # Exceptions to the Warnings items above
  #
  # -Wall enables the following warning but we currently have to suppress them.
  #   -Wstrict-aliasing :: We have punning problems which abound in
  #                        include/query/TypeSystem.h. This warning is enabled
  #                        when '-fstrict-aliasing' option is enabled, and
  #                        -fstrict-aliasing is enabled with optimization
  #                        levels '-O2', '-O3', '-Os'.
  # -Wextra (-W) enables the following when -Wall is supplied
  #   -Wunused-parameter :: we have numerous places throughout the code where the passed parameters is not used at all in a function.
  #                          (e.g. the base class virtual function throws regardless of the value of the passed values).
  # -pedantic
  #    -Wvariadic-macros :: See SDB-6560. We use (in Arena) the GNU version of variadic macros not the C90/C++11 standard.
  # -Wunused-result
  #     Ubuntu 'decided' to turn on -D_FORTIFY_SOURCE=2 with Optimization level
  #     -O2,-O3 which enables additional compile-time and run-time checks for several
  #     libc functions. CentOS did not make this same decision. Note, with gcc
  #     adding '(void)' in front of a function which must use the return result
  #     does not squash the warning. (e.g.  (void) ::getline(&buf, &len, fp());)
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-strict-aliasing")
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-unused-parameter")
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-variadic-macros")
  # Warn but don't error
  set(CLIKE_LANGUAGE_WARNINGS "${CLIKE_LANGUAGE_WARNINGS} -Wno-error=unused-result") 
  ## GNU 5.4.0 (on Ubuntu 16.04) detects the following.
  #    ISO C++ forbids variable length array: src/util/Backtrace.cpp:51:27:
  #                                              src/query/parser/Factory.cpp:126:22
  scidb_add_cxx_flag(-Wno-error=vla)
  #    comparison between signed and unsigned integer expressions: stage/build/utils/iquery/scanner.cpp:1480:44
  #                                                                stage/build/src/query/parser/Lexer.cpp:1365:44
  scidb_add_cxx_flag(-Wno-error=sign-compare)
  #    the program should also define ‘void operator delete(void*, long unsigned int)’ : src/network/GlobalNew.cpp:72:6
  #    the program should also define ‘void operator delete [](void*, long unsigned int)’: src/network/GlobalNew.cpp:92:6
  scidb_add_cxx_flag(-Wno-error=sized-deallocation)
  #    MPI deprecated calls.
  scidb_add_cxx_flag(-Wno-error=deprecated-declarations)

  message(STATUS "CLIKE_LANGUAGE_WARNINGS = ${CLIKE_LANGUAGE_WARNINGS}")
  ##
  ##  Fortran Warnings
  ##
  set(FORTRAN_LANGUAGE_WARNINGS "")
  # - TODO -fno-f2c is not necessary and possibly confuses people. By specifying
  #          -fno-f2c it makes it seem like the default is -ff2c where the opposite is
  #          actually the case.
  #     FROM gfortran-4 info file ::
  #       In the table below, only one of the
  #       forms is listed--the one which is not the default.  You can figure out
  #       the other form by either removing 'no-' or adding it.
  #
  #       '-ff2c'
  #           Generate code designed to be compatible with code generated by
  #           'g77' and 'f2c'.
  # - '-Wline-truncation'
  #         Warn when a source code line will be truncated.  This option is
  #         implied by '-Wall'.
  #
  # TODO: Should we specify -Wall, since we do with C-like languages?
  set(FORTRAN_LANGUAGE_WARNINGS "${FORTRAN_LANGUAGE_WARNINGS} -fno-f2c -Wline-truncation")

  ##
  ##  Add the warning flags to each of the build_types.
  ##
  foreach(_BTYPE IN LISTS _SCIDB_BUILD_TYPES)
    string(TOUPPER ${_BTYPE} _BUILDTYPE)
    set(CMAKE_CXX_FLAGS_${_BUILDTYPE}     "${CMAKE_CXX_FLAGS_${_BUILDTYPE}} ${CLIKE_LANGUAGE_WARNINGS}")
    set(CMAKE_C_FLAGS_${_BUILDTYPE}       "${CMAKE_C_FLAGS_${_BUILDTYPE}} ${CLIKE_LANGUAGE_WARNINGS}")
    set(CMAKE_Fortran_FLAGS_${_BUILDTYPE} "${CMAKE_Fortran_FLAGS_${_BUILDTYPE}} ${FORTRAN_LANGUAGE_WARNINGS}")
    unset(_BUILDTYPE)
  endforeach()

  unset(CLIKE_LANGUAGE_WARNINGS)
  unset(FORTRAN_LANGUAGE_WARNINGS)
else()
  message(STATUS "No CMake_<LANG>_FLAGS_<BuildType> updated for this compiler. ${CMAKE_CXX_COMPILER_ID}")
endif()

unset(_SCIDB_BUILD_TYPES)
