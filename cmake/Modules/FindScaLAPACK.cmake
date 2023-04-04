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

# SCALAPACK_FOUND
# SCALAPACK_LIBRARIES

# This is a quick and dirty CMake module, and it would be well suited to be being updated to
# follow the CMake Find Modules guidelines:
#   https://cmake.org/Wiki/CMake:How_To_Find_Libraries#Writing_find_modules
find_library(SCALAPACK_LIBRARIES
             NAMES
                 scalapack-scidb-mpich2scidb
                 scalapack-scidb-openmpi
                 scalapack-scidb-mpich
                 scalapack-scidb
                 scalapack-mpich2scidb
                 scalapack-openmpi
                 scalapack-mpich
                 scalapack
             PATH_SUFFIXES
                 mpich2scidb/lib
                 openmpi/lib
                 openmpi3/lib
                 mpich/lib
  )

find_package_handle_standard_args(SCALAPACK
  REQUIRED_VARS SCALAPACK_LIBRARIES)

mark_as_advanced(SCALAPACK_LIBRARIES)
