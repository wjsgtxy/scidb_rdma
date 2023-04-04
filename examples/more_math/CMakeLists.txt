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

# shared library for user defined objects
set (more_math_src
    more_math.cpp
)

scidb_add_library(more_math SHARED ${more_math_src})
target_link_libraries(more_math PUBLIC Boost::boost)

scidb_install(TARGETS more_math
        # DESTINATION ${CMAKE_INSTALL_LIBDIR}/scidb/plugins # see SDB-6657
        DESTINATION lib/scidb/plugins
        COMPONENT scidb-plugins
  )
