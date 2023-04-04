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

if (NOT WITHOUT_SERVER)
    set (rbac_src
        NamespacesCommunicator.cpp
        Rbac.cpp
        Rights.cpp
    )

    add_library(rbac_lib STATIC ${rbac_src})
    target_link_libraries(rbac_lib
      PUBLIC Boost::filesystem
             Boost::program_options
             Boost::regex
             Boost::serialization
             Boost::system
             ${LOG4CXX_LIBRARIES}
             network_lib
      )
endif()
