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

#scidb-dev package
install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT scidb-dev
        PATTERN ".git" EXCLUDE
  )

# M K L
# override platform blas,lapack when MKL available
if(MKL_BLAS_FOUND)
    message(STATUS "install.cmake: copying library redirection links from ${LIB_LINKS_DIRECTORY} to lib")
    install(DIRECTORY "${LIB_LINKS_DIRECTORY}/" DESTINATION lib COMPONENT scidb)
else()
    message(SEND_ERROR "Missing libblas, liblapack redirects. Unsupported configuration.")
endif()

# S O U R C E   P A C K A G E
set(SRC_PACKAGE_FILE_NAME
    "scidb-${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}.${SCIDB_VERSION_PATCH}.${SCIDB_BUILD_REVISION}")

add_custom_target(src_package
    COMMAND rm -rf /tmp/${SRC_PACKAGE_FILE_NAME}
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/${SRC_PACKAGE_FILE_NAME}.tgz
    #
    COMMAND rm -rf /tmp/${SRC_PACKAGE_FILE_NAME}.tar
    COMMAND git archive --prefix=${SRC_PACKAGE_FILE_NAME}/ --output=/tmp/${SRC_PACKAGE_FILE_NAME}.tar HEAD
    COMMAND tar --directory=/tmp -xf /tmp/${SRC_PACKAGE_FILE_NAME}.tar
    COMMAND rm -rf /tmp/${SRC_PACKAGE_FILE_NAME}.tar
    # Put revision into revision file in the tarball
    COMMAND git rev-list --abbrev-commit -1 HEAD > /tmp/${SRC_PACKAGE_FILE_NAME}/revision
    COMMAND ${CMAKE_BINARY_DIR}/../utils/licensing.pl /tmp/${SRC_PACKAGE_FILE_NAME} ${CMAKE_BINARY_DIR}/../utils/scidb.lic
    COMMAND tar  --directory=/tmp/${SRC_PACKAGE_FILE_NAME} -czf ${CMAKE_BINARY_DIR}/${SRC_PACKAGE_FILE_NAME}.tgz .
    COMMAND rm -rf /tmp/${SRC_PACKAGE_FILE_NAME}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
