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

# shared library for executing simple text queries
set(scidbclient_src
    SciDBRemote.cpp
    SciDBRemoteStubs.cpp
    ClientAuthenticator.cpp
    Credential.cpp
    ../array/Metadata.cpp
    ../array/ArrayDesc.cpp
    ../array/ArrayName.cpp
    ../array/AttributeDesc.cpp
    ../array/Attributes.cpp
    ../array/DimensionDesc.cpp
    ../array/Dimensions.cpp
    ../array/ObjectNames.cpp


    ../array/Array.cpp
    ../array/ConstIterator.cpp
    ../array/ConstArrayIterator.cpp
    ../array/ArrayIterator.cpp
    ../array/ArrayWriter.cpp
    ../array/ConstItemIterator.cpp
    ../array/ConstChunk.cpp
    ../array/Chunk.cpp
    ../array/ChunkMaterializer.cpp
    ../array/ConstChunkIterator.cpp
    ../array/AllocationBuffer.cpp
    ../array/CompressedBuffer.cpp
    ../array/ChunkBuffer.cpp
    ../array/SharedBuffer.cpp

    ../array/MaterializedChunkCache.cpp
    ../array/MemChunk.cpp
    ../array/RLE.cpp
    ../array/StreamArray.cpp
    ../array/TemplateParser.cpp
    ../array/DeepChunkMerger.cpp
    ../array/Tile.cpp
    ../array/Coordinate.cpp

    ../network/BaseConnection.cpp
    ../network/MessageDesc.cpp
    ../network/MessageUtils.cpp
    ../query/FunctionDescription.cpp
    ../query/FunctionLibrary.cpp
    ../query/QueryID.cpp
    ../query/TileFunctions.cpp
    ../query/TypeSystem.cpp
    ../query/UserQueryException.cpp
    ${CMAKE_BINARY_DIR}/src/system/Constants.cpp
    ../system/ErrorsLibrary.cpp
    ../system/Exceptions.cpp
    ../system/Sysinfo.cpp
    ../system/Warnings.cpp
    ../util/FileIO.cpp
    ../util/PluginManager.cpp
    ../util/PluginObjects.cpp
    ../util/Semaphore.cpp
    ../util/StringUtil.cpp
    ../util/CoordinatesMapper.cpp
    ../util/CryptoUtils.cpp
)

scidb_add_library(scidbclient SHARED ${scidbclient_src})

# FunctionLibrary.cpp includes src/query/BuiltInFunctions.h, a large
# set of C macros.  Tracking down all the expansions is too painful,
# so turn off conversion.  The sheer number of function calls causes a
# compiler-internal hash table used to generate debug info to exceed
# its size limit, so turn off var-tracking assignments.  (SDB-3631)
  set_source_files_properties(../query/FunctionLibrary.cpp
    PROPERTIES
    COMPILE_FLAGS "-Wno-conversion -fno-var-tracking-assignments"
    )

set_target_properties(scidbclient PROPERTIES
  LINK_FLAGS "-Wl,--no-undefined"
  )

target_compile_definitions(scidbclient PRIVATE "SCIDB_CLIENT")

target_link_libraries(scidbclient
  PUBLIC scidb_msg_lib
         json_lib
         compression_lib
         util_lib
         OpenSSL::Crypto
         ${LIBRT_LIBRARIES}
         Boost::filesystem
         Boost::program_options
         Boost::regex
         Boost::serialization
         Boost::system
         ${LOG4CXX_LIBRARIES}
         Threads::Threads
         ${CMAKE_DL_LIBS}
  )

scidb_install(TARGETS scidbclient
        # DESTINATION ${CMAKE_INSTALL_LIBDIR} // see SDB-6657
        DESTINATION lib
        COMPONENT scidb-client
  )
