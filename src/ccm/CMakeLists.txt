#
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
#

# The CcmBridge acts as a thick client to issue queries much the same way the
# the original iquery client does.  This is linked into the scidb executable but
# it is NOT a SCIDB_CLIENT and we do not link against the `scidbclient` library.
# We recompile (<sigh>) the necessary files from the capi "module".
# Phase two of the client communication manager will re-factor/remove the need for this
# bridge.
# BJC says: My opinion is that libccmbridge_lib.a/ccmbridge_lib.lib is a horrible naming
#      convention. It is also counter to CMake common practices, but that's what people
#      want. The command is add_library so why it's mandatory to add "_lib" is
#      incomprehensible.  At least, we don't require adding _exe to add_executable
#      commands.
add_library(ccmbridge_lib STATIC
  ../capi/ClientAuthenticator.cpp
  ../capi/Credential.cpp
  ../capi/SciDBRemote.cpp
  ../query/executor/SciDBExecutor.cpp
  CcmBridge.cpp
  )
target_link_libraries(ccmbridge_lib
  PRIVATE rbac_lib
  )


protobuf_generate_cpp(CCM_PROTO_SRC CCM_PROTO_HDRS Ccm.proto)
protobuf_generate_python(CCM_PROTO_PY Ccm.proto)
# protobuf_generate_python adds a custom_command, but to trigger it, a target
# needs to depend upon it.
add_custom_target(ccmprotopy ALL DEPENDS ${CCM_PROTO_PY})

# BJC says: Ugh _lib again...
add_library(ccm_lib STATIC
  ${CCM_PROTO_SRC}
  CcmConnection.cpp
  CcmConversation.cpp
  CcmConversationState.cpp
  CcmErrorCode.cpp
  CcmMsgHeader.cpp
  CcmMsgType.cpp
  CcmProperties.cpp
  CcmService.cpp
  CcmSession.cpp
  CcmSessionCapsule.cpp
  CcmSessionState.cpp
  ClientCommManager.cpp
  Uuid.cpp
  )

# Generated protobuf files have conversion warnings.
set_source_files_properties(${CCM_PROTO_SRC}
  PROPERTIES
  COMPILE_FLAGS -Wno-conversion
  )

# The generated protobuf header file (Ccm.pb.h) is in the
# ${CMAKE_CURRENT_BINARY_DIR}
target_include_directories(ccm_lib
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  )

target_link_libraries(ccm_lib
  PRIVATE protobuf::libprotobuf
          Boost::boost
          OpenSSL::SSL
          ccmbridge_lib
  )


install(TARGETS ccm_lib
  DESTINATION lib
  COMPONENT scidb
  )
install(FILES ${CCM_PROTO_PY}
              SciDBMessage.py
  DESTINATION bin/scidblib
  COMPONENT scidb-utils
  )
install(PROGRAMS ccmtest.py
  DESTINATION bin
  COMPONENT scidb-utils
  )
