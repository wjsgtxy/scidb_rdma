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

protobuf_generate_cpp(PROTO_SRC PROTO_HDRS "scidb_msg.proto")
add_custom_target(gen_scidb_msg_proto ALL DEPENDS ${PROTO_HDRS} ${PROTO_SRC})

# The -Wconversion option hates the generated code, so...
set_source_files_properties("${PROTO_SRC}"
  PROPERTIES
  COMPILE_FLAGS -Wno-conversion
  )

add_library(scidb_msg_lib STATIC ${PROTO_SRC} ${PROTO_HDRS})
target_link_libraries(scidb_msg_lib
  PUBLIC protobuf::libprotobuf
  )
