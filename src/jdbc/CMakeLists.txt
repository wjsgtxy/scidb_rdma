find_package(Java)

if (NOT JAVA_FOUND)
   message(FATAL_ERROR "Java 1.8 is currently the only supported version for building JDBC!")
endif()

set (ANT_BUILD_FILE ${CMAKE_CURRENT_SOURCE_DIR}/build.xml)
set (BUILD_DIR  ${CMAKE_CURRENT_BINARY_DIR}/build)
set (DIST_DIR   ${CMAKE_BINARY_DIR}/bin/jdbc)
set (DEPS_DIR   ${CMAKE_CURRENT_BINARY_DIR}/deps)
set (REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/report)
set (BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set (PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/network/proto/)
set (JAR_OUTS ${DIST_DIR}/jdbctest.jar
          ${DIST_DIR}/scidb4j.jar
          ${DIST_DIR}/example.jar
)

set(MESSAGE_IN   ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/io/network/Message.java.in)
set(MESSAGE_JAVA ${CMAKE_CURRENT_BINARY_DIR}/scidb4j/org/scidb/io/network/Message.java)
add_custom_command(OUTPUT ${MESSAGE_JAVA}
                   COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/scidb4j/org/scidb/io/network
                   COMMAND m4 -I${CMAKE_CURRENT_SOURCE_DIR}/../../include ${MESSAGE_IN} > ${MESSAGE_JAVA}
                   DEPENDS ${MESSAGE_IN})

set (JAVA_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/AFLTour.java
    ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/ConnectionInfo.java
    ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/HelloWorld.java
    ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/JDBCExample.java
    ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/Types.java

    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/iquery/Iquery.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/iquery/Config.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/iquery/XsvFormatter.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/util/ByteBufferExtensions.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/util/InputStreamWithReadall.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSetWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/StatementWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/IStatementWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Statement.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Connection.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Driver.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/TypeException.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSetMetaData.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSet.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/IResultSetWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/io/network/Network.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Chunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Type.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Connection.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Array.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/AuthenticationFile.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/ConfigUser.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/ConfigUserException.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Schema.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/WarningCallback.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/IChunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/EmptyChunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Result.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/PrepareResult.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/SciDBException.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/SessionProperties.java

    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/Option.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/Options.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/CommandLine.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/CommandLineParser.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/DefaultParser.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/apache/commons/cli/ParseException.java

    ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest/org/scidb/JDBCBasicTestCase.java
    ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest/org/scidb/JDBCTest.java
)

set_source_files_properties(${JDBC_SRC_COPY_FILE}
  PROPERTIES
  GENERATED TRUE
  )

execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/lib
                                                           ${CMAKE_CURRENT_BINARY_DIR}/lib)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/logging.properties
                                                           ${CMAKE_CURRENT_BINARY_DIR}/logging.properties)
set (ENV{JAVA_HOME} "${Java_HOME}")

add_custom_target ( jdbc_build ALL DEPENDS ${JAR_OUTS})

add_custom_command (OUTPUT ${JAR_OUTS}
           DEPENDS ${JAVA_SOURCES} ${MESSAGE_JAVA}
           COMMAND export JAVA_HOME=${Java_HOME}\; export ANT_HOME=/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/apache-ant-1.9.4\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/example ${CMAKE_CURRENT_BINARY_DIR}\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest ${CMAKE_CURRENT_BINARY_DIR}\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j ${CMAKE_CURRENT_BINARY_DIR}\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR} configure
           COMMAND export JAVA_HOME=${Java_HOME}\; export ANT_HOME=/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/apache-ant-1.9.4\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR}
           )

#scidb-jdbc package
install(FILES "${CMAKE_BINARY_DIR}/bin/jdbc/scidb4j.jar"
              "${CMAKE_BINARY_DIR}/bin/jdbc/example.jar"
              "${CMAKE_BINARY_DIR}/bin/jdbc/jdbctest.jar"
        DESTINATION jdbc
        COMPONENT scidb-jdbc
  )
