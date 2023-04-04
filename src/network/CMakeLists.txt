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
# dz add 忽略编译告警
remove_warnings()

add_subdirectory(proto)

if (NOT WITHOUT_SERVER)
    set (network_src
        Authenticator.cpp
        NetworkManager.cpp
        MessageDesc.cpp
        BaseConnection.cpp
        Connection.cpp
        Session.cpp
        MessageHandleJob.cpp
        AuthMessageHandleJob.cpp
        ClientMessageHandleJob.cpp
        MessageUtils.cpp
        OrderedBcast.cpp
        InstanceAuthenticator.cpp
        Network.cpp
    )
#    include_directories(rdma)
    set(rdma_src
            rdma/prototypes.h
            rdma/RdmaCommManager.cpp
            rdma/RdmaConnection.cpp
            rdma/RdmaService.cpp
            rdma/RdmaProperties.cpp
            rdma/RdmaUtilities.cpp
            )

    configure_file(test/mu_driver.py "${GENERAL_OUTPUT_DIRECTORY}/mu_driver.py" COPYONLY)
    configure_file(test/mu_random.py "${GENERAL_OUTPUT_DIRECTORY}/mu_random.py" COPYONLY)

    add_library(network_lib STATIC ${network_src} ${rdma_src})
#    dz add start
#    set(THREADS_PREFER_PTHREAD_FLAG ON)
#    find_package(Threads REQUIRED)
#    dz add end
    set_target_properties(network_lib PROPERTIES
            COMPILE_FLAGS "-pthread"
            LINK_FLAGS "-pthread")

    # dz add szu_lab 更换librdmacm和libibverbs库版本           
    find_library(lib_rdmacm librdmacm.so /home/dengzhong/rdma-core/build/lib NO_DEFAULT_PATH)
    find_library(lib_ibverbs libibverbs.so /home/dengzhong/rdma-core/build/lib NO_DEFAULT_PATH)
    message("dz rdmacm: ${lib_rdmacm}")           
    message("dz ibverbs: ${lib_ibverbs}")           

    target_link_libraries(network_lib
      PUBLIC Boost::filesystem
             Boost::program_options
             Boost::regex
             Boost::serialization
             Boost::system
             ${LOG4CXX_LIBRARIES}
             rbac_lib
             catalog_lib
             scidb_msg_lib
             compression_lib
             qproc_lib

            muduo_net # dz add
            muduo_base # dz add

             #ibverbs # dz add rdma相关
             #rdmacm
             ${lib_rdmacm}
             ${lib_ibverbs}
             rt # dz add
#             Threads::Threads # dz add
      )
endif()
