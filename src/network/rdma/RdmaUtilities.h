//
// Created by dz on 2022/7/9.
//

#ifndef  RDMAUTILITIES_H
#define RDMAUTILITIES_H
#include <string>

namespace scidb {
    // static void *r_cm_event_thread(void *param);

    // int r_listener_bind(struct our_control *listen_conn);

    // static int r_create_event_channel(struct r_control *ctx);

    // 转移到 rdmaconn中去了
//    int r_migrate_id(struct r_control *ctx, struct rdma_cm_id *new_cm_id,
//                     struct r_connect_info *connect_info);

    // static int r_create_cm_event_thread(struct r_control *ctx);

    int changeNonBlock(int fd);

    std::string dec2hex(uint64_t i);

    uint64_t hex2dec(std::string data);
}
#endif
