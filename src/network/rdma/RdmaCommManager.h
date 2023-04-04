//
// Created by dz on 2022/7/6.
//

#ifndef RDMA_COMM_MANAGER_H
#define RDMA_COMM_MANAGER_H

// c++ standard libraries
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <cstdio>
#include <utility>

// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>
// scidb
#include "query/InstanceID.h"
#include <util/Singleton.h>
#include "util/Mutex.h"

// dz rdma libraries
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <netdb.h> // dz linux中的头文件
#include "prototypes.h"
#include "RdmaUtilities.h"
//#include "RdmaConnection.h"

#include <functional> // std::bind

#include <system/ErrorCodes.h>
#include <util/InjectedErrorCodes.h>
#include <system/Exceptions.h>

// muduo
#include "muduo/net/EventLoop.h"
#include "muduo/net/Channel.h"

extern std::unordered_map<uint64_t, struct ibv_recv_wr *> gmap_wr; // 全局变量

namespace scidb {
//    namespace rdma {
    // 配置rdma属性
    class RdmaProperties;
    // 前向声明
    class RdmaConnection;

    typedef std::unordered_map<InstanceID, std::shared_ptr<RdmaConnection>> RConnectionMap;
    typedef std::unordered_set<InstanceID> RConnectionSetInit;
    typedef std::unordered_map<InstanceID, uint64_t> RConnectionGenMap;

/**
 * @defgroup Rdma Communications Manager
 */

/**
 * @class RdmaCommManager
 *
 * @ingroup Rdma
 *
 * A tool that listens for incoming client messages and processes them to execute queries on SciDB.
 */
class RdmaCommManager: public Singleton<RdmaCommManager> // 改用单例模式了，只有一个mgr了
{
public:
    // explicit RdmaCommManager(RdmaProperties const&);
    explicit RdmaCommManager();
    ~RdmaCommManager() noexcept;

    /**
     * Block waiting for external client requests, and service them when they arrive.
     */
    void run();

    /**
     * Stop the rdma Communications Manager.
     * // 注意，要在另外的线程中停止这个
     */
    void stop();

//            void handle_completion(const boost::system::error_code &error, std::size_t bytes_transferred);
    void handle_completion(); // muduo

    int check_completion_status(struct ibv_wc* work_completion); // 检测wc状态
    int process_work_completion(struct ibv_wc* work_completion);
    void connect_remote(InstanceID src_id, InstanceID dest_id, const std::string& remoteIp);

    void startCompletion();

    // 自身的ins id
    InstanceID _selfInstanceID;
    // 自身server id
    size_t server_id = 0;
    // 自身在同一个节点的index
    size_t ins_index = 0;

    struct rdma_event_channel* _cm_event_channel; // 监听cm_id绑定的事件channel
    struct ibv_comp_channel* _completion_channel; // 完成队列channel
    struct ibv_cq* _completion_queue; // send queue和 recv queue对应的cq，所有qp共享同一个cq，方便处理
    struct rdma_cm_id* _cm_id; // 监听cm_id

//            muduo::net::EventLoop* _loop = nullptr;

//            boost::asio::posix::stream_descriptor ch_fd; // 监听channel对应的fd，生成的流，用与asio监控
//            boost::asio::posix::stream_descriptor cq_fd; // cq channel对应的fd

//    boost::asio::posix::stream_descriptor* ch_fd = nullptr; // 监听channel对应的fd，生成的流，用与asio监控
//    boost::asio::posix::stream_descriptor* cq_fd = nullptr; // cq channel对应的fd

    muduo::net::Channel* _ch_channel = nullptr;
    muduo::net::Channel* _cq_channel = nullptr;

    /**
    * 保存到其他instance的conn连接
    */

//            RConnectionMap _rConnections; // 只要一个就可以了
//            RConnectionMapInit _rInitConnections; // 是否已经开始初始化了，这么做的原因是上面的这个map是连接建立成功我才添加到
//            Mutex _mutex; // 主要用来锁上面的map


    RConnectionMap _rinConnections; // 作为server接收的连接
    Mutex _inMutex; // 主要用来锁上面的map
    RConnectionSetInit _rinInitConnections; // 是否已经开始初始化了，这么做的原因是上面的这个map是连接建立成功我才添加到,所以先占个位
    Mutex _inInitMutex;

    RConnectionMap _routConnections; // 作为client发出的连接
    Mutex _outMutex;
    RConnectionSetInit _routInitConnections;
    Mutex _outInitMutex;

    // instance ID <-> generation ID used to maintain only one connection per remote instance
//            RConnectionGenMap _rinConnections;

    /************************************************CQE start*************************************************/
    // wc数组，用来存放从ibv_poll_cq返回的CQE array
    struct ibv_wc* _work_completion;

    // cq队列的长度，等于之前申请的cqe的长度加一，保证即使一次性将所有cqe poll出来都能存的下
    int	max_n_work_completions;

    // 当前这次poll出来的cqe个数，用来轮询
    int	current_n_work_completions;

    // 索引，指向已经poll出来的但是还没有处理的下一个cqe
    int	index_work_completions;

    /* dynamically allocated array to hold histogram of number of
     * completions returned on each call to ibv_poll_cq()
     */
    unsigned long* completion_stats;

    /* counts number of successful calls to ibv_req_notify() */
    unsigned long notification_stats;

    // 待确认的cq event
    unsigned int cq_events_that_need_ack;
    /************************************************CQE end***************************************************/

    struct ibv_context* verbs = nullptr;

    struct ibv_pd* _protection_domain = nullptr;

private:
//    class Impl;
//    std::unique_ptr<Impl> _impl;

    void startAccept();
    void startAcceptNextConnection();
    void handleConnectionAccept(boost::system::error_code ec);
    int initRdma();
//            void handleEvent(const boost::system::error_code &error, std::size_t bytes_transferred);
    void handleEvent();

    // 等待completion channel事件
    int wait_for_completion_notification();

//   void agent(struct rdma_cm_id *new_cm_id);

    int create_event_channel(); // 创建channel
    int create_completion_channel(); // 创建完成队列的通知channel
    int create_cq(); // 创建完成队列

    // 拒绝连接
    void reject_conn(struct rdma_cm_id* cm_id, const std::string reject_msg);


//            int create_cm_event_thread(); // 创建channel线程
//            void _destroy_event_channel(); // 销毁线程

    // void* _cm_event_thread(void* param);


//    boost::asio::io_service& _ios; // asio事件循环监听服务 dz 现在变成引用了
//            RdmaProperties const& _props; // 屏蔽了
    std::shared_ptr<RdmaConnection> _nextConnection; // 不断创建的一个新的链接，get_request之后就会将新的cm_id放到这个链接里面的cm_id
    bool _running;


    struct rdma_addrinfo* _addrinfo; // 监听地址


    /*处理cm event的那个异步线程 */
    pthread_t  _cm_event_thread_id;

    char one_byte_buffer;
    char one_byte_buffer_cq;

    };
//} // namespace rdma
}  // namespace scidb::rdma

// 之前放在文件开头编译不通过，把RdmaConnection的声明提取出来放到前面也不行，只能放到这里了，这里是ok的
extern std::unordered_map<uint64_t, std::shared_ptr<scidb::RdmaConnection>> gmap_conn; // work_request和conn的全局映射

#endif
