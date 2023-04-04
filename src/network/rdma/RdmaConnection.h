#ifndef RDMA_CONNECTION_H_
#define RDMA_CONNECTION_H_

// boost libraries, not yet implemented as a c++ standard
#include <boost/asio.hpp>
//#include <boost/asio/ssl.hpp>
// c++ standard libraries
#include <unordered_set>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#include <random>
#include <sys/time.h>

#include <functional>
#include "prototypes.h"

#include <network/MessageDesc.h>

namespace google { namespace protobuf {
        class Message;
    }}  // namespace google::protobuf

namespace scidb {
//    namespace rdma {

    // 前向声明
    class RdmaProperties;
    enum class CcmErrorCode;
    enum class CcmMsgType;
    class RdmaCommManager;

/**
 * @class RdmaConnection
 *
 * @ingroup Rdma
 *
 * Request messages read from the connection are processed in a
 * run-to-completion model.  The session and message type are specified in the
 * header. The connection will process the complete message for the session
 * before beginning processing on another message (possibly from another
 * session).
 */
class RdmaConnection : public std::enable_shared_from_this<RdmaConnection>
        {
public:
    RdmaConnection( RdmaCommManager& rdmaCommManager
//                            ,boost::asio::io_service& io_service
//                            ,RdmaProperties const& props
                    );
    ~RdmaConnection() noexcept;

    // conn创建的时候，要传过来
    RdmaCommManager& _mgr;

    struct rdma_cm_id* _cm_id; // rdma通信id, server端建立的连接是从请求中创建的，client端的是主动rdma_create_ep创建的

//  struct ibv_qp* _queue_pair;

    struct ibv_pd* _protection_domain;

    // event queue channel
    struct rdma_event_channel* _cm_event_channel;

    // completion channel
    struct ibv_comp_channel* _completion_channel;

    // completion 队列
    struct ibv_cq* _completion_queue;

    InstanceID _selfInstanceID;

    InstanceID _remoteInstanceId; // 连接的远端instance

    std::string _remoteIp;

    uint16_t _remotePort;

    int start(enum Start_type type = Start_type::server);

    // 借用rdma能力，向远端发送
    int send_remote(std::vector<boost::asio::const_buffer>& constBuffers);

    // 转移到channel中
    void migrate_id(enum Start_type type);

//            int r_listener_bind(struct our_control *listen_conn);

//            boost::asio::ssl::stream<boost::asio::ip::tcp::socket>::lowest_layer_type& getSocket();

    char one_byte_buffer_cq;

    // 建立连接
    int connect(InstanceID src_id, InstanceID dest_id, const std::string remoteIp);

    // 断开连接
    void disconnect();

    // mgr需要访问，所以放到public了
    int post_recv(struct ibv_recv_wr *recv_work_request);

    int post_send(struct ibv_send_wr *send_work_request);

private:
    void _stop() noexcept;

    /*void _handleReadTimeOut(const boost::system::error_code& ec);
    void _startReadProcessing();
//            void _handleTlsHandshake(const boost::system::error_code& e);
    void _handleReadData(const boost::system::error_code& ec, size_t bytes_read);
    void _handleRecordPortion(const boost::system::error_code& ec, size_t bytes_read);
    template <typename T>
    bool _parseRecordPortion(T&);
    void _processRequestMessage();
    void _postSendMessage(const boost::system::error_code& ec, size_t bytes_transferr);*/


    ////////////////方法区///////////////////
    // 创建pd
    int create_pd();

    // 设置queue pair属性
    void setup_qp_params(struct ibv_qp_init_attr *init_attr);

    // 创建queue pair
    int create_qp();

    // 轮询处理cqe
    void handleConpletionQueue();

    // 检查wc状态
    int check_completion_status(struct ibv_wc* work_completion);

    struct ibv_mr* setup_mr(void *addr, unsigned int length, int access);

    void unsetup_mr(struct ibv_mr **mr, int max_slots);

    void setup_sge(const void *addr, unsigned int length, unsigned int lkey, struct ibv_sge *sge);

    int setup_mr_sge(void *addr, unsigned int length, int access, struct ibv_mr **mr, struct ibv_sge *sge);

    void setup_send_wr(void *context, struct ibv_sge *sg_list, enum ibv_wr_opcode opcode, int n_sges, struct ibv_send_wr *send_work_request);

    void setup_recv_wr(void *context, struct ibv_sge *sg_list, int n_sges, struct ibv_recv_wr *recv_work_request);

    int setup_buffer_data(int n_bufs, int access, int data_size);

    void unsetup_buffers();

    int setup_recv_buffers_and_mr();

    int check_wc_status(enum ibv_wc_status status);

    ////////////////方法区///////////////////

//            boost::asio::ip::tcp::socket _socket;
//            RdmaProperties const& _props;
//            boost::asio::ssl::stream<boost::asio::ip::tcp::socket> _sslStream;
//            boost::asio::deadline_timer _readTimer; // 定时器


    // Status of connection variables
    volatile bool _stopped;

//            boost::asio::streambuf _messageStream;

// 下面是 从 r_control 移动过来的
//////////////////////////////////////////////////////////////////////////////////

    // client端地址解析
    struct rdma_addrinfo *_res;

/***** this part contains fields for general info *****/
    uint64_t		wc_send;
    uint64_t		wc_recv;

/***** this part contains fields for CM and completions interaction *****/

    // rdma_disconnect 被调用的时候，设置为1
    volatile int disconnected;

    /* holds count of number of cq_events that still need to be acked */
    unsigned int cq_events_that_need_ack;

 // 异步线程取 cm events

    /* lock to protect asynchronous access to the cm event fields */
//    pthread_mutex_t			cm_event_info_lock;

    // 条件变量
//    pthread_cond_t			cm_event_info_notify;

    /* non-zero when thread is waiting on cm_event_info_notify */
//    unsigned int			waiting_on_info_notify;

    /* holds type of latest asynch cm event reported by cm */
    enum rdma_cm_event_type		latest_cm_event_type;

    /* holds status of latest asynch cm event reported by cm */
    int				latest_status;

    /* holds cm_id from latest asynch cm event reported by cm */
    struct rdma_cm_id		*latest_cm_id;

    // private数据长度
    int				latest_private_data_len;

//            struct r_connect_info latest_private_data;

    enum rdma_cm_event_type		current_cm_event_type;

    // 处理cm event的那个线程
    pthread_t cm_event_thread_id;

/***** 数据传输相关 *****/

    /* queue pair */
//    struct ibv_qp* queue_pair;

    // buffer数组，存储了多个buffer的指针
    unsigned char* buffer_data[MAX_BUFFER_NUMS];

    // 分配的memory region数组
    struct ibv_mr* user_data_mr[MAX_MR];

    // scatter-gather element array for user_data send/recv operations
    struct ibv_sge user_data_sge[MAX_SGE];

    // 发送请求wr数组，目前是256个
    struct ibv_send_wr	send_work_request[MAX_WORK_REQUESTS];

    // 接收请求wr数组
    struct ibv_recv_wr	recv_work_request[MAX_WORK_REQUESTS];

    // 注册的buffer数量
    int n_data_bufs = 0;

//    struct ibv_mr *mr, *send_mr; // 注册的memory region  前面那个mr用不上了，换成上面的mr数组了

/////////////////////////////////////////////////////////////////////////////////

    // 接收的消息

    // other
    bool _is_client = false;

    std::independent_bits_engine<std::default_random_engine, 64, unsigned long int> engine;
};

//} // namespace rdma
}  // namespace scidb

#endif
