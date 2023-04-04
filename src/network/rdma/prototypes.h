
#ifndef _PROTOTYPES_H_
#define _PROTOTYPES_H_

//#include <stdio.h>
#include <cstdio>
//#include <stdlib.h>
#include <cstdlib>
#include <unistd.h>
//#include <errno.h>
#include <cerrno>

//#include <ctype.h>
#include <cctype>

//#include <string.h>
#include <string>
//#include <time.h>
//#include <sys/time.h>

#include <sys/resource.h>
#include <sys/param.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <inttypes.h>
#include <infiniband/arch.h>
#include <pthread.h>
#include <semaphore.h>

// dz add
#include <iostream>
#include <vector>


//#define WHITE_SPACE " \n\r\t\v\f"

//#define _MAX_STRERROR_MALLOC	64
#define Asio boost::asio

#define LISTEN_PORT 12321

// rdma监听端口
static const char *listen_port = "12321";

// 之前rq depth是16，改成256了
#define SQ_DEPTH	64
#define RQ_DEPTH	64
#define MAX_SEND_SGE 4
#define MAX_RECV_SGE 4

// 默认端口，在config.ini中可以另外配置
#define RDMA_DEFAULT_PORT	12321

// 监听backlog
#define RDMA_BACKLOG		3

/* default number of iterations */
#define OUR_DEFAULT_LIMIT       500000

/* default size of data buffer transfered in one message */
#define DEFAULT_DATA_SIZE		8
// 用4096在传输的时候，发现发送了一个4159的数据的时候，收到对方发来的错误信息：remote invalid request error，
// 应该是对端接收的buffer大小不够，报了这个错误，这里先调大一倍
//#define MAX_BUFFER_SIZE       4096
#define MAX_BUFFER_SIZE       8192

// szu_lab中实测 max_recv_wr是64，max_send_wr是111
#define MAX_BUFFER_NUMS     64
//#define MAX_BUFFER_NUMS     8 // recv设置8个

// 最大的memory region数量
#define MAX_MR     64
#define MAX_SGE     64

// 最大的发送/接收 work request 数量
#define MAX_WORK_REQUESTS       64

enum class Start_type{client, server};

/* bits in our flags word */
/*
#define VERIFY		0x01
#define TRACING		0x02
#define VERBOSE_TRACING	0x04
#define PRINT_STATS	0x08
 */
/*
struct rdma_options {
    char		*server_name;
    char		*server_port;
    char		*message;
    unsigned long	server_port_number;
    uint64_t	limit;
    uint64_t	data_size;
    int		send_queue_depth;
    int		recv_queue_depth;
    int		max_send_sge;
    int		max_recv_sge;
    unsigned int	flags;
};*/


/* structure to hold private data client sends to server on a connect */
// 保存发送过来的private data
struct r_connect_info { // dz
    uint64_t	remote_limit;
    uint64_t	remote_data_size;
};

/* this structure holds all info relevant to a single connection,
 * whether client, listener or agent
 */
struct r_control {
    uint64_t		wc_send;
    uint64_t		wc_recv;

    // connection manager event channel
    struct rdma_event_channel	*cm_event_channel;

    // connection manager id
    struct rdma_cm_id		*cm_id;

    // 设置为1表示断开了
    volatile int			disconnected; // volatile关键字

    // pd
    struct ibv_pd			*protection_domain;

    // comp channel
    struct ibv_comp_channel		*completion_channel;

    // completion queue
    struct ibv_cq			*completion_queue;

    // 待确认的cq events
    unsigned int			cq_events_that_need_ack;

    pthread_mutex_t			cm_event_info_lock;

    // 条件变量
    pthread_cond_t			cm_event_info_notify;


    // 最新的event类型
    enum rdma_cm_event_type		latest_cm_event_type;

    int				latest_status;

    struct rdma_cm_id		*latest_cm_id;

    // private数据长度
    int				latest_private_data_len;

    // private_data
    struct r_connect_info		latest_private_data;

    // 主线程现在cm 类型
    enum rdma_cm_event_type		current_cm_event_type;

    // 处理cm event的那个线程
    pthread_t			cm_event_thread_id;

    // array of work completions //
    struct ibv_wc			*work_completion;

    // work_completion最多的元素
    int				max_n_work_completions;

    // poll出来的数量
    int				current_n_work_completions;

    // wc中要处理的下一个index
    int				index_work_completions;

    // dynamically allocated array to hold histogram of number of
     // completions returned on each call to ibv_poll_cq()
     //
    unsigned long			*completion_stats;

    // counts number of successful calls to ibv_req_notify() //
    unsigned long			notification_stats;

    // 数据传输
    // queue pair
    struct ibv_qp			*queue_pair;

    // 存储的数据
    unsigned char		*user_data[MAX_BUFFER_NUMS];

    // mr
    struct ibv_mr		*user_data_mr[MAX_MR];

    // sge
    struct ibv_sge		user_data_sge[MAX_SGE];

    // send wr
    struct ibv_send_wr	user_data_send_work_request[MAX_WORK_REQUESTS];

    // rece wr
    struct ibv_recv_wr	user_data_recv_work_request[MAX_WORK_REQUESTS];

    int			n_user_data_bufs;

};


struct r_cm_event_thread_params {
    // to synchronize thread start-up with main-line thread //
    sem_t sem; // dz 用于线程同步，在创建cm thread的时候先初始化然后作为参数传递过去了
    struct r_control *conn;
    struct rdma_event_channel *cm_event_channel;
};


//extern void * our_calloc(unsigned long size, const char *message);

#endif	/* _PROTOTYPES_H_ */
