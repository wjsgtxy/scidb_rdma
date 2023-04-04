// The header file for the implementation details in this file
#include "RdmaCommManager.h"
// header files from the rdma module
#include "RdmaConnection.h"
#include "RdmaProperties.h"
// third-party libraries
#include <log4cxx/logger.h>
#include <mutex>
#include <ccm/Uuid.h>

#include <network/Connection.h> // for ServerMessageDesc
#include <array/CompressedBuffer.h>
#include <condition_variable> // dz szu_lab 
#include <thread> // dz szu_lab gcc5
//using boost::asio::ip::tcp;
#include <storage/StorageMgr.h>

using namespace std;
typedef Asio::posix::stream_descriptor descriptro_type;
extern std::mutex g_rdma_mutex;

std::unordered_map<uint64_t, struct ibv_recv_wr *> gmap_wr; // 全局变量，保存wr和对应的wr指针
std::unordered_map<uint64_t, std::shared_ptr<scidb::RdmaConnection>> gmap_conn; // work_request和conn的全局映射
bool g_is_rdma_init_ok = false;
extern std::condition_variable g_cv_rdma_thread_init_ok; // 条件变量，告诉主线程当前rdma线程准备OK了

/**
 * 服务器的接收端，接收其他节点发过来的请求，同时它还可以作为 rdma连接的发起端，就像conn中在 networkdmanager中的 async_connect
 */
namespace {
    // 匿名空间，会有一个唯一的名字，编译器在下方会同时添加一条using namespace + 编译器对该匿名空间生成的唯一名字，类似于static的效果，但是
    // 这个里面定义的函数，对外extern不可见，因为你不知道匿名空间名称，c11标准推荐使用匿名空间而不是static
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.rdma.RdmaCommManager"));

    static const char *server = "0.0.0.0";
//    static const char *port = "7471"; // 每个instance 监听的端口应该不同才可以
    // 每个instance在初始化的时候，server id和ins id就已经存储在本地了，后面network manager启动的时候，直接从本地的文件中读取了，所以source instance id是直接有的
    // 但是socket端口号，每个ins都不同，需要从boost acceptor中获取，我们这里的端口号，也需要动态变化，初始化的时候从某个端口号开始获取吧。
}

muduo::net::EventLoop* g_loop = nullptr; // muduo全局loop循环

// 线程函数
void rdmaThread(){
    g_loop = new muduo::net::EventLoop();

    // 初始化rdma连接管理器
    scidb::RdmaCommManager::getInstance()->run();

    LOG4CXX_DEBUG(logger, "begin muduo loop");
    g_loop->loop(); // 线程循环
    LOG4CXX_DEBUG(logger, "after muduo loop");
}

namespace scidb {
//    namespace rdma {

        typedef std::shared_ptr<RdmaConnection> srptr;

        /**
         * RdmaCommManager
         */
        // RdmaCommManager::RdmaCommManager(RdmaProperties const& props)
        RdmaCommManager::RdmaCommManager()
                :
//                _ios() // ioservice使用默认构造函数 后面改成引用了，引用的话就需要初始化了，不需要这个成员变量了
                // , _props(props)
//                , ch_fd(_ios) // 使用muduo后不需要这两个流了
//                , cq_fd(_ios)
//                , _sessionCapsule(_ios)
//                , _acceptor(_ios, tcp::endpoint(tcp::v4(), _props.getPort()))
                _nextConnection(nullptr)
                , _running(false)
        {
            // LOG4CXX_DEBUG(logger, "Creating RdmaCommManager on port " << props.getPort());
//            LOG4CXX_DEBUG(logger, "Creating RdmaCommManager on port " << LISTEN_PORT); // 固定的12321端口
            // 记录线程id
            LOG4CXX_DEBUG(logger, "Creating RdmaCommManager on thread " << std::hex << std::this_thread::get_id() << std::dec);
            LOG4CXX_DEBUG(logger, "Creating RdmaCommManager pointer this is " << this);
            cq_events_that_need_ack = 0;

            LOG4CXX_DEBUG(logger, "Creating RdmaCommManager inmap is " << &_rinConnections);
            LOG4CXX_DEBUG(logger, "Creating RdmaCommManager outmap is " << &_routConnections);
        }

        RdmaCommManager::~RdmaCommManager(){
            LOG4CXX_DEBUG(logger, "~RdmaCommManager");
            stop();
        }

        /**
         * conn mgr线程主循环
         */
        void RdmaCommManager::run()
        {
            LOG4CXX_INFO(logger, "Starting RdmaCommManager Impl.");

            // 先获取自身的instance id
            _selfInstanceID = StorageMgr::getInstance()->getInstanceId();

            // 通过instance id，获取server id 和 ins id
            server_id = getServerId(_selfInstanceID);
            ins_index = getServerInstanceId(_selfInstanceID);

            int ret;
            // 开始监听 rdma连接
            try {
                if (_running) {
                    LOG4CXX_WARN(logger, "RdmaCommManager is already running.");
                    return;
                }

                // 先查找设备，获取ibv_context上下文
                int	num_devices;
                struct ibv_context	**list;
                list = rdma_get_devices(&num_devices);
                if(list == nullptr || num_devices == 0) {
                    LOG4CXX_ERROR(logger, "no rdma device.");
                    return;
                }
                verbs = *list; // 设备上下文
                LOG4CXX_DEBUG(logger, "ibv_context is " << verbs);

                // 创建一个pd，给所有的qp使用，只用一个pd，简单方便一些
                _protection_domain = ibv_alloc_pd(verbs);

                // 初始化工作，创建cm_id和channel,到监听那一步
                ret = initRdma();
                if(ret){
                    LOG4CXX_ERROR(logger, "init rdma errro.");
                    return;
                }

                // 下一个事件循环监听channel，等待cm事件RDMA_CM_EVENT_CONNECT_REQUEST
                // 开始接受下一个链接
                LOG4CXX_DEBUG(logger, "start accept");
                LOG4CXX_DEBUG(logger, "this pointer is " << this); // this直接0x输出的
                startAccept();

                LOG4CXX_DEBUG(logger, "start watch completion channel");
                startCompletion(); // 这里可以async read

//                startAcceptNextConnection();

                // 线程主循环，这个后面的代码就要退出的时候才能执行了
                _running = true;
//                _ios.run(); // boost asio的循环，使用muduo的循环，就不用这个了

//                EventLoop* loop = new EventLoop();
                // 初始化成功，修改同步的条件变量，让主线程继续运行
                std::unique_lock<std::mutex> locker(g_rdma_mutex); // 类似lock_guard,不过更加灵活，会自动释放锁
                g_is_rdma_init_ok = true;
                g_cv_rdma_thread_init_ok.notify_one(); // notify一个阻塞的线程

            } catch (std::exception& e) {
                LOG4CXX_ERROR(logger, "Unexpected failure when running rdma communications manager." << e.what());
            }
//            _running = false;
//            LOG4CXX_DEBUG(logger, "RdmaCommManager._running is false now");
        }

        void RdmaCommManager::stop()
        {
            LOG4CXX_INFO(logger, "Stopping RdmaCommManager Impl.");

            // 释放资源, 注意rdma相关资源的释放顺序
             rdma_destroy_id(_cm_id);
            // 释放监听的cm_id，create_ep创建的cm_id才需要这个
//            rdma_destroy_ep(_cm_id);

            // 释放channel
            // 代码注释中说：All rdma_cm_id's associated with the event channel must be destroyed, and all returned events must be acked before calling this function.
            rdma_destroy_event_channel(_cm_event_channel);

            // 释放地址
//            rdma_freeaddrinfo(_addrinfo);

            // 释放cq
            int ret = ibv_destroy_cq(_completion_queue);
            if (ret != 0) {
                // 如果还有其他qp关联到这个cq上，会失败
                LOG4CXX_ERROR(logger, "ibv_destroy_cq failed");
            }

            // 释放malloc分配的内存
            free(_work_completion);
            
            _running = false;
            LOG4CXX_INFO(logger, "RdmaCommManager Impl stopped.");
//            _ios.stop();
        }

        void RdmaCommManager::handleConnectionAccept(boost::system::error_code ec)
        {
            if (!ec) {
                _nextConnection->start();
                // 相当于主线程有2个异步监听程序了，或者说和另外一个event线程一起，不用另外起一个线程了
            } else {
                LOG4CXX_ERROR(logger, "Error from accept() on socket. (" << ec.message() << ")");
            }
            // Start accept for the next incoming connection.
//            _startAcceptNextConnection();
        }

        /**
         * 初始化RDMA环境，包括监听cm_id和channel, 只到监听listen那一步
         */
        int RdmaCommManager::initRdma() {
            int	ret = 0;
            LOG4CXX_INFO(logger, "start init rdma env.");

            // 创建一个channel，给所有cm_id使用, 创建的channel放在成员变量_cm_event_channel中，这个是事件通知的channel
            ret = create_event_channel();
            if(ret){
                LOG4CXX_ERROR(logger, "create_event_channel failed. reason is: " << gai_strerror(ret));
                return -1;
            }
            LOG4CXX_INFO(logger, "create_event_channel success, event channel is " << (void*)_cm_event_channel);

            // 创建cm_id，创建的时候传入了channel就表示是个异步的cm_id了！！！
            ret = rdma_create_id(_cm_event_channel, &_cm_id, this, RDMA_PS_TCP); // RC链接，第三个参数是自定义的和cm_id关联的context
            if (ret != 0) {
                LOG4CXX_ERROR(logger, "rdma_create_id error");
                rdma_destroy_event_channel(_cm_event_channel);
                return ret;
            }
            LOG4CXX_INFO(logger, "rdma_create_id cm_id is: " << _cm_id << ", event channel is " << _cm_id->channel); // rdma_cm_id结构体里面还有很多内容，只能当指针输出地址了

            // 解析地址
//            struct rdma_addrinfo hints; // *_addrinfo; // res变成成员变量，后面方便释放

            /*memset(&hints, 0, sizeof hints);
            hints.ai_flags = RAI_PASSIVE; // server端
            hints.ai_port_space = RDMA_PS_TCP; // RC连接
            ret = rdma_getaddrinfo(server, listen_port, &hints, &_addrinfo); // server是0000 port 12321
            if (ret) {
                LOG4CXX_ERROR(logger, "rdma_getaddrinfo: " << gai_strerror(ret));
                return -1;
            }*/

            // 原来解析本地地址的方法
            struct addrinfo* aptr, hints;

            // 获取local绑定地址
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_PASSIVE; // server端
            unsigned int port = LISTEN_PORT + ins_index;
            LOG4CXX_DEBUG(logger, "getaddrinfo before, server is " << server << ", listen port is " << port); // 每个ins监听的端口是 12321加上自身在节点的ins索引

            ret = getaddrinfo(server, to_string(port).c_str(), &hints, &aptr); // 重要，这里要绑定一个具体的地址，绑定0000的话，对应的listenid没有verbs参数
            if (ret != 0) {
                LOG4CXX_ERROR(logger, "getaddrinfo error: " << gai_strerror(ret));
                return ret;
            }

            // 绑定到地址，会同时绑定到一个本地设备，绑定到具体ip才会绑定到设备，如果只是监听端口同时server是通配符，那么不会有context绑定上
            errno = 0;
            LOG4CXX_DEBUG(logger, "rdma_bind_addr before, ai_addr is " << (struct sockaddr *)aptr->ai_addr);
            ret = rdma_bind_addr(_cm_id, (struct sockaddr *)aptr->ai_addr);
            if (ret != 0) {
                LOG4CXX_ERROR(logger, "rdma_bind_addr error: " << gai_strerror(ret));
                return ret;
            }

            // 绑定addr之后，cm_id 会新增verbs， port， pd
            LOG4CXX_INFO(logger, "rdma_bind_addr success, cm_id:" << (void *)_cm_id);
//            LOG4CXX_INFO(logger, "rdma_bind_addr success, cm_id:" << (void *)_cm_id << ", ibv_context is " << (void*)_cm_id->verbs <<
//                    ", port num is " << _cm_id->port_num << ", pd is " << _cm_id->pd ); // port num是nul， pd 是0
//            LOG4CXX_INFO(logger, "ibv_context dev name is " << _cm_id->verbs->device->dev_name); // 绑定0000没有对应的context的


            // queue pair 初始化属性
//            struct ibv_qp_init_attr init_attr;
//            memset(&init_attr, 0, sizeof init_attr);
//            init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = 1; // set 发送和接受wr最大为1
//            init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1; // set sge为1
//            init_attr.cap.max_inline_data = 16; // 最大inline数据
//            init_attr.sq_sig_all = 1; // 所有的wr都通知

//            LOG4CXX_DEBUG(logger, "init rdma: rdma_create_ep");
            // 原型：int rdma_create_ep (struct rdma_cm_id **id, struct rdma_addrinfo *_addrinfo, struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);
            // 这个函数创建cm_id，通过第一个参数返回创建的cm_id, 如果提供了第三个参数，会创建pd, 提供第四个参数qp初始化属性，会创建qp, 这个qp是绑定到后面的连接cm_id上的
//            ret = rdma_create_ep(&_cm_id, _addrinfo, NULL, &init_attr);

            /*ret = rdma_create_ep(&_cm_id, _addrinfo, NULL, NULL); // 先不创建qp了，后面再创建
            if (ret) {
                LOG4CXX_ERROR(logger, "rdma_create_ep error");
                return -1;
            }*/

//            LOG4CXX_DEBUG(logger, "ibv context is " << _cm_id->verbs << ", dev name is " << _cm_id->verbs->device->dev_name); // 使用rdma_create_ep创建的cm_id, 它的verbs是个空指针！！！
//            LOG4CXX_INFO(logger, "rdma_create_ep cm_id:" << (void *)_cm_id << ", pd should be null : " << _cm_id->pd); // rdma_cm_id结构体里面还有很多内容，只能当指针输出地址了, 实际输出0x7f2c34007d20这种


            // 将生成的cm_id转移到前面的channel中，同步变异步
            // 不需要了，上面用的 rdma_create_id 直接绑定了channel
            /*LOG4CXX_DEBUG(logger, "before: _cm_id event channel is " << (void*)_cm_id->channel);
            ret = rdma_migrate_id(_cm_id, _cm_event_channel);
            if(ret){
                LOG4CXX_ERROR(logger, "rdma_migrate_id error, errno is" << errno);
                return -1;
            }
            LOG4CXX_DEBUG(logger, "after: _cm_id event channel is " << (void*)_cm_id->channel);*/

            // 将channel对应的fd更改成非阻塞的，这样在获取这个channel的事件的时候，才不会阻塞
            ret = changeNonBlock(_cm_event_channel->fd);
            if(ret){
                LOG4CXX_ERROR(logger, "cm_event_channel changeNonBlock error");
                return -1;
            }

            // 创建另外的一个线程，处理cm events
            // 现在变成同一个线程处理所有的事件了
//            _create_cm_event_thread();

            // 开始监听
            // 用rdma_create_ep之后，服务端对应的cm_id会被置为listening state, 就不用之前的那种rdma_bind_addr了
            LOG4CXX_DEBUG(logger, "start rdma_listen...");
            ret = rdma_listen(_cm_id, RDMA_BACKLOG); // 之前demo是0，但是ping-sr-3用的是backlog=3 应该表示缓冲中的连接
            if (ret) {
                LOG4CXX_ERROR(logger, "rdma_listen error: " << gai_strerror(ret));
                return ret;
            }

            // 创建cq对应的channel和cq
//            LOG4CXX_DEBUG(logger, "start create completion channel using ibv open.");
            /*int	num_devices, count;
            struct ibv_context	**list, **lptr;
            list = rdma_get_devices(&num_devices);
            if(list == nullptr || num_devices == 0) {
                LOG4CXX_ERROR(logger, "No RDMA devices found");
                return -1;
            }
            LOG4CXX_DEBUG(logger, "ibv_context is " << (void*)(*list) << " ,dev name is" << (*list)->device->dev_name);
//            struct ibv_context * vctx = ibv_open_device(struct ibv_device *device);
            _completion_channel = ibv_create_comp_channel(*list);
            if(_completion_channel){
                LOG4CXX_DEBUG(logger, "using device,  completion channel is " << (void*)_completion_channel << ", ch fd is " << _completion_channel->fd);
            }*/

            LOG4CXX_DEBUG(logger, "start create completion channel.");
            ret = create_completion_channel();
            if(ret){
                LOG4CXX_ERROR(logger, "create_completion_channel error");
                return ret;
            }
            // 将cq的channel也变成异步的
            LOG4CXX_DEBUG(logger, "comp channel fd id " << _completion_channel->fd);
            ret = changeNonBlock(_completion_channel->fd);
            if(ret){
                LOG4CXX_ERROR(logger, "completion_channel changeNonBlock error");
                return -1;
            }

            return ret;
        }

        /**
         * 开始接受请求
         */
        void RdmaCommManager::startAccept()
        {
//            assert(_selfInstanceID != INVALID_INSTANCE);
//            std::shared_ptr<Connection> newConnection =
//                    std::make_shared<Connection>(*this, getNextConnGenId(), _selfInstanceID); // 这里是新建的一个conn链接，是要等待连接过来的，所以没有目标instanceid.
//            _acceptor.async_accept(newConnection->getSocket(),
//                                   std::bind(&NetworkManager::handleAccept,
//                                             this,
//                                             newConnection,
//                                             std::placeholders::_1));

//            Asio::async_read(ch_fd, Asio::buffer(buf, 1), boost::bind(&funcCallback, Asio::placeholders::error, conn, cm_event_channel, options));

            /*ch_fd.async_read_some(boost::asio::buffer((void*)&one_byte_buffer, sizeof(one_byte_buffer)),
                                   std::bind(&RdmaCommManager::handleEvent,
                                             this,
                                             std::placeholders::_1,
                                             std::placeholders::_2));*/

//            ch_fd->async_read_some(boost::asio::buffer((void*)&one_byte_buffer, sizeof(one_byte_buffer)),
//                                  std::bind(&RdmaCommManager::handleEvent,
//                                            this,
//                                            std::placeholders::_1,
//                                            std::placeholders::_2));

            _ch_channel = new muduo::net::Channel(g_loop, _cm_event_channel->fd); // muduo的channel，封装一个fd，监听这个fd的改变
//            _ch_channel->setReadCallback(std::bind(&RdmaCommManager::handleEvent, this, std::placeholders::_1));
            _ch_channel->setReadCallback(std::bind(&RdmaCommManager::handleEvent, this)); //
            _ch_channel->enableReading(); // muduo监听fd的可读消息！因为rdma中的eventchannel实际上只是一个fd，只会报告可读消息，但是要读取信息，不能再这个fd上面读取
        }

void RdmaCommManager::reject_conn(struct rdma_cm_id* cm_id, const std::string reject_msg)
{
    // 拒绝建立连接
    int ret = rdma_reject(cm_id, reject_msg.c_str(), reject_msg.size());
    errno = 0;
    if(ret){
        LOG4CXX_ERROR(logger, "rdma_reject failed, errno is " << errno);
    }
}

void ack_event(struct rdma_cm_event *cm_event){
    // 所有rdma_get_cm_event的event都需要确认
    rdma_ack_cm_event(cm_event);
}

/**
 * 处理事件回调
 * @param error
 * @param bytes_transferred
 */
//        void RdmaCommManager::handleEvent(const boost::system::error_code &error, std::size_t bytes_transferred){
//        void RdmaCommManager::handleEvent(Timestamp receiveTime){ // muduo可以传一个参数回来，这里感觉用不上，就算了
void RdmaCommManager::handleEvent(){
    LOG4CXX_DEBUG(logger, "RdmaCommManager handle event start.");
    // 先判断是否出错
//  if (!error)
//  {
        enum rdma_cm_event_type this_event_type;
        int	this_event_status;
        struct rdma_cm_event *cm_event; // 获取的事件详细信息
        struct rdma_cm_id *cm_id;

        int	ret, disconnected, give_signal;

        // 默认这里是阻塞函数，现在改成非阻塞函数
        errno = 0;
        ret = rdma_get_cm_event(_cm_event_channel, &cm_event);

        // 先判断返回的结果
        if (ret != 0) { // error返回了11，表示EAGIN
//                    LOG4CXX_DEBUG(logger, "rdma_get_cm_event ret is: " << ret << ", error no is: " << errno);
            if(errno == 11){
                // 表示EAGIN，则继续获取
                LOG4CXX_TRACE(logger, "rdma_get_cm_event ret is: " << ret << ", error no is: " << errno);
                /*ch_fd.async_read_some(boost::asio::buffer((void*)&one_byte_buffer, sizeof(one_byte_buffer)),
                                      std::bind(&RdmaCommManager::handleEvent, this,
                                                std::placeholders::_1,
                                                std::placeholders::_2));*/
//                        startAccept();
                return;
            }else{
                LOG4CXX_ERROR(logger, "rdma_get_cm_event error， errno is: " << errno);
                // 现在先不管发生什么错误，都继续接受下一次请求
//                        startAccept();
                return;
            }
        }

        /* synch with main-line thread on who calls rdma_disconnect() */
//                pthread_mutex_lock(&conn->cm_event_info_lock);

        // 获取该事件对应的cm_id
        cm_id = cm_event->id; // 如果type是RDMA_CM_EVENT_CONNECT_REQUEST,那么这个是新生成的一个id
        // 获取当前事件类型
        this_event_type = cm_event->event;
        // 事件的状态
        this_event_status = cm_event->status;
        LOG4CXX_DEBUG(logger, "rdma_get_cm_event return id: " << (void*)cm_id << ", type: " << rdma_event_str(this_event_type) << ", status: " << this_event_status);

//                disconnected = conn->disconnected;
//                if (this_event_type == RDMA_CM_EVENT_DISCONNECTED && disconnected == 0) {
//                    /* main-line did not make call, we must do it */
//                    conn->disconnected = 1;
//                }



        /* check the conn to which this cm_id belongs */
        //  check一下
//                if (conn != cm_id->context) {
//                    our_report_ptr("cm_event_thread",
//                                   "WARNING: conn", conn, options);
//                    our_report_ptr("cm_event_thread",
//                                   "WARNING: cm_id->context",
//                                   cm_id->context, options);
//                }

        if(this_event_type == RDMA_CM_EVENT_CONNECT_REQUEST) {
            // 是建立连接的请求，从中获取新的cm_id，有了新的id之后，就可以创建conn了
            struct rdma_cm_id* listen_id = cm_event->listen_id; // 对应的监听id
            if(listen_id != _cm_id){
                LOG4CXX_ERROR(logger, "rdma_get_cm_event listen_id is: " << listen_id << ", mgr listen id is " << _cm_id);
                // assert(listen_id == _cm_id); // 这两个id应该是相等的 模拟环境的时候发现两个不一样 new: 真实环境测试中发现又一样了
            }

            // 将这个链接加入到instance map中
            InstanceID remote_Iid = INVALID_INSTANCE;

            int private_data_len = cm_event->param.conn.private_data_len; // dz uint8_t 是 unsigned char, 转成int
            int retry_cnt = cm_event->param.conn.retry_count; // 这两个也都是 uint8_t
            int rnr_retry_count = cm_event->param.conn.rnr_retry_count;

            LOG4CXX_TRACE(logger, "uint8 private_data_len:" << cm_event->param.conn.private_data_len << ", int private_data_len:" << private_data_len); // 大部分int是56
//            LOG4CXX_DEBUG(logger, "uint8 retry_cnt:" << cm_event->param.conn.retry_count << ", int retry_cnt:" << retry_cnt);
//            LOG4CXX_DEBUG(logger, "uint8 r_retry_cnt:" << cm_event->param.conn.rnr_retry_count << ", int rnr_retry_count:" << rnr_retry_count);

            if((private_data_len) > 0){ // 这个值会比实际申请的值要大一些，一般可能是56字节
                // 将instance id通过 连接请求的private data传输过来
//                const char* data = static_cast<const char*>(cm_event->param.conn.private_data); // 本身是void*
//                LOG4CXX_DEBUG(logger, "conn.private data is " << data);
//                std::string sdata(data);
//                remote_Iid = hex2dec(sdata); // 暂未考虑网络字节序的问题。其实不用，因为字符型的不用考虑网络字节序

                uint64_t private_data;
                memcpy(&private_data, cm_event->param.conn.private_data, 8);
//                LOG4CXX_DEBUG(logger, "network order private data is " << private_data);

                remote_Iid = ntohll(private_data); // 将网络字节序变成主机字节序，这里是直接传输的instance id，也就是一个uint64_t，需要考虑网络字节序
                LOG4CXX_INFO(logger, "rdma connect request remote Iid: " << Iid(remote_Iid)); // id is 0, private data len is 8
            }

            if(remote_Iid != INVALID_INSTANCE)
            {
                // 读取也要加锁
                bool flag = false;
                {
                    ScopedMutexLock scope(_inMutex, PTW_SML_NM); // 第二个参数用的network manager的
                    auto it = _rinConnections.find(remote_Iid);
                    flag = (it != _rinConnections.end());

                    if(flag){
                        // 找到了，有问题，重复建立链接
                        LOG4CXX_ERROR(logger, "error receive instanceid already exist in conn map, id is " << remote_Iid << ", rdma conn cm_id is " << (*it).second->_cm_id);
                        reject_conn(cm_id, "already exist");
                        ack_event(cm_event);
                        return;
                    }
                }

                // map中没找到，则新建一个, 新建的时候，先判断目前是否还有其他的在新建了，如果有，则不新建了
                {
                    ScopedMutexLock scope(_inInitMutex, PTW_SML_NM);
                    if(_rinInitConnections.count(remote_Iid) != 0){
                        // 找到了，目前有这个remote端到当前节点的其他连接正在建立中，就不建立了
                        LOG4CXX_ERROR(logger, "receive instanceid other link is building, id is " << remote_Iid);
                        reject_conn(cm_id, "other building");
                        ack_event(cm_event);
                        return;
                    }else{
                        // 没找到，说明还没有连接开始建立，则先插入initmap，表明开始建立链接了
                        _rinInitConnections.insert(remote_Iid);
                    }
                }

                // 开始建立连接了
                _nextConnection = std::make_shared<RdmaConnection>(*this);
                _nextConnection->_cm_id = cm_id;

                LOG4CXX_DEBUG(logger, "_nextConnection start begin.");
                int start_ret = _nextConnection->start(Start_type::server);
                if(start_ret == 0)
                {
                    // 启动成功才放到map中去
                    LOG4CXX_DEBUG(logger, "_nextConnection start success. _nextConnection _cm_id is " << _nextConnection->_cm_id);
                    ScopedMutexLock scope(_inMutex, PTW_SML_NM);
                    auto insert_ret = _rinConnections.insert(std::make_pair(remote_Iid, _nextConnection));
                    LOG4CXX_DEBUG(logger, "insert into map " << (insert_ret.second == true ? "success" : "failed") << " ,now server inmap constains remoteid " << remote_Iid
                        << " conn cm_id is " << _rinConnections[remote_Iid]->_cm_id);
                }else{
                    // 启动没有成功，上面的_nextConnection是智能指针，会自动析构释放
                    LOG4CXX_ERROR(logger, "_nextConnection started failed. now is going to deconstructing conn, cm_id is " << cm_id);
                    reject_conn(cm_id, "self internal error");
                }
                // 不管成功或者失败，都删除initset中的信息，因为如果上面的map insert失败了，那么这里不能阻止其他的连接建立
                {
                    ScopedMutexLock scope(_inInitMutex, PTW_SML_NM);
                    _rinInitConnections.erase(remote_Iid);
                }

            }else{
                // 建立连接的请求需要携带远端的 instance id，否则拒绝连接
                LOG4CXX_ERROR(logger, "RDMA_CM_EVENT_CONNECT_REQUEST：remote id is invalid, id is " << remote_Iid);
                reject_conn(cm_id, "no src id");
            }

        } else if (this_event_type == RDMA_CM_EVENT_DISCONNECTED) {
            // 关闭连接
            if (disconnected == 0) {
                // 远端先关闭了，我们也关闭本地的连接，注意，关闭的是事件对应的cm_id，不是监听的 _cm_id
                LOG4CXX_INFO(logger, "rdma get event: disconnected, cm_id is " << (void *)cm_id);
                errno = 0;
                ret = rdma_disconnect(cm_id);
                if (ret == 0) {
                    // 正常关闭
                    LOG4CXX_INFO(logger, "rdma_disconnect ok. cm_id is" << (void *)cm_id);
                    // disconnected = 1;
                } else if (errno != EINVAL) { // EINVAL表示参数错误
                    // 非正常
                    LOG4CXX_ERROR(logger, "rdma_disconnect error. errno is " << errno << ", cm_id is" << (void*)cm_id);
                }
            } /* else already flushed by remote */

        } else if(this_event_type == RDMA_CM_EVENT_ESTABLISHED){
            LOG4CXX_INFO(logger, "RDMA_CM_EVENT_ESTABLISHED, established cm_id is " << cm_id);
            // 连接建立成功 map要不要再这里插入？ new：不太行，实际没有private_data 数据

            // established 也有private_data, 也是在ack之后释放
//            uint64_t iid = ntohll(*(uint64_t*)cm_event->param.conn.private_data); // dz 实际测试发现 private_data_len是0，private_data是0x0
//            LOG4CXX_DEBUG(logger, "remote established id is " << iid << ", transform to instance id is " << Iid(iid));
        } else {
            // 其他事件暂时不处理，记录error日志
            // 目前接受到过 RDMA_CM_EVENT_REJECTED，是已经建立链接accept之后
            LOG4CXX_ERROR(logger, "other event type, todo to process... type is "
                                    << rdma_event_str(this_event_type));
        }

        // dz 将rdma_ack_cm_event确认的时机放到最后了，之前放在上面，但是这个函数会释放掉 event里面的private_data数据！！
        // 导致我调试了这个private_data的segmentation fault错误好几天！
        ack_event(cm_event);

//            } else {
//                // boost errro 错误
//                LOG4CXX_ERROR(logger, "dz handleEvent error");
//                LOG4CXX_ERROR(logger, "Error #" << error
//                                                << " : '" << error.message()
//                                                << "' when handle event");
//                throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_ACCEPT_CONNECTION) << error << error.message();
//            }

}

        /**
         * 接收下一个请求，请求放在一个新建的RdmaConn中的cm_id里面
         * */
//        void RdmaCommManager::startAcceptNextConnection(struct rdma_cm_id* cm_id)
//        {
//            _nextConnection = std::make_shared<RdmaConnection>(_ios, _props, context, _sessionCapsule);

            // register a call back to process when a connection is accepted on the
            // socket.  handleConnectionAccept calls startAcceptNextConnection after
            // starting the accepted connection so that the second and subsequent
            // connections are started.
            /*_acceptor.async_accept(_nextConnection->getSocket(),
                                   std::bind(&RdmaCommManager::_handleConnectionAccept,
                                             this,
                                             std::placeholders::_1));*/

            // 新建一个conn, 将产生的新的cm_id放到_nextConnection->cm_id中
//            _nextConnection = std::make_shared<RdmaConnection>(*this, _ios, _props);
//            _nextConnection->_cm_id = cm_id;
//            LOG4CXX_DEBUG(logger, "start get next request...");

            // 现在变成异步了，不需要调用这个了，直接在rdma_get_cm_event里面去获取新的cm_id
//            ret = rdma_get_request(_cm_id, &(_nextConnection->cm_id)); // 这是个同步操作，会阻塞掉
//            if (ret) {
//                LOG4CXX_ERROR(logger, "rdma_get_request error");
//                return;
//            }

            // 在这个新的连接里面，去接收数据
//            _nextConnection->start();
//        }


/**
 * 创建channel，所有rdma conn共享一个channel
 */
int RdmaCommManager::create_event_channel(){
    int	ret = 0;

    // 这个锁和条件变量先屏蔽了
    /* initialize lock to protect asynchronous access to cm event fields */
    // ret = pthread_mutex_init(&ctx->cm_event_info_lock, NULL); // 初始化互斥锁，第二个是互斥锁属性，为空表示使用默认属性，为快速互斥锁
    // if (ret != 0) {
    //     LOG4CXX_ERROR(logger, "pthread_mutex_init cm_event_info_lock error");
    //     goto out0;
    // }

    /* initialize condition variable to wait for cm event field changes */
    // ret = pthread_cond_init(&ctx->cm_event_info_notify, NULL); // 初始化条件变量
    // if (ret != 0) {
    //     LOG4CXX_ERROR(logger, "pthread_cond_init cm_event_info_notify error");
    //     goto out1;
    // }

    /* initialize latest asynch cm event from cm to unlikely value */
    // ctx->latest_cm_event_type = RDMA_CM_EVENT_CONNECT_ERROR;
    // ctx->current_cm_event_type = RDMA_CM_EVENT_CONNECT_ERROR; // curr和latest区别

    /* 创建cm event channel，使用异步编程模式 */
    errno = 0; // 这个是系统定义的error no
    _cm_event_channel = rdma_create_event_channel();

    if (_cm_event_channel == nullptr) {
        ret = ENOMEM; // 内存不足 Out of memory
        LOG4CXX_ERROR(logger, "rdma_create_event_channel failed, errno is " << errno);
        return ret;
    }

    // channel创建完成，重新分配ch_fd，让asio监控这个输入流 待删除，不需要了
//    ch_fd.assign(_cm_event_channel->fd);
//    if(ch_fd == nullptr){
//        ch_fd = new boost::asio::posix::stream_descriptor(_ios, _cm_event_channel->fd);
//        LOG4CXX_DEBUG(logger, "ch_fd new ok. ch_fd is " << (void*)ch_fd);
//    }else{
//        LOG4CXX_ERROR(logger, "rdma_create_event_channel assign ch_fd, ch_fd is not nullptr, exist ch_fd id " << (void*)ch_fd);
//    }

    /* report the new communication channel created by us and its fd */
    LOG4CXX_DEBUG(logger, "rdma_create_event_channel: " << _cm_event_channel << ", assigned fd:" << _cm_event_channel->fd);

    return ret;
}


// 创建一个异步线程，监听所有channel event
/*
int RdmaCommManager::_create_cm_event_thread() {
    int	ret;
    struct r_cm_event_thread_params	*params = new r_cm_event_thread_params(); // 传递给线程的参数
    params->ctx = this; //
    params->cm_event_channel = _cm_event_channel; // rdma_cm_id中包含channel //  监听的cm_id绑定了channel
//    params->options = options;
    ret = sem_init(&params->sem, 0, 0); // 用于同步
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "create_cm_event_thread sem_init failed");
        goto out1;
    }

    // 创建新线程，处理cm events
    errno = 0;
    LOG4CXX_DEBUG(logger, "start a new thread : r_cm_event_thread");
    ret = pthread_create(&_cm_event_thread_id, NULL, _cm_event_thread, params);
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "cm event pthread_create failed");
        goto out2;
    }

    LOG4CXX_INFO(logger, "created cm_event_thread_id: " << _cm_event_thread_id);
    // 等待线程
    ret = sem_wait(&params->sem); // 子线程创建ok后会通知这里
    if (ret != 0){
        LOG4CXX_ERROR(logger, "cm thread sem_wait failed");
    }

    out2:
        if (sem_destroy(&params->sem) != 0){
            LOG4CXX_ERROR(logger, "sem_destroy failed");
        }
    out1:
        // free(params);
        delete params; // 换成delete了
    out0:
        return ret;
}*/

/**
 * 创建completion channel
 * @return 是否成功
 */
int RdmaCommManager::create_completion_channel()
{
    int	ret = 0;
    errno = 0;

    _completion_channel = ibv_create_comp_channel(verbs); // verbs是struct ibv_context*，即设备指针
    if (_completion_channel == nullptr) {
        ret = ENOMEM;
        LOG4CXX_ERROR(logger, "ibv_create_comp_channel failed, errno is " << errno);
    } else {
        LOG4CXX_INFO(logger, "ibv_create_comp_channel created completion channel:" << _completion_channel);
        LOG4CXX_INFO(logger, "ibv_create_comp_channel assigned fd is:" << _completion_channel->fd);

        // 激活流，实际上有问题，换一个方式了
//        cq_fd.assign(_completion_channel->fd);
//        if(cq_fd == nullptr){
//            cq_fd = new boost::asio::posix::stream_descriptor(_ios, _completion_channel->fd);
//            LOG4CXX_DEBUG(logger, "cq_fd new ok. cq_fd is " << (void*)cq_fd);
//        }else{
//            LOG4CXX_ERROR(logger, "ibv_create_comp_channel assign cq_fd, cq_fd is not nullptr, cq_fd is " << (void*)cq_fd);
//        }

        // 继续创建completion queue
        ret = create_cq();
    }
    return ret;
}

/**
 * 创建完成队列
 */
int RdmaCommManager::create_cq()
{
    int	ret = 0;
    LOG4CXX_INFO(logger, "device completion num_comp_vectors is:" << verbs->num_comp_vectors);

    errno = 0;
    // send queue和recv queue共用cq，所以要乘以2，然后2个节点共用cq，再乘以2， context设置为this
    // 最后的参数是num_comp_vectors，需要>=0同时<verbs->num_comp_vectors, 模拟的返回的是128
    // 一个conn设置一个cq会不会好一点，这样wc里面会有conn的this指针了
    _completion_queue = ibv_create_cq(verbs, SQ_DEPTH * 4, this, _completion_channel, 0);
    if (_completion_queue == nullptr) {
        /* when ibv_create_cq() returns NULL, it should set errno */
        ret = ENOMEM;
        LOG4CXX_ERROR(logger, "ibv_create_cq failed, errno is " << errno);
        return ret;
    }
    LOG4CXX_DEBUG(logger, "RdmaCommManager _completion_queue is " << _completion_queue);

    // 最后设置的cqe不一定是自己的cqe，要看最后的cq属性，ping-sr-3中申请的是32，返回的是63
    LOG4CXX_INFO(logger, "ibv_create_cq returned cqe is:" << _completion_queue->cqe);

    // 设置一个足够大的wr数组，即使一次性将所有cq中的cqe都取出，任然有空余
    max_n_work_completions = _completion_queue->cqe + 1; // 模拟中是64
    LOG4CXX_INFO(logger, "ibv_create_cq set max_n_work_completions is:" << max_n_work_completions);

    _work_completion = (struct ibv_wc*)malloc(max_n_work_completions * sizeof(struct ibv_wc));
    if (_work_completion == nullptr) {
        ret = ENOMEM;
        return ret;
    }

    // 这个之前是用来统计每次poll出来的个数是多少，是一个数组，这里暂时用不到
//    completion_stats = (unsigned long	*)our_calloc((conn->max_n_work_completions + 1) * sizeof(unsigned long), "completion_stats");
//    if (conn->completion_stats == NULL) {
//        ret = ENOMEM;
//        goto out2;
//    }

    // 初始化，目前没有wc要处理
    current_n_work_completions = 0;
    index_work_completions = 0;

    // 激活针对该cq的事件通知机制
    // Request notification before any completion can be created (to prevent races)
    errno = 0;
    ret = ibv_req_notify_cq(_completion_queue, 0);
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "ibv_req_notify_cq failed, errno is" << errno);
        return ret;
    }

    return ret;
}

/*
 * 异步线程，等待cm events, 之后中继转回主线程
 */
/*
void* _cm_event_thread(void *param) {
    struct r_cm_event_thread_params *params;
    struct rdma_event_channel *cm_event_channel;
//    struct our_options		*options;
    struct rdma_cm_event *cm_event;
    struct rdma_cm_id *cm_id;
    // struct r_control *ctx;
    RdmaCommManager::Impl* mgr; // 新的ctx是RdmaCommManager::Impl，不是RdmaCommManager
    enum rdma_cm_event_type this_event_type;
    int this_event_status;
    int ret, disconnected, give_signal;

    // 获取传给线程的参数
    params = param;
    mgr = params->conn; // 控制信息
    cm_event_channel = params->cm_event_channel; // channel
//        options = params->options;
    LOG4CXX_DEBUG(logger, "r_cm_event_thread is started, ctx is:" << (void*)ctx ", cm_event_channel is:" << (void*)cm_event_channel);

    *//* signal main-line that we are alive and kicking *//*
    // 给主线程发送信号，set_post会给sem + 1
    LOG4CXX_DEBUG(logger, "set_post to main thread");
    if (sem_post(&params->sem) != 0) {
        perror("our_cm_event_thread sem_post");
        LOG4CXX_ERROR(logger, "cm_event_thread sem_post to notify main thread.");
        return (void *) (-1);
    }

    do { // dz 这是一个死循环 就会一直在这里阻塞获取event然后处理event
        // 等待从cm_event_chanel获取下一个event
        errno = 0;
        ret = rdma_get_cm_event(cm_event_channel, &cm_event); // 这个会阻塞 注意：这个线程一直监听的只是这个channel，不会监听其他的channel！！！
        if (ret != 0) {
            LOG4CXX_ERROR(logger, "cm_event_thread rdma_get_cm_event fatal error, ret=" << ret);
//                exit(EXIT_FAILURE);
            return (void*)(-1);
        }

        // synch with main-line thread on who calls rdma_disconnect()
        // 上锁，和主线程同步， 主线程会调用 rdma_disconnetc()
        pthread_mutex_lock(&ctx->cm_event_info_lock);

        cm_id = cm_event->id; // 从获取的event中提取对应rdma_cm_id, 如果type是RDMA_CM_EVENT_CONNECT_REQUEST,那么这个是新生成的一个id
        this_event_type = cm_event->event;

        disconnected = ctx->disconnected;
        if (this_event_type == RDMA_CM_EVENT_DISCONNECTED && disconnected == 0) {
            *//* main-line did not make call, we must do it *//*
            // 主线程没有调用 disconnet, 我们必须调用
            ctx->disconnected = 1;
        }

        this_event_status = cm_event->status;

        if (ctx->latest_private_data_len > 0) {
            //
            if (ctx->latest_private_data_len >= sizeof(struct r_connect_info)) {
                memcpy(&ctx->latest_private_data, cm_event->param.conn.private_data, sizeof(struct r_connect_info));
            } else {
                LOG4CXX_ERROR(logger, "cm_event_thread private_data_len is too small, len=" << ctx->latest_private_data_len);
                ret = EPROTO;    *//* Protocol error *//*
            }
        }


        LOG4CXX_INFO(logger, "cm_event_thread got cm event: " << this_event_type << ", event_str: " << rdma_event_str(this_event_type)
                                                              << ", ctx: " << (void*)ctx << ", disconnected status: " << disconnected);

        // 校验：cm_id中绑定的自定义context 应该是 ctx
        if (ctx != cm_id->context) {
            LOG4CXX_ERROR(logger, "cm_event_thread error: cm_id->context is not equal control ctx. cm_id->context is " << (void*)cm_id->context);
        }

        // 如果是断开连接的消息
        if (this_event_type == RDMA_CM_EVENT_DISCONNECTED) {
            if (disconnected == 0) {
                LOG4CXX_DEBUG(logger, "cm_event_thread disconnecting cm_id:" << (void*)cm_id);
                errno = 0;
                ret = rdma_disconnect(cm_id);
                if (ret == 0) {
                    LOG4CXX_DEBUG(logger, "cm_event_thread rdma_disconnect ok, cm_id:" << (void*)cm_id);
                } else if (errno != EINVAL) {
                    LOG4CXX_DEBUG(logger, "cm_event_thread rdma_disconnect error, errno:" << errno);
                }
            } // else already flushed by remote
            // break; // 不应该break，和之前的demo不一样，这个channel中要处理很多的链接，所以这个循环要一直进行下去，直到系统关闭的时候主线程通知
        }
    } while (1); // 之后这里要添加上条件，方便退出线程

    LOG4CXX_INFO(logger, "Exited cm_event_thread_id, thread id is:" << ctx->cm_event_thread_id);
    return NULL;
}*/


/**
 * rdmaconn链接建立之后conn调用的
 * 等待cq对应的channel中出现事件
*/
/*
int RdmaCommManager::wait_for_completion()
{
    int	ret;
    struct ibv_wc* current_work_completion;

    // 轮询处理事件，先判断上次poll出来的是否全部处理完了，处理完了之后再开始等待下一个通知
    do	{
        // 判断是否处理完了
        if (index_work_completions < current_n_work_completions) {
            // 上一次返回的wc还没有处理完，先处理了，index要++
            current_work_completion = &_work_completion[index_work_completions++];
            break;
        }
        // 已经全部处理完成了，等待下一个event通知
        ret = wait_for_notification(conn, options); // 这里面会阻塞等待
        if (ret != 0) {
            // goto out0;
            return ret;
        }
        conn->notification_stats++;
        // now collect every completion in the queue
        errno = 0;
        ret = ibv_poll_cq(conn->completion_queue,
                          conn->max_n_work_completions, // 一次性poll了最多这么多回来，这个数值比创建 complete_queue的还要大一点 + 1
                          conn->work_completion);
        if (ret < 0) {
            LOG4CXX_ERROR(logger, "ibv_poll_cq error, ret is " << ret);
            return ret;
        }

        *//* keep statistics on the number of items returned *//*
        if (ret > conn->max_n_work_completions) {
            LOG4CXX_ERROR(logger, "ibv_poll_cq error, ret greater than conn->max_n_work_completions, ret is " << ret << ", max_n_work_completions is " << conn->max_n_work_completions);
        } else {
            conn->completion_stats[ret]++;
        }
        if (ret > 0) {
            // 将第一个要处理的wc放到work_completion中
            conn->current_n_work_completions = ret; // 放到这个参数里面，一个个处理，这个循环的开头就是处理这个的，如果没有处理完，直接break
            conn->index_work_completions = 1; // 当前处理到哪一个了
            *work_completion = &conn->work_completion[0]; // 所有的都放到这个里面了，当前处理第一个
        }
    } while (ret == 0);

    // 检测当前wc状态
    ret = check_completion_status(work_completion);
    return ret;
}*/

/**
 * 开始接受完成事件
 */
void RdmaCommManager::startCompletion()
{
//    cq_fd->async_read_some(boost::asio::buffer((void*)&one_byte_buffer_cq, sizeof(one_byte_buffer_cq)),
//                           std::bind(&RdmaCommManager::handle_completion,
//                                     this,
//                                     std::placeholders::_1,
//                                     std::placeholders::_2));

    // 现在这个只用调用一次，不用多次调用了
    _cq_channel = new muduo::net::Channel(g_loop, _completion_channel->fd);
//            _ch_channel->setReadCallback(std::bind(&RdmaCommManager::handleEvent, this, std::placeholders::_1));
    _cq_channel->setReadCallback(std::bind(&RdmaCommManager::handle_completion, this));
    _cq_channel->enableReading();
}



/**
 * completion queue收到通知的事件回调
 */

//void RdmaCommManager::handle_completion(const boost::system::error_code &error, std::size_t bytes_transferred){
void RdmaCommManager::handle_completion(){
    // 先从completion queue中取出所有的cqe, 取出的所有cqe放在_work_completion数组中
    int ret;
    struct ibv_cq* event_queue; // pointer to completion queue (CQ) associated with event
    void* event_context; // user supplied context set in ibv_create_cq  现在没用上，因为一共才一个cq
    errno = 0;

    // 之前这个函数是阻塞等待completion channel上面的事件，现在变成非阻塞了
    // 获取completion queue event。对于epoll水平触发模式，必须要执行ibv_get_cq_event并将该cqe取出，否则会不断重复唤醒epoll
    // muduo默认采用的epoll水平触发方式
    ret = ibv_get_cq_event(_completion_channel, &event_queue, &event_context);
    if (ret != 0) {
        if(errno == 11){
            // EAGAIN
            LOG4CXX_TRACE(logger, "Failed to get cq_event, ret is " << ret << ", errno is " << errno);
        }else{
            LOG4CXX_ERROR(logger, "Failed to get cq_event, ret is " << ret << ", errno is " << errno);
        }

        // 再次异步等待
        /*cq_fd->async_read_some(boost::asio::buffer((void*)&one_byte_buffer_cq, sizeof(one_byte_buffer_cq)),
                              std::bind(&RdmaCommManager::handle_completion,
                                        this,
                                        std::placeholders::_1,
                                        std::placeholders::_2));*/
//      startCompletion();
        return;
    }

    // Ack the event
    // ibv_ack_cq_events(event_queue, 1); // 这个里面会加锁，可以多个一起确认，性能会更好
    cq_events_that_need_ack++;
    if (cq_events_that_need_ack >= 10) { // 10个一起确认
        ibv_ack_cq_events(event_queue, cq_events_that_need_ack);
        cq_events_that_need_ack = 0; //  不需要加锁 变成原子操作 目前是单线程的
    }

    errno = 0;
    ret = ibv_req_notify_cq(_completion_queue, 0); // 一个cq获取了cq_event通知后，需要重新调用这个函数才能让他再发通知，否则是不会发通知的
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "ibv_req_notify_cq error, ret is " << ret);
        return;
    }

    // 校验event_queue,应该要是cm_channel对应的completion_queue
    if (event_queue != _completion_queue) {
        LOG4CXX_ERROR(logger, "ibv_get_cq_event got notify for completion queue, " <<
                    std::hex << (void*)event_queue << ", exected queue is " << (void*)_completion_queue << std::dec);
        return;
    }

    // 校验context  现在还没设置context
    /*if (event_context != conn) {
        LOG4CXX_ERROR(logger, "ibv_get_cq_event got notify for completion context, " <<
            std::hex << (void*)event_context << ", exected context is " << (void*)this << std::dec);
        return;
    }*/

    // 开始poll cq
    //  cq满了会有IBV_EVENT_CQ_ERR事件通知
    errno = 0;
    ret = ibv_poll_cq(_completion_queue,
                      max_n_work_completions, // 一次性poll了最多这么多回来，这个数值比创建 complete_queue的还要大一点 + 1
                      _work_completion);
    LOG4CXX_TRACE(logger, "ibv_poll_cq end, the num of poll wc is " << ret); // return 返回的是获取到的wc的数量

    if (ret == 0) {
        LOG4CXX_DEBUG(logger, "ibv_poll_cq ret is 0" << ", errno is " << errno); // 可能虚假唤醒
        // 再次异步等待
        /*cq_fd->async_read_some(boost::asio::buffer((void*)&one_byte_buffer_cq, sizeof(one_byte_buffer_cq)),
                              std::bind(&RdmaCommManager::handle_completion,
                                        this,
                                        std::placeholders::_1,
                                        std::placeholders::_2));*/
//      startCompletion();
        return;
    }

    if(ret < 0){
        LOG4CXX_ERROR(logger, "ibv_poll_cq ret is " << ret << ", errno is " << errno);
        return;
    }

    // poll出来了，则轮询
    if (ret > max_n_work_completions) {
        // 一次性poll出来了这么多，比数组最大值还多，应该不会发生，因为这个数组的长度，就是按照cq队列的长度+1来设置的
        LOG4CXX_ERROR(logger, "ibv_poll_cq error, ret greater than max_n_work_completions, ret is " << ret << ", max_n_work_completions is " << max_n_work_completions);
    } else {
        // 记录统计数据 TODO待处理
        // completion_stats[ret]++;
    }

    // 当前要处理的wc
    struct ibv_wc* current_work_completion;
    if (ret > 0) {
        current_n_work_completions = ret; // 放到这个参数里面，一个个处理，这个循环的开头就是处理这个的，如果没有处理完，直接break
        index_work_completions = 0; // 当前处理到哪一个了

        while(index_work_completions < current_n_work_completions)
        {
            current_work_completion = &_work_completion[index_work_completions]; // 将第一个要处理的wc放到work_completion中,index从0开始，正好对应数组下标
            index_work_completions++;

            // 检测wc状态
            ret = check_completion_status(current_work_completion); // 发现只要失败一次，后续的就都会端了，不会poll了
            if(ret != 0){
                LOG4CXX_ERROR(logger, "work_completion status error");
                continue;
            }

            // 数据正确收到了，要处理对应的消息了
            LOG4CXX_TRACE(logger, "wc status ok, begin process its content.");
            ret = process_work_completion(current_work_completion);
        };
    }

    // 再次异步等待
    /*cq_fd->async_read_some(boost::asio::buffer((void*)&one_byte_buffer_cq, sizeof(one_byte_buffer_cq)),
                          std::bind(&RdmaCommManager::handle_completion,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2));*/
//                startCompletion(); // 不需要了，一次就可以了
}

/**
 * 检测wc状态
 */
int RdmaCommManager::check_completion_status(struct ibv_wc* wc)
{
    enum ibv_wc_status status =  wc->status;
    if (status != IBV_WC_SUCCESS) {
//        LOG4CXX_DEBUG(logger, "IBV_WC_SEND is " << IBV_WC_SEND << ", IBV_WC_RECV is " << IBV_WC_RECV); // IBV_WC_SEND is 0, IBV_WC_RECV is 128
        LOG4CXX_ERROR(logger, "opcode is " << wc->opcode << ", wc_type: " << (wc->opcode == IBV_WC_SEND ? "IBV_WC_SEND" : (wc->opcode == IBV_WC_RECV ? "IBV_WC_RECV" : to_string(wc->opcode))));
        LOG4CXX_ERROR(logger, "wc_status_str: " << ibv_wc_status_str(status));
        LOG4CXX_ERROR(logger, "wc wr_id: " << std::hex << wc->wr_id << std::dec <<
                                ", qp_num is " << wc->qp_num << ", src_qp is " << wc->src_qp);

        if (status == IBV_WC_WR_FLUSH_ERR) {
            // dz：通常是因为前一个操作出现了错误，接下来的一系列操作都会出现该错误，poll出来64个wc，这64个都是这个错误
//			pthread_mutex_lock(&cm_event_info_lock); //  是否需要加锁 conn的disconnected是否涉及多线程访问，目前不需要
            /*if (disconnected == 0) {
                // 远端flush了，本端不用调用rdma_disconnect()
                disconnected = 3;
            }*/
//			pthread_mutex_unlock(&cm_event_info_lock);
            LOG4CXX_ERROR(logger, "ibv_poll_cq completion status flushed");
        }
        return -1;
    }else{
        LOG4CXX_TRACE(logger, "wc_status_str:" << ibv_wc_status_str(status));
        return 0;
    }
}

/**
 * 处理wc
 * @param work_completion
 * @return
 */
int RdmaCommManager::process_work_completion(struct ibv_wc* work_completion)
{
    int	ret = 0;
//    RdmaConnection* conn = reinterpret_cast<RdmaConnection*>(param);
    void* ptr = nullptr;
    RdmaConnection* conn = nullptr;
    struct ibv_recv_wr* wr = nullptr;

    switch (work_completion->opcode) {
        case IBV_WC_RECV:
//            conn->wc_recv++;

            // 校验收到的数据长度，是否符合预期, rdma中不知道一开始要收到多少数据，跳过这个
            // if (work_completion->byte_len != options->data_size) {
            //     LOG4CXX_ERROR(logger, "received “<< work_completion->byte_len <<  ” bytes, expected is " << options->data_size);
            // }

            /* repost our receive to catch the client's next send */
//            ret = post_recv(conn, &conn->user_data_recv_work_request[0]);
//            if (ret != 0) {
//                return ret;
//            }

            // 分发消息，数据已经存在之前的接收buffer中了
            uint64_t wr_id; // 注意是64位的，不是int32位的
            uint32_t len;
            unsigned int imm_data;
            unsigned int local_qp;
            unsigned int remote_qp;

            wr_id = work_completion->wr_id;
            len = work_completion->byte_len; // 数据长度
            imm_data = work_completion->imm_data; // 网络字节序
            local_qp = work_completion->qp_num;
            remote_qp = work_completion->src_qp; // 远端qp，文档写的是 remote qp number

            LOG4CXX_TRACE(logger, "recv message from remote. wr_id is " << std::hex << wr_id << std::dec << " ,data len is "
                                    << len << " ,local qp is " << local_qp << ", remote qp is " << remote_qp);
            // 取mr0的数据来看看

            // 2022年9月29日 17:47:04 突然想到一个好方法，wr_id 不再随机生成了，而是直接使用wr request的地址，同时建立一个全局map，映射这个地址
            // 和对应的 rdma conn ，这样在处理 complete的时候，wr_id就是wr的地址，通过map可以获取对应的conn的shareptr，然后处理完数据之后，这个request就又可以重用了，post next recv

//            if(gmap_wr.count(wr_id) != 0){
            if(gmap_conn.count(wr_id) != 0){
                // 找到保存的rdma conn了
//                wr = gmap_wr[wr_id];
                // 保存的wr_id直接就是之前work_request指针地址
                wr = reinterpret_cast<struct ibv_recv_wr*>(wr_id);
                LOG4CXX_TRACE(logger, "recv, wr_id to wr_recv_request in map is " << wr);

                // 获取对应的数据
                struct ibv_sge* sge = wr->sg_list;
//                char* addr = (char *)(sge->addr);
//                void* addr = reinterpret_cast<void*>(sge->addr);
                char* addr = reinterpret_cast<char*>(sge->addr);
                LOG4CXX_TRACE(logger, "recv the sge addr is " << std::hex << (void*)addr << std::dec);
                /* set the number of bytes in that memory area */
                uint32_t length = sge->length;
//                LOG4CXX_DEBUG(logger, "the addr content is " << addr);

                // 从addr处取出header信息 之前字节流，但是现在消息是分开几个发的，并不在一起，如何处理？ 解决办法：发送前就放到一起
                MessageHeader* header = reinterpret_cast<MessageHeader*>(addr);

                uint16_t messageType = header->getMessageType();
                uint64_t record_size = header->getRecordSize();
                InstanceID sourceInstanceID = header->getSourceInstanceID();

//                LOG4CXX_DEBUG(logger, "MessageHeader str() is " << header->str());
                // scidb_msg::NET_PROTOCOL_CURRENT_VER

                if(!isValidPhysicalInstance(sourceInstanceID)){
                    // 不应该收到对方 client id 发送的 rdma消息，client instance id应该用tcp发送，不用rdma
                    LOG4CXX_DEBUG(logger, "rdma process wc error, remote src id is invalid: " << Iid(sourceInstanceID));
                    return -1;
                }

                // 分发消息前，要先将消息转换成MessageDesc的shared_ptr才行
                std::shared_ptr<MessageDesc> _messageDesc = std::make_shared<Connection::ServerMessageDesc>();

                // 崩溃了，发现_netProtocolVersion字段的值变成了_recordSize的值，也就是少了64bit，8个字节，应该是少了一个虚函数表的位置！！！
                memcpy(reinterpret_cast<char*>(_messageDesc.get()) + 8, addr, sizeof(MessageHeader)); // +8字节跳过虚函数表复制数据！！！

                // 试试是否复制成功了，跳过虚函数表之后就OK了，复制成功了，但是这里多了一次复制的过程
//                LOG4CXX_DEBUG(logger, "test cpy message header str() is " << _messageDesc->getMessageHeader().str()); //sizeof(MessageHeader)=56

                // 需要我们准备 boost::asio::streambuf _recordStream; 这是一个流，后面会从这个流里面反序列化出来数据
                //  _messageDesc->_recordStream.prepare(record_size); // 准备空间

                // parseRecord代码如下：现在我们要自己写了，因为之前是从流转换，现在要从字符数组转换了
//                _recordStream.commit(bufferSize);
//                istream inStream(&_recordStream);
//                bool rc = _record->ParseFromIstream(&inStream); // 改成 ParseFromArray

                // MessageDesc中将本类设置成conn的friend class了，可以直接访问private成员_record了
                // 校验msg type，如果有问题，报错
                if(!_messageDesc->validate()){
                    LOG4CXX_ERROR(logger, "rdma validate msg type error, type is " << header->getMessageType());
                    return -1; // 后续 注册的空间应该要重用才可以
                }
                _messageDesc->_record = _messageDesc->createRecord(static_cast<MessageID>(header->getMessageType()));

                // protobuf c反序列化api bool ParseFromArray(const void* data, int size);
                bool rc = _messageDesc->_record->ParseFromArray(addr + sizeof(MessageHeader), record_size); // 直接从array中反序列化，不用以前的流api了

                if(!rc){
                    LOG4CXX_ERROR(logger, "ParseFromArray failed"); // dz
                    return -1;
                }

                if(!_messageDesc->_record->IsInitialized()){
                    // 没有初始化，也是失败
                    LOG4CXX_ERROR(logger, "IsInitialized is false, failed");
                    return -1;
                }

//                LOG4CXX_DEBUG(logger, "protobuf parse array ok");

                // test是否解析成功? new: 对比了发送端和接收端的，是解析成功的
//                if(strMsgType(header->getMessageType())  == "mtNotify"){
//                    LOG4CXX_DEBUG(logger, "test mtNotify record");
//                    std::shared_ptr<scidb_msg::Liveness> livemsg = dynamic_pointer_cast<scidb_msg::Liveness>(_messageDesc->_record);
//                    LOG4CXX_DEBUG(logger, "cluster_uuid is " << livemsg->cluster_uuid() << ", membership_id is "
//                        << livemsg->membership_id() << ", version is " << livemsg->version());
//                }

                // 原来的conn中的解析
//                if (!_messageDesc->parseRecord(bytes_transferr)) { // dz 这个里面会调用protobuf的反序列化
//                    LOG4CXX_ERROR(logger,
//                                  "Network error in handleReadRecordPart: cannot parse record for "
//                                          << " msgID="
//                                          << _messageDesc->getMessageHeader().getMessageType()
//                                          << ", closing connection");
//
//                    handleReadError(bae::make_error_code(bae::eof)); //  原来里面的解析失败的错误处理
//                    return -1;
//                }

                // dz 准备binary数据buffer，会根据binary size来准备, 会申请arena：malloc内存，析构的时候free，但是我们这里数据已经OK了，不应该alloc了，直接使用这个数据
//                _messageDesc->prepareBinaryBuffer(); // 不用这个了，这个里面会申请分配内存，但是我们不需要，因为数据已经传过来了

                uint64_t binary_size = header->getBinarySize();
                if(binary_size > 0){
                    // mtRemoteChunk 这种消息类型有binary buffer, 测试可以看这种类型消息
                    // copy二进制数据  先不copy了，直接使用这个二进制数据
                    _messageDesc->_binary = std::shared_ptr<SharedBuffer>(new scidb::CompressedBuffer());

                    // 算了，还是先分配一下buffer，把数据copy进去看还有没有问题吧
                    _messageDesc->_binary->allocate(binary_size);

                    memcpy(_messageDesc->_binary->getWriteData(), addr + sizeof(MessageHeader) + record_size, binary_size);

                    // 下面的这种先不用了
//                    dynamic_pointer_cast<scidb::CompressedBuffer>(_messageDesc->_binary)->setData(addr + sizeof(MessageHeader) + record_size); // dz 在compress buffer中新增的成员函数
//                    dynamic_pointer_cast<scidb::CompressedBuffer>(_messageDesc->_binary)->setCompressedSize(binary_size);
                }

//                msg->initRecord(mtNotify);
//                std::shared_ptr<scidb_msg::Liveness> record = msg->getRecord<scidb_msg::Liveness>();
//                bool res = serializeLiveness(query->getCoordinatorLiveness(), record.get());
//                SCIDB_ASSERT(res);
//                msg->setQueryID(query->getQueryID());
//                NetworkManager::getInstance()->broadcastLogical(msg);

                // 应该要调用消息分发和处理的东西了
                NetworkManager* mgr = NetworkManager::getInstance(); // 直接从单例模式中获取NetworkManager, 经测试获取ok
                if(mgr == nullptr){
                    LOG4CXX_ERROR(logger, "mgr is nullptr");
                    return -1;
                }

                // 数据初始化完成之后，交给network manager处理
//                mgr->dispatchMessage(const std::shared_ptr<Connection>& connection, const std::shared_ptr<MessageDesc>& messageDesc);
                // 从 conn的map中能获取到conn吗？ new: 能取到，但是不能取outconn，outconn相当于客户端，没有session，会被拦截，只有inconn才有对应的session
//                std::shared_ptr<scidb::Connection> conn = (mgr->_outConnections)[sourceInstanceID];

                // map下标访问会插入元素！！不想插入的话，需要使用find找迭代器
//                std::shared_ptr<scidb::Connection> scidb_conn = (mgr->_inSharedConns)[sourceInstanceID];

                std::shared_ptr<scidb::Connection> scidb_conn;
                auto iter = mgr->_inSharedConns.find(sourceInstanceID);
                if(iter != mgr->_inSharedConns.end()){
                    // 已经插入了通过auth的in conn了
                    scidb_conn = iter->second;
                    LOG4CXX_DEBUG(logger, "rdma dis msg to netmgr: " << mgr << ", conn: " << scidb_conn << ", from: "
                        << Iid(sourceInstanceID) << ", msgtype is: " << strMsgType(_messageDesc->getMessageType()));
//                    LOG4CXX_DEBUG(logger, "rdma handle msg, decode msg is " << _messageDesc->str()); // 包含header的和record的解析成功了
                    mgr->handleMessage(scidb_conn, _messageDesc);
                }else{
                    // 还没有通过auth的conn，有问题，即使交给mgr处理，也会到需要auth的步骤
                    // 这个消息会被直接丢掉，看看是什么消息
                    LOG4CXX_ERROR(logger, "has no session conn, msg is " << _messageDesc->str());
                }

                // 数据处理完成之后，要重新利用这个work_request, 即post next recv 重用这个buffer，上面已经处理完成了
                srptr p = gmap_conn[wr_id];
                p->post_recv(wr);

            }else{
                LOG4CXX_ERROR(logger, "can not find wr in gmap_wr, wr_id is:" << std::hex << wr_id << std::dec);
                return -1;
            }
            break;

        // send的完成，不用怎么处理，统计数据++就OK了 为了高性能，send的completion应该不发
        case IBV_WC_SEND:
//            conn->wc_send++;
            LOG4CXX_TRACE(logger, "case ibv_wc_send, send completed ok. wr_id is " << std::hex << work_completion->wr_id << std::dec);
            break;

        default:
            LOG4CXX_ERROR(logger, "ibv_poll_cq bad work completion opcode, opcode is " << work_completion->opcode);
            return -1;
    }	/* switch */

    return ret;
}

/**
 * 作为client主动建立到远端的连接，连接成功后将连接保存到map中，同时start这个连接，让它同时也可以recv
 * 注意，这个conn里面创建的 cm_id 不能取代监听的 _cm_id, 这个链接建立以后，可以收发，但是无法监听
 */
void RdmaCommManager::connect_remote(InstanceID src_id, InstanceID dest_id, const std::string& remoteIp) {
    LOG4CXX_DEBUG(logger, "connect_remote start, dest_id is " << Iid(dest_id) << ", remote ip is " << remoteIp);

    // 还不知道远端的instance id 处理好时序关系 new:fetch 之后应该获取到了
    if(dest_id == INVALID_INSTANCE){
        LOG4CXX_DEBUG(logger, "connect_remote return, because dest_id is INVALID_INSTANCE");
        return;
    }

    // 先判断是否已经有连接开始建立了，如果有的话，就不建立连接了
    {
        ScopedMutexLock scope(_outInitMutex, PTW_SML_NM);
        if(_routInitConnections.count(dest_id) != 0){
            // map中没有找到，但是set中找到了，说明有连接正在建立之中，那么这里直接返回
            LOG4CXX_DEBUG(logger, "connect_remote return, because other conn is building.");
        }else{
            // 没有找到，那么先占个坑
            _routInitConnections.insert(dest_id);
        }
    }
    // 现在我们是到remote id的第一个建立的连接了
    LOG4CXX_DEBUG(logger, "now we are the first to dest_id " << Iid(dest_id) << ", remote ip is " << remoteIp);
    std::shared_ptr<RdmaConnection> client_conn = std::make_shared<RdmaConnection>(*this);
    int ret = client_conn->connect(src_id, dest_id, remoteIp);

    if(ret == 0){
        // 连接建立成功 将建立的连接存入instance map中
        ScopedMutexLock scope(_outMutex, PTW_SML_NM);
//        LOG4CXX_DEBUG(logger, "dest Iid is valid, now we insert into the map");
        auto p = _routConnections.insert(std::make_pair(dest_id, client_conn));
        LOG4CXX_DEBUG(logger, "insert into the rdma conn outmap ok? " << (p.second == true ? "true" : "false") << ", out conn dest_id is "
                            << Iid(dest_id) << ", cm_id is " << client_conn->_cm_id);
    }else{
        // 连接失败
        LOG4CXX_ERROR(logger, "connect_remote failed, dest_id is " << dest_id << ", remote ip is " << remoteIp);
    }

    // 不管连接建立成功还是失败，都删除initset中的东西
    {
        ScopedMutexLock scope(_outInitMutex, PTW_SML_NM);
        _routInitConnections.erase(dest_id);
    }
}

//} // namespace rdma 去掉这个命名空间
}  // namespace scidb
