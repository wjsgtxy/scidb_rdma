// SciDB modules
#include <system/Utils.h>  // For SCIDB_ASSERT
#include <ccm/Uuid.h>

// third-party libraries
#include <log4cxx/logger.h>

// c++ standard libraries
#include <iomanip>
#include <unordered_map>
#include <string>
//#include <string.h>

#include <thread>

// header files from the rdma module
#include "RdmaConnection.h"
#include "RdmaProperties.h"
#include "RdmaCommManager.h"
#include <ctime> // for random

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.rdma.RdmaConnection"));
}

namespace asio = boost::asio;
namespace gpb = google::protobuf;

namespace scidb {
//    namespace rdma {
    // 随机数引擎
//    static std::independent_bits_engine<std::default_random_engine, 64, unsigned long int> engine;

RdmaConnection::RdmaConnection( RdmaCommManager& rdmaCommManager // dz 这里用的是引用
//                                ,asio::io_service& ioService
//                                ,const RdmaProperties& props
                                )
        : _mgr(rdmaCommManager)
//        , _socket(ioService)
//        , _props(props)
//        , _sslStream(ioService, context)
//        , _readTimer(ioService)
//        , _sessionCapsule(cap)
//        , _activeSession()
        , _stopped(false) // 默认是非关闭的
        , _cm_event_channel(_mgr._cm_event_channel) // 每个连接都使用同一个channel
        , _completion_channel(_mgr._completion_channel) // 所有conn全部使用同一个完成队列
        , _completion_queue(_mgr._completion_queue)
        , _selfInstanceID(INVALID_INSTANCE)
        , _remoteInstanceId(INVALID_INSTANCE)
        , _remoteIp("")
        , _remotePort(0)
        , _protection_domain(nullptr)
{
    // set the duration, but do NOT schedule it with async_wait.
//    _reimer.expires_at(boost::posix_time::pos_infin);
    // _cm_id在上层已经设置了
    LOG4CXX_TRACE(logger, "Creating RdmaConnection");

    // 初始化随机数引擎
//    struct timeval tv;
//    gettimeofday(&tv, NULL);
//    int64_t seconds = tv.tv_sec;
    std::random_device rd;
//    engine.seed(seconds); // 用当前时间做种子
    engine.seed(rd());
}

RdmaConnection::~RdmaConnection() noexcept
{
    LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection start, cm_id is " << _cm_id);
    int ret = 0;
    _stop();
    RdmaCommManager* mgr = RdmaCommManager::getInstance();
    if(_is_client){
        // client销毁out conn只用销毁map中的，因为set中的在建立成功的时候就已经删除了
        {
            ScopedMutexLock scope(mgr->_outMutex, PTW_SML_NM);
            RConnectionMap& mp = mgr->_routConnections;
            auto iter = mp.find(_remoteInstanceId);
            if(iter != mp.end()){
                if(_cm_id == (*iter).second->_cm_id){
                    mp.erase(iter);
                    LOG4CXX_DEBUG(logger, "Deconstructing client RdmaConnection erase _remoteInstanceId is " << _remoteInstanceId);
                }else{
                    LOG4CXX_DEBUG(logger, "Deconstructing client RdmaConnection exist entry conn cm_id not equal, just free.");
                    LOG4CXX_DEBUG(logger, "existed client entry conn cm_id is " << (*iter).second->_cm_id << ", destory conn cm_id is " << _cm_id);
                }
            }
        }
    }else{
        // server
        // 先从map中删除entry，对map的删除要加锁
        {
            ScopedMutexLock scope(mgr->_inMutex, PTW_SML_NM);
            RConnectionMap& mp = mgr->_rinConnections;
            auto iter = mp.find(_remoteInstanceId);
            if(_remoteInstanceId != INVALID_INSTANCE){
                if(iter != mp.end()){
                    // mp中存在指向该 remote节点的rdma连接, 同时保存的连接的cm_id 需要等于 这个conn 的 cm_id
                    if(_cm_id == (*iter).second->_cm_id){
                        mp.erase(iter);
                        LOG4CXX_DEBUG(logger, "Deconstructing server RdmaConnection erase _remoteInstanceId is " << _remoteInstanceId);
                    }else{
                        LOG4CXX_DEBUG(logger, "Deconstructing server RdmaConnection exist entry conn cm_id not equal, just free.");
                        LOG4CXX_DEBUG(logger, "existed server entry conn cm_id is " << (*iter).second->_cm_id << ", destory conn cm_id is " << _cm_id);
                    }
                }else{
                    LOG4CXX_ERROR(logger, "Deconstructing server RdmaConnection, map does not contain this remoteid " << _remoteInstanceId);
                }
            }else{
                LOG4CXX_ERROR(logger, "Deconstructing server RdmaConnection _remoteInstanceId is invalid, remoteid is " << _remoteInstanceId);
            }
        };
//        // 再删除initset 待删除，不需要在这里erase
//        {
//            ScopedMutexLock scope(mgr->_inInitMutex, PTW_SML_NM);
//            mgr->_rinInitConnections.erase(_remoteInstanceId);
//        }
    }

    // 断开连接, 同时要从mp中删除对应的项目
    rdma_disconnect(_cm_id);

    // 释放注册的内存
//    if ((send_flags & IBV_SEND_INLINE) == 0){
//        rdma_dereg_mr(send_mr);
//    }
//    rdma_dereg_mr(mr); // 这里coredump了

    unsetup_buffers(); // 现在注册了很多mr，用这个来释放，上面的不要了

    // 销毁queue pair
    rdma_destroy_qp(_cm_id); // 必须要在销毁cm_id前销毁对应的qp

    // 销毁pd
    errno = 0;
    if(_protection_domain != nullptr){
        ret = ibv_dealloc_pd(_protection_domain);
        if(ret){
            LOG4CXX_ERROR(logger, "ibv_dealloc_pd failed"); // 失败的原因可能是这个pd上面还有其他的东西关联到这个pd(包括qp,mr等)
        }
    }

    // 销毁cm_id
    if(_is_client){
        LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection client cm_id");
        rdma_destroy_ep(_cm_id);
    }else{
        LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection server cm_id");
        rdma_destroy_id(_cm_id);
    }

    if(_res != nullptr){
        LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection start _res: " << _res);
        rdma_freeaddrinfo(_res); // client端才会有这个地址解析
        LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection end _res: " << _res);
    }
    LOG4CXX_DEBUG(logger, "Deconstructing RdmaConnection end");
}


// 断开连接
void RdmaConnection::disconnect()
{
    LOG4CXX_DEBUG(logger, "Disconnecting from " << getPeerId());
//    _socket.close();
//    _connectionState = DISCONNECTED;
//    _query.reset();
//    _remoteIp = boost::asio::ip::address();
//    _remotePort = 0;
//    ClientQueries clientQueries;
//    {
//        ScopedMutexLock mutexLock(_mutex, PTW_SML_CON);
//        clientQueries.swap(_activeClientQueries);
//    }
//
//    LOG4CXX_TRACE(logger, str(boost::format("Number of active client queries %lld") % clientQueries.size()));
//
//    for (ClientQueries::const_iterator i = clientQueries.begin();
//         i != clientQueries.end(); ++i)
//    {
//        assert(_remoteInstanceId == CLIENT_INSTANCE);
//        QueryID queryID = i->first;
//        const ClientContext::DisconnectHandler& dh = i->second;
//        _networkManager.handleClientDisconnect(queryID, dh);
//    }
//
//    if (_session && _session->hasCleanup()) {
//        _networkManager.handleSessionClose(_session);
//    }

}

/* set up parameters to define properties of the new queue pair */
/**
 * 设置queue pair属性
 * @param init_attr
 */
void RdmaConnection::setup_qp_params(struct ibv_qp_init_attr *init_attr)
{
    memset(init_attr, 0, sizeof(*init_attr));

    init_attr->qp_context = this;
    init_attr->send_cq = _completion_queue; // 发送和接受都用一个cp
    init_attr->recv_cq = _completion_queue;
    init_attr->srq = nullptr;
    init_attr->cap.max_send_wr = SQ_DEPTH;
    init_attr->cap.max_recv_wr = RQ_DEPTH;
    init_attr->cap.max_send_sge = MAX_SEND_SGE;
    init_attr->cap.max_recv_sge = MAX_RECV_SGE;
    init_attr->cap.max_inline_data = 0;
    init_attr->qp_type = IBV_QPT_RC; // RC可靠连接服务
    // https://www.rdmamojo.com/2014/06/30/working-unsignaled-completions/
    init_attr->sq_sig_all = 0; // 不通知，设置成0同时send wr不设置IBV_SEND_SIGNALED，则成功的send wr不会产生wc
}

int RdmaConnection::create_pd()
{
    int ret = 0;
    // 先创建pd
    errno = 0;
    if(_protection_domain != nullptr) return ret; // 已经存在了

    // 每一个queue pair 分配一个pd
//    _protection_domain = ibv_alloc_pd(_mgr.verbs); // 先不分配了，统一使用一个
    _protection_domain = _mgr._protection_domain;
    if (_protection_domain == nullptr) {
        ret = ENOMEM;
        LOG4CXX_ERROR(logger, "ibv_alloc_pd failed, errno is " << errno);
        return ret;
    } else {
        LOG4CXX_TRACE(logger, "ibv_alloc_pd success, pd is " << _protection_domain);
    }
    return ret;
}


/**
 * 创建queue pair
 * @return
 */
int RdmaConnection::create_qp()
{
    struct ibv_qp_init_attr	init_attr;
    int	ret = 0;

    // 先设置属性
    setup_qp_params(&init_attr);

    // 分配一个queue pair，会自动关联上传入的cm_id，之后就可以发送和接收数据了
    errno = 0;
    ret = rdma_create_qp(_cm_id, _protection_domain, &init_attr); // 使用listen id对应的pd
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "rdma_create_qp failed, errno is " << errno);
        return ret;
    } else {
        // 输出qp属性
        // szu_lab 是 111
        LOG4CXX_TRACE(logger, "rdma_create_qp, max_send_wr is " << init_attr.cap.max_send_wr);
        // 重点关注一下这个参数是多少！关系到我一次性post多少个recv wr szu_lab这个参数是64
        LOG4CXX_TRACE(logger, "rdma_create_qp, max_recv_wr is " << init_attr.cap.max_recv_wr);
        LOG4CXX_TRACE(logger, "rdma_create_qp, max_send_sge is " << init_attr.cap.max_send_sge); // szu_lab 6
        LOG4CXX_TRACE(logger, "rdma_create_qp, max_recv_sge is " << init_attr.cap.max_recv_sge); // szu_lab 4
        LOG4CXX_TRACE(logger, "rdma_create_qp, max_inline_data is " << init_attr.cap.max_inline_data); // szu_lab 88
    }

    return ret;
}

/**
 * 在这个新建立的链接里面，开始绑定channel，accept请求，然后post_recv等
 */
int RdmaConnection::start(enum Start_type type)
{
    int ret = 0;
    // 将这个cm_id 转移到一个channel中去，同步变异步
    // 通过event 获取的id，有event channel了，就是监听的cm_id 绑定的那个，函数里面会判断
    migrate_id(type);

    // 更改属性，绑定cq，这个函数是更改qp状态的，绑定不了cq，还是单独创建吧
    // int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr, enum ibv_qp_attr_mask attr_mask)

    // 创建pd
    ret = create_pd();
    if(ret){
        LOG4CXX_ERROR(logger, "create_pd failed, errno is" << errno);
        return -1;
    }

    // 创建qp，新的这个cmid还没有qp绑定上（因为client端用的rdma_creta_ep, 没有传qp对应的初始化属性，也就不会创建了，server端是event中获取的通信id，也没有qp）
    // 自己创建qp控制性更强
    ret = create_qp();
    if(ret){
        LOG4CXX_ERROR(logger, "create_qp failed, errno is" << errno);
        return -1;
    }

    // 查询qp属性
/*    struct ibv_qp_init_attr init_attr;
    struct ibv_qp_attr qp_attr; // 保存qp属性
    memset(&qp_attr, 0, sizeof qp_attr);
    memset(&init_attr, 0, sizeof init_attr);

    LOG4CXX_DEBUG(logger, "start get the attributes of a queue pair, use ibv_query_qp.");
    // ibv_query_qp: get the attributes of a queue pair (QP)
    ret = ibv_query_qp(_cm_id->qp, &qp_attr, IBV_QP_CAP, &init_attr); // 监听的时候传了qp属性，所以这里也会分配qp
    if (ret) {
        LOG4CXX_ERROR(logger, "ibv_query_qp error");
        return;
    }

    if (init_attr.cap.max_inline_data >= 16){
        send_flags = IBV_SEND_INLINE;
    }else{
        LOG4CXX_INFO(logger, "rdma_server: device doesn't support IBV_SEND_INLINE, using sge sends");
    }*/

    // 这里再次request notification，之前在创建的时候已经request一次了
    errno = 0;
    LOG4CXX_DEBUG(logger, "ibv_req_notify_cq again. _completion_queue is " << (void*)_completion_queue);
    ret = ibv_req_notify_cq(_completion_queue, 0);
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "ibv_req_notify_cq failed, errno is" << errno);
        return -1;
    }

    LOG4CXX_DEBUG(logger, "register agent memory, rdma_reg_msgs.");

    // 注册MR内存 struct ibv_mr* mr
/*    mr = rdma_reg_msgs(_cm_id, user_data, MAX_BUFFER_SIZE);
    if (!mr) {
        LOG4CXX_ERROR(logger, "rdma_reg_msgs for recv_msg error");
        return;
    }*/

/*    if ((send_flags & IBV_SEND_INLINE) == 0) {
        send_mr = rdma_reg_msgs(_cm_id, send_msg, 16);
        if (!send_mr) {
            LOG4CXX_ERROR(logger, "rdma_reg_msgs for recv_msg 2 error");
            return;
        }
    }*/

    // 下发recv, 要下发多个recv
/*    ret = rdma_post_recv(_cm_id, nullptr, user_data, MAX_BUFFER_SIZE, mr); // context可以穿一个参数过去，会在返回的wc中出现
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma_post_recv error");
        return;
    }
    LOG4CXX_DEBUG(logger, "rdma conn post a revc.");*/

    if(type == Start_type::client){
        LOG4CXX_DEBUG(logger, "setting client param.");

        // 连接的参数，可以传递 private data过去
        struct rdma_conn_param client_param;
        memset(&client_param, 0, sizeof(client_param)); // 清空

        // 不通过string传输参数了，直接传输uint64_t
//        std::string sid = dec2hex(_selfInstanceID); // uint64_t
//        LOG4CXX_DEBUG(logger, "string sid is " << sid);
//        const void *private_data = sid.c_str(); // id是 uint64_t，转成16进制有8个字节, 直接换成16进制的可以节省内存
//        uint8_t private_data_len = static_cast<uint8_t>(sid.size() + 1); // szu_lab
//        LOG4CXX_DEBUG(logger, "Src Instance id is " << _selfInstanceID << ", string id is " << sid << ", size is " << (int)private_data_len);

        // 设置client连接参数，将src instance id传过去
        uint64_t private_data = htonll(_selfInstanceID); // 需要转换字节序，instance id是uint64_t
        client_param.private_data = &private_data;
        client_param.private_data_len = 8; // 这个参数对方接收的可能会比我们设置的要大，文档里面是这么说的，实际也是这样的，对方经常接受到56B的数据
        // https://forums.developer.nvidia.com/t/rdma-read-failing-with-remote-invalid-request-error/207359/2
        // 好像会导致 remote invalid request error这个错误 new：好像不是，是因为传过去的数据太大，超过了之前设定的4096的buffer长度
        client_param.responder_resources = 2;
        client_param.initiator_depth = 2;
        client_param.retry_count = 5;
        client_param.rnr_retry_count = 5;

        LOG4CXX_DEBUG(logger, "Src Instance id is " << _selfInstanceID << ", uint64_t network order is " << private_data);

        // uint32_t qp_num; // 设置qp num 如果cm_id上面绑定有qp的话，就没有用处

        // client 主动连接对方，改成在这里rdma_connect,之前在scidb conn里面直接connect，可能是其他qp资源都没有创建，connect直接返回111错误
        // 百度了一下，这个错误表示远端拒绝连接了
        // rdma连接 现在创建了channel，应该变成异步的方法了
        errno = 0;
        ret = rdma_connect(_cm_id, &client_param);
        if (ret) {
            LOG4CXX_ERROR(logger, "rdma_connect error, ret is: " << ret << ", errno is " << errno);
            return -1;
        }
        LOG4CXX_DEBUG(logger, "rdma_connect remote success, Accepted RdmaConnection cm_id " << _cm_id);

    }else{
        // server端的这个才需要设置 recv work request
        ret = setup_recv_buffers_and_mr();
        if (ret) {
            LOG4CXX_ERROR(logger, "setup_recv_buffers_and_mr error");
            return -1;
        }

        // 一次性post多个recv request请求
        LOG4CXX_DEBUG(logger, "start post " << MAX_BUFFER_NUMS << " recv work request");
        int i = 0;
        for(; i < MAX_BUFFER_NUMS; i++){
            // wc产生了很多flush error，不知道是不是因为这里太大了，先手动改小一点试试 new：flush error应该是之前的错误了，qp进入错误状态了，导致后续的错误了
            // 会不会是因为后续没有继续post_recv 才导致没办法接收对方发送的东西！！！
            ret = post_recv(&recv_work_request[i]);
            if(ret != 0){
                LOG4CXX_ERROR(logger, "post_recv error");
                return -1;
            }
        }
        LOG4CXX_DEBUG(logger, "end post " << i << " recv work request");

        // 准备工作完成，accept这个cm_id
        ret = rdma_accept(_cm_id, nullptr); // accept也可以传递conn_param
        LOG4CXX_DEBUG(logger, "this conn accepted _cm_id: " << std::hex << _cm_id << std::dec);
        if (ret) {
            LOG4CXX_ERROR(logger, "rdma_accept error");
            return -1;
        }
        LOG4CXX_INFO(logger, "_cm_id: " << std::hex << _cm_id << std::dec << " accepted");
    }

    // 之后等待cq中的事件，处理事件 这部分在 mgr中处理了
    // 多个conn对象绑定这个 fd的异步操作，this指针都不同，到时候能回调到正确的this指针么？感觉不行！感觉还是要根据弹出的event中的context来回调不同对象的同一个处理 cqe的方法才可以
/*    _mgr.cq_fd.async_read_some(boost::asio::buffer((void*)&one_byte_buffer_cq, sizeof(one_byte_buffer_cq)),
                          std::bind(&RdmaConnection::handleConpletionQueue,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2));*/

    return 0;
}

/**
 * 处理cqe数据
 */
/*void RdmaConnection::handleConpletionQueue(){

}*/

/**
 * 将新生成的cm_id绑定到channel中，这样操作就变成异步的了
 */
void RdmaConnection::migrate_id(enum Start_type type) {
    int	ret;

    errno = 0;
    if(type == Start_type::server){
        // 服务端，现在已经使用rdma_create_id了，提前就绑定channel了
        if(_cm_id->channel != nullptr){
            LOG4CXX_DEBUG(logger, "server cm_id already has event channel, channel is " << (void*)_cm_id->channel << ", channel fd is " << _cm_id->channel->fd);
        }
    }else{
        // 注意，这个channel还是用来做事件通知的，不是completion queue的channel
        if(_cm_id->channel != nullptr){
            LOG4CXX_DEBUG(logger, "client cm_id already has event channel, channel is " << (void*)_cm_id->channel << ", channel fd is " << _cm_id->channel->fd);
        }
        ret = rdma_migrate_id(_cm_id, _cm_event_channel);
        if (ret != 0) {
            LOG4CXX_ERROR(logger, "rdma_migrate_id failed");
        }
        LOG4CXX_DEBUG(logger, "rdma_migrate_id success, client cm_id new channel is " << (void*)_cm_id->channel << ", channel fd is " << _cm_id->channel->fd);
    }

    // 绑定context到conn这个对象自身
    _cm_id->context = this;
}

void RdmaConnection::_stop() noexcept
{
    // Make certain to use the error_code not the throwing
    // forms of boost::asio operations. stop() MAY NOT throw.
    LOG4CXX_DEBUG(logger, "Stopping RdmaConnection");
    if (_stopped) {
        LOG4CXX_INFO(logger, "Connection stopped more than once.");
        return;
    }

//    boost::system::error_code ec;

    _stopped = true;
    // "...and soon the last shared pointer reference to this connection object will go away."
    // Unless, of course, it already has and this function is being called from the destructor.
    LOG4CXX_TRACE(logger, "RdmaConnection STOPPED");
}

/*void RdmaConnection::_handleReadTimeOut(const boost::system::error_code& ec)
{
    if (ec == asio::error::operation_aborted) {
        LOG4CXX_TRACE(logger, "_readTimer aborted/rescheduled for:  " << _activeSession->getId());
    } else {
        ASSERT_EXCEPTION(!ec, "Unexpected error code in _handleReadTimeOut:" << ec.message());
        LOG4CXX_TRACE(logger, "Input timer for reading expired: " << _activeSession->getId());
        sendError(CcmErrorCode::READ_TIMEOUT);
        _stop();
    }
}*/

/*void RdmaConnection::_startReadProcessing()
{
    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to read next message after connection is stopped.");
        return;
    }

    if (_messageStream.size() > 0) {
        // The client sent more then one request on the socket (without waiting for the
        // response to the last one).
        LOG4CXX_TRACE(logger, "Still have " << _messageStream.size() << " bytes in _messageStream");
        boost::system::error_code ec;
        _handleReadData(ec, _messageStream.size());
        return;
    }

    if (_props.getTLS()) {
        asio::async_read(_sslStream,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&RdmaConnection::_handleReadData,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    } else {
        asio::async_read(_socket,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&RdmaConnection::_handleReadData,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }
}*/
/*
void RdmaConnection::_handleTlsHandshake(const boost::system::error_code& ec)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempting to handle TLS handshake connection is stopped." << ec << ec.message());
        return;
    }
    if (ec) {
        // TLS/SSL Failed handshake
        LOG4CXX_WARN(logger, "TLS handshake failed " << ec << " -- " << ec.message());
        return;
    }
    _startReadProcessing();
}*/

/*
void RdmaConnection::_handleReadData(const boost::system::error_code& ec, size_t bytes_read)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempting to handle ReadData after connection is stopped: "
                              << ec << ec.message() << " bytes_read " << bytes_read);
        return;
    }

    if (ec) {
        // It's very likely that the client application exited and closed the socket, but
        // sendResponse (invoked by sendErrorMessage) checks that the socket is available
        LOG4CXX_INFO(logger,
                     "RdmaConnection::handleReadData boost condition: " << ec << ": " << ec.message());
        sendError(CcmErrorCode::GENERAL_ERROR);
        return;
    }

    LOG4CXX_TRACE(logger, "read " << bytes_read << "; _messageStream size " << _messageStream.size());

    // We have started reading a new message (at least one byte). Start the read timer so
    // that we get the full message or time out.
    if (_readTimer.expires_at().is_infinity()) {
        LOG4CXX_TRACE(logger, "Set Initial _read_timer");
        _readTimer.expires_from_now(boost::posix_time::seconds(_props.getReadTimeOut()));
        _readTimer.async_wait(
                std::bind(&RdmaConnection::_handleReadTimeOut, shared_from_this(), std::placeholders::_1));
    } else {
        // The entire message (header and protobuf) must be read before the _read_timer
        // expires.  So keep existing time-out, and prevent the degenerate bad-actor case
        // where the client sends one byte, waits 'slightly less than the timer', then
        // sends the next byte.
        LOG4CXX_TRACE(logger, "_read_timer is already running... So don't reset");
    }

    if (_messageStream.size() < sizeof(CcmMsgHeader)) {
        // We need more data to have enough to populate the _readHeader.
        if (_props.getTLS()) {
            asio::async_read(_sslStream,
                             _messageStream,
                             asio::transfer_at_least(1),
                             std::bind(&RdmaConnection::_handleReadData,
                                       shared_from_this(),
                                       std::placeholders::_1,
                                       std::placeholders::_2));

        } else {
            asio::async_read(_socket,
                             _messageStream,
                             asio::transfer_at_least(1),
                             std::bind(&RdmaConnection::_handleReadData,
                                       shared_from_this(),
                                       std::placeholders::_1,
                                       std::placeholders::_2));
        }
    } else {
        std::iostream is(&_messageStream);
        is.read(reinterpret_cast<char*>(&_readHeader), sizeof(CcmMsgHeader));

        LOG4CXX_TRACE(logger,
                      "Read from _messageStream into _readHeader; "
                              << " _messageStream size now " << _messageStream.size()
                              << "; HEADER is: " << _readHeader);

        if (_readHeader.isValid()) {
            // mjl wants this assert just in case the const & changed since we tested
            // it at the top of the function.
            SCIDB_ASSERT(!ec);
            // We have the header so go work on the record (protobuf) data.
            _handleRecordPortion(ec, _messageStream.size());
        } else {
            // The header has invalid values.
            //   Rather than try to figure out why the version/type are invalid just close the
            //   connection. If the session was authenticated, it still is. So reset the
            //   connection and let the client re-establish a new connection with a fresh
            //   stream of data.
            _activeSession.reset();
            LOG4CXX_TRACE(logger, "Invalid message header: " << _readHeader);
            sendError(CcmErrorCode::INVALID_MSG_HEADER);
            _stop();
        }
    }
}
*/

/*
void RdmaConnection::_handleRecordPortion(const boost::system::error_code& ec, size_t bytes_read)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger,
                      "Attempt to read protobuf after connection is stopped: "
                              << ec.message() << "; bytes_read: " << bytes_read);
        return;
    }

    auto streamSize = _messageStream.size();
    auto recordSize = _readHeader.getRecordSize();
    if (ec) {
        // It's very likely that the client application exited and closed the
        // socket, but sendResponse (called by sendError) checks that the socket
        // is available.
        LOG4CXX_INFO(logger, "handleRecordPortion boost error: " << ec << ": " << ec.message());
        sendError(CcmErrorCode::GENERAL_ERROR);
        return;
    }

    if (streamSize >= recordSize) {
        LOG4CXX_TRACE(logger, "bytes read: " << bytes_read << "; stream size:  " << streamSize);
        _processRequestMessage();
    } else {
        // We don't have enough data to create the record so read some more.
        SCIDB_ASSERT(!ec);
        LOG4CXX_TRACE(logger,
                      " Read more for protobuf. Needed: " << recordSize << "; Have: " << streamSize);
        asio::async_read(_socket,
                         _messageStream,
                         asio::transfer_at_least(1),
                         std::bind(&RdmaConnection::_handleRecordPortion,
                                   shared_from_this(),
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }
}*/

/*template <typename T>
bool RdmaConnection::_parseRecordPortion(T& msg)
{
    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to parse Record (protobuf) after connection is stopped .");
        return false;
    }
    auto recordSize = _readHeader.getRecordSize();

    SCIDB_ASSERT(_messageStream.size() >= recordSize);

    // We could just pass the _messageStream to
    // google::protobuf::Message.ParseFromIstream() but we don't because the
    // stream is doing read-ahead.  ParseFromIstream(...) consumes the entire
    // input stream.[In addition Message::ParseFromIstream() is a heavyweight
    // I/O operation which requires we use Message and are not able to use
    // MessageLite. (Using ParseFromIstream means we cannot link to
    // libprotobuf-lite.so)].  So use ParseFromString().
    asio::streambuf::const_buffers_type bufs = _messageStream.data();
    std::string str(asio::buffers_begin(bufs), asio::buffers_begin(bufs) + recordSize);
    _messageStream.consume(recordSize);
    bool rc = msg.ParseFromString(str);
    // clang-format off
    LOG4CXX_TRACE(logger,
                  "Protobuf parsing success = " << std::boolalpha << rc
                                                << "; msg.IsInitialized() = " << msg.IsInitialized() << std::noboolalpha
                                                << "; _messageStream.size() is now " << _messageStream.size());
    //clang-format on

    return rc && msg.IsInitialized();
}*/
/*
void RdmaConnection::_processRequestMessage()
{
    // We have finished a full read of a request message (the header and the
    // protocol buffer), so abort the _read_timer and set the duration to
    // indicate that the timer /would/ expire at infinity if it async_wait()
    // were called on the timer.  This is a sentinel value so that we can see
    // whether the timer is running or not in _handleReadData(). Do NOT
    // async_wait on the timer here.
    _readTimer.expires_at(boost::posix_time::pos_infin);


    if (_stopped) {
        LOG4CXX_TRACE(logger, "Attempting to process Request message after connection is stopped .");
        return;
    }
    SCIDB_ASSERT(_readHeader.isValid());
    _activeSession = _sessionCapsule.getSession(Uuid(_readHeader.getSession()));
    if (!_activeSession) {
        _activeSession = _sessionCapsule.createSession(_props.getSessionTimeOut());
    }


    *//*
     * We still have a switch case but this should be the ONE AND ONLY location of a
     * switch on msgtype.
     *//*
    switch (_readHeader.getType()) {
        case CcmMsgType::AuthLogonMsg: {
            LOG4CXX_TRACE(logger, "RdmaConnection: Processing AuthLogon");
            msg::AuthLogon protoMsg;
            if (_parseRecordPortion<msg::AuthLogon>(protoMsg)) {
                _activeSession->processAuthLogon(shared_from_this(), protoMsg);
            } else {
                // The protobuf was bad, who knows what remains in the incoming socket
                // We need to close the connection.
                // However, We do not explicitly remove the session.
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::AuthResponseMsg: {
            LOG4CXX_TRACE(logger, "RdmaConnection: Processing AuthResponse");
            msg::AuthResponse protoMsg;
            if (_parseRecordPortion<msg::AuthResponse>(protoMsg)) {
                _activeSession->processAuthResponse(shared_from_this(), protoMsg);
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::ExecuteQueryMsg: {
            LOG4CXX_TRACE(logger, "RdmaConnection: Processing ExecuteQuery");
            msg::ExecuteQuery protoMsg;
            if (_parseRecordPortion<msg::ExecuteQuery>(protoMsg)) {
                // Execute Query Can take a 'very long' time. so cancel the session timer
                _sessionCapsule.cancelTimer(_activeSession->getId());
                _activeSession->processExecuteQuery(shared_from_this(), protoMsg);
                if (_activeSession) {
                    // and re-enable it here. if we still have the session (which can be
                    // reset in the case of an invalid message for the current
                    // CcmSessionState)
                    _sessionCapsule.restartTimer(_activeSession->getId(),
                                                 _activeSession->getTimeOut());
                }
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        case CcmMsgType::FetchIngotMsg: {
            LOG4CXX_TRACE(logger, "RdmaConnection: Processing FetchIngot: ");
            msg::FetchIngot protoMsg;
            if (_parseRecordPortion<msg::FetchIngot>(protoMsg)) {
                // wait for the back-end to generate data in a long running query.
                _activeSession->processFetchIngot(shared_from_this(), protoMsg);
            } else {
                sendError(CcmErrorCode::MALFORMED_REQUEST);
                _stop();
            }
        } break;
        default: {
            LOG4CXX_WARN(logger,
                         "RdmaConnection: Invalid Message Type value: "
                                 << _readHeader.getType() << " sent as a  Client Request.");
            sendError(CcmErrorCode::INVALID_REQUEST);
            _stop();
        } break;
    }
}*/
/*
void RdmaConnection::removeSession()
{
    SCIDB_ASSERT(_activeSession);
    _sessionCapsule.removeSession(_activeSession->getId());
    _activeSession.reset();
}

std::shared_ptr<CcmSession> RdmaConnection::createSession(long timeOut)
{
    _activeSession = _sessionCapsule.createSession(timeOut);
    return _activeSession;
}

std::shared_ptr<CcmSession> RdmaConnection::createSession()
{
    return createSession(_props.getSessionTimeOut());
}*/

/*void RdmaConnection::sendResponse(CcmMsgType type, std::shared_ptr<gpb::Message> msg)
{
    std::string a;
    sendResponse(type, msg, a);
}*/

/*void RdmaConnection::sendResponse(CcmMsgType type, std::shared_ptr<gpb::Message> msg, const std::string & binary)
{
    if (_stopped) {
        LOG4CXX_WARN(logger, "Attempting to send response on stopped/closed socket.");
        return;
    }

    // If the socket is closed,  we cannot send anything, so don't try.
    if (!getSocket().is_open()) {
        return;
    }

    if (!_activeSession) {
        _activeSession = _sessionCapsule.invalidSession();
    }
    LOG4CXX_TRACE(logger, "SENDING (" << type << ") CcmSession: " << _activeSession->getId());
    CcmMsgHeader writeHeader;
    writeHeader.setType(type);
    asio::streambuf writeBuffer;
    std::ostream output(&writeBuffer);
    msg->SerializeToOstream(&output);
    writeHeader.setSession(_activeSession->getId().data());
    writeHeader.setRecordSize(static_cast<uint32_t>(writeBuffer.size()));

    std::vector<asio::const_buffer> buffers;
    buffers.push_back(asio::buffer(&writeHeader, sizeof(writeHeader)));
    buffers.push_back(asio::buffer(writeBuffer.data()));
    if (binary.size() > 0) {
        buffers.push_back(asio::buffer(binary));
    }
    if (_props.getTLS()) {
        asio::async_write(_sslStream,
                          buffers,
                          std::bind(&RdmaConnection::_postSendMessage,
                                    shared_from_this(),
                                    std::placeholders::_1,
                                    std::placeholders::_2));

    } else {
        asio::async_write(_socket,
                          buffers,
                          std::bind(&RdmaConnection::_postSendMessage,
                                    shared_from_this(),
                                    std::placeholders::_1,
                                    std::placeholders::_2));
    }
}*/

/*void RdmaConnection::sendError(CcmErrorCode code)
{
    auto msg = std::make_shared<msg::Error>();
    msg->set_code(static_cast<uint32_t>(code));
    msg->set_text(errorCodeToString(code));
    sendResponse(CcmMsgType::ErrorMsg, msg);
}*/

/**
 * register a memory addr of length bytes for appropriate access
 * returns != nullptr if all ok,
 *	   == nullptr on error (and error message has been given)
 */
struct ibv_mr* RdmaConnection::setup_mr(void *addr, unsigned int length, int access)
{
    struct ibv_mr* mr = nullptr;

    errno = 0;
    // (enum ibv_access_flags)
    mr = ibv_reg_mr(_protection_domain, addr, length, access); // 注册到申请的pd上面，这个pd也绑定了pq
    if (mr == nullptr) {
        LOG4CXX_ERROR(logger, "ibv_reg_mr failed, errno is " << errno);
    }
    return mr;
}


/**
 * unregister a memory area
 * @param mr
 * @param max_slots
 */
void RdmaConnection::unsetup_mr(struct ibv_mr **mr, int max_slots)
{
    struct ibv_mr  **ptr;
    int ret, i;

    ptr = mr;
    for (i = 0; i < max_slots; i++) {
        if (*ptr != nullptr) {
            errno = 0;
            ret = ibv_dereg_mr(*ptr);
            if (ret != 0) {
                LOG4CXX_ERROR(logger, "ibv_dereg_mr failed, errno is " << errno);
            }
            *ptr = nullptr;
        }
        ptr++;
    }
}


/**
 * fill in scatter-gather element for length bytes at registered addr
 * @param addr sge地址
 * @param length
 * @param lkey
 * @param sge
 */
void RdmaConnection::setup_sge(const void *addr, unsigned int length, unsigned int lkey, struct ibv_sge *sge)
{
    /* point at the memory area */
    sge->addr = (uint64_t)(unsigned long)addr;

    /* set the number of bytes in that memory area */
    sge->length = length;

    /* set the registration key for that memory area */
    sge->lkey = lkey;
}

/**
 * register a memory addr of length bytes for appropriate access and fill in scatter-gather element for it
 * @param addr 注册的内存地址
 * @param length 长度
 * @param access 权限
 * @param mr
 * @param sge
 * @return
 */
int RdmaConnection::setup_mr_sge(void *addr, unsigned int length, int access, struct ibv_mr **mr, struct ibv_sge *sge)
{
    /* register the address for appropriate access */
    *mr = setup_mr(addr, length, access);
    if (*mr == nullptr) {
        return -1;
    }

    /* fill in the fields of a single scatter-gather element */
    setup_sge(addr, length, (*mr)->lkey, sge);

    LOG4CXX_TRACE(logger, "regeister data addr is " << (*mr)->addr << ", size is " << (*mr)->length << ", lkey is " << (*mr)->lkey);
    return 0;
}


/**
 * 设置send work_request，sg_list包含n个 scatter-gather sges
 * @param sg_list
 * @param opcode
 * @param n_sges
 * @param send_work_request
 */
void RdmaConnection::setup_send_wr(void *context, struct ibv_sge *sg_list, enum ibv_wr_opcode opcode, int n_sges, struct ibv_send_wr *send_work_request)
{
    // 这个context到时候可以通过wc返回
//    send_work_request->wr_id = (uint64_t)(unsigned long)context;
    uint64_t id = (uint64_t)(engine());
    send_work_request->wr_id = id;
//    gmap_wr[id] = send_work_request; // 保存映射，这是是发送的，就先不保存了

    // 遇到的bug，直接用一个局部变量的地址，几乎每次都相同，所以这里先改成随机变量了，反正发送的wr的id不需要映射
//    uint64_t id = reinterpret_cast<uint64_t>(send_work_request);
//    send_work_request->wr_id = id;
    LOG4CXX_DEBUG(logger, "send wr_id is " << std::hex << id << std::dec);

    /* not chaining this work request to other work requests */
    send_work_request->next = nullptr;

    /* point at array of scatter-gather elements for this send */
    send_work_request->sg_list = sg_list;

    /* number of scatter-gather elements in array actually being used */
    send_work_request->num_sge = n_sges;

    /* the type of send */
    send_work_request->opcode = opcode;

    /* set SIGNALED flag so every send generates a completion
     * (if we don't do this, then the send queue fills up!)
     */
    send_work_request->send_flags = IBV_SEND_SIGNALED; //dz 设置了这个，每个send都会产生wc，但是并不是都需要，不设置这个，只有出错的send才会产生wc

    /* not sending any immediate data */
    send_work_request->imm_data = 0;
}

/**
 * 设置receive work request
 * @param context
 * @param sg_list
 * @param n_sges
 * @param recv_work_request
 */
void RdmaConnection::setup_recv_wr(void *context, struct ibv_sge *sg_list, int n_sges, struct ibv_recv_wr *recv_work_request)
{
    /* set the user's identification to be our conn pointer */
//    recv_work_request->wr_id = (uint64_t)(unsigned long)context; // 这里的wr_id都是一样的
//    scidb::ccm::Uuid id = scidb::ccm::Uuid::create(); // uuid 128位的，截断了一半了
//    uint64_t uid = *((uint64_t*)id.data()); // uuid data返回的是一个指向uint8的指针，是一个包含16个uint8的数组，强转成64*看行不行
//    recv_work_request->wr_id = uid;

//    uint64_t id = (uint64_t)(engine());
//    recv_work_request->wr_id = id;
//    LOG4CXX_DEBUG(logger, "recv id is " << std::hex << id << std::dec);
//    gmap_wr[id] = recv_work_request; // 保存映射
//    LOG4CXX_DEBUG(logger, "map the id to the work request is " << recv_work_request << ", gmap_wr[id] is " << gmap_wr[id]);

    // 换一种方式，context直接保存recv_work_request指针地址
    uint64_t id = reinterpret_cast<uint64_t>(recv_work_request);
    recv_work_request->wr_id = id;
    LOG4CXX_TRACE(logger, "recv wr_id is " << std::hex << id << std::dec);
    gmap_conn[id] = shared_from_this();
    LOG4CXX_TRACE(logger, "map the wr_id to the work request is " << recv_work_request << ", gmap_conn[id] is " << gmap_conn[id].get()); // 后面这个可以获取原始指针

    /* not chaining this work request to other work requests */
    recv_work_request->next = nullptr;

    /* point at array of scatter-gather elements for this recv */
    recv_work_request->sg_list = sg_list;

    /* number of scatter-gather elements in array actually being used */
    recv_work_request->num_sge = n_sges;
}


/**
 * 分配buffer，并注册到内存
 * @param n_bufs 需要小于 MAX_BUFFERS 否则数组存不下
 * @param access buffer 权限
 * @param data_size 每个buffer的大小
 * @return
 */
int RdmaConnection::setup_buffer_data(int n_bufs, int access, int data_size)
{
    int	ret, i;
    n_data_bufs = 0;

    for (i = 0; i < n_bufs; i++) {
        buffer_data[i] = (unsigned char *)malloc(data_size);
        if (buffer_data[i] == nullptr) {
            ret = ENOMEM;
            LOG4CXX_ERROR(logger, "malloc buffer failed, errno is " << errno);
            unsetup_buffers();
            return ret;
        }

        // register 当前这一块内存区域到 memory region中，然后将一个sge指向这一块内存
        ret = setup_mr_sge(buffer_data[i], data_size, access, &user_data_mr[i], &user_data_sge[i]); // access改成int型了，不是数组了，数组不好传入，因为大部分都是一样的权限
        if (ret != 0) {
            free(buffer_data[i]);
            unsetup_buffers();
            return ret;
        }

        n_data_bufs++;
    }
    return 0;
}

/**
 * 释放所有分配的buffer
 */
void RdmaConnection::unsetup_buffers()
{
    unsetup_mr(user_data_mr, n_data_bufs);

    for (int i = 0; i < n_data_bufs; i++) {
        free(buffer_data[i]);
    }
}


int RdmaConnection::setup_recv_buffers_and_mr()
{
    LOG4CXX_DEBUG(logger, "start setup_recv_buffers_and_mr");
    int	ret;
//    int	access[2] = {0, IBV_ACCESS_LOCAL_WRITE};
    int access = IBV_ACCESS_LOCAL_WRITE;

    // 一次性直接申请max_buffer个buffer和mr, szu_lab实测 max_recv_wr=64，max_send_wr=111
    ret = setup_buffer_data(MAX_BUFFER_NUMS, access, MAX_BUFFER_SIZE);
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "data buffer set failed.");
        return ret;
    }

    for(int i=0; i<MAX_BUFFER_NUMS; i++){
        setup_recv_wr(this, &user_data_sge[i], 1, &recv_work_request[i]); // 设置context
    }

    LOG4CXX_INFO(logger, "n_data_bufs is " << n_data_bufs);
    return ret;
}


int RdmaConnection::check_wc_status(enum ibv_wc_status status)
{
	// enum ibv_wc_status 确保ret在这个enum范围内，才能使用下面的ibv_wc_status_str，否则直接返回
	if (status < IBV_WC_SUCCESS || status > IBV_WC_GENERAL_ERR)
		return status;

    LOG4CXX_ERROR(logger, "ibv_post_send returned status error, wc status is " << ibv_wc_status_str(status));
	return 0;
}	/* check_wc_status */

/**
 * called to post a work request to the send queue
 */
int RdmaConnection::post_send(struct ibv_send_wr *send_work_request)
{
	struct ibv_send_wr	*bad_wr;
	int	ret;

	errno = 0;
    // 发送一串wr到queue pair的send queue，遇到第一个wr错误会返回，并将wr赋值给bad_wr
    LOG4CXX_TRACE(logger, "just before post_send data to rdma, local qp num is " << _cm_id->qp->qp_num);
	ret = ibv_post_send(_cm_id->qp, send_work_request, &bad_wr); // wr使用的buffer，只有对应的wce被从cq中取出然后处理了，才能重新使用，不过如果使用了ibv_send_inline那么buffer可以立即重新使用
	if (ret != 0) {
        LOG4CXX_ERROR(logger, "ibv_post_send error, errno is " << errno);
		if (check_wc_status((enum ibv_wc_status)ret) != 0) {
            LOG4CXX_ERROR(logger, "ibv_post_send error, wc status not in enum, ret is " << ret);
		}
	}
	return ret;
}


/**
 * called to post a work request to the receive queue
 */
int RdmaConnection::post_recv(struct ibv_recv_wr *recv_work_request)
{
	struct ibv_recv_wr	*bad_wr;
	int	ret;

	errno = 0; // 下面调用失败之后，errno会被设置，所以这里先置0
	ret = ibv_post_recv(_cm_id->qp, recv_work_request, &bad_wr);
    if (ret != 0) {
		if (check_wc_status((enum ibv_wc_status)ret) != 0) {
            LOG4CXX_ERROR(logger, "ibv_post_recv error, wc status not in enum, ret is " << ret);
		}
	}
	return ret;
}


/**
 * 新建的一个线程，不断从completion queue对应的event channel中 阻塞获取收到数据的event，然后一次性
 * 全部poll出来，轮询处理，处理完成所有的wc之后，继续阻塞等待cq对应的event channel。
 * 这里的一个线程是对应一个conn的
*/
/*
void* complete_queue_thread(void* param){
    struct ibv_wc* work_completion;
	int	ret;
    RdmaConnection* conn = reinterpret_cast<RdmaConnection*>(param);

	conn->wc_recv = conn->wc_send = 0; // 这个目前只有这个线程处理
    // 无限轮询，可以变成有退出条件的，如果conn的disconnectted变化了
	for ( ; ; ) {
		// 等待下一个wc的通知，然后poll一次取出多个wc，一个个处理
		ret = wait_for_completion(conn, &work_completion);
		if (ret != 0) {
			*//* hit error or FLUSH_ERR, in either case leave now *//*
			// goto out0;
            LOG4CXX_ERROR(logger, "wait_for_completion error, ret is " << ret);
            continue;
		}

		*//* see whether it was the send or recv that just completed *//*
		switch (work_completion->opcode) {
        // 收到消息了！
		case IBV_WC_RECV:
			conn->wc_recv++;
            // 校验收到的数据长度，是否符合预期
			// if (work_completion->byte_len != options->data_size) {
			// 	fprintf(stderr,
			// 		"%s: received %d bytes, expected %lu\n",
			// 		options->message,
			// 		work_completion->byte_len,
			// 		options->data_size);
            //     LOG4CXX_ERROR(logger, "received %d bytes, expected is %lu ", work_completion->byte_len, options->data_size);
			// }

			*//* repost our receive to catch the client's next send *//*
			ret = post_recv(conn, &conn->user_data_recv_work_request[0]);
			if (ret != 0) {
				return ret;
			}

            // 应该要调用消息分发和处理的东西了
            // _networkManager->
            NetworkManager* mgr = conn.io_service.getmgr(); // 从对应的ioservice里面回去对应的 networkmgr
            if(mgr == nullptr){
                LOG4CXX_ERROR(logger, "mgr is nullptr");
                return -1;
            }
            // 分发消息，数据已经存在之前的接收buffer中了
            unsigned int wr_id = work_completion->wr_id;
            int len = work_completion->byte_len; // 数据长度
            int imm_data = work_completion->imm_date; // 网络字节序
            unsigned int local_qp = work_completion->qp_num;
            unsigned int remote_qp = work_completion->src_qp; // 远端qp，文档写的是 remote qp number

            // 之前的buffer如何获取，要写个map映射wr_id和work request
            // mgr->dispatchMessage(const std::shared_ptr<Connection>& connection, const std::shared_ptr<MessageDesc>& messageDesc);

            // 分发消息前，要先将消息转换成MessageDesc的shared_ptr才行            

			break;
        // send的完成，不用怎么处理，统计数据++就OK了
		case IBV_WC_SEND:
			conn->wc_send++;
            LOG4CXX_DEBUG(logger, "%lu send completed ok.", conn->wc_send);
			break;

		default:
			our_report_ulong("ibv_poll_cq",
					"bad work completion opcode",
					work_completion->opcode, options);
            LOG4CXX_ERROR(logger, "ibv_poll_cq bad work completion opcode, opcode is %s", work_completion->opcode);
            return -1;
		}	*//* switch *//*
	}	*//* for *//*

	return ret;
}*/

/**
 * 阻塞等待channel的事件发生
*/
/*
static int wait_for_notification(RdmaConnection* conn)
{
	struct ibv_cq	*event_queue;
	void		*event_context;
	int		ret;

	// 等待一个 completion notification，会阻塞
	errno = 0;
	ret = ibv_get_cq_event(conn->completion_channel, &event_queue, &event_context);
	if (ret != 0) {
        LOG4CXX_ERROR(logger, "ibv_get_cq_event error, ret is " << ret);
        return ret;
	}

	conn->cq_events_that_need_ack++;
	if (conn->cq_events_that_need_ack == UINT_MAX) { // 如果编号已经到达unsigned要溢出了，就设置为0
		ibv_ack_cq_events(conn->completion_queue, UINT_MAX);
		conn->cq_events_that_need_ack = 0;
	}

	*//* request a notification when next completion arrives
	 * into an empty completion queue.
	 * See examples on "man ibv_get_cq_event" for how an
	 * "extra event" may be triggered due to a race between
	 * this ibv_req_notify() and the subsequent ibv_poll_cq()
	 * that empties the completion queue.
	 * The number of occurrences of this race will show up as
	 * the value of completion_stats[0].
	 *//*
	errno = 0;
	ret = ibv_req_notify_cq(conn->completion_queue, 0); // 一个cq获取了cq_event通知后，需要重新调用这个函数才能让他再发通知，否则是不会发通知的
	if (ret != 0) {
		LOG4CXX_ERROR(logger, "ibv_req_notify_cq error, ret is " << ret);
        return -1;
	}

    // 校验 event_queue应该要是 cm_channel对应的completion_queue
	if (event_queue != conn->completion_queue) {
		// fprintf(stderr, "%s, %s got notify for completion queue %p, exected queue %p\n", options->message, "ibv_get_cq_event", event_queue, conn->completion_queue);
        LOG4CXX_ERROR(logger, "ibv_get_cq_event got notify for completion queue, %p, exected queue %p", event_queue, conn->completion_queue);
        return -1;
	}
	
    // 校验context
	if (event_context != conn) {
		// fprintf(stderr, "%s, %s got notify for completion "
		// 	"context %p, exected context %p\n",
		// 	options->message, "ibv_get_cq_event",
		// 	event_context, conn);
        LOG4CXX_ERROR(logger, "ibv_get_cq_event got notify for completion context, %p, exected context %p", event_context, conn);            
        return -1;
	}

	return ret;
}*/


/** 
 * rdmaconn链接建立之后conn调用的
 * 等待cq对应的channel中出现事件
*/
/*
int wait_for_completion(RdmaConnection* conn, struct ibv_wc **work_completion)
{
	int	ret;

	// 注意，这里也是循环，先判断上一次取出来的wc是否全部处理完成了，然后阻塞等待event
	do	{
		if (conn->index_work_completions < conn->current_n_work_completions) {
			// 上一次返回的wc还没有处理完，先处理了，index要++
			*work_completion = &conn->work_completion[conn->index_work_completions++];
			break;
		}
		// 已经全部处理完成了，等待下一个event通知
		ret = wait_for_notification(conn, options); // 这里面会阻塞等待
		if (ret != 0) {
			// goto out0;
            return ret;
		}
		conn->notification_stats++;
		*//* now collect every completion in the queue *//*
		errno = 0;
		ret = ibv_poll_cq(conn->completion_queue,
				conn->max_n_work_completions, // 一次性poll了最多这么多回来，这个数值比创建 complete_queue的还要大一点 + 1
				conn->work_completion);
		if (ret < 0) {
            LOG4CXX_ERROR("ibv_poll_cq error, ret is " << ret);
            return ret;
		}

		*//* keep statistics on the number of items returned *//*
		if (ret > conn->max_n_work_completions) {
            LOG4CXX_ERROR("ibv_poll_cq error, ret greater than conn->max_n_work_completions, ret is " << ret << ", max_n_work_completions is " << conn->max_n_work_completions);
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

// 2022年8月9日14:39:30 处理收到的消息，然后分发到network manager处理

/**
 * @brief Validate _messageDesc & read from socket
 *    _messageDesc->getMessageHeader().getRecordSize() bytes of data
 */
 /*
void RdmaConnection::handleReadMessage(const boost::system::error_code& error, size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

    // MessageDescPtr _messageDesc是conn的成员变量，存储incoming message
    // typedef std::shared_ptr<MessageDesc> MessageDescPtr; 它是这个指针类型
   if(!_messageDesc->validate() ||
      _messageDesc->getMessageHeader().getSourceInstanceID() == _selfInstanceId) {
      LOG4CXX_ERROR(logger, "RdmaConnection::handleReadMessage: unknown/malformed message, closing RdmaConnection");
      handleReadError(bae::make_error_code(bae::eof));
      return;
   }

   assert(bytes_transferr == sizeof(_messageDesc->getMessageHeader()));
   assert(_messageDesc->getMessageHeader().getSourceInstanceID() != _selfInstanceId);
   assert(_messageDesc->getMessageHeader().getNetProtocolVersion() == scidb_msg::NET_PROTOCOL_CURRENT_VER);

   boost::asio::async_read(_socket,
                           _messageDesc->_recordStream.prepare(
                               _messageDesc->getMessageHeader().getRecordSize()), // dz：准备这么多的空间，读取的数据就放在_recordStream里面
                           std::bind(&RdmaConnection::handleReadRecordPart,
                                     shared_from_this(),
                                     std::placeholders::_1,
                                     std::placeholders::_2));

   LOG4CXX_TRACE(logger, "RdmaConnection::handleReadMessage: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " from instanceID="
            << Iid(_messageDesc->getMessageHeader().getSourceInstanceID())
            << " ; recordSize="
            << _messageDesc->getMessageHeader().getRecordSize()
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());
}*/

/*
 * @brief  If header indicates data is available, read from
 *         socket _messageDesc->binarySize bytes of data.
 */
/*
void RdmaConnection::handleReadRecordPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   assert(_messageDesc->validate());

   assert(  _messageDesc->getMessageHeader().getRecordSize() ==
            bytes_transferr);

   assert(  _messageDesc->getMessageHeader().getSourceInstanceID() !=
            _selfInstanceId);

   if (!_messageDesc->parseRecord(bytes_transferr)) { // dz：这里说明header是没有序列化的，只有后面的record属于Google那个消息才序列
       LOG4CXX_ERROR(logger,
                     "Network error in handleReadRecordPart: cannot parse record for "
                     << " msgID="
                     << _messageDesc->getMessageHeader().getMessageType()
                     << ", closing RdmaConnection");

       handleReadError(bae::make_error_code(bae::eof));
       return;
   }
   _messageDesc->prepareBinaryBuffer();

   LOG4CXX_TRACE(logger,
        "handleReadRecordPart: "
            << strMsgType(_messageDesc->getMessageHeader().getMessageType())
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());

   if (_messageDesc->_messageHeader.getBinarySize())
   {
       boost::asio::async_read(_socket,
                               boost::asio::buffer(_messageDesc->_binary->getWriteData(),
                                                   _messageDesc->_binary->getSize()),
                               std::bind(&RdmaConnection::handleReadBinaryPart,
                                         shared_from_this(),
                                         std::placeholders::_1,
                                         std::placeholders::_2));
       return;
   }

   handleReadBinaryPart(error, 0); // dz:没有binary数据
}*/

/*
 * @brief Invoke the appropriate dispatch routine
 */
/*
void RdmaConnection::handleReadBinaryPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }
   assert(_messageDesc);

   assert(_messageDesc->getMessageHeader().getBinarySize() ==
          bytes_transferr);

   std::shared_ptr<MessageDesc> msgPtr;
   _messageDesc.swap(msgPtr);

   _lastRecv = getCoarseTimestamp();

   // mtHangup means we need not read from this RdmaConnection again, it
   // is for outbound peer traffic.  (We did read from it during
   // authentication, but we are done now.)
   //
   // NOTE, however, that because of SDB-5702 mtHangup is not
   // currently being used.
   //
   if (msgPtr->getMessageType() == mtHangup) {
       if (isOutbound()) {
           LOG4CXX_TRACE(logger, "Hanging up on outbound RdmaConnection to "
                         << getPeerId() << " on conn=" << hex << this << dec);
           // We just need to NOT queue up another readMessage() call.
           return;
       }
       LOG4CXX_DEBUG(logger, "DROPPED mtHangup from non-peer " << getPeerId()
                     << " on conn=" << hex << this << dec);
   }
   else {
       std::shared_ptr<RdmaConnection> self(shared_from_this());
       _networkManager.handleMessage(self, msgPtr);
   }

   // Preparing to read new message
   assert(_messageDesc.get() == nullptr);
   readMessage();
}*/

//        vector<boost::asio::const_buffer>
//typedef std::list<std::shared_ptr<MessageDesc> > MessageDescList;
//        std::shared_ptr<MessageDescList>
/**
 * 将buffer中包含的数据全部发送出去，
 * @param constBuffers 注意，传引用过来
 */
int RdmaConnection::send_remote(std::vector<boost::asio::const_buffer>& constBuffers){
    int ret = 0;
//    int access = 0; // 本地不需要写入权限，只需要读取就可以了
    int access = IBV_ACCESS_LOCAL_WRITE; // 本地不需要写入权限吗 access改成int型了，不是数组了，数组不好传入，因为大部分都是一样的权限
//    struct ibv_mr* mr[MAX_MR]; // 一次不用申请这么大的数组，改成了下面的用vector动态存储，根据constbuffer里面的buffer数量来
//    struct ibv_sge sge[MAX_SGE];
//    struct ibv_send_wr send_wr[MAX_WORK_REQUESTS];

    // 根据constBuffers的大小来设置动态的send_wr数组, 节约内存
    unsigned int n = constBuffers.size();
    LOG4CXX_DEBUG(logger, "this const buffers contains " << n << " buffers, this: " << this);
    std::vector<struct ibv_mr*> mr(n, nullptr); // 注册的buffer对应的mr都放在这里面了
    std::vector<struct ibv_sge> sge(n);
    std::vector<struct ibv_send_wr> send_wr(n); // 生命周期会直接结束

//        std::vector<struct ibv_send_wr> send_wr = new std::vector<struct ibv_send_wr>(n);
//    LOG4CXX_DEBUG(logger, "send_wr is " << &mr);
//    LOG4CXX_DEBUG(logger, "send_wr is " << &sge);
    LOG4CXX_DEBUG(logger, "send_wr addr is " << &send_wr);
//    LOG4CXX_DEBUG(logger, "send_wr 0 is " << &send_wr[0]);

    // 先注册内存 注册的内存要取消注册 另外之前的msg里面的内存也要删除
    unsigned int count = 0; // 记录有多少个wr需要发送，在这里会将同一个消息的buffer链接在一起，当成一个wr发送。
    unsigned int next = 0;
    for(unsigned int i=0; i<n; i++){
//    for(auto &buffer : constBuffers){
//        void* data = const_cast<void*>(boost::asio::detail::buffer_cast_helper(buffer));
//        std::size_t data_size = boost::asio::detail::buffer_size_helper(buffer);
        auto& buffer = constBuffers[i];
        void* data = const_cast<void*>(buffer.data()); // 获取里面的原始指针
        std::size_t data_size = buffer.size();
        // dz 2022年12月4日 16:53:42出现了一个size为0的buffer，导致下面的register失败了，mr注册不允许长度为0，报错22：invalid argument
//        LOG4CXX_DEBUG(logger, "buffer data addr is " << data << ", size is " << data_size);
        // mtSyncResponse的消息用于同步，创建了Dummy msg 这个类型的消息在 scidb_msg.proto文件定义的是DummyQuery，只有可选的两个参数，也就是说长度可以为0
        if(data_size == 0){
//            LOG4CXX_ERROR(logger, "buffer size is 0");
            continue; // 直接跳过，注意后面dereg mr的时候也要跳过这个，因为根本就没有注册
        }

        // register 当前这一块内存区域到 memory region中，然后将一个sge指向这一块内存
        // 重要！！！！下面的内存是不是重新注册了，多注册了一遍？ new：没有，这个这是把这几个buffer的内存都注册了一遍，下面的设置send_wr的只针对header
        // 这里面注册的同一个消息的header，record，binary会串成一个sg_list发送
        ret = setup_mr_sge(data, data_size, access, &mr[i], &sge[i]);

        if (ret != 0) {
//            free(data); // 失败了，不能free，消息还没发
//            unsetup_buffers();
            LOG4CXX_ERROR(logger, "setup mr and sge failed");
            return ret;
        }

        // 一个消息的buffer分成3个部分，header，record，binary，至少会有前面2部分，可能有第三部分
        if(i == next){
            // 一个消息的起始buffer部分，对应header，不是header部分不用 设置wr，现在是将一个消息的2个或者3个buffer放到一个wr的sge list中
            MessageHeader* header = reinterpret_cast<MessageHeader*>(data);
            uint16_t protocal = header->getNetProtocolVersion();
            if(protocal != 11){ // 目前版本是11
                LOG4CXX_ERROR(logger, "protocal version error, protocal is " << protocal);
                return -1;
            }
            uint64_t recordSize = header->getRecordSize();
            uint64_t binarySize = header->getBinarySize();

            uint64_t totla_size = 58 + recordSize + binarySize;
            if(totla_size > MAX_BUFFER_SIZE){
                // 对端buffer最大为max_buffer_size, 如果超过这个大小，对端无法接受，报错 remote invalid request error
                LOG4CXX_ERROR(logger, "total send msg size is too large: " << totla_size);
            }

            if(binarySize > 0){
                next += 3; // 包含binary
                // 注意！！这里sge是当成数组的，用vector语义应该也可以，会根据n_sges参数，当前sge[i]及后面的一共n_sges当成一个发送出去！！！
                // 没有考虑 record=0 && binary !=0的情况，不知道有没有这种，有这种的话就不太好处理了哦 new：实际测试了一下，暂时没有发现这种情况
                setup_send_wr(this, &sge[i], IBV_WR_SEND, 3, &send_wr[count]);
            }else{
                if(recordSize == 0){
                    // 有的消息record size也是0，比如Hangup、DummyQuery等
                    setup_send_wr(this, &sge[i], IBV_WR_SEND, 1, &send_wr[count]);
                }else{
                    setup_send_wr(this, &sge[i], IBV_WR_SEND, 2, &send_wr[count]);
                }
                next += 2; // 虽然recordsize是0，但是也会push到这个const buffer里面，所以这里要跳过
            }
            count++;
        }
    }

    // 将上面生成的work request发送出去，因为有合并发送，所以wr数量少于buffer数量
    for(unsigned int j=0; j<count; j++){
        // 先设置send work request, 注意：之前这里将j错写成了i，导致同一个地址注册了多次mr，发送之后wc提示失败，报错 local protection error
//        setup_send_wr(this, &sge[j], IBV_WR_SEND, 1, &send_wr[j]); // 不需要了，挪到上面了，多个buffer放到一个wr中发送
        // 然后发送
        ret = post_send(&send_wr[j]);
        if(ret != 0){
            LOG4CXX_ERROR(logger, "spost_send failed, j is " << j << ".");
        }
    }

    // 这里的buffer不是我们malloc的，我们只是将这一段地址映射到了memory region中，所以只需要取消映射，不需要释放内容
    // 我们改变了这个流程，free buffer的时机要注意，原来的代码没有机会释放了
//    for(int i = 0; i < n; i++){
//        if(mr[i] != nullptr){
//            errno = 0;
//            ret = ibv_dereg_mr(mr[i]);
//            if (ret != 0) {
//                LOG4CXX_ERROR(logger, "send remote: ibv_dereg_mr failed, errno is " << errno);
//            }
//        }
//    }

    send_wr.clear(); // 这里删除掉wr

    // 先自己设置一下mr, 这样是可以发送成功的
    /**
    void* data = malloc(100);
    std::string str = "hello";
    strncpy(reinterpret_cast<char*>(data), str.c_str(), 6);
    int data_size = 100;

    struct ibv_mr* mr2;
    struct ibv_sge sge2;
    struct ibv_send_wr send_wr;
    ret = setup_mr_sge(data, data_size, access, &mr2, &sge2);
    if (ret != 0) {
        LOG4CXX_ERROR(logger, "setup mr and sge failed");
        return ret;
    }
    setup_send_wr(this, &sge2, IBV_WR_SEND, 1, &send_wr);
    ret = post_send(&send_wr);
    if(ret != 0){
        LOG4CXX_ERROR(logger, "spost_send failed");
    }
    */
    return 0;
}

int RdmaConnection::connect(InstanceID src_id, InstanceID dest_id, const std::string remoteIp)
{
    int ret = -1;

    // 通过 dest_id 来推断对方rdma的监听端口
    size_t index = getServerInstanceId(dest_id);
    unsigned int port = LISTEN_PORT + index;
    LOG4CXX_DEBUG(logger, "RdmaConnection connect start as client, dest_id is " << Iid(dest_id) << ", remote ip is "
            << remoteIp << ", infer port is " << port);

    _is_client = true; // as client
    _selfInstanceID = src_id;
    _remoteInstanceId = dest_id;
    _remoteIp = remoteIp;

    struct rdma_addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_port_space = RDMA_PS_TCP;
    // dz 添加char*转换 szu_lab 编译不允许
    ret = rdma_getaddrinfo((char*)remoteIp.c_str(), (char*)std::to_string(port).c_str(), &hints, &_res);
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma_getaddrinfo error: " << gai_strerror(ret));
        return -1;
    }

    // struct ibv_qp_init_attr init_attr; // queue pair 初始化属性
    // memset(&init_attr, 0, sizeof init_attr);
    // init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = 1;
    // init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1;
    // init_attr.cap.max_inline_data = 16;
    // init_attr.qp_context = _cm_id;
    // init_attr.sq_sig_all = 1;

    // ret = rdma_create_ep(&_cm_id, res, NULL, &init_attr); // 这个函数创建cm_id，如果提供了第三个参数，会创建pd, 提供第四个参数qp初始化属性，会创建qp
    /*if(_cm_id == nullptr){
        LOG4CXX_ERROR(logger, "this thread is starting, _cm_id is nullptr");
        return;
    }*/

    ret = create_pd();
    if(ret){
        LOG4CXX_ERROR(logger, "create_pd error, errno is:" << errno);
        return -1;
    }

    // 改成常规的那种，这里会创建channel，会需要fd，如果不平凡调用这个函数，其实也还好
    // 通过rdma_client程序测试过了，不传后面的两个参数，cm_id会有verbs，channel，pd(使用系统默认的pd)，没有qp，测试中发现remote ip是127.0.0.1的话，会报错！！
    ret = rdma_create_ep(&_cm_id, _res, _protection_domain, NULL); // 目前这里会先绑定一个channel，后面在conn start里面会转移到 mgr event channel里面去的
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma_create_ep error, errno is:" << errno);
        return -1;
    }
    LOG4CXX_DEBUG(logger, "rdma_create_ep success, ret is:" << ret << ", _cm_id is " << _cm_id << ", ep assign fd is " << _cm_id->channel->fd);

    // Check to see if we got inline data allowed or not
    /*
      static int send_flags;
      if (init_attr.cap.max_inline_data >= 16){
        send_flags = IBV_SEND_INLINE;
    }else{
        LOG4CXX_DEBUG(logger, "rdma_client: device doesn't support IBV_SEND_INLINE, using sge sends");
    }*/

    // 启动client conn
    return start(Start_type::client);

} // connect

//} // namespace rdma
}  // namespace scidb

//  LocalWords:  RdmaConnection
