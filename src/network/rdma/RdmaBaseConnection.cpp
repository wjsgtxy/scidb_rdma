#include "RdmaBaseConnection.h"

#include <memory>

#include <network/MessageDesc.h>
#include <system/Exceptions.h>

//using namespace std;
namespace asio = boost::asio;

// 简单点，这个类先不用了

namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
log4cxx::LoggerPtr RdmaBaseConnection::logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * B a s e C o n n e c t i o n
 */
RdmaBaseConnection::RdmaBaseConnection (boost::asio::io_service& ioService)
    : _socket(ioService)
{ }

RdmaBaseConnection::~RdmaBaseConnection()
{
    disconnect();
}

void RdmaBaseConnection::connect(std::string address, uint32_t port)
{
   LOG4CXX_DEBUG(logger, "rdma Connecting to " << address << ":" << port);

/*   asio::ip::tcp::resolver resolver(_socket.get_io_service());

   stringstream serviceName;
   serviceName << port; // dz stringstream数据类型转换
   asio::ip::tcp::resolver::query query(address, serviceName.str());
   asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query); // dz 解析地址
   asio::ip::tcp::resolver::iterator end;

   boost::system::error_code error = boost::asio::error::host_not_found;
   while (error && endpoint_iterator != end)
   {
      _socket.close();
      _socket.connect(*endpoint_iterator++, error);
   }
   if (error)
   {
      LOG4CXX_FATAL(logger, "Error #" << error << " when connecting to " << address << ':' << port << " (" << error.message() << ')');
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR) << error << address << port;
   }

   configConnectedSocket();
   LOG4CXX_DEBUG(logger, "Connected to " << address << ":" << port);*/

    // rdma建立同步链接 2022年7月1日15:11:51
 /*   struct rdma_addrinfo hints, *res;

    struct ibv_wc wc;
    int ret;

    memset(&hints, 0, sizeof hints);
    hints.ai_port_space = RDMA_PS_TCP;
    // int rdma_getaddrinfo(char *node, char *service, struct rdma_addrinfo *hints, struct rdma_addrinfo **res)
    stringstream str_port;
    str_port << port;
    ret = rdma_getaddrinfo(address.c_str(), str_port.c_str(), &hints, &res);
    if (ret) {
//        printf("rdma_getaddrinfo: %s\n", gai_strerror(ret));
//        goto out;
        LOG4CXX_FATAL(logger, "Error #" << error << " when connecting to " << address << ':' << port << " (" << error.message() << ')');
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CONNECTION_ERROR) << error << address << port;
    }

    // 设置qp属性
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof attr);
    attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
    attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 16;
    attr.qp_context = id;
    attr.sq_sig_all = 1;

    // int rdma_create_ep(struct rdma_cm_id **id, struct rdma_addrinfo *res, struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr)
    // 通信id是要返回的值，res是rdma_getaddrinfo返回的, protect domain 可选，
    ret = rdma_create_ep(&id, res, NULL, &attr);
    // Check to see if we got inline data allowed or not
    if (attr.cap.max_inline_data >= 16)
        send_flags = IBV_SEND_INLINE; // 应该是直接就在请求中传输数据了，不用通过sge传输了
    else {
        LOG4CXX_DEBUG(logger, "RdmaBaseConnection: device doesn't support IBV_SEND_INLINE, using sge sends");
    }

    if (ret) {
        LOG4CXX_ERROR(logger, "rdma_create_ep failed");
        goto out_free_addrinfo;
    }

    // 注册mr
    mr = rdma_reg_msgs(id, recv_msg, 16);
    if (!mr) {
        LOG4CXX_ERROR(logger, "rdma register memory failed");
        ret = -1;
        goto out_destroy_ep;
    }

    if ((send_flags & IBV_SEND_INLINE) == 0) {
        //
        send_mr = rdma_reg_msgs(id, send_msg, 16);
        if (!send_mr) {
            LOG4CXX_ERROR(logger, "rdma register memory failed");
            ret = -1;
            goto out_dereg_recv;
        }
    }

    ret = rdma_post_recv(id, NULL, recv_msg, 16, mr);
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma rdma_post_recv failed");
        goto out_dereg_send;
    }

    // 建立连接
    ret = rdma_connect(id, NULL);
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma rdma_connect failed");
        goto out_dereg_send;
    }*/

    // 这里只用到建立连接就ok了
    /*
    ret = rdma_post_send(id, NULL, send_msg, 16, send_mr, send_flags);
    if (ret) {
        LOG4CXX_ERROR(logger, "rdma rdma_post_send failed");
        goto out_disconnect;
    }

    while ((ret = rdma_get_send_comp(id, &wc)) == 0); // 找send的完成，ret等于找到的completions，如果等于0，说明还没有，一直循环
    if (ret < 0) {
        LOG4CXX_ERROR(logger, "rdma rdma_get_send_comp failed");
        goto out_disconnect;
    }

    while ((ret = rdma_get_recv_comp(id, &wc)) == 0);
    if (ret < 0)
        LOG4CXX_ERROR(logger, "rdma rdma_get_recv_comp failed");
    else
        ret = 0;
    */

//        return ret;

}

void RdmaBaseConnection::configConnectedSocket()
{
   /*boost::asio::ip::tcp::no_delay no_delay(true);
   _socket.set_option(no_delay); // dz 无延迟，不需要ack即可发送下一个小包
   boost::asio::socket_base::keep_alive keep_alive(true);
   _socket.set_option(keep_alive); // dz tcp是一直连接的，这个参数表示一段时间没有数据发送后，是否需要发送探测包看对方是否还在

   int s = _socket.native_handle();
   int optval;
   socklen_t optlen = sizeof(optval);

   *//* Set the option active *//*
   optval = 1; // 探测尝试的次数
   if(setsockopt(s, SOL_TCP, TCP_KEEPCNT, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPCNT)");
   }
   optval = 30;// 如该连接在30秒内没有任何数据往来,则进行探测
   if(setsockopt(s, SOL_TCP, TCP_KEEPIDLE, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPIDLE)");
   }
   optval = 30; // 探测时发包的时间间隔为30秒
   if(setsockopt(s, SOL_TCP, TCP_KEEPINTVL, &optval, optlen) < 0) {
      perror("setsockopt(TCP_KEEPINTVL)");
   }

   if (logger->isTraceEnabled()) { // 记录详细日志
      boost::asio::socket_base::receive_buffer_size optionRecv;
      _socket.get_option(optionRecv);
      int size = optionRecv.value();
      LOG4CXX_TRACE(logger, "Socket receive buffer size = " << size);
      boost::asio::socket_base::send_buffer_size optionSend;
      _socket.get_option(optionSend);
      size = optionSend.value();
      LOG4CXX_TRACE(logger, "Socket send buffer size = " << size);
   }*/
}

void RdmaBaseConnection::disconnect()
{
//    _socket.close();
    // 释放资源
    /*rdma_disconnect(id);
    if ((send_flags & IBV_SEND_INLINE) == 0){
        rdma_dereg_mr(send_mr);
    }
    rdma_dereg_mr(mr);
    rdma_destroy_ep(id);
    rdma_freeaddrinfo(res);
    LOG4CXX_DEBUG(logger, "RdmaBaseConnection：Disconnected");*/
}

void RdmaBaseConnection::send(std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "RdmaBaseConnection::send begin");
    /*try
    {
        std::vector<boost::asio::const_buffer> constBuffers;
        messageDesc->_messageHeader.setSourceInstanceID(CLIENT_INSTANCE);

        messageDesc->writeConstBuffers(constBuffers);
        boost::asio::write(_socket, constBuffers); // 同步的方法

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::send end");
    }
    catch (const boost::system::system_error &se)
    {
        stringstream ss;
        ss << "Write failed: " << se.code().message() << " (" << se.code() << ')';
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "send" << ss.str();
    }
    catch (const boost::exception &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE)
            << "send" << boost::diagnostic_information(e);
    }*/
}

} // namespace scidb
