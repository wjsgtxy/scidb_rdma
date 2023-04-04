//
// Created by dz on 2022/7/9.
//
#include "prototypes.h"
#include "RdmaUtilities.h"
#include <log4cxx/logger.h>
#include <fcntl.h> // 文件控制函数 fcntl

#include <sstream>

namespace scidb {
    // Logger for network subsystem. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.rdma.RdmaUtilities"));

    int changeNonBlock(int fd){
        // 设置为异步模式，变成非阻塞的
        int flags = fcntl(fd, F_GETFL);
        int rc = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        if(rc < 0){
            perror("O_NONBLOCK change failed");
            return -1;
        }
        return 0;
    }

    // 10进制转16进制，输出字符串
    std::string dec2hex(uint64_t i)
    {
        std::stringstream ioss;     //定义字符串流
        std::string s_temp;         //存放转化后字符
        ioss << std::hex << i;      //以十六制形式输出
        ioss >> s_temp;
        return s_temp;
    }

    uint64_t hex2dec(std::string data){
        std::stringstream ioss;
        uint64_t i;
        ioss << data;
        ioss >> std::dec >> i;
        return i;
    }
}