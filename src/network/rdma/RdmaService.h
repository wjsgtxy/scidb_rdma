//
// Created by dz on 2022/7/6.
//

#ifndef RDMASERVICE_H
#define RDMASERVICE_H

// c++ standard libraries
#include <memory>

namespace scidb {
//    namespace rdma {

/**
 * @brief 节点之间rdma通信
 */
        class RdmaService
        {
        public:
            RdmaService();
            ~RdmaService() noexcept;

            void start();
            void stop();

        private:
            struct Impl;
            std::unique_ptr<Impl> _impl;
        };

//    }
}  // namespace scidb::rdma

#endif
