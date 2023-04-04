//
// Created by dz on 2022/7/6.
//

#ifndef RDMA_PROPERTIES_H
#define RDMA_PROPERTIES_H

// c++ standard libraries
#include <cstdint>
#include <iosfwd>

namespace scidb {
//    namespace rdma {

        extern const long INVALID_PORT;

/**
 * A set of properties needed by the Rdma Communication Manager
 *
 * This class is just a set individual properties that can be expanded/changed without
 * having to change the interface to the RdmaService.
 */
        class RdmaProperties
        {
        public:
            RdmaProperties();

            /**
             * Get the TCP/IP port on which the Rdma should listen for incoming requests.
             */
            uint16_t getPort() const;
            /**
             * Set the TCP/IP port on which the Rdma should listen for incoming requests.
             *
             * @return a reference to @c this
             */
            RdmaProperties& setPort(uint16_t port);
            /**
             * Set the TCP/IP port on which the Rdma should listen for incoming requests.
             *
             * @note This version is useful for command line arguments, which only support @c int
             * and not @c uint16_t as a checked type.
             *
             * @throw SYSTEM_EXCEPTION if the passed @int cannot be represented as a uint16_t (bad
             *        numeric cast)
             */
            RdmaProperties& setPort(int port);

            /**
             * Get the status of whether incoming TCP port of the Rdma will use TLS
             */
            bool getTLS() const;
            /**
             * Set the status of whether incoming TCP port of the Rdma will use TLS
             *
             * @return a reference to @c this
             */
            RdmaProperties& setTLS(int onoff);

            /**
             * Set the time-out (in seconds) to wait for a read of a Client Message (both the
             * header and protobuf) to complete.
             *
             * @note The type @c long is used, because that is the value used by
             *       boost::posix_time::seconds.
             *
             */
            long getReadTimeOut() const;
            /**
             * Get the time-out (in seconds) to wait for a read of a Client Message header/protobuf
             * to complete.
             *
             * @note The type @c long is used, because that is the value used by
             *       boost::posix_time::seconds.
             *
             */
            RdmaProperties& setReadTimeOut(long timeOut);

            /**
             * Set The time-out between messages allowed before a client session times out.
             *
             * @note The type @c long is used, because that is the value used by
             *       boost::posix_time::seconds.
             */
            long getSessionTimeOut() const;
            /**
             * Get The time-out  between messages allowed before a client session times out.
             *
             * @note The type @c long is used, because that is the value used by
             *       boost::posix_time::seconds.
             */
            RdmaProperties& setSessionTimeOut(long timeOut);

            friend std::ostream& operator<<(std::ostream& out, const RdmaProperties& props);

        private:
            uint16_t _port;
            bool _tls;
            long _readTimeOut;
            long _sessionTimeOut;
        };
//    }
}  // namespace scidb::rdma

#endif
