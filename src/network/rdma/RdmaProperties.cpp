//
// Created by dz on 2022/7/6.
//

// The header file for the implementation details in this file
#include "RdmaProperties.h"
// SciDB modules
#include <util/Utility.h>
#include <system/Exceptions.h>

namespace scidb {
//    namespace rdma {

        extern const long INVALID_PORT = 0;

        RdmaProperties::RdmaProperties()
                : _port(INVALID_PORT)
                , _tls(true)
                , _readTimeOut(0L)
                , _sessionTimeOut(0L)
        {}

        uint16_t RdmaProperties::getPort() const
        {
            return _port;
        }

        RdmaProperties& RdmaProperties::setPort(uint16_t port)
        {
            ASSERT_EXCEPTION(port != INVALID_PORT, "Invalid TCP port specified for rdma");
            _port = port;
            return *this;
        }

        RdmaProperties& RdmaProperties::setPort(int p)
        {
            return setPort(safe_static_cast<uint16_t>(p));
        }

        bool RdmaProperties::getTLS() const
        {
            return _tls;
        }

        RdmaProperties& RdmaProperties::setTLS(int onoff)
        {
            _tls = static_cast<bool>(onoff);
            return *this;
        }

        long RdmaProperties::getReadTimeOut() const
        {
            return _readTimeOut;
        }

        RdmaProperties& RdmaProperties::setReadTimeOut(long timeOut)
        {
            _readTimeOut = timeOut;
            return *this;
        }

        long RdmaProperties::getSessionTimeOut() const
        {
            return _sessionTimeOut;
        }

        RdmaProperties& RdmaProperties::setSessionTimeOut(long timeOut)
        {
            _sessionTimeOut = timeOut;
            return *this;
        }

        std::ostream& operator<<(std::ostream& os, const RdmaProperties& props)
        {
            os << "port: " << props.getPort() << " tls: " << std::boolalpha << props.getTLS()
               << " session time-out: " << std::noboolalpha << props.getSessionTimeOut()
               << " read time-out:  " << props.getReadTimeOut();
            return os;
        }

//    }
}  // namespace scidb::rdma
