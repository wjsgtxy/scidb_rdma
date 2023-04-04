/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
#ifndef CCM_PROPERTIES_H_
#define CCM_PROPERTIES_H_

// c++ standard libraries
#include <cstdint>
#include <iosfwd>

namespace scidb { namespace ccm {

extern const long INVALID_PORT;

/**
 * A set of properties needed by the Client Communication Manager
 *
 * This class is just a set individual properties that can be expanded/changed without
 * having to change the interface to the CcmService.
 */
class CcmProperties
{
  public:
    CcmProperties();

    /**
     * Get the TCP/IP port on which the CCM should listen for incoming requests.
     */
    uint16_t getPort() const;
    /**
     * Set the TCP/IP port on which the CCM should listen for incoming requests.
     *
     * @return a reference to @c this
     */
    CcmProperties& setPort(uint16_t port);
    /**
     * Set the TCP/IP port on which the CCM should listen for incoming requests.
     *
     * @note This version is useful for command line arguments, which only support @c int
     * and not @c uint16_t as a checked type.
     *
     * @throw SYSTEM_EXCEPTION if the passed @int cannot be represented as a uint16_t (bad
     *        numeric cast)
     */
    CcmProperties& setPort(int port);

    /**
     * Get the status of whether incoming TCP port of the CCM will use TLS
     */
    bool getTLS() const;
    /**
     * Set the status of whether incoming TCP port of the CCM will use TLS
     *
     * @return a reference to @c this
     */
    CcmProperties& setTLS(int onoff);

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
    CcmProperties& setReadTimeOut(long timeOut);

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
    CcmProperties& setSessionTimeOut(long timeOut);

    friend std::ostream& operator<<(std::ostream& out, const CcmProperties& props);

  private:
    uint16_t _port;
    bool _tls;
    long _readTimeOut;
    long _sessionTimeOut;
};
}}  // namespace scidb::ccm

#endif  // ! CCM_PROPERTIES_H_
