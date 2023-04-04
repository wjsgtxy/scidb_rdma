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

// The header file for the implementation details in this file
#include <ccm/CcmProperties.h>
// SciDB modules
#include <util/Utility.h>
#include <system/Exceptions.h>

namespace scidb { namespace ccm {

extern const long INVALID_PORT = 0;

CcmProperties::CcmProperties()
    : _port(INVALID_PORT)
    , _tls(true)
    , _readTimeOut(0L)
    , _sessionTimeOut(0L)
{}

uint16_t CcmProperties::getPort() const
{
    return _port;
}

CcmProperties& CcmProperties::setPort(uint16_t port)
{
    ASSERT_EXCEPTION(port != INVALID_PORT, "Invalid TCP port specified for CCM");
    _port = port;
    return *this;
}

CcmProperties& CcmProperties::setPort(int p)
{
    return setPort(safe_static_cast<uint16_t>(p));
}

bool CcmProperties::getTLS() const
{
    return _tls;
}

CcmProperties& CcmProperties::setTLS(int onoff)
{
    _tls = static_cast<bool>(onoff);
    return *this;
}

long CcmProperties::getReadTimeOut() const
{
    return _readTimeOut;
}

CcmProperties& CcmProperties::setReadTimeOut(long timeOut)
{
    _readTimeOut = timeOut;
    return *this;
}

long CcmProperties::getSessionTimeOut() const
{
    return _sessionTimeOut;
}

CcmProperties& CcmProperties::setSessionTimeOut(long timeOut)
{
    _sessionTimeOut = timeOut;
    return *this;
}

std::ostream& operator<<(std::ostream& os, const CcmProperties& props)
{
    os << "port: " << props.getPort() << " tls: " << std::boolalpha << props.getTLS()
       << " session time-out: " << std::noboolalpha << props.getSessionTimeOut()
       << " read time-out:  " << props.getReadTimeOut();
    return os;
}

}}  // namespace scidb::ccm
