/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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

/**
 * @file Authenticator.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <rbac/Authenticator.h>

#include "Connection.h"
#include "InstanceAuthenticator.h"
#include "TrustAuthenticator.h"

#include <rbac/Rbac.h>
#include <rbac/UserDesc.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>
#include <rbac/SessionProperties.h>

#include <log4cxx/logger.h>
#include <random> // dz 添加 没这个文件makeCookie函数会报错

using namespace std;

using NsComm = scidb::namespaces::Communicator;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network.auth"));


AuthMethod Authenticator::getRequiredAuthMethod()
{
    static AuthMethod required = AUTH_NONE;

    // The configured security=... mode dictates the authentication
    // method.  Be paranoid: demand tough authentication if the
    // cluster appears to be misconfigured (and warn in the log).

    if (required == AUTH_NONE) {
        Config& cfg = *Config::getInstance();
        string mode = cfg.getOption<string>(CONFIG_SECURITY);
        transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
        if (mode == "trust") {
            required = AUTH_TRUST;
        } else if (mode == "password") {
            required = AUTH_RAW;
        } else if (mode == "pam") {
            required = AUTH_PAM;
        } else {
            LOG4CXX_WARN(logger, "Unrecognized security mode \"" << mode
                         << "\", will require \""
                         << auth::strMethodTag(AUTH_RAW)
                         << "\" authentication ("
                         << auth::strMethodDesc(AUTH_RAW) << ')');
            required = AUTH_RAW;
        }
    }

    return required;
}


AuthenticatorPtr
Authenticator::create(string const& auth_tag,
                      string const& user,
                      ConnectionPtr& conn)
{
    AuthenticatorPtr result;
    AuthMethod method = auth::tagToMethod(auth_tag);

    // The auth_tag was provided by the user, so in general we don't
    // trust it.  The exceptions are the cluster-internal auth methods
    // AUTH_I2I and AUTH_MPI, but if client wants to use one of those
    // then they'll have to pass the InstanceAuthenticator handshake.
    // Otherwise clients get the Authenticator for the currently
    // configured security mode no matter what they asked for.

    if (method == AUTH_I2I || method == AUTH_MPI) {
        result = make_shared<InstanceAuthenticator>(user, method);
    }
    else {
        method = getRequiredAuthMethod();
        SCIDB_ASSERT(method != AUTH_I2I);
        SCIDB_ASSERT(method != AUTH_MPI);
        SCIDB_ASSERT(method != AUTH_NONE);

        if (method == AUTH_TRUST) {
            result = make_shared<TrustAuthenticator>(user);
        }
        else if (method == AUTH_RAW || method == AUTH_PAM) {
            // Call out to plugin to get required authenticator.
            result.reset(NsComm::makeAuthenticator(user, method));
        }
        else {
            stringstream ss;
            ss << "Requiring unimplemented auth method \"" << auth_tag
               << "\" (" << int(method) << ") ?!";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_AUTHENTICATION_ERROR)
                << ss.str();
        }
    }

    if (!result) {
       stringstream ss;
       ss << "Unknown authentication method \"" << auth_tag << '"';
       throw USER_EXCEPTION(SCIDB_SE_AUTHENTICATION, SCIDB_LE_AUTHENTICATION_ERROR)
           << ss.str();
    }

    result->_method = method;
    result->_conn = conn.get();
    conn->setAuthenticator(result);
    return result;
}


Authenticator::Cookie Authenticator::makeCookie()
{
    static thread_local random_device r;
    _cookie = r();
    return _cookie;
}


void Authenticator::installSession()
{
    switch (_method) {
    case AUTH_TRUST:
    case AUTH_I2I:
    case AUTH_MPI:
        _userDesc.setName(rbac::DBA_USER);
        _userDesc.setId(rbac::DBA_ID);
        break;
    default:
        ASSERT_EXCEPTION(!_userDesc.getName().empty(),
                         "Should have a username by now");
        ASSERT_EXCEPTION(_userDesc.getId() != rbac::NOBODY,
                         "Should have a userid by now");
    }

    shared_ptr<Session> sess = make_shared<Session>(_userDesc,
                                                    _conn->getSessionPriority(),
                                                    isPeerToPeer());

    // If ADMIN priority was requested, verify authorization.  Only
    // operator role or better can use high priority execution queues.
    if (_conn->getSessionPriority() > SessionProperties::NORMAL) {
        rbac::RightsMap rights;
        rights.upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
        NsComm::checkAccess(sess.get(), &rights);
    }

    // Let subclasses see/modify the Session object.
    openSession(*sess);

    LOG4CXX_DEBUG(logger, "dz Authenticator set session, conn is " << std::hex << _conn << std::dec); // dz 加日志
    _conn->setSession(sess);
}


string Authenticator::getRemoteHost() const
{
    SCIDB_ASSERT(_conn);
    string endpt(_conn->getRemoteEndpointName());
    string::size_type colon = endpt.find(':');
    return colon == string::npos ? endpt : endpt.substr(0, colon);
}


void Authenticator::setReason(string const& r)
{
    if (_reason.empty()) {
        _reason = r;
    } else {
        _reason += '\n';
        _reason += r;
    }
}


string Authenticator::getFailureReason() const
{
    static string const GENERIC_DENIED_REASON("Access denied");
    return _reason.empty()
        ? GENERIC_DENIED_REASON
        : GENERIC_DENIED_REASON + '\n' + _reason;
}


string const& Authenticator::getRemark() const
{
    return _reason;
}

} // namespace
