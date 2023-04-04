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

#ifndef CLIENT_AUTH_H
#define CLIENT_AUTH_H

#include <util/safebuf.h>

namespace scidb {

class BaseConnection;
class SessionProperties;

/**
 * @brief Do client authentication based on desired session properties.
 */
class ClientAuthenticator
{
public:

    /**
     * @brief Create client-side authenticator based on properties.
     */
    static ClientAuthenticator* create(SessionProperties const& props);

    /** Destructor. */
    virtual ~ClientAuthenticator();

    /// @brief Authenticate the connection, or throw an Exception.
    virtual void authenticate(BaseConnection* conn) { fourWayHandshake(conn); }

protected:
    ClientAuthenticator(SessionProperties const& props);
    SessionProperties const& _props;

    /// @brief Canned messaging for logon/challenge/response/complete exchange.
    void fourWayHandshake(BaseConnection* conn);

    /**
     * @brief Customization point for auth method.
     *
     * @note By returning a safebuf, we have some assurance that the
     *       subclass author has given some thought to destroying (and
     *       not just releasing for heap garbage collection!)
     *       sensitive in-memory information.  Right?  Right.
     */
    virtual safebuf answerChallenge(int code, std::string const& text) = 0;
};

} // namespace

#endif /* ! CLIENT_AUTH_H */
