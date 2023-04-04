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
 * @file Authenticator.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef AUTHENTICATOR_H
#define AUTHENTICATOR_H

#include <system/Auth.h>
#include <rbac/UserDesc.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>

namespace scidb {

class Authenticator;
class Connection;
class Session;

typedef std::shared_ptr<Authenticator> AuthenticatorPtr;

/**
 * @brief Base class for encapsulated server-side authentication logic.
 *
 * @description
 * Authentication mechanisms are hidden behind this interface, so that
 * they need not be aware of the details of message exchange.
 *
 * @p On receipt of an AuthLogon message from some remote entity, the
 * instance creates an object of some Authenticator subclass, extracts
 * a challenge from that authenticator (getChallenge), and transmits
 * the challenge to the remote entity.  When the entity responds, the
 * message handling layer gives the response to the authenticator and
 * asks whether the user is authenticated or not (putResponse).  There
 * are three possibilities: ALLOW or DENY terminate the authentication
 * handshake as you'd expect, while CHALLENGE indicates that the auth
 * machinery wants to query the remote entity again with another
 * challenge.
 *
 * @p The NetworkManager is always posting new reads on the Connection
 * socket, so the authentication handshake needs to be serialized to
 * guard against hostile clients jamming junk down the pipe.  This is
 * done in two ways:
 *
 *   1. We atomically insert the Authenticator into the Connection
 *      object.  If there's already one there, the client has broken
 *      the handshake protocol.
 *
 *   2. Every challenge contains a random cookie.  The client can't
 *      know the right cookie to echo in the response until it sees
 *      the challenge, so a response with a wrong cookie means a
 *      handshake error.
 */
class Authenticator : public boost::noncopyable
{
public:

    typedef uint32_t Cookie;

    struct Challenge {
        int32_t     code;       // AuthMethod-specific challenge type
        std::string text;       // Text of challenge
    };

    enum Status {
        DENY,                   // Access denied, user not authenticated
        ALLOW,                  // User successfully authenticated
        CHALLENGE               // I have another question to ask the user
    };

    /// @brief Connecting user requests authentication with particular method.
    ///
    /// @param authmethod hint to request particular authentication method
    /// @param user remote entity requesting authentication
    /// @param conn connection to hold session once authenticated
    /// @return Server-side object to perform authentication handshake.
    ///
    /// @note By setting 'authmethod', the client doesn't actually get to
    ///       choose the authentication scheme.  This is just a way to
    ///       indicate client-to-instance vs. instance-to-instance
    ///       authentication (they are usually different).  Also,
    ///       letting connecting entities hint at the desired protocol
    ///       lets us test new authentication methods during
    ///       development.
    ///
    static AuthenticatorPtr create(std::string const& authmethod,
                                   std::string const& user,
                                   std::shared_ptr<Connection>& conn);

    virtual ~Authenticator() {}

    /// @brief Extract next challenge for this user.
    virtual Challenge getChallenge() = 0;

    /// @brief Inject challenge response, and get resulting status.
    virtual Status putResponse(std::string const& response) = 0;

    /// @brief Is this an instance-to-instance (AUTH_I2I) authenticator?
    /// @note Message handling needs to know.
    virtual bool isPeerToPeer() const { return false; }

    /// @brief Generate cookie for challenge messages, and store it for later.
    Cookie makeCookie();

    /// @brief Get current cookie to check response messages.
    Cookie getCookie() const { return _cookie; }

    /// @brief Return authentication method actually in use.
    AuthMethod getAuthMethod() const { return _method; }

    /// @brief Auth succeeded, so install a Session object in the Connection.
    /// @throw UserException on error.  One possible reason: client
    ///        requested an ADMIN priority connection, but now we find
    ///        that s/he is not authorized to have one.
    /// @note Installing the session clears the Authenticator out of
    ///       the connection, so the caller of this method is holding
    ///       the last reference to it.
    void installSession();

    /// @brief Retrieve failure reason.
    /// @details On authentication failure, use this to extract any
    ///          optionally supplied reason for the failure.
    /// @see getRemark
    std::string getFailureReason() const;

    /// @brief Retrieve optional additional information on success.
    /// @details On authentication success, use this to extract any
    ///          optionally supplied additional information, for
    ///          example "Account expires in <N> days".
    /// @see getFailureReason
    std::string const& getRemark() const;

    /// @brief Retrieve username.
    std::string const& getUser() const { return _userDesc.getName(); }

    /// @brief What AuthMethod does configured security mode require?
    static AuthMethod getRequiredAuthMethod();

protected:
    Authenticator(std::string const& user)
        : _conn(nullptr)
        , _userDesc(user)
        , _cookie(0)
        , _method(AUTH_NONE)
    {}

    /// @brief Let subclasses modify the post-authentication session object.
    virtual void openSession(Session& sess) {}

    /// @brief Set reason, typically for returning a DENY.
    /// @note This string will be user-visible, so be circumspect: do
    ///       not give guessing attacks more information than they
    ///       should have.
    void setReason(std::string const& r);

    UserDesc& getUserDesc() { return _userDesc; }

    // Subclasses in P4 can't #include Connection.h, so add accessors
    // here for whatever's needed.
    std::string getRemoteHost() const;

private:
    Connection*     _conn;          // Weak backpointer to owning Connection
    UserDesc        _userDesc;      // Subject we want to authenticate
    Cookie          _cookie;        // Current random challenge cookie
    AuthMethod      _method;        // Method code, set by create()
    std::string     _reason;        // Reason for failure
};

} // namespace

#endif /* ! AUTHENTICATOR_H */
