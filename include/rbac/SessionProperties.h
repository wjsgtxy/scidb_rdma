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

/**
 * @file SessionProperties.h
 */

#ifndef SESSION_PROPERTIES_H
#define SESSION_PROPERTIES_H

#include <rbac/Credential.h>
#include <system/Auth.h>

namespace scidb
{

/**
 * An abstraction for a set of properties that can be set for a client session
 */
class SessionProperties
{
public:

    enum {
        NORMAL = 0,             ///< Use normal channel priority
        ADMIN = 1,              ///< Request dedicated admin channel
    };

    static bool validPriority(int pri)
    {
        return pri == NORMAL || pri == ADMIN;
    }

    SessionProperties() = default;

    SessionProperties(const std::string &userName, const std::string &authTok)
        : _cred(userName, authTok)
    { }

    SessionProperties(Credential const& cred)
        : _cred(cred)
    { }

    /// Get/set priority for resource reservations
    /// @{
    int getPriority() const { return _priority; }
    void setPriority(int p) { _priority = p; }
    /// @}

    /// Get/set requested authentication method for session
    /// @{
    AuthMethod getAuthMethod() const { return _authMethod; }
    void setAuthMethod(AuthMethod m) { _authMethod = m; }
    /// @}

    /// Accessors for credential and credential callback.
    /// @{
    Credential& getCred() { return _cred; }
    Credential const& getCred() const { return _cred; }
    void setCred(Credential const& c) { _cred = c; }
    void setCredCallback(Credential::Callback cb, void* data = nullptr)
    {
        _cb = cb;
        _cbData = data;
    }
    /// @}

    /// Accessors for individual parts of credential.
    /// @{
    std::string const& getAuthToken(bool withCallback = false) const;
    void setAuthToken(std::string const& t) { _cred.setPassword(t); }
    std::string const& getUserName() const { return _cred.getUsername(); }
    void setUserName(std::string const& u) { _cred.setUsername(u); }
    /// @}

private:
    int                  _priority { NORMAL };
    AuthMethod           _authMethod { AUTH_NONE };
    mutable Credential   _cred;         // getAuthToken() callback can modify
    Credential::Callback _cb { nullptr };
    void*                _cbData { nullptr };
};


inline std::string const& SessionProperties::getAuthToken(bool withCb) const
{
    if (_cred.getPassword().empty() && withCb && _cb) {
        _cb(&_cred, _cbData);
    }
    return _cred.getPassword();
}

}  // namespace scidb

#endif // ! SESSION_PROPERTIES_H
