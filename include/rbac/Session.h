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
 * @file Session.h
 */

#ifndef SESSION_H_
#define SESSION_H_

#include <rbac/NamespaceDesc.h>
#include <rbac/Rights.h>
#include <rbac/UserDesc.h>

namespace scidb {

/**
 *  The Session object is stored in an authenticated Connection.
 *  It contains authorization credentials and other context for
 *  the authenticated remote client or SciDB peer.
 */
class Session
{
public:

    /// Asynchronous cleanup action.
    typedef std::function<void()> Callback;

    Session(UserDesc const& user, int channelPriority, bool peer);
    ~Session();

    /// Get/set session channel priority
    /// @see SessionProperties
    /// @{
    int getPriority() const { return _priority; }
    void setPriority(int p) { _priority = p; }
    /// @}

    /// True iff remote endpoint is an instance peer.
    bool remoteIsPeer() const { return _isPeer; }

    /// True iff remote endpoint is a client application.
    bool remoteIsClient() const { return !_isPeer; }

    /// Get/set current namespace
    /// @{
    NamespaceDesc const& getNamespace() const { return _currentNs; }
    void setNamespace(const NamespaceDesc &nsDesc) { _currentNs = nsDesc; }
    /// @}

    /// Get authenticated user
    const scidb::UserDesc &getUser() const { return _userDesc; }

    /// Set functor to run upon Session destruction, returning previous value.
    /// @note cb is now owned and will be deleted after it is called
    Callback swapCleanup(Callback cb)
    {
        Callback prev = _onClose;
        _onClose = cb;
        return prev;
    }

    /// Return true iff work to do upon close.
    bool hasCleanup() const { return bool(_onClose); }

    /// Get/set access control privileges
    /// @{
    rbac::RightsMap* getRights() const { return _rights.get(); }
    void setRights(rbac::RightsMap& r) const
    {
        // Cannot update existing rights: user has to log on again to
        // get a refresh.
        SCIDB_ASSERT(!_rights);
        _rights.reset(new rbac::RightsMap(r));
    }
    /// @}

    /// Encode as JSON for transmission to workers.
    std::string toJson() const;

    /// Decode Session object from JSON string.
    static std::shared_ptr<Session> fromJson(std::string const&);

private:
    NamespaceDesc           _currentNs;
    UserDesc                _userDesc;
    int                     _priority;
    bool                    _isPeer;
    Callback                _onClose;
    mutable std::unique_ptr<rbac::RightsMap> _rights;
};

}  // namespace scidb

#endif /* ! SESSION_H_ */
