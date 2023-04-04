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
 * @file InstanceAuthenticator.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef INSTANCE_AUTHENTICATOR_H
#define INSTANCE_AUTHENTICATOR_H

#include <rbac/Authenticator.h>

namespace scidb {

/**
 * @brief Authenticator for instance-to-instance and MPI connections.
 *
 * @note Both AUTH_I2I and AUTH_MPI use this Authenticator, but the
 *       message handling will differ depending on which.  This is the
 *       only Authenticator subclass that overrides isPeerToPeer().
 */
class InstanceAuthenticator : public Authenticator
{
public:

    InstanceAuthenticator(std::string const& user,
                          AuthMethod method)
        : Authenticator(user)
        , _method(method)
    {}

    /// @brief Is this an instance-to-instance (AUTH_I2I) authenticator?
    bool isPeerToPeer() const override { return _method == AUTH_I2I; }

    /// @brief Extract next challenge for this user.
    Challenge getChallenge() override;

    /// @brief Inject challenge response, and get resulting status.
    Status putResponse(std::string const& response) override;

    /// @brief Compute response to an instance-to-instance challenge.
    static std::string getResponse(std::string const& challenge);

private:
    AuthMethod  _method;
};

} // namespace

#endif /* ! INSTANCE_AUTHENTICATOR_H */
