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
 * @file TrustAuthenticator.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef TRUST_AUTHENTICATOR_H
#define TRUST_AUTHENTICATOR_H

#include <rbac/Authenticator.h>

namespace scidb {

/**
 * @brief Authenticator for security=trust mode.
 */
class TrustAuthenticator : public Authenticator
{
public:
    TrustAuthenticator(std::string const& user)
        : Authenticator(user)
    {}

    /// @brief Extract next challenge for this user.
    Challenge getChallenge() override
    {
        Challenge ch;
        ch.code = AUTH_TRUST;
        ch.text = "What... is your quest?";
        return ch;
    }

    /// @brief Inject challenge response, and get resulting status.
    Status putResponse(std::string const& response) override
    {
        return ALLOW;
    }
};

} // namespace

#endif /* ! TRUST_AUTHENTICATOR_H */
