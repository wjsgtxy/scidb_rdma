/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file Credential.h
 * @brief Username/password pair and related method.
 */

#ifndef CREDENTIAL_H
#define CREDENTIAL_H

#include <string>

namespace scidb
{

class Credential
{
    std::string _username;
    std::string _password;

public:

    /// Upcall to application: fill in this credential please.
    typedef void (*Callback)(Credential*, void*);

    // 'structors
    Credential() = default;
    Credential(std::string const& u);
    Credential(std::string const& u, std::string const& p);
    Credential(Credential const&) = default;
    Credential& operator= (Credential const&) = default;
    ~Credential();

    /**
     * @brief Read credentials from an authentication file.
     *
     * @details The file must be parseable as either JSON or INI, and
     * not have any 'group' or 'other' permissions.  The file may
     * specify both username and password or only a username, but lone
     * passwords are not allowed.
     *
     * @p Empty passwords are considered OK because maybe we'll get
     * one later via a Credential::Callback.
     *
     * @throw USER_EXCEPTION for parse errors, file problems, invalid values.
     */
    void fromAuthFile(std::string const& fileName);

    /**
     * @brief Return true iff argument is a valid SciDB username.
     * @details A valid username:
     * - must not be empty
     * - must not contain whitespace or control characters
     * - must begin with an alphabetic character
     * - must be ASCII-only
     * - must be shorter than LOGIN_NAME_MAX (256) bytes
     * - only underscore (_), hyphen (-), at-sign (@), and period (.) are allowed as punctuation
     * - must not end with punctuation
     */
    static bool isValidUsername(std::string const& u);

    /**
     * @brief Return true iff argument is an acceptable SciDB password.
     * @details A valid password:
     * - may be empty (presumably this is a temporary condition)
     * - must not contain control characters
     * - must be ASCII-only
     * - must be shorter than LOGIN_NAME_MAX (256) bytes
     */
    static bool isValidPassword(std::string const& p);

    /** Return true iff username and password are valid. */
    bool isValid() const
    {
        return isValidUsername(_username) && isValidPassword(_password);
    }

    std::string const& getUsername() const { return _username; }
    std::string const& getPassword() const { return _password; }

    void setUsername(std::string const& u);
    void setUsername(char const* u)
    {
        if (u) {
            setUsername(std::string(u));
        } else {
            _username.clear();
        }
    }

    void setPassword(std::string const& p);
    void setPassword(char const* p)
    {
        if (p) {
            setPassword(std::string(p));
        } else {
            _password.clear();
        }
    }
};

} // namespace scidb

#endif /* ! CREDENTIAL_H */
