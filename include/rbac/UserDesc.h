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
 * @file UserDesc.h
 * @brief Class to hold user name/id pairs and Postgres 'users' table entries.
 */

#ifndef USER_DESC_H_
#define USER_DESC_H_

#include <rbac/Rbac.h>
#include <util/safebuf.h>

namespace scidb
{
    class UserDesc
    {
    public:
        // 'structors
        UserDesc() : _id(rbac::NOBODY) {}
        UserDesc(std::string const& nm, rbac::ID id = rbac::NOBODY)
            : _name(nm), _id(id)
        {
            // If we're somebody, we have a name.
            SCIDB_ASSERT(_id == rbac::NOBODY || !_name.empty());
        }

        /// @brief Setters 'n getters
        /// @{
        void setName(std::string const& nm) { _name = nm; }
        std::string const& getName() const { return _name; }
        void setId(rbac::ID id) { _id = id; }
        rbac::ID getId() const { return _id; }
        void setAuthTag(std::string const& m) { _authTag = m; }
        std::string const& getAuthTag() const { return _authTag; }
        /// @}

        /// Authentication token handling.  Non-const, caller uses
        /// safebuf interface directly.
        safebuf& getAuthToken() { return _authToken; }

        /// @return true iff this user is the DBA
        /// @note Sometimes only the _name is initialized.
        bool isDbAdmin() const
        {
            if (_id == rbac::DBA_ID ||
                (_id == rbac::NOBODY && _name == rbac::DBA_USER)) {
                return true;
            }
            return false;
        }

    private:
        std::string     _name;
        rbac::ID        _id;
        std::string     _authTag;
        safebuf         _authToken;
    };

} // namespace

#endif /* USER_DESC_H_ */
