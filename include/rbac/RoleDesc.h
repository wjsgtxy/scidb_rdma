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
 * @file RoleDesc.h
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a role.
 *
 */

#ifndef _ROLE_DESC_H_
#define _ROLE_DESC_H_

#include <cstdint>
#include <string>
#include <vector>

namespace scidb
{
    /**
     * Contains role specific information
     */
    class RoleDesc
    {
    public:
        typedef uint64_t ID;  // Note:  0 is an invalid ID

        /// Well-known role names.
        /// @{
        static constexpr char const * const ADMIN_ROLE = "admin";
        static constexpr char const * const OPS_ROLE = "operator";
        /// @}

        /**
         * @brief Default constructor
         */
        RoleDesc() : _id(0UL) {};

        /**
         * @brief Constructor given a reference name
         * param[in] rName  the name representing this role
         * param[in] id     the numeric id of this role, if known
         */
        RoleDesc(const std::string &rName, ID id = 0UL)
            : _name(rName)
            , _id(id)
        {}

        /**
         * @brief Retrieve the name of the role
         * @returns - the name of the role
         */
        const std::string &getName() const
        {
            return _name;
        }

        /**
         * @brief Sets the name of the role
         * param[in] rName - the name representing this role
         */
        void setName(const std::string &name)
        {
            _name = name;
        }

        ID getId() const { return _id; }
        void setId(ID x) { _id = x; }

    private:
        std::string     _name;
        ID              _id;
    };

} // namespace scidb

#endif /* _ROLE_DESC_H_ */
