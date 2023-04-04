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
 * @file NamespaceDesc.h
 * @author mcorbett@paradigm.com
 * @brief A class for describing a namespace.
 */

#ifndef NAMESPACE_DESC_H_
#define NAMESPACE_DESC_H_

#include <rbac/Rbac.h>
#include <string>

namespace scidb
{
    class NamespaceDesc
    {
    public:
        /**
         * @brief Default constructor
         */
        NamespaceDesc()
            : _name(rbac::PUBLIC_NS_NAME)
            , _id(rbac::PUBLIC_NS_ID)
        { }

        /**
         * @brief Constructor given a reference name
         * param[in] rName - the name representing this namespace
         */
        NamespaceDesc(const std::string &name)
            : _name(name)
            , _id(rbac::INVALID_NS_ID)
        {
            if (name == rbac::PUBLIC_NS_NAME) {
                _id = rbac::PUBLIC_NS_ID;
            }
        }

        /**
         * @brief Constructor given a reference name and id
         * param[in] rName - the name representing this namespace
         * param[in] id - the id representing this namespace
         */
        NamespaceDesc(const std::string & name, rbac::ID id)
            : _name(name)
            , _id(id)
        {
            if (name == rbac::PUBLIC_NS_NAME) {
                _id = rbac::PUBLIC_NS_ID;
            }
        }

        /**
         * Determine if the namespace Id is valid.
         */
        inline bool isIdValid() const
        {
            return _id != rbac::INVALID_NS_ID
                && _id != rbac::ALL_NS_ID;
        }

        /**
         * @brief Retrieve the name of the namespace
         * @returns - the name of the namespace
         */
        inline const std::string &getName() const
        {
            return _name;
        }

        /**
         * @brief Sets the name of the namespace
         * param[in] rName - the name representing this namespace
         */
        void setName(const std::string &name)
        {
            _name = name;
        }

        /**
         * @brief Get the id for the namespace
         */
        inline rbac::ID getId() const
        {
            return _id;
        }

        /**
         * @brief Set the id for the namespace
         * @param[in] - The id for the namespace
         */
        inline void setId(rbac::ID id)
        {
            _id = id;
        }

    private:
        std::string _name;
        rbac::ID    _id;
    };

} // namespace scidb

#endif /* NAMESPACE_DESC_H_ */
