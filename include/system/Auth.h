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
 * @file Auth.h
 * @brief Common authentication definitions for both clients and servers.
 */

#ifndef AUTH_H
#define AUTH_H

#include <string>

namespace scidb {

/// @brief Numeric codes for supported authentication methods.
enum AuthMethod {
#define X(name, val, tag, desc)  name = val ,
#include <system/AuthMethods.inc>
#undef X
};

namespace auth {

/// @brief Translate authentication method value to name, tag, or description.
/// @{
const char *strMethodName(AuthMethod m);
const char *strMethodTag(AuthMethod m);
const char *strMethodDesc(AuthMethod m);
/// @}

/// @brief Translate tag to AuthMethod.
/// @{
AuthMethod tagToMethod(const char* tag);
AuthMethod tagToMethod(std::string const& tag);
/// @}

} } // namespaces

#endif /* ! AUTH_H */
