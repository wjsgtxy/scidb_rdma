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
 * @file Rbac.h
 * @brief Role-based access control definitions.
 *
 * @note We do not have a complete RBAC API yet.  Functions defined
 *       here are a starting point.
 */

#ifndef RBAC_H
#define RBAC_H

#include <inttypes.h>
#include <string>
#include <vector>

namespace scidb {

class UserDesc;

namespace rbac {

/// @brief Metadata sequence number, used for roles, users, namespaces, ....
typedef uint64_t ID;

/// @brief Well-known name and id of the database administrator.
/// @{
ID const DBA_ID = 1ULL;
constexpr char const * const DBA_USER = "scidbadmin";
constexpr char const * const DEPRECATED_DBA_USER = "root";
/// @}

/// @brief Well-known id and name of nobody in particular.
/// @note This is the default value for unspecified rbac::IDs.
constexpr char const * const NOBODY_USER = "nobody";
ID const NOBODY = 0ULL;

/// @brief Well-known namespace names and ids.
/// @{
ID const INVALID_NS_ID = 0ULL;

constexpr char const * const PUBLIC_NS_NAME = "public";
ID const PUBLIC_NS_ID = 1ULL;

constexpr char const * const ALL_NS_NAME = "all";
ID const ALL_NS_ID = ~0ULL;
/// @}

/// @brief user descriptors for all registered users.
/// @note Formerly security::Communicator::getUsers()
void listUsers(std::vector<UserDesc>&);

/// @brief Look up user by name or by id (depending on what's set in UserDesc).
/// @return true iff user was found
bool findUser(UserDesc&);

/// @brief True iff 's' is a valid AFL identifier.
/// @param s string to test
///
/// @details Certain security entities (namespaces, roles, etc.)
/// should have been AFL identifiers all along.  Until the namespaces
/// API is revised, this manual check will have to suffice.
///
/// @note Also enforces maximum length of 256.
/// @see src/query/parser/Lexer.ll
///
bool isAflIdentifier(std::string const& s);

/// @brief True iff 's' uses only the base64 character set.
/// @param s string to test
///
bool isBase64Alphabet(std::string const& s);

} } // namespaces

#endif /* ! RBAC_H */
