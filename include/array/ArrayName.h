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
 * @file ArrayName.h
 * @brief Array name manipulation functions transplanted from ArrayDesc.
 */

#ifndef ARRAY_NAME_H
#define ARRAY_NAME_H

#include <array/VersionID.h>

#include <string>

namespace scidb {

/**
 * Given a fully qualified array name of the form "namespace_name.array_name"
 * split it into its constituent parts.  If the qualifiedArrayName is not qualified
 * then namespaceName will not be modified.
 *
 * @param[in] qualifiedArrayName    Potentially qualified array name of the form
 *                                  "namespace_name.array_name"
 * @param[out] namespaceName        If qualifiedArrayName is qualified, 'namespace_name' from
 *                                  qualifiedArrayName.  Otherwise, not modified
 * @param[out] arrayName            'array_name' portion of the fullyQualifiedArrayName
 */
void splitQualifiedArrayName(const std::string& qualifiedArrayName,
                             std::string& namespaceName,
                             std::string& arrayName);

/**
 * Given a potentially fully qualified array name of the form "namespace_name.array_name"
 * retrieve the arrayName portion.
 *
 * @param[in] arrayName A potentially fully qualified array name
 * @return The unqualified portion of the array name
 */
std::string getUnqualifiedArrayName(const std::string& arrayName);

/**
 * Make a fully qualified array name from a namespace name and an array name
 * If arrayName is already fully-qualified it is returned without modification
 * @param namespaceName The name of the namespace to use in the fully-qualified name
 * @param arrayName The name of the array to use in the fully-qualified name
 */
std::string makeQualifiedArrayName(const std::string& namespaceName,
                                   const std::string& arrayName);

/**
 * Determine if an array name is qualified or not
 * @return true qualified, false otherwise
 */
bool isQualifiedArrayName(const std::string& arrayName);

/**
 * Find out if an array name is for a versioned array.
 * In our current naming scheme, in order to be versioned, the name
 * must contain the "@" symbol, as in "myarray@3". However, NID
 * array names have the form "myarray@3:dimension1" and those arrays
 * are actually NOT versioned.
 * @param[in] name the name to check. A nonempty string.
 * @return true if name contains '@' at position 1 or greater and does not contain ':'.
 *         false otherwise.
 */
bool isNameVersioned(std::string const& name);

/**
 * Find out if an array name is for an unversioned array - not a NID and not a version.
 * @param[in] the name to check. A nonempty string.
 * @return true if the name contains neither ':' nor '@'. False otherwise.
 */
bool isNameUnversioned(std::string const& name);

/**
 * Given the versioned array name, extract the corresponing name for the unversioned array.
 * In other words, compute the name of the "parent" array. Or, simply put, given "foo@3" produce "foo".
 * @param[in] name
 * @return a substring of name up to and excluding '@', if isNameVersioned(name) is true.
 *         name otherwise.
 */
std::string makeUnversionedName(std::string const& name);

/**
 * Given the versioned array name, extract the version id.
 * Or, simply put, given "foo@3" produce 3.
 * @param[in] name
 * @return a substring of name after and excluding '@', converted to a VersionID, if
 *         isVersionedName(name) is true.
 *         0 otherwise.
 */
VersionID getVersionFromName(std::string const& name);

/**
 * Given an unversioned array name and a version ID, stitch the two together.
 * In other words, given "foo", 3 produce "foo@3".
 * @param[in] name must be a nonempty unversioned name
 * @param[in] version the version number
 * @return the concatenation of name, "@" and version
 */
std::string makeVersionedName(std::string const& name, VersionID const version);

} // namespaces

#endif /* ! ARRAY_NAME_H */
