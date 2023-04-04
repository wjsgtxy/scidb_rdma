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

/*
 * NamespacesCommunicator.h
 *
 *  Created on: May 8, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef NAMESPACE_PLUGIN_COMMUNICATOR_H_
#define NAMESPACE_PLUGIN_COMMUNICATOR_H_

#include <rbac/RoleDesc.h>
#include <system/Auth.h>
#include <system/SystemCatalog.h>

#include <pqxx/transaction>

#include <memory>
#include <string>

namespace scidb
{
    class Authenticator;
    class NamespaceDesc;
    class Session;
    namespace rbac { class RightsMap; }

    typedef uint64_t ArrayID;

    namespace namespaces
    {
        /**
         * Communication interface between the namespaces plugin and SciDB.
         */
        class Communicator
        {
        private:

        // -------------------------------------------------------------
        public:  // Methods

            /**
             * Constructor
             */
            Communicator() { }

            /**
             * @brief Return server-side authenticator for auth method.
             */
            static scidb::Authenticator* makeAuthenticator(
                std::string const& username,
                scidb::AuthMethod method);

            /**
             * Find array and, if found, fill in its label ArrayIDs.
             *
             * @param[in] args a (namespace, arrayname, version, catalogVersion) tuple
             * @param[out] result array id, dist id, and flags from array table
             * @param[in,out] tr Postgres transaction pointer
             * @return true iff described array was found
             *
             * @description
             * Given namespace name, array name, and relevant
             * versions, try to find the corresponding array.
             *
             * On success, all result fields will be non-zero with the
             * possible exception of flags.  On failure, all result
             * fields are set to zero (invalid foo id).
             *
             * The catalogVersion input field serves as an upper bound
             * on the resolved value of arrayId.getAid().  A suitable
             * parameter value can be obtained from Query::getCatalogVersion().
             */
            static bool findArray(
                SystemCatalog::FindArrayArgs const& args,
                SystemCatalog::FindArrayResult& result,
                pqxx::basic_transaction* tr);

            /**
             * Map array id to namespace descriptor.
             *
             * @param[in] arrayId array id
             * @param[out] nsDesc name and id of containing namespace
             * @param[in,out] tr Postgres transaction pointer
             * @throws SCIDB_LE_ARRAYID_DOESNT_EXIST if id not found
             * @throws SCIDB_LE_PLUGIN_FUNCTION_ACCESS if plugin method missing
             */
            static void findNamespaceForArray(
                ArrayID arrayId,
                NamespaceDesc& nsDesc,
                pqxx::basic_transaction* tr);

            /**
             * Map a namespace name to an id, or -vice versa-.
             *
             * @param namespaceDesc - holds the name of the namespace and receives the id
             * @param byName - if true map name-to-id, else map id-to-name
             * @param tr - the transaction to use
             * @return true iff name (or id) was found and mapped
             */
            static bool findNamespaceWithTransaction(
                NamespaceDesc &             namespaceDesc,
                bool                        byName,
                pqxx::basic_transaction *   tr);

            /**
             * @brief Perform authorization/access checks.
             *
             * @param pSession pointer to Session
             * @param pRights pointer to RightsMap of needed rights
             * @throw PLUGIN_USER_EXCEPTION if access denied
             * @note This is a no-op in AUTH_TRUST mode.
             */
            static void checkAccess(Session const* pSession,
                                    rbac::RightsMap const* pRights);

            /**
             * List arrays in given namespace.
             *
             * @param[in,out] nsDesc namespace descriptor with name or id set
             * @param[out] arrayIds unversioned array ids of namespace members
             * @param[in] ignoreVersions return only unversioned array ids
             * @param[in] orderByName order results by name (else by ArrayID)
             * @param[in,out] tr Postgres transaction pointer
             * @throws SCIDB_LE_CANNOT_RESOLVE_NAMESPACE if no such namespace
             * @throws SCIDB_LE_PLUGIN_FUNCTION_ACCESS if plugin method missing
             * 
             * @description If nsDesc.getId() is not INVALID_NS_ID, it
             * is assumed to be correct.  Otherwise the name from
             * nsDesc is looked up and used to dump the relevant
             * namespace_members table entries, and nsDesc is updated
             * with the correct id.
             */
            static void listArrayIds(
                NamespaceDesc& nsDesc,
                std::vector<scidb::ArrayID>& arrayIds,
                bool ignoreVersions,
                bool orderByName,
                pqxx::basic_transaction* tr);

            /**
             * List array IDs in all namespaces.
             *
             * @param[out] arrayIds unversioned array ids of namespace members
             * @param[in] ignoreVersions return only unversioned array ids
             * @param[in] orderByName sort output by (nsName, arrayName), or
             *            if false, by array id
             * @param[in,out] tr Postgres transaction pointer
             * @throws SCIDB_LE_PLUGIN_FUNCTION_ACCESS if plugin method missing
             */
            static void listAllArrayIds(
                std::vector<scidb::ArrayID>& arrayIds,
                bool ignoreVersions,
                bool orderByName,
                pqxx::basic_transaction* tr);

            /**
             * List namespaces.
             *
             * @param[out] nsVec contents of namespaces table
             * @param[in,out] tr Postgres transaction pointer
             * @throws SCIDB_LE_PLUGIN_FUNCTION_ACCESS if plugin method missing
             */
            static void getNamespacesWithTransaction(
                std::vector<NamespaceDesc>& nsVec,
                pqxx::basic_transaction* tr);

            /**
             * Retrieves a vector of role descriptors
             * @param roleDescs - the vector of roles to be returned
             */
            static bool getRoles(
                std::vector<scidb::RoleDesc> &  roleDescs);

            /**
             * Add an array to a given namespace
             *
             * @param nsDesc - target namespace name or id
             * @param UAId - the unversioned id of the array to add to the namespace
             * @param tr - the transaction to use
             * @throws SCIDB_LE_PLUGIN_FUNCTION_ACCESS if plugin method missing
             *
             * @description Uses the namespace id from nsDesc if it is
             * valid, otherwise looks up the namespace name and uses
             * the result.
             */
            static void addArrayToNamespaceWithTransaction(
                const NamespaceDesc &       nsDesc,
                const ArrayID               UAId,
                pqxx::basic_transaction *   tr);

        };
    } // namespace namespaces
} // namespace scidb

#endif // NAMESPACE_PLUGIN_COMMUNICATOR_H_
