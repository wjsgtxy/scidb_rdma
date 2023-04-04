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
 * @file SystemCatalog.h
 * @brief API for fetching and updating system catalog metadata.
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SYSTEMCATALOG_H_
#define SYSTEMCATALOG_H_

#include <string>
#include <vector>
#include <map>
#include <list>
#include <assert.h>
#include <memory>
#include <pqxx/except>
#include <pqxx/transaction>

#include <query/TypeSystem.h>
#include <query/QueryID.h>
#include <array/ArrayDistributionInterface.h>
#include <array/Attributes.h>
#include <monitor/MonitorConfig.h>
#include <util/Singleton.h>
#include <system/Cluster.h>
#include <system/LockDesc.h>
#include <rbac/NamespaceDesc.h>


namespace pqxx
{
// forward declaration of pqxx::connection
    class connect_direct;
    template<typename T> class basic_connection;
    typedef basic_connection<connect_direct> connection;
    class basic_transaction;
}


namespace scidb
{
class Mutex;
class PhysicalBoundaries;
class VersionDesc;

void verifyLatestArrayVersionTable(pqxx::connection* connection,
                                   log4cxx::LoggerPtr logger);

/**
 * @brief Global object for accessing and manipulating cluster's metadata.
 *
 * On first access catalog object will be created and as result private constructor of SystemCatalog
 * will be called where connection to PostgreSQL will be created. After this
 * cluster can be initialized. Instance must add itself to SC or mark itself as online,
 * and it ready to work (though we must wait other instances online, this can be
 * implemented as PostgreSQL event which will be transparently transported to
 * NetworkManager through callback for example).
 *
 * @note for developers:
 *   If you want to change array_dimension or array_attribute tables,
 *   such as to add the capability to rename attributes or dimensions,
 *   you should make sure an array's dimension names and attribute names do not collide.
 *   @see Donghui Zhang's comment inside SystemCatalog::_addArray().
 *
 * @note IMPORTANT: The SystemCatalog API must remain useable even
 *       when there is no query context (for example, when rebuilding
 *       the catalog during disaster recovery).  Therefore methods
 *       that require array names ALSO REQUIRE NAMESPACE NAMES, since
 *       the "current namespace" can only be learned from query
 *       context, and this module is forbidden from looking at Query
 *       objects.
 */
class SystemCatalog : public Singleton<SystemCatalog>
{
public:

    /**
     * Log errors produced by sql
     */
    void _logSqlError(
        const std::string &t,
        const std::string &w);

    /**
     * Add the 'INVALID' flag to all array entries in the catalog currently
     * marked as being 'TRANSIENT'.
     */
    void invalidateTempArrays();

    /**
     * Rename old array (and all of its versions) to the new name
     * @param[in] ns_name namespace name
     * @param[in] old_array_name
     * @param[in] new array_name
     * @throws SystemException(SCIDB_LE_ARRAY_DOESNT_EXIST) if old_array_name does not exist
     * @throws SystemException(SCIDB_LE_ARRAY_ALREADY_EXISTS) if new_array_name already exists
     * @note Rename across namespaces is not currently supported,
     *       hence only one namespace parameter at this point.
     */
    void renameArray(const std::string& ns_name,
                     const std::string& old_array_name,
                     const std::string& new_array_name);

    /**
     * @throws a scidb::Exception if necessary
     */
    typedef std::function<bool()> ErrorChecker;

    /**
     * Acquire locks in the catalog for each lock in the container. On a coordinator the method will
     * block until the lock can be acquired.  On a worker instance, the lock will not be acquired
     * unless a corresponding coordinator lock exists.
     * @param[in] arrayLocks container of lock descriptors
     * @param[in] errorChecker that is allowed to interrupt the lock acquisition
     * @throw condition TBD
     */
    void lockArrays(const QueryLocks& arrayLocks, ErrorChecker& errorChecker);

    /**
     * Acquire a lock in the catalog. On a coordinator the method will block until the lock can be acquired.
     * On a worker instance, the lock will not be acquired unless a corresponding coordinator lock exists.
     * @param[in] lockDesc the lock descriptor
     * @param[in] errorChecker that is allowed to interrupt the lock acquisition
     * @return true if the lock was acquired, false otherwise
     */
    bool lockArray(const std::shared_ptr<LockDesc>&  lockDesc, ErrorChecker& errorChecker);

    /**
     * Release a lock in the catalog.
     * @param[in] lockDesc the lock descriptor
     * @return true if the lock was released, false if it did not exist
     */
    bool unlockArray(const std::shared_ptr<LockDesc>& lockDesc);

    /**
     * Update the lock with new fields. Array name, query ID, instance ID, instance role
     * cannot be updated after the lock acquisition.
     * @param[in] lockDesc the lock descriptor
     * @return true if the lock was released, false if it did not exist
     */
    bool updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc);

    /**
     * Get all arrays locks from the catalog for a given instance.
     * @param[in] instanceId
     * @param[in] coordLocks locks acquired as in the coordinator role
     * @param[in] workerLocks locks acquired as in the worker role
     */
    void readArrayLocks(const InstanceID instanceId,
            std::list< std::shared_ptr<LockDesc> >& coordLocks,
            std::list< std::shared_ptr<LockDesc> >& workerLocks);

    /**
     * Get the count of array locks from the catalog greater-than or
     * equal-to the given lock mode.
     * @param[in] lockMode The start of the lock mode range.  If the
     *   value of this parameter is LockDesc::LockMode::CRT, then all
     *   locks with mode equal-to or greater-than
     *   LockDesc::LockMode::CRT will be returned.
     * @return The number of array locks fitting the selection
     *   criterion given above.
     */
    uint64_t countLocksAtLeast(LockDesc::LockMode lockMode);

    /**
     * Create an instance of the global array lock and return it to
     * the caller.  This method does not modify the catalog.
     * @return An instance of the global array lock.
     */
    std::shared_ptr<LockDesc> createGlobalArrayLock() const;

    /**
     * Delete all arrays locks created as coordinator from the catalog on a given instance.
     * @param[in] instanceId
     * @return number of locks deleted
     */
    uint32_t deleteCoordArrayLocks(InstanceID instanceId);

    /**
     * Delete all arrays locks created as coordinator from the catalog for a given query on a given instance.
     * @param[in] instanceId
     * @param[in] queryId
     * @return number of locks deleted
     */
    uint32_t deleteWorkerArrayLocks(InstanceID instanceId);

    /**
     * Delete all arrays locks from the catalog for a given query on a given instance, and role
     * @param[in] instanceId
     * @param[in] queryId
     * @param[in] instance role (coord or worker), if equals LockDesc::INVALID_ROLE, it is ignored
     * @return number of locks deleted
     */
    uint32_t deleteArrayLocks(InstanceID instanceId, const QueryID& queryId,
                              LockDesc::InstanceRole role = LockDesc::INVALID_ROLE);

    /**
     * Check if a coordinator lock for given array name and query ID exists in the catalog
     * @param[in] namespaceName The namespace the array belongs to
     * @param[in] arrayName
     * @param[in] queryId
     * @return the lock found in the catalog possibly empty
     */
    std::shared_ptr<LockDesc> checkForCoordinatorLock(
        const std::string& namespaceName,
        const std::string& arrayName,
        const QueryID& queryId);

    /**
     * Populate PostgreSQL database with metadata, generate cluster UUID and return
     * it as result
     *
     * @return Cluster UUID
     */
    const std::string& initializeCluster();

    /**
     * @return is cluster ready to work?
     */
    bool isInitialized() const;


    /**
     * @return UUID if cluster initialized else - void string
     */
    const std::string& getClusterUuid() const;

    /**
     * @note IMPORTANT: Array updates must not use this interface, @see SystemCatalog::addArrayVersion().
     * Add new array to the catalog by descriptor.
     * @param[in] array_desc fully populated descriptor
     */
    void addArray(const ArrayDesc &array_desc);

    /**
     * Transactionally add a new array version.
     * Basically, this is how all array updates become visible to other queries.
     * @param unversionedDesc schema for the unversioned array if not already in the catalog, NULL if it is
     *        Must have the array ID already filled in.
     * @param versionedDesc schema for the new versioned array to be added
     *        Must have the array ID already filled in.
     * @see scidb::SystemCatalog::getNextArrayId()
     * @throws scidb::SystemException if the catalog state is not consistent with this operation
     */
    void addArrayVersion(const ArrayDesc* unversionedDesc,
                         const ArrayDesc& versionedDesc);

    /**
     * Fills vector with array schemas in the given namespace.
     *
     * @param[in] nsName Namespace to list.  If empty, list all namespaces.
     * @param[in] ignoreOrphanAttributes whether to ignore attributes whose UDT/UDF are not available
     * @param[in] ignoreVersions whether to ignore version arrays (i.e. of the name <name>@<version>)
     * @param[in] orderByName order results by qualified array name (else order by array id)
     * @param[out] arrayDescs vector of ArrayDesc objects
     * @throws scidb::SystemException on error
     */
    void getArrays(const std::string& nsName,
                   std::vector<ArrayDesc>& arrayDescs,
                   bool ignoreOrphanAttributes,
                   bool ignoreVersions,
                   bool orderByName);

    /**
     * Retrieve the id of the array in the catalog.
     *
     * @param[in] nsName namespace name
     * @param[in] arrayName array name
     * @return if the array exists the id is returned, otherwise INVALID_ARRAY_ID is returned
     */
    ArrayID getArrayId(const std::string &nsName,
                       const std::string &arrayName);

    /**
     * Checks if there is array with specified name in the storage.
     *
     * @note Use with CAUTION, the result is not constrained by a "catalogVersion".
     *       The array may come and go in the course of a query execution.
     *       Use array locks appropriately to guarantee the correct transactional behavior.
     * @param[in] nsName namespace name
     * @param[in] arrayName array name
     * @return true if there is array with such name in the storage, false otherwise
     */
    bool containsArray(const std::string &nsName,
                       const std::string &arrayName);

    /**
     * @brief Handle a request to the catalog (usually from a plugin).
     *
     * @param action function to be performed via a transaction
     * @param serialize if true then serialize the sql transaction
     *
     * @description
     * Your component needs to access the catalog, but you do not have
     * a transaction object.  Write an Action functor with your SQL
     * code, and hand it to this method.  The SystemCatalog will
     * create a transaction for you and handle the serialization and
     * retry logic.
     *
     * Some subsystems (typically plugins but not necessarily) need to
     * run specialized SQL against (non-array-related!) tables in the
     * system catalog's underlying Postgres database.  This method
     * allows them to execute functors using SystemCatalog's framework
     * for query serialization and retry.  ("Non-array-related"
     * because ALL array-related queries MUST go through this
     * SystemCatalog API: the SystemCatalog is the Single Point Of
     * Truth for all matters concerning arrays and their schemas.)
     *
     * @note IMPORTANT: No plugin should use this method to circumvent
     * access to tables ordinarily managed by the SciDB core, such as the
     * array table, array_* tables, etc.
     */
    typedef std::function<void(pqxx::basic_transaction *)> Action;
    void execute(Action& action, bool serialize = true);

    /**
     * Given a NamespaceDesc with a namespace id, set the namespace name.
     * @param[in,out] nsDesc namespace descriptor
     * @return true iff namespace name was found
     * @throws scidb::SystemException on unrecoverable database errors
     * @note If the namespace library would be needed to translate an
     *       id but it is not loaded, we return false rather than
     *       throw the usual plugin function access exception.
     */
    bool findNamespaceById(NamespaceDesc& nsDesc);

    /**
     * Given a NamespaceDesc with a namespace name, set the namespace id.
     * @param[in,out] nsDesc namespace descriptor
     * @return true iff namespace id was found
     * @throws scidb::SystemException on unrecoverable database errors
     * @note If the namespace library would be needed to translate a
     *       name but it is not loaded, we return false rather than
     *       throw the usual plugin function access exception.
     */
    bool findNamespaceByName(NamespaceDesc& nsDesc);

    /**
     * Return a descriptor for each namespace.
     * @param[out] nsVec one entry for each namespace
     */
    void getNamespaces(std::vector<NamespaceDesc>& nsVec);

    /// Unrestricted catalog version
    static constexpr ArrayID ANY_VERSION = ArrayID(std::numeric_limits<int64_t>::max());
    static constexpr ArrayID MAX_ARRAYID = ArrayID(std::numeric_limits<int64_t>::max());
    static constexpr VersionID MAX_VERSIONID = VersionID(std::numeric_limits<int64_t>::max());

    /**
     *  Argument struct for internal findArray methods.
     *
     *  @note Used also by namespaces::Communicator, hence public.
     */
    struct FindArrayArgs {
        const std::string* nsNamePtr;
        const std::string* arrayNamePtr;
        VersionID versionId;
        ArrayID catalogVersion;

        FindArrayArgs()
            : nsNamePtr(nullptr)
            , arrayNamePtr(nullptr)
            , versionId(NO_VERSION)
            , catalogVersion(SystemCatalog::ANY_VERSION)
        {}
    };

    /**
     *  Results struct for internal findArray methods.
     *
     *  @note Used also by namespaces::Communicator, hence public.
     */
    struct FindArrayResult {
        ArrayID arrayId;
        uint64_t distId;
        int32_t flags;
        rbac::ID nsId;

        FindArrayResult()
            : arrayId(INVALID_ARRAY_ID)
            , distId(0)
            , flags(0)
            , nsId(rbac::INVALID_NS_ID)
        {}
    };

    /**
     *  Argument struct for getArrayDesc() method.
     *
     *  @description The getArrayDesc() method that uses this argument
     *  struct describes the individual fields and flags in more detail.
     */
    struct GetArrayDescArgs {
        ArrayDesc*      result;             ///< ArrayDesc object to modify.
        std::string     nsName;             ///< Namespace name
        std::string     arrayName;          ///< Array to describe
        VersionID       versionId;          ///< Array version wanted
        ArrayID         catalogVersion;     ///< Don't return result w/ versioned_array_id > this.
        bool            throwIfNotFound;    ///< Throw if array doesn't exist
        bool            ignoreOrphanAttrs;  ///< Allow missing attr UDTs or UDFs

        GetArrayDescArgs()
            : result(nullptr)
            , versionId(NO_VERSION)
            , catalogVersion(ANY_VERSION)
            , throwIfNotFound(false)
            , ignoreOrphanAttrs(false)
        {}
    };

    /**
     *  Get metadata for an array.
     *
     *  @param[in,out] args POD describing the desired array metadata.
     *  @return true iff the array was found
     *  @throw SCIDB_LE_ARRAY_DOESNT_EXIST if not found and throwIfNotFound set
     *  @throw SCIDB_LE_PLUGIN_ARRAY_ACCESS if missing plugin prevents access
     *  @throw scidb::SystemException on unrecoverable database errors
     *  @see Query::getCatalogVersion()
     *
     *  @description Fill in an ArrayDesc array descriptor based on the
     *  fields of the GetArrayDescArgs argument struct.  Not all fields
     *  are required.  In approximate order of importance, fields are:
     *  - result
     *    Pointer to existing ArrayDesc object where the result will be
     *    stored.  MUST NOT be null.
     *  - nsName
     *    Namespace in which to look for array.  MUST NOT be empty.
     *  - arrayName
     *    Name of the array within the namespace.  MUST NOT be empty.
     *    MAY have a version suffix (but it cannot conflict with versionId).
     *  - versionId
     *    Set this field if a specific version of the array metadata
     *    is wanted.  In addition to a specific version number,
     *    LAST_VERSION is also accepted.  Defaults to NO_VERSION,
     *    which corresponds to the "unversioned array id".
     *  - catalogVersion
     *    Obtained earlier by calling Query::getCatalogVersion(),
     *    this "versioned array id" is a high bound on the array id of the
     *    result ArrayDesc.  If specified, it is used to enforce a
     *    consistent view across successive catalog accesses.  Defaults to
     *    ANY_VERSION.
     *  - throwIfNotFound
     *    Ordinarily getArrayDesc() returns false if the desired array is
     *    not found in the catalog.  If this flag is set, a
     *    SCIDB_LE_ARRAY_DOESNT_EXIST system exception is thrown instead.
     *  - ignoreOrphanAttrs
     *    User-defined type (UDT) plugins for all attributes MUST be
     *    loaded, unless this flag is set.  Defaults to false.
     */
    bool getArrayDesc(GetArrayDescArgs& args);

    /**
     * @brief Return array metadata associated with a DataStore id.
     *
     * @param dsid datastore id, that is, the unversioned array identifier
     * @return shared pointer to array descriptor, or nil if not found
     *
     * @description This is a special-purpose lookup method intended
     * for use by the storage subsystem.  Because of this special use,
     * there are a few implied constraints:
     *
     * - A DataStore::DsId is actually an unversioned array id.  No
     *   problem there, SystemCatalog knows all about those.
     *
     * - Missing plugins must not cause failures.  The storage
     *   subsystem is interested in the metadata of existing arrays
     *   regardless of whether plugins for user-defined attribute
     *   types or for the namespaces feature have been loaded.  A
     *   failure causes the array data to be garbage collected, a
     *   heavy price to pay for a missing or improperly loaded plugin!
     *   See SDB-5721.
     *
     * - Dimension chunk intervals are required.  If the schema found
     *   based on the dsid/uaid has unresolved intervals (that is, is
     *   autochunked), then return the schema for the latest stored
     *   version of the array (whose intervals are fully resolved).
     *   See SDB-5457.
     */
    std::shared_ptr<ArrayDesc> getArrayDescForDsId(const ArrayID dsid);

    /**
     * Delete array from catalog by its name and all of its versions if this is the base array.
     * @param[in] ns_name namespace name, MUST NOT be empty
     * @param[in] array_name array name, MUST NOT be empty or qualified
     * @return true if array was deleted, false if it did not exist
     */
    bool deleteArray(const std::string& ns_name,
                     const std::string& array_name);

    /**
     * Delete all versions prior to given version from array with given name
     * @param[in] ns_name namespace name, MUST NOT be empty
     * @param[in] array_name array name, MUST NOT be empty or qualified
     * @param[in] array_version Array version prior to which all versions should be deleted.
     * @return true if array versions were deleted, false if array did not exist
     */
    bool deleteArrayVersions(const std::string& ns_name,
                             const std::string& array_name,
                             const VersionID array_version);

    /**
     * Delete array from persistent system catalog manager by its ID
     * @param[in] id array identifier
     */
    void deleteArray(const ArrayID id);

    /**
     * Get an array ID suitable for using in a schema(ArrayDesc) for a persistent array,
     * the one stored in the catalog (including the temp arrays).
     */
    ArrayID getNextArrayId();

    /**
     * Delete version of the array
     * @param[in] arrayID array ID
     * @param[in] versionID version ID
     */
    void deleteVersion(const ArrayID arrayID, const VersionID versionID);

    /**
     * Get last version of an array.
     * The version provided by this method corresponds to an array with id <= catalogVersion
     * @param[in] unvAId unversioned array ID
     * @param[in] catalogVersion as previously returned by getCurrentVersion().
     *            If catalogVersion == SystemCatalog::ANY_VERSION,
     *            the array ID corresponding to the result is not bounded by catalogVersion
     * @return identifier of last array version or 0 if this array has no versions
     */
    VersionID getLastVersion(const ArrayID unvAId,
                             const ArrayID catlogVersion=ANY_VERSION);

    /**
     * Get array id of oldest version of array
     * @param[in] id array ID
     * @return array id of oldest version of array or 0 if array has no versions
     */
    ArrayID getOldestArrayVersion(const ArrayID id);

    /**
     * Get the latest version preceeding specified timestamp
     * @param[in] id array ID
     * @param[in] timestamp string with timestamp
     * @return identifier ofmost recent version of array before specified timestamp or 0 if there is no such version
     */
    VersionID lookupVersionByTimestamp(const ArrayID id, const uint64_t timestamp);

    /**
     * Get list of updatable array's versions
     * @param[in] arrayId array identifier
     * @return vector of VersionDesc
     */
    std::vector<VersionDesc> getArrayVersions(const ArrayID array_id);

    /**
     * Get array actual upper boundary
     * @param[in] id array ID
     * @return array of maximal coordinates of array elements
     */
    Coordinates getHighBoundary(const ArrayID array_id);

    /**
     * Get array actual low boundary
     * @param[in] id array ID
     * @return array of minimum coordinates of array elements
     */
    Coordinates getLowBoundary(const ArrayID array_id);

    /**
     * Update array high and low boundaries and chunk intervals
     * @param[in] desc the array descriptor
     * @param[in] bounds the boundaries of the array
     */
    void updateArrayBoundariesAndIntervals(ArrayDesc const& desc,
                                           PhysicalBoundaries const& bounds);

    /**
     * Update the attributes for a transient array.
     * @param[in] desc the array descriptor
     */
    void updateTransientAttributes(ArrayDesc const& desc);

    /**
     * Get number of registered instances
     * return total number of instances registered in catalog
     */
    uint32_t getNumberOfInstances();

    /**
     * Add new instance to catalog
     * @param[in] instance Instance descriptor
     * @param[in] str opaque undocumented
     * @return Identifier of instance (ordinal number actually)
     */
    uint64_t addInstance(const InstanceDesc &instance,
                         const std::string& str);

    /**
     * Get all instances which are cluster memebers
     * @param[out] instances Instances vector
     * @return current memebrhip ID
     */
    MembershipID getInstances(Instances &instances);

    /**
     * Get all instances registered in catalog
     * @param[out] instances Instances vector
     * @return current memebrhip ID
     */
    MembershipID getAllInstances(Instances &instances);

    /**
     * Look up physical instance id of a cluster member
     * @param serverId server id as found in config.ini server-<X>=... directive
     * @param serverInstId server instance id from value part of same directive
     *
     * @details Input parameters must be non-negative (they are only
     * signed to match their underlying Postgres types).  Only the
     * active cluster membership is considered: if the instance
     * Throws on illegal inputs or internal error.
     *
     * @return Desired physical id on success, INVALID_INSTANCE_ID on failure.
     */
    InstanceID getPhysicalInstanceId(int32_t serverId, int32_t serverInstId);

    /**
     * Temporary method for connecting to PostgreSQL database used as metadata
     * catalog
     *
     * @param[in] doUpgrade run upgrade scripts depending on metadata version
     */
    void connect(bool doUpgrade);

    /**
     * Temporary method for checking connection to catalog's database.
     *
     * @return is connection established
     */
    bool isConnected() const;

    /**
     * Load library, and record loaded library in persistent system catalog
     * manager.
     *
     * @param[in] library name
     */
    void addLibrary(const std::string& libraryName);

    /**
     * Get info about loaded libraries from the persistent system catalog
     * manager.
     *
     * @param[out] libraries vector of library names
     */
    void getLibraries(std::vector< std::string >& libraries);

    /**
     * Unload library.
     *
     * @param[in] library name
     */
    void removeLibrary(const std::string& libraryName);

    /**
     * Returns version of loaded catalog metadata
     *
     * @return[out] metadata version
     */
    int getMetadataVersion() const;

    /**
     * Updates the query array locks with the highest array ID committed to the catalog for each array
     * in the list of ACQUIRED locks.
     * @param locks [in/out] all already ACQUIRED array locks for the current query
     */
    void getCurrentVersion(QueryLocks& locks);

    /**
     * Record provisional array metadata in the catalog.
     *
     * This array version metadata was created during multiquery execution by
     * a subquery on the given instance.
     *
     * @param uaid The unversioned array ID of the provisional array.
     * @param aid The versioned array ID of the provisional array.
     * @param vid The version of the provisional array.
     * @param instanceID The physical instance ID generating this record.
     * @param multiqueryID The query ID of the multiquery responsible for
     * the subquery that created the provisional array entry.
     */
    void recordProvisionalEntry(ArrayUAID uaid,
                                ArrayID aid,
                                VersionID vid,
                                const InstanceID& instanceID,
                                const QueryID& multiqueryID);

    /**
     * Remove provisional array metadata from the catalog for a given instance.
     *
     * @param uaid The unversioned array ID of the provisional array.
     * @param aid The versioned array ID of the provisional array.
     * @param vid The version of the provisional array.
     * @param instanceID The physical instance ID generating this record.
     */
    void removeProvisionalEntry(ArrayUAID uaid,
                                ArrayID aid,
                                VersionID vid,
                                const InstanceID& instanceID);

    /**
     * For a given instance, read all of the provisional array metadata entries that
     * it created into three-tuples of (ArrayUAID, ArrayID, VersionID).
     *
     * @param instanceID The instance ID whose provisional array metadata records we'd
     * like to read from the catalog.
     * @param[out] provArrs A collection of three-tuples of (ArrayUAID, ArrayID, VersionID),
     * each of which individually corresponds to a provisional array record in the
     * catalog.
     */
    using ProvArray = std::tuple<ArrayUAID, ArrayID, VersionID>;
    using ProvArrays = std::vector<ProvArray>;
    void readProvisionalEntries(const InstanceID& instanceId,
                                ProvArrays& provArrs);

    void readProvisionalEntries(const QueryID& multiqueryID,
                                ProvArrays& provArrs);

    /**
     * Indicates that there are provisional arrays remaining for the given
     * multiquery ID.
     *
     * @param multiqueryID the query ID of some multiquery running in the system.
     * @return true if there are provisional arrays remaining for the given
     * multiquery ID; false if not.
     */
    bool hasProvisionalEntries(const QueryID& multiqueryID);

    /**
     * Request the latest version of the array, it has the current schema.
     *
     * @param namespaceName The name of the namespace containing the array
     *     in question.
     * @param arrayName The name of the array in question.
     * @param arrayId The versioned array ID (VAID) for the array in question.
     * @return The corresponding array descriptor from the catalog.
     */
    ArrayDesc getLatestVersion(std::string const& namespaceName,
                               std::string const& arrayName,
                               ArrayID arrayId);

private:  // Definitions

    ArrayDistPtr getArrayDistribution(uint64_t arrDistId,
                                      pqxx::basic_transaction* tr);

    ArrayResPtr getArrayResidency(ArrayID uaid,
                                  pqxx::basic_transaction* tr);

private:  // Methods

    /**
     * Helper method to get an appropriate SQL string for a given lock
     */
    static std::string getLockInsertSql(const std::shared_ptr<LockDesc>& lockDesc);

    /// SQL to garbage-collect unused mapping arrays
    static const std::string cleanupMappingArraysSql;

    /**
     * Default constructor for SystemCatalog()
     */
    SystemCatalog();
    virtual ~SystemCatalog();

    void _invalidateTempArray(const std::string& arrayName);
    void _renameArray(const std::string& ns_name,
                      const std::string& old_array_name,
                      const std::string& new_array_name);

    enum class LockResult
    {
        NONE,
        ALREADY_HELD,
        DID_NOT_ACQUIRE,
        ACQUIRED,
        BUSY
    };

    static bool _isLockHeld(LockResult lr)
    {
        return lr == LockResult::ALREADY_HELD
            || lr == LockResult::ACQUIRED;
    }

    static bool _commitNeeded(LockResult lr)
    {
        return lr == LockResult::DID_NOT_ACQUIRE
            || lr == LockResult::ACQUIRED
            || lr == LockResult::BUSY;
    }


    /**
     * Install a lock entry into array_version_lock for the array
     * corresponding to lockDesc.
     *
     * Callers to _lockArray must acquire a table lock on the array_version_lock
     * postgres table via the _lockAVLTable method.
     *
     * @param lockDesc An instance of LockDesc corresponding to the array
     * to lock.
     * @param errorChecker Caller-specific error checking and handling.
     * @param tr A postgres transaction.
     * @return A value from the LockResult enum, the status of
     * the requested lock.
     */
    LockResult _lockArray(const std::shared_ptr<LockDesc>& lockDesc,
                          ErrorChecker& errorChecker,
                          pqxx::work& tr);
    using LockAction = std::function<LockResult()>;
    LockResult _performLockAction(const LockAction& action);

    bool _unlockArray(const std::shared_ptr<LockDesc>& lockDesc);
    bool _updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc);
    void _readArrayLocks(const InstanceID instanceId,
            std::list< std::shared_ptr<LockDesc> >& coordLocks,
            std::list< std::shared_ptr<LockDesc> >& workerLocks);
    uint32_t _deleteArrayLocks(InstanceID instanceId, const QueryID& queryId, LockDesc::InstanceRole role);
    std::shared_ptr<LockDesc> _checkForCoordinatorLock(const std::string& namespaceName,
                                                       const std::string& arrayName,
                                                       const QueryID& queryId);
    std::string _findCredParam(const std::string& creds, const std::string& what);
    std::string _makeCredentials();
    void _initializeCluster();

    void _execute(Action& action, bool serialize = true);

    void _addArray(
        const ArrayDesc &array_desc);
    void _addArray(
        const ArrayDesc &array_desc,
        pqxx::basic_transaction* tr);
    void _addArrayVersion(
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc);

    // Input and output structs are public, see above.
    bool _findArrayWithTxn(const FindArrayArgs& args,
                           FindArrayResult& result,
                           pqxx::basic_transaction* tr);

    ArrayID _getArrayIdWork(const std::string& nsName,
                            const std::string& arrayName);
    bool _findNamespaceWork(NamespaceDesc& nsDesc, bool byName);
    void _getNamespacesWork(std::vector<NamespaceDesc>& nsVec);
    void _listNamespaceArrayIds(const NamespaceDesc& nsDesc,
                                std::vector<ArrayID>& arrayIds,
                                bool ignoreVersions,
                                bool orderByName,
                                pqxx::basic_transaction* tr);

    /**
     * _listPublicArrayIds
     *
     * Fetch all array IDs for arrays in the public namespace.
     *
     * In the case that latestVersion is true, inspect the
     * latest_array_version table as it will have
     * array IDs corresponding to the latest array version only.
     * These IDs conveniently reference the correct set of attributes
     * from the array_attribute table rather than the first version's
     * attributes from that same table.  This query will return IDs
     * for arrays where those arrays have version numbers as part
     * of their names.  Callers will need to know and remove the
     * array version suffixes from the array names if they use
     * the resulting IDs to fetch such names from the catalog later.
     *
     * @param arrayIds      IDs of arrays within the public namespace.
     * @param latestVersion If true, then the latest version of each
     *                      array is returned; otherwise, all versions
     *                      of each array are returned.
     * @param orderByName   If true, then order results by
     *                      array name; otherwise, order by array ID.
     * @param tr            Pointer to postgres transaction.
     */
    void _listPublicArrayIds(std::vector<ArrayID>& arrayIds,
                             bool latestVersion,
                             bool orderByName,
                             pqxx::basic_transaction* tr);
    void _listAllArrayIds(std::vector<ArrayID>& arrayIds,
                          bool ignoreVersions,
                          bool orderByName,
                          pqxx::basic_transaction* tr);
    void _getArraysOneNamespace(std::string const& nsName,
                                std::vector<ArrayDesc>& arrayDescs,
                                bool ignoreOrphanAttributes,
                                bool latestVersion,
                                bool orderByName);
    void _getArraysAllNamespaces(std::vector<ArrayDesc>& arrayDescs,
                                 bool ignoreOrphanAttributes,
                                 bool latestVersion,
                                 bool orderByName);
    bool _containsArray(const ArrayID array_id);
    void _getArrayDescWork(GetArrayDescArgs& args);
    void _getArrayDescWithTxn(GetArrayDescArgs& args,
                              pqxx::basic_transaction* tr);
    std::shared_ptr<ArrayDesc> _getArrayDescForDsId(const ArrayID id);
    std::shared_ptr<ArrayDesc> _getArrayDescByIdWithTxn(const ArrayID id,
                                                        bool allowMissingPlugins,
                                                        bool wantChunkIntervals,
                                                        pqxx::basic_transaction* tr,
                                                        const NamespaceDesc* nsDesc = nullptr);
    bool _fillAttributes(pqxx::basic_transaction* tr,
                         const ArrayID aid,
                         Attributes& attrs,
                         bool ignoreOrphans = false);
    bool _fillDimensions(pqxx::basic_transaction* tr,
                         const ArrayID aid,
                         Dimensions& dims);
    ArrayID _getUaidFromVersionedAid(ArrayID vaid,
                                     VersionID vid,
                                     pqxx::basic_transaction* tr);
    bool _mapArrayIdToNamespace(ArrayID aid,
                                NamespaceDesc& nsDesc,
                                pqxx::basic_transaction* tr);

    void insertArrayDistribution(uint64_t arrDistId,
                                 const ArrayDistPtr& distribution,
                                 pqxx::basic_transaction* tr);

    void insertArrayResidency(ArrayID uaid,
                              const ArrayResPtr& residency,
                              pqxx::basic_transaction* tr);


    bool _deleteArrayByName(const std::string& ns_name, const std::string& array_name);
    bool _deleteArrayVersions(const std::string& ns_name,
                              const std::string &array_name,
                              const VersionID array_version);
    void _deleteArrayById(const ArrayID id);
    ArrayID _getNextArrayId();
    ArrayID _getNextArrayId(pqxx::basic_transaction* tr);
    VersionID _createNewVersion(const ArrayID id, const ArrayID version_array_id,
                                pqxx::basic_transaction* tr);
    void _deleteVersion(const ArrayID arrayID, const VersionID versionID);
    VersionID _getLastVersion(const ArrayID id, const ArrayID catalogVersion);
    VersionDesc _getLastVersionWithTxn(const ArrayID id,
                                       const ArrayID catalogVersion,
                                       pqxx::basic_transaction*);
    ArrayID _getOldestArrayVersion(const ArrayID id);
    VersionID _lookupVersionByTimestamp(const ArrayID id, const uint64_t timestamp);
    std::vector<VersionDesc> _getArrayVersions(const ArrayID array_id);
    Coordinates _getHighBoundary(const ArrayID array_id);
    Coordinates _getLowBoundary(const ArrayID array_id);
    void _updateArrayBoundariesAndIntervals(ArrayDesc const& desc, PhysicalBoundaries const& bounds);
    void _updateTransientAttributes(ArrayDesc const& desc);
    uint32_t _getNumberOfInstances();
    InstanceID _addInstance(const InstanceDesc &instance, const std::string& online);
    MembershipID _getInstances(Instances &instances, bool all);
    InstanceID _getPhysInstId(int32_t sid, int32_t siid);
    void _addLibrary(const std::string& libraryName);
    void _getLibraries(std::vector< std::string >& libraries);
    void _removeLibrary(const std::string& libraryName);
    void _getCurrentVersion(QueryLocks& locks);

    /**
     * Indicate the presence of the global array lock.
     * @param tr A postgres transaction.
     * @return true if the global array lock exists in the catalog, false if not.
     */
    bool _hasGlobalArrayLock(pqxx::work& tr);

    /**
     * Indicate if lockDesc is the global array lock.
     * @param lockDesc an instance of a lock descriptor.
     * @return true if lockDesc is the global array lock, false if not.
     */
    bool _isGlobalArrayLock(std::shared_ptr<LockDesc> lockDesc) const;

    /**
     * Acquire a Postgres lock on the array_version_lock table for
     * the duration of the passed-in transaction.
     * @param tr A postgres transaction.
     */
    void _lockAVLTable(pqxx::work& tr) const;

private:  // Variables

    bool _initialized;
    pqxx::connection *_connection;
    std::string _uuid;
    int _metadataVersion;

    //FIXME: libpq don't have ability of simultaneous access to one connection from
    // multiple threads even on read-only operatinos, so every operation must
    // be locked with this mutex while system catalog using PostgreSQL as storage.
    static Mutex _pgLock;

    friend class Singleton<SystemCatalog>;
    /// number of attempts to reconnect to PG
    int _reconnectTries;
    /// number of attempts to re-execute a conflicting/concurrent serialized txn
    int _serializedTxnTries;
    static const int DEFAULT_SERIALIZED_TXN_TRIES =10;

    void throwOnSerializationConflict(const pqxx::sql_error& e);
};

// Helper for debug logging.
std::ostream& operator<< (std::ostream& os,
                          SystemCatalog::GetArrayDescArgs const& a);

} // namespace scidb

#endif /* SYSTEMCATALOG_H_ */
