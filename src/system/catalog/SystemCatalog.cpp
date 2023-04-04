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
 * @file SystemCatalog.cpp
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @brief API for fetching and updating system catalog metadata.
 */

#include <system/SystemCatalog.h>

#include <array/ArrayID.h>
#include <array/ArrayName.h>
#include <array/VersionDesc.h>
#include <query/executor/ScopedPostgressStats.h>
#include <query/Expression.h>
#include <query/Multiquery.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <query/Serialize.h>
#include <system/Config.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <system/LockDesc.h>
#include <system/SciDBConfigOptions.h>
#include <system/catalog/data/CatalogMetadata.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/NamespacesCommunicator.h>
#include <util/Mutex.h>
#include <util/OnScopeExit.h>
#include <util/Pqxx.h>
#include <util/Utility.h>

#include <vector>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <limits.h>
#include <pwd.h>
#include <sys/stat.h>

#include <log4cxx/logger.h>

using namespace std;
using namespace pqxx;
using namespace pqxx::prepare;

using NsComm = scidb::namespaces::Communicator;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.catalog"));

    // Exception used to request PG query restart on serialization conflict.
    struct TxnIsolationConflict : public pqxx::sql_error
    {
        TxnIsolationConflict(const std::string& what,
                             const std::string& query)
            : pqxx::sql_error(what, query)
        {}
    };

    ostream& operator<< (ostream& os,
                         scidb::SystemCatalog::GetArrayDescArgs const& a)
    {
        os << '{' << makeQualifiedArrayName(a.nsName, a.arrayName);
        switch (a.versionId) {
        case LAST_VERSION:
            os << "@last";
            break;
        case ALL_VERSIONS:      // Not used below; here for completeness.
            os << "@all";
            break;
        case NO_VERSION:
            break;
        default:
            os << '@' << a.versionId;
            break;
        }
        if (a.catalogVersion == SystemCatalog::ANY_VERSION) {
            os << ", catVer:any";
        } else {
            os << ", catVer:" << a.catalogVersion;
        }
        os << ", result:" << hex << a.result << dec;
        if (a.throwIfNotFound) {
            os << ", throwIfNotFound";
        }
        if (a.ignoreOrphanAttrs) {
            os << ", ignoreOrphanAttrs";
        }
        os << '}';

        return os;
    }

    Mutex SystemCatalog::_pgLock;

    void SystemCatalog::_logSqlError(
        const std::string &t,
        const std::string &w)
    {
        LOG4CXX_ERROR(logger,
            "sql_error"
            << " name=" << t
            << " what=" << w);
    }

    void SystemCatalog::_invalidateTempArray(const std::string& arrayName)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG1);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::_removeTempArray()");

        try
        {
         /* Add the 'DEAD' flag to all entries of the 'array' table whose
          * 'flags' field currently has the 'TRANSIENT' bit set... */

            string sql("update \"array\" set flags = (flags | $1) where (flags & $2)!=0");

            if (!arrayName.empty()) {
                sql += " and name=$3";
            }

            pqxx::transaction<pqxx::serializable> tr(*_connection);
            _connection->prepare(sql,sql);

            pqxx::prepare::invocation invc = tr.prepared(sql)
                (int(ArrayDesc::DEAD))
                (int(ArrayDesc::TRANSIENT));
            if (!arrayName.empty()) {
                invc(arrayName);
            }

            invc.exec();
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);

            LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: query:"              << e.query());
            if (isDebug()) {
                const string t = typeid(e).name();
                LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: postgress exception type:"<< t);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,SCIDB_LE_PG_QUERY_EXECUTION_FAILED)       << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                SCIDB_ASSERT(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Invalidated temp arrays");
    }

    void SystemCatalog::invalidateTempArrays()
    {
        const string allArrays;
        std::function<void()> work1 = std::bind(&SystemCatalog::_invalidateTempArray,
                                                this,
                                                std::cref(allArrays));
        std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    const std::string& SystemCatalog::initializeCluster()
    {
        std::function<void()> work = std::bind(&SystemCatalog::_initializeCluster, this);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
        return _uuid;
    }

    void SystemCatalog::_initializeCluster()
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG2);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::initializeCluster()");

        try
        {
            work tr(*_connection);
            tr.exec(string(CURRENT_METADATA));

            result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
            _uuid = query_res[0].at("uuid").as(string());
            query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
            _metadataVersion = query_res[0].at("version").as(int());
            assert(METADATA_VERSION == _metadataVersion);
            _initialized = true;

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Initialized cluster uuid = " << _uuid << ", metadata version = " << _metadataVersion);
    }

    bool SystemCatalog::isInitialized() const
    {
        return _initialized;
    }

    const std::string& SystemCatalog::getClusterUuid() const
    {
        return _uuid;
    }

    ArrayID SystemCatalog::getNextArrayId()
    {
        std::function<ArrayID()> work = std::bind(static_cast<ArrayID (SystemCatalog::*)(void)>(
                                                      &SystemCatalog::_getNextArrayId),
                                                  this);
        return Query::runRestartableWork<ArrayID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::_getNextArrayId()
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG3);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        ArrayID arrId(0);
        try {
            assert(_connection);
            work tr(*_connection);
            arrId = _getNextArrayId(&tr);
            tr.commit();
            LOG4CXX_TRACE(logger, "SystemCatalog::_getNextArrayId(): " << arrId);
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        SCIDB_ASSERT(arrId >0);
        return arrId;
    }

    ArrayID SystemCatalog::_getNextArrayId(pqxx::basic_transaction* tr)
    {
        result query_res = tr->exec("select nextval from nextval('array_id_seq')");
        const ArrayID arrId = query_res[0].at("nextval").as(int64_t());
        return arrId;
    }

    /**
     * Map versioned ArrayID and VersionID to unversioned ArrayID.
     *
     * @param[in] vaid versioned array id
     * @param[in] version version number corresponding to vaid
     * @param[in,out] tr Postgres transaction pointer
     * @return unversioned array id, or INVALID_ARRAY_ID if vaid not found
     *
     * @note The version parameter acts as a consistency check: the
     *       vaid alone strictly determines the array version, but in
     *       the calling context we already know what that version is
     *       supposed to be.  So here, we just confirm it.
     *
     * @note Not thread safe.  Must be called with active connection
     *       under _pgLock.
     */
    ArrayID SystemCatalog::_getUaidFromVersionedAid(ArrayID vaid,
                                                    VersionID version,
                                                    pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_getUaidFromVersionedAid(vaid="
                      << vaid << ", vid=" << version << ", tr)");

        SCIDB_ASSERT(vaid);
        SCIDB_ASSERT(version && !isSpecialVersionId(version));
        SCIDB_ASSERT(tr);
        SCIDB_ASSERT(_connection);

        string sql =
            "select array_id, version_id from \"array_version\" "
            "where version_array_id = $1";
        _connection->prepare(sql, sql);
        pqxx::result qres = tr->prepared(sql)(vaid).exec();
        if (qres.empty()) {
            return INVALID_ARRAY_ID;
        }

        // Check result consistency.
        ASSERT_EXCEPTION((qres.size() == 1),
                         "SysCatalog is inconsistent, too many array id rows");
        int64_t vid = qres[0].at("version_id").as(int64_t());
        ASSERT_EXCEPTION((static_cast<VersionID>(vid) == version),
                         "Array version does not match array ID");

        // Done.
        int64_t uaid = qres[0].at("array_id").as(int64_t());
        SCIDB_ASSERT(uaid != INVALID_ARRAY_ID);
        return static_cast<ArrayID>(uaid);
    }

    void SystemCatalog::addArrayVersion(
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc)
    {
        std::function<void()> work1 = std::bind(&SystemCatalog::_addArrayVersion,
                                                this,
                                                unversionedDesc,
                                                std::cref(versionedDesc));

        std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);

        Query::runRestartableWork<void, broken_connection>(
            work2, _reconnectTries);
    }

    void SystemCatalog::_addArrayVersion(
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_addArrayVersion(...)");

        assert(versionedDesc.getUAId()>0);
        assert(versionedDesc.getUAId()<versionedDesc.getId());
        assert(versionedDesc.getVersionId()>0);

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG4);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            if (unversionedDesc != NULL) {
                SCIDB_ASSERT(unversionedDesc->getId() == versionedDesc.getUAId());
                SCIDB_ASSERT(unversionedDesc->getUAId() == unversionedDesc->getId());
                _addArray(*unversionedDesc, &tr);
            }
            _addArray(versionedDesc, &tr);
            _createNewVersion(versionedDesc.getUAId(), versionedDesc.getId(), &tr);

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::addArray(const ArrayDesc &arrayDesc)
    {
        std::function<void()> work = std::bind(static_cast<void (SystemCatalog::*)(const ArrayDesc&)>(
                                                   &SystemCatalog::_addArray),
                                               this,
                                               std::cref(arrayDesc));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addArray(
        const ArrayDesc &arrayDesc)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG5);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        try
        {
            work tr(*_connection);
            _addArray(arrayDesc, &tr);
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::_addArray(
        const ArrayDesc &array_desc,
        pqxx::basic_transaction* tr)
    {
        // caller sets the timing scope
        LOG4CXX_DEBUG(logger, "SystemCatalog::_addArray array_desc: "<< array_desc );
        SCIDB_ASSERT(!array_desc.getNamespaceName().empty());

        const ArrayID arrId = array_desc.getId();
        const string arrayName = array_desc.getName();
        const ArrayUAID uaid = array_desc.getUAId();
        const VersionID vid = array_desc.getVersionId();
        ArrayResPtr residency = array_desc.getResidency();
        ArrayDistPtr distribution = array_desc.getDistribution();

        ASSERT_EXCEPTION(isValidDistType(distribution->getDistType()),
                         string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION(not isUndefined(distribution->getDistType()),
                         string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION(not isUninitialized(distribution->getDistType()),
                         string("Invalid array descriptor: ")+array_desc.toString());
        {
            size_t instanceShift(0);
            ArrayDistributionFactory::getTranslationInfo(distribution.get(), instanceShift);
            ASSERT_EXCEPTION(instanceShift==0,
                             string("Invalid array descriptor: ")+array_desc.toString());
        }
        ASSERT_EXCEPTION(arrId>0, string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION( ( (isNameUnversioned(arrayName) && uaid==arrId) ||
                            (vid == getVersionFromName(arrayName) &&
                             uaid > 0 &&
                             uaid < arrId) ),
                          string("Invalid array version descriptor: ")+array_desc.toString() );

        //XXX TODO: for now we will use the uaid to identify distributions,
        //XXX TODO: at some point we might want to reuse distributions across many arrays
        uint64_t arrDistId = uaid;
        if (uaid == arrId) {
            // inserting an unversioned array
            insertArrayDistribution(arrDistId, distribution, tr);
        }

        // Note from Donghui Zhang 2015-6-23:
        // The logic below makes sure attribute names and dimension names do not collide,
        // as a more efficient implementation of the trigger removed in ticket:3676 (SDB-3914).
        //
        // Additional note from Donghui Zhang 2015-12-18: As revealed in ticket:5068 (SDB-3977), we
        // cannot rely on a user exception here because it will be too late.  In the scenario
        // described in the ticket, the engine crashed.  We fixed the problem by making store()
        // produce unique names.  Here instead of throwing a user exception, we assert that there
        // should not be conflicts.
        //
        ASSERT_EXCEPTION(array_desc.areNamesUnique(),
            "In SystemCatalog::_addArray(), duplicate dimension/attribute names are found.");

        // Does array already exist?
        FindArrayResult findResult;
        FindArrayArgs findArgs;
        findArgs.nsNamePtr = &array_desc.getNamespaceName();
        findArgs.arrayNamePtr = &arrayName;
        bool found = _findArrayWithTxn(findArgs, findResult, tr);
        if (found) {
            // Since we also check this in a few inferSchema() methods,
            // if we fail here there may be racing creates for this array.
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXISTS)
                << array_desc.getQualifiedArrayName();
        }

        // Insert it!
        string sql1 =
            "insert into \"array\" (name, id, distribution_id, flags) "
            "values ($1, $2, $3, $4)";
        _connection->prepare(sql1, sql1);
        pqxx::result queryRes = tr->prepared(sql1)
            (array_desc.getName())
            (arrId)
            (arrDistId)
            (array_desc.getFlags()).exec();
        ASSERT_EXCEPTION(queryRes.affected_rows() == 1,
                         "Duplicate array on insert but not on prior lookup?!");

        if (uaid == arrId) {
            // inserting an unversioned array
            insertArrayResidency(uaid, residency, tr);
        }

        // Update latest array version table
        auto version_id = array_desc.getVersionId();
        auto arrayBaseName = array_desc.getName().substr(0,array_desc.getName().find("@"));
        assert(!arrayBaseName.empty());
        ostringstream latestArrayVersionSql;
        if (version_id < 1) {
            latestArrayVersionSql
                << "insert into latest_array_version (namespace_name, array_name, array_id) values ('"
                << array_desc.getNamespaceName() << "','"
                << arrayBaseName << "',"
                << array_desc.getId() << ")";
        }
        else {
            latestArrayVersionSql << "update latest_array_version set array_id="
                                  << array_desc.getId()
                                  << " where namespace_name='"
                                  << array_desc.getNamespaceName()
                                  << "' and array_name='"
                                  << arrayBaseName
                                  << "'";
        }
        tr->exec(latestArrayVersionSql.str());

        string sql2 =
            "insert into \"array_attribute\" ( "
            "    array_id, id, name, type, flags, default_compression_method, "
            "    reserve, default_missing_reason, default_value) "
            "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        _connection->prepare(sql2, sql2);

        Attributes const& attributes = array_desc.getAttributes();
        Attributes cachedAttributes;
        for (const auto& attr : attributes)
        {
            tr->prepared(sql2)
                (arrId)
                (attr.getId())
                (attr.getName())
                (attr.getType())
                (attr.getFlags())
                (toUType(attr.getDefaultCompressionMethod()))
                (attr.getReserve())
                (static_cast<int>(attr.getDefaultValue().getMissingReason()))
                (attr.getDefaultValueExpr()).exec();

            //Attribute in descriptor has no some data before adding to catalog so build it manually for
            //caching
            cachedAttributes.push_back(
                AttributeDesc(attr.getName(), attr.getType(), attr.getFlags(),
                          attr.getDefaultCompressionMethod(), std::set<std::string>(), attr.getReserve(),
                          &attr.getDefaultValue(), attr.getDefaultValueExpr()));
        }

        string sql3 =
            "insert into \"array_dimension\" ("
            "    array_id, id, name, startMin, currStart, currEnd, endMax,"
            "    chunk_interval, chunk_overlap) "
            "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        _connection->prepare(sql3, sql3);

        Dimensions const& dims = array_desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& dim = dims[i];
            tr->prepared(sql3)
                (arrId)
                (i)
                (dim.getBaseName())
                (dim.getStartMin())
                (dim.getCurrStart())
                (dim.getCurrEnd())
                (dim.getEndMax())
                (dim.getRawChunkInterval())
                (dim.getChunkOverlap()).exec();
            //TODO: If DimensionDesc will store IDs in future, here must be building vector of
            //dimensions for caching as for attributes.
        }

        // Insert array into namespace if necessary.  Throws on error.
        if ((uaid == arrId) &&
            (array_desc.getNamespaceName() != rbac::PUBLIC_NS_NAME))
        {
            NamespaceDesc nsDesc(array_desc.getNamespaceName());
            NsComm::addArrayToNamespaceWithTransaction(nsDesc, arrId, tr);
        }
    }

    void SystemCatalog::execute(Action& action, bool serialize /* = true */)
    {
        std::function<void()> work1 = std::bind(&SystemCatalog::_execute,
                                                this,
                                                std::ref(action),
                                                serialize);

        if(serialize)
        {
            std::function<void()>
                work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                  work1,
                                  _serializedTxnTries);

            Query::runRestartableWork<void, pqxx::broken_connection>(
                work2, _reconnectTries);
        } else {
            Query::runRestartableWork<void, pqxx::broken_connection>(
                work1, _reconnectTries);
        }
    }

    void SystemCatalog::_execute(Action &action, bool serialize)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG6);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try {
            if(serialize)
            {
                pqxx::transaction<pqxx::serializable> tr(*_connection);
                action(&tr);
                tr.commit();
            } else {
                work tr(*_connection);
                action(&tr);
                tr.commit();
            }
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const unique_violation& e)
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_NOT_UNIQUE)
                     << e.what();
        }
        catch (const sql_error &e)
        {
            if(serialize) {
                throwOnSerializationConflict(e);
            }

            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                    << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_UNKNOWN_ERROR)
                    << e.what();
        }
    }

    bool SystemCatalog::findNamespaceById(NamespaceDesc& nsDesc)
    {
        // Handle easy cases up front.
        if (nsDesc.getId() == rbac::PUBLIC_NS_ID) {
            nsDesc.setName(rbac::PUBLIC_NS_NAME);
            return true;
        } else if (nsDesc.getId() == rbac::ALL_NS_ID) {
            nsDesc.setName(rbac::ALL_NS_NAME);
            return true;
        } else if (nsDesc.getId() == rbac::INVALID_NS_ID) {
            ASSERT_EXCEPTION_FALSE("Attempted namespace lookup of invalid id");
        }

        // Assume not found until proven otherwise.
        nsDesc.setName("");

        std::function<bool()> work1 = std::bind(&SystemCatalog::_findNamespaceWork,
                                                this,
                                                std::ref(nsDesc),
                                                false);  // find by id

        std::function<bool()> work2 = std::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);

        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::findNamespaceByName(NamespaceDesc& nsDesc)
    {
        // Handle easy cases up front.
        string const& nsName = nsDesc.getName();
        if (nsName.empty()) {
            nsDesc.setName(rbac::PUBLIC_NS_NAME);
            nsDesc.setId(rbac::PUBLIC_NS_ID);
            return true;
        } else if (nsName == rbac::PUBLIC_NS_NAME) {
            nsDesc.setId(rbac::PUBLIC_NS_ID);
            return true;
        } else if (nsName == rbac::ALL_NS_NAME) {
            nsDesc.setId(rbac::ALL_NS_ID);
            return true;
        }

        // Assume not found until proven otherwise.
        nsDesc.setId(rbac::INVALID_NS_ID);

        std::function<bool()> work1 = std::bind(&SystemCatalog::_findNamespaceWork,
                                                this,
                                                std::ref(nsDesc),
                                                true);  // find by name

        std::function<bool()> work2 = std::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);

        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::_findNamespaceWork(NamespaceDesc& nsDesc, bool byName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_findNamespaceWork(\"" << nsDesc.getName()
                      << "\", " << nsDesc.getId() << ')');

        bool found = false;

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG7);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            found = NsComm::findNamespaceWithTransaction(nsDesc, byName, &tr);
            tr.commit();
        }
        catch (const Exception &e)
        {
            // Since public namespace lookups have already been
            // handled in the wrappers above, we can confidently
            // translate "plugin not loaded" into "namespace not
            // found".  See SDB-5327.
            if (e.getLongErrorCode() == SCIDB_LE_PLUGIN_FUNCTION_ACCESS) {
                found = false;
            } else {
                e.raise();
            }
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_UNKNOWN_ERROR)
                << e.what();
        }

        return found;
    }

    bool SystemCatalog::_findArrayWithTxn(const FindArrayArgs& args,
                                          FindArrayResult& result,
                                          pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_findArrayWithTxn(...)");

        // Validate arguments.
        SCIDB_ASSERT(args.nsNamePtr);
        SCIDB_ASSERT(!args.nsNamePtr->empty());
        SCIDB_ASSERT(args.arrayNamePtr);
        SCIDB_ASSERT(!args.arrayNamePtr->empty());
        SCIDB_ASSERT(!isQualifiedArrayName(*args.arrayNamePtr));

        string const& nsName = *args.nsNamePtr;
        string arrayName(*args.arrayNamePtr);

        LOG4CXX_TRACE(logger, "SystemCatalog::_findArrayWithTxn("
                      << makeQualifiedArrayName(nsName, arrayName)
                      << ", v:" << args.versionId
                      << ", catV:" << args.catalogVersion << ')');

        // Hand off non-public namespace requests to the plugin.
        bool publicNs = (nsName == rbac::PUBLIC_NS_NAME);
        if (!publicNs) {
            return NsComm::findArray(args, result, tr);
        }

        bool found = false;

        // We want the versioned array name (it matches the in-catalog name).
        if (!isNameVersioned(arrayName)
            && args.versionId != NO_VERSION
            && !isSpecialVersionId(args.versionId))
        {
            arrayName = makeVersionedName(arrayName, args.versionId);
        }

        pqxx::result qres;
        string key("find-public-array");
        string sql =
            "select id, flags, distribution_id from public_arrays "
            "where name = $1 and id <= $2";

        tr->conn().prepare(key, sql);
        qres = tr->prepared(key)(arrayName)(args.catalogVersion).exec();

        if (!qres.empty()) {
            SCIDB_ASSERT(qres.size() == 1);
            result.arrayId = qres[0].at("id").as(uint64_t());
            result.flags = qres[0].at("flags").as(int32_t());
            result.distId = qres[0].at("distribution_id").as(uint64_t());
            result.nsId = rbac::PUBLIC_NS_ID;
            found = true;
        }

        return found;
    }

    void SystemCatalog::getNamespaces(std::vector<NamespaceDesc>& nsVec)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNamespaces()");
        nsVec.clear();

        std::function<void()> work1 = std::bind(&SystemCatalog::_getNamespacesWork,
                                                this,
                                                std::ref(nsVec));

        std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);

        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    void SystemCatalog::_getNamespacesWork(std::vector<NamespaceDesc>& nsVec)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG8);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            NsComm::getNamespacesWithTransaction(nsVec, &tr);
            tr.commit();
        }
        catch (const Exception& e)
        {
            if (e.getLongErrorCode() != SCIDB_LE_PLUGIN_FUNCTION_ACCESS) {
                e.raise();
            }

            // No plugin, only public namespace is visible.
            nsVec.push_back(NamespaceDesc(rbac::PUBLIC_NS_NAME,
                                          rbac::PUBLIC_NS_ID));
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_UNKNOWN_ERROR)
                << e.what();
        }
    }

    namespace {
        /// Return true iff namespace descriptor is for the public namespace.
        bool isPublicNs(NamespaceDesc const& nsDesc)
        {
            return nsDesc.getId() == rbac::PUBLIC_NS_ID
                || (!nsDesc.isIdValid()
                    && nsDesc.getName() == rbac::PUBLIC_NS_NAME);
        }
    }

    void SystemCatalog::_listNamespaceArrayIds(const NamespaceDesc& nsDesc,
                                               vector<ArrayID>& arrayIds,
                                               bool ignoreVersions,
                                               bool orderByName,
                                               pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_listNamespaceArrayIds(...)");

        if (isPublicNs(nsDesc)) {
            _listPublicArrayIds(arrayIds, ignoreVersions, orderByName, tr);
        } else {
            NamespaceDesc nsDesc1(nsDesc);
            NsComm::listArrayIds(nsDesc1, arrayIds,
                                 ignoreVersions, orderByName, tr);
        }
    }

    void SystemCatalog::_listPublicArrayIds(vector<ArrayID>& arrayIds,
                                            bool latestVersion,
                                            bool orderByName,
                                            pqxx::basic_transaction* tr)
    {
        stringstream sql;
        if (latestVersion) {
            sql <<
                "with PA as ("
                    "select namespace_name as namespace, "
                       "array_name as name, "
                       "array_id as id "
                    "from latest_array_version) "
                "select id from PA where namespace='public' and name is not null ";
        }
        else {
            sql << "select id from public_arrays as PA where name is not null ";
        }
        if (orderByName) {
            sql << "order by PA.name";
        } else {
            sql << "order by PA.id";
        }

        pqxx::result qres = tr->exec(sql.str());

        // Return the array ids.
        arrayIds.clear();
        arrayIds.reserve(qres.size());
        for (auto const& row : qres) {
            arrayIds.push_back(row.at("id").as(uint64_t()));
        }
    }

    void SystemCatalog::_listAllArrayIds(vector<ArrayID>& arrayIds,
                                         bool ignoreVersions,
                                         bool orderByName,
                                         pqxx::basic_transaction* tr)
    {
        // Try the plugin first.
        try {
            NsComm::listAllArrayIds(arrayIds, ignoreVersions, orderByName, tr);
            return; // Success!
        }
        catch (const Exception& e) {
            if (e.getLongErrorCode() != SCIDB_LE_PLUGIN_FUNCTION_ACCESS) {
                e.raise();
            }
        }

        // Still here?  No namespaces plugin, so list all public arrays.
        LOG4CXX_TRACE(logger, __FUNCTION__ << ": Only public namespace is available");
        _listPublicArrayIds(arrayIds, ignoreVersions, orderByName, tr);
    }

    bool SystemCatalog::containsArray(const string& nsName, const string& arrayName)
    {
        return (getArrayId(nsName, arrayName) != INVALID_ARRAY_ID);
    }

    ArrayID SystemCatalog::getArrayId(const string& nsName, const string& arrayName)
    {
        std::function<ArrayID()> work = std::bind(&SystemCatalog::_getArrayIdWork,
                                                  this,
                                                  std::cref(nsName),
                                                  std::cref(arrayName));

        return Query::runRestartableWork<ArrayID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::_getArrayIdWork(const string& nsName, const string& arrayName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_getArrayIdWork("
                      << makeQualifiedArrayName(nsName, arrayName)
                      << ')');

        FindArrayResult result;
        FindArrayArgs args;
        string unversionedName(arrayName);
        args.versionId = getVersionFromName(arrayName);
        if (args.versionId != NO_VERSION) {
            unversionedName = makeUnversionedName(arrayName);
        }
        args.nsNamePtr = &nsName;
        args.arrayNamePtr = &unversionedName;

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG9);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        try
        {
            work tr(*_connection);
            _findArrayWithTxn(args, result, &tr);
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        return result.arrayId;
    }

    bool SystemCatalog::getArrayDesc(GetArrayDescArgs& args)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc(" << args << ')');

        // Argument validation.
        SCIDB_ASSERT(args.result != nullptr);
        SCIDB_ASSERT(!args.nsName.empty());
        SCIDB_ASSERT(!args.arrayName.empty());
        SCIDB_ASSERT(!isQualifiedArrayName(args.arrayName));

        // Allow version suffixes (if there is no conflict with versionid).
        VersionID vid = getVersionFromName(args.arrayName);
        if (vid != NO_VERSION) {
            SCIDB_ASSERT(args.versionId == NO_VERSION);
            args.versionId = vid;
            args.arrayName = makeUnversionedName(args.arrayName);
        }

        try {
            std::function<void()> work1 = std::bind(&SystemCatalog::_getArrayDescWork,
                                                    this,
                                                    std::ref(args));

            std::function<void()>
                work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                  work1,
                                  _serializedTxnTries);

            Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
            return true;            // If we got here, we must have found it.
        }
        catch (Exception& e) {
            if (e.getLongErrorCode() == SCIDB_LE_ARRAY_DOESNT_EXIST) {
                bool raise = args.throwIfNotFound;
                if (!raise) {
                    return false;
                }
            }
            e.raise();
        }
        SCIDB_UNREACHABLE();
    }

    void SystemCatalog::_getArrayDescWork(GetArrayDescArgs& args)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG10, true);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            _getArrayDescWithTxn(args, &tr);
            if (args.versionId != LAST_VERSION) {
                tr.commit();
                return;
            }

            // We have info for the unversioned array (the one added by
            // create_array), but caller wants the latest version.
            //

            // t.sdqp.sdb-6026
            // This postgres query is executed ten times for this test.
            // Can we cache the latest to avoid repeating this request
            // for unmodified arrays?  Purge the cached version when
            // the array is removed/updated.
            VersionID vid = _getLastVersionWithTxn(args.result->getId(),
                                                   args.catalogVersion,
                                                   &tr).getVersionID();
            if (vid == NO_VERSION) {
                // There is no later version.
                tr.commit();
                return;
            }

            // Look again with versionId specified.  Use a copy of the
            // args, in case of restart.
            GetArrayDescArgs args2(args);
            args2.versionId = vid;
            _getArrayDescWithTxn(args2, &tr);
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR)
                << e.what();
        }
    }

    void SystemCatalog::_getArrayDescWithTxn(GetArrayDescArgs& args,
                                             pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_getArrayDescWithTxn("<< args <<')');

        assert(_connection);
        assert(args.result);

        FindArrayResult findResult;
        FindArrayArgs findArgs;
        findArgs.nsNamePtr = &args.nsName;
        findArgs.arrayNamePtr = &args.arrayName;
        findArgs.versionId = args.versionId;
        findArgs.catalogVersion = args.catalogVersion;
        bool found = _findArrayWithTxn(findArgs, findResult, tr);
        if (!found) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST)
                << makeQualifiedArrayName(args.nsName, args.arrayName);
        }

        string metadataArrName(args.arrayName);
        ArrayID array_id = findResult.arrayId;
        ArrayUAID uaid = array_id;

        if (args.versionId && !isSpecialVersionId(args.versionId)) {
            metadataArrName = makeVersionedName(metadataArrName, args.versionId);
            uaid = _getUaidFromVersionedAid(findResult.arrayId, args.versionId, tr);
            if (uaid == INVALID_ARRAY_ID) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST)
                    << makeQualifiedArrayName(args.nsName, args.arrayName);
            }
        }

        Attributes attributes;
        if (_fillAttributes(tr, array_id, attributes, args.ignoreOrphanAttrs)) {
            findResult.flags |= ArrayDesc::INCOMPLETE;
        }

        Dimensions dimensions;
        bool isAutochunked = _fillDimensions(tr, array_id, dimensions);
        if (isAutochunked && args.versionId == NO_VERSION) {
            // If any data was stored, use the latest chunk
            // intervals---which should *not* be autochunked.
            // If no data was stored, and the array version ID is not
            // the unversioned ID (e.g., because someone created an array
            // and added an attribute to it), then the array will still
            // have autochunked chunk intervals.
            VersionDesc vDesc = _getLastVersionWithTxn(array_id, args.catalogVersion, tr);
            if (vDesc.getArrayID() != array_id) {
                SCIDB_ASSERT(vDesc.getArrayID() > array_id);
                dimensions.clear();
                (void) _fillDimensions(tr, vDesc.getArrayID(), dimensions);
            }
        }

        ArrayDistPtr distribution = getArrayDistribution(findResult.distId, tr);
        ArrayResPtr residency = getArrayResidency(uaid, tr);

        ArrayDesc newDesc(array_id, uaid, args.versionId,
                          args.nsName,
                          metadataArrName,
                          attributes,
                          dimensions,
                          distribution,
                          residency,
                          findResult.flags);
        *args.result = newDesc;

        SCIDB_ASSERT(args.result->getUAId() != 0);
        SCIDB_ASSERT(args.result->getId() <= args.catalogVersion);
        SCIDB_ASSERT(args.result->getUAId() <= args.catalogVersion);
    }


void SystemCatalog::insertArrayDistribution(uint64_t arrDistId,
                                            const ArrayDistPtr& distribution,
                                            pqxx::basic_transaction* tr)
{
    if (isDebug()) {
        InstanceID instanceShift(0);
        ArrayDistributionFactory::getTranslationInfo(distribution.get(), instanceShift);
        SCIDB_ASSERT(instanceShift == 0);
    }

    string sql =
        "INSERT INTO array_distribution ( "
        "   id, partition_function, partition_state, redundancy ) "
        "VALUES ($1, $2, $3, $4)";
    _connection->prepare(sql, sql);
    tr->prepared(sql)
    (arrDistId)
    ((int)distribution->getDistType())
    (distribution->getState())
    ((int)distribution->getRedundancy()).exec();
}

ArrayDistPtr SystemCatalog::getArrayDistribution(uint64_t arrDistId,
                                                 pqxx::basic_transaction* tr)
{
    string sql = "select partition_function, partition_state, redundancy"
    " from \"array_distribution\" where id = $1";

    _connection->prepare(sql, sql);

    result query_res = tr->prepared(sql)(arrDistId).exec();
    ASSERT_EXCEPTION(query_res.size()>0, "No array distribution found");

    DistType dt =
    static_cast<DistType>(query_res[0].at("partition_function").as(int()));
    LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDistribution(): arrDistId:" << arrDistId << " dt: " << dt);

    SCIDB_ASSERT(isValidDistType(dt));

    std::string state = query_res[0].at("partition_state").as(std::string());

    int redundancy = query_res[0].at("redundancy").as(int());
    SCIDB_ASSERT(redundancy>=0);

    ArrayDistPtr arrDist = ArrayDistributionFactory::getInstance()->construct(dt,redundancy,state);
    ASSERT_EXCEPTION(arrDist, "Unknown array distribution in SysCatalog");
    return arrDist;
}

void SystemCatalog::insertArrayResidency(ArrayID uaid,
                                         const ArrayResPtr& residency,
                                         pqxx::basic_transaction* tr)
{
    const string sql2 = "insert into \"array_residency\" (array_id, instance_id) values ";
    const size_t resSize = residency->size();
    SCIDB_ASSERT(resSize>0);
    static const size_t MAX_ROWS = 500;

    stringstream ss;
    ss << "(" << uaid << ",";
    const string prefix = ss.str();
    for (size_t i = 0; i < resSize; ) {
        size_t j=0;
        ss.str("");
        ss.clear();
        for (; j < MAX_ROWS && i < resSize; ++j, ++i) {
            const InstanceID inst = residency->getPhysicalInstanceAt(i);
            ss << prefix << inst <<")";
            if (j+1 < MAX_ROWS && i+1 < resSize) {
                ss << ",";
            }
        }
        result query_res = tr->exec(sql2 + ss.str());
        ASSERT_EXCEPTION((query_res.affected_rows()==j), "Unable to update array residency");
    }
}

ArrayResPtr SystemCatalog::getArrayResidency(ArrayID uaid,
                                             pqxx::basic_transaction* tr)
{
    string sql = "select instance_id "
    " from \"array_residency\" where array_id = $1 order by instance_id";

    _connection->prepare(sql, sql);
    result query_res = tr->prepared(sql)(uaid).exec();

    std::vector<InstanceID> instances;

    ASSERT_EXCEPTION(query_res.size()>0, "ArrayResidency is not found in SysCatalog");
    instances.reserve(query_res.size());

    for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
    {
        instances.push_back(i.at("instance_id").as(int64_t()));
    }
    ArrayResPtr residency = createDefaultResidency(PointerRange<InstanceID>(instances));
    return residency;
}

bool SystemCatalog::_fillAttributes(pqxx::basic_transaction* tr,
                                    const ArrayID aid,
                                    Attributes& attributes,
                                    bool ignoreOrphans)
{
    assert(_connection);
    assert(tr);
    assert(aid);

    string query =
        "select id, name, type, flags, default_compression_method, reserve, "
        "       default_missing_reason, default_value "
        "from \"array_attribute\" "
        "where array_id = $1 "
        "order by id";
    _connection->prepare(__FUNCTION__, query);
    result qres = tr->prepared(__FUNCTION__)(aid).exec();

    bool haveOrphans = false;
    for (auto cursor = qres.begin(); cursor != qres.end(); ++cursor) {
        Value defaultValue;
        int missingReason = cursor.at("default_missing_reason").as(int());
        string defaultValueExpr;
        if (missingReason >= 0) {
            defaultValue.setNull(safe_static_cast<Value::reason>(missingReason));
        } else {
            defaultValueExpr = cursor.at("default_value").as(string());
            try {
                // Do type check before constructor check in the next if.
                TypeId typeId = cursor.at("type").as(TypeId());
                defaultValue = Value(TypeLibrary::getType(typeId));

                // Evaluate expression if present and set default value
                if (!defaultValueExpr.empty())
                {
                    Expression expr = deserializePhysicalExpression(defaultValueExpr);
                    defaultValue = expr.evaluate();
                }
                // Else fallback to null or zero as before
                else
                {
                    if (cursor.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                    {
                        defaultValue.setNull();
                    }
                    else
                    {
                        defaultValue = TypeLibrary::getDefaultValue(typeId);
                    }
                }
            } catch (const scidb::Exception& e) {
                if (ignoreOrphans &&
                    (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED ||
                     e.getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND))
                {
                    // Either the attribute's user-defined type (UDT)
                    // or a user-defined function (UDF) used to
                    // compute its default value was not loaded.
                    // Sometimes we don't care: just mark the
                    // attribute invalid and move on.

                    haveOrphans = true;
                    Value tmp;
                    defaultValue.swap(tmp);
                } else {
                    e.raise();
                }
            }
        }
        AttributeDesc att(
            cursor.at("name").as(string()),
            cursor.at("type").as(TypeId()),
            cursor.at("flags").as(int16_t()),
            static_cast<CompressorType>(
                cursor.at("default_compression_method").as(uint16_t())),
            std::set<std::string>(),
            cursor.at("reserve").as(int16_t()),
            &defaultValue,
            defaultValueExpr);
        attributes.push_back(att);
    }

    return haveOrphans;
}

bool SystemCatalog::_fillDimensions(pqxx::basic_transaction* tr,
                                    const ArrayID aid,
                                    Dimensions& dimensions)
{
    assert(_connection);
    assert(tr);
    assert(aid);

    string query =
        "select name, startmin, currstart, currend, endmax, "
        "       chunk_interval, chunk_overlap "
        "from \"array_dimension\" "
        "where array_id = $1 "
        "order by id";
    _connection->prepare(__FUNCTION__, query);
    result qres = tr->prepared(__FUNCTION__)(aid).exec();

    bool isAutochunked = false;
    dimensions.reserve(qres.size());
    for (auto cursor = qres.begin(); cursor != qres.end(); ++cursor) {
        DimensionDesc dim(cursor.at("name").as(string()),
                          cursor.at("startmin").as(int64_t()),
                          cursor.at("currstart").as(int64_t()),
                          cursor.at("currend").as(int64_t()),
                          cursor.at("endmax").as(int64_t()),
                          cursor.at("chunk_interval").as(int64_t()),
                          cursor.at("chunk_overlap").as(int64_t()));
        isAutochunked |= dim.isAutochunked();
        dimensions.push_back(dim);
    }

    // Return true iff any dimension was autochunked.
    return isAutochunked;
}

std::shared_ptr<ArrayDesc> SystemCatalog::getArrayDescForDsId(const ArrayID uaid)
{
    SCIDB_ASSERT(uaid != INVALID_ARRAY_ID);
    std::function<std::shared_ptr<ArrayDesc>()> work =
        std::bind(&SystemCatalog::_getArrayDescForDsId, this, uaid);
    return Query::runRestartableWork<std::shared_ptr<ArrayDesc>, broken_connection>(work, _reconnectTries);
}

std::shared_ptr<ArrayDesc> SystemCatalog::_getArrayDescForDsId(const ArrayID uaid)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::" << __FUNCTION__ << '(' << uaid << ')');
    ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
    ScopedWaitTimer timer(PTW_SWT_PG11);
    ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

    assert(_connection);
    std::shared_ptr<ArrayDesc> newDesc;
    try
    {
        work tr(*_connection);
        newDesc = _getArrayDescByIdWithTxn(uaid,
                                           /*allowMissingPlugins:*/ true,
                                           /*wantChunkIntervals:*/ true,
                                           &tr);
        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }

    SCIDB_ASSERT(newDesc->getUAId() == uaid);
    return newDesc;
}

std::shared_ptr<ArrayDesc>
SystemCatalog::_getArrayDescByIdWithTxn(const ArrayID array_id,
                                        bool allowMissingPlugins,
                                        bool wantChunkIntervals,
                                        pqxx::basic_transaction* tr,
                                        const NamespaceDesc* ns_desc_ptr)
{
    assert(_connection && &tr->conn() == _connection);

    string sql1 = "select id, name, distribution_id, flags from \"array\" where id = $1";
    _connection->prepare("find-by-id", sql1);
    pqxx::result query_res1 = tr->prepared("find-by-id")(array_id).exec();
    if (query_res1.size() <= 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
    }

    SCIDB_ASSERT(array_id == query_res1[0].at("id").as(uint64_t()));
    string array_name = query_res1[0].at("name").as(string());
    int32_t flags = query_res1[0].at("flags").as(int());

    //array distribution should be the same as that of the unversioned array
    uint64_t arrDistId = query_res1[0].at("distribution_id").as(uint64_t());

    // We need the unversioned array id too (if this isn't it).
    VersionID vid = NO_VERSION;
    ArrayUAID uaid = array_id;
    if (isNameVersioned(array_name)) {
        vid = getVersionFromName(array_name);
        uaid = _getUaidFromVersionedAid(array_id, vid, tr);
        if (uaid == INVALID_ARRAY_ID) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST)
                << array_name;
        }
    }

    // If we don't know the namespace, go find it.
    NamespaceDesc nsDesc;
    if (!ns_desc_ptr) {
        if (!_mapArrayIdToNamespace(uaid, nsDesc, tr)) {
            nsDesc.setName("_UNKNOWN_NS");
            if (allowMissingPlugins) {
                flags |= ArrayDesc::INCOMPLETE;
            } else {
                stringstream ss;
                ss << makeQualifiedArrayName(nsDesc.getName(), array_name)
                   << ", array_id=" << array_id;
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANNOT_RESOLVE_NAMESPACE)
                    << ss.str();
            }
        }
        ns_desc_ptr = &nsDesc;
    }

    Attributes attributes;
    if (_fillAttributes(tr, array_id, attributes, allowMissingPlugins)) {
        flags |= ArrayDesc::INCOMPLETE;
    }

    Dimensions dimensions;
    bool isAutochunked = _fillDimensions(tr, array_id, dimensions);
    if (isAutochunked && wantChunkIntervals) {
        VersionDesc vDesc = _getLastVersionWithTxn(array_id, ANY_VERSION, tr);
        if (vDesc.getArrayID() != array_id) {
            SCIDB_ASSERT(vDesc.getArrayID() > array_id);
            dimensions.clear();
            isAutochunked = _fillDimensions(tr, vDesc.getArrayID(), dimensions);
            SCIDB_ASSERT(!isAutochunked);
        }
    }

    ArrayDistPtr distribution = getArrayDistribution(arrDistId, tr);
    ArrayResPtr residency = getArrayResidency(uaid, tr);

    return std::shared_ptr<ArrayDesc>(new ArrayDesc(array_id, uaid, vid,
                                                    ns_desc_ptr->getName(),
                                                    array_name,
                                                    attributes,
                                                    dimensions,
                                                    distribution,
                                                    residency,
                                                    flags));
}

/**
 * @brief Find the namespace containing the array with the given id.
 *
 * @param[in] aid array id whose namespace info is wanted
 * @param[out] nsDesc namespace descriptor
 * @param[in,out] tr Postgres transaction pointer
 * @throws PLUGIN_SYSTEM_EXCEPTION if namespaces plugin determines the
 *         array id does not exist (which should @em not happen, see below)
 * @return true iff containing namespace is found
 *
 * @details In a perfect world the namespaces plugin will be present
 * and perform the mapping (via NsComm).  All arrays must exist in a
 * namespace, so if the given array id is valid then @em some
 * namespace will be found.  If it's not valid, the plugin will throw
 * SCIDB_LE_ARRAYID_DOESNT_EXIST.
 *
 * @p It so happens that all callers of this method have already
 * validated the array id before passing it here.  Therefore, no
 * plugin exception should be thrown.  (If one does get thrown it's no
 * big deal since SCIDB_LE_ARRAYID_DOESNT_EXIST would be the correct
 * error anyway.)
 *
 * @p That covers all cases when the namespaces plugin is loaded.  If
 * it's not, NsComm throws a SCIDB_LE_PLUGIN_FUNCTION_ACCESS exception
 * and either (a) we can find the array id in the public namespace, or
 * (b) the array exists in a non-public namespace but for political
 * reasons we're not allowed to look at plugin metadata ourselves, so
 * we cannot map it and must return false.
 */
bool SystemCatalog::_mapArrayIdToNamespace(ArrayID aid,
                                           NamespaceDesc& nsDesc,
                                           pqxx::basic_transaction* tr)
{
    try {
        NsComm::findNamespaceForArray(aid, nsDesc, tr);
        return true;
    }
    catch (Exception const& e) {
        if (e.getLongErrorCode() != SCIDB_LE_PLUGIN_FUNCTION_ACCESS) {
            e.raise();
        }
    }

    // Plugin not loaded, it's either public or we have a problem.
    string key = "is-aid-in-public-ns";
    string sql = "select 1 from public_arrays where id = $1";
    tr->conn().prepare(key, sql);
    pqxx::result qres = tr->prepared(key)(aid).exec();

    if (!qres.empty()) {
        // Hurray, public.
        nsDesc.setName(rbac::PUBLIC_NS_NAME);
        nsDesc.setId(rbac::PUBLIC_NS_ID);
        return true;
    }

    LOG4CXX_DEBUG(logger, "" << __FUNCTION__
                  << ": Cannot find namespace for array_id=" << aid);
    return false;
}

    bool SystemCatalog::deleteArray(const string& ns_name, const string& array_name)
    {
        std::function<bool()> work1 = std::bind(&SystemCatalog::_deleteArrayByName,
                                                this,
                                                std::cref(ns_name),
                                                std::cref(array_name));
        std::function<bool()> work2 = std::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);
        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayByName(const string& nsName, const string& arrayName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( name = " << arrayName << ")");

        SCIDB_ASSERT(!nsName.empty());
        SCIDB_ASSERT(!arrayName.empty());
        SCIDB_ASSERT(!isQualifiedArrayName(arrayName));

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG12);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        bool rc = false;
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            // Translate (ns, array) to unversioned array_id.
            FindArrayArgs args;
            FindArrayResult result;
            args.nsNamePtr = &nsName;
            args.arrayNamePtr = &arrayName;
            bool found = _findArrayWithTxn(args, result, &tr);
            if (!found) {
                // Nothing deleted (because there's nothing to delete).
                return false;
            }

            // Delete the uaid and all of its versioned ids.
            string sql =
                "delete from \"array\" where id = $1 or id = "
                "    any(select version_array_id from array_version "
                "        where array_id = $1)";
            string key("delete-array-all-versions");
            _connection->prepare(key, sql);
            pqxx::result qres = tr.prepared(key)(result.arrayId).exec();
            ASSERT_EXCEPTION(qres.affected_rows() > 0,
                             "Found array to delete, but no rows deleted");
            rc = true;          // Success!

            // XXX TODO: these queries are a bit heavy, need to optimize
            const string deleteResidencySql =
                "delete from \"array_residency\" as RES where not exists "
                "( select 1 from \"array\" as ARR where ARR.id=RES.array_id)";

            _connection->prepare(deleteResidencySql, deleteResidencySql);
            pqxx::result query_res_res = tr.prepared(deleteResidencySql).exec();
            bool rc_res = (query_res_res.affected_rows() > 0);
            SCIDB_ASSERT(!isNameUnversioned(arrayName) || rc_res);

            const string deleteDistributionSql =
                "delete from \"array_distribution\" as DIST where not exists "
                "( select 1 from \"array\" as ARR where ARR.distribution_id=DIST.id)";

            _connection->prepare(deleteDistributionSql, deleteDistributionSql);
            pqxx::result query_res_dist = tr.prepared(deleteDistributionSql).exec();
            bool rc_dist = (query_res_dist.affected_rows() > 0);
            //XXX TODO: until we use the uaid to identify distributions
            SCIDB_ASSERT(!isNameUnversioned(arrayName) || rc_dist);

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return rc;
    }

    bool SystemCatalog::deleteArrayVersions(const std::string &ns_name,
                                            const std::string &array_name,
                                            const VersionID array_version)
    {
        std::function<bool()> work1 = std::bind(&SystemCatalog::_deleteArrayVersions,
                                                this,
                                                std::cref(ns_name),
                                                std::cref(array_name),
                                                array_version);
        std::function<bool()> work2 = std::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayVersions(const std::string& ns_name,
                                             const std::string& array_name,
                                             const VersionID array_version)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayVersions( array_name = " <<
                      array_name << ", array_version = " << array_version << ")");

        SCIDB_ASSERT(!ns_name.empty());

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG13);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        bool rc = false;
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            // Translate (ns, array) to unversioned array_id.
            FindArrayArgs args;
            FindArrayResult result;
            args.nsNamePtr = &ns_name;
            args.arrayNamePtr = &array_name;
            bool found = _findArrayWithTxn(args, result, &tr);
            if (!found) {
                // Nothing deleted (because there's nothing to delete).
                return false;
            }

            // Delete all versions less than the given array_version.
            string sql =
                "delete from \"array\" where id = "
                "    any(select version_array_id from array_version "
                "        where array_id = $1 and version_id < $2)";
            string key("delete-some-array-versions");
            _connection->prepare(key, sql);
            pqxx::result qres = tr.prepared(key)
                (result.arrayId)(array_version).exec();
            rc = qres.affected_rows() > 0;
            tr.commit();
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: "
                          << array_name << " version:" << array_version);

            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return rc;
    }

    void SystemCatalog::deleteArray(const ArrayID array_id)
    {
        std::function<void()> work1 = std::bind(&SystemCatalog::_deleteArrayById,
                                                this,
                                                array_id);
        std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                work1,
                                                _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    void SystemCatalog::_deleteArrayById(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG14);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            // Lock the latest array version table for this whole transaction.
            std::string lockSql = "lock table latest_array_version";
            tr.exec(lockSql);

            std::ostringstream nnanSql;
            nnanSql << "select namespace_name, substring(array_name,'([^@]+).*') as array_name "
                "from namespace_arrays where array_id="
                    << array_id;
            auto nnanResult = tr.exec(nnanSql.str());
            std::string namespaceName;
            std::string arrayName;
            if (!nnanResult.empty()) {
                SCIDB_ASSERT(nnanResult.size() == 1);
                namespaceName = nnanResult[0].at("namespace_name").as(string());
                arrayName = nnanResult[0].at("array_name").as(string());
            }

            const string sql1 = "delete from \"array\" where id = $1";
            _connection->prepare("delete-array-id", sql1);
            result query_res = tr.prepared("delete-array-id")(array_id).exec();

            if (query_res.affected_rows() > 0) {
                // XXX TODO: these queries are a bit heavy, need to optimize
                const string deleteResidencySql =
                   "delete from \"array_residency\" as RES where not exists "
                   "( select 1 from \"array\" as ARR where ARR.id=RES.array_id)";

                _connection->prepare(deleteResidencySql, deleteResidencySql);
                result query_res_res = tr.prepared(deleteResidencySql).exec();
                LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById(): removed residency rows="
                              << query_res_res.affected_rows());

                const string deleteDistributionSql =
                   "delete from \"array_distribution\" as DIST where not exists "
                   "( select 1 from \"array\" as ARR where ARR.distribution_id=DIST.id)";

                _connection->prepare(deleteDistributionSql, deleteDistributionSql);
                result query_res_dist = tr.prepared(deleteDistributionSql).exec();
                LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById(): removed distribution rows="
                              << query_res_dist.affected_rows());

                // Finally, update the latest array version table.
                std::ostringstream deleteSql;
                deleteSql << "delete from latest_array_version where namespace_name='"
                          << namespaceName << "' and array_name='" << arrayName << "'";
                tr.exec(deleteSql.str());

                std::ostringstream updateSql;
                updateSql <<
                    "insert into latest_array_version ("
                    "  with mids as ("
                    "    select ARR.namespace_name, "
                    "           substring(ARR.array_name,'([^@]+).*') as arr_name, "
                    "           max(ARR.array_id) as max_arr_id "
                    "           from namespace_arrays as ARR group by namespace_name, arr_name"
                    "  ) select namespace_name, arr_name as array_name, max_arr_id as array_id "
                    "           from mids where namespace_name='"
                    << namespaceName << "' and arr_name='" << arrayName << "')";
                tr.exec(updateSql.str());
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::_createNewVersion(const ArrayID array_id,
                                               const ArrayID version_array_id,
                                               pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_createNewVersion( array_id = " << array_id << ")");
        VersionID version_id = (VersionID)-1;

        string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
        _connection->prepare(sql, sql);
        result query_res = tr->prepared(sql)(array_id).exec();
        version_id = query_res[0].at("vid").as(uint64_t());

        version_id += 1;

        string sql1 = "insert into \"array_version\"(array_id, version_array_id, version_id, time_stamp)"
        " values ($1, $2, $3, $4)";
        int64_t timestamp = time(NULL);
        _connection->prepare(sql1, sql1);
        tr->prepared(sql1)(array_id)(version_array_id)(version_id)(timestamp).exec();

        return version_id;
    }

    void SystemCatalog::deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        // TODO This method is unnecessary, to be addressed under SDB-5696.

        std::function<void()> work = std::bind(&SystemCatalog::_deleteVersion,
                                               this,
                                               array_id,
                                               version_id);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        // TODO This method is unnecessary, to be addressed under SDB-5696.

        LOG4CXX_TRACE(logger, "SystemCatalog::deleteVersion( array_id = " << array_id << ", version_id = " << version_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG15);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try
        {
            work tr(*_connection);

            _connection->prepare("delete-version",
                                 "delete from \"array_version\" where array_id=$1 and version_id = $2");
            tr.prepared("delete-version")(array_id)(version_id).exec();
            _connection->unprepare("delete-version");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::getLastVersion(const ArrayID array_id,
                                            const ArrayID catalogVersion)
    {
        std::function<VersionID()> work = std::bind(&SystemCatalog::_getLastVersion,
                                                    this,
                                                    array_id,
                                                    catalogVersion);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::getOldestArrayVersion(const ArrayID id)
    {
        std::function<VersionID()> work = std::bind(&SystemCatalog::_getOldestArrayVersion,
                                                    this,
                                                    id);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

/*
** TODO: We will need to rework this so that we only need to go back to the
**       persistent meta-data store when the local cache is invalidated
**       due to a new version being created with an UPDATE. This can be
**       piggy-backed on a "heartbeat" message, which contains the latest
**       lamport clock value. Meta-data additions to the persistent store
**       will increment the lamport clock value, which will be propogated
**       throughout the instances. When the local instance's clock value < the
**       'global', and the local is obliged to check it's local catalogs,
**       then it can reload meta-data from the persistent store.
**
** Some additional background: We currently leverage Postgres to
** achieve ACID properties for SciDB.  The catalogVersion is the
** version of an array as it should be seen by a particular query,
** that is, the versioned array id of the array at the time that the
** query obtained a COORD lock for it.  Here and elsewhere,
** catalogVersion serves as a "stake in the ground" to make sure the
** query sees one isolated view of the array unmolested by any
** concurrently executing queries.  Hence the talk of Lamport clocks
** above, which foreshadows a time (coming Real Soon Now(tm)!) when we
** remove our dependency on Postgres and begin using our own
** OrderedBcast code to ensure isolation.
**
** If you really do want the absolute latest version without reference
** to any active query, use catalogVersion == ANY_VERSION (and *NOT*
** LAST_VERSION, see comment in _getLastVersionWithTxn() below!).
*/
VersionID SystemCatalog::_getLastVersion(const ArrayID array_id,
                                         const ArrayID catalogVersion)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::getLastVersion( array_id = " << array_id
                  <<", catalogVersion = "<<catalogVersion
                  << ")");

    ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG16);
    ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

    assert(_connection);

    try
    {
        work tr(*_connection);
        VersionID version_id = _getLastVersionWithTxn(array_id, catalogVersion, &tr).getVersionID();
        tr.commit();
        return version_id;
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }
    return 0;
}

VersionDesc SystemCatalog::_getLastVersionWithTxn(ArrayID array_id,
                                                  ArrayID catalogVersion,
                                                  pqxx::basic_transaction* tr)
{
    SCIDB_ASSERT(tr);

    // Guard against confusion.  ANY_VERSION is a large positive
    // ArrayID that makes a fine catalogVersion for getting the very
    // latest version of an array regardless of what query holds which
    // locks.  But the like-named LAST_VERSION is (VersionID)-1 and
    // although VersionID is unsigned, a Postgres bigint *IS* signed
    // and so the SQL here winds up returning no results.  I hope this
    // teaches lessons re. C-style casts, choice of names, etc. etc.
    SCIDB_ASSERT(catalogVersion != LAST_VERSION);

    string const key = "select-last-version";
    string sql =
        "select version_array_id, version_id, time_stamp "
        "from array_version "
        "where array_id = $1 and version_array_id <= $2 "
        "order by version_id desc "
        "limit 1";
    _connection->prepare(key, sql);
    pqxx::result query_res = tr->prepared(key)(array_id)(catalogVersion).exec();

    if (query_res.empty()) {
        return VersionDesc(array_id, 0, 0);
    } else {
        SCIDB_ASSERT(query_res.size() == 1); // Since we said "limit 1".
        return VersionDesc(query_res[0].at("version_array_id").as(uint64_t()),
                           query_res[0].at("version_id").as(uint64_t()),
                           query_res[0].at("time_stamp").as(time_t()));
    }
    SCIDB_UNREACHABLE();
}

    void
    SystemCatalog::getCurrentVersion(QueryLocks& locks)
    {
        std::function<void()> work1 = std::bind(&SystemCatalog::_getCurrentVersion,
                                                this,
                                                std::ref(locks));
        std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    namespace {
        class LockDescMap
        {
            // Maps fully-qualified array names to instances of LockDesc objects.
            std::unordered_map<std::string, LockDesc*> _lockDescriptors;

            std::string _createKey(const std::string& namespace_name,
                                   const std::string& array_name) const
            {
                return namespace_name + '.' + array_name;
            }

        public:
            auto empty() const
            {
                return _lockDescriptors.empty();
            }

            auto end() const
            {
                return _lockDescriptors.end();
            }

            void insert(const std::string& namespace_name,
                        const std::string& array_name,
                        LockDesc* lockDesc)
            {
                auto key = _createKey(namespace_name, array_name);
                _lockDescriptors[key] = lockDesc;
            }

            auto find(const std::string& namespace_name,
                      const std::string& array_name) const
            {
                auto key = _createKey(namespace_name, array_name);
                return _lockDescriptors.find(key);
            }

            auto size() const
            {
                return _lockDescriptors.size();
            }
        };
    }  // anonymous namespace

    void SystemCatalog::_getCurrentVersion(QueryLocks& locks)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getCurrentVersion()");

        assert(locks.size() > 0);

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG17);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            ostringstream sql;
            sql << "select namespace_name, array_name, array_id "
                   "from latest_array_version "
                   "where ";

            LockDescMap name2lock;
            for (const auto& lock : locks) {
                sql << (name2lock.empty() ? "" : "or ")
                    << "(namespace_name='"
                    << lock->getNamespaceName()
                    << "' and array_name='"
                    << lock->getArrayName()
                    << "') ";
                name2lock.insert(lock->getNamespaceName(),
                                 lock->getArrayName(),
                                 lock.get());
            }

            // prepare & execute PG txn
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            result query_res = tr.exec(sql.str());

            // update the query locks with the max catalog array ids
            assert(locks.size()>=query_res.size());
            assert(name2lock.size()==locks.size());

            // Walk each query result, updating the corresponding lock with the array ID
            // of that array's latest version.
            for (result::const_iterator i = query_res.begin(); i != query_res.end();  ++i)
            {
                const ArrayID maxArrayId    = i.at("array_id").as(uint64_t());
                assert(maxArrayId>0);
                const string& namespaceName = i.at("namespace_name").as(string());
                const string& arrayName     = i.at("array_name").as(string());

                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): namespaceName= " << namespaceName);
                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): arrayName= " << arrayName);
                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): maxArrayId= " << maxArrayId);

                auto iter = name2lock.find(namespaceName, arrayName);
                ASSERT_EXCEPTION(iter != name2lock.end(), "SystemCatalog::_getCurrentVersion(): invalid array name");

                LockDesc* lock = (*iter).second;
                assert(lock);

                ASSERT_EXCEPTION(lock->isLocked(), "Array lock is not locked");
                ASSERT_EXCEPTION(arrayName == lock->getArrayName(),
                                 "Catalog array name does not match the name in LockDesc");

                // Assigns the array ID of the latest version of the array pertaining to
                // this lock back to this lock as the array catalog ID.  The latest array ID
                // is the latest known ID to the catalog.  This is the only place where this
                // is called.
                lock->setArrayCatalogId(maxArrayId);
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::recordProvisionalEntry(ArrayUAID uaid,
                                               ArrayID aid,
                                               VersionID vid,
                                               const InstanceID& instanceID,
                                               const QueryID& multiqueryID)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        pqxx::transaction<pqxx::serializable> tr(*_connection);
        std::string lock_sql = "lock table provisional_arrays";
        tr.exec(lock_sql);
        std::ostringstream insert_sql;
        insert_sql << "insert into provisional_arrays"
                      " (array_id, array_version_id, array_version, instance_id, multiquery_id)"
                      " values ("
                   << uaid << ","
                   << aid << ","
                   << vid << ","
                   << instanceID << ","
                   << multiqueryID << ")";
        tr.exec(insert_sql.str());
        tr.commit();
    }

    void SystemCatalog::removeProvisionalEntry(ArrayUAID uaid,
                                               ArrayID aid,
                                               VersionID vid,
                                               const InstanceID& instanceID)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        pqxx::transaction<pqxx::serializable> tr(*_connection);
        std::string lock_sql = "lock table provisional_arrays";
        tr.exec(lock_sql);
        std::ostringstream remove_sql;
        remove_sql << "delete from provisional_arrays where "
                   << " array_id = " << uaid
                   << " and array_version_id = " << aid
                   << " and array_version = " << vid
                   << " and instance_id = " << instanceID;
        tr.exec(remove_sql.str());
        tr.commit();
    }

    void SystemCatalog::readProvisionalEntries(const InstanceID& instanceId,
                                               ProvArrays& provArrs)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        pqxx::transaction<pqxx::serializable> tr(*_connection);
        std::string lock_sql = "lock table provisional_arrays";
        tr.exec(lock_sql);
        std::ostringstream read_sql;
        read_sql << "select array_id, array_version_id, array_version "
                    "from provisional_arrays where instance_id = "
                 << instanceId;
        auto query_res = tr.exec(read_sql.str());
        for (size_t i = 0; i < query_res.size(); ++i) {
            provArrs.emplace_back(query_res[i].at("array_id").as(ArrayUAID()),
                                  query_res[i].at("array_version_id").as(ArrayID()),
                                  query_res[i].at("array_version").as(VersionID()));
        }
        tr.commit();
    }

    void SystemCatalog::readProvisionalEntries(const QueryID& multiqueryID,
                                               ProvArrays& provArrs)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            std::ostringstream read_sql;
            read_sql << "select array_id, array_version_id, array_version "
                "from provisional_arrays where multiquery_id = '"
                     << multiqueryID << "'";
            auto query_res = tr.exec(read_sql.str());
            for (size_t i = 0; i < query_res.size(); ++i) {
                provArrs.emplace_back(query_res[i].at("array_id").as(ArrayUAID()),
                                      query_res[i].at("array_version_id").as(ArrayID()),
                                      query_res[i].at("array_version").as(VersionID()));
            }
            tr.commit();
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    bool SystemCatalog::hasProvisionalEntries(const QueryID& parentQueryID)
    {
        ProvArrays provArrays;
        readProvisionalEntries(parentQueryID, provArrays);
        auto hasEntries = !provArrays.empty();
        return hasEntries;
    }

    ArrayDesc SystemCatalog::getLatestVersion(std::string const& namespaceName,
                                              std::string const& arrayName,
                                              ArrayID arrayId)
    {
        ArrayDesc latestVersionDesc;
        SystemCatalog::GetArrayDescArgs latestVersionArgs;
        latestVersionArgs.result = &latestVersionDesc;
        latestVersionArgs.arrayName = arrayName;
        latestVersionArgs.nsName = namespaceName;
        latestVersionArgs.throwIfNotFound = true;
        latestVersionArgs.catalogVersion = arrayId;
        latestVersionArgs.versionId = LAST_VERSION;
        auto foundLatestVersion = getArrayDesc(latestVersionArgs);
        SCIDB_ASSERT(foundLatestVersion);
        return latestVersionDesc;
    }

    // These are scoped to this file as they're needed only here.
    static constexpr const char* CATALOG_NAMESPACE = "%%catalog%%";
    static constexpr const char* GLOBAL_ARRAY_LOCK = "%%global_array_lock%%";

    bool SystemCatalog::_hasGlobalArrayLock(pqxx::work& tr)
    {
        std::string sql = "select count(*) from array_version_lock where namespace_name='";
        sql.append(CATALOG_NAMESPACE)
           .append("' and array_name='")
           .append(GLOBAL_ARRAY_LOCK)
           .append("'");
        auto result = tr.exec(sql);
        return (result.size() > 0) &&
               (result[0].at("count").as(uint64_t()) > 0);
    }

    bool SystemCatalog::_isGlobalArrayLock(std::shared_ptr<LockDesc> lockDesc) const
    {
        SCIDB_ASSERT(lockDesc);
        return lockDesc->getNamespaceName() == CATALOG_NAMESPACE &&
               lockDesc->getArrayName() == GLOBAL_ARRAY_LOCK;
    }

    void SystemCatalog::_lockAVLTable(pqxx::work& tr) const
    {
        SCIDB_ASSERT(_connection);
        static const std::string lockTableSql{"LOCK TABLE array_version_lock"};
        _connection->prepare(lockTableSql, lockTableSql);
        tr.prepared(lockTableSql).exec();
    }

    std::shared_ptr<LockDesc> SystemCatalog::createGlobalArrayLock() const
    {
        auto queryId = QueryID::getGlobalArrayLockQueryId();
        const auto role = LockDesc::COORD;
        const auto lockMode = LockDesc::XCL;
        auto arrayLock = std::make_shared<LockDesc>(CATALOG_NAMESPACE,
                                                    GLOBAL_ARRAY_LOCK,
                                                    queryId,
                                                    queryId.getCoordinatorId(),
                                                    role,
                                                    lockMode);
        return arrayLock;
    }

    ArrayID SystemCatalog::_getOldestArrayVersion(const ArrayID id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getOldestArrayVersion( id = " << id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG18);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            string sql =
                "select COALESCE(min(version_array_id),0) as vid from \"array_version\" where array_id=$1";
            _connection->prepare("select-oldest-version", sql);
            result query_res = tr.prepared("select-oldest-version")(id).exec();
            ArrayID array_version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return array_version_id;
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: "
                          << " arrayId:" << id);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    VersionID SystemCatalog::lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        std::function<VersionID()> work = std::bind(&SystemCatalog::_lookupVersionByTimestamp,
                                                    this,
                                                    array_id,
                                                    timestamp);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    VersionID SystemCatalog::_lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::lookupVersionByTimestamp( array_id = " << array_id << ", timestamp = " << timestamp << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG19);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        try
        {
            work tr(*_connection);

            string sql =
                "SELECT COALESCE(max(version_id),0) AS vid "
                "FROM array_version WHERE array_id = $1 AND time_stamp <= $2";
            _connection->prepare("select-version-by-timestamp", sql);
            result query_res = tr.prepared("select-version-by-timestamp")(array_id)(timestamp).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    std::vector<VersionDesc> SystemCatalog::getArrayVersions(const ArrayID array_id)
    {
        std::function<std::vector<VersionDesc>()> work = std::bind(&SystemCatalog::_getArrayVersions,
                                                                   this,
                                                                   array_id);
        return Query::runRestartableWork<std::vector<VersionDesc>, broken_connection>(work,
                                                                                      _reconnectTries);
    }

    std::vector<VersionDesc> SystemCatalog::_getArrayVersions(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayVersions( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG20);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            string sql =
                "SELECT version_array_id, version_id, time_stamp "
                "FROM array_version WHERE array_id = $1 ORDER BY version_id";

            _connection->prepare("select-all-versions", sql);
            result query_res = tr.prepared("select-all-versions")(array_id).exec();
            std::vector<VersionDesc> versions(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                versions[j++] = VersionDesc(i.at("version_array_id").as(uint64_t()),
                                            i.at("version_id").as(uint64_t()),
                                            i.at("time_stamp").as(time_t()));
            }
            tr.commit();
            return versions;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return std::vector<VersionDesc>();
    }

    Coordinates SystemCatalog::getHighBoundary(const ArrayID array_id)
    {
        std::function<Coordinates()> work = std::bind(&SystemCatalog::_getHighBoundary,
                                                      this,
                                                      array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getHighBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getHighBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG21);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            string sql = "select currEnd from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-high-boundary", sql);
            result query_res = tr.prepared("select-high-boundary")(array_id).exec();
            Coordinates highBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                highBoundary[j++] = i.at("currEnd").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return highBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    Coordinates SystemCatalog::getLowBoundary(const ArrayID array_id)
    {
        std::function<Coordinates()> work = std::bind(&SystemCatalog::_getLowBoundary,
                                                      this,
                                                      array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getLowBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLowBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG22);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            string sql = "select currStart from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-low-boundary", sql);
            result query_res = tr.prepared("select-low-boundary")(array_id).exec();
            Coordinates lowBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                lowBoundary[j++] = i.at("currStart").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return lowBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    void SystemCatalog::updateArrayBoundariesAndIntervals(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        std::function<void()> work = std::bind(&SystemCatalog::_updateArrayBoundariesAndIntervals,
                                               this,
                                               std::cref(desc),
                                               std::ref(bounds));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_updateArrayBoundariesAndIntervals(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        PhysicalBoundaries trimmed = bounds.trimToDims(desc.getDimensions());
        Coordinates const& low = trimmed.getStartCoords();
        Coordinates const& high = trimmed.getEndCoords();
        ArrayID array_id = desc.getId();
        Dimensions const& dims = desc.getDimensions();

        LOG4CXX_DEBUG(logger, "SystemCatalog::updateArrayBoundaries( array_id = " << desc.getId()
                      << ", low = [" << low << "], high = [" << high << "])");

        SCIDB_ASSERT(low.size() == high.size());
        SCIDB_ASSERT(low.size() == desc.getDimensions().size());
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG23);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            string sql1 =
                "UPDATE array_dimension "
                "SET currstart = $1, currend = $2 "
                "WHERE array_id = $3 AND id = $4";
            _connection->prepare("update-boundary", sql1);

            string sql2 =
                "UPDATE array_dimension SET chunk_interval = $1 "
                "WHERE array_id = $2 AND id = $3 AND chunk_interval = -1";
            _connection->prepare("update-chunk-interval", sql2);

            // Don't assert(affected_rows() == 1) below, since this
            // can be zero for either SQL statement (bounds didn't
            // change, or chunk_interval already set).

            for (size_t i = 0, n = low.size(); i < n; i++) {
                tr.prepared("update-boundary")(low[i])(high[i])(array_id)(i).exec();
                tr.prepared("update-chunk-interval")(dims[i].getChunkInterval())(array_id)(i).exec();
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::updateTransientAttributes(ArrayDesc const& desc)
    {
        SCIDB_ASSERT(desc.isTransient());
        std::function<void()> work = std::bind(&SystemCatalog::_updateTransientAttributes,
                                               this,
                                               std::cref(desc));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_updateTransientAttributes(ArrayDesc const& desc)
    {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG23);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        try
        {
            work tr(*_connection);

            tr.exec("delete from array_attribute where array_id = " + std::to_string(desc.getId()));

            string sql3 =
                "insert into \"array_attribute\" ( "
                "    array_id, id, name, type, flags, default_compression_method, "
                "    reserve, default_missing_reason, default_value) "
                "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
            _connection->prepare(sql3, sql3);

            const auto& attributes = desc.getAttributes();
            for (const auto& attr : attributes)
            {
                tr.prepared(sql3)
                    (desc.getId())
                    (attr.getId())
                    (attr.getName())
                    (attr.getType())
                    (attr.getFlags())
                    (toUType(attr.getDefaultCompressionMethod()))
                    (attr.getReserve())
                    (static_cast<int>(attr.getDefaultValue().getMissingReason()))
                    (attr.getDefaultValueExpr()).exec();
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    uint32_t SystemCatalog::getNumberOfInstances()
    {
        std::function<uint32_t()> work = std::bind(&SystemCatalog::_getNumberOfInstances,
                                                   this);
        return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
    }

    uint32_t SystemCatalog::_getNumberOfInstances()
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNumberOfInstances()");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG24);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        uint32_t n_instances;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select count(*) as cnt from \"instance\"");
            n_instances = query_res[0].at("cnt").as(uint32_t());
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return n_instances;
    }

    InstanceID SystemCatalog::addInstance(const InstanceDesc &instance,
                                          const std::string& online)
    {
        std::function<InstanceID()> work = std::bind(&SystemCatalog::_addInstance,
                                                     this,
                                                     std::cref(instance),
                                                     std::cref(online));
        return Query::runRestartableWork<InstanceID, broken_connection>(work, _reconnectTries);
    }

    InstanceID SystemCatalog::_addInstance(const InstanceDesc &instance,
                                           const std::string& online)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addInstance( " << instance << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG25);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        int64_t instance_id = 0;
        try
        {
            work tr(*_connection);

            // Q: Why does instance_id use a Postgres sequence?
            // A: See wiki:DEV/Command_Control/Func+spec+15.12
            string sql1 =
                "insert into \"instance\" "
                "    (instance_id, host, port, online_since, base_path, server_id, server_instance_id) "
                "values ( (($5::bigint << 32) | (nextval('instance_id_seq') & (x'00FFFFFFFF'::bigint))), "
                "         $1, $2, $3, $4, $5, $6)";

            _connection->prepare(sql1, sql1);
            result query_res = tr.prepared(sql1)
                    (instance.getHost())
                    (instance.getPort())
                    (online)
                    (instance.getBasePath())
                    (instance.getServerId())
                    (instance.getServerInstanceId()).exec();

            size_t numRows = query_res.affected_rows();

            ASSERT_EXCEPTION( numRows == 1, "Instance not added");

            string sql2 = "select instance_id from \"instance\" where server_id=$1 and server_instance_id=$2";

            _connection->prepare(sql2, sql2);

            query_res = tr.prepared(sql2)
                    (instance.getServerId())
                    (instance.getServerInstanceId()).exec();

            numRows = query_res.size();

            ASSERT_EXCEPTION( numRows == 1, "Instance not added");

            instance_id = query_res[0].at("instance_id").as(int64_t());

            ASSERT_EXCEPTION(isValidPhysicalInstance(static_cast<InstanceID>(instance_id)),
                             "Invalid instance ID inserted");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return instance_id;
    }

    MembershipID SystemCatalog::getInstances(Instances &instances)
    {
        std::function<MembershipID()> work = std::bind(&SystemCatalog::_getInstances,
                                                       this,
                                                       std::ref(instances),
                                                       false);
        return Query::runRestartableWork<MembershipID, broken_connection>(work, _reconnectTries);
    }

    MembershipID SystemCatalog::getAllInstances(Instances &instances)
    {
        std::function<MembershipID()> work = std::bind(&SystemCatalog::_getInstances,
                                                       this,
                                                       std::ref(instances),
                                                       true);
        return Query::runRestartableWork<MembershipID, broken_connection>(work, _reconnectTries);
    }

    MembershipID SystemCatalog::_getInstances(Instances &instances, bool all) // 查询pg数据库，获取所有的instance
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getInstances()");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG26);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        MembershipID membershipId(0);
        try
        {
            work tr(*_connection); //Dont need serializability because of the table lock

            const string lockTableSql = "LOCK TABLE instance";
            tr.exec(lockTableSql);

            const string sql1 = "select id from membership";
            result query_res = tr.exec(sql1);
            ASSERT_EXCEPTION(query_res.size() == 1, "Membership table has more/less than 1 row");
            membershipId = query_res[0].at("id").as(int64_t());

            string sql2 =
                "SELECT instance_id, membership_id, host, port, "
                "       date_part('epoch', online_since)::bigint as ts,"
                "       isfinite(online_since) as fin, "
                "       base_path, server_id, server_instance_id "
                "FROM instance ";
            if (!all) {
                sql2 += " WHERE membership_id >= 0 AND isfinite(online_since)";
            }
            sql2 += " order by instance_id";
            query_res = tr.exec(sql2);
            ASSERT_EXCEPTION(query_res.size() > 0, "No instances online");

            if (query_res.size() > 0)
            {
                instances.reserve(query_res.size());
                for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
                {
                    double ts = InstanceDesc::INFINITY_TS;
                    if (i.at("fin").as(bool())) {
                        ts = i.at("ts").as(double());
                    }

                    MembershipID mId = i.at("membership_id").as(int64_t());
                    SCIDB_ASSERT (mId < std::numeric_limits<int64_t>::max() ||
                                  mId == InstanceDesc::INVALID_MEMBERSHIP_ID);

                    instances.push_back(
                        InstanceDesc(
                            i.at("instance_id").as(InstanceID()),
                            mId,
                            i.at("host").as(string()),
                            i.at("port").as(uint16_t()),
                            ts,
                            i.at("base_path").as(string()),
                            i.at("server_id").as(uint32_t()),
                            i.at("server_instance_id").as(uint32_t())
                                ));

                }
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                  << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Retrieved " << instances.size()
                      << " instances from catalogs. MembershipID="
                      << membershipId);
        return membershipId;
    }

    InstanceID SystemCatalog::getPhysicalInstanceId(int32_t sid, int32_t siid)
    {
        if (sid < 0) {
            throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PARAMETER_OUT_OF_RANGE)
                << "server_id" << 0 << std::numeric_limits<int32_t>::max();
        }
        if (siid < 0) {
            throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PARAMETER_OUT_OF_RANGE)
                << "server_instance_id" << 0 << std::numeric_limits<int32_t>::max();
        }
        std::function<InstanceID()> work = std::bind(&SystemCatalog::_getPhysInstId, this, sid, siid);
        return Query::runRestartableWork<InstanceID, broken_connection>(work, _reconnectTries);
    }

    InstanceID SystemCatalog::_getPhysInstId(int32_t sid, int32_t siid)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getPhysicalInstanceId(" << sid << ',' << siid << ')');

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG26_A);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);
        InstanceID result { INVALID_INSTANCE };
        try
        {
            work tr(*_connection);

            // The membership_id and online_since predicates here
            // allegedly restrict the results to current cluster
            // members only (as opposed to, perhaps, newly registered
            // but not yet started instances...?).
            string sql =
                "select instance_id from instance "
                "where server_id = $1 and server_instance_id = $2 "
                "  and membership_id >= 0 and isfinite(online_since)";

            _connection->prepare(sql, sql);
            pqxx::result qres = tr.prepared(sql)(sid)(siid).exec();
            ASSERT_EXCEPTION(qres.size() < 2,
                             "Instance table has multiple rows for (sid,siid)");
            if (qres.size() == 1) {
                result = qres[0].at("instance_id").as(int64_t());
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                  << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "SystemCatalog::getPhysicalInstanceId("
                      << sid << ',' << siid << ") --> " << Iid(result));
        return result;
    }

    /**
     * Find a particular parameter within a Postgres connection string.
     *
     * @param creds connection string to examine
     * @param key parameter to search for, including trailing '='
     * @return value of parameter
     *
     * @note
     * We don't support spaces around = in key = value pairs.  We also brashly assume that the
     * embedded values won't be quoted and won't contain funny characters... which *should*
     * certainly be true for host, port, dbname, and user parameters.
     */
    string SystemCatalog::_findCredParam(const string& creds, const string& key)
    {
        assert(key.at(key.size() - 1) == '=');
        string::size_type pos = creds.find(key);
        if (pos == string::npos) {
            LOG4CXX_DEBUG(logger, __FUNCTION__ << ": '" << key << "' not found");
            return string();
        }
        string rest(creds.substr(pos));         // "key1=v1 key2=v2 ..."
        rest = rest.substr(rest.find('=') + 1); // "v1 key2=v2 ..."
        if (rest.empty()) {
            // Hilarious.
            LOG4CXX_DEBUG(logger, __FUNCTION__ << ": '" << key << "' is empty");
            return string("''");
        }
        pos = rest.find_first_of(" \t");
        return rest.substr(0, pos); // Logged below in _makeCredentials().
    }

    /**
     * Build a Postgres connection string.
     */
    string SystemCatalog::_makeCredentials()
    {
        // RESIST THE TEMPTATION TO WRITE PASSWORDS INTO THE LOG!!!

        // (We could easily cache the result of all this work in a
        // static: at Postgres connection time we are still running
        // single-threaded, so no locking needed.  But... we are only
        // called once, so why bother.)

        Config& config = *Config::getInstance();

        // Backward compatibility: if given a password, use it... but complain.
        string creds = config.getOption<string>(CONFIG_CATALOG);
        if (creds.find("password=") != string::npos) {
            // Password in cleartext on the command line?!  BAD!!!
            LOG4CXX_WARN(logger, "Postgres password provided in cleartext on command line, how embarrassing!");
            return creds;
        }

        // Find the pgpass file: first config.ini, then environment, then $HOME
        string pgpass_file(config.getOption<string>(CONFIG_PGPASSFILE));
        if (pgpass_file.empty()) {
            const char *pgpass_env = ::getenv("PGPASSFILE");
            if (pgpass_env) {
                pgpass_file = pgpass_env;
            } else {
                struct passwd* pw = ::getpwuid(::getuid());
                if (pw == NULL) {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG)
                        << "Cannot find my own /etc/passwd entry?!";
                }
                assert(pw->pw_dir);
                pgpass_file = pw->pw_dir;
                pgpass_file += "/.pgpass";
            }
        }
        LOG4CXX_DEBUG(logger, __FUNCTION__ << ": Postgres credentials file is "
                      << pgpass_file)

        // Must be mode 600 and owned by the scidb user.
        struct stat stat;
        int rc = ::stat(pgpass_file.c_str(), &stat);
        if (rc < 0) {
            stringstream ss;
            ss << "Cannot stat('" << pgpass_file << "'): " << ::strerror(errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }
        if (stat.st_uid != ::getuid()
            || !S_ISREG(stat.st_mode)
            || (stat.st_mode & (S_IRWXG|S_IRWXO)))
        {
            stringstream ss;
            ss << "Permission check failed on " << pgpass_file;
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }

        // Parse the partial creds to figure out what to look for in ~/.pgpass .
        string host(_findCredParam(creds, "host="));
        string port(_findCredParam(creds, "port="));
        string dbname(_findCredParam(creds, "dbname="));
        string user(_findCredParam(creds, "user="));

        // Build a partial .pgpass line to search for.
        // See http://www.postgresql.org/docs/9.3/interactive/libpq-pgpass.html
        stringstream srch;
        srch << host << ':' << port << ':' << dbname << ':' << user << ':';
        string search(srch.str());
        LOG4CXX_DEBUG(logger, __FUNCTION__ << ": Search for '" << search << '\'');

        // Search for it!
        FILE *fp = ::fopen(pgpass_file.c_str(), "r");
        if (fp == NULL) {
            stringstream ss;
            ss << "Cannot fopen('" << pgpass_file << "', 'r'): " << ::strerror(errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }
        size_t len = 128;
        char* buf = static_cast<char*>(::malloc(len));
        SCIDB_ASSERT(buf);
        ssize_t nread = 0;
        string password;
        while ((nread = ::getline(&buf, &len, fp)) != EOF) {
            if (buf[nread-1] == '\n') {
                buf[nread-1] = '\0';
            }
            if (0 == ::strncmp(buf, search.c_str(), search.size())) {
                password = &buf[search.size()];
                if (password.empty()) {
                    password = "''";
                } else if (password.find_first_of(" \t") != string::npos) {
                    stringstream ss;
                    ss << '\'' << password << '\'';
                    password = ss.str();
                }
                break;
            }
        }
        if (buf) {
            ::free(buf);
        }
        ::fclose(fp);

        // Did we lose?
        if (password.empty()) {
            stringstream ss;
            ss << "Cannot find " << pgpass_file << " entry for '" << creds << '\'';
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }

        // Win!
        creds += " password=" + password;
        return creds;
    }

    void SystemCatalog::connect(bool doUpgrade)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::connect(doUpgrade = " << doUpgrade << ")");

        if (!PQisthreadsafe())
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_LIBPQ_NOT_THREADSAFE);

        try
        {
            _connection = new pqxx::connection(_makeCredentials());

            work tr(*_connection);
            result query_res = tr.exec("select count(*) from pg_tables where tablename = 'cluster'");
            _initialized = query_res[0].at("count").as(bool());

            if (_initialized)
            {
                result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
                _uuid = query_res[0].at("uuid").as(string());

                query_res = tr.exec("select count(*) from pg_proc where proname = 'get_metadata_version'");
                if (query_res[0].at("count").as(bool()))
                {
                    query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
                    _metadataVersion = query_res[0].at("version").as(int());
                }
                else
                {
                    LOG4CXX_WARN(logger, "Cannot find procedure get_metadata_version in catalog. "
                                 "Assuming catalog metadata version is 0");
                    _metadataVersion = 0;
                }
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }

        verifyLatestArrayVersionTable(_connection, logger);

        if (_initialized && doUpgrade)
        {
            if (_metadataVersion > METADATA_VERSION)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CATALOG_NEWER_THAN_SCIDB)
                        << METADATA_VERSION << _metadataVersion;
            }
            else if (_metadataVersion < METADATA_VERSION)
            {
                if (Config::getInstance()->getOption<bool>(CONFIG_ENABLE_CATALOG_UPGRADE) == false)
                {
                    string const& configName = Config::getInstance()->getOptionName(CONFIG_ENABLE_CATALOG_UPGRADE);
                    ostringstream message;
                    message <<"In order to proceed, SciDB needs to perform an upgrade of the system "<<
                              "catalog. This is not reversible. To confirm, please restart the system "<<
                              "with the setting \'"<<configName<<"\' set to 'true'";
                    LOG4CXX_ERROR(logger, message.str());
                    std::cerr<<message.str()<<"\n";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_NEED_UPGRADE_CONFIRMATION);
                }

                LOG4CXX_WARN(logger, "Catalog metadata version (" << _metadataVersion
                        << ") lower than SciDB metadata version (" << METADATA_VERSION
                        << "). Trying to upgrade catalog...");

                try
                {
                    work tr(*_connection);
                    sleep(5); //XXX why sleep ?
                    for(int ver = _metadataVersion + 1; ver <= METADATA_VERSION; ++ver)
                    {
                        LOG4CXX_WARN(logger, "Upgrading metadata from " << ver-1 << " to " << ver);
                        string upgradeScript(METADATA_UPGRADES_LIST[ver]);
                        tr.exec(string(METADATA_UPGRADES_LIST[ver]));
                    }
                    tr.commit();
                    _metadataVersion = METADATA_VERSION;
                }
                catch (const sql_error &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                        << e.query() << e.what();
                }
                catch (const pqxx::failure &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                }
            }
        }
    }

    bool SystemCatalog::isConnected() const
    {
        if (_connection)
            return _connection->is_open();
        return false;
    }

    void SystemCatalog::addLibrary(const string& libraryName)
    {
        std::function<void()> work = std::bind(&SystemCatalog::_addLibrary,
                                               this,
                                               std::cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary( libraryName ='" << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG27);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('libraries_id_seq')");
            int64_t lid = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"libraries\"(id, name)"
                " values ($1, $2)";
            _connection->prepare("addLibrary", sql1);
            tr.prepared("addLibrary")
                (lid)
                (libraryName).exec();
            _connection->unprepare("addLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const unique_violation& e)
        {
            // we allow double insertions, to support the case:
            // load_library()
            // unload_library()
            // load_library()
            LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary: unique constraint violation:"
                          << e.what() << ", lib name="<< libraryName);
            return;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::getLibraries(vector<string >& libraries)
    {
        std::function<void()> work = std::bind(&SystemCatalog::_getLibraries,
                                               this,
                                               std::ref(libraries));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getLibraries(vector< string >& libraries)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLibraries ( &libraries )");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG28);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql1 = "select name from \"libraries\"";
            _connection->prepare("getLibraries", sql1);
            result query_res = tr.prepared("getLibraries").exec();
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i) {
                libraries.push_back(i.at("name").as(string()));
            }
            _connection->unprepare("getLibraries");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Loaded " << libraries.size() << " libraries.");
    }

    void SystemCatalog::removeLibrary(const string& libraryName)
    {
        std::function<void()> work = std::bind(&SystemCatalog::_removeLibrary,
                                               this,
                                               std::cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_removeLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::removeLibrary ( " << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedWaitTimer timer(PTW_SWT_PG29);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"libraries\" where name = $1";
            _connection->prepare("removeLibrary", sql1);
            tr.prepared("removeLibrary")(libraryName).exec();
            _connection->unprepare("removeLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    SystemCatalog::SystemCatalog() :
    _initialized(false),
    _connection(NULL),
    _uuid(""),
    _metadataVersion(-1),
    _reconnectTries(Config::getInstance()->getOption<int>(CONFIG_CATALOG_RECONNECT_TRIES)),
    _serializedTxnTries(DEFAULT_SERIALIZED_TXN_TRIES)
    {

    }

    SystemCatalog::~SystemCatalog()
    {
        if (_connection)
        {
            try
            {
                if (isConnected())
                {
                    _connection->disconnect();
                }
                delete _connection;
                _connection = NULL;
            }
            catch (...)
            {
                LOG4CXX_DEBUG(logger, "Error when disconnecting from PostgreSQL.");
            }
        }
    }

int SystemCatalog::getMetadataVersion() const
{
    return _metadataVersion;
}

std::string SystemCatalog::getLockInsertSql(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   ASSERT_EXCEPTION( (lockDesc->getInstanceRole() == LockDesc::COORD ||
                      lockDesc->getInstanceRole() == LockDesc::WORKER),
                     string("Invalid lock role requested: ")+lockDesc->toString());

   string lockInsertSql;
   bool isInvalidRequest(false);

   if (lockDesc->getLockMode() == LockDesc::RD) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         // COORD_RD
         lockInsertSql = "insert into array_version_lock"
         " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
         "  array_version, coordinator_id, lock_mode)"
         "(select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where"
         "   AVL.namespace_name=$1::VARCHAR and "
         "   AVL.array_name=$2::VARCHAR and "
         "   AVL.lock_mode>$10 and "
         "   AVL.instance_id=AVL.coordinator_id))";

      } else {
          isInvalidRequest = true;
      }
   } else if (lockDesc->getLockMode() == LockDesc::WR ||
              lockDesc->getLockMode() == LockDesc::CRT) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         // COORD_WR
         lockInsertSql = "insert into array_version_lock"
         " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, array_version, "
         "  coordinator_id, lock_mode)"
         "(select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where"
         "   AVL.namespace_name=$1::VARCHAR and "
         "   AVL.array_name=$2::VARCHAR and "
         "  (AVL.query_id<>$4 or AVL.coordinator_id<>$8) and "
         "   AVL.lock_mode>$10))";

      } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
          if (lockDesc->getLockMode() == LockDesc::CRT) {
              isInvalidRequest = true;
          } else {
              // WORKER_WR
              lockInsertSql = "insert into array_version_lock"
              " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, array_version, "
              "   coordinator_id, lock_mode)"
              "  (select AVL.namespace_name, AVL.array_name, AVL.array_id, AVL.query_id, $4, "
              "   AVL.array_version_id, AVL.array_version, AVL.coordinator_id, AVL.lock_mode "
              " from array_version_lock as AVL where "
              "   AVL.namespace_name=$1::VARCHAR and "
              "   AVL.array_name=$2::VARCHAR and "
              "   AVL.query_id=$3 and "
              "   AVL.coordinator_id=$5 and "
              "   AVL.coordinator_id=AVL.instance_id and "
              "   (AVL.lock_mode=$6 or AVL.lock_mode=$7))";
          }
      }
   } else if (lockDesc->getLockMode() == LockDesc::RM ||
              lockDesc->getLockMode() == LockDesc::RNF ||
              lockDesc->getLockMode() == LockDesc::XCL) {

       if (lockDesc->getInstanceRole() == LockDesc::COORD) {
           // COORD_XCL
           lockInsertSql = "insert into array_version_lock"
           " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
           "   array_version, coordinator_id, lock_mode)"
           " (select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where "
           " not exists (select AVL.namespace_name, AVL.array_name from array_version_lock as AVL where"
           "   AVL.namespace_name=$1::VARCHAR and "
           "   AVL.array_name=$2::VARCHAR and "
           "   (AVL.query_id<>$4 or AVL.coordinator_id<>$8)))";
       } else if (lockDesc->getInstanceRole() == LockDesc::WORKER &&
                  lockDesc->getLockMode() == LockDesc::XCL) {
           // WORKER_XCL
           lockInsertSql = "insert into array_version_lock"
           " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
           "   array_version, coordinator_id, lock_mode)"
           " (select AVL.namespace_name, AVL.array_name, AVL.array_id, AVL.query_id, $4, "
           "   AVL.array_version_id, AVL.array_version, AVL.coordinator_id, AVL.lock_mode"
           " from array_version_lock as AVL where "
           "   AVL.namespace_name=$1::VARCHAR and "
           "   AVL.array_name=$2::VARCHAR and "
           "   AVL.query_id=$3 and "
           "   AVL.coordinator_id=$5 and "
           "   AVL.instance_id=AVL.coordinator_id and "
           "   AVL.lock_mode=$6 and "
           "  not exists (select 1 from array_version_lock as AVL2 where "
           "   AVL2.namespace_name=$1::VARCHAR and "
           "   AVL2.array_name=$2::VARCHAR and "
           "   AVL2.query_id=$3 and "
           "   AVL.coordinator_id=$5 and "
           "   AVL2.instance_id=$4))";
       } else {
          isInvalidRequest = true;
      }
   } else {
        isInvalidRequest = true;
    }

   if (isInvalidRequest) {
      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << string("Invalid lock requested: ")+lockDesc->toString();
   }

   return lockInsertSql;
}

void SystemCatalog::lockArrays(const QueryLocks& arrayLocks,
                               ErrorChecker& errorChecker)
{
    std::function<void()> work = [this, &arrayLocks, &errorChecker] () {
        // If any exceptions are thrown or if the work is restarted, then
        // all the locks must be in the 'not locked' state.
        OnScopeExit clearLocksOnException([&arrayLocks] () {
                                              for (const auto& lock : arrayLocks) {
                                                  lock->setLocked(false);
                                              }
                                          });
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
        pqxx::work tr(*_connection);
        _lockAVLTable(tr);
        for (auto& lock : arrayLocks) {
            assert(lock);
            LOG4CXX_TRACE(logger, "Acquiring lock: " << lock->toString());

            auto rc = _performLockAction([this, &lock, &errorChecker, &tr] () {
                                             return _lockArray(lock, errorChecker, tr);
                                         });

            // lockArrays assumes that all locks will be successfully acquired or
            // none of them will be (preserving the assumption as held before the
            // changes in commit sha 70dcb1d183 were introduced).  This means that
            // if *any* array lock is busy, then none of the array locks are acquired
            // and if *any* array lock otherwise fails to acquire, then emit a runtime
            // exception.  The assumption is that we'll always, even in the case of
            // a pqxx::broken_connection, acquire all locks for the passed arrayLocks
            // container.
            if (rc == LockResult::BUSY) {
                throw LockBusyException(REL_FILE, __FUNCTION__, __LINE__);
            }
            if (!_isLockHeld(rc)) {
                assert(false);
                throw std::runtime_error((string("Failed to acquire SystemCatalog lock")+lock->toString()).c_str());
            }
            lock->setLocked(true);
        }
        _performLockAction([this, &tr] () {
                               ScopedWaitTimer commitTimer(PTW_SWT_PG30_H, true);
                               tr.commit();
                               return LockResult::NONE;
                           });
        // Successfully committed lock state in postgres.
        clearLocksOnException.cancel();
    };

    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

/**
 * If a query didn't result in any libpqxx exceptions, then a commit is
 * needed regardless of the query result itself (e.g., if we acquired the lock,
 * didn't acquire it, was busy).  If a query resulted in an exception,
 * and that exception was a pqxx::connection_broken exception, then the
 * transaction is discarded and the query restarted.  If the query resulted
 * in an exception, and that exception was a pqxx::unique_violation, then
 * assume that we already acquired the lock and return LockResult::ALREADY_HELD,
 * which is a successful lock acquisition as far as the System Catalog is
 * concerned.  If the query resulted in any other exception, then that is
 * propagated up to the client of the System Catalog to determine if it's
 * worth continuing at that layer.
 */
bool SystemCatalog::lockArray(const std::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
    std::function<bool()> work =
        [this, &lockDesc, &errorChecker] () {
            ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
            ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());
            pqxx::work tr(*_connection);
            _lockAVLTable(tr);
            auto lockResult =
                _performLockAction([this, &lockDesc, &errorChecker, &tr] () {
                                       return _lockArray(lockDesc, errorChecker, tr);
                                   });
            if (_commitNeeded(lockResult)) {
                // pqxx::work::commit throws exceptions and returns void.  Any thrown
                // exceptions will be handled by _performLockAction and re-thrown as
                // necessary.  If the lock was busy during _lockArray, it'll still be
                // busy now and we'll throw the LockBusyException below with the
                // transaction committed.
                _performLockAction([this, &tr] () {
                                       ScopedWaitTimer commitTimer(PTW_SWT_PG30_H, true);
                                       tr.commit();
                                       return LockResult::NONE;
                                   });
                lockDesc->setLocked(lockResult == LockResult::ACQUIRED);
                if (lockResult == LockResult::BUSY) {
                    throw LockBusyException(REL_FILE, __FUNCTION__, __LINE__);
                }
            }
            return _isLockHeld(lockResult);
        };
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

SystemCatalog::LockResult SystemCatalog::_lockArray(const std::shared_ptr<LockDesc>& lockDesc,
                                                    ErrorChecker& errorChecker,
                                                    pqxx::work& tr)
{
    // for read-only queries, like t.sdqp.sdb-6026, we would prefer to take no lock,
    // (as none would have been taken) but we call this once for the query in the
    // sdb-6026 test

    // Callers to _lockArray must acquire a table lock on the array_version_lock
    // postgres table via the _lockAVLTable method.

    LOG4CXX_DEBUG(logger, "SystemCatalog::lockArray: "<<lockDesc->toString());

    SCIDB_ASSERT(lockDesc);
    SCIDB_ASSERT(_connection);

    try {
        const bool requestingGlobalLock = _isGlobalArrayLock(lockDesc);
        if (!requestingGlobalLock &&
            lockDesc->getLockMode() >= LockDesc::WR &&
            _hasGlobalArrayLock(tr)) {
            // If the caller's requesting an array lock above LockDesc::RD
            // and the global array lock is in place, then don't allow the
            // array lock to take.
            return LockResult::BUSY;
        }
        else if (requestingGlobalLock &&
                 _hasGlobalArrayLock(tr)) {
            // If the caller's requesting the global array lock and the
            // global array lock is already in place, then this query
            // can't acquire it.
            return LockResult::DID_NOT_ACQUIRE;
        }

        const std::string lockInsertSql = getLockInsertSql(lockDesc);
        size_t affectedRows = 0;

        if (lockDesc->getLockMode() == LockDesc::RD) {
            ScopedWaitTimer timer(PTW_SWT_PG30_A, true);

            if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                string uniquePrefix("COORD_RD_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)lockDesc->getLockMode())
                    ((int)LockDesc::CRT).exec();
                affectedRows = query_res.affected_rows();

            } else { assert(false);}
        } else if (lockDesc->getLockMode() == LockDesc::WR
                   || lockDesc->getLockMode() == LockDesc::CRT) {

            if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                ScopedWaitTimer timer(PTW_SWT_PG30_B, true);
                string uniquePrefix("COORD_WR_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)lockDesc->getLockMode())
                    ((int)LockDesc::RD).exec();
                affectedRows = query_res.affected_rows();

            } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
                ScopedWaitTimer timer(PTW_SWT_PG30_C, true);
                assert(lockDesc->getLockMode() != LockDesc::CRT);
                string uniquePrefix("WORKER_WR_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)LockDesc::WR)
                    ((int)LockDesc::CRT).exec();
                affectedRows = query_res.affected_rows();

                if (query_res.affected_rows() == 1) {
                    ScopedWaitTimer timer2(PTW_SWT_PG30_C, true);
                    string lockReadSql =
                        "select array_id, array_version_id, array_version "
                        "from array_version_lock "
                        "where "
                        "  namespace_name=$1::VARCHAR and "
                        "  array_name=$2::VARCHAR and "
                        "  query_id=$3 and "
                        "  coordinator_id=$4 and "
                        "  instance_id=$5";

                    _connection->prepare(lockReadSql, lockReadSql);

                    result query_res_read = tr.prepared(lockReadSql)
                        (lockDesc->getNamespaceName())
                        (lockDesc->getArrayName())
                        (lockDesc->getQueryId().getId())
                        (lockDesc->getQueryId().getCoordinatorId())
                        (lockDesc->getInstanceId()).exec();

                    assert(query_res_read.size() == 1);

                    lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                    lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                    lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                }
            } else { assert(false); }
        } else if (lockDesc->getLockMode() == LockDesc::XCL) {

            if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                ScopedWaitTimer timer(PTW_SWT_PG30_D, true);
                string uniquePrefix("COORD_XCL_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)lockDesc->getLockMode()).exec();
                affectedRows = query_res.affected_rows();

            } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
                ScopedWaitTimer timer(PTW_SWT_PG30_E, true);

                string uniquePrefix("WORKER_XCL_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                pqxx::prepare::invocation invc = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)LockDesc::XCL);

                result query_res = invc.exec();
                affectedRows = query_res.affected_rows();

                // Handle store(blah(scan(tempA)),tempA) or join(tempB,tempB)
                // in which case both store & scan will try to lock (or two scans)
                if (affectedRows == 1 || affectedRows == 0) {
                    ScopedWaitTimer timer2(PTW_SWT_PG30_E, true);
                    string lockReadSql =
                        "select array_id, array_version_id, array_version "
                        "from array_version_lock "
                        "where "
                        "  namespace_name=$1::VARCHAR and "
                        "  array_name=$2::VARCHAR and "
                        "  query_id=$3 and "
                        "  coordinator_id=$4 and "
                        "  instance_id=$5";

                    _connection->prepare(lockReadSql, lockReadSql);

                    result query_res_read = tr.prepared(lockReadSql)
                        (lockDesc->getNamespaceName())
                        (lockDesc->getArrayName())
                        (lockDesc->getQueryId().getId())
                        (lockDesc->getQueryId().getCoordinatorId())
                        (lockDesc->getInstanceId()).exec();

                    affectedRows = query_res_read.size();
                    if (affectedRows == 1 ) {
                        lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                        lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                        lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                    } else {
                        ASSERT_EXCEPTION(affectedRows == 0, "Array lock entry not unique on worker");
                    }
                } else {
                    ASSERT_EXCEPTION_FALSE("Array lock entry not unique on worker");
                }
            } else { assert(false); }
        } else if (lockDesc->getLockMode() == LockDesc::RM) {
            ScopedWaitTimer timer(PTW_SWT_PG30_F, true);

            assert(lockDesc->getInstanceRole() == LockDesc::COORD);

            string uniquePrefix("RM_");
            _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

            result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                (lockDesc->getNamespaceName())
                (lockDesc->getArrayName())
                (lockDesc->getArrayId())
                (lockDesc->getQueryId().getId())
                (lockDesc->getInstanceId())
                (lockDesc->getArrayVersionId())
                (lockDesc->getArrayVersion())
                (lockDesc->getQueryId().getCoordinatorId())
                ((int)lockDesc->getLockMode()).exec();
            affectedRows = query_res.affected_rows();
        } else if (lockDesc->getLockMode() == LockDesc::RNF) {
            if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                ScopedWaitTimer timer(PTW_SWT_PG30_G, true);

                string uniquePrefix("COORD_RNF_");
                _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql);

                result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                    (lockDesc->getNamespaceName())
                    (lockDesc->getArrayName())
                    (lockDesc->getArrayId())
                    (lockDesc->getQueryId().getId())
                    (lockDesc->getInstanceId())
                    (lockDesc->getArrayVersionId())
                    (lockDesc->getArrayVersion())
                    (lockDesc->getQueryId().getCoordinatorId())
                    ((int)lockDesc->getLockMode()).exec();
                affectedRows = query_res.affected_rows();
            } else { assert(false); }
        } else {
            assert(false);
        }
        if (affectedRows == 1) {
            LOG4CXX_DEBUG(logger, "SystemCatalog::lockArray: locked "<<lockDesc->toString());
            return LockResult::ACQUIRED;
        }
        if (lockDesc->getInstanceRole() == LockDesc::WORKER &&
            affectedRows != 1) {
            // workers must error out immediately
            assert(affectedRows==0);
            return LockResult::DID_NOT_ACQUIRE;
        }
    }
    catch (const pqxx::unique_violation &e)
    {
        if (!lockDesc->isLocked()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        // On coordinator we may try to acquire the same lock
        // multiple times. If the lock is already acquired,
        // we should just return success.
        // XXX tigor 6/20/2013 TODO: Technically, just checking isLocked()
        // before running the query should be sufficient.
        // After debugging we should/can switch to doing just that.

        ASSERT_EXCEPTION((lockDesc->getInstanceRole() == LockDesc::COORD),
                         string("On a worker instance the array lock: ")+
                         lockDesc->toString()+
                         string(" cannot be acquired more than once"));
        return LockResult::ALREADY_HELD;  // transaction *not* committed
    }

    // transaction committed by this point in execution
    if (errorChecker && !errorChecker()) {
        return LockResult::DID_NOT_ACQUIRE;
    }
    return LockResult::BUSY;
}

SystemCatalog::LockResult SystemCatalog::_performLockAction(const SystemCatalog::LockAction& action)
{
    // for read-only queries, like t.sdqp.sdb-6026, we would prefer to take no lock,
    // (as none would have been taken) but we call this once for the query in the
    // sdb-6026 test

   assert(_connection);

   try {
       return action();
   }
   catch (const pqxx::unique_violation &e)
   {
       // _performLockAction will be used not only to call through to exec() on transactions but
       // also commit().  From a survey of the libpqxx library code on github, unique_violation
       // can be thrown both by exec() and also commit(), hence the extra catch of that exception
       // type here as well as in _lockArray.
       return LockResult::ALREADY_HELD;  // transaction *not* committed
   }
   catch (const pqxx::broken_connection &e)
   {
       throw;
   }
   catch (const pqxx::sql_error &e)
   {
      LOG4CXX_ERROR(logger,
                    "SystemCatalog::_performLockAction: postgress exception:" <<
                    e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::_performLockAction: query:" << e.query());
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

bool SystemCatalog::unlockArray(const std::shared_ptr<LockDesc>& lockDesc)
{
    std::function<bool()> work = std::bind(&SystemCatalog::_unlockArray,
                                           this,
                                           std::cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_unlockArray(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   LOG4CXX_DEBUG(logger, "SystemCatalog::unlockArray: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockDeleteSql(
        "delete from array_version_lock "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  instance_id=$5");

      {
         ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
         ScopedWaitTimer timer(PTW_SWT_PG31);
         ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

         work tr(*_connection);
         _lockAVLTable(tr);
         _connection->prepare(lockDeleteSql, lockDeleteSql);

         result query_res = tr.prepared(lockDeleteSql)
         (lockDesc->getNamespaceName())
         (lockDesc->getArrayName())
         (lockDesc->getQueryId().getId())
         (lockDesc->getQueryId().getCoordinatorId())
         (lockDesc->getInstanceId()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

bool SystemCatalog::updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc)
{
    std::function<bool()> work = std::bind(&SystemCatalog::_updateArrayLock,
                                           this,
                                           std::cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);

   LOG4CXX_TRACE(logger, "SystemCatalog::updateArrayLock: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockUpdateSql =
        "update array_version_lock "
        "set array_id=$6, array_version_id=$7, array_version=$8, lock_mode=$9 "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  instance_id=$5";

      {
         ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
         ScopedWaitTimer timer(PTW_SWT_PG32);
         ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

         work tr(*_connection);
         _lockAVLTable(tr);
         _connection->prepare(lockUpdateSql, lockUpdateSql);

         result query_res = tr.prepared(lockUpdateSql)
         (lockDesc->getNamespaceName())
         (lockDesc->getArrayName())
         (lockDesc->getQueryId().getId())
         (lockDesc->getQueryId().getCoordinatorId())
         (lockDesc->getInstanceId())
         (lockDesc->getArrayId())
         (lockDesc->getArrayVersionId())
         (lockDesc->getArrayVersion())
         ((int)lockDesc->getLockMode()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

void SystemCatalog::readArrayLocks(const InstanceID instanceId,
                                   std::list<std::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<std::shared_ptr<LockDesc> >& workerLocks)
{
    std::function<void()> work = std::bind(&SystemCatalog::_readArrayLocks,
                                           this,
                                           instanceId,
                                           std::ref(coordLocks),
                                           std::ref(workerLocks));
    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

void SystemCatalog::_readArrayLocks(const InstanceID instanceId,
                                   std::list<std::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<std::shared_ptr<LockDesc> >& workerLocks)
{
   SCIDB_ASSERT(isValidPhysicalInstance(instanceId));

   ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
   ScopedWaitTimer timer(PTW_SWT_PG33);
   ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql =
        "select "
        "  namespace_name, array_name, array_id, query_id, array_version_id, "
        "  array_version, coordinator_id, lock_mode "
        "from array_version_lock "
        "where instance_id=$1";

      _connection->prepare(sql, sql);

      result query_res = tr.prepared(sql)(instanceId).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks: found "<< size <<" locks");

      for (size_t i=0; i < size; ++i) {
          const QueryID qId(query_res[i].at("coordinator_id").as(InstanceID()),
                            query_res[i].at("query_id").as(uint64_t()));
          SCIDB_ASSERT(qId.isValid());

          const LockDesc::InstanceRole role =
             qId.getCoordinatorId() == instanceId ? LockDesc::COORD : LockDesc::WORKER ;

          std::shared_ptr<LockDesc> lock(
            new LockDesc(
                query_res[i].at("namespace_name").as(string()),
                query_res[i].at("array_name").as(string()),
                qId,
                instanceId,
                role,
                static_cast<LockDesc::LockMode>(query_res[i].at("lock_mode").as(int()))
                ));
         lock->setArrayVersion(query_res[i].at("array_version").as(VersionID()));
         lock->setArrayId(query_res[i].at("array_id").as(ArrayID()));
         lock->setArrayVersionId(query_res[i].at("array_version_id").as(ArrayID()));

         if (lock->getInstanceRole() == LockDesc::COORD) {
            coordLocks.push_back(lock);
         } else {
            workerLocks.push_back(lock);
         }
         LOG4CXX_TRACE(logger, lock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: instance ID = " << instanceId);
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

uint64_t SystemCatalog::countLocksAtLeast(LockDesc::LockMode lockMode)
{
    std::function<uint64_t()> work = [this, lockMode] () {
        ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
        pqxx::work tr(*_connection);
        std::string readArrayLocksSql =
            "select count(*) from array_version_lock where lock_mode >= ";
        readArrayLocksSql.append(std::to_string(lockMode));
        result query_result = tr.exec(readArrayLocksSql);
        assert(query_result.size() == 1);
        uint64_t lockCount = query_result[0].at("count").as(uint64_t());
        return lockCount;
    };

    return Query::runRestartableWork<uint64_t, broken_connection>(work, _reconnectTries);
}

uint32_t SystemCatalog::deleteCoordArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID, LockDesc::COORD);
}

uint32_t SystemCatalog::deleteWorkerArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID, LockDesc::WORKER);
}

uint32_t SystemCatalog::deleteArrayLocks(InstanceID instanceId, const QueryID& queryId, LockDesc::InstanceRole role)
{
    std::function<uint32_t()> work = std::bind(&SystemCatalog::_deleteArrayLocks,
                                               this,
                                               instanceId,
                                               queryId,
                                               role);
    return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
}

uint32_t SystemCatalog::_deleteArrayLocks(InstanceID instanceId, const QueryID& queryId, LockDesc::InstanceRole role)
{
    // for read-only queries, like t.sdqp.sdb-6026, we would prefer to delete no locks,
    // but we call this method once per query in the sdb-6026 test
    LOG4CXX_DEBUG(logger, "SystemCatalog::deleteArrayLocks instanceId = "
                  << Iid(instanceId)
                  << " role = "<<role
                  << " queryId = "<<queryId);
   size_t numLocksDeleted = 0;
   ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
   ScopedWaitTimer timer(PTW_SWT_PG34, true);
   ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

   try
   {
      assert(_connection);

      uint16_t argNum = 1;
      string lockDeleteSql("delete from array_version_lock where instance_id=$1");
      bool isQuerySpecified = queryId.isValid();
      bool isRoleSpecified = (role != LockDesc::INVALID_ROLE);

      result query_res;
      stringstream ss;

      if (isQuerySpecified) {
          ss << " and query_id=$"<< ++argNum;
          ss << " and coordinator_id=$"<< ++argNum;
      }
      std::string uniquePrefix;
      if (isRoleSpecified) {
          if (role == LockDesc::COORD) {
              ss << " and instance_id=coordinator_id";
              uniquePrefix = std::string("COORD_");
          } else {
              SCIDB_ASSERT(role == LockDesc::WORKER);
              ss << " and instance_id!=coordinator_id";
              uniquePrefix = std::string("WORKER_");
          }
      }

      lockDeleteSql += ss.str();

      _connection->prepare(uniquePrefix + lockDeleteSql, lockDeleteSql);

      work tr(*_connection);
      _lockAVLTable(tr);
      pqxx::prepare::invocation invc = tr.prepared(uniquePrefix+lockDeleteSql);
      invc(instanceId);

      if (isQuerySpecified) {
          invc(queryId.getId());
          invc(queryId.getCoordinatorId());
      }

      query_res = invc.exec();

      numLocksDeleted = query_res.affected_rows();

      LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayLocks: deleted "
                    << numLocksDeleted
                    << " locks for instance " << Iid(instanceId));
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return safe_static_cast<uint32_t>(numLocksDeleted);
}

std::shared_ptr<LockDesc>
SystemCatalog::checkForCoordinatorLock(
    const string& namespaceName, const string& arrayName, const QueryID& queryId)
{
    std::function<std::shared_ptr<LockDesc>()> work = std::bind(&SystemCatalog::_checkForCoordinatorLock,
                                                                this,
                                                                std::cref(namespaceName),
                                                                std::cref(arrayName),
                                                                queryId);
    return Query::runRestartableWork<std::shared_ptr<LockDesc>, broken_connection>(
        work, _reconnectTries);
}

std::shared_ptr<LockDesc>
SystemCatalog::_checkForCoordinatorLock(
    const string& namespaceName,
    const string& arrayName,
    const QueryID& queryId)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock:"
                 << " namespaceName = " << namespaceName
                 << " arrayName = " << arrayName
                 << " queryID = " << queryId);

   std::shared_ptr<LockDesc> coordLock;

   ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
   ScopedWaitTimer timer(PTW_SWT_PG35);
   ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

   assert(_connection);
   try
   {
       // Serializable txn should not be necessary because when
       // this check is performed, the same lock is not supposed to be
       // re-inserted. So, the lock row is either in the table or not ...
       work tr(*_connection);

      string sql =
        "select array_id, instance_id, array_version_id, array_version, lock_mode "
        "from array_version_lock "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  coordinator_id=instance_id";
      _connection->prepare(sql, sql);

      result query_res = tr.prepared(sql)
        (namespaceName)(arrayName)(queryId.getId())(queryId.getCoordinatorId()).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock found "<< size <<" locks");

      assert(size < 2);
      if (size > 0) {
          const InstanceID instanceId = query_res[0].at("instance_id").as(InstanceID());
          SCIDB_ASSERT(instanceId == queryId.getCoordinatorId());
          coordLock = std::shared_ptr<LockDesc>(
                new LockDesc(
                    namespaceName,
                    arrayName,
                    queryId,
                    instanceId,
                    LockDesc::COORD,
                    static_cast<LockDesc::LockMode>(query_res[0].at("lock_mode").as(int()))
                    ));
         coordLock->setArrayVersion(query_res[0].at("array_version").as(VersionID()));
         coordLock->setArrayId(query_res[0].at("array_id").as(ArrayID()));
         coordLock->setArrayVersionId(query_res[0].at("array_version_id").as(ArrayID()));
         LOG4CXX_TRACE(logger, coordLock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return coordLock;
}

void SystemCatalog::renameArray(const string& ns_name,
                                const string &old_array_name,
                                const string &new_array_name)
{
    std::function<void()> work1 = std::bind(&SystemCatalog::_renameArray,
                                            this,
                                            std::cref(ns_name),
                                            std::cref(old_array_name),
                                            std::cref(new_array_name));
    std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                            work1,
                                            _serializedTxnTries);
    Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
}

void SystemCatalog::_renameArray(const string& ns_name,
                                 const string &old_array_name,
                                 const string &new_array_name)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::renameArray(ns=" << ns_name
                  << ", old=" << old_array_name
                  << ", new=" << new_array_name << ')');

    SCIDB_ASSERT(!isQualifiedArrayName(old_array_name));
    SCIDB_ASSERT(!isQualifiedArrayName(new_array_name));
    SCIDB_ASSERT(!ns_name.empty());

    ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
    ScopedWaitTimer timer(PTW_SWT_PG36);
    ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

    assert(_connection);
    try
    {
        pqxx::transaction<pqxx::serializable> tr(*_connection);

        // Translate (ns_name, old_array_name) to unversioned array id.
        FindArrayArgs args;
        FindArrayResult result;
        args.nsNamePtr = &ns_name;
        args.arrayNamePtr = &old_array_name;
        bool found = _findArrayWithTxn(args, result, &tr);
        if (!found) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST)
                << makeQualifiedArrayName(ns_name, old_array_name);
        }

        // Update all versions with the new name string.
        string sql =
            "update \"array\" as ARR "
            "set name = regexp_replace(ARR.name, '^'||$1::VARCHAR||'(@.+)?$', $2::VARCHAR||E'\\\\1') "
            "where id = $3 or id = any(select version_array_id "
            "                          from array_version where array_id = $3)";
        string key("rename-all-versions");
        _connection->prepare(key, sql);
        pqxx::result qres = tr.prepared(key)
            (old_array_name)
            (new_array_name)
            (result.arrayId).exec();
        ASSERT_EXCEPTION(qres.affected_rows() > 0,
                         "Found array to rename, but no rows updated");

        std::ostringstream updateLatestArrayVersionSql;
        updateLatestArrayVersionSql << "update latest_array_version set array_name='"
                                    << new_array_name
                                    << "' where namespace_name='"
                                    << ns_name
                                    << "' and array_name='"
                                    << old_array_name
                                    << "'";
        tr.exec(updateLatestArrayVersionSql.str());

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const unique_violation& e)
    {
        LOG4CXX_ERROR(logger, "SystemCatalog::renameArray: unique constraint violation: " << e.what());
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXISTS) << new_array_name;
    }
    catch (const sql_error &e)
    {
        throwOnSerializationConflict(e);
        if (isDebug() ) {
            const string t = typeid(e).name();
            const string w = e.what();
            assert(false);
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }
}

void SystemCatalog::getArrays(const std::string& nsName,
                              std::vector<ArrayDesc> &arrays,
                              bool ignoreOrphanAttributes,
                              bool ignoreVersions,
                              bool orderByName)
{
    std::function<void()> work1;
    if (nsName.empty()) {
        work1 = std::bind(&SystemCatalog::_getArraysAllNamespaces,
                          this,
                          std::ref(arrays),
                          ignoreOrphanAttributes,
                          ignoreVersions,
                          orderByName);
    } else {
        work1 = std::bind(&SystemCatalog::_getArraysOneNamespace,
                          this,
                          std::cref(nsName),
                          std::ref(arrays),
                          ignoreOrphanAttributes,
                          ignoreVersions,
                          orderByName);
    }

    std::function<void()> work2 = std::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                            work1,
                                            _serializedTxnTries);
    Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
}

void SystemCatalog::_getArraysOneNamespace(const std::string& nsName,
                                           std::vector<ArrayDesc> &arrays,
                                           bool ignoreOrphanAttributes,
                                           bool latestVersion,
                                           bool orderByName)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::_getArraysOneNamespace(" << nsName << ", ...)");

    ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
    ScopedWaitTimer timer(PTW_SWT_PG37);
    ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

    assert(_connection);

    try
    {
        pqxx::transaction<pqxx::serializable> tr(*_connection);

        // Look up the namespace.  (Here we rely on a side effect of
        // the NamespaceDesc ctor: if nsName is the public namespace,
        // then the id will be correctly set to the well-known public
        // id.  The assertion verifies this.)
        //
        NamespaceDesc nsDesc(nsName);
        SCIDB_ASSERT(!nsDesc.isIdValid() ||
                     (nsName == rbac::PUBLIC_NS_NAME &&
                      nsDesc.getId() == rbac::PUBLIC_NS_ID));
        if (!nsDesc.isIdValid()) {
            bool found =
                NsComm::findNamespaceWithTransaction(nsDesc, /*byName:*/true, &tr);
            if (!found) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANNOT_RESOLVE_NAMESPACE)
                    << nsName;
            }
        }

        // List its array ids.
        vector<ArrayID> arrayIds;
        _listNamespaceArrayIds(nsDesc, arrayIds, latestVersion, orderByName, &tr);

        // Build vector of ArrayDesc objects.
        arrays.clear();
        arrays.reserve(arrayIds.size());
        for (ArrayID aid : arrayIds) {
            std::shared_ptr<ArrayDesc> aDesc =
                _getArrayDescByIdWithTxn(aid, ignoreOrphanAttributes,
                                         /*wantChunkIntervals:*/ false,
                                         &tr, &nsDesc);
            SCIDB_ASSERT(aDesc);
            if (latestVersion) {
                // The array names returned by _getArrayDescByIdWithTxn will have the
                // versions as part of their name.  I don't want to show the version
                // in the latestVersion case, so overwrite the array name with its
                // unversioned name.
                aDesc->setName(makeUnversionedName(aDesc->getName()));
            }
            arrays.push_back(*aDesc);
        }

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throwOnSerializationConflict(e);
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }

    LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() <<
                  " arrays from namespace " << nsName << " catalog");
}

void SystemCatalog::_getArraysAllNamespaces(std::vector<ArrayDesc> &arrays,
                                            bool ignoreOrphanAttributes,
                                            bool latestVersion,
                                            bool orderByName)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::_getArraysAllNamespaces()");

    assert(_connection);

    ScopedMutexLock mutexLock(_pgLock, PTW_SML_PG);
    ScopedWaitTimer timer(PTW_SWT_PG38);
    ScopedPostgressStats pgStats(MonitorConfig::getInstance()->isEnabled());

    try
    {
        pqxx::transaction<pqxx::serializable> tr(*_connection);

        // If orderByName, it may be faster to first read the entire
        // namespace table (NsComm::getNamespacesWithTransaction), since
        // that would avoid repeated _mapArrayIdToNamespace calls in
        // _getArrayDescByIdWithTxn().  We'll address it if it actually
        // becomes a problem.

        vector<ArrayID> arrayIds;
        _listAllArrayIds(arrayIds, latestVersion, orderByName, &tr);

        // Collect the array descriptors.
        arrays.clear();
        arrays.reserve(arrayIds.size());
        for (ArrayID const& aid : arrayIds) {
            std::shared_ptr<ArrayDesc> aDesc =
                _getArrayDescByIdWithTxn(aid, ignoreOrphanAttributes,
                                         /*wantChunkIntervals:*/ false,
                                         &tr, nullptr);
            SCIDB_ASSERT(aDesc);
            if (latestVersion) {
                // The array names returned by _getArrayDescByIdWithTxn will have the
                // versions as part of their name.  I don't want to show the version
                // in the latestVersion case, so overwrite the array name with its
                // unversioned name.
                aDesc->setName(makeUnversionedName(aDesc->getName()));
            }
            arrays.push_back(*aDesc);
        }

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throwOnSerializationConflict(e);
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }

    LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");
}

void SystemCatalog::throwOnSerializationConflict(const pqxx::sql_error& e)
{
  // libpqxx does not provide SQLSTATE via which the serialization problem can be identified
  // See http://pqxx.org/development/libpqxx/ticket/219, so do the text comparison ...
  static const string SERIALIZATION_CONFLICT
    ("ERROR:  could not serialize access");
  static const string::size_type scSize = SERIALIZATION_CONFLICT.size();

  if (SERIALIZATION_CONFLICT.compare(0, scSize, e.what(), scSize) == 0) {
    LOG4CXX_WARN(logger, "SystemCatalog::_invalidateTempArray: postgress exception:"<< e.what());
    throw TxnIsolationConflict(e.what(), e.query());
  }
}

} // namespace catalog
