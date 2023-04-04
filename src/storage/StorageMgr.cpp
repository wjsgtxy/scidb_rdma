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
 * @file StorageMgr.cpp
 *
 * @author Steve Fridella
 *
 * @brief Implementation of storage manager class.
 */

#include <storage/StorageMgr.h>
#include <system/Config.h>
#include <system/Constants.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <system/SystemCatalog.h>
#include <system/LockDesc.h>
#include <query/Multiquery.h>
#include <query/Query.h>

namespace scidb {
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.storage.storemgr"));

const size_t MAX_CFG_LINE_LENGTH = 1 * KiB;

/* Utility functions
   TODO:  These functions were brought over from the old storage
   implementation to support some code cut and paste.  We should
   get rid of them or make them generic enough to be put into the
   system/Util.h
 */
inline static char* strtrim(char* buf)
{
    char* p = buf;
    char ch;
    while ((unsigned char)(ch = *p) <= ' ' && ch != '\0') { // skip initial non-printing characters at beginning
        p += 1;                                                     // coverage     // have to use a non-printing character storage name or unit test
    }
    char* q = p + strlen(p); // skip non-printing characters at the end
    while (q > p && (unsigned char)q[-1] <= ' ') {
        q -= 1;
    } // (note that non-printing charcters in middle are kept)
    *q = '\0';  // modifies the string passed in
    return p;
}

inline static string relativePath(const string& dir, const string& file)
{
    return file[0] == '/' ? file : dir + "/" + file;
}

/**
 * StorageMgr implementation
 */

void StorageMgr::initStorageConfigFile(std::string const& storageConfigPath)
{
    char buf[MAX_CFG_LINE_LENGTH];
    _databasePath = scidb::getDir(storageConfigPath);
    _configFileName = scidb::getFile(storageConfigPath);

    FILE* f = scidb::fopen(storageConfigPath.c_str(), "r");
    if (f == NULL) {        // coverage, f is never null, so file always available for read, fake that.  called indirectly from NetworkManger->run()
        /* File not found, create and initialize the file
         */
        f = scidb::fopen(storageConfigPath.c_str(), "w");       // coverage, would fail if directory doesn't exist, e.g.
        if (!f) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE)
                << storageConfigPath.c_str() << ferror(f);
        }

        size_t basenameEnd = _configFileName.find_last_of('.');
        if (basenameEnd == string::npos) {
            basenameEnd = _configFileName.size();
        }
        string databaseName = _configFileName.substr(0, basenameEnd);
        _databaseHeader = _databasePath + "/" + databaseName + ".hdr17";

        scidb::fprintf(f, "%s.hdr17\n", databaseName.c_str());
    } else {
        /* File found, read it and store the header file path
         */
        if (!fgets(buf, sizeof buf, f)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_STORAGE_DESCRIPTOR_INVALID_FORMAT);
        }
        _databaseHeader = relativePath(_databasePath, strtrim(buf));
    }
    scidb::fclose(f);
}

void StorageMgr::initStorageHeader()
{
    /* Open storage header file and lock it (prevent multiple scidb
       processes on same db)
     */
    int flags = O_LARGEFILE | O_RDWR | O_CREAT;
    _hd = FileManager::getInstance()->openFileObj(_databaseHeader.c_str(), flags);
    if (!_hd) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_FILE)   // coverage, caller of initStorageHeader
            << _databaseHeader << ::strerror(errno) << errno;
    }

    struct flock flc;
    flc.l_type = F_WRLCK;
    flc.l_whence = SEEK_SET;
    flc.l_start = 0;
    flc.l_len = 1;

    if (_hd->fsetlock(&flc)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_LOCK_DATABASE); // coverage
    }

    /* Read the header (or create it) and verify the magic and version
     */
    size_t rc = _hd->read(&_hdr, sizeof(_hdr), 0);
    if (rc != 0 && rc != sizeof(_hdr)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "read" << ::strerror(errno) << errno;
    }

    if (rc == 0) {                  // coverage in here, so sometimes fake out a zero read.  it should overwrite the header
        // clang-format off
            LOG4CXX_TRACE(logger, "storagemgr open:  initializing storage header");
        // clang-format on

        /* Database is not initialized
         */
        ::memset(&_hdr, 0, sizeof(_hdr));   // coverage
        _hdr.magic = SCIDB_STORAGE_HEADER_MAGIC;
        _hdr.versionLowerBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.versionUpperBound = SCIDB_STORAGE_FORMAT_VERSION;
        _hdr.instanceId = INVALID_INSTANCE;
    } else {
        // clang-format off
            LOG4CXX_TRACE(logger, "storagemgr open:  opening storage header");
        // clang-format on

        /* Check for corrupted metadata file
         */
        if (_hdr.magic != SCIDB_STORAGE_HEADER_MAGIC) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_INVALID_STORAGE_HEADER);  // coverage
        }

        /* At the moment, both upper and lower bound versions in the file must
           equal to the current version in the code.
        */
        if (_hdr.versionLowerBound != SCIDB_STORAGE_FORMAT_VERSION ||
            _hdr.versionUpperBound != SCIDB_STORAGE_FORMAT_VERSION) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_MISMATCHED_STORAGE_FORMAT_VERSION)    // coverage
                << _hdr.versionLowerBound << _hdr.versionUpperBound
                << SCIDB_STORAGE_FORMAT_VERSION;
        }
    }
}

void StorageMgr::doTxnRecoveryOnStartup()
{
    ASSERT_EXCEPTION(getInstanceId() != INVALID_INSTANCE,
                     "Invalid instance number in transaction recovery");

    _inRecovery = true;

    mst::rollbackOnStartup(getInstanceId());

    list<std::shared_ptr<LockDesc>> coordLocks;
    list<std::shared_ptr<LockDesc>> workerLocks;

    SystemCatalog::getInstance()->readArrayLocks(getInstanceId(), coordLocks, workerLocks);

    {  // Deal with the  LockDesc::COORD type locks first

        for (list<std::shared_ptr<LockDesc>>::const_iterator iter = coordLocks.begin();
             iter != coordLocks.end();
             ++iter) {
            const std::shared_ptr<LockDesc>& lock = *iter;

            if (lock->getLockMode() == LockDesc::RM) {
                const bool checkLock = false;                   // coverage
                RemoveErrorHandler::handleRemoveLock(lock, checkLock);
            } else if (lock->getLockMode() == LockDesc::CRT ||
                       lock->getLockMode() == LockDesc::WR) {
                UpdateErrorHandler::handleErrorOnCoordinator(lock);
            } else {
                ASSERT_EXCEPTION((lock->getLockMode() == LockDesc::RNF ||
                                  lock->getLockMode() == LockDesc::XCL ||
                                  lock->getLockMode() == LockDesc::RD),
                                 string("Unrecognized array lock on recovery: ") +
                                     lock->toString());
            }
        }

        /* NOTE: All transient arrays are invalidated on (re)start in the
           catalog
         */

        SystemCatalog::getInstance()->deleteCoordArrayLocks(getInstanceId());
    }

    {  // Deal with the worker locks next
        for (list<std::shared_ptr<LockDesc>>::const_iterator iter = workerLocks.begin();
             iter != workerLocks.end();
             ++iter) {
            const std::shared_ptr<LockDesc>& lock = *iter;

            if (lock->getLockMode() == LockDesc::CRT || lock->getLockMode() == LockDesc::WR) {
                // From a rollback perspective, query cancellation and
                // instance startup look the same.
                const bool queryCancelled = true;
                UpdateErrorHandler::handleErrorOnWorker(lock, queryCancelled);
            } else {
                ASSERT_EXCEPTION(lock->getLockMode() == LockDesc::XCL,                      // coverage
                                 string("Unrecognized array lock on recovery: ") +
                                     lock->toString());
            }
        }

        /* NOTE: All transient arrays are invalidated on (re)start in the
           catalog.
         */

        SystemCatalog::getInstance()->deleteWorkerArrayLocks(getInstanceId());
    }

    _inRecovery = false;
}

void StorageMgr::openStorage(std::string const& storageConfigPath)
{   // this function called from NetworkManager::run() which is called from entry.cpp, so its hard test different branches
    // in this file ... each one will require a different restart of scidb, so defer until ready to do that.
    /* Read/create the storage description file
     */
    initStorageConfigFile(storageConfigPath);

    /* Initialize the data stores
     */
    string dataStoresBase = _databasePath + "/datastores";
    DataStores::getInstance()->initDataStores(dataStoresBase.c_str());

    /* Read/initialize metadata header
     */
    initStorageHeader();                        // calls deeper things

    if (getInstanceId() != INVALID_INSTANCE) {
        /* Rollback uncommitted changes for persistent arrays
           (or compete remove and remove version transactions)
        */
        doTxnRecoveryOnStartup();
    }

    /* Check metadata for chunks from removed versions
     */
}

/**
 * Return the instance ID for this server
 */
InstanceID StorageMgr::getInstanceId() const
{
    return _hdr.instanceId; // 注意，自身的ins id存储在/opt/scidb/19.11/DB-scidb/0/* 目录里面的storage.hdr里面
}

/**
 * Set the instance ID for this server
 */
void StorageMgr::setInstanceId(InstanceID id)           // coverage
{
    _hdr.instanceId = id;
    _hd->writeAll(&_hdr, sizeof(_hdr), 0);
    int rc = _hd->fsync();
    if (rc != 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED_WITH_ERRNO)
            << "fsync" << ::strerror(errno) << errno;
    }
}

void checkConfigNewStorage()
{
    // SDB-5852: newDBArray requires that chunk-size-limit-mb < mem-array-threshold
    auto chunk_size_limit_mb = Config::getInstance()->getOption<size_t>(CONFIG_CHUNK_SIZE_LIMIT);
    auto mem_array_threshold = Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD);

    if (chunk_size_limit_mb > mem_array_threshold) {
        // this is currently called outside of the general exception catcher which would
        // print the exception to the log (because there is no query yet, etc)
        // so log the error directly
        LOG4CXX_ERROR(logger, "scidb config.ini: chunk-size-limit-mb=" << chunk_size_limit_mb
                              << ") must be <= mem-array-threshold=" << mem_array_threshold);

        // if the code ever moves inside the general exception catcher
        // "Bad value '%1%' for configuration option %2%: %3%")
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_BAD_CONFIG_VALUE) << chunk_size_limit_mb  //coverage
                                                                            << "CONFIG_CHUNK_SIZE_LIMIT"
                                                                            << "must be <= mem-array-threshold";
    }
}

}  // namespace scidb
