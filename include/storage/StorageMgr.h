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
 * @file StorageMgr.h
 *
 * @author Steve Fridella
 *
 * @brief Declaration of storage management classes.
 */

#ifndef STORAGEMGR_H_
#define STORAGEMGR_H_

#include <util/Singleton.h>
#include <util/DataStore.h>
#include <util/Arena.h>
#include <util/PointerRange.h>
#include <util/Mutex.h>
#include <util/Condition.h>

namespace scidb {

/**
 * StorageMgr manages location of storage directory,
 * instance number and storage version.
 */
class StorageMgr : public Singleton<StorageMgr>
{
public:
    /**
     * If you are changing the format of the first three fields of the
     * StorageHeader class (very rare), then you MUST change this number.
     * Illegal values are values that are likely to occur in a corrupted
     * file by accident, like:
     * 0x00000000
     * 0xFFFFFFFF
     *
     * Or values that have been used in the past:
     * 0xDDDDBBBB
     * 0x5C1DB123
     *
     * You must pick a value that is not equal to any of the values above -
     * AND add it to the list.  Picking a new magic has the effect of storage
     * file not being transferrable between scidb versions with different magic
     * values.
     */
    const uint32_t SCIDB_STORAGE_HEADER_MAGIC = 0x5C1DB123;

    /**
     * If you are changing the format of the StorageHeader class
     * (other than the first 3 fields), or any other structures that are saved
     * to disk, such as DataStore or DiskIndex::Key - you must increment this
     * number.
     * When storage format versions are different, it is up to the scidb code
     * to determine if an upgrade is possible. At the moment of this writing,
     * scidb with storage version X simply will refuse to read the metadata
     * created by storage version Y. Future behavior may be a lot more
     * sophisticated.
     *
     * Revision history:
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 11:
     *    Author: Steve F.
     *    Date: 8/10/17
     *    Ticket:
     *    Note: New storage stack and metadata storage using RocksDB.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 10:
     *    Author: tigor
     *    Date:
     *    Ticket:
     *    Note: Allowing chunk header to track uint64 instance IDs
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 9:
     *    Author: Steve F.
     *    Date: 7/2/15
     *    Ticket: 4534
     *    Note: Allowing chunk header to track more than 2^32 - 1 elements
     *          in a chunk
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 8:
     *    Author: Dave Gosselin
     *    Date: 8/21/14
     *    Ticket: 3672
     *    Note: Removing the dependency on sparsity and RLE checks means that
     *          uncompressed chunks will always be stored in RLE format.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 7:
     *    Author: Steve F.
     *    Date: 7/11/14
     *    Ticket: 3719
     *    Note: Now store storage version and datastore id in the tombstone
     *          chunk header
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 6:
     *    Author: Steve F.
     *    Date: TBD
     *    Ticket: TBD
     *    Note: Changed data file format to use power-of-two allocation size
     *          with buddy blocks.
     *          Also, split data file into multiple files, one per array.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 5:
     *    Author: tigor
     *    Date: 10/31/2013
     *    Ticket: 3404
     *    Note: Removal of PersistentChunk clones
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 4:
     *    Author: Alex P.
     *    Date: 5/28/2013
     *    Ticket: 2253
     *    Note: As a result of a long discussion, revamped and tested this
     *          behavior. Added min and max version to the storage header.
     *          Added a version number to each chunk header - to allow for
     *          future upgrade flexibility.
     *
     * SCIDB_STORAGE_FORMAT_VERSION = 3:
     *    Author: ??
     *    Date: ??
     *    Ticket: ??
     *    Note: Initial implementation dating back some time
     */
    const uint32_t SCIDB_STORAGE_FORMAT_VERSION = 11;

    /**
     * The format of the metadata file that describes the storage
     * version.
     */
    struct StorageHeader
    {
        /**
         * A constant special value the header file must begin with.
         * If it's not equal to SCIDB_STORAGE_HEADER_MAGIC, then we know
         * for sure the file is corrupted.
         */
        uint32_t magic {0};

        /**
         * The smallest storage version of all the chunks that are currently
         * stored.  Currently it's always equal to versionUpperBound; this
         * is a placeholder for the future.
         */
        uint32_t versionLowerBound {0};

        /**
         * The largest storage version of all the chunks that are currently
         * stored.  Currently it's always equal to versionLowerBound; this
         * is a placeholder for the future.
         */
        uint32_t versionUpperBound {0};

        /**
         * This instance ID.
         */
        InstanceID instanceId {INVALID_INSTANCE};
    };

    /**
     * Constructor
     */
    StorageMgr()
        : _inRecovery(false)
    {}

    /**
     * Initialize the storage header.  Verify the storage magic and storage
     * version number are correct.
     * @param[in] storageConfigPath  location of the storage config file
     */
    void openStorage(std::string const& storageConfigPath);

    /**
     * Return the instance ID for this server
     */
    InstanceID getInstanceId() const;

    /**
     * Set the instance ID for this server
     */
    void setInstanceId(InstanceID id);

    /**
     * Check whether recover is in process
     */
    bool inRecovery() { return _inRecovery; }

private:
    /**
     * Read/create the config file
     */
    void initStorageConfigFile(std::string const& storageConfigFile);

    /**
     * Read/verify or create the storage header file
     */
    void initStorageHeader();

    /**
     * Find and recover any on-going transactions after a crash or
     * reboot.
     * @pre instance id must be valid
     */
    void doTxnRecoveryOnStartup();

    // clang-format off
    std::string   _databasePath;   // full path to db directory
    std::string   _configFileName; // file name of the config file
    std::string   _databaseHeader; // full path of chunk header file
    StorageHeader _hdr;            // in-memory storage header
    File::FilePtr _hd;             // storage header file descriptor
    bool          _inRecovery;     // true when running transaction recovery
    // clang-format on
};

void checkConfigNewStorage();

}  // namespace scidb
#endif /* STORAGEMGR_H_ */
