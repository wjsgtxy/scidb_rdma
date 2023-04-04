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
 * @file LockDesc.h
 * @brief SystemCatalog lock descriptors.
 */

#ifndef LOCKDESC_H
#define LOCKDESC_H

#include <array/ArrayID.h>
#include <array/VersionID.h>
#include <query/QueryID.h>
#include <query/InstanceID.h>
#include <system/Exceptions.h>

#include <memory>
#include <set>

namespace scidb {

class LockDesc
{
public:
    typedef enum {INVALID_ROLE=0, COORD, WORKER} InstanceRole;

    // Array locks are used for mutual exclusion AND for TXN rollback purposes
    // Some locks have no effect on the rollback actions, while others do.
    // See CachedStorage::doTxnRecoveryOnStartup() for details.
    // Currently, the locks are used to maintain the SNAPSHOT ISOLATION txn serialization.
    typedef enum {
        INVALID_MODE=0,
        RD,                 /// Read, conflict with >=RM
        WR,                 /// Write, rollback implications, conflict with >=WR
        CRT,                /// Create,rollback implications, conflict with >=WR
        RM,                 /// Remove,rollback implications, conflict with >=RD
        XCL,                /// Exclusive Lock, conflict with >=RD
        RNF                 /// Rename from, conflict with >=RD
    } LockMode;

    LockDesc(const std::string &namespaceName,
             const std::string& arrayName,
             const QueryID&  queryId,
             InstanceID   instanceId,
             InstanceRole instanceRole,
             LockMode lockMode);

    ~LockDesc() = default;

    const std::string& getArrayName() const { return _arrayName; }
    const std::string& getNamespaceName() const { return _namespaceName; }

    ArrayID   getArrayId() const { return _arrayId; }
    const QueryID&   getQueryId() const { return _queryId; }
    InstanceID    getInstanceId() const { return _instanceId; }
    VersionID getArrayVersion() const { return _arrayVersion; }
    ArrayID   getArrayVersionId() const { return _arrayVersionId; }
    ArrayID   getArrayCatalogId() const { assert(isLocked()); return _arrayCatalogId; }
    InstanceRole  getInstanceRole() const
    {
        if (_queryId.getCoordinatorId() == _instanceId) {
            return COORD;
        }
        return WORKER;
    }
    LockMode  getLockMode() const { return _lockMode; }
    bool  isLocked() const { return _isLocked; }
    void setArrayId(ArrayID arrayId) { _arrayId = arrayId; }
    void setArrayVersionId(ArrayID versionId) { _arrayVersionId = versionId; }
    void setArrayCatalogId(ArrayID catalogId) { _arrayCatalogId = catalogId; }
    void setArrayVersion(VersionID version) { _arrayVersion = version; }
    void setLockMode(LockMode mode) { _lockMode = mode; }
    void setLocked(bool isLocked) { _isLocked = isLocked; }
    std::string toString();

private:
    LockDesc(const LockDesc&) = delete;
    LockDesc& operator=(const LockDesc&) = delete;

    std::string     _namespaceName; // For example, ns1
    std::string     _arrayName;     // For example, COEFFS
    std::string     _fullArrayName; // For example, ns1.COEFFS

    ArrayID  _arrayId;          // Unversioned array id (UAID) of locked array
    QueryID  _queryId;          // Id of query holding this lock
    InstanceID   _instanceId;   // Physical instance id holding this lock
    ArrayID  _arrayVersionId;   // Versioned array id (VAID) of locked array
    ArrayID  _arrayCatalogId;   // Highest (version) array ID for _arrayId; right after all the
                                //   query locks are acquired, the state of the catalog should be
                                //   such that _arrayId <= _arrayCatalogId < _arrayVersionId (where
                                //   0 means non-existent).
    VersionID _arrayVersion;    // For example, the 6 in ns1.COEFFS@6
    LockMode  _lockMode;        // See above
    bool _isLocked;             // True iff lock is committed in the SystemCatalog
};


/**
 * Deep comparison of shared pointers to LockDesc.
 */
struct LockPtrLess : std::binary_function <const std::shared_ptr<LockDesc>&,
                                           const std::shared_ptr<LockDesc>&,
                                           bool>
{
    bool operator() (const std::shared_ptr<LockDesc>& l,
                     const std::shared_ptr<LockDesc>& r) const
    {
        ASSERT_EXCEPTION(l && r, "LockPtrLess: NULL argument");
        if (l->getNamespaceName() < r->getNamespaceName()) {
            return true;
        }
        if (l->getNamespaceName() > r->getNamespaceName()) {
            return false;
        }
        return l->getArrayName() < r->getArrayName();
    }
};


/**
 * This exception is thrown when an array is already locked (by a different query).
 */
DECLARE_SYSTEM_EXCEPTION_SUBCLASS(LockBusyException,
                                  SCIDB_SE_EXECUTION,
                                  SCIDB_LE_RESOURCE_BUSY);

typedef std::set<std::shared_ptr<LockDesc>, LockPtrLess > QueryLocks;

} // namespace scidb

#endif  /* ! LOCKDESC_H */
