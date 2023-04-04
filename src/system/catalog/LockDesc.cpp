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

#include <system/LockDesc.h>

#include <array/ArrayName.h>
#include <system/Utils.h> // For SCIDB_ASSERT

namespace scidb {

LockDesc::LockDesc(const std::string& namespaceName,
                   const std::string& arrayName,
                   const QueryID&  queryId,
                   InstanceID   instanceId,
                   InstanceRole instanceRole,
                   LockMode lockMode)
    : _namespaceName(namespaceName),
      _arrayName(arrayName),
      _fullArrayName(
          makeQualifiedArrayName(
              _namespaceName, _arrayName)),
    _arrayId(0),
    _queryId(queryId),
    _instanceId(instanceId),
    _arrayVersionId(0),
    _arrayCatalogId(0),
    _arrayVersion(0),
    _lockMode(lockMode),
    _isLocked(false)
{
    SCIDB_ASSERT(!_namespaceName.empty());
    SCIDB_ASSERT(!_arrayName.empty());
    SCIDB_ASSERT(!isQualifiedArrayName(arrayName));
    SCIDB_ASSERT(isValidPhysicalInstance(instanceId));
    SCIDB_ASSERT(isValidPhysicalInstance(_queryId.getCoordinatorId()));

    ASSERT_EXCEPTION((instanceRole == COORD && _queryId.getCoordinatorId() == instanceId) ||
                     (instanceRole == WORKER && _queryId.getCoordinatorId() != instanceId),
                     "Invalid query ID or instance ID");
}

std::string LockDesc::toString()
{
    std::ostringstream out;
    out << "Lock: "
        << "namespaceName="
        << _namespaceName
        << ", arrayName="
        << _arrayName
        << ", arrayId="
        << _arrayId
        << ", queryId="
        << _queryId
        << ", instanceId="
        << _instanceId
        << ", instanceRole="
        << (_queryId.getCoordinatorId() == _instanceId ? "COORD" : "WORKER")
        << ", lockMode="
        << _lockMode
        << ", arrayVersion="
        << _arrayVersion
        << ", arrayVersionId="
        << _arrayVersionId
        << ", arrayCatalogId="
        << _arrayCatalogId;

    return out.str();
}

} // namespace scidb
