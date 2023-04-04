#ifndef VERSIONID_H_
#define VERSIONID_H_
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

#include <type_traits>
#include <cstdint>
#include <ctime>

namespace scidb {
/**
 * Identifier of array version
 */
typedef uint64_t VersionID;

const VersionID   NO_VERSION            = 0UL;
const VersionID   LAST_VERSION          = (VersionID)-1;
const VersionID   ALL_VERSIONS          = (VersionID)-2;
const VersionID   LAST_SPECIAL_VERSION  = (VersionID)-3; // MUST BE LAST!


inline bool isSpecialVersionId(VersionID vid)
{
    static_assert(std::is_unsigned<VersionID>::value,
        "Comparison assumes unsigned VersionID, fix if that changes");
    return vid >= LAST_SPECIAL_VERSION;
}

}  // namespace scidb
#endif
