#ifndef ATTRIBUTEID_H_
#define ATTRIBUTEID_H_
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

#include <cstdint>

namespace scidb {
/**
 * Attribute identifier (attribute number in array description)
 */
typedef uint32_t AttributeID;

const AttributeID INVALID_ATTRIBUTE_ID  = static_cast<uint32_t>(~0);

// It turns-out that postgres' integer width is 4 bytes, and due to
// the schema that we use for tracking SciDB arrays in the catalog,
// the max attribute ID is 0x7FFFFFFF (2,147,483,647).  Let's
// make the EBM attribute ID at-rest equal-to one-less than that value,
// 0x7FFFFFFE (2,147,483,646), so that should we choose to, we can make
// the INVALID_ATTRIBUTE_ID value above equal-to 0x7FFFFFFF as that
// value will still be available and unreserved.
const AttributeID EBM_ATTRIBUTE_ID = static_cast<uint32_t>(0x7FFFFFFE);

}  // namespace scidb
#endif
