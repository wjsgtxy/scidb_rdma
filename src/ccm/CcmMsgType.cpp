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

// The header file for the implementation details in this file
#include "CcmMsgType.h"
// c++ standard libraries
#include <iostream>

namespace scidb { namespace ccm {

std::ostream& operator<<(std::ostream& os, const scidb::ccm::CcmMsgType& mtype)
{
    switch (mtype) {
        // clang-format off
#   define X(_name, _code, _desc)                \
        case (scidb::ccm::CcmMsgType::_name): {  \
            os << #_name << "(" << _code << ")"; \
        } break;

#   include "CcmMsgTypes.inc"
#   undef X
        // clang-format on
        default:
            os << " UNKNOWN_CCM_MSG_TYPE (" << static_cast<uint32_t>(mtype) << ")";
            break;
    }
    return os;
}
}}  // namespace scidb::ccm
