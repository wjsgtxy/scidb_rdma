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
#include "CcmErrorCode.h"
// c++ standard libraries
#include <string>
#include <vector>

namespace scidb { namespace ccm {

namespace {
std::vector<std::string> ecodes;
// clang-format off
}
// clang-format on

std::string errorCodeToString(CcmErrorCode e)
{
    if (ecodes.size() == 0) {
        ecodes.resize(100);  // THIS should be good enough for now
        // clang-format off
#       define X(_name, _code, _message)                                         \
          ecodes[static_cast<size_t>(CcmErrorCode::_name)] = #_name ": " _message;
#       include "CcmErrorCodes.inc"
#       undef X
        // clang-format on
    }
    return ecodes[static_cast<size_t>(e)];
}
}}  // namespace scidb::ccm
