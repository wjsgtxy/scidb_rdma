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

#include <system/UserException.h>
#include <util/compression/CompressorType.h>

#include <algorithm>

namespace scidb {

CompressorType stringToCompressorType(std::string ctStr)
{
    std::transform(ctStr.begin(), ctStr.end(), ctStr.begin(), ::tolower);
    CompressorType ct = CompressorType::NONE;

    if (ctStr == "none") {
        // Even though ct is initialized to CompressorType::NONE,
        // let's be explicit about each supported type.
        ct = CompressorType::NONE;
    }
    else if (ctStr == "zlib") {
        ct = CompressorType::ZLIB;
    }
    else if (ctStr == "bzlib") {
        ct = CompressorType::BZLIB;
    }
    else {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_COMPRESSOR_DOESNT_EXIST)
            << ctStr;
    }

    return ct;
}

std::string compressorTypeToString(CompressorType ct)
{
    switch (ct) {
    case CompressorType::NONE:
        return "none";
        break;
    case CompressorType::ZLIB:
        return "zlib";
        break;
    case CompressorType::BZLIB:
        return "bzlib";
        break;
    default:
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_COMPRESSOR_DOESNT_EXIST)
            << std::to_string(ct);
    };

    SCIDB_UNREACHABLE();
}

}  // namespace scidb
