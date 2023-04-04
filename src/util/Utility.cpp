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
 * @file util/Utility.cpp
 * @brief A catch-all for snippets unworthy (so far) of their own .cpp file.
 */

#include <util/Utility.h>

#include <cxxabi.h>
#include <memory>

using namespace std;

#define SCIDB_DO_PRAGMA(x) _Pragma (#x)
#define SCIDB_MESSAGE(s) SCIDB_DO_PRAGMA(message ("SCIDB_MESSAGE:  " #s))


/****************************************************************************/
namespace scidb {

#if !defined (__GNUC__)
SCIDB_MESSAGE(NEED TO IMPLEMENT getTrailingZeros(size_t))

/*  Example code for the non-GNUC environment
size_t getTrailingZeros(size_t value)
{
    // Algorithm - O(lg N)
    if (0UL == value)  return 64;

    size_t result = 0;
    if (0UL == (value & 0x00000000FFFFFFFFUL)) { result += 32;  value >>= 32; }
    if (0UL == (value & 0x000000000000FFFFUL)) { result += 16;  value >>= 16; }
    if (0UL == (value & 0x00000000000000FFUL)) { result +=  8;  value >>=  8; }
    if (0UL == (value & 0x000000000000000FUL)) { result +=  4;  value >>=  4; }
    if (0UL == (value & 0x0000000000000003UL)) { result +=  2;  value >>=  2; }
    if (0UL == (value & 0x0000000000000001UL)) { result +=  1;  }

    return result;
}
*/
#endif  // #if !defined (__GNUC__)

std::string demangle_cpp(const char* name)
{
    int status = -4; // abi::__cxa_demangle sets a status int in the range [-3, 0]
    std::unique_ptr<char, void (*)(void*)> res{abi::__cxa_demangle(name, NULL, NULL, &status),
                                               std::free};
    return (status == 0) ? res.get() : name;
}

}  // namespace scidb  { ... }
