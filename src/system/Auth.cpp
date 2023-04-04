/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file Auth.cpp
 * @brief Authentication related functions.
 */

#include <system/Auth.h>
#include <system/Config.h>

#include <algorithm>
#include <string>
#include <string.h>

using namespace std;

namespace scidb { namespace auth {

namespace {
    struct AuthMethodInfo {
        int value;
        const char* name;
        const char* tag;
        const char* desc;
    };

    AuthMethodInfo authMethodTable[] = {
#   define X(name, val, tag, desc)  { val, #name , tag, desc },
#   include <system/AuthMethods.inc>
#   undef X
    };

    int const TBL_SIZE = sizeof(authMethodTable) / sizeof(AuthMethodInfo);

} // anonymous namespace


const char *strMethodName(AuthMethod m)
{
    int index = int(m);
    if (index >= TBL_SIZE) {
        return "strMethodName: bad auth method";
    }
    return authMethodTable[index].name;
}

const char *strMethodTag(AuthMethod m)
{
    int index = int(m);
    if (index >= TBL_SIZE) {
        return "strMethodTag: bad auth method";
    }
    return authMethodTable[index].tag;
}

const char *strMethodDesc(AuthMethod m)
{
    int index = int(m);
    if (index >= TBL_SIZE) {
        return "strMethodDesc: bad auth method";
    }
    return authMethodTable[index].desc;
}

AuthMethod tagToMethod(const char* tag)
{
    for (unsigned i = 0; i < TBL_SIZE; ++i) {
        if (!::strcmp(tag, authMethodTable[i].tag)) {
            return (AuthMethod)authMethodTable[i].value;
        }
    }
    return AUTH_NONE;
}

AuthMethod tagToMethod(string const& tag)
{
    return tagToMethod(tag.c_str());
}

} } // namespaces
