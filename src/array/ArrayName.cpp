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

#include <array/ArrayName.h>
#include <rbac/Rbac.h>
#include <system/Exceptions.h>

#include <sstream>

using namespace std;

namespace scidb {

void splitQualifiedArrayName(
    const string&     qualifiedArrayName,
    string&           namespaceName,
    string&           arrayName)
{
    string::size_type pos = qualifiedArrayName.find(".");
    if(pos != string::npos) {
        namespaceName = qualifiedArrayName.substr(0, pos);
        arrayName = qualifiedArrayName.substr(pos+1, qualifiedArrayName.length());
    } else {
        // The namespaceName is not set if the qualifiedArrayName is not qualified.
        arrayName = qualifiedArrayName;
    }
}

string getUnqualifiedArrayName(
    const string& arrayName)
{
    string::size_type pos = arrayName.find(".");
    if(pos != string::npos) {
        return arrayName.substr(pos+1, arrayName.length());
    }

    return arrayName;
}

string makeQualifiedArrayName(
    const string&           namespaceNameIn,
    const string&           arrayNameIn)
{
    if (arrayNameIn.empty()) {
        return arrayNameIn;
    }

    string arrayName = arrayNameIn;
    string nsName = namespaceNameIn;

    if(isQualifiedArrayName(arrayName))
    {
        splitQualifiedArrayName(arrayNameIn, nsName, arrayName);
        if(nsName != namespaceNameIn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ARRAY_NAME_ALREADY_QUALIFIED)
                  << nsName << arrayName;
        }

        return arrayNameIn;
    }

    if (nsName.empty()) {
        nsName = rbac::PUBLIC_NS_NAME;
    }

    stringstream ss;
    ss << nsName << '.' << arrayName;
    return ss.str();
}

bool isQualifiedArrayName(const string& arrayName)
{
    return (arrayName.find('.') != string::npos);
}

bool isNameVersioned(string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
              << "calling isNameVersioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt > 0
        && locationOfAt < name.size()
        && locationOfColon == string::npos;
}

bool isNameUnversioned(string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
              << "calling isNameUnversioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt == string::npos && locationOfColon == string::npos;
}

string makeUnversionedName(string const& name)
{
    if (isNameVersioned(name))
    {
        size_t const locationOfAt = name.find('@');
        return name.substr(0, locationOfAt);
    }
    return name;
}

VersionID getVersionFromName(string const& name)
{
    if(isNameVersioned(name))
    {
        size_t locationOfAt = name.find('@');
        return ::atol(&name[locationOfAt+1]);
    }
    return 0;
}

string makeVersionedName(string const& name, VersionID const version)
{
    assert(!isNameVersioned(name));
    stringstream ss;
    ss << name << "@" << version;
    return ss.str();
}

}  // namespace scidb
