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
 * @file QueryID.cpp
 */

#include <query/QueryID.h>

using namespace std;

namespace scidb {

/// Stream input operator for QueryID
/// it is used format the string representation of QueryID
std::istream& operator>>(std::istream& is, QueryID& qId)
{
    InstanceID instanceId(INVALID_INSTANCE);
    uint64_t id(0);
    char dot(0);

    is >> instanceId;
    if (is.fail()) { return is; }

    is >> dot;
    if (is.fail()) { return is; }
    if( dot != '.') {
        is.setstate(std::ios::failbit);
        return is;
    }

    is  >> id;
    if (is.fail()) { return is; }

    qId = QueryID(instanceId,id);
    return is;
}

/// Stringification method for QueryID
string QueryID::toString() const
{
    ostringstream oss;
    oss << getCoordinatorId() << '.' << getId();
    return oss.str();
}

QueryID QueryID::fromString(const std::string& queryIdStr)
{
    QueryID id;
    std::istringstream idstrm(queryIdStr);
    idstrm >> id;
    return id;
}

/// Stream output operator for QueryID
ostream& operator<<(std::ostream& os, const QueryID& qId)
{
    os << qId.toString();
    return os;
}

} // namespace scidb
