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
 * @file StringUtil.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <util/StringUtil.h>

using namespace std;

template <typename T>
string strJoinImpl(vector<string> const& items, T sep)
{
    ostringstream oss;
    string s;
    for (auto const& i : items) {
        if (s.empty()) {
            s = sep;
        } else {
            oss << s;
        }
        oss << i;
    }
    return oss.str();
}

string strJoin(vector<string> const& items, string const& sep)
{
    return strJoinImpl(items, sep);
}

string strJoin(vector<string> const& items, char sep)
{
    return strJoinImpl(items, sep);
}
