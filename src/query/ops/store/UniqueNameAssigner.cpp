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

#include "UniqueNameAssigner.h"
#include <assert.h>
#include <sstream>

using namespace std;

namespace scidb
{

UniqueNameAssigner::UniqueNameAssigner(const std::string& category)
  : _nameToCount(), _category(category)  // initializing _nameToCount may avoid a warning if compile with -Weffc++
{}

void UniqueNameAssigner::insertName(const string& name)
{
    _nameToCount[name] = 0;
}

void UniqueNameAssigner::removeName(const string& name)
{
    _nameToCount.erase(name);
}

string UniqueNameAssigner::assignUniqueName(const string& name)
{
    NameToCount::iterator it = _nameToCount.find(name);
    if (it == _nameToCount.end()) {
        return name;
    }

    // Determine a unique count.
    size_t count = it->second + 1;  // pre-increment.
    if (count >= 2) {
        // The name was assigned before! We definitely need a name with COUNT suffix. Make sure we pick a unique one.
        while (true) {
            string tmpName = forgeName(name, count);
            if (_nameToCount.find(tmpName) == _nameToCount.end()) {
                break;
            }
            ++count;
        }
    }

    // Record the count.
    it->second = count;

    // Return a unique name.
    if (count == 1) {
        return name;
    }
    return forgeName(name, count);
}

std::string UniqueNameAssigner::forgeName(const string& name, const size_t count)
{
    assert(count >= 1);

    if (count == 1) {
        return name;
    }
    stringstream ss;
    ss << name << "_" << count;
    return ss.str();
}

}  // namespace scidb
