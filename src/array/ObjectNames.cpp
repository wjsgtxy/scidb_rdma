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

#include <array/ObjectNames.h>

#include <array/ArrayName.h>
#include <rbac/Rbac.h>

#include <boost/algorithm/string.hpp>

using namespace std;

namespace scidb {

ObjectNames::ObjectNames()
{}

ObjectNames::ObjectNames(const std::string &baseName):
    _baseName(baseName)
{
    addName(baseName);
}

ObjectNames::ObjectNames(const std::string &baseName, const NamesType &names):
    _names(names),
    _baseName(baseName)
{}

void ObjectNames::addName(const std::string &name)
{
    string trimmedName = name;
    boost::algorithm::trim(trimmedName);
    assert(trimmedName != "");

    if (hasNameAndAlias(name))
        return;

    _names[name] = set<string>();
}

void ObjectNames::addAlias(const std::string &alias, const std::string &name)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        boost::algorithm::trim(trimmedAlias);
        assert(trimmedAlias != "");

        string trimmedName = name;
        boost::algorithm::trim(trimmedName);
        assert(trimmedName != "");

        _names[name].insert(alias);
    }
}

void ObjectNames::addAlias(const std::string &alias)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        boost::algorithm::trim(trimmedAlias);
        assert(trimmedAlias != "");

        for (const NamesPairType &nameAlias : _names)
        {
            _names[nameAlias.first].insert(alias);
        }
    }
}

bool ObjectNames::hasNameAndAlias(const std::string &name, const std::string &alias) const
{
    NamesType::const_iterator nameIt = _names.find(name);

    if (nameIt != _names.end())
    {
        if (alias.empty())
            return true;
        else {
            std::string qualifiedAlias;
            if(!isQualifiedArrayName(alias))
            {
                qualifiedAlias = makeQualifiedArrayName(rbac::PUBLIC_NS_NAME, alias);
            }

            return
                ((*nameIt).second.find(alias) != (*nameIt).second.end()) ||
                ((*nameIt).second.find(qualifiedAlias) != (*nameIt).second.end());
        }
    }

    return false;
}

const ObjectNames::NamesType& ObjectNames::getNamesAndAliases() const
{
    return _names;
}

const std::string& ObjectNames::getBaseName() const
{
    return _baseName;
}

bool ObjectNames::operator==(const ObjectNames &o) const
{
    return _names == o._names;
}

std::ostream& operator<<(std::ostream& os,const ObjectNames::NamesType::value_type& pair)
{
    auto i = pair.second.begin();
    auto e = pair.second.end();
    auto d = ", ";

    os << '{' << pair.first;
    while (i != e)
    {
        os << d << *i++ << '.' << pair.first;
    }
    os << '}';

    return os;
}

std::ostream& operator<<(std::ostream& os, const ObjectNames::NamesType& ob)
{
    auto i = ob.begin();
    auto e = ob.end();
    auto d = ", ";

    if (i != e)                                          // Non-empty range?
    {
        os << *i;                                        // ...format first

        for ( ++i; i != e; ++i)                          // ...for each next
        {
            os << d << *i;                               // ....delimit it
        }
    }

    return os;                                           // Return the stream
}

void printNames (std::ostream& os, const ObjectNames::NamesType &ob)
{
    for (auto nameIt = ob.begin(); nameIt != ob.end(); ++nameIt)
    {
        if (nameIt != ob.begin())
        {
            os << ", ";
        }
        os << nameIt->first;
    }
}


}  // namespace scidb
