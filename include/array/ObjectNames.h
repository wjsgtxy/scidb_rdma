#ifndef OBJECTNAMES_H_
#define OBJECTNAMES_H_
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
#include <array/Coordinate.h>

#include <boost/operators.hpp>
#include <boost/serialization/map.hpp>

#include <string>

namespace scidb {

/**
 * @brief Class containing all possible object names
 *
 * During array processing schemas can be merged in many ways. For example NATURAL JOIN contain all
 * attributes from both arrays and dimensions combined. Attributes in such example received same names
 * as from original schema and also aliases from original schema name if present, so it can be used
 * later for resolving ambiguity. Dimensions in output schema received not only aliases, but also
 * additional names, so same dimension in output schema can be referenced by old name from input schema.
 *
 * Despite object using many names and aliases catalog storing only one name - base name. This name
 * will be used also for returning in result schema. So query processor handling all names but storage
 * and user API using only one.
 *
 * @note Alias this is not full name of object! Basically it prefix received from schema name or user
 * defined alias name.
 */
class ObjectNames : boost::equality_comparable<ObjectNames>
{
public:
    typedef std::set<std::string>               AliasesType;
    typedef std::map<std::string,AliasesType>   NamesType;
    typedef std::pair<std::string,AliasesType>  NamesPairType;

    ObjectNames();

    /**
     * Constructing initial name without aliases and/or additional names. This name will be later
     * used for returning to user or storing to catalog.
     *
     * @param baseName base object name
     */
    explicit ObjectNames(const std::string &baseName);

    /**
     * Constructing full name
     *
     * @param baseName base object name
     * @param names other names and aliases
     */
    ObjectNames(const std::string &baseName, const NamesType &names);

    /**
     * Add new object name
     *
     * @param name object name
     */
    void addName(const std::string &name);

    /**
     * Add new alias name to object name
     *
     * @param alias alias name
     * @param name object name
     */
    void addAlias(const std::string &alias, const std::string &name);

    /**
     * Add new alias name to all object names
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * Check if object has such name and alias (if given).
     *
     * @param name object name
     * @param alias alias name
     * @return true if has
     */
    bool hasNameAndAlias(const std::string &name, const std::string &alias = "") const;

    /**
     * Get all names and aliases of object
     *
     * @return names and aliases map
     */
    const NamesType& getNamesAndAliases() const;

    /**
     * Get base name of object.
     *
     * @return base name of object
     */
    const std::string& getBaseName() const;

    bool operator==(const ObjectNames &o) const;

    friend std::ostream& operator<<(std::ostream& stream, const ObjectNames &ob);
    friend void printSchema (std::ostream& stream, const ObjectNames &ob);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _baseName;
        ar & _names;
    }

protected:
    NamesType   _names;
    std::string _baseName;
};

void printNames (std::ostream&,const ObjectNames::NamesType&);
std::ostream& operator<<(std::ostream&,const ObjectNames::NamesType&);

}  // namespace scidb
#endif
