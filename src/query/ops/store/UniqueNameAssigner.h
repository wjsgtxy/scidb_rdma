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

#ifndef UNIQUENAMEASSIGNER_H_
#define UNIQUENAMEASSIGNER_H_

#include <string>
#include <unordered_map>

namespace scidb
{

/**
 * A utility that assigns unique names.
 *
 * Usage:
 * @code
 *   UniqueNameAssigner assigner;
 *   for (each attribute or dimension name that could have duplicates)
 *       assigner.insertName(the name)
 *   for (each attribute or dimension name that could have duplicates)
 *       string uniqueName = assigner.assignUniqueName(the name)
 * @endcode
 *
 * @author Donghui Zhang
 */
class UniqueNameAssigner
{
public:
    /// @param category  an optional descriptive name.
    explicit UniqueNameAssigner(const std::string& category = "");

    /// Insert a name, ignoring duplicates.
    void insertName(const std::string& name);

    /// Remove a name.
    void removeName(const std::string& name);

    /// @return  a name that has not been returned before.
    std::string assignUniqueName(const std::string& name);

    /// @return  the category
    const std::string& category() const noexcept
    {
        return _category;
    }

private:
    /// @return  a name that consists of name and count.
    /// Here we preserve the existing SciDB convention of name, name_2, name_3, etc.
    /// @pre count >= 1.
    std::string forgeName(const std::string& name, const size_t count);

    using NameToCount = std::unordered_map<std::string, size_t>;
    NameToCount _nameToCount;
    std::string _category;
};

}  // namespace scidb

#endif /* UNIQUENAMEASSIGNER_H_ */
