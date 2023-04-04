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
 * @file Projector.h
 */

#include <array/AttributeID.h>
#include <string>
#include <vector>

namespace scidb
{

/**
 * @brief List of attributes for project()-like operations, with helper methods.
 */
struct Projection : std::vector<AttributeID>
{
    /// Translate to string for transmission as part of physical query plans.
    std::string toString() const;

    /// Create from string during receive of physical query plan.
    static Projection fromString(std::string const& s);
};

} // namespace scidb
