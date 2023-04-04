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

/*
 * VariableWindow.h
 *  Created on: Feb 9, 2011
 *      Author: poliocough@gmail.com
 */

#include "WindowEdge.h"

namespace scidb
{

using CoordsToWindowEdge = std::unordered_map<Coordinates, std::shared_ptr<WindowEdge>, CoordinatesHash>;

/**
 * A wrapper over an CoordsToWindowEdge object, to enable equality comparison of two such objects.
 */
class ChunkEdge
{
public:
    bool operator == (const ChunkEdge & other ) const;
    bool operator != (const ChunkEdge & other ) const;
    CoordsToWindowEdge& operator()();
    const CoordsToWindowEdge& operator()() const;

private:
    friend std::ostream& operator<<(std::ostream& stream, const ChunkEdge& chunkEdge);

    CoordsToWindowEdge _impl;
};

////////////////////////////////////////////////////////////////////////////////
// Below are inline member functions of ChunkEdge
////////////////////////////////////////////////////////////////////////////////

inline bool ChunkEdge::operator!=(const ChunkEdge & other ) const
{
    return !((*this) == other);
}

inline CoordsToWindowEdge& ChunkEdge::operator()()
{
    return _impl;
}

inline const CoordsToWindowEdge& ChunkEdge::operator()() const
{
    return _impl;
}

} //namespace
