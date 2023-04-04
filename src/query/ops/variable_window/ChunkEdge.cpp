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

#include "ChunkEdge.h"

namespace scidb
{

bool ChunkEdge::operator == (const ChunkEdge & other ) const
{
    if (_impl.size() != other._impl.size() )
    {
        return false;
    }
    CoordsToWindowEdge::const_iterator iter = _impl.begin();
    CoordsToWindowEdge::const_iterator iter2;
    while(iter!=_impl.end())
    {
        Coordinates const& coords = iter->first;
        iter2 = other._impl.find(coords);
        if(iter2 == other._impl.end())
        {
            return false;
        }
        std::shared_ptr<WindowEdge> const& edge = iter->second;
        std::shared_ptr<WindowEdge> const& other_edge = iter2->second;
        if((edge.get() && !other_edge.get()) || (!edge.get() && other_edge.get()))
        {
            return false;
        }
        if( edge.get() && (*edge) != (*other_edge) )
        {
            return false;
        }
        iter++;
    }
    return true;
}

std::ostream& operator<<(std::ostream& stream, const ChunkEdge& chunkEdge)
{
    CoordsToWindowEdge::const_iterator iter = chunkEdge().begin();
    while (iter!= chunkEdge().end())
    {
        Coordinates const& coords = iter->first;
        std::shared_ptr<WindowEdge> const& edge = iter->second;
        stream<<CoordsToStr(coords)<<":"<<(*edge)<<"; ";
        iter++;
    }
    return stream;
}

} //namespace
