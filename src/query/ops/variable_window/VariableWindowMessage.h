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

typedef std::unordered_map<Coordinates, std::shared_ptr<ChunkEdge>, CoordinatesHash> Coords2ChunkEdge;
typedef std::unordered_map <Coordinates, std::vector<Value>, CoordinatesHash> Coords2ValueVector;
typedef std::unordered_map<Coordinates, std::shared_ptr<Coords2ValueVector>, CoordinatesHash> Coords2Coords2ValueVector;

class VariableWindowMessage
{
public:
    Coords2ChunkEdge _chunkEdges;
    Coords2Coords2ValueVector _computedValues;

    void addValues(Coordinates const& chunkPos, Coordinates const& valuePos, std::vector<Value> const& v);
    friend std::ostream& operator<<(std::ostream& stream, const VariableWindowMessage& message);
    bool hasData() const;
    void clear();

    //[nChunkEdges][edgeCoords1][nWindowEdges][windowEdgeCoords1][windowEdge1]..[edgeCoords2]..
    //[nValueChunks][valueChunkCoords1][nValues][valueCoords1][value1]...
    size_t getBinarySize(size_t nDims, size_t nAggs) const;

    bool operator == (const VariableWindowMessage & other) const;
    char* marshall (size_t nDims, size_t nAggs, char* buf) const;
    const char* unMarshall (const char* data, size_t nDims, size_t nAggs);
};

////////////////////////////////////////////////////////////////////////////////
// Below are inline member functions of VariableWindowMessage.h
////////////////////////////////////////////////////////////////////////////////

inline bool VariableWindowMessage::hasData() const
{
    return _chunkEdges.size() > 0 || _computedValues.size() > 0;
}

inline void VariableWindowMessage::clear()
{
    _chunkEdges.clear();
    _computedValues.clear();
}

} //namespace
