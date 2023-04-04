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

#include "VariableWindowMessage.h"

namespace scidb
{

void VariableWindowMessage::addValues(Coordinates const& chunkPos, Coordinates const& valuePos, std::vector<Value> const& v)
{
    std::shared_ptr< Coords2ValueVector >& mp = _computedValues[chunkPos];
    if(mp.get()==0)
    {
        mp.reset(new Coords2ValueVector());
    }
    assert(mp->count(valuePos)==0);
    (*mp)[valuePos] = v;
}

std::ostream& operator<<(std::ostream& stream, const VariableWindowMessage& message)
{
    stream<<"Chunk Edges: "<<message._chunkEdges.size()<<"\n";
    Coords2ChunkEdge::const_iterator iter = message._chunkEdges.begin();
    while(iter!= message._chunkEdges.end())
    {
        Coordinates const& coords = iter->first;
        std::shared_ptr<ChunkEdge> const& edge = iter->second;
        stream<<"   "<<CoordsToStr(coords)<<": "<<(*edge)<<"\n";
        iter++;
    }
    stream<<"Computed Value Chunks: "<<message._computedValues.size()<<"\n";
    Coords2Coords2ValueVector::const_iterator iter2 = message._computedValues.begin();
    while(iter2 != message._computedValues.end())
    {
        Coordinates const& coords = iter2->first;
        stream<<"   "<<CoordsToStr(coords)<<": ";
        std::shared_ptr<Coords2ValueVector> const& valChunk = iter2->second;
        Coords2ValueVector::const_iterator iter3 = valChunk->begin();
        while(iter3!=valChunk->end())
        {
            Coordinates const& coords2 = iter3->first;
            std::vector<Value> const& vals = iter3->second;
            stream<<CoordsToStr(coords2)<<":{";
            for(size_t j=0; j<vals.size(); j++)
            {
                stream<<vals[j].size()<<","<<vals[j].getMissingReason()<<" ";
            }
            stream<<"}; ";
            iter3++;
        }
        stream<<"\n";
        iter2++;
    }
    return stream;
}

size_t VariableWindowMessage::getBinarySize(size_t nDims, size_t nAggs) const
{
    //nChunkEdges, nValueChunks
    size_t totalSize = 2*sizeof(size_t);
    Coords2ChunkEdge::const_iterator iter = _chunkEdges.begin();
    while(iter!=_chunkEdges.end())
    {
        //chunk edge coordinates + nWindowEdges
        totalSize+= nDims*sizeof(Coordinate) + sizeof(size_t);
        std::shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
        CoordsToWindowEdge::iterator innerIter = (*chunkEdge)().begin();
        while(innerIter!=(*chunkEdge)().end())
        {
            //window edge coordinates
            totalSize+= nDims*sizeof(Coordinate);
            std::shared_ptr<WindowEdge> const& windowEdge = innerIter->second;
            //window edge size
            totalSize+= windowEdge->getBinarySize();
            innerIter++;
        }
        iter++;
    }
    Coords2Coords2ValueVector::const_iterator iter2 = _computedValues.begin();
    while(iter2!=_computedValues.end())
    {
        //value chunk coordinates + nValues
        totalSize+= nDims*sizeof(Coordinate)+ sizeof(size_t);

        std::shared_ptr <Coords2ValueVector>const& innerMap = iter2->second;
        Coords2ValueVector::iterator innerIter = innerMap->begin();
        while(innerIter != innerMap->end())
        {
            //valueCoords + VALSIZE or VALMC
            std::vector<Value> const& v = innerIter->second;
            assert(v.size() == nAggs);
            totalSize += nDims*sizeof(Coordinate) + sizeof(int64_t)*nAggs;
            for(size_t i =0; i<nAggs; i++)
            {
                if (!v[i].isNull())
                {
                    totalSize+=v[i].size();
                }
            }
            innerIter++;
        }
        iter2++;
    }
    return totalSize;
}

bool VariableWindowMessage::operator == (const VariableWindowMessage & other) const
{
    if (_chunkEdges.size() != other._chunkEdges.size() || _computedValues.size() != other._computedValues.size())
    {
        return false;
    }
    Coords2ChunkEdge::const_iterator iter = _chunkEdges.begin();
    Coords2ChunkEdge::const_iterator oiter;
    while(iter!=_chunkEdges.end())
    {
        Coordinates const& chunkCoords = iter->first;
        oiter = other._chunkEdges.find(chunkCoords);
        if(oiter == other._chunkEdges.end())
        {
            return false;
        }
        std::shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
        std::shared_ptr<ChunkEdge> const& oChunkEdge = oiter->second;
        if ((chunkEdge.get() && !oChunkEdge.get()) || (!chunkEdge.get() && oChunkEdge.get()))
        {
            return false;
        }
        if( chunkEdge.get() && (*chunkEdge) != (*oChunkEdge) )
        {
            return false;
        }
        iter++;
    }
    Coords2Coords2ValueVector::const_iterator iter2 = _computedValues.begin();
    Coords2Coords2ValueVector::const_iterator oiter2;
    while(iter2 != _computedValues.end())
    {
        Coordinates const& coords = iter2->first;
        oiter2 = other._computedValues.find(coords);
        if(oiter2 == other._computedValues.end())
        {
            return false;
        }
        std::shared_ptr<Coords2ValueVector> const& valMap = iter2->second;
        std::shared_ptr< Coords2ValueVector> const& oValMap = oiter2->second;
        if ((valMap.get() && !oValMap.get()) || (!valMap.get() && oValMap.get()) )
        {
            return false;
        }
        if (valMap.get())
        {
            Coords2ValueVector::const_iterator iter3 = valMap->begin();
            Coords2ValueVector::const_iterator oiter3;
            while(iter3!=valMap->end())
            {
                Coordinates const& coords = iter3->first;
                oiter3 = oValMap->find(coords);
                if(oiter3 == oValMap->end())
                {
                    return false;
                }

                std::vector<Value> const& v = iter3->second;
                std::vector<Value> const& v2 = oiter3->second;
                if(v.size()!=v2.size())
                {
                    return false;
                }

                for(size_t i=0; i<v.size(); i++)
                {
                    if(v[i].isNull() != v2[i].isNull() || v[i].size() != v2[i].size())
                    {
                        return false;
                    }
                    else if (v[i].isNull() && v[i].getMissingReason()!=v2[i].getMissingReason())
                    {
                        return false;
                    }
                    else if(memcmp(v[i].data(), v2[i].data(), v[i].size())!=0)
                    {
                        return false;
                    }
                }
                iter3++;
            }
        }
        iter2++;
    }
    return true;
}

char* VariableWindowMessage::marshall (size_t nDims, size_t nAggs, char* buf) const
{
    size_t* sizePtr = (size_t*) buf;
    *sizePtr = _chunkEdges.size();
    Coordinate* coordPtr = (Coordinate*) (sizePtr+1);

    Coords2ChunkEdge::const_iterator iter = _chunkEdges.begin();
    while(iter!=_chunkEdges.end())
    {
        Coordinates const& chunkCoords = iter->first;
        assert(chunkCoords.size() == nDims);
        for(size_t i=0; i<nDims; i++)
        {
            *coordPtr = chunkCoords[i];
            coordPtr++;
        }

        std::shared_ptr<ChunkEdge> const& chunkEdge = iter->second;
        size_t *edgeSizePtr = (size_t*) coordPtr;
        *edgeSizePtr = (*chunkEdge)().size();
        coordPtr = (Coordinate*) (edgeSizePtr+1);
        CoordsToWindowEdge::const_iterator innerIter = (*chunkEdge)().begin();
        while(innerIter!=(*chunkEdge)().end())
        {
            Coordinates const& edgeCoords = innerIter->first;
            assert(edgeCoords.size() == nDims);
            for(size_t i=0; i<nDims; i++)
            {
                *coordPtr = edgeCoords[i];
                coordPtr++;
            }
            std::shared_ptr<WindowEdge>const& windowEdge = innerIter->second;
            coordPtr = (Coordinate*) windowEdge->marshall((char*) coordPtr);
            innerIter++;
        }
        iter++;
    }

    size_t* valuesCountPtr = (size_t*) coordPtr;
    *valuesCountPtr = _computedValues.size();
    coordPtr = (Coordinate*) (valuesCountPtr+1);
    Coords2Coords2ValueVector::const_iterator iter2 = _computedValues.begin();
    while(iter2!=_computedValues.end())
    {
        Coordinates const& chunkCoords = iter2->first;
        assert(chunkCoords.size()==nDims);
        for(size_t i=0; i<nDims; i++)
        {
            *coordPtr = chunkCoords[i];
            coordPtr++;
        }
        size_t* nValuesPtr = (size_t*) coordPtr;
        std::shared_ptr <Coords2ValueVector> const& innerMap = iter2->second;
        *nValuesPtr = innerMap->size();
        coordPtr = (Coordinate*) (nValuesPtr+1);
        Coords2ValueVector::const_iterator innerIter = innerMap->begin();
        while(innerIter != innerMap->end())
        {
            Coordinates const& coords = innerIter->first;
            assert(coords.size()==nDims);
            for(size_t i=0; i<nDims; i++)
            {
                *coordPtr = coords[i];
                coordPtr++;
            }
            std::vector<Value> const& v = innerIter->second;
            assert(v.size() == nAggs);

            for(size_t i =0; i<nAggs; i++)
            {
                int64_t* sizePtr = (int64_t*) coordPtr;
                if(v[i].isNull())
                {
                    *sizePtr = v[i].getMissingReason() * (-1);
                    coordPtr = (Coordinate*) (sizePtr+1);
                }
                else
                {
                    int64_t vsize = v[i].size();
                    *sizePtr = vsize;
                    sizePtr++;
                    char* dataPtr = (char*) sizePtr;
                    memcpy(dataPtr, v[i].data(), vsize);
                    coordPtr = (Coordinate*) (dataPtr+vsize);
                }
            }
            innerIter++;
        }
        iter2++;
    }
    char* result = (char*) coordPtr;
    assert( (size_t) (result - buf) == getBinarySize(nDims, nAggs) );
    return result;
}

const char* VariableWindowMessage::unMarshall (const char* data, size_t nDims, size_t nAggs)
{
    auto numEdgesPtr = reinterpret_cast<const size_t*>(data);
    size_t numEdges = *numEdgesPtr;
    auto coordPtr = reinterpret_cast<const Coordinate*>(numEdgesPtr+1);
    for(size_t i=0; i<numEdges; i++)
    {
        Coordinates chunkCoords(nDims);
        for(size_t j=0; j<nDims; j++)
        {
            chunkCoords[j]=*coordPtr;
            coordPtr++;
        }

        std::shared_ptr<ChunkEdge> &rce = _chunkEdges[chunkCoords];
        if(rce.get()==0)
        {
            rce.reset(new ChunkEdge());
        }
        auto numWindowEdgesPtr = reinterpret_cast<const size_t*>(coordPtr);
        size_t numWindowEdges = *numWindowEdgesPtr;
        coordPtr = reinterpret_cast<const Coordinate*>(numWindowEdgesPtr+1);
        for(size_t j=0; j<numWindowEdges; j++)
        {
            Coordinates windowEdgeCoords(nDims);
            for(size_t k=0; k<nDims; k++)
            {
                windowEdgeCoords[k]=*coordPtr;
                coordPtr++;
            }
            std::shared_ptr<WindowEdge> rwe(new WindowEdge());
            coordPtr = (Coordinate*) rwe->unMarshall(reinterpret_cast<const char*>(coordPtr));
            (*rce)()[windowEdgeCoords] = rwe;
        }
    }
    auto numValueChunksPtr = reinterpret_cast<const size_t*>(coordPtr);
    size_t numValueChunks = *numValueChunksPtr;
    coordPtr = reinterpret_cast<const Coordinate*>(numValueChunksPtr+1);
    for(size_t i=0; i<numValueChunks; i++)
    {
        Coordinates chunkCoords(nDims);
        for (size_t j=0; j<nDims; j++)
        {
            chunkCoords[j]=*coordPtr;
            coordPtr++;
        }
        std::shared_ptr<Coords2ValueVector>& valueChunk = _computedValues[chunkCoords];
        if(valueChunk.get()==0)
        {
            valueChunk.reset(new Coords2ValueVector());
        }
        auto numValuesPtr = reinterpret_cast<const size_t*>(coordPtr);
        size_t numValues = *numValuesPtr;
        coordPtr = reinterpret_cast<const Coordinate*>(numValuesPtr+1);
        for(size_t j=0; j<numValues; j++)
        {
            Coordinates valueCoords(nDims);
            for (size_t k=0; k<nDims; k++)
            {
                valueCoords[k] = *coordPtr;
                coordPtr ++;
            }

            for(size_t k=0; k<nAggs; k++)
            {
                Value val;
                auto sizeOrNullPtr = reinterpret_cast<const int64_t*>(coordPtr);
                int64_t valSize = *sizeOrNullPtr;
                if(valSize <= 0)
                {
                    val.setNull(safe_static_cast<Value::reason>(valSize*-1));
                    coordPtr = reinterpret_cast<const Coordinate*>(sizeOrNullPtr+1);
                }
                else
                {
                    char* dataPtr=(char*) (sizeOrNullPtr+1);
                    val.setData(dataPtr, valSize);
                    coordPtr = reinterpret_cast<const Coordinate*>(dataPtr+valSize);
                }
                (*valueChunk)[valueCoords].push_back(val);
            }
        }
        _computedValues[chunkCoords] = valueChunk;
    }
    auto ret = reinterpret_cast<const char*>(coordPtr);
    return ret;
}

} //namespace
