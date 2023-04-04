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

std::shared_ptr<WindowEdge> WindowEdge::split(size_t nPreceding, size_t nFollowing)
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size() && _values.size() == nPreceding + nFollowing);

    std::shared_ptr<WindowEdge> newEdge ( new WindowEdge());
    newEdge->_values.assign(_values.begin(), _values.end());
    newEdge->_valueCoords.assign(_valueCoords.begin() + nPreceding, _valueCoords.end());
    newEdge->_instanceIDs.assign(_instanceIDs.begin() + nPreceding, _instanceIDs.end());
    if(newEdge->_valueCoords.size())
    {
        newEdge->_numFollowing = safe_static_cast<uint32_t>(newEdge->_valueCoords.size() -1);
    }
    else
    {
        newEdge->_numFollowing =0;
    }

    _valueCoords.erase(_valueCoords.begin() + nPreceding, _valueCoords.end());
    _instanceIDs.erase(_instanceIDs.begin() + nPreceding, _instanceIDs.end());
    if(_valueCoords.size())
    {
        _numFollowing = safe_static_cast<uint32_t>(_values.size()-1);
    }
    else
    {
        _numFollowing = safe_static_cast<uint32_t>(_values.size());
    }
    return newEdge;
}

std::shared_ptr<AggregatedValue> WindowEdge::churn (size_t numPreceding, size_t numFollowing, std::vector<AggregatePtr> const& aggs)
{
    assert(_instanceIDs.size() > 0 && _instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    if(_instanceIDs.size() == 0 || _valueCoords.size()==0 || _values.size() == 0 || _instanceIDs.size() != _valueCoords.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Incorrect churn call";
    }

    std::shared_ptr<AggregatedValue> result (new AggregatedValue(_valueCoords.front(), _instanceIDs.front(), aggs.size()));
    size_t currentPreceding = (size_t) std::max<int64_t>( (int64_t) (_values.size() - _numFollowing - 1), 0);
    assert(currentPreceding <= numPreceding); //yes; otherwise the result won't be centered around the right coordinate
    size_t windowSize = currentPreceding + std::min<size_t>((size_t)_numFollowing, numFollowing) + 1;
    assert(windowSize <= _values.size());

//TODO: make this whole class an extension of ConstRLEPayload where you can add to back and remove from front.
//        RLEPayload::append_iterator iter(bitSize);
//        for (size_t i=0; i<windowSize; i++)
//        {
//            iter.add(_values[i]);
//        }
//        iter.flush();
//RIDICULOUS:
//        std::unique_ptr<RLEPayload>payload(iter.getPayload());
//        agg->accumulateIfNeeded(state, payload.get());
//        payload.reset();


    for(size_t h =0; h<aggs.size(); h++)
    {
        Value state(aggs[h]->getStateType());
        aggs[h]->initializeState(state);
        for (size_t i=0; i<windowSize; i++)
        {
            aggs[h]->accumulateIfNeeded(state, _values[i]);
        }
        aggs[h]->finalResult(result->vals[h], state);
    }

    _valueCoords.pop_front();
    _instanceIDs.pop_front();
    if(_values.size() - _numFollowing > numPreceding)
    {
        _values.pop_front();
    }
    if(_numFollowing>0)
    {
        _numFollowing --;
    }

    return result;
}

//Marshalling scheme: [nCOORDS][nFollowing][COORDS][INSTANCEIDS][nVALS][VAL1SIZE][VAL1][-VAL2MC][VAL3SIZE][VAL3]...
//Each value is preceded by VALSIZE or VALMC. If the value is negative - it is the missing code.
size_t WindowEdge::getBinarySize()
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());

    //for nCOORDS, nVALS, numFollowing
    size_t result = 3 * sizeof(size_t);

    if(_valueCoords.size())
    {
        result += (sizeof(InstanceID) + sizeof(Coordinate)) * _valueCoords.size();
    }
    if(_values.size())
    {
        for(size_t i =0; i<_values.size(); i++)
        {
            //for VALSIZE or VALMC
            result += sizeof(int64_t);
            if(_values[i].isNull()==false)
            {
                result += _values[i].size();
            }
        }
    }
    return result;
}

char* WindowEdge::marshall (char* buf)
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    size_t* numCoords = (size_t*) buf;
    *numCoords=_valueCoords.size();
    numCoords++;
    *numCoords=_numFollowing;
    numCoords++;
    Coordinate* coordPtr = (Coordinate*) numCoords;
    for(size_t i=0; i<_valueCoords.size(); i++)
    {
        *coordPtr = _valueCoords[i];
        coordPtr++;
    }
    InstanceID* instancePtr = (InstanceID*) coordPtr;
    for(size_t i=0; i<_instanceIDs.size(); i++)
    {
        *instancePtr = _instanceIDs[i];
        instancePtr++;
    }
    size_t* numVals = (size_t*) instancePtr;
    *numVals = _values.size();
    numVals++;
    int64_t* valSizePtr = (int64_t*) numVals;
    for (size_t i=0; i<_values.size(); i++)
    {
        if (_values[i].isNull())
        {
            *valSizePtr = _values[i].getMissingReason() * -1;
            valSizePtr++;
        }
        else
        {
            size_t size = _values[i].size();
            *valSizePtr = size;
            char* data = (char*) (valSizePtr+1);
            memcpy(data, _values[i].data(), size);
            valSizePtr = (int64_t*) (data+size);
        }
    }
    return (char*) valSizePtr;
}

char const* WindowEdge::unMarshall(char const* buf)
{
    size_t* numCoordsPtr = (size_t*) buf;
    size_t numCoords = *numCoordsPtr;
    numCoordsPtr++;
    _numFollowing += safe_static_cast<uint32_t>(*numCoordsPtr);
    Coordinate* coordPtr = (Coordinate*)(numCoordsPtr+1);
    for(size_t i=0; i<numCoords; i++)
    {
        Coordinate coord = *coordPtr;
        _valueCoords.push_back(coord);
        coordPtr++;
    }
    InstanceID* nPtr = (InstanceID*) coordPtr;
    for(size_t i=0; i<numCoords; i++)
    {
        InstanceID nid = *nPtr;
        _instanceIDs.push_back(nid);
        nPtr++;
    }
    size_t* numValsPtr = (size_t*) nPtr;
    size_t numVals = *numValsPtr;
    int64_t* valSizePtr = (int64_t*)(numValsPtr+1);
    for(size_t i=0; i<numVals; i++)
    {
        int64_t valSize = *valSizePtr;
        Value val;
        if(valSize<=0)
        {
            val.setNull(safe_static_cast<Value::reason>(valSize*-1));
            valSizePtr++;
        }
        else
        {
            char* dataPtr=(char*) (valSizePtr+1);
            val.setData(dataPtr, valSize);
            valSizePtr = (int64_t*) (dataPtr+valSize);
        }
        _values.push_back(val);
    }
    return (char*) (valSizePtr);
}

bool WindowEdge::operator == (const WindowEdge & other ) const
{
    if (_values.size() != other._values.size() ||
        _valueCoords.size() != other._valueCoords.size() ||
        _instanceIDs.size() != other._instanceIDs.size() ||
        _numFollowing != other._numFollowing)
    {
        return false;
    }
    for(size_t i=0; i<_values.size(); i++)
    {
        Value const& v = _values[i];
        Value const& v2 = other._values[i];
        if(v.isNull() != v2.isNull() || v.size() != v2.size())
        {
            return false;
        }
        else if (v.isNull() && v.getMissingReason()!=v2.getMissingReason())
        {
            return false;
        }
        else if(memcmp(v.data(), v2.data(), v.size())!=0)
        {
            return false;
        }
    }
    for (size_t i=0; i<_instanceIDs.size(); i++)
    {
        if(_instanceIDs[i]!=other._instanceIDs[i])
        {
            return false;
        }
    }
    for(size_t i=0; i<_valueCoords.size(); i++)
    {
        if(_valueCoords[i]!=other._valueCoords[i])
        {
            return false;
        }
    }
    return true;
}

} //namespace
