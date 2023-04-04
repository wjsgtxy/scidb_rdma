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

#include <query/Aggregate.h>

#include <deque>

namespace scidb
{

struct AggregatedValue
{
    AggregatedValue(Coordinate const& crd, InstanceID const& nid, size_t nAggs):
        coord(crd), instanceId(nid),  vals(nAggs)
    {}

    Coordinate coord;
    InstanceID instanceId;
    std::vector<Value> vals;
};

class WindowEdge
{
private:
    //Replace this with new object CBRLEPayload
    std::deque<Value> _values;
    std::deque<Coordinate> _valueCoords;
    std::deque<InstanceID> _instanceIDs;
    uint32_t _numFollowing;

public:
    WindowEdge(): _values(0), _valueCoords(0), _instanceIDs(0), _numFollowing(0)
    {}

    virtual ~WindowEdge()
    {}

    size_t getNumValues() const;
    size_t getNumCoords() const;
    void clearCoords();
    void addPreceding(Value const& v);
    void addCentral(Value const& v, Coordinate const& coord, InstanceID const& nid);
    void addFollowing(Value const& v);
    size_t getNumFollowing() const;
    size_t getNumFinalFollowing() const;
    void addLeftEdge(std::shared_ptr<WindowEdge> const& leftEdge);
    std::shared_ptr<WindowEdge> split(size_t nPreceding, size_t nFollowing);
    std::shared_ptr<AggregatedValue> churn (size_t numPreceding, size_t numFollowing, std::vector<AggregatePtr> const& aggs);
    void clear();

    //Marshalling scheme: [nCOORDS][nFollowing][COORDS][INSTANCEIDS][nVALS][VAL1SIZE][VAL1][-VAL2MC][VAL3SIZE][VAL3]...
    //Each value is preceded by VALSIZE or VALMC. If the value is negative - it is the missing code.
    size_t getBinarySize();

    Coordinate getNextCoord() const;
    char* marshall (char* buf);
    char const* unMarshall(char const* buf);
    bool operator == (const WindowEdge & other ) const;
    bool operator != (const WindowEdge & other ) const;
    friend std::ostream& operator<<(std::ostream& stream, const WindowEdge& edge);
};

////////////////////////////////////////////////////////////////////////////////
// Below are inline member functions of WindowEdge
////////////////////////////////////////////////////////////////////////////////

inline size_t WindowEdge::getNumValues() const
{
    return _values.size();
}

inline size_t WindowEdge::getNumCoords() const
{
    return _valueCoords.size();
}

inline void WindowEdge::clearCoords()
{
    _valueCoords.clear();
    _instanceIDs.clear();
}

inline void WindowEdge::addPreceding(Value const& v)
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    _values.push_back(v);
}

inline void WindowEdge::addCentral(Value const& v, Coordinate const& coord, InstanceID const& nid)
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    if(_valueCoords.size())
    {
        _numFollowing++;
    }
    _values.push_back(v);
    _valueCoords.push_back(coord);
    _instanceIDs.push_back(nid);
}

inline void WindowEdge::addFollowing(Value const& v)
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    _values.push_back(v);
    _numFollowing++;
}

inline size_t WindowEdge::getNumFollowing() const
{
    return _numFollowing;
}

inline size_t WindowEdge::getNumFinalFollowing() const
{
    if(_valueCoords.size())
    {
        return _numFollowing - _valueCoords.size() + 1;
    }
    else
    {
        //if there are no coords, there can't be any following
        assert(_numFollowing == 0);
        return 0;
    }
}

inline void WindowEdge::addLeftEdge(std::shared_ptr<WindowEdge> const& leftEdge)
{
    assert(leftEdge.get());
    if (_valueCoords.size())
    {
        _numFollowing += safe_static_cast<uint32_t>(leftEdge->_values.size());
    }
    else
    {
        _numFollowing = leftEdge->_numFollowing;
    }

    _values.insert(_values.end(), leftEdge->_values.begin(), leftEdge->_values.end());
    _valueCoords.insert(_valueCoords.end(), leftEdge->_valueCoords.begin(), leftEdge->_valueCoords.end());
    _instanceIDs.insert(_instanceIDs.end(), leftEdge->_instanceIDs.begin(), leftEdge->_instanceIDs.end());

}

inline void WindowEdge::clear()
{
    assert(_instanceIDs.size() == _valueCoords.size() && _values.size()>=_valueCoords.size());
    _values.clear();
    _valueCoords.clear();
    _instanceIDs.clear();
    _numFollowing=0;
}

inline Coordinate WindowEdge::getNextCoord() const
{
    assert(_valueCoords.size() > 0);
    return _valueCoords.front();
}

inline bool WindowEdge::operator != (const WindowEdge & other ) const
{
    return !((*this)==other);
}

inline std::ostream& operator<<(std::ostream& stream, const WindowEdge& edge)
{
    stream<<"{values "<<edge._values.size()<<" coords "<<edge._valueCoords.size()<<" nids "<<edge._instanceIDs.size()<<" following "<<edge._numFollowing<<"}";
    return stream;
}

} //namespace
