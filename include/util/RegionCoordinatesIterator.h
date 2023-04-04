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
 * RegionCoordinatesIterator.h
 *
 *  Created on: Jun 15, 2012
 *      Author: dzhang
 *  Iterators over all logical chunkPos in a multi-dim region.
 *
 *********************************************************************************************************************
 *  THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION
 *  - The request:
 *    (a) All tools that iterate over the logical chunk/cell space,
 *        such as RegionCoordinatesIterator, should be accompanied with
 *        a warning, and a pointer to this note.
 *        E.g. see the comment for class RegionCoordinatesIterator.
 *    (b) All usages of such tools should be accompanied with a comment
 *        describing why it is ok to iterate over the logical space.
 *  - Requester: Donghui Zhang, 12/8/2014.
 *  - Rationale:
 *    Iterating over the logical space is dangerous. Say you have an extremely sparse array with only a few chunks, but
 *    with billions of logical chunks in the space. If you use a tool to iterate over the logical space, replying on some
 *    kind of probing to decide whether each chunkPos is valid, your algorithm may end up taking too long to run.
 *********************************************************************************************************************
 */

#ifndef REGIONCOORDINATESITERATOR_H_
#define REGIONCOORDINATESITERATOR_H_

#include <array/Array.h>

#include <ostream>
#include <vector>

namespace scidb
{

/**
 * RegionCoordinatesIteratorParam: parameters to the constructor of RegionCoordinatesIterator.
 */
struct RegionCoordinatesIteratorParam
{
    Coordinates _low;
    Coordinates _high;
    std::vector<size_t> _intervals;

    RegionCoordinatesIteratorParam(size_t size): _low(size), _high(size), _intervals(size)
    {}
};

/**
 * RegionCoordinatesIterator iterates over all the chunk-start coordinates, in a region described by a pair of Coordinates.
 *
 *  @note Use with caution! This class iterates over the logical space.
 *  @see THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION in RegionCoordinatesIterator.h.
 */
class RegionCoordinatesIterator: public ConstIterator
{
public:
    /**
     * The caller should make sure low, high, and intervals remain valid throughout the life time of this object.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high, const std::vector<size_t>& intervals);

    /**
     * The caller should make sure param remain valid throughout the life time of this object.
     */
    RegionCoordinatesIterator(const RegionCoordinatesIteratorParam& param);

    /**
     * The caller should make sure low, and high remain valid throughout the life time of this object.
     * By default, every interval is 1.
     */
    RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high);

    /**
     * Check if end is reached
     * @return true if iterator reaches the end of the region
     */
    bool end() override
    {
        return _current > _high;
    }

    /**
     * Position cursor to the next chunk.
     */
    void operator ++() override;

    /**
     * Advance to the smallest position >= a given newPos.
     * @param newPos  the position to reach or exceed.
     * @return whether any advancement is made.
     */
    bool advanceToAtLeast(Coordinates const& newPos);

    /**
     * Get coordinates of the current element.
     */
    Coordinates const& getPosition() override
    {
        return _current;
    }

    /**
     * Set iterator's current positions
     * @return true if specified position is valid (in the region),
     * false otherwise
     * @note the pos MUST be a chunk start.
     */
    bool setPosition(Coordinates const& pos) override;

    /**
     * Move iterator to the first coordinates.
     */
    void restart() override
    {
        _current = _low;
    }

private:
    /**
     * Whether _current is inside the box specified by _low and _high.
     */
    bool inBox();

    ///Check the consistency of input parameters.
    void checkConsistency();

    /// low coordinates of the region.
    Coordinates const& _low;

    /// high coordinates of the region.
    Coordinates const& _high;

    /// a pointer to a passed-in vector of chunk intervals, or nullptr representing all-one intervals.
    std::vector<size_t> const*const _intervals;

    /// The current coordinates.
    Coordinates _current;

    friend std::ostream& operator<<(std::ostream& os,const RegionCoordinatesIterator& rci);
};

//----------------------------------------------------------------------------//

inline RegionCoordinatesIterator::RegionCoordinatesIterator(
    const Coordinates& low, const Coordinates& high, const std::vector<size_t>& intervals)
    : _low(low), _high(high), _intervals(&intervals), _current(low)
{
    checkConsistency();
}

inline RegionCoordinatesIterator::RegionCoordinatesIterator(const RegionCoordinatesIteratorParam& param)
    : _low(param._low), _high(param._high), _intervals(&param._intervals), _current(param._low)
{
    checkConsistency();
}

inline RegionCoordinatesIterator::RegionCoordinatesIterator(const Coordinates& low, const Coordinates& high)
    : _low(low), _high(high), _intervals(nullptr), _current(low)
{
    checkConsistency();
}

inline void RegionCoordinatesIterator::checkConsistency()
{
    assert(_low.size()==_high.size());
    assert((_intervals == nullptr) || _intervals->size()==_high.size());
    assert(_low.size()>0);
    for (size_t i=0; i<_low.size(); ++i) {
        assert(_low[i] <= _high[i]);
    }
}

inline void RegionCoordinatesIterator::operator++()
{
    const bool useDefaultIntervals = (_intervals == nullptr);
    for (size_t i=_current.size()-1; i>=1; --i) {
        _current[i] += ( useDefaultIntervals ? 1 : (*_intervals)[i]);
        if (_current[i] <= _high[i]) {
            return;
        }
        _current[i] = _low[i];
    }
    _current[0] += ((_intervals == nullptr) ? 1 : (*_intervals)[0]);
}

inline bool RegionCoordinatesIterator::advanceToAtLeast(Coordinates const& newPos)
{
    if (_current >= newPos) {
        return false;
    }
    _current = newPos;

    if (end() || inBox()) {
        return true;
    }

    // I will scan all dimension from nDims-1 downto 1.
    // For each dimension, I will make sure current[i] is between low[i] and high[i].
    // If I have to increase current[i], not a problem: the new position is "advancing".
    // But if I have to decrease current[i], there is a "carryover" in that some later (i.e. smaller) dimension has to increase.
    // I'll use this variable to indicate whether there is a carryover.
    bool needToInc = false;
    const bool useDefaultIntervals = (_intervals == nullptr);

    for (size_t i=_current.size()-1; i>=1; --i) {
        if (_current[i] < _low[i]) {
            // No action needed to satisfy needToInc from before, because we have to increase dim i anyways.

            // Make sure the i'th dim is inside low[i], high[i]:
            _current[i] = _low[i];

            // No request from me to increase later dimensions.
            needToInc = false;
        }
        else if (_current[i] > _high[i]) {
            // We have to reduce _current[i], and increase some later dimension, whatsoever.
            // So we should directly reduce to _low[i].
            _current[i] = _low[i];
            needToInc = true;
        }
        else {
            // Good, this dimension is in range.

            // Try to deal with earlier needToInc.
            if (needToInc) {
                if (_current[i] + static_cast<Coordinate>(useDefaultIntervals ? 1 : (*_intervals)[i]) <= _high[i]) {
                    _current[i] = _current[i] + (useDefaultIntervals ? 1 : (*_intervals)[i]);
                    needToInc = false;
                }
            }

            // If I cannot handle previous needToInc, reduce to low[i] and let later dimensions deal with it.
            if (needToInc) {
                _current[i] = _low[i];
            }

            // There is no problem with previous needToInc now.
            else {
                // Shortcut! If inBox(), done with advancing!
                if (inBox()) {
                    return true;
                }

                // Not inBox() means some future dimensions will need to increase.
                // So I'll try to set myself to _low[i].
                if (_current[i] > _low[i]) {
                    _current[i] = _low[i];
                    needToInc = true;
                }
            }
        }
    } // end for

    // Now is the time to process dimension 0.
    if (needToInc) {
        _current[0] += (useDefaultIntervals ? 1 : (*_intervals)[0]);
    }
    if (_current[0] < _low[0]) {
        _current[0] = _low[0];
    }

    return true;
}

inline bool RegionCoordinatesIterator::setPosition(Coordinates const& pos)
{
    assert(pos.size()==_current.size());
    const bool useDefaultIntervals = (_intervals == nullptr);
    for (size_t i=0, n=pos.size(); i<n; ++i) {
        // Out of bound?
        if (pos[i]<_low[i] || pos[i]>_high[i]) {
            return false;
        }

        // Not the first cell in a chunk?
        if ((pos[i]-_low[i]) % (useDefaultIntervals ? 1 : (*_intervals)[i]) != 0) {
           assert(false);
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "RegionCoordinatesIterator::setPosition";
        }
    }
    _current = pos;
    return true;
}

inline bool RegionCoordinatesIterator::inBox()
{
    for (size_t i=0, n = _current.size(); i<n; ++i) {
        if (_current[i] < _low[i] || _current[i] > _high[i]) {
            return false;
        }
    }
    return true;
}

inline std::ostream& operator<<(std::ostream& os,const RegionCoordinatesIterator& rci) {
    os << "RegionCoordinatesIterator::_low: " << rci._low  << std::endl; 
    os << "RegionCoordinatesIterator::_high: " << rci._high  << std::endl; 

    if (rci._intervals) {
        auto intervals = *(rci._intervals);
        os << "RegionCoordinatesIterator::_intervals: {" ;
        for(size_t i=0; i<intervals.size(); i++) {
            os << intervals[i] << ( i==(intervals.size()-1) ? "}" : ",");
        }
    } else {
        os << "RegionCoordinatesIterator::_intervals: null";
    }
    os << std::endl;

    os << "RegionCoordinatesIterator::_current: " << rci._current  << std::endl; 
    return os;
}

}  // namespace scidb

#endif /* REGIONCOORDINATESITERATOR_H_ */
