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
#ifndef ARRAY_RESIDENCY_H_
#define ARRAY_RESIDENCY_H_

#include <iostream>
#include <memory>
#include <vector>

#include <array/ArrayDistributionInterface.h>
#include <util/iqsort.h>

/**
 * @file ArrayResidency.h
 *
 * ArrayResidency implementaions.
 *
 * Array residency is a set of physical SciDB instances to which an array is distributed.
 * Different arrays can have different residencies. In addition to the array distribution,
 * the optimizer uses the array residency information to correctly (or more efficiently)
 * distribute array data for computation.
 */
namespace scidb
{

/// Array residency in which the set of physical instance IDs
/// is the same as the same as the set of their ranks.
class ConsecutiveArrayResidency : public ArrayResidency, public boost::noncopyable
{
 public:

    ConsecutiveArrayResidency(size_t nInstances)
    : _nInstances(nInstances)
    {
        ASSERT_EXCEPTION(nInstances>0,
                         "ConsecutiveArrayResidency: invalid number of instances");
    }

    virtual ~ConsecutiveArrayResidency() {}

    virtual InstanceID getPhysicalInstanceAt(InstanceID logicalInstanceId) const
    {
        if (logicalInstanceId < _nInstances) {
            return logicalInstanceId;
        }
        ASSERT_EXCEPTION_FALSE("ConsecutiveArrayResidency: invalid logicalInstanceId");
    }

    virtual InstanceID getIndexForPhysicalId(InstanceID physId) const
    {
        if (physId < _nInstances) {
            return physId;
        }
        ASSERT_EXCEPTION_FALSE("ConsecutiveArrayResidency: invalid physialInstanceId");
    }

    virtual size_t size() const
    {
        return _nInstances;
    }

    bool operator==(const ConsecutiveArrayResidency& other) const
    {
        return (_nInstances==other._nInstances);
    }

private:
    size_t _nInstances;
};

/// Array residency in which the physical ID is looked up with its rank
class MapArrayResidency : public ArrayResidency, public boost::noncopyable
{
private:

    void validateInstances()
    {
        ASSERT_EXCEPTION(_instances.size()>0,
                         "MappedArrayResidency: no instance found");
        for (size_t i=1; i < _instances.size(); ++i) {
            if ( !(_instances[i-1] < _instances[i]) ) {
                ASSERT_EXCEPTION_FALSE("MappedArrayResidency: instance IDs must be ordered");
            }
        }
    }

public:

    template<class InputIterator>
    MapArrayResidency(InputIterator begin, InputIterator end)
    : _instances(begin,end)
    {
        validateInstances();
    }

    /// Constructor
    /// @param[in,out] sortedInstances a vector of physical instance IDs,
    ///                which must be sorted in ascending order.
    ///                sortedInstances is empty on return
    MapArrayResidency(std::vector<InstanceID>& sortedInstances)
    {
        _instances.swap(sortedInstances);
        validateInstances();
    }

    virtual ~MapArrayResidency() {}

    virtual InstanceID getPhysicalInstanceAt(InstanceID logicalInstanceId) const
    {
        if (logicalInstanceId < _instances.size()) {
            return _instances[logicalInstanceId];
        }
        ASSERT_EXCEPTION_FALSE("MappedArrayResidency: invalid logicalInstanceId");
    }

    virtual InstanceID getIndexForPhysicalId(InstanceID physId) const
    {
        size_t index=0;
        // _instances must be sorted
        bool found = bsearch(_instances, physId, index);
        ASSERT_EXCEPTION(found, "MappedArrayResidency: invalid physicalInstanceId");
        return index;
    }

    virtual size_t size() const
    {
        return _instances.size();
    }

    bool operator==(const MapArrayResidency& other) const
    {
        return (_instances==other._instances);
    }

private:
    mutable std::vector<InstanceID> _instances;
};

} // namespace scidb

#endif // ARRAY_RESIDENCY_H_
