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
 * @file OperatorID.h
 *
 * @brief Operator ID
 */

#ifndef OPERATOR_ID_H_
#define OPERATOR_ID_H_

#include <functional>
#include <iostream>
#include <limits>

#include <boost/serialization/serialization.hpp>

namespace scidb
{

/**
 * Operator Identifier class used to identify the execution context of a scatter-gather or other operator-scoped operation
 */
class OperatorID
{
public:
    ~OperatorID() = default;

    //
    // ctor, to "uninitialized" value
    //
    OperatorID ()
    :   _id(std::numeric_limits<uint64_t>::max())
    {}

    //
    // ctor to a specific value
    // @param id A sequential numbering of the operators
    //     in a physical plan so that they can be looked
    //     up from messages, much like QueryIDs can
    //
    explicit OperatorID (uint64_t id)
    :   _id(id)
    {}

    // copy ctor
    OperatorID(const OperatorID& other) = default;

    // assignment
    OperatorID& operator=(const OperatorID& other) = default;

    // relationals
    bool operator==(const OperatorID& other) const
    {
        return (_id == other._id);
    }

    bool operator!=(const OperatorID& other) const
    {
        return !(*this==other);
    }

    bool operator<(const OperatorID& other) const
    {
        return (_id < other._id);
    }

    uint64_t getValue() const
    {
        return _id;
    }

    bool isValid() const
    {
        return _id !=  std::numeric_limits<uint64_t>::max() ;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _id;
    }

private:
    uint64_t _id; /// Unique integer, assigned by the PhysicalPlanner and optimizer

};

inline std::ostream& operator<<(std::ostream& os, const OperatorID& oId)
{
    os << oId.getValue() ;
    return os;
}

} // scidb namespace

#endif /* OPERATOR_ID_H_ */
