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
 * @file QueryID.h
 */

#ifndef QUERY_ID_H_
#define QUERY_ID_H_

#include <iostream>
#include <functional>
#include <boost/functional/hash.hpp>
#include <query/InstanceID.h>

namespace scidb
{

/**
 * Query identifier structure used to identify the execution context of a user query.
 * TODO: consider moving QueryID creation logic from the Query class to this class.
 */
class QueryID
{
private:
    /// Physical instance ID of the coordinator
    InstanceID _coordinatorId;
    /// Unique (per instance) integer
    uint64_t _id;

public:

    explicit QueryID ()
    : _coordinatorId(INVALID_INSTANCE), _id(0)
    {}

    explicit QueryID (InstanceID coord, uint64_t id)
    : _coordinatorId(coord), _id(id)
    {}

    QueryID(const QueryID& other)
    : _coordinatorId(other._coordinatorId), _id(other._id)
    {}

    QueryID& operator=(const QueryID& other)
    {
      if (&other != this) {
         _coordinatorId = other._coordinatorId;
         _id = other._id;
      }
      return *this;
    }

    // relying on the default destructor

    bool operator==(const QueryID& other) const
    {
        return ((_coordinatorId == other._coordinatorId) && (_id == other._id));
    }

    bool operator!=(const QueryID& other) const
    {
        return (!operator==(other));
    }

    bool operator<(const QueryID& other) const
    {
        return ((_coordinatorId < other._coordinatorId) ||
                ((_coordinatorId == other._coordinatorId) && (_id < other._id)));
    }

    InstanceID getCoordinatorId() const
    {
        return _coordinatorId;
    }

    uint64_t getId() const
    {
        return _id;
    }

    static QueryID getFakeQueryId()
    {
        /**
         * This is the value that is used when a fake query is needed.
         * It is invoked when code like the following is executed:
         *
         * create array A <a:string> [x=-2:3,2,1];
         * show('select * from A');
         *
         * The 'select ...' query string given to show() is not actually going to run as a query,
         * but a QueryID is still needed to compute its output schema.  This fake id serves the purpose.
         */
        return QueryID(0, 0);
    }

    /**
     * The global array lock exists beyond the lifetime of the query that
     * installs it.  Query objects by default remove all locks matching their
     * query ID from postgres.  Rather than expand the interface of the Query
     * class to allow operators to control locking, create a special query ID
     * that doesn't match any real query IDs and thus avoids being removed
     * at the end of a query's lifetime.
     * @return the QueryID object for the global array lock.
     */
    static QueryID getGlobalArrayLockQueryId()
    {
        return QueryID(0, 1);
    }

    /**
     * @return true if *this is an instance of the global array lock query ID, false if not.
     */
    bool isGlobalArrayLockQueryId() const
    {
        return (*this == getGlobalArrayLockQueryId());
    }

    bool isFake() const
    {
        return (*this == getFakeQueryId());
    }

    bool isValid() const
    {
        return (isValidPhysicalInstance(_coordinatorId) && _id > 0);
    }

    /**
     * Serialize a query ID to a string.
     * @return A string containing the serialized form of the query ID.
     */
    std::string toString() const;

    /**
     * Deserialize a query ID from a string to a QueryID object.
     * @param queryIdStr A string holding the serialized query ID.
     * @return QueryID instance.
     */
    static QueryID fromString(const std::string& queryIdStr);
};

std::ostream& operator<<(std::ostream& os, const QueryID& qId);

std::istream& operator>>(std::istream& is, QueryID& qId);

const QueryID INVALID_QUERY_ID = QueryID();

} // scidb namespace

namespace std
{
    template<>
    struct hash<scidb::QueryID>
    {
        size_t operator()(scidb::QueryID const& q) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, q.getCoordinatorId());
            boost::hash_combine(seed, q.getId());
            return seed;
        }
    };
} // std namespace

#endif /* QUERY_ID_H_ */
