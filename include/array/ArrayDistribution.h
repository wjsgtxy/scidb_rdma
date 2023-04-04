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
#ifndef ARRAY_DISTRIBUTION_H_
#define ARRAY_DISTRIBUTION_H_

#include <array/ArrayDistributionInterface.h>

#include <iostream>
#include <memory>
#include <sstream>

#include <boost/operators.hpp>

#include <array/ArrayResidency.h>
#include <array/ArrayDistributionUtils.h>
#include <array/Coordinate.h>

#include <system/Utils.h> // For SCIDB_ASSERT

/**
 * @file ArrayDistribution.h
 *
 * This file contains code needed by by several components: the Operators,
 * the Arrays and Storage, the query compiler and optimization, and probably others.
 *
 * It was factored from Operator.cpp to support some generalization of DistType.
 * Some of it is old code.
 *
 * Note that changes to this file could potentially affect array storage formats
 * depending on whether any of this information is stored into those formats.
 *
 * A word about SciDB data distribution [originally written for the uniq() operator
 * by Alex, but generally applicable.]
 * <br>
 * <br>
 * The default distribution scheme that SciDB uses is called "dtHashPartitioned". In reality, it is a hash of the chunk
 * coordinates, modulo the number of instances. In the one-dimensional case, if data starts at 1 with a chunk size
 * of 10 on 3 instances, then chunk 1 goes to instance 0,  chunk 11 to instance 1, chunk 21 to instance 2, chunk 31 to
 * instance 0, and on...
 * <br>
 * <br>
 * In the two-plus dimensional case, the hash is not so easy to describe. For the exact definition, read
 * HashedArrayDistribution::getPrimaryChunkLocation()
 * <br>
 * <br>
 * All data is currently stored with this distribution. But operators emit data in different distributions quite often.
 * For example, ops like cross, cross_join and some linear algebra routines will output data in a completely different
 * distribution. Worse, ops like slice, subarray, repart may emit "partially filled" or "ragged" chunks - just like
 * we do in the algorithm example above.
 * <br>
 * <br>
 * Data whose distribution is so "violated" must be redistributed before it is stored or processed by other ops that
 * need a particular distribution. The functions [pull]RedistributeXXX() are available and are sometimes
 * called directly by the operator (see PhysicalIndexLookup.cpp for example).
 * Other times, the operator simply tells the SciDB optimizer that
 * it requires input data in a particular distribution or outputs data in a particular distribtuion.
 * The optimizer then inserts the appropriate SG() operators.
 * That approach is more advanatageous, as the optimizer is liable to get smarter about delaying or waiving the
 * call to redistributeXXX(). For this purpose, the functions
 * <br> getOutputDistribution(),
 * <br> changedDistribution() and
 * <br> outputFullChunks()
 * are provided. See their use in the Operator class.
 */

namespace scidb
{
    class Query;

// { SG APIs

/// Compute a logical destination instance ID to which a given array chunk belongs.
/// The logical set of instance IDs is the consecutive set of natural numbers
/// {0,...,#live_instances_participating_in_query-1}
/// This method performs two steps:
/// 1. It identifies the physical instance ID from the array residency using the array distribution.
/// 2. It maps the physical instance ID to a logical instance ID using the query liveness set.
/// The physical instances of the destination array residency must be mappable
/// to a subset of the logical instance IDs via the query liveness set.
/// In other words, the physical instances of the array residency must be live.
/// @param chunkPosition chunk coordinates
/// @param dims array dimensions
/// @param outputArrDistribution destination array distribution
/// @param outputArrResidency destination array residency set
/// @param query current query context
/// @return destination instance ID
/// @throws SystemException if the destination instance ID is not live

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputArrDistribution,
                               ArrayResPtr const& outputArrResidency,
                               std::shared_ptr<Query> const& query);

/// Compute a logical destination instance ID to which a given array chunk belongs.
/// The logical set of instance IDs is the consecutive set of natural numbers
/// {0,...,#live_instances_participating_in_query-1}
/// This method uses the array distribution to identify the destination instance ID from the query liveness set.
/// @param chunkPosition chunk coordinates
/// @param dims array dimensions
/// @param outputArrDistribution destination array distribution
/// @param query current query context
/// @return destination instance ID
/// @throws SystemException if the destination instance ID is not live

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputArrDistribution,
                               std::shared_ptr<Query> const& query);
// SG APIs }

/**
 * Whether a DistType has optional data.
 */
static inline bool doesDistTypeHaveData(DistType dt)
{
    return dt==dtGroupBy || dt==dtScaLAPACK || dt==dtLocalInstance ;
}

/**
 * Whether a DistType is valid
 */
inline bool isValidDistType(DistType dt, bool allowOptionalData=true)
{
    bool isLegalRange = ((dtMIN <= dt) && (dt < dtEND));
    if (!isLegalRange)
    {
        return false;
    }

    if (!allowOptionalData && doesDistTypeHaveData(dt))
    {
        return false;
    }

    return true;
}

/// Helper class used by the optimizer for placing the necessary SG nodes into the physical plan.
/// The necessity for the SG nodes is determined by the ArrayDistribution & ArrayResidency reported
/// by each operator in the plan.
class RedistributeContext
{
private:

    ArrayDistPtr _arrDistribution;
    ArrayResPtr _arrResidency;

public:

    /// Default constructor
    RedistributeContext() {}

    /// Destructor
    virtual ~RedistributeContext() {}

    /// Constructor
    RedistributeContext(const ArrayDistPtr& arrDist,
                        const ArrayResPtr& arrRes);

    /// Copy constructor
    RedistributeContext(const RedistributeContext& other);

    /// Assignment operators
    RedistributeContext& operator= (const RedistributeContext& rhs);

    bool isSatisfiedBy(const scidb::RedistributeContext&) const;
    bool isColocated(RedistributeContext const& other) const;
    bool sameDistribAndResidency(RedistributeContext const& other) const;

    /// @return internal array distribution
    ArrayDistPtr getArrayDistribution() const
    {
        ASSERT_EXCEPTION(_arrDistribution, "Attempt to use NULL ArrayDistribution");
        return _arrDistribution;
    }

    /// @return internal array residency
    ArrayResPtr getArrayResidency() const
    {
        ASSERT_EXCEPTION(_arrResidency, "Attempt to use NULL ArrayResidency");
        return _arrResidency;
    }

    /// @return true if the DistType of the internal array distribution is dtUndefined;
    ///         false otherwise
    bool isUndefined() const
    {
        return (getDistType() == dtUndefined);
    }

    /// @return the DistType of the internal array distribution
    DistType getDistType() const
    {
        return getArrayDistribution()->getDistType();
    }

    /// Stream output operator for RedistributeContext
    /// @todo XXX remove friendship
    friend std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist);
};

/// Stream output operator RedistributeContext
std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist);

// ArrayDistribution Implementations

/// An ArrayDistribution where the system does not know, in advance, the location of chunks.
/// I.e. there is no defined distribution pattern.
/// This is useful in cases such as importing unorganized data or anywhere
/// where it will be useful to force a redistribution by declaring the current one to be undefined.
///
/// TODO: implementations should be moved to the .cpp file
/// TODO: the checkCompaibility() methods are largely duplicates and can be refactored
///       except for the dynamic_cast
class UndefinedArrayDistribution : public ArrayDistribution
{
public:

    UndefinedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtUndefined, NO_REDUNDANCY)
    {
        SCIDB_ASSERT(redundancy==NO_REDUNDANCY);
        SCIDB_ASSERT(ctx.empty());
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override;

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const UndefinedArrayDistribution*>(otherArrDist.get()) != NULL);
    }
};

/// ArrayDistribution which determines chunk locations
/// by applying a hash function on the chunk coordinates
class HashedArrayDistribution : public ArrayDistribution
{
   public:
    HashedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtHashPartitioned, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override
    {
        InstanceID destInstanceId = getHashedChunkInstance(dims, chunkPosition, nInstances);
        return destInstanceId;
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const HashedArrayDistribution*>(otherArrDist.get()) != NULL);
    }

   private:
    /// @return the hash value corresponding to a hash function
    ///         applied on a given chunk position as follows:
    /// The chunk coordinates are converted
    /// to the chunk numbers along each dimension (relative to the dimension start).
    /// i.e. each coordinate is first made relative to the start coordinate
    /// and divided by the chunk size along that coordinate's dimension.
    /// (that's actually a performance issue, because arrays are re-distributed when their
    /// start coordinates change which introduces wasteful and unecessary SGs)
    /// XXX TODO (for JHM?): If this behavior is changed (e.g. by hashing the coordinates directly),
    /// XXX TODO: The replica hashing code (fib64()) in Storage.cpp should be updated accordingly.
    static uint64_t getHashedChunkInstance(scidb::Dimensions const& dims,
                                           scidb::Coordinates const& pos,
                                           InstanceID nInstances);
};

/// ArrayDistribution where chunks are assigned to instances such that:
///   1. the max per instance is idivCeiling(width-in-chunks, nInstances)
///   2. the min per instance is max - 1
///   where idivCeiling(n, divisor) = (n+divisor-1)/divisor
///
///   Note: requires 2 (or more) dimensions.
///
///   Note: the instance load is guaranteed to be within 1 of the max only
///   when all chunks are present.
///   For a distribution that handles sparse chunks better, see
///   ColCyclic (TBD)
class ByColumnArrayDistribution : public ArrayDistribution
{
   public:
    ByColumnArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtByCol, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    virtual bool valid(const Dimensions& dims) const override;

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override;

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ByColumnArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};


/// ArrayDistribution where chunks are assigned to instances such that:
///   1. the max per instance is idivCeiling(height-in-chunks, nInstances)
///   2. the min per instance is max - 1
///   where idivCeiling(n, divisor) = (n+divisor-1)/divisor
///
///   Note: the instance load is guaranteed to be within 1 of the max only
///   when all chunks are present.
///   For a distribution that handles sparse chunks better, see
///   RowCyclic (TBD)
class ByRowArrayDistribution : public ArrayDistribution
{
public:
   ByRowArrayDistribution(size_t redundancy, const std::string& ctx)
   : ArrayDistribution(dtByRow, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override;

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ByRowArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};

/// A block-cyclic assignment of chunks to instances
class RowCyclicDistrib : public ArrayDistribution
{
public:
    RowCyclicDistrib(size_t redundancy, const std::string& ctx)
    :
        ArrayDistribution(dtRowCyclic, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const RowCyclicDistrib*>(otherArrDist.get())!=NULL);
    }

    bool getNextChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                           size_t nInstances, InstanceID instanceID) const override;
    InstanceID getPrimaryChunkLocation(Coordinates const&, Dimensions const&, size_t nInstances) const override;
};

/// A block-cyclic assignment of chunks to instances
class ColCyclicDistrib : public ArrayDistribution
{
public:
    ColCyclicDistrib(size_t redundancy, const std::string& ctx)
    :
        ArrayDistribution(dtColCyclic, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ColCyclicDistrib*>(otherArrDist.get())!=NULL);
    }

    virtual bool valid(const Dimensions& dims) const override;

    InstanceID getPrimaryChunkLocation(Coordinates const&, Dimensions const&, size_t nInstances) const override;
};

/// ArrayDistribution that maps each chunk to ALL instances
class ReplicatedArrayDistribution : public ArrayDistribution
{
   public:
    ReplicatedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtReplication, NO_REDUNDANCY) // ignore redundancy, use 0 redundancy instead
    {
        SCIDB_ASSERT(getRedundancy()==NO_REDUNDANCY); // replicated is self-redundant
        SCIDB_ASSERT(ctx.empty());
    }

    /// @return ALL_INSTANCE_MASK to indicate
    ///         that a chunk with a given position belongs on all instances
    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override
    {
       return InstanceID(ALL_INSTANCE_MASK);
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ReplicatedArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};

/// ArrayDistribution that uses groups to assign chunks to positions
/// @todo XXX define groups
class GroupByArrayDistribution : public ArrayDistribution
{
public:

    GroupByArrayDistribution(size_t redundancy, const std::vector<uint8_t>& arrIsGroupbyDim)
    : ArrayDistribution(dtGroupBy, DEFAULT_REDUNDANCY),
    _arrIsGroupbyDim(arrIsGroupbyDim)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        ASSERT_EXCEPTION((!_arrIsGroupbyDim.empty()),
                         "GroupByArrayDistribution: invalid context");
    }

    GroupByArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtGroupBy, DEFAULT_REDUNDANCY)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        std::stringstream sin(ctx);
        dist::readInts(sin, _arrIsGroupbyDim);

        ASSERT_EXCEPTION((!_arrIsGroupbyDim.empty()),
                         "GroupByArrayDistribution: invalid context");
    }

    /// @return a string represenation of the internal state, in this case the
    /// a bitmask representaion of the group-by dimensions
    std::string getState() const override
    {
        std::stringstream sout;
        dist::writeInts(sout, _arrIsGroupbyDim);
        return sout.str();
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override
    {
        ASSERT_EXCEPTION(chunkPosition.size() == _arrIsGroupbyDim.size(), "EMPTY GroupByDim");

        InstanceID destInstanceId = hashForGroupBy(chunkPosition, _arrIsGroupbyDim);

        return destInstanceId % nInstances;
    }

    ArrayDistPtr adjustForNewResidency(size_t nInstances) override
    {
        ASSERT_EXCEPTION_FALSE("GroupByArrayDistribution::adjustForNewResidency "
                               "not implemented");
        return ArrayDistPtr();
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override
    {
        if (!ArrayDistribution::psEqual(otherArrDist)) {
            return false;
        }
        const GroupByArrayDistribution *gbard =
           dynamic_cast<const GroupByArrayDistribution*>(otherArrDist.get());
        if (gbard == NULL) {
            return false;
        }
        return (_arrIsGroupbyDim == gbard->_arrIsGroupbyDim);
    }

private:

    /**
     * Compute hash over the groupby dimensions.
     * @param   allDims   Coordinates containing all the dims.
     * @param   isGroupby   For every dimension, whether it is a groupby dimension.
     *
     * @note The result can be larger than #instances!!! The caller should mod it.
     */
    InstanceID
    hashForGroupBy(const Coordinates& allDims,
                   const std::vector<uint8_t>& isGroupby ) const
    {
        SCIDB_ASSERT(allDims.size()==isGroupby.size());

        _groups.clear();
        _groups.reserve(allDims.size()/2);

        for (size_t i=0; i<allDims.size(); ++i) {
            if (isGroupby[i]) {
                _groups.push_back(allDims[i]);
            }
        }
        return VectorHash<Coordinate>()(_groups);
    }

private:
    std::vector<uint8_t> _arrIsGroupbyDim;
    mutable Coordinates _groups; //temp to avoid mallocs
};

/// ArrayDistribution which assigns all chunks to the same instances
class LocalArrayDistribution : public ArrayDistribution
{
public:

    LocalArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(dtLocalInstance, DEFAULT_REDUNDANCY),
    _localInstance(INVALID_INSTANCE)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        std::stringstream sin(ctx);
        sin >> _localInstance;
        SCIDB_ASSERT(_localInstance != ALL_INSTANCE_MASK);
        ASSERT_EXCEPTION(_localInstance != INVALID_INSTANCE, "Invalid local instance");
    }

    LocalArrayDistribution(InstanceID i)
    : ArrayDistribution(dtLocalInstance, DEFAULT_REDUNDANCY), _localInstance(i)
    {
        SCIDB_ASSERT(i != ALL_INSTANCE_MASK);
        SCIDB_ASSERT(i != INVALID_INSTANCE);
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const override
    {
        ASSERT_EXCEPTION(_localInstance < nInstances,
                         "LocalArrayDistribution::getPrimaryChunkLocation "
                         "invalid local instance or instance number");
        return _localInstance;
    }

    ArrayDistPtr adjustForNewResidency(size_t nInstances) override
    {
        ASSERT_EXCEPTION(_localInstance < nInstances,
                         "LocalArrayDistribution::adjustForNewResidency "
                         "invalid local instance or instance number");
        return ArrayDistribution::adjustForNewResidency(nInstances);
    }

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override;

    /// @return a string represenation of the internal state, in this case the
    /// logical ID of the instance to which all array chunks are mapped
    std::string getState() const override
    {
        std::stringstream sout;
        sout << _localInstance;
        return sout.str();
    }

    /// @return the logical instance ID to which all array chunks are mapped
    InstanceID getLogicalInstanceId() const { return _localInstance; }

   private:
    InstanceID _localInstance;
};

/// An ArrayDistribution that assigns chunks to the logical instance
/// specified by their DFD_INST coordinate.
class DataframeDistribution : public ArrayDistribution
{
public:

    DataframeDistribution(size_t redundancy, std::string const&)
        : ArrayDistribution(dtDataframe, redundancy)
    { }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPos,
                                       Dimensions const& dims,
                                       size_t nInstances) const override;

    bool checkCompatibility(const ArrayDistPtr& otherArrDist) const override;
};

} // namespace scidb

#endif // ARRAY_DISTRIBUTION_H_
