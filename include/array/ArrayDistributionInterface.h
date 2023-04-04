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

#ifndef ARRAY_DISTRIBUTION_INTERFACE_H_
#define ARRAY_DISTRIBUTION_INTERFACE_H_

#include <iostream>
#include <memory>
#include <unordered_map>

#include <array/ArrayDesc.h>
#include <array/Coordinate.h>
#include <array/Dimensions.h>
#include <query/InstanceID.h>
#include <util/PointerRange.h>
#include <util/Singleton.h>

namespace scidb
{

/**
 * Partitioning schema shows how an array is distributed among the SciDB instances.
 *
 * Guidelines for introducing a new DistType:
 *   - Add to enum DistType (right above dtEND).
 *   - Modify the doxygen comments in LogicalSG.cpp.
 *   - Provide an implementation of ArrayDistribution interface and register it with ArrayDistributionFactory.
 *   - Modify std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist),
 *        see ArrayDistribution.cpp).
 *   - If the DistType uses extra data, modify doesDistTypeHaveData().
 *   - Update the isPartition(), isParameterless(), and other predicates as necessary.
 */
enum DistType
{
    dtUninitialized = -1, // e.g. ps after ArrayDesc::ArrayDesc()
    dtMIN = 0,
    dtReplication = dtMIN,
    dtHashPartitioned,
    dtLocalInstance,    // all chunks on a single instance, typically coordinator.  consider renaming
    dtByRow,            // divide chunks across instances, not cyclic, so instances go unused
    dtByCol,            //     if range of row or col is under populated
    dtUndefined,        // a range of meanings, including sometimes "wildcard"
                        // TODO: replace with psWildcard and others as required
    dtGroupBy,
    dtScaLAPACK,
    dtDataframe,
    dtRowCyclic,        // 1D chunk-cyclic across instances (1st coord)
    dtColCyclic,        // 1D chunk-cyclic across instances (2nd coord)
    dtEND               // A newly introduced DistType should be added before this line.
};

void checkDistTypeConsistency();

// inline only so that it gets updated with the enum
// must be kept in sync with DistType and printSchema()
inline DistType toDistType(const std::string name)
{
    // NOTE: Names prefixed with  underscore have not yet been exposed to the user
    //       and may still be changed. Names without underscore have been exposed
    //       and may not be changed without breaking backward compatibility.

         if(name == "_uninitialized")  { return dtUninitialized; }
    else if(name == "replicated")      { return dtReplication; }      // string may not change
    else if(name == "hashed")          { return dtHashPartitioned; }  // string may not change
    else if(name == "_local_instance") { return dtLocalInstance; }
    else if(name == "_by_row")         { return dtByRow; }
    else if(name == "_by_col")         { return dtByCol; }
    else if(name == "_undefined")      { return dtUndefined; }
    else if(name == "_group_by")       { return dtGroupBy; }
    else if(name == "_scalapack")      { return dtScaLAPACK; }
    else if(name == "dataframe")       { return dtDataframe; }        // string may not change
    else if(name == "row_cyclic")      { return dtRowCyclic; }        // string may not change
    else if(name == "col_cyclic")      { return dtColCyclic; }        // string may not change

    // otherwise...
    // since this may be called with a user-supplied string, we want a user-oriented
    // exception that causes them to examine the string value of the distributon they
    // supplied
    throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_SPECIFIC_DISTRIBUTION_REQUIRED);
}

// inline only so that it gets updated with the enum
// must be kept in sync with DistType and toDistType()
inline const char * distTypeToStr(const DistType dist)
{
    // NOTE: Names prefixed with  underscore have not yet been exposed to the user
    //       and may still be changed. Names without underscore have been exposed
    //       and may not be changed without breaking backward compatibility.

    switch(dist) {
    case dtUninitialized: return "_uninitialized";
        break; // unreached
    case dtReplication: return "replicated";         // string may not change
        break; // unreached
    case dtHashPartitioned: return "hashed";         // string may not change
        break; // unreached
    case dtLocalInstance: return "_local_instance";
        break; // unreached
    case dtByRow: return "_by_row";
        break; // unreached
    case dtByCol: return "_by_col";
        break; // unreached
    case dtUndefined: return "_undefined";
        break; // unreached
    case dtGroupBy: return "_group_by";
        break; // unreached
    case dtScaLAPACK: return "_scalapack";
        break; // unreached
    case dtDataframe: return "dataframe";           // string may not change
        break; // unreached
    case dtRowCyclic: return "row_cyclic";          // string may not change
        break; // unreached
    case dtColCyclic: return "col_cyclic";          // string may not change
        break; // unreached
    default:
        // it is a programmer error to reach here
        // for every DistType enum added, the programmer must add
        // a case statement above
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE);
        break; // unreached
    }
}

inline void printDistType(std::ostream& os, const DistType dist) {
    os << distTypeToStr(dist);
}

// use predicates wherever possible so that
// direct use of dtXXXX is easily reviewed in code
bool isParameterless(DistType); // requires no parameters to construct
bool isPartition(DistType);  // e.g. not psReplicated, dtUndefined
bool isDistributed(DistType); // e.g. not dtLocalInstance, dtUndefined
bool isStorable(DistType);    // can be written to a datastore
bool isUserSpecifiable(DistType); // can be used with "distribution xxx" syntax
bool isDataframeCompatible(DistType);   // can be used with dataframes

inline bool isUninitialized(DistType dt) { return dt==dtUninitialized; }
inline bool isReplicated(DistType dt) { return dt==dtReplication; }
inline bool isHashed(DistType dt) { return dt==dtHashPartitioned; }
inline bool isLocal(DistType dt) { return dt==dtLocalInstance; }
inline bool isByRow(DistType dt) { return dt==dtByRow; }
inline bool isByCol(DistType dt) { return dt==dtByCol; }
inline bool isUndefined(DistType dt) { return dt==dtUndefined; }
inline bool isGroupBy(DistType dt) {return dt==dtGroupBy; }
inline bool isScaLAPACK(DistType dt) { return dt==dtScaLAPACK; }
inline bool isDataframe(DistType dt) { return dt==dtDataframe; }
inline bool isRowCyclic(DistType dt) { return dt==dtRowCyclic; }
inline bool isColCyclic(DistType dt) { return dt==dtColCyclic; }

bool isValidForDistType(DistType distType, const Dimensions& dims);

class ArrayDistribution;

typedef std::shared_ptr<const ArrayDistribution> ArrayDistPtr;

/**
 * Array distribution interface abstracts a method for assigning array chunks to
 * the consecutive set of size N of natural numbers [0,...,N-1].
 * Such a method uses the chunk coordinates and the array dimensions to produce
 * an integer to which the chunk is assigned. The natural number can be used to compute
 * the SciDB instances to which a given chunk belongs.
 * ArrayDistirbution is part of array metadata and provides some additional information
 * such as array redundancy. Redundancy is the number of *additional* chunk copies
 * stored by the system for a given array. The original chunk is called the primary chunk
 * and the copies are its replicas.
 */
class ArrayDistribution : public std::enable_shared_from_this<ArrayDistribution>, boost::noncopyable
{
public:

    /// Give the distribution a chance to declare itself incompatible with array dimensions.
    /// for example, ByCol and ColCyclic distributions are not compatible with
    /// arrays with less than 2 dimensions and will override.  Checked by the create_array operator.
    virtual bool valid(const Dimensions& dims) const;

    /// the primary purpose of ArrayDistributions are to provide this method
    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const = 0;

    /// Produce a new distribution for a given residency set size.
    /// Some distributions may need to mutate when the number of instances changes.
    /// @param nInstances cardinality of the new residency set
    /// @return a new array distribution that fits a given residency
    virtual ArrayDistPtr adjustForNewResidency(size_t nInstances)
    {
        return shared_from_this();
    }

    /// Check if this distribution is compatible with another distribution
    /// Two distributions are compatible if they are interchangible.
    /// In other words, an SG converting an array from one distribution to the other
    /// would NOT change the results of any operation on the array.
    /// It must be commutative, i.e. A.checkCompatibility(B) == B.checkCompatibility(A)
    /// @deprecated USE isCompatibleWith()
    /// @todo XXX remove this method
    /// @param otherArrDist array distribution to check
    /// @return true if the distributions are interchangible; false otherwise
    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const = 0;

    /// Check if this distribution is compatible with another distribution
    /// Two distributions are compatible if they are interchangible.
    /// In other words, an SG converting an array from one distribution to the other
    /// would NOT change the results of any operation on the array.
    /// It must be commutative, i.e. A.checkCompatibility(B) == B.checkCompatibility(A)
    /// @param otherArrDist array distribution to check
    /// @return true if the distributions are interchangible; false otherwise
    virtual bool isCompatibleWith(const ArrayDistPtr& otherArrDist) const
    {
        return checkCompatibility(otherArrDist);
    }

    /// Get an opaque string representation of the internal state used by the concrete implementation.
    /// This string is used for reconstruction of the object remotely or from the SystemCatalog.
    /// @see ArrayDistributionFactory::construct()
    virtual std::string getState() const { return {}; }

    /// Get the DistType ID which uniquely identifies this type of distribution
    DistType getDistType() const
    {
        return _dt;
    }

    /// Get array redundancy, where redundancy is the number
    /// of additional copies of array stored by the system.
    /// E.g. redundancy==2 means that the system stores 3 copies
    /// of each array chunk, and no 2 copies are stored on the
    /// same physical instance.
    size_t getRedundancy() const
    {
        return _redundancy;
    }

    /// @param outPos outputs curPos if it is valid position, otherwise same as getNextChunkCoord
    /// @param dims array dimensions
    /// @param curPos minimum position the caller will accept
    /// @param nInstances cardinality of the residency set of the array
    /// @param instanceID logical instanceID
    /// @return true when a valid position returned in outPos, false if no such position exists
    bool getFirstChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                            size_t nInstances, InstanceID instanceID) const;

    /// @param outPos outputs the next valid position *after* curPos
    /// @param dims array dimensions
    /// @param curPos minimum position the caller desires. typically the result of
    ///        a prior get{First,Next}ChunkCoord()
    /// @param nInstances cardinality of the residency set of the array
    /// @param instanceID logical instanceID
    /// @return true when a valid position returned in outPos, false if no such position exists
    virtual bool getNextChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                                   size_t nInstances, InstanceID instanceID) const;

protected:

    /// Constructor
    ArrayDistribution(DistType dt, size_t redundancy)
    : _dt(dt), _redundancy(redundancy)
    {
    }

    /// @return true if the DistType of the input distribution
    ///         is the same as that of this distribution; false otherwise
    bool psEqual(const ArrayDistPtr& otherArrDist) const
    {
        return (getDistType() == otherArrDist->getDistType());
    }

private:
    ArrayDistribution();

    DistType _dt;
    size_t _redundancy;
};

/// Stream output operator for ArrayDistribution
std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& res);

/**
 * Array distribution interface abstracts a set of physical SciDB instances
 * on which an array is stored. Each physical instance is referred to by its index (or rank)
 * from the consecutive set of size N of natural numbers [0,...,N-1], where N is the size of the residency.
 * @todo XXX TODO: factor in its own file
 */
class ArrayResidency;
typedef std::shared_ptr<const ArrayResidency> ArrayResPtr;
class ArrayResidency
{
public:

    /// @return the physical instance ID at a given index
    virtual InstanceID getPhysicalInstanceAt(InstanceID index) const = 0;

    /// @return the index for a given physical instance ID
    /// It asserts if the physical instance ID is not in residency
    virtual InstanceID getIndexForPhysicalId(InstanceID physId) const = 0;

    /// @return residency size (i.e. number of physical instances)
    virtual size_t size() const = 0;

    /// @return true if the instances in this and the other residencies are the same
    ///         false otherwise
    /// The functionality is logically equivalent operator==(),
    /// but needs to work with a virtual interface
    bool isEqual(const ArrayResPtr& ar) const
    {
        if (ar.get() == this) {
            return true;
        }
        const size_t nInstances = size();
        if (ar->size() != nInstances) {
            return false;
        }
        for (size_t i = 0; i < nInstances; ++i) {
            if ( ar->getPhysicalInstanceAt(i) != getPhysicalInstanceAt(i)) {
                return false;
            }
        }
        return true;
    }
    /// @return true if the instances in this residencie
    ///         and the collection specified by the input iterators are the same;
    ///         false otherwise
    template<class Iterator>
    bool isEqual(Iterator begin, Iterator end) const
    {
        const size_t nInstances = size();
        size_t i = 0;

        for (; i < nInstances && begin != end; ++i, ++begin) {
            if ( (*begin) != getPhysicalInstanceAt(i)) {
                return false;
            }
        }
        if (begin != end || i != nInstances) {
            return false;
        }
        return true;
    }

protected:
    ArrayResidency() {}
};

/// Stream output operator for ArrayResidency
std::ostream& operator<<(std::ostream& stream, const ArrayResidency& res);

/**
 * A concrete factory for creating ArrayDistributions based on
 * DistTypes and any additional context, typically
 * stored with array metadata in SystemCatalog.
 */
class ArrayDistributionFactory : public scidb::Singleton<ArrayDistributionFactory>
{
public:
    /// Constructor
    explicit ArrayDistributionFactory ()
    {
        checkDistTypeConsistency();
        registerBuiltinDistributions(); //must be non-virtual
    }

    /// Destructor
    virtual ~ArrayDistributionFactory () {}

    /**
     * A functor type for constructing ArrayDistributions
     * @param ps DistType
     * @param redundancy
     * @param context
     */
    typedef std::function< ArrayDistPtr(DistType dt,
                                          size_t redundancy,
                                          const std::string& ctx) >
    ArrayDistributionConstructor;

    /**
     * Register a construction functor for a distribution identified
     * by a DistType
     * @param ps DistType
     * @param constructor for the distribution
     */
    void registerConstructor(DistType dt,
                             const ArrayDistributionConstructor& constructor);

    /**
     * Construct an ArrayDistribution
     *
     * @param ps DistType
     * @param redundancy
     * @param context, "" by default
     * @param instanceShift shift to be added to the chunk location
     *        before it is returned from getPrimaryChunkLocation()
     *        (dtByRow, dtByCol only)
     */
    ArrayDistPtr construct(DistType dt,
                           size_t redundancy,
                           const std::string& ctx=std::string(),
                           size_t instanceShift=0);


    /**
     * Some ArrayDistribution implementations are wrappers which only perform translations
     * on the inputs and/or the output of getPrimaryChunkLocation() of a different distribution.
     * This method provides the information about the translations if any.
     * @param arrDist [in]
     * @param instanceShift [out] added to the chunk location
     *        before it is returned from getPrimaryChunkLocation()
     * @note This is somewhat of a HACK to make some subarray() optimizations to work.
     * Hopefully, we will get rid of the optimizations or the need for them
     * (e.g. by distributing arrays based on the actual coordinates rather than on the relative ones)
     * and the need for the "CoordianteTranslator" hackery will disappear.
     */
    static void getTranslationInfo(const ArrayDistribution* arrDist,
                                   InstanceID& instanceShift);

    /**
     * Default implementation of the ArrayDistribution construction functor
     */
    template<class ArrayDistType>
    static ArrayDistPtr defaultConstructor(DistType dt,
                                           size_t redundancy,
                                           const std::string& ctx)
    {
        return std::make_shared<ArrayDistType>(redundancy, ctx);
    }

private:

    void registerBuiltinDistributions();

    typedef int KeyType;
    typedef ArrayDistributionConstructor ValueType;
    typedef std::unordered_map<KeyType, ValueType > FactoryMap;
    FactoryMap _constructors;
};

/// Persistent arrays may have non-default redundancy (returned by scan() and written by store(), ...)
/// Arrays existing only during query execution have the default redundancy of 0.
static const size_t NO_REDUNDANCY=0;
static const size_t DEFAULT_REDUNDANCY=0;
/// Max redundancy (it exists for performance reasons)
static const size_t MAX_REDUNDANCY=8;

/// @return a partitioning DistType to use when unspecified by the user or the correct value has not yet been plumbed into place
DistType defaultDistType();         // used by create, store, general default TODO: rename to defaultDistTypePartition()
DistType defaultDistTypeInput();    // to control input (load), independently of above
DistType defaultDistTypeRoot();     // for testing: allows setting physical plan's root inheritance
                                    // to a storable distribution

/**
 * Historical context for  defaultDistribution()
 *
 * Most of the code was assuming that arrays are scanned and stored in
 * dtHashPartitioned Partitioning (currently a category of ArrayDistribution).
 * Therefore there were many "dtHashPartitioned" scatter about the code that
 * logically mean "whatever the default DistType is".  We are moving on
 * a path toward generalizing what distributions can be stored.  The first attempt
 * will be to extend tempArrays to dtReplication, dtByRow, and dtByCol.  When that
 * is working we will enable it for dbArrays.  Later we will move on to distributions
 * which require additional parameterizations to be stored, such as the 2D
 * ScaLAPACK.
 *
 * As scaffolding for that effort, we are making the "default" DistType
 * configurable.  This will allow us to (1) experiment with performance of alternate
 * distributions for certain workflows and (2) start fixing the query compiler/optimizer
 * to insert SGs without assuming that dtHashPartitioned is the universal goal.
 *
 * Then we can add a create array parameterization for particular distributions
 * and drive the query tree's output toward that. (e.g. right now, for iterated spgemm,
 * dtByRow and dtByCol are far more efficient than psHashed)
 *
 * Once this is working for TEMP arrays, we can allow it for general arrays
 *
 * Once this all works correctly via create statements, we will no longer be managing
 * "ps" outside of ArrayDesc's and ArrayDistributions, this function should no longer be referenced
 * and this scaffolding removed.
 */


/// Helper method for creating array distributions which do not require any context
/// @param ps DistType
/// @param redundancy, DEFAULT_REDUNDANCY by default
ArrayDistPtr createDistribution(DistType dt, size_t redundancy=DEFAULT_REDUNDANCY);

/**
 * Create a default implementation of ArrayResidency with the specified physical instances
 * @param physInstances an ordered collection of physical instance IDs,
 *        the order in which they are specified determines their ranks
 *        mapping the output of ArrayDistribution::getPrimaryChunkLocation()
 *        to the physical instances
 * @return ArrayResidency
 */
ArrayResPtr createDefaultResidency(PointerRange<InstanceID> physInstances);

} // namespace scidb

#endif // ARRAY_DISTRIBUTION_INTERAFCE_H_
