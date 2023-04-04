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
#include <array/ArrayDistribution.h>

#include <array/DimensionDesc.h>
#include <query/Query.h>
#include <system/BlockCyclic.h> // for SCALAPACKArrayDistribution
#include <system/Config.h>
#include <util/PointerRange.h>

#include <log4cxx/logger.h>

#include <sstream>

#include <cstdint>

/**
 * @file ArrayDistribution.cpp
 *
 * This file contains implementation for ArrayDistribution.h
 *
 * Note that changes to this file could potentially affect array storage formats
 * depending on whether any of this information is stored into those formats.
 * Currently that is true of the PartitioniningSchema enumeration itself, which
 * is currently being left in Metadata.h
 *
 */

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("array_distribution"));

void checkDistTypeConsistency()
{
    if(isDebug()) {
        for(int i = dtUninitialized; i < dtEND; ++i) {
            auto dt = DistType(i);
            ASSERT_EXCEPTION(toDistType(distTypeToStr(dt)) == dt, "inconsistent DistType: " << dt);
        }
    }
}

bool isParameterless(DistType distType)
{
    // excludes:
    // dtLocalInstance -- parameter is the instance
    // dtGroupBy -- don't know enough about this yet
    // dtScaLAPACK -- has many parameters
    // dtUndefined -- a partition of arbitrary organization

    return (distType == dtReplication ||
            distType == dtHashPartitioned ||
            distType == dtDataframe ||
            distType == dtByRow ||
            distType == dtByCol ||
            distType == dtRowCyclic ||
            distType == dtColCyclic);
}

bool isPartition(DistType distType)
{
    // excludes:
    // dtUninitialized -- the never set value
    // dtUndefined -- a partition of arbitrary organization
    // dtReplication
    return (distType != dtUninitialized &&
            distType != dtUndefined &&
            distType != dtReplication);
}

bool isStorable(DistType distType)
{
    // These are the distributions that can be explicitly used by store
    return distType == dtHashPartitioned
        || distType == dtReplication
        || distType == dtDataframe
        || distType == dtRowCyclic
        || distType == dtColCyclic;

}

bool isUserSpecifiable(DistType distType)
{
    // These are the distributions that can be explicitly given to create array.
    // Note how this omits dtDataframe, which is implied by schema and not given
    // as an explicit 'distribution: x' option.
    return distType == dtHashPartitioned
        || distType == dtReplication
        || distType == dtRowCyclic
        || distType == dtColCyclic;
}

bool isDataframeCompatible(DistType distType)
{
    // These distributions are allowable for dataframes.
    return distType == dtDataframe
        || distType == dtReplication;
}


//
// Is this a legitimate DistType for the Dimensions
//
// NOTE: there is a Distribution::valid(const Dimensions&)
//       but unfortunately, until we have converted distribution
//       inheritance and synthesis to Distributions (rather
//       than DistTypes), there are places in the code that need
//       to make this check and do not have the Distribution available,
//       only the DistType.
//
//       So unfortunately need to keep this in sync manually
//       with all implementations of Distributioni::valid()
//
//       To avoid accidental divergence, there are SCIDB_ASSERTS in
//       Distribution::valid() implementations that their answers are
//       consistent.
//
//       TODO: Once inheritance/synthesis are converted to Distributions
//       [which impedes stored parameterized distributions as well],
//       this function can (and should be) eliminated
//
bool isValidForDistType(DistType distType, const Dimensions& dims)
{
    if (distType == dtByCol ||
        distType == dtColCyclic) {

        return dims.size() >= 2;
    }

    return true;
}


// defaultDistTypeRoot() is allowed to be dtReplication
// this allows using the entire test tree to test inheritance of dtReplication
// and many cases of synthesizing dtReplication resulting from its inheritance.
// (for example, build)
DistType defaultDistTypeRoot()
{
    static DistType S_defaultDistTypeRoot = dtUninitialized;
    if (isUninitialized(S_defaultDistTypeRoot)) {
        auto tmp=static_cast<DistType>(Config::getInstance()->getOption<int>(CONFIG_X_DFLT_DIST_ROOT));
        if(not isHashed(tmp)) {
            LOG4CXX_ERROR(logger, "defaultDistTypeRoot: unsupported config var: x-dflt-dist-root= " << tmp
                                  << " " << distTypeToStr(tmp));
        }

        // dtReplication disallowed because by definition this returns a partition
        bool isOk = isUserSpecifiable(tmp) &&
                    S_defaultDistTypeRoot != dtByCol;     // dtByCol would prevent single dimension arrays
        ASSERT_EXCEPTION(isOk, "x-dflt-dist-root disallowed value: " << tmp << " " << distTypeToStr(tmp));

        S_defaultDistTypeRoot = tmp;   // checks complete
        LOG4CXX_INFO(logger, "S_defaultDistTypeRoot: " << S_defaultDistTypeRoot
                             << " " << distTypeToStr(S_defaultDistTypeRoot));
    }

    // postconditions
    SCIDB_ASSERT(isUserSpecifiable(S_defaultDistTypeRoot));
    SCIDB_ASSERT(dtByCol != S_defaultDistTypeRoot);

    return S_defaultDistTypeRoot;
}

// TODO: rename this to defaultDistTypePartition() or some other name
//       that makes it clear it always returns a partition
DistType defaultDistType()
{
    static DistType S_defaultDistType = dtUninitialized;
    if (isUninitialized(S_defaultDistType)) {
        auto tmp = static_cast<DistType>(Config::getInstance()->getOption<int>(CONFIG_X_DFLT_DIST_OTHER));
        if(not isHashed(tmp)) {
            LOG4CXX_ERROR(logger, "defaultDistType: unsupported config var: x-dflt-dist= " << tmp
                                  << " " << distTypeToStr(tmp));
        }

        // dtReplication disallowed because by definition this returns a partition
        bool isOk = isUserSpecifiable(tmp) &&
                    S_defaultDistType != dtByCol &&     // dtByCol would prevent single dimension arrays
                    not isReplicated(S_defaultDistType); // dtReplication is not a partition
        ASSERT_EXCEPTION(isOk, "x-dflt-dist disallowed value: " << tmp << " " << distTypeToStr(tmp));

        S_defaultDistType = tmp;   // checks complete
        LOG4CXX_INFO(logger, "S_defaultDistType: " << S_defaultDistType
                             << " " << distTypeToStr(S_defaultDistType));
    }

    // postconditions
    SCIDB_ASSERT(isUserSpecifiable(S_defaultDistType));
    SCIDB_ASSERT(dtByCol != S_defaultDistType);
    SCIDB_ASSERT(isPartition(S_defaultDistType));           // unlike ... Root this one is never replicated

    return S_defaultDistType;
}

DistType defaultDistTypeInput()
{
    static DistType S_defaultDistTypeInput = dtUninitialized;

    if (isUninitialized(S_defaultDistTypeInput)) {
        auto tmp = static_cast<DistType>(Config::getInstance()->getOption<int>(CONFIG_X_DFLT_DIST_INPUT));
        if(not isHashed(tmp)) {
            LOG4CXX_ERROR(logger, "defaultDistTypeInput: unsupported config var:"
                                  << " x-dflt-dist-input = " << tmp
                                  << " " << distTypeToStr(tmp));
        }

        // dtByCol disallowed because it would prevent single dim arrays
        bool isOk = isUserSpecifiable(tmp) && S_defaultDistTypeInput != dtByCol;
        ASSERT_EXCEPTION(isOk, "x-dflt-dist-input disallowed value: "<< tmp << " " << distTypeToStr(tmp));

        S_defaultDistTypeInput = tmp;   // checks complete
        LOG4CXX_INFO(logger, "S_defaultDistTypeInput: " << S_defaultDistTypeInput
                             << " " << distTypeToStr(S_defaultDistTypeInput));
    }

    // postconditions
    SCIDB_ASSERT(isUserSpecifiable(S_defaultDistTypeInput));
    SCIDB_ASSERT(dtByCol != S_defaultDistTypeInput);

    return S_defaultDistTypeInput;
}

ArrayDistPtr createDistribution(DistType dt, size_t redundancy)
{
    return ArrayDistributionFactory::getInstance()->construct(dt, redundancy);
}

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputAdist,
                               ArrayResPtr const& outputAres,
                               std::shared_ptr<Query> const& query)
{
    InstanceID location = outputAdist->getPrimaryChunkLocation(chunkPosition,
                                                               dims,
                                                               outputAres->size());
    if (location == ALL_INSTANCE_MASK) {
        //XXX TODO: this only works if the residency is the same as
        //XXX TODO: the default query residency because
        //XXX TODO: SG interprets ALL_INSTANCE_MASK wrt the default residency
        //XXX TODO: However, it should cause only extra data movement (to non-participating instances),
        //XXX TODO: but still preserve correctness.
        return location;
    }
    InstanceID physicalInstanceId = outputAres->getPhysicalInstanceAt(location);

    InstanceID logicalInstanceId = query->mapPhysicalToLogical(physicalInstanceId);
    return logicalInstanceId;
}

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputAdist,
                               std::shared_ptr<Query> const& query)
{
    InstanceID queryLogicalInstance = outputAdist->getPrimaryChunkLocation(chunkPosition,
                                                                           dims,
                                                                           query->getInstancesCount());
    return queryLogicalInstance;
}

bool ArrayDistribution::valid(const Dimensions& dims) const
{
    SCIDB_ASSERT(isValidForDistType(getDistType(), dims)); // check consistency with pre-construction info
    return true;
}

bool ArrayDistribution::getNextChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                                          size_t nInstances, InstanceID instanceID) const
{
    // curPos is pass-by-value because it is modified internally as a side-effect

    // this brute-force search implementation moved from BuildArrayIterator::_nextChunk() sha 2be3e13a
    SCIDB_ASSERT(dims.size() > 0);
    while (true) {
        // advance to the next chunk position
        size_t curDim = dims.size() - 1;   // start with largest-numbered dimension
        while ((curPos[curDim] += dims[curDim].getChunkInterval()) > dims[curDim].getEndMax()) { // end dim
            if (curDim == 0) {
                return false;          // out of space to search
            }
            curPos[curDim] = dims[curDim].getStartMin();
            --curDim;
        }
        // if it belongs to us, we are done
        auto loc = getPrimaryChunkLocation(curPos, dims, nInstances);
        if (loc == instanceID ||
            loc == InstanceID(ALL_INSTANCE_MASK)) {
            outPos = curPos;
            return true;
        }
        // handy during alterations:
        //std::stringstream tmp;
        //std::copy(curPos.begin(), curPos.end(), std::ostream_iterator<int64_t>(tmp, ","));
        //LOG4CXX_WARN(logger, "NOCHECKIN: ArrayDistributionFactory::getNextChunkPosition skip at: " << tmp.str());
    }
}

/// returns true if input position is valid: use when starting iteration
bool ArrayDistribution::getFirstChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                                           size_t nInstances, InstanceID instanceID) const
{
    /// curPos is pass-by-value because it is modified internally as a side-effect
    auto loc = getPrimaryChunkLocation(curPos, dims, nInstances);
    if (loc == instanceID ||
        loc == InstanceID(ALL_INSTANCE_MASK)) {
        outPos = curPos;
        return true;    // input position is valid
    }

    /// advance to a valid position
    return getNextChunkCoord(outPos, dims, curPos, nInstances, instanceID);
}

InstanceID
UndefinedArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPosition, Dimensions const& dims,
                                                    size_t nInstances) const
{
    // for dtUndefined, anyone who has a copy of the chunk IS primary.
    // If we were to output DBArray chunks from load (instead of MemArray)
    // this would be needed for that (but we don't do that)
    // This case does arise during failover, when an array is no longer in its
    // original distribution.  Its possible we should make a new kind of
    // distribution for that case and handle it specially.
    // However, so far this is not necessary, and we can pave that road when we actually
    // have enough traffic that we should pave it.
    ASSERT_EXCEPTION_FALSE("UndefinedArrayDistribution cannot map chunks to instances");
    return  InstanceID(INVALID_INSTANCE);
}

// code controlled by XOR_EXPERIMENT
// is not fully validated yet.
// Bernstein et. al. tells me that we have to measure whether
// post-murmur xor-folding increases entropy.  (it does not for CityHash, for example)
// but the code below is working and can be used to make that test at
// at later point. Until then, we'll use the 128-bit mumrmur hash
// output to scale into the number of instances
// which also avoids the (expensive) integer divide caused by %
#define XOR_EXPERIMENT 0
#if XOR_EXPERIMENT

/**
 * MurmurHash3_x64_128_xor is "owned" by getHashedChunkNumber.
 * IT MAY NOT BE CHANGED  to produce different results without increasing
 * the database revision number.
 * TODO: RESULT_T should be an unsigned integer.  can that be enforced somehow?
 */
inline uint8_t fold(__uint128_t fullHash, uint8_t dummy) {
    return uint8_t (( fullHash     &0xFF) ^
                    ((fullHash>>8 )&0xFF) ^
                    ((fullHash>>16)&0xFF) ^
                    ((fullHash>>24)&0xFF) ^
                    ((fullHash>>32)&0xFF) ^
                    ((fullHash>>40)&0xFF) ^
                    ((fullHash>>48)&0xFF) ^
                    ((fullHash>>56)&0xFF) );  // actually, this stopped early,fullhash is to 120
}
inline uint16_t fold(__uint128_t fullHash, uint16_t dummy) {
    return uint16_t(( fullHash         &0xFFFF) ^
                    ((fullHash>>(1*16))&0xFFFF) ^
                    ((fullHash>>(2*16))&0xFFFF) ^
                    ((fullHash>>(3*16))&0xFFFF) ^
                    ((fullHash>>(4*16))&0xFFFF) ^
                    ((fullHash>>(5*16))&0xFFFF) ^
                    ((fullHash>>(6*16))&0xFFFF) ^
                    ((fullHash>>(7*16))&0xFFFF) );
}
inline uint32_t fold(__uint128_t fullHash, uint32_t dummy) {
    return uint32_t(( fullHash         &0xFFFFFFFF) ^
                    ((fullHash>>(1*32))&0xFFFFFFFF) ^
                    ((fullHash>>(2*32))&0xFFFFFFFF) ^
                    ((fullHash>>(3*32))&0xFFFFFFFF) );
}

inline uint64_t fold(__uint128_t fullHash, uint64_t dummy) {
    return uint64_t(( fullHash         &0xFFFFFFFFFFFFFFFF) ^
                    ((fullHash>>(1*64))&0xFFFFFFFFFFFFFFFF) );
}


// this template choose the fold (xoring) function from above based on RESULT_T
template <typename RESULT_T>
inline void MurmurHash3_x64_128_xor( const void * key, int len, uint32_t seed, RESULT_T* out )
{
  __uint128_t fullHash;
  MurmurHash3_x64_128(key, len, seed, &fullHash);

  RESULT_T dummy;
  *(uint64_t*)out = fold(fullHash, dummy);
}

#endif // XOR_EXPERIMENT


uint64_t
HashedArrayDistribution::getHashedChunkInstance(scidb::Dimensions const& dims,
                                                scidb::Coordinates const& pos,
                                                InstanceID nInstances)
{
    // The goal here is to produce a good hash function without using array
    // dimension sizes (which can change in the case of unbounded arrays)
    // NOTE: although that is a very good goal, subarray() "knows" that the hash
    // function is relative to startMin
    // Other code is likely sensitive to the pre-existing division by chunk size, too
    scidb::Coordinates key = pos;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        key[i] -= dims[i].getStartMin();
        key[i] /= dims[i].getChunkInterval();

    }

    int keylen = int(key.size() * sizeof(key[0]));
    uint64_t result;

#if XOR_EXPERIMENT  // preserving until expermintally validated or rejected
    // results folded to the minimum number of bytes required have higher entropy in the
    // lower bits, which results in higher entropy when the hash is followed by "% numInstances"
    // as is the case here in SciDB
    if(numInstances < UINT8_MAX) {
        uint8_t shorterResult;
        MurmurHash3_x64_128_xor(&key[0], keylen,  0 /*seed*/, &shorterResult);
        result = shorterResult;
    } else if (numInstances < UINT16_MAX) {
        uint16_t shorterResult;
        MurmurHash3_x64_128_xor(&key[0], keylen,  0 /*seed*/, &shorterResult);
        result = shorterResult;
    } else if (numInstances < UINT32_MAX) {
        uint32_t shorterResult;
        MurmurHash3_x64_128_xor(&key[0], keylen,  0 /*seed*/, &shorterResult);
        result = shorterResult;
    } else {
        MurmurHash3_x64_128_xor(&key[0], keylen,  0l /*seed*/, &result);
    }
#else
    // this version is "good enough" for the moment.
    // and uneven replica distribution is probably a higher priority than the small
    // potential entropy improvement possible if xor_experiment can be validated.

    __uint128_t result128;
    MurmurHash3_x64_128(&key[0], keylen,  0l /*seed*/, &result128);
    __uint128_t result128low = (result128 >> 64) & 0xFFFFFFFFFFFFFFFF ;
    __uint128_t nInstances128low = nInstances ;
    result = static_cast<uint64_t>(((result128low * nInstances128low)>>64) & 0xFFFFFFFFFFFFFFFF);

#endif

    if (result >= nInstances) {
        std::stringstream ss;
        ss << "result(" << result <<") >= nInstances("<<nInstances ;
        ASSERT_EXCEPTION(result < nInstances, ss.str());
    }
    return result;
}

static uint64_t idivCeil(size_t n, uint64_t divisor) {
    SCIDB_ASSERT(divisor > 0);
    return (n+divisor-1)/divisor;
}

bool ByColumnArrayDistribution::valid(const Dimensions& dims) const
{
    bool result = dims.size() >= 2;
    SCIDB_ASSERT(result == isValidForDistType(getDistType(), dims)); // check consistency with pre-construction info
    return result;
}

InstanceID
ByColumnArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPos,
                                                   Dimensions const& dims,
                                                   size_t nInstances) const
{
    const size_t COL = 1;
    uint64_t result = 0;

    if (dims.size() > COL) {
        SCIDB_ASSERT(dims[COL].getChunkInterval() > 0);

        // number of chunks wide the entire array is
        const auto widthInChunks = idivCeil(dims[COL].getLength(), dims[COL].getChunkInterval());

        // the max number of chunks per instance is therefore ...
        const auto maxChunksPerInstance = idivCeil(widthInChunks, nInstances);

        const auto posInChunks = (chunkPos[COL] - dims[COL].getStartMin()) / dims[COL].getChunkInterval();
        result = posInChunks / maxChunksPerInstance ;
    }

    ASSERT_EXCEPTION(result < nInstances, "ByColumnArrayDistribution::getPrimaryChunkLocation() overflow");
    return result;
}

InstanceID
ByRowArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPos,
                                                Dimensions const& dims,
                                                size_t nInstances) const
{
    const size_t ROW = 0;
    uint64_t result = 0;

    if (dims.size() > ROW) {            // dataframes don't have rows
        SCIDB_ASSERT(dims[ROW].getChunkInterval() > 0);

        // number of chunks high the entire array is
        const auto widthInChunks = idivCeil(dims[ROW].getLength(), dims[ROW].getChunkInterval());

        // the max number of chunks per instance is therefore ...
        const auto maxChunksPerInstance = idivCeil(widthInChunks, nInstances);

        const auto posInChunks = (chunkPos[ROW] - dims[ROW].getStartMin()) / dims[ROW].getChunkInterval();
        result = posInChunks / maxChunksPerInstance ;
    }

    ASSERT_EXCEPTION(result < nInstances, "ByRowArrayDistribution::getPrimaryChunkLocation() overflow");
    return result;
}

InstanceID
RowCyclicDistrib::getPrimaryChunkLocation(Coordinates const& pos, Dimensions const& dims, size_t nInstances) const
{
    const size_t ROW = 0;

    auto row = (pos[ROW] - dims[ROW].getStartMin());
    auto chunk = row/dims[ROW].getChunkInterval() ;
    return chunk % nInstances;                     // cyclic assignment
}

bool RowCyclicDistrib::getNextChunkCoord(Coordinates& outPos, Dimensions const& dims, Coordinates curPos,
                                            size_t nInstances, InstanceID instanceID) const
{
    // curPos is pass-by-value because it is modified internally as a side-effect
    SCIDB_ASSERT(dims.size() > 0);

    while (true) {
        // advance to the next chunk position (at most 1 step in each dimension)
        size_t curDim = dims.size() - 1;   // start with largest-numbered dimension
        while ((curPos[curDim] += dims[curDim].getChunkInterval()) > dims[curDim].getEndMax()) { // end dim
            if (curDim == 0) {
                return false;          // out of space to search
            }
            curPos[curDim] = dims[curDim].getStartMin();
            --curDim;
        }
        // if it belongs to us, we are done
        if (getPrimaryChunkLocation(curPos, dims, nInstances) == instanceID) {
            outPos = curPos;
            return true;
        }
        // it does not belong to us, so we are on the wrong row

        // reset all dimensions except for the row
        // (this is the optimization over base class version)
        for (curDim = dims.size() -1 ; curDim > 0; --curDim) { // end dim
            curPos[curDim] = dims[curDim].getStartMin();
        }
        // increment the row
        curPos[0] += dims[0].getChunkInterval();
        if(curPos[0] > dims[0].getEndMax()) { // end dim
            return false;          // out of space to search
        }

        // if it belongs to us, we are done
        if (getPrimaryChunkLocation(curPos, dims, nInstances) == instanceID) {
            outPos = curPos;
            return true;
        }
    }
}


//
// ColCyclicDistrib
//
bool ColCyclicDistrib::valid(const Dimensions& dims) const
{
    bool result = dims.size() >= 2;
    SCIDB_ASSERT(result == isValidForDistType(getDistType(), dims)); // check consistency with pre-construction info
    return result;
}

InstanceID
ColCyclicDistrib::getPrimaryChunkLocation(Coordinates const& pos, Dimensions const& dims, size_t nInstances) const
{
    const size_t COL = 1;
    if (dims.size() <= COL) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);
    }

    SCIDB_ASSERT(dims[COL].getChunkInterval() > 0);
    auto col = (pos[COL] - dims[COL].getStartMin());
    auto chunk = col/dims[COL].getChunkInterval() ;
    return chunk % nInstances;                     // cyclic assignment
}

namespace dist {

// TODO: this might no longer be used and can be removed

/**
 * ArrayDistribution wrapper distribution which applies transformation(s) on the inputs and/or outputs of another distribution.
 * It is not intended as a public interface because it might introduce an unnecessary level of complexity.
 * Currently, it is used only to support the instanceShift property used by byRow and byCol distributions.
 * The hope is that the need for this class will disappear when instanceShift is handled by byRow & byCol
 */
class TranslatedArrayDistribution : public ArrayDistribution
{
 public:
    /**
     * Constructor
     * @param arrDist input distribution for which to use translation
     * @param instanceShift the output of the input distribution is (modulo)added this amount
     */
    TranslatedArrayDistribution (ArrayDistPtr& arrDist,
                                 size_t instanceShift)
    :  ArrayDistribution(arrDist->getDistType(), arrDist->getRedundancy()),
       _inputArrDist(arrDist), _instanceShift(instanceShift)
    {
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const ;

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const ;

    std::string getState() const override
    {
        // FYI: SystemCatalog guards against storing TranslatedArrayDistribution
        return _inputArrDist->getState();
    }

    size_t getInstanceShift() const { return _instanceShift;  }

 private:
    ArrayDistPtr _inputArrDist;
    size_t _instanceShift;
};

InstanceID
TranslatedArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                                     Dimensions const& dims,
                                                     size_t nInstances) const
{
    InstanceID destInstanceId = _inputArrDist->getPrimaryChunkLocation(chunkPosition,
                                                                       dims,
                                                                       nInstances);
    return (destInstanceId + _instanceShift) % nInstances;
}

bool
TranslatedArrayDistribution::checkCompatibility(const ArrayDistPtr& otherArrDist) const
{
    SCIDB_ASSERT(otherArrDist);
    const TranslatedArrayDistribution* other =
       dynamic_cast<const TranslatedArrayDistribution*>(otherArrDist.get());
    if (other==NULL) {
        return false;
    }

    if (!_inputArrDist->checkCompatibility(other->_inputArrDist)) {
        return false;
    }

    if (_instanceShift != other->_instanceShift) {
        return false;
    }

    return true;
}

} // namespace dist


//
// LocalArrayDistribution
//

bool LocalArrayDistribution::checkCompatibility(const ArrayDistPtr& otherArrDist) const
{
    if (!ArrayDistribution::psEqual(otherArrDist)) {
        return false;
    }
    const LocalArrayDistribution *lard =
       dynamic_cast<const LocalArrayDistribution*>(otherArrDist.get());
    if (lard == NULL) {
        return false;
    }

    return (_localInstance == lard->_localInstance);
}


//
// DataframeDistribution
//

InstanceID
DataframeDistribution::getPrimaryChunkLocation(Coordinates const& chunkPos,
                                               Dimensions const& dims,
                                               size_t nInstances) const
{
    // Preliminary sanity checks...
    SCIDB_ASSERT(!chunkPos.empty());
    SCIDB_ASSERT(nInstances);
    ASSERT_EXCEPTION(isDataframe(dims),
                     "Cannot use dataframe distribution on non-dataframe: "
                     << dims);
    ASSERT_EXCEPTION(chunkPos[DF_INST_DIM] > -1,
                     "Illegal dataframe chunk position: "
                     << CoordsToStr(chunkPos));

    // Interpretation of the $inst coordinate will depend on the mode.
    // By default dataframePhysicalIdMode() is true (and we'll get rid
    // of the option flag in a later release).
    InstanceID iid = static_cast<InstanceID>(chunkPos[DF_INST_DIM]);

    // Ordinarily we want to avoid SGing dataframes, so we'd like to
    // just return the logical id of the current instance.  However,
    // restoring from opaque backup via the input() operator presents
    // a problem.  In that case we'd like to send the dataframe chunk
    // back to its original node, to avoid a situation where, before
    // backup, the chunks were more or less evenly distributed, but
    // after restore they are all suddenly stored on a single
    // instance, which could mean bad performance (more SG when the
    // whole point is to have less SG).  Unfortunately here we can't
    // know if it's the "restore from backup" case or not.  So we have
    // to do a little more work.

    if (dataframePhysicalIdMode()) {

        // Because of the backup/restore case we can't just return the
        // local logical id, we have to actually attempt the mapping.
        // If the mapping fails, then we're looking at a chunk from
        // backup or a replica chunk, and then it's OK to use the
        // local logical instance id.

        auto query = Query::getQueryPerThread();
        ASSERT_EXCEPTION(query, "Per-thread query pointer not available?!");
        try {
            iid = query->mapPhysicalToLogical(iid);
        } catch (scidb::Exception const&) {
            // Physical id's node is no longer with us.  Keep it local.
            iid = query->getInstanceID();
        }
        ASSERT_EXCEPTION(iid < nInstances,
                         "Bad nInstances, or instance id mapping is broken! "
                         "iid=" << iid << " >= nInstances=" << nInstances);
    }
    else if (iid >= nInstances) {

        // Mode switch says iid is a logical id, but it's outside the nInstances
        // range.  But ReplicationManager::isResponsibleFor() thought we should
        // see the chunk, so rather than throw an error, just keep it local.

        auto query = Query::getQueryPerThread();
        ASSERT_EXCEPTION(query, "Per-thread query pointer not available?!");
        iid = query->getInstanceID();
    }

    return iid;
}

bool DataframeDistribution::checkCompatibility(const ArrayDistPtr& otherArrDist) const
{
    // Other must be dtDataframe too.
    return psEqual(otherArrDist);
}

//
// Factory
//

void ArrayDistributionFactory::getTranslationInfo(const ArrayDistribution* arrDist,
                                                  InstanceID& instanceShift)
{
    instanceShift = 0;

    const dist::TranslatedArrayDistribution *translated =
       dynamic_cast<const dist::TranslatedArrayDistribution*>(arrDist);

    if (translated) {
        instanceShift = translated->getInstanceShift();
    }
}

ArrayResPtr createDefaultResidency(PointerRange<InstanceID> instances)
{
    ArrayResPtr residency = std::make_shared<MapArrayResidency>(instances.begin(),instances.end());
    return residency;
}

std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist)
{
    InstanceID instanceShift(0);
    ArrayDistributionFactory::getTranslationInfo(&dist, instanceShift);

    stream << " dt: " ;

    switch (dist.getDistType()) {
    case dtReplication:     stream<<"dtRepl";
                            break;
    case dtHashPartitioned: stream<<"dtHash";
                            break;
    case dtLocalInstance:   stream<<"dtLoca";
                            break;
    case dtByRow:           stream<<"dtByro";
                            break;
    case dtByCol:           stream<<"dtByco";
                            break;
    case dtUndefined:       stream<<"dtUndef";
                            break;
    case dtGroupBy:         stream<<"dtGroupby";
                            break;
    case dtScaLAPACK:       stream<<"dtScaLAPACK";
                            break;
    case dtDataframe:       stream<<"dtDataframe";
                            break;
    case dtRowCyclic:       stream<<"dtRCyclic";
                            break;
    case dtColCyclic:       stream<<"dtCCyclic";
                            break;
    default:
        SCIDB_ASSERT( false);
    }
    stream << " = " << dist.getDistType();

    stream << " state: '" << dist.getState();
    stream << "' redun: " << dist.getRedundancy();
    stream << " shift: " << instanceShift;
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const ArrayResidency& res)
{
    stream<<"[";
    const size_t nInstances = res.size();
    for (size_t i = 0; i < nInstances; ++i) {
        if (i>0) {
            stream << ", ";
        }
        stream << res.getPhysicalInstanceAt(i);
    }
    stream<<"]";
    return stream;
}

ArrayDistPtr ArrayDistributionFactory::construct(DistType dt,
                                                 size_t redundancy,
                                                 const std::string& ctx,
                                                 size_t instanceShift)
{
    SCIDB_ASSERT (!_constructors.empty());

    const KeyType key(dt);
    FactoryMap::iterator iter = _constructors.find(key);

    if (iter == _constructors.end()) {
        LOG4CXX_WARN(logger, "ArrayDistributionFactory::construct, unknown DistType: " << dt);
        ASSERT_EXCEPTION_FALSE("Unknown array distribution type");
        return ArrayDistPtr();
    }

    ArrayDistPtr arrDist = (iter->second)(dt, redundancy, ctx);

    if (instanceShift!=0) {
        return std::make_shared<dist::TranslatedArrayDistribution> (arrDist, instanceShift);
    }
    return arrDist;
}

void
ArrayDistributionFactory::registerConstructor(DistType dt,
                                              const ArrayDistributionConstructor& constructor)
{
    SCIDB_ASSERT(constructor);
    SCIDB_ASSERT(dt>=dtMIN && dt<dtEND );

    const KeyType key(dt);

    std::pair<FactoryMap::iterator, bool> res =
    _constructors.insert(FactoryMap::value_type(key, constructor));
    if (!res.second) {
        LOG4CXX_ERROR(logger, "ArrayDistributionFactory::registerConstructor, failed DistType: " << dt);
        SCIDB_ASSERT(false);
        std::stringstream ss;
        ss << "ArrayDistributionFactory::registerConstructor(" << dt <<")";
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_OPERATION_FAILED) << ss.str();
    }
    LOG4CXX_TRACE(logger, "ArrayDistributionFactory::registerConstructor, succeeded DistType: " << dt);
}

void
ArrayDistributionFactory::registerBuiltinDistributions()
{
    LOG4CXX_TRACE(logger, "ArrayDistributionFactory::registerBuiltinDistributions start");

    ArrayDistributionConstructor adc(
        std::bind(&ArrayDistributionFactory::defaultConstructor<HashedArrayDistribution>,
                  std::placeholders::_1,
                  std::placeholders::_2,
                  std::placeholders::_3));
    registerConstructor(dtHashPartitioned, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<ReplicatedArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtReplication, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<LocalArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtLocalInstance, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<ByRowArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtByRow, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<RowCyclicDistrib>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtRowCyclic, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<ByColumnArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtByCol, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<ColCyclicDistrib>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtColCyclic, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<GroupByArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtGroupBy, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<ScaLAPACKArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(scidb::dtScaLAPACK, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<UndefinedArrayDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtUndefined, adc);

    adc = std::bind(&ArrayDistributionFactory::defaultConstructor<DataframeDistribution>,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    std::placeholders::_3);
    registerConstructor(dtDataframe, adc);

    LOG4CXX_TRACE(logger, "ArrayDistributionFactory::registerBuiltinDistributions done");
}


//
// RedistributeContext
//

RedistributeContext::RedistributeContext(const ArrayDistPtr& arrDist,
                                         const ArrayResPtr& arrRes)
  : _arrDistribution(arrDist), _arrResidency(arrRes)
{

    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    if(!isValidDistType(_arrDistribution->getDistType(), true)) {
        ASSERT_EXCEPTION_FALSE("RedistributeContext: invalid DistType");
    }
    auto translated = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());

    SCIDB_ASSERT(_arrDistribution->getDistType() != dtUndefined || translated == NULL);
}

RedistributeContext::RedistributeContext(const RedistributeContext& other)
: _arrDistribution(other._arrDistribution), _arrResidency(other._arrResidency)
{
    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    auto thisTranslated  = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());
    auto otherTranslated = dynamic_cast<const dist::TranslatedArrayDistribution*>(other._arrDistribution.get());
    SCIDB_ASSERT(thisTranslated == otherTranslated);

    SCIDB_ASSERT(_arrDistribution->getDistType() != dtUndefined || thisTranslated == NULL);
}

RedistributeContext&
RedistributeContext::operator= (const RedistributeContext& rhs)
{
    if (this != &rhs) {
        ASSERT_EXCEPTION(rhs._arrDistribution, "Invalid array distribution");
        ASSERT_EXCEPTION(rhs._arrResidency, "Invalid array distribution");

        _arrDistribution = rhs._arrDistribution;
        _arrResidency = rhs._arrResidency;
        auto thisTranslated = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());
        auto rhsTranslated  = dynamic_cast<const dist::TranslatedArrayDistribution*>(rhs._arrDistribution.get());
        SCIDB_ASSERT(thisTranslated == rhsTranslated);
    }
    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    return *this;
}

bool
RedistributeContext::isSatisfiedBy(RedistributeContext const& other) const
{
    if (this->isUndefined()) {
        ASSERT_EXCEPTION_FALSE("RedistributeContext::isSatisifedBy self.isUndefined not satisfiable");
        return false;
    }

    return sameDistribAndResidency(other);
}

bool
RedistributeContext::isColocated(RedistributeContext const& other) const
{
    return sameDistribAndResidency(other) &&
           not this->isUndefined() &&    // dtUndefineds (e.g. slice() output) colocate
           not other.isUndefined();      // with nothing, not even themselves
}

bool
RedistributeContext::sameDistribAndResidency(RedistributeContext const& other) const
{
    // TODO: _translation has never been checked (dating back to when this was operator==)
    //       document when it matters or eliminate _translation

    // TODO: the following if condition seems to be premature optimization.  If so, eliminate it.
    if (this != &other) {
        SCIDB_ASSERT(other._arrDistribution);
        if (!_arrDistribution->checkCompatibility(other._arrDistribution)) {
            return false;
        }
        if (_arrResidency && !other._arrResidency) { return false; }
        if (!_arrResidency && other._arrResidency) { return false; }
        if (_arrResidency && other._arrResidency &&
            !_arrResidency->isEqual(other._arrResidency)) { return false; }
    }
    return true;
}

std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist)
{
    stream<<"dist: ";

    if (dist._arrDistribution) {

        stream << "dist: [ " << *dist.getArrayDistribution() << " ]" ;

    } else {
        stream << "default";
    }

    stream << " res: ";

    if (dist._arrResidency) {
        stream << (*dist.getArrayResidency());
    } else {
        stream << "default";
    }

    return stream;
}

} // namespace scidb
