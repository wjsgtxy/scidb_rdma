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

///
/// @file PhysicalOperator.h
///

#ifndef PHYSICAL_OPERATOR_H_
#define PHYSICAL_OPERATOR_H_

#include <array/ArrayDesc.h>
#include <array/ArrayDistribution.h>
#include <array/MemoryBuffer.h>
#include <array/MultiStreamArray.h>
#include <query/TypeSystem.h>
#include <query/LogicalExpression.h>
#include <query/Expression.h>
#include <query/OperatorContext.h>
#include <query/OperatorDist.h>
#include <query/OperatorID.h>
#include <query/OperatorParam.h>
#include <query/PhysicalBoundaries.h>

namespace scidb
{
class Aggregate;
class Array;
class ConstChunk;
class JobQueue;
class OperatorContext;
class Query;
class ThreadPool;
class InjectedErrorListener;


class DistributionRequirement
{
public:
    enum reqType
    {
        Any,
        Collocated,
        SpecificAnyOrder
    };

    DistributionRequirement (reqType rt,
                             std::vector<RedistributeContext> specificRequirements =
                             std::vector<RedistributeContext>(0));

    virtual ~DistributionRequirement()
    {}

    reqType getReqType() const
    {
        return _requiredType;
    }

    std::vector<RedistributeContext> const& getSpecificRequirements()
    {
        return _specificRequirements;
    }

private:
    reqType                     const _requiredType;
    std::vector<RedistributeContext> const _specificRequirements;
};


class DimensionGrouping
{
public:
    DimensionGrouping()
    {}

    DimensionGrouping(Dimensions const& originalDimensions,
                      Dimensions const& groupedDimensions)
    {
        _dimensionMask.reserve(originalDimensions.size());

        for (size_t i=0,n=groupedDimensions.size(); i!=n ; ++i)
        {
            DimensionDesc d = groupedDimensions[i];
            std::string const& baseName = d.getBaseName();
            ObjectNames::NamesType const& aliases = d.getNamesAndAliases();
            for (size_t j=0,k=originalDimensions.size(); j!=k ; ++j)
            {
                DimensionDesc dO = originalDimensions[j];
                if (dO.getBaseName() == baseName &&
                    dO.getNamesAndAliases() == aliases &&
                    (dO.getLength()==CoordinateBounds::getMaxLength() || dO.getLength()==d.getLength()))
                {
                    _dimensionMask.push_back(j);
                }
            }
        }

        assert(_dimensionMask.empty() || _dimensionMask.size()==groupedDimensions.size());
    }

    Coordinates reduceToGroup(CoordinateCRange in) const
    {
        Coordinates out(std::max(_dimensionMask.size(),1UL));
        reduceToGroup(in,out);
        return out;
    }

    void reduceToGroup(CoordinateCRange in,CoordinateRange out) const
    {
        if (_dimensionMask.empty())
        {
            out[0] = 0;
        }
        else
        for (size_t i=0, n=_dimensionMask.size(); i!=n; ++i)
        {
            out[i] = in[_dimensionMask[i]];
        }
    }

private:
    std::vector<size_t> _dimensionMask;
};

typedef std::shared_ptr < std::pair<Coordinates, InstanceID> > ChunkLocation;

/**
 * A map that describes which chunks exist on which instances for the purpose of searching along an axis.
 * The map is constructed with a set of dimensions and an axis of interest. After that, given the coordinates of a chunk,
 * the map can be used to locate the next or previous chunk along the specified axis.
 *
 * The map can be serialized for transfer between instances.
 */
class ChunkInstanceMap
{
private:
    size_t _numCoords;
    size_t _axis;

    size_t _numChunks;

    typedef std::unordered_map<Coordinates, std::shared_ptr< std::map<Coordinate, InstanceID> >, CoordinatesHash> Coords2Map;
    Coords2Map _chunkLocations;
    Coords2Map::iterator _outerIter;
    std::map<Coordinate, InstanceID>::iterator _innerIter;

    template <int SEARCH_MODE>
    ChunkLocation search(Coordinates const& coords) const
    {
        assert(coords.size() == _numCoords);
        Coordinates copy = coords;
        Coordinate inner = copy[_axis];
        copy[_axis]=0;

        Coords2Map::const_iterator outerIter = _chunkLocations.find(copy);
        if (outerIter == _chunkLocations.end())
        {   return std::shared_ptr < std::pair<Coordinates,InstanceID> >(); }

        std::map<Coordinate, InstanceID>::const_iterator iter = outerIter->second->find(inner);
        if (iter == outerIter->second->end())
        {   return std::shared_ptr < std::pair<Coordinates,InstanceID> >(); }

        if (SEARCH_MODE == 1)
        {
            iter++;
            if (iter == outerIter->second->end())
            {   return std::shared_ptr < std::pair<Coordinates,InstanceID> >(); }
        }
        else if (SEARCH_MODE == 2)
        {
            if (iter == outerIter->second->begin())
            {   return std::shared_ptr < std::pair<Coordinates,InstanceID> >(); }
            iter--;
        }

        copy[_axis]=iter->first;
        return std::make_shared<std::pair<Coordinates,InstanceID> >(copy,iter->second);
    }

public:
    /**
     * Create empty map.
     * @param numCoords number of dimensions; must not be zero
     * @param axis the dimension of interest; must be between zero and numCoords
     */
    ChunkInstanceMap(size_t numCoords, size_t axis):
        _numCoords(numCoords), _axis(axis), _numChunks(0)
    {
        if (numCoords==0 || axis >= numCoords)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
               << "Invalid parameters passed to ChunkInstanceMap ctor";
        }
    }

    virtual ~ChunkInstanceMap()
    {}

    /**
     * Add information about a chunk to the map.
     * @param coords coordinates of the chunk. Must match numCoords. Adding duplicate chunks is not allowed.
     * @param instanceId the id of the instance that contains the chunk
     */
    void addChunkInfo(Coordinates const& coords, InstanceID instanceId)
    {
        if (coords.size() != _numCoords)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
               << "Invalid coords passed to ChunkInstanceMap::addChunkInfo";
        }

        Coordinates copy = coords;
        Coordinate inner = copy[_axis];
        copy[_axis]=0;

        _outerIter=_chunkLocations.find(copy);
        if(_outerIter==_chunkLocations.end())
        {
            _outerIter=_chunkLocations.insert(
                    std::pair<Coordinates, std::shared_ptr<std::map<Coordinate, InstanceID> > >(
                            copy,
                            std::make_shared<std::map<Coordinate,uint64_t> >()
                            )).first;
        }

        if (_outerIter->second->find(inner) != _outerIter->second->end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
               << "Duplicate chunk information passed to ChunkInstanceMap::addChunkInfo";
        }

        _outerIter->second->insert(std::pair<Coordinate, InstanceID>(inner, instanceId));
        _numChunks++;
    }

    inline std::vector<Coordinates> getAxesList() const
    {
        std::vector<Coordinates> result;
        Coords2Map::const_iterator outerIter = _chunkLocations.begin();
        while (outerIter != _chunkLocations.end())
        {
            result.push_back(outerIter->first);
            outerIter++;
        }
        return result;
    }

    /**
     * Print human-readable form.
     */
    friend std::ostream& operator<<(std::ostream& stream, const ChunkInstanceMap& cm)
    {
        if(cm._chunkLocations.size()==0)
        {
            stream<<"[empty]";
            return stream;
        }

        Coords2Map::const_iterator outerIter;
        std::map<Coordinate, InstanceID>::const_iterator innerIter;

        std::vector<Coordinates> key;
        outerIter=cm._chunkLocations.begin();
        while(outerIter!=cm._chunkLocations.end()) {
            key.push_back(outerIter->first);
            ++outerIter;
        }
        /* otherwise output not detirministic */
        std::sort(key.begin(), key.end(), CoordinatesLess());

        for(std::vector<Coordinates>::const_iterator i = key.begin(), e = key.end();
            i != e; ++i) {
            Coordinates coords = *i;
            outerIter = cm._chunkLocations.find(*i);
            innerIter = outerIter->second->begin();
            while (innerIter!=outerIter->second->end())
            {
                Coordinate coord = innerIter->first;
                coords[cm._axis]=coord;
                stream << "[";
                for (size_t i=0; i<coords.size(); i++)
                {
                    if(i>0)
                    { stream <<","; }
                    stream<<coords[i];
                }
                stream<<"]:"<<innerIter->second<<" ";
                ++innerIter;
            }
            stream<<"| ";
        }
        return stream;
    }

    class axial_iterator
    {
    private:
        ChunkInstanceMap const& _cm;
        Coords2Map::const_iterator _outer;
        std::map<Coordinate, InstanceID>::const_iterator _inner;

    public:
        axial_iterator(ChunkInstanceMap const& cm):
             _cm(cm)
        {
            reset();
        }

        ChunkLocation getNextChunk(bool& moreChunksInAxis)
        {
            ChunkLocation result;
            if (_inner==_outer->second->end())
            {
                assert(!end());
                ++_outer;
                _inner=_outer->second->begin();
            }

            Coordinates coords = _outer->first;
            coords[_cm._axis]=_inner->first;
            result = std::make_shared<std::pair<Coordinates,InstanceID> >(coords,_inner->second);
            ++_inner;
            moreChunksInAxis = true;
            if(_inner==_outer->second->end())
            {
                moreChunksInAxis = false;
            }
            return result;
        }

        inline ChunkLocation getNextChunk()
        {
            bool placeholder;
            return getNextChunk(placeholder);
        }

        inline bool end() const
        {
            if(_outer == _cm._chunkLocations.end())
            {
                return true;
            }
            else if(_inner==_outer->second->end())
            {
                Coords2Map::const_iterator i = _outer;
                i++;
                if(i==_cm._chunkLocations.end())
                {
                    return true;
                }
            }
            return false;
        }

        inline void setAxis(Coordinates const& axisPos)
        {
            _outer=_cm._chunkLocations.find(axisPos);
            if(_outer == _cm._chunkLocations.end())
            {
                return;
            }
            _inner=_outer->second->begin();
        }

        inline bool endOfAxis() const
        {
            if (_outer == _cm._chunkLocations.end())
            {
                return true;
            }
            return _inner==_outer->second->end();
        }

        inline void reset()
        {
            _outer = _cm._chunkLocations.begin();
            if (_outer != _cm._chunkLocations.end())
            {
                _inner=_outer->second->begin();
            }
        }
    };

    axial_iterator getAxialIterator() const
    {
        return axial_iterator(*this);
    }

    /**
     * Given a chunk, find the next chunk along the axis.
     * @param coords coordinates of a chunk.
     * @return a pair of coords and instance id of next chunk. Null if no such chunk in map.
     */
    ChunkLocation getNextChunkFor(Coordinates const& coords) const
    {
        return search <1> (coords);
    }

    /**
     * Given a chunk, find the previous chunk along the axis.
     * @param coords coordinates of a chunk.
     * @return a pair of coords and instance id of next chunk. Null if no such chunk in map.
     */
    ChunkLocation getPrevChunkFor(Coordinates const& coords) const
    {
        return search <2> (coords);
    }

    /**
     * Get the information about a chunk by coordinates.
     * @param coords coordinates of a chunk.
     * @return a pair of coords and instance id that contains the chunk. Null if no such chunk in map.
     */
    ChunkLocation getChunkFor(Coordinates const& coords) const
    {
        return search <3> (coords);
    }

    /**
     * Get the size of the map in buffered form.
     * @return size in bytes.
     */
    inline size_t getBufferedSize() const
    {
        return (_numCoords * sizeof(Coordinate) + sizeof(InstanceID)) * _numChunks + 3 * sizeof(size_t);
    }

    /**
     * Marshall map into a buffer.
     * @return buffer of getBufferedSize() that completely describes the map.
     */
    std::shared_ptr<SharedBuffer> serialize() const
    {
        if(_chunkLocations.size()==0)
        {
            return std::shared_ptr<SharedBuffer>();
        }

        size_t totalSize = getBufferedSize();
        std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, totalSize));
        MemoryBuffer* b = (MemoryBuffer*) buf.get();
        auto sizePtr = reinterpret_cast<size_t*>(b->getWriteData());

        *sizePtr = _numCoords;
        sizePtr++;

        *sizePtr = _axis;
        sizePtr++;

        *sizePtr = _numChunks;
        sizePtr++;

        Coordinate* coordPtr = (Coordinate*) sizePtr;

        Coords2Map::const_iterator outerIter;
        std::map<Coordinate, InstanceID>::const_iterator innerIter;

        outerIter=_chunkLocations.begin();
        while(outerIter!=_chunkLocations.end())
        {
            Coordinates coords = outerIter->first;
            innerIter = outerIter->second->begin();
            while (innerIter!=outerIter->second->end())
            {
                Coordinate coord = innerIter->first;
                coords[_axis]=coord;

                for (size_t i=0; i<_numCoords; i++)
                {
                    *coordPtr = coords[i];
                    coordPtr++;
                }

                InstanceID* instancePtr = (InstanceID*) coordPtr;
                *instancePtr = innerIter->second;
                instancePtr++;
                coordPtr = (Coordinate*) instancePtr;
                ++innerIter;
            }
            ++outerIter;
        }
        return buf;
    }

    /**
     * Merge information from another map into this.
     * @param serializedMap a buffer received as the result of calling serialize() on another ChunkInstanceMap.
     */
    void merge( std::shared_ptr<SharedBuffer> const& serializedMap)
    {
        if(serializedMap.get() == 0)
        {   return; }

        auto sizePtr = reinterpret_cast<const size_t*>(serializedMap->getConstData());

        size_t nCoords = *sizePtr;
        assert(nCoords == _numCoords);
        sizePtr++;

        assert(*sizePtr == _axis);
        sizePtr++;

        size_t numChunks = *sizePtr;
        sizePtr++;

        assert(serializedMap->getSize() == (nCoords * sizeof(Coordinate) + sizeof(InstanceID)) * numChunks + 3 * sizeof(size_t));

        Coordinate* coordPtr = (Coordinate*) sizePtr;

        Coordinates coords(nCoords);
        for(size_t i=0; i<numChunks; i++)
        {
            for(size_t j = 0; j<nCoords; j++)
            {
                coords[j] = *coordPtr;
                coordPtr++;
            }

            InstanceID* instancePtr = (InstanceID*) coordPtr;
            InstanceID nid = *instancePtr;
            addChunkInfo(coords, nid);
            instancePtr ++;
            coordPtr = (Coordinate*) instancePtr;
        }
    }
};


/**
 *      This is the parent class for all physical operators. In order to add new physical operator
 *      you must inherit new class from this class and implement methods. Note, every physical
 *      operator has a logical and implement it. So inferring schema already should be done in
 *      logical operator.
 */
class PhysicalOperator : public std::enable_shared_from_this<PhysicalOperator>, public OperatorDist
{
public:

    PhysicalOperator(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema);

    virtual ~PhysicalOperator();

    ///
    /// @param opID The OperatorID assigned to this
    ///       object by the PhysicalPlan
    void setOperatorID(const OperatorID& opID);

    ///
    /// @return the ID passed by prior setOperatorID()
    ///
    OperatorID getOperatorID() const;

    ///
    /// @param opContext The SG or other context needed
    ///        for remote execution on behalf of this
    ///        operator
    /// @param jobQueue The jobQueue to use for the
    ///        remote side of the execution
    ///
    void setOperatorContext(std::shared_ptr<OperatorContext> const& opContext,
                            std::shared_ptr<JobQueue> const& jobQueue = std::shared_ptr<JobQueue>());

    ///
    /// clears the OperatorContext to be returned by
    /// getOperatorContext
    void unsetOperatorContext();

    ///
    /// @return OperatorContext from setOperatorContext()
    ///
    std::shared_ptr<OperatorContext> getOperatorContext() const;

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    const std::string& getPhysicalName() const;

    /// @return LogicalOperatorName() for a logical operator and PhysicalOperatorName() for a PhysicalOperator
    const std::string& getOperatorName() const override;

    const Parameters& getParameters() const
    {
        return _parameters;
    }

    const KeywordParameters& getKeywordParameters() const
    {
        return _kwParameters;
    }

    Parameter findKeyword(const std::string& kw) const
    {
        auto const kwPair = _kwParameters.find(kw);
        return kwPair == _kwParameters.end() ? Parameter() : kwPair->second;
    }

    const ArrayDesc& getSchema() const
    {
        return _schema;
    }

    /**
     * Return the arena from which any resources associated with the execution
     * of this operator instance should be allocated.
     */
    arena::ArenaPtr getArena() const
    {
        return _arena;
    }

    void setSchema(const ArrayDesc& schema)
    {
        _schema = schema;
    }

    /// @return true iff operator is guaranteed to be single threaded
    /// @note Determines whether corresponding per-operator arena should
    ///       be made thread-safe.
    virtual bool isSingleThreaded() const
    {
        return true;
    }

    virtual void setQuery(const std::shared_ptr<Query>&);

    /**
     * This method is executed on coordinator instance before sending out plan on remote instances and
     * before local call of execute method.
     */
    virtual void preSingleExecute(std::shared_ptr<Query>)
    {
    }

    /**
     * This method is executed on coordinator instance before sending out plan on remote instances and
     * after call of execute method in overall cluster.
     */
    virtual void postSingleExecute(std::shared_ptr<Query>)
    {
    }

    void setParameters(const Parameters& parameters)
    {
        _parameters = parameters;
    }

    /**
     * @brief Inject keyword parameters into physical operator.
     *
     * @details To promote backward source compatibility, operator
     * constructor signatures do not include a KeywordParameters
     * argument.  Instead, this method is called (sometimes: see note
     * below).  This method calls setKeywordParamHook() before
     * returning; derived classes can override that do any additional
     * post-processing on the injected keyword parameters.
     *
     * @note Always called for operators in the original, unoptimized
     *       physical plan.  However, the optimizer can insert
     *       additional physical operators (e.g. repart) *without*
     *       ever calling this.  Perhaps it's time to modify *all*
     *       PhysicalOperator constructors to take the additional
     *       KeywordParameters argument.
     *
     * @note Uses swap, so caller's passed input is clobbered.
     */
    void setKeywordParameters(KeywordParameters& kwParams);

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString(std::ostream &out, int indent = 0) const;

    /**
     * SciDBExecutor call this framework method, executeWrapper()
     * which normally simply calls execute() with the same arguments.
     * The indirection provides a hook for profiling or execution accounting.
     * One may then easily replace that default implementation in OperatorProfiling.cpp
     * with a custom one.  This cleanly separates SciDB from purpose-built profiling code.
     *
     * For arguments, see execute()
     */
    virtual std::shared_ptr< Array> executeWrapper(std::vector<std::shared_ptr<Array> >&, std::shared_ptr<Query>);

    /**
     * Build this operator's piece of the Array plumbing for the executor's data flow machinery.
     *
     * @note The result Array MUST NOT have any autochunked dimensions.  For simple operators, an
     * early call to checkOrUpdateIntervals() may be enough to fulfill this requirement; see
     * PhysicalApply for an example.  Physical operators with more complicated needs often implement
     * a private fixChunkIntervals() method to fix up the schema intervals to match actual inputs.
     */
    virtual std::shared_ptr< Array> execute(
            std::vector< std::shared_ptr< Array> >& inputArrays,
            std::shared_ptr<Query> query) = 0;

    /**
     * This routine allows operators to communicate with profiling code
     * such as provided via executeWrapper(), means to give a factor
     * by which their execution time would scale with the parameters of the problem
     * if they operated exactly as intended.  For example, an operator that received
     * an array of N cells could return the value N*logN .
     * If all actual timings divided by their problemScaleNormalization are relatively
     * constant, then the scaling is relatively successful.
    */
    virtual double problemScaleNormalization() { return 1; }

    /**
     * This routine allows problemScaleNormaliation() to optionally provide a set of
     * units for its normalization measure.  For example, if the execution time would
     * become a constant when divided by the number of cells this might return
     * string("cells").  The profiling code can then use this string to make results
     * much easier to read
     */
    virtual std::string problemScaleNormalizationName() { return std::string("null"); }

    virtual DistributionRequirement getDistributionRequirement(
            std::vector<ArrayDesc> const& sourceSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Any);
    }

    /**
     *  [Optimizer API] Determine if the output chunks of this operator
     *  will be completely filled.
     *  @param sourceSchemas shapes of all arrays that will given as inputs.
     *  @return true if output chunking is guraranteed full, false otherwise.
     */
    virtual bool outputFullChunks(
            std::vector<ArrayDesc> const& sourceSchemas) const
    {
        return true;
    }

    /**
     *  [Old SG API] Determine the distribution of operator output.
     *  @param inputDistributions distributions of inputs that will be provided in order same as inputSchemas
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return distribution of the output
     */
    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistributions,
            std::vector<ArrayDesc> const& inputSchemas) const ;

    /**
     *  [Optimizer API] Determine the boundaries of operator output.
     *  @param inputBoundaries the boundaries of inputs that will be provided in order same as inputSchemas
     *  @param inputSchemas shapes of all arrays that will given as inputs
     *  @return boundaries of the output
     */
    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& sourceBoundaries,
            std::vector< ArrayDesc> const& sourceSchemas) const
    {
        return PhysicalBoundaries::createFromFullSchema(_schema);
    }

    /**
     *  [Optimizer API] Can operator affect data integrity enforcement?
     *  @return true iff operator overrides getIsStrict() .
     */
    virtual bool canToggleStrict() const
    {
        return false;
    }

    /**
     * [Optimizer API]
     * return true if the operator can handle the extra
     *     requirements when input[i] is a
     *     a PullSGArrayBlocking object (efficient pullSG)
     *
     *     This requires :
     *     (a) Access is SINGLE_PASS
     *     (b) that all attribes in input[i] are pulled
     *         and pulled in numerical order.
     *     (c) never causes ConstChunk::getEmptyBitmap() to call
     *         setPosition(), which would happen if
     *         a chunk is accessed before its EmptyBitmap is
     *         is created and set on it.
     *
     * It is currently legal to support pullSG for some inputs
     * and not all (or not at all -- the default)
     * as we migrate to more widespread support of pullSG
     */
    virtual bool acceptsPullSG(size_t input) const
    {
        return false;
    }

    /**
     *  [Optimizer API] Is operator enforcing data integrity?
     *  @return true iff enforcement is enabled
     *  @throws SCIDB_SE_INTERNAL_ERROR if operator has no idea what you're talking about
     *
     *  @note The optimizer needs to adjust the "strictness" of implicitly inserted SG operators
     *  based on what certain explicit operators are doing.  Those operators override this method to
     *  give an answer.  If the optimizer asks but the operator didn't override the method, it's an
     *  internal error.
     */
    virtual bool getIsStrict() const
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
            << "Optimizer requested strictness of strictness-unaware operator";
    }

    /**
     *  [Optimizer API] Determine if the operator requires redimensioning/repartitioning of its inputs.
     *
     *  @param[in] inputSchemas shapes of arrays that will be given as input
     *  @param[out] modifiedPtrs pointers to redimensioning/repartitioning schemas for corresponding inputs
     *
     *  @description During query optimization the physical operator gets a chance to examine
     *  its input schemas and decide whether and how each should be redimensioned/repartitioned.
     *  To indicate that a modification to inputSchemas[i] is needed, set modifiedPtrs[i] to
     *  be a pointer to the desired ArrayDesc schema descriptor---which may be one of the
     *  inputSchemas (not well  tested), or may be a synthesized schema stored locally.  A NULL
     *  entry in modifiedPtrs indicates that the corresponding input should not be modified.
     *
     *  Calling code MUST NOT call @c delete on any of the returned redimensionSchema pointers.
     *  Operators implementing this method may use the protected _redimRepartSchemas vector
     *  below to manage schema storage.
     *
     *  The default is "no modification needed", which is indicated by an empty modifiedPtrs
     *  vector.  We also provide some canned redimension/repartition policies:
     *  - repartByLeftmost(), intended to be the default policy for non-unary operators
     *  - repartForStoreOrInsert(), exactly what it says
     *  Other canned policies are possible.
     *
     *  Repartition versus redimension selection:
     *    A repartition operator will be inserted into the physical tree if the schema's
     *    startMin AND endMax are NOT changed but the chunkOverlap and/or the chunkInterval
     *    are changed.
     *
     *    A redimension operator will be inserted into the physical tree if the schema's
     *    startMin OR endMax are changed.  Note:  Currently only endMax would change and
     *    the only operator that would change it is "merge."
     *
     *    The insertion of the repartition or redimension operator into the physical tree
     *    is handled by the HabilisOptimizer.
     *
     *  @note Implementations of this method must ensure that the modified input arrays can
     *  produce the promised output schema, that is, the output schema produced by
     *  LogicalFoo::inferSchema() and stored locally as PhysicalFoo::_schema.  See ticket #4712.
     *
     *  @note It's difficult to compute the optimal chunk sizes (and overlaps?) over the entire
     *  query.  One day we may have a better solution, but for now we make repartitioning
     *  decisions locally, by asking each PhysicalOperator how it would like its inputs
     *  repartitioned.
     *
     *  @see PhysicalOperator::repartByLeftmost
     *  @see PhysicalJoin::requiresRedimensionOrRepartition
     *  @see PhysicalWindow::requiresRedimensionOrRepartition
     *  ...and others.
     */
    virtual void requiresRedimensionOrRepartition(
        std::vector<ArrayDesc> const &  inputSchemas,
        std::vector<ArrayDesc const*> & modifiedPtrs) const
    {
        modifiedPtrs.clear();
    }

    /**
     *  [Optimizer API] Give physical operator a chance to inspect its "parent" logical operator.
     *
     *  @param[in] lop logical operator object that this physical operator implements
     *
     *  @description Sometimes a physical operator needs access to results computed in the logical
     *  operator.  For example, PhysicalXgrid needs to know which input dimensions were autochunked
     *  (and therefore not scaled), so that at execute time it can scale those dimensions as if they
     *  had been fully specified all along.  This virtual method lets a physical operator optionally
     *  call the const methods of its "parent".  It is called just after physical operator creation
     *  on the coordinator.
     *
     *  @p By far the most common action for this method is to set this physical op's control cookie
     *  to the logical op's "inspectable" string, so for convenience, that is the default
     *  implementation.
     */
    virtual void inspectLogicalOp(class LogicalOperator const& lop);

    /**
     * Set/get a control cookie.
     *
     * @description Sometimes a PhysicalOperator on the coordinator wants to insure that all its
     * peers receive some arbitrary control information.  Since plan nodes (and not physical
     * operators themselves) are serialized during plan distribution, these methods are used to copy
     * any control cookie @em into the plan node on the coordinator, and later retrieve it on all
     * workers.
     *
     * @p This method is virtual to allow derived classes to parse the cookie in an
     * operator-dependent way and set private data members accordingly.
     *
     * @note So far we don't have any operators that need to overload the control cookie with
     * multiple uses.  If we ever do, perhaps a JSON dict-of-strings is the way to do it.
     *
     * @{
     */
    virtual void setControlCookie(const std::string& cookie) { _cookie = cookie; }
    const std::string& getControlCookie() const { return _cookie; }
    /** @} */

    bool getTileMode() const
    {
        return _tileMode;
    }

    void setTileMode(bool enabled)
    {
        _tileMode = enabled;
    }

    static InjectedErrorListener&    getInjectedErrorListener();

    /**
     * Helper: print the contents of input into logger, debug level.
     * @param[in] input an array to output
     * @param[in] logger the logger object to use
     */
    static void dumpArrayToLog(std::shared_ptr<Array> const& input, log4cxx::LoggerPtr& logger);

    /**
     * Helper: ensures that the given array object can satisfy the Array::RANDOM access pattern.
     * If input already satisfies this, simply return it. Otherwise create and return a new array object.
     * Note: the input may be reset as the result of calling this function, always use the returned pointer.
     * @param[in|out] input the array; invalidated by the function
     * @param[out] query the query context
     * @return an object with the same data and schema as input that supports Array::RANDOM access.
     */
    static std::shared_ptr<Array> ensureRandomAccess(std::shared_ptr<Array>& input, std::shared_ptr<Query> const& query);

    /*
     * Get a global queue so as to push operator-based jobs into it.
     * The function, upon first call, will create a ThreadPool of CONFIG_RESULT_PREFETCH_THREADS threads.
     */
    static std::shared_ptr<JobQueue>  getGlobalQueueForOperators();

protected:
    Parameters _parameters;
    KeywordParameters _kwParameters;
    // _schema can be changed by getOutputDistribution()
    mutable ArrayDesc _schema;
    arena::ArenaPtr _arena;
    bool _tileMode;
    std::weak_ptr<Query> _query;
    mutable std::vector<std::shared_ptr<ArrayDesc> > _redimRepartSchemas;

    /**
     * Called by setKeywordParameters() after _kwParameters are set.
     */
    virtual void setKeywordParamHook() {}

    // Shorthand to index into the top-level parameter list.
    template<class T>
    std::shared_ptr<T> const& param(size_t i) const
    {
        SCIDB_ASSERT(i < getParameters().size());

        return (std::shared_ptr<T> const &)(getParameters()[i]);
    }

    /**
     * Check that @c checkMe schema's intervals match the resolved array.
     *
     * @description If an interval is AUTOCHUNKED in @c checkMe, take its value from @c resolved.
     * Otherwise corresponding intervals must match.
     *
     * @param[in,out] checkMe schema to check, or update if any dimensions were autochunked
     * @param[in] resolved Array whose schema has all intervals specified
     * @throws SCIDB_LE_DIMENSIONS_DONT_MATCH if non-autochunked intervals don't match.
     *
     * @note Physical operators that may have acquired an autochunked _schema during logical
     * inferSchema() need to fix those autochunked intervals according to their actual input arrays,
     * which they first get to inspect in physical execute().  This method takes care of the easy
     * case: a unary operator that makes no major modifications to input dimensions.
     *
     *@{
     */
    static void checkOrUpdateIntervals(ArrayDesc& checkMe, std::shared_ptr<Array>& resolved);
    static void checkOrUpdateIntervals(ArrayDesc& checkMe, Dimensions const& resolvedDims);
    /**@}*/

    /**
     * Canned impl of requiresRedimensionOrRepartition() for use by most n-ary
     * auto-repartitioning operators.
     *
     * @description Operators whose only requirement is that all inputs have like chunk sizes
     * and overlaps may call this canned implementation.  All input arrays will be repartitioned
     * to match the chunk sizes of the left-most non-autochunked input schema (if they do not
     * already match).  Minimum overlap values will be chosen (the 'join' operator insists on
     * this... for now).
     *
     * @param[in] inputSchemas shapes of arrays that will be given as input
     * @param[in,out] modifiedPtrs pointers to repartitioning schemas for corresponding inputs
     * @throws scidb::UserException SCIDB_LE_ALL_INPUTS_AUTOCHUNKED
     *         if all input schemas are autochunked
     */
    void repartByLeftmost(std::vector<ArrayDesc> const& inputSchemas,
                          std::vector<ArrayDesc const*>& modifiedPtrs) const;

    /**
     * Canned impl of requiresRedimensionOrRepartition() for use by storing operators.
     *
     * @description Repartition according to the intervals and overlaps recorded in the catalog.
     * Catalog intervals *may* be unspecified, so in that case use the corresponding interval from
     * the input.
     *
     * @param[in] inputSchemas shapes of arrays that will be given as input
     * @param[in,out] modifiedPtrs pointers to repartitioning schemas for corresponding inputs
     * @note Assumes a single input array.
     */
    void repartForStoreOrInsert(std::vector<ArrayDesc> const& inputSchemas,
                                std::vector<ArrayDesc const*>& modifiedPtrs) const;

    /**
     * Helper that executes in Debug build only
     * Asserts that a given array descriptor corresponds to the last committed array version.
     * If arrayExists == false, asserts if the last version is found in the catalog.
     *
     * @param nsName
     * @param arrayName
     * @param arrayExists if an array has not been found in the catalog prior to this call (desc is undefined)
     * @param desc array descriptor to be validated (if arrayExists == true)
     */
    static void assertLastVersion(std::string const& nsName,
                                  std::string const& arrayName,
                                  bool arrayExists,
                                  ArrayDesc const& desc);
    /**
     * Helper that executes in Debug build only
     * Asserts that a given array descriptor's distribution and residency is consistent with those of the redistribute context
     * @param schema array descriptor
     * @param distro redistribute context
     */
    static void assertConsistency(ArrayDesc const& schema,
                                  RedistributeContext const& distro);

    /// @return identical distribution BUT with  DEFAULT_REDUNDANCY, possibly the same
    static ArrayDistPtr convertToDefaultRedundancy(ArrayDistPtr const& inputDist) ;

private:
    std::string _logicalName;
    std::string _physicalName;
    std::string _cookie;
    OperatorID                       _operatorID;
    std::shared_ptr<OperatorContext> _operatorContext;

    // global thread pool for operators, that is automatically created in getGlobalQueueForOperators()
    static std::shared_ptr<ThreadPool> _globalThreadPoolForOperators;

    // global queue for operators
    static std::shared_ptr<JobQueue> _globalQueueForOperators;

    // mutex that protects the call to getGlobalQueueForOperators
    static Mutex _mutexGlobalQueueForOperators;

};


/**
 * It's base class for constructing physical operators. It declares some virtual functions that
 * will have template implementation.
 */
class BasePhysicalOperatorFactory
{
public:
    BasePhysicalOperatorFactory(const std::string& logicalName,
                                const std::string& physicalName)
        : _logicalName(logicalName)
        , _physicalName(physicalName)
    {}

    virtual ~BasePhysicalOperatorFactory() {}

    const std::string& getLogicalName() const
    {
        return _logicalName;
    }

    const std::string& getPhysicalName() const
    {
        return _physicalName;
    }

    virtual std::shared_ptr<PhysicalOperator> createPhysicalOperator(const Parameters& parameters, const ArrayDesc& schema) = 0;

protected:
    void registerFactory();

private:
    std::string _logicalName;
    std::string _physicalName;
};

/**
 * This is template implementation of physical operators factory. To declare operator factory for
 * new physical operator just declare variable
 * PhysicalOperatorFactory<NewPhysicalOperator> newPhysicalOperatorFactory("logical_name", "physical_name");
 */
template<class T>
struct PhysicalOperatorFactory : BasePhysicalOperatorFactory
{
    PhysicalOperatorFactory(const std::string& logicalName,
                            const std::string& physicalName)
        : BasePhysicalOperatorFactory(logicalName, physicalName)
    {}

    virtual ~PhysicalOperatorFactory() = default;

    std::shared_ptr<PhysicalOperator>
    createPhysicalOperator(const Parameters& parameters,
                           const ArrayDesc& schema)
    {
        return std::shared_ptr<PhysicalOperator>(new T(getLogicalName(),
                                                       getPhysicalName(),
                                                       parameters,
                                                       schema));
    }
};

template<class T>
struct UserDefinedPhysicalOperatorFactory : PhysicalOperatorFactory<T>
{
    UserDefinedPhysicalOperatorFactory(const std::string& logicalName,
                                       const std::string& physicalName)
    : PhysicalOperatorFactory<T>(logicalName, physicalName)
    {
        BasePhysicalOperatorFactory::registerFactory();
    }

    virtual ~UserDefinedPhysicalOperatorFactory() = default;
};

// WARNING: Logical operator MUST be delcared BEFORE any corresponding
// physical operator(s)!
//
#define DECLARE_PHYSICAL_OPERATOR_FACTORY(name, ulname, upname)         \
    static PhysicalOperatorFactory<name> _physicalFactory##name(ulname, upname); \
    BasePhysicalOperatorFactory* get_physicalFactory##name()            \
    {                                                                   \
        return &_physicalFactory##name;                                 \
    }

#define REGISTER_PHYSICAL_OPERATOR_FACTORY(name, ulname, upname) \
    static UserDefinedPhysicalOperatorFactory<name> _physicalFactory##name(ulname, upname)


#ifndef SCIDB_CLIENT

static const bool DEFAULT_INTEGRITY_ENFORCEMENT = true;

/**
 * Redistribute (i.e S/G) a given array without full materialization.
 * It returns an array that streams data when pulled (via an ArrayIterator).
 * @note IMPORTANT:
 * The returned array has several limitations:
 * 1. Each attribute needs to be pulled one at a time. If the input array supports only the SINGLE_PASS access
 * and has more than one attribute, the array returned by pullRedistribute() can be used to pull only one attribute.
 * redistributeToArray()/ToRandomAccess() can be used to pull *all*  attributes from the SINGLE_PASS array.
 * 2. All desired attributes must be pulled *completely* before pullRedistribute()/redistributeXXX()
 * can be called again. An attribute must either be pulled completely or not at all.
 * 3. After all selected attributes are completely consumed,
 * the SynchableArray::sync() method must be called on the returned array.
 * @param inputArray to redistribute, must support at least MULTI_PASS access or have only a single attribute
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new SynchableArray if inputArray needs redistribution; otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array>
pullRedistribute(std::shared_ptr<Array>& inputArray,
                 const ArrayDistPtr& outputArrayDist,
                 const ArrayResPtr& outputArrayRes,
                 const std::shared_ptr<Query>& query,
                 const std::shared_ptr<PhysicalOperator>& phyOp,
                 bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT); //XXX TODO: remove default

/**
 * Functor type to map chunks to their destination instances
 *
 * @parm Query the current query context
 * @parm Coordinates chunk coordinates
 * @parm ArrayDesc of the array being distributed
 * @return InstanceID where to send the chunk
 */
typedef std::function <
    InstanceID ( const std::shared_ptr<Query>&,
                 const Coordinates&,
                 const ArrayDesc& )
              > SGInstanceLocator;

/**
 * Redistribute (i.e S/G) a given array without full materialization.
 * It returns an array that streams data when pulled (via an ArrayIterator).
 * @note IMPORTANT:
 * The returned array has several limitations:
 * 1. Each attribute needs to be pulled one at a time. If the input array supports only the SINGLE_PASS access
 * and has more than one attribute, the array returned by pullRedistribute() can be used to pull only one attribute.
 * redistributeToArray()/ToRandomAccess() can be used to pull *all*  attributes from the SINGLE_PASS array.
 * 2. All desired attributes must be pulled *completely* before pullRedistribute()/redistributeXXX()
 * can be called again. An attribute must either be pulled completely or not at all.
 * 3. After all selected attributes are completely consumed,
 * the SynchableArray::sync() method must be called on the returned array.
 * @param inputArray to redistribute, must support at least MULTI_PASS access or have only a single attribute
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param instanceLocator a functor to map chunks to their destination instances
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new SynchableArray if inputArray needs redistribution; otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array>
pullRedistribute(std::shared_ptr<Array>& inputArray,
                 const ArrayDistPtr& outputArrayDist,
                 const ArrayResPtr& outputArrayRes,
                 const std::shared_ptr<Query>& query,
                 const std::shared_ptr<PhysicalOperator>& phyOp,
                 SGInstanceLocator& instanceLocator,
                 bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);

/**
 * A vector of partial chunk merger pointers
 */
typedef std::vector<std::shared_ptr<MultiStreamArray::PartialChunkMerger> > PartialChunkMergerList;

/**
 * Redistribute (i.e S/G) a given array without full materialization.
 * It returns an array that streams data when pulled (via an ArrayIterator).
 * It delivers the chunks in the order of attribute IDs specified by attributesOrdered.
 * It is not necessary to specify all the input array attributes; the ommitted ones will not be delivered.
 * @note IMPORTANT:
 * The returned array has several limitations:
 * 1. The chunks need to be pulled 'horizontally' in the order specified by attributes.
 * 2. All specified attributes must be pulled *completely* before pullRedistribute()/redistributeXXX()
 * can be called again. All attributes must either be pulled completely or not at all.
 * 3. After all selected attributes are completely consumed,
 * the SynchableArray::sync() method must be called on the returned array.
 * @param inputArray to redistribute, must support at least MULTI_PASS access or have only a single attribute
 * @param attributes
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new SynchableArray if inputArray needs redistribution; otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array> pullRedistributeInAttributeOrder(std::shared_ptr<Array>& inputArray,
                                                        AttributeOrdering& attributes,
                                                        const ArrayDistPtr& outputArrayDist,
                                                        const ArrayResPtr& outputArrayRes,
                                                        const std::shared_ptr<Query>& query,
                                                        const std::shared_ptr<PhysicalOperator>& phyOp,
                                                        bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);
/**
 * Redistribute (i.e S/G) a given array into an array with the RANDOM access support
 * @param inputArray to redistribute
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new Array with RANDOM access if inputArray needs redistribution;
 *         otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array>
redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                           const ArrayDistPtr& outputArrayDist,
                           const ArrayResPtr& outputArrayRes,
                           const std::shared_ptr<Query>& query,
                           const std::shared_ptr<PhysicalOperator>& phyOp,
                           bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);  //XXX TODO: remove default

/**
 * Redistribute (i.e S/G) a given AGGREGATE array into another AGGREGATE array with the RANDOM access support.
 * An aggregate array is an array of intermediate aggregate states.
 * @see Aggregate.h
 * @param inputArray an aggregate array to redistribute
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param aggreagtes a list aggregate function pointers for each attribute, the aggregate function pointers can be NULL
 *        The aggregate function will be used to merge partial chunks from different instances (in an undefined order).
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new Array with RANDOM access if inputArray needs redistribution;
 *         otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array> redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                                                  const ArrayDistPtr& outputArrayDist,
                                                  const ArrayResPtr& outputArrayRes,
                                                  const std::shared_ptr<Query>& query,
                                                  const std::shared_ptr<PhysicalOperator>& phyOp,
                                                  const std::vector<std::shared_ptr<Aggregate> >& aggregates,
                                                  bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);
/**
 * Redistribute (i.e S/G) a given array into an array with the RANDOM access support.
 * The caller can specify a custom partial chunk merger for each attribute.
 * @param inputArray to redistribute
 * @param outputArrayDist output array distribution
 * @param outputArrayRes output array residency; if NULL, the query deafault residency is assumed
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param mergers [in/out] a list of mergers for each attribute, a merger can be NULL if the default merger is desired.
 *        Upon return the list will contain ALL NULLs.
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely).
 * @return a new Array with RANDOM access if inputArray needs redistribution;
 *         otherwise inputArray is returned and enforceDataIntegrity has no effect
 */
std::shared_ptr<Array> redistributeToRandomAccess(std::shared_ptr<Array>& inputArray,
                                                  const ArrayDistPtr& outputArrayDist,
                                                  const ArrayResPtr& outputArrayRes,
                                                  const std::shared_ptr<Query>& query,
                                                  const std::shared_ptr<PhysicalOperator>& phyOp,
                                                  PartialChunkMergerList& mergers,
                                                  bool enforceDataIntegrity);
/**
 * Redistribute (i.e S/G) from a given input array into a given output array
 * @param inputArray to redistribute from
 * @param outputArray to redistribute into, the output array distribution & residency is obtained from the ArrayDesc
 * @param newChunkCoordinates [in/out] if not NUll on entry, a set of all chunk positions added to outputArray
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely). If inputArray does not require redistribution,
 *        enforceDataIntegrity has no effect and the method is equivalent to Array::append()
 */
void redistributeToArray(std::shared_ptr<Array>& inputArray,
                         std::shared_ptr<Array>& outputArray,
                         std::set<Coordinates, CoordinatesLess>* newChunkCoordinates,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);  //XXX TODO: remove default

/**
 * Redistribute (i.e S/G) from a given input array into a given output array
 * @param inputArray to redistribute from
 * @param outputArray to redistribute into, the output array distribution & residency is obtained from the ArrayDesc
 * @param mergers [in/out] a list of mergers for each attribute, a merger can be NULL if the default merger is desired.
 *        Upon return the list will contain ALL NULLs.
 * @param newChunkCoordinates [in/out] if not NUll on entry, a set of all chunk positions added to outputArray
 * @param query context
 * @param phyOp the PhysicalOperator for the SG
 * @param enforceDataIntegrity if true  data collision/unordered data would cause a UserException
 *        while pulling on the output array (either locally or remotely). If inputArray does not require redistribution,
 *        enforceDataIntegrity has no effect and the method is equivalent to Array::append()
 */
void redistributeToArray(std::shared_ptr<Array>& inputArray,
                         std::shared_ptr<Array>& outputArray,
                         PartialChunkMergerList& mergers,
                         std::set<Coordinates, CoordinatesLess>* newChunkCoordinates,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity=DEFAULT_INTEGRITY_ENFORCEMENT);  //XXX TODO: remove default

void syncBarrier(uint64_t barrierId, const std::shared_ptr<scidb::Query>& query);

/**
 * For internal use only. Flushes any outgoing SG related messages.
 */
void syncSG(const std::shared_ptr<Query>& query);

#endif // SCIDB_CLIENT

std::shared_ptr<Aggregate> resolveAggregate(std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                                            Attributes const& inputAttributes,
                                            AttributeDesc* inputAttributeID = NULL,
                                            std::string* outputName = NULL);

} // namespace

#endif /* PHYSICAL_OPERATOR_H_ */
