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
 * RedimensionCommon.h
 *
 *  Created on: Oct 15, 2012
 *  @author dzhang
 *  @author poliocough@gmail.com
 */

#ifndef REDIMENSIONCOMMON_H_
#define REDIMENSIONCOMMON_H_

#include <limits>
#include <log4cxx/logger.h>
#include <vector>

#include <query/Aggregate.h>
#include <query/PhysicalOperator.h>
#include <array/Array.h>

#include <util/BitManip.h>
#include <util/Timing.h>
#include <util/ArrayCoordinatesMapper.h>

#include "RedimSettings.h"
#include "SyntheticDimChunkMerger.h"

namespace scidb {

using std::shared_ptr;
using arena::ArenaPtr;

class ChunkIdMap;
class LocalMetrics;

// Bits used to mark attributes/dimensions
const size_t FLIP      = 1U << 31; // attribute is flipped into dimension or vise versa
const size_t SYNTHETIC = 1U << 30; // dimension of target array is not present in source array

/**
 * Whether flipped, i.e. an attribute came from dim or vise versa.
 */
inline bool isFlipped(size_t j) {
    return isAllOn(j, FLIP);
}

class AttributeMapping
{
    AttributeDesc _attr;
    bool _flipped{false};

public:
    AttributeID getAttrId() const
    {
        return _attr.getId();
    }

    const AttributeDesc& getAttr() const
    {
        return _attr;
    }

    void setAttr(const AttributeDesc& attr)
    {
        _attr = attr;
    }

    bool isFlipped() const
    {
        return _flipped;
    }

    void setFlipped()
    {
        _flipped = true;
    }
};

class DimensionMapping
{
    size_t _dimension;
    bool _flipped{false};

public:
    size_t getDim() const
    {
        return _dimension;
    }

    void setDim(size_t dim)
    {
        _dimension = dim;
    }

    bool isFlipped() const
    {
        return _flipped;
    }

    void setFlipped()
    {
        _flipped = true;
    }
};

/**
 * Superclass for operators PhysicalRedimension and PhysicalRepart.
 */
class RedimensionCommon : public PhysicalOperator
{
    using super = PhysicalOperator;

    bool isSingleThreaded() const override
    {
        return false;
    }

    /**
     * A state vector that may contain both scalar values and aggregate values.
     * It provides init() and accumulate() calls.
     *
     * @note Immediately after an init() call, the states cannot be acquired using get(); only after
     *       at least one accumulate can the states be acquired.
     *
     * @note For a scalar field, if there are multiple values that accumulated into it, keep the
     *       first one (by default).
     */
    class StateVector {
        mgd::vector<Value>               _destItem;   // the state vector
        PointerRange<const AggregatePtr> _aggregates; // the aggregate pointers
        bool _valid;  // whether the state vector contains valid data, i.e. whether accumulate() was called

    public:
        /**
         * Constructor.
         * @param  aggregates The aggregate pointers.
         * @pre Size should match.
         */
        StateVector(const ArenaPtr& arena,
                    PointerRange<const AggregatePtr> a)
            : _destItem(arena,a.size()),
              _aggregates(a.begin(),a.end()),
              _valid(false)
        {
            assert(!a.empty());
            init();
        }

        /**
         * Initialize the state vector.
         * For the aggregate attributes, call the aggregate pointer's initializeState() method on the state;
         * For the scalar attributes, do nothing (the value will be overwritten upon the first accumulate.
         */
        void init() {
            _valid = false;
            for (size_t i=0; i<_destItem.size(); ++i) {
                if (_aggregates[i]) {
                    _aggregates[i]->initializeState(_destItem[i]);
                }
            }
        }

        /**
         * Accumulate an item to the state vector.
         *
         * For the aggregate attributes, call the aggregate pointer's accumulate() method on the
         * state; For the scalar attributes, keep the first one that accumulated (if
         * keepFirstScalar==true), or the last (if keepFirstScalar==false).
         *
         * @param item   The item to accumulate.
         * @param keepFirstScalar   For a scalar field, keep the first value that was accumulated.
         */
        void accumulate(PointerRange<const Value> item, bool keepFirstScalar = true) {
            assert(_destItem.size() == item.size());
            for (size_t i=0; i<_destItem.size(); ++i) {
                if (_aggregates[i]) {
                    _aggregates[i]->accumulateIfNeeded(_destItem[i], item[i]);
                }
                else if (!_valid || !keepFirstScalar) {
                    _destItem[i] = item[i];
                }
            }
            _valid = true;
        }

        /**
         * Return the state vector.
         * @pre _valid must be true.
         */
        PointerRange<const Value> get() const {
            assert(_valid);
            return _destItem;
        }

        /**
         * Return whether the state vector is valid
         * @return true iff vector is in valid state
         */
        bool isValid() const {
            return _valid;
        }
    };

public:
    static log4cxx::LoggerPtr logger;

    /**
     * Vanilla.
     * @param logicalName the name of operator
     * @param physicalName the name of the physical counterpart
     * @param parameters the operator parameters - the output schema and optional aggregates
     * @param schema the result of Logical inferSchema
     */
    RedimensionCommon(const std::string& logicalName,
                      const std::string& physicalName,
                      const Parameters& parameters,
                      const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema),
    _hasDataIntegrityIssue(false)
    {}

    /**
     * @see PhysicalOperator::getOutputBoundaries
     * @return full bounadries based on the schema
     */
    PhysicalBoundaries getOutputBoundaries(std::vector<PhysicalBoundaries> const&,
                                           std::vector<ArrayDesc> const&)
        const override
    {
        return PhysicalBoundaries::createFromFullSchema(_schema);
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     * @return RedistributeContext(Any)
     */
    RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&,
                                              std::vector<ArrayDesc> const&)
        const override
    {
        ASSERT_EXCEPTION_FALSE("RedimensionCommon::getOutputDistribution not implemented!");
        return RedistributeContext();
    }

    /**
     * Set source-to-destination mappings based on variable names.
     *
     * @description
     * Sets up the private mapping vectors.
     * For every aggregate parameter of redimension():
     * Let j be the output attribute number that matches the aggregate parameter.
     * Set _aggregates[j] = the aggregate function, and
     * set _attrMapping[j] = the input attribute ID.
     * set _dimMapping[j] = the proper dimension mapping
     *
     * @param[in] srcArrayDesc the descriptor of the input array
     * @param[in] destAttrs attributes of the destination array, excluding empty bitmap
     * @param[in] destDims dimensions of the destination array
     *
     * @note both _aggregates and _attrMapping have only the real
     *            attributes, i.e. not including the empty tag.
     */
    void setupMappingsByName(ArrayDesc const& srcArrayDesc,
                             const Attributes& destAttrs,
                             PointerRange<DimensionDesc const> destDims);

    /**
     * Set source-to-destination mappings based on order of source variables.
     *
     * @description
     * Set up trivial direct mapping of source attributes/dimensions
     * to destination, used by repart() and redimension()-acting-as-repart.
     *
     * Every i-th attribute of source maps to the i-th destination attribute.
     * Every i-th dimension of source maps to the i-th destination dimension.
     * No aggregation... we are in "repart mode".
     *
     * @param[in] destAttrs attributes of the destination array, excluding empty bitmap
     * @param[in] destDims dimensions of the destination array
     */
    void setupMappingsByOrder(const Attributes& destAttrs,
                              PointerRange<DimensionDesc const> destDims);

    enum RedistributeMode {
        AUTO = 0,   // delegate SG to optimizer
        AGGREGATED, // SG with aggregation/synthetic dimension
        VALIDATED   // SG with data validation (enforce order & no data collisions)
    };

    /**
     * A common routine that redimensions an input array into a materialized output array and returns it.
     * @param srcArray      [in/out] the input array, reset upon return
     * @param query         The query context.
     * @param phyOp         the PhysicalOperator for the SG
     * @param timing        For logging purposes.
     * @param redistributeMode mode of the output redistribution
     * @return the redimensioned array
     */
    std::shared_ptr<Array> redimensionArray(std::shared_ptr<Array>& srcArray,
                                            std::shared_ptr<Query> const& query,
                                            const std::shared_ptr<PhysicalOperator>& phyOp,
                                            ElapsedMilliSeconds& timing,
                                            RedistributeMode redistributeMode,
                                            bool doPhase2Sort=true);

protected:

    /** Parse keyword parameters when they arrive. */
    void setKeywordParamHook() override;

    /* Test the correctness of ChunkIdMap.
     */
    bool testChunkIdMap();

    /* Private interface to manage the 1-d 'redimensioned' array
     */
    std::shared_ptr<MemArray>
    initializeRedimensionedArray(std::shared_ptr<Query> const& query,
                                 const Attributes&    srcAttrs,
                                 const Attributes&    destAttrs,
                                 mgd::vector< std::shared_ptr<ArrayIterator> >& redimArrayIters,
                                 mgd::vector< std::shared_ptr<ChunkIterator> >& redimChunkIters,
                                 size_t  redimChunkSize);

    void appendItemToRedimArray(PointerRange<const Value> item,
                                std::shared_ptr<Query> const& query,
                                PointerRange< std::shared_ptr<ArrayIterator> const > redimArrayIters,
                                PointerRange< std::shared_ptr<ChunkIterator> >       redimChunkIters,
                                size_t& redimCount,
                                size_t redimChunkSize);

    bool updateSyntheticDimForRedimArray(std::shared_ptr<Query> const& query,
                                         ArrayCoordinatesMapper const& coordMapper,
                                         ChunkIdMap& chunkIdMap,
                                         size_t dimSynthetic,
                                         std::shared_ptr<Array>& redimensioned);

    /* Helper function to append data to 'beforeRedistribution' array
     * Note that 'tmp' is provided so it will not be repeatedly created
     * within (at the cost of a malloc), whereas the caller can provide
     * the same Coordinate to use, repeatedly at lower cost
     */
    void appendItemToBeforeRedistribution(ArrayCoordinatesMapper const& coordMapper,
                                          CoordinateCRange lows,
                                          CoordinateCRange intervals,
                                          Coordinates & tmp,
                                          position_t prevPosition,
                                          PointerRange< std::shared_ptr<ChunkIterator> const> chunkItersBeforeRedist,
                                          StateVector& stateVector);
private:

    /// A vector with size = #dest attributes (not including empty tag). The i'th element is
    /// (a) src attribute number that maps to this dest attribute, or
    /// (b) src attribute number that generates this dest aggregate attribute, or
    /// (c) src dimension number that maps to this dest attribute (with FLIP).
    std::vector<size_t>       _attrMapping;

    /// A vector with size = #dest dimensions. The i'th element is
    /// (a) src dim number that maps to this dest dim, or
    /// (b) src attribute number that maps to this dest dim (with FLIP), or
    /// (c) SYNTHETIC.
    std::vector<size_t>       _dimMapping;

    /// A vector of AggregatePtr with size = #dest attributes (not including empty tag).  The i'th
    /// element, if not NULL, is the aggregate function that is used to generate the i'th attribute
    /// in the destArray.
    std::vector<AggregatePtr> _aggregates;

    /// Parsed keyword parameters.
    RedimSettings _settings;

    /// Helper to redistribute the input array into an array with a synthetic dimension
    std::shared_ptr<Array> redistributeWithSynthetic(std::shared_ptr<Array>& inputArray,
                                                       const std::shared_ptr<Query>& query,
                                                       const std::shared_ptr<PhysicalOperator>& phyOp,
                                                       const SyntheticDimChunkMerger::RedimInfo* redimInfo);

    std::shared_ptr<Array> redistributeWithAggregates(std::shared_ptr<Array>& inputArray,
                                                        ArrayDesc const& outSchema,
                                                        const std::shared_ptr<Query>& query,
                                                        const std::shared_ptr<PhysicalOperator>& phyOp,
                                                        bool enforceDataIntegrity,
                                                        bool hasOverlap,
                                                        PointerRange<const AggregatePtr> aggregates);

    /// true if a data integrity issue has been found
    bool _hasDataIntegrityIssue;

    /// Set up provisional dimensions, return true iff they are necessary.
    bool makeProvisionalChunking(bool hasSynthetic, size_t synthDimIndex, Dimensions& provDims);

    /// Replicate metrics across instances, merge them, and compute interval(s).
    void exchangeMetrics(std::shared_ptr<Query> const& query,
                         std::shared_ptr<PhysicalOperator> const& phyOp,
                         LocalMetrics& localMetrics,
                         ssize_t synthDim,
                         Dimensions& inOutDims);
};

} //namespace scidb

#endif /* REDIMENSIONCOMMON_H_ */
