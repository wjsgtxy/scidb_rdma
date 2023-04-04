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

#include "RedimensionCommon.h"

#include "ChunkEstimator.h"
#include "ChunkIdMap.h"
#include "LocalMetrics.h"
#include "OverlapRemapperArray.h"
#include "RedimSettings.h"
#include "RemapperArray.h"
#include "Remapper.h"

#include <array/SortArray.h>
#include <array/UnpinAllChunks.h>
#include <query/AggregateChunkMerger.h>
#include <system/Config.h>
#include <util/OnScopeExit.h>
#include <util/OverlappingChunksIterator.h>
#include <util/PerfTimeScope.h>

#include <array/TupleArray.h>  // for SortingAttributeInfos

using namespace std;

namespace scidb
{

using namespace arena;
using mgd::vector; // place as much of locally allocate memory on the operator
using mgd::map;    //  arena as possible, please...

log4cxx::LoggerPtr RedimensionCommon::logger(log4cxx::Logger::getLogger("scidb.array.RedimensionCommon"));


void RedimensionCommon::setKeywordParamHook()
{
    _settings.init(getPhysicalName(), _kwParameters, /*logical:*/ false);
}

void RedimensionCommon::setupMappingsByName(ArrayDesc const&                  srcArrayDesc,
                                            const Attributes&                 destAttrs,
                                            PointerRange<DimensionDesc const> destDims)
{
    // Mapping vectors should be empty at this point.  We'll resize them now.
    SCIDB_ASSERT(_aggregates.empty() && _attrMapping.empty() && _dimMapping.empty());
    _aggregates.resize(destAttrs.size());
    _attrMapping.resize(destAttrs.size());
    _dimMapping.resize(destDims.size());
    SCIDB_ASSERT(_schema.getAttributes(true).size() == _aggregates.size());

    auto srcAttrs = srcArrayDesc.getAttributes(true);
    PointerRange<DimensionDesc const> srcDims = srcArrayDesc.getDimensions();

    for(size_t i =1; i<_parameters.size(); i++)
    {
        if(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
        {
            AttributeDesc inputAttId;
            string aggOutputName;
            AggregatePtr agg = resolveAggregate((std::shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                    srcArrayDesc.getAttributes(),
                    &inputAttId,
                    &aggOutputName);

            bool found = false;
            if (inputAttId == AttributeDesc())
            {
                inputAttId = srcArrayDesc.getAttributes().firstDataAttribute();
            }

            for (const auto& attr : _schema.getAttributes(true))
            {
                if (attr.getName() == aggOutputName)
                {
                    _aggregates[attr.getId()] = agg;
                    _attrMapping[attr.getId()] = inputAttId.getId();
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR6) << aggOutputName;
            }
        } else {
            assert(_parameters[i]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            assert(i==1);
        }
    }

    for (const auto& dstAttr : destAttrs)
    {
        if (_aggregates[dstAttr.getId()])
        {//already populated
            continue;
        }

        for (const auto& srcAttr : srcAttrs) {
            if (srcAttr.getName() == dstAttr.getName()) {
                _attrMapping[dstAttr.getId()] = srcAttr.getId();
                goto NextAttr;
            }
        }
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(dstAttr.getName())) {
                _attrMapping[dstAttr.getId()] = turnOn(j, FLIP);
                goto NextAttr;
            }
        }

        // A dest attribute either comes from a src dimension or a src attribute. Can't reach here.
        SCIDB_UNREACHABLE();

        NextAttr:;
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destDims[i].getBaseName())) {
                _dimMapping[i] = j;
                goto NextDim;
            }
        }
        for (const auto& srcAttr : srcAttrs) {
            if (destDims[i].hasNameAndAlias(srcAttr.getName())) {
                _dimMapping[i] = turnOn(static_cast<size_t>(srcAttr.getId()), FLIP);
                goto NextDim;
            }
        }
        _dimMapping[i] = SYNTHETIC;
        LOG4CXX_DEBUG(logger, "setupMappingsByName() _dimMapping [" <<i<<"] is SYNTHETIC");
        LOG4CXX_DEBUG(logger, "setupMappingsByName() destDims[i].getBaseName() " << destDims[i].getBaseName());

        NextDim:;
    }
}

void RedimensionCommon::setupMappingsByOrder(const Attributes& destAttrs,
                                             PointerRange<DimensionDesc const> destDims)
{
    // Mapping vectors should be empty at this point.  We'll resize them now.
    SCIDB_ASSERT(_aggregates.empty() && _attrMapping.empty() && _dimMapping.empty());
    _aggregates.resize(destAttrs.size());
    _attrMapping.resize(destAttrs.size());
    _dimMapping.resize(destDims.size());
    SCIDB_ASSERT(_schema.getAttributes(true).size() == _aggregates.size());

    for (const auto& attr : destAttrs) {
        _attrMapping[attr.getId()] = attr.getId();
    }
    for (size_t i = 0; i < _dimMapping.size(); ++i) {
        _dimMapping[i] = i;
    }
    // ...and no aggregates at all.
}

std::shared_ptr<MemArray> RedimensionCommon::initializeRedimensionedArray(
    std::shared_ptr<Query> const&             query,
    const Attributes&    srcAttrs,
    const Attributes&    destAttrs,
    vector< std::shared_ptr<ArrayIterator> >& redimArrayIters,
    vector< std::shared_ptr<ChunkIterator> >& redimChunkIters,
    size_t                               redimChunkSize)
{
    // Create a 1-D MemArray called 'redimensioned' to hold the redimensioned records.
    // Each cell in the array corresponds to a cell in the destination array,
    // where its position within the destination array is determined by two
    // additional attributes: the destination chunk identifier, and the
    // position within the destination chunk.

    // The schema is adapted from destArrayDesc, with the following differences:
    //    (a) An aggregate field's type is replaced with the source field type, but still uses the name of the dest attribute.
    //        The motivation is that multiple dest aggregate attribute may come from the same source attribute,
    //        in which case storing under the source attribute name would cause a conflict.
    //    (b) Two additional attributes are appended to the end:
    //        (1) '$tmpDestPositionInChunk', that stores the location of the item in the dest chunk
    //        (2) '$tmpDestChunkId', that stores the id of the destination chunk
    //
    // The data is derived from the inputarray as follows.
    //    (a) They are "redimensioned".
    //    (b) Each record is stored as a distinct record in the MemArray. For an aggregate field, no aggregation is performed;
    //        For a synthetic dimension, just use dimStartSynthetic.
    //
    // Local aggregation will be performed at a later step, when generating the MemArray called 'beforeRedistribute'.
    // Global aggregation will be performed at the redistributeAggregate() step.
    //

    Dimensions dimsRedimensioned(1);
    Attributes attrsRedimensioned;
    // Avoid having to call safe_static_cast<> in each iteration of for loops
    // destAttrs is constant so there is no reason to accrue the overhead
    ASSERT_EXCEPTION(destAttrs.size() <= std::numeric_limits<AttributeID>::max(),
                     "Too many Destination Attributes");
    AttributeID destAttrsSize = safe_static_cast<AttributeID>(destAttrs.size());
    SCIDB_ASSERT(_aggregates.size() == destAttrsSize);
    SCIDB_ASSERT(_attrMapping.size() == destAttrsSize);

    for (const auto& destAttr : destAttrs) {
        // For aggregate field, store the source data but under the name of the dest attribute.
        // The motivation is that multiple dest aggregate attribute may come from the same source attribute,
        // in which case storing under the source attribute name would cause conflict.
        //
        // An optimization is possible in this special case, to only store the source attribute once.
        // But some unintuitive bookkeeping would be needed.
        // We decide to skip the optimization at least for now.
        const auto i = destAttr.getId();
        if (_aggregates[i]) {
            AttributeDesc const& srcAttrForAggr = srcAttrs.findattr(_attrMapping[i]);
            attrsRedimensioned.push_back(AttributeDesc(destAttrs.findattr(i).getName(),
                                                       srcAttrForAggr.getType(),
                                                       srcAttrForAggr.getFlags(),
                                                       srcAttrForAggr.getDefaultCompressionMethod()));
        } else {
            attrsRedimensioned.push_back(destAttrs.findattr(i));
        }
    }

    attrsRedimensioned.push_back(
        AttributeDesc("$tmpDestPositionInChunk", TID_INT64,
                      0, CompressorType::NONE));
    attrsRedimensioned.push_back(
        AttributeDesc("$tmpDestChunkId", TID_INT64,
                      0, CompressorType::NONE));
    dimsRedimensioned[0] = DimensionDesc("Row", 0, CoordinateBounds::getMax(), redimChunkSize, 0);

    auto attrsRedimensionedWithET = attrsRedimensioned;
    ArrayDesc schemaRedimensioned("",
                                  attrsRedimensionedWithET.addEmptyTagAttribute(),
                                  dimsRedimensioned,
                                  createDistribution(dtUndefined), // jhm: dtUndefined appropriate
                                  _schema.getResidency());
    std::shared_ptr<MemArray> redimensioned = std::make_shared<MemArray>(schemaRedimensioned, query);

    // Initialize the iterators
    redimArrayIters.resize(attrsRedimensioned.size());
    redimChunkIters.resize(attrsRedimensioned.size());
    // Avoid having to call safe_static_cast<> in each iteration of for loops
    // attrsRedimensioned is constant so there is no reason to accrue the overhead
    ASSERT_EXCEPTION(attrsRedimensioned.size() <= std::numeric_limits<AttributeID>::max(),
                     "Too many Attributes in Redimension");
    for (const auto& attr : attrsRedimensioned)
    {
        redimArrayIters[attr.getId()] = redimensioned->getIterator(attr);
    }

    return redimensioned;
}

void RedimensionCommon::appendItemToRedimArray(PointerRange<const Value> item,
                                               std::shared_ptr<Query> const& query,
                                               PointerRange< std::shared_ptr<ArrayIterator> const > redimArrayIters,
                                               PointerRange< std::shared_ptr<ChunkIterator> >        redimChunkIters,
                                               size_t& redimCount,
                                               size_t redimChunkSize)
{
    // if necessary, refresh the chunk iterators
    if (redimCount % redimChunkSize == 0)
    {
        Coordinates chunkPos(1);
        int chunkMode = ChunkIterator::SEQUENTIAL_WRITE;  // important for performance & mem usage
        chunkPos[0] = redimCount;
        for (size_t i = 0; i < redimArrayIters.size(); i++)
        {
            if (redimArrayIters[i]) {
                Chunk& chunk = redimArrayIters[i]->newChunk(chunkPos,
                                                            CompressorType::NONE);
                redimChunkIters[i] = chunk.getIterator(query, chunkMode);
                chunkMode |= ChunkIterator::NO_EMPTY_CHECK;  // creat iterator without this flag only for first attr
            }
        }
    }

    // append the item to the current chunks
    for (size_t i = 0; i < item.size(); i++)
    {
        if (redimChunkIters[i]) {
            redimChunkIters[i]->writeItem(item[i]);
        }
        else {
            --i;
        }
    }
    redimCount++;

    // flush the current chunks, or advance the iters
    if (redimCount % redimChunkSize == 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            if (redimChunkIters[i]) {
                redimChunkIters[i]->flush();
                redimChunkIters[i].reset();
            }
        }
    }
    else
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            if (redimChunkIters[i]) {
                ++(*redimChunkIters[i]);
            }
        }
    }
}

// used during PHASE 3
bool RedimensionCommon::updateSyntheticDimForRedimArray(std::shared_ptr<Query> const& query,
                                                        ArrayCoordinatesMapper const& coordMapper,
                                                        ChunkIdMap& chunkIdMaps,
                                                        size_t dimSynthetic,
                                                        std::shared_ptr<Array>& redimensioned)
{
    using std::shared_ptr;
    // If there is a synthetic dimension, and if there are duplicates, modify the values
    // (so that the duplicates get distinct coordinates in the synthetic dimension).
    //

    queue< pair<position_t, position_t> > updates;
    bool needsResort = false;
    size_t currChunkId;
    size_t nextChunkId;
    position_t prevPosition;
    position_t currPosition;
    Coordinates currPosCoord(coordMapper.getDims().size());
    AttributeID chunkIdAttr = safe_static_cast<AttributeID>(
        redimensioned->getArrayDesc().getAttributes(true).size() - 1);
    AttributeID posAttr = chunkIdAttr - 1;

    const auto& redimensionedAttrs = redimensioned->getArrayDesc().getAttributes(true);

    auto chunkIdAttrIter = redimensionedAttrs.find(chunkIdAttr);
    SCIDB_ASSERT(chunkIdAttrIter != redimensionedAttrs.end());
    shared_ptr<ConstArrayIterator> arrayChunkIdIter = redimensioned->getConstIterator(*chunkIdAttrIter);

    auto posAttrIter = redimensionedAttrs.find(posAttr);
    SCIDB_ASSERT(posAttrIter != redimensionedAttrs.end());
    shared_ptr<ArrayIterator> arrayPosIter = redimensioned->getIterator(*posAttrIter);

    SCIDB_ASSERT(!arrayChunkIdIter->end());
    SCIDB_ASSERT(!arrayPosIter->end());
    shared_ptr<ConstChunkIterator> chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
    shared_ptr<ConstChunkIterator> chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
    shared_ptr<ChunkIterator> chunkPosWriteIter;
    Coordinates lows(coordMapper.getDims().size());
    Coordinates intervals(coordMapper.getDims().size());

    // initialize the previous position value, current chunk id, and lows and intervals
    prevPosition = chunkPosReadIter->getItem().getInt64();
    currChunkId = chunkChunkIdIter->getItem().getInt64();
    coordMapper.chunkPos2LowsAndIntervals(chunkIdMaps.mapIdToChunkPos(currChunkId),
                                          lows,
                                          intervals);
    coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
    ++(*chunkPosReadIter);
    ++(*chunkChunkIdIter);

    // scan array from beginning to end
    Coordinates updatePos(1);                           // moved out of inner loop to avoid malloc
    while (!arrayChunkIdIter->end())
    {
        while (!chunkChunkIdIter->end())
        {
            // Are we processing a new output chunk id?
            nextChunkId = chunkChunkIdIter->getItem().getInt64();
            if (nextChunkId != currChunkId)
            {
                prevPosition = chunkPosReadIter->getItem().getInt64();
                currChunkId = nextChunkId;
                coordMapper.chunkPos2LowsAndIntervals(chunkIdMaps.mapIdToChunkPos(currChunkId),
                                                      lows,
                                                      intervals);
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
                goto nextitem;
            }

            // Are we processing a run of identical positions?
            currPosition = chunkPosReadIter->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                // found a duplicate --- add an update to the list
                pair<position_t, position_t> pu;

                currPosCoord[dimSynthetic]++;
                pu.first = chunkPosReadIter->getPosition()[0];
                pu.second = coordMapper.coord2posWithLowsAndIntervals(lows,
                                                                      intervals,
                                                                      currPosCoord);
                updates.push(pu);

                // make sure the number of duplicates is less than chunk interval (for the synthetic dim)
                if ((currPosCoord[dimSynthetic] - lows[dimSynthetic]) >=
                    intervals[dimSynthetic])
                {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_REDIMENSION_STORE_ERROR7);
                }
            }
            else
            {
                prevPosition = currPosition;
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, currPosCoord);
            }

        nextitem:
            ++(*chunkPosReadIter);
            ++(*chunkChunkIdIter);
        }

        // At the end of a chunk, process any updates we have accumulated...
        if (!updates.empty())
        {
            needsResort = true;

            // OVERWRITING existing cells
            chunkPosWriteIter = arrayPosIter->updateChunk().getIterator(query,
                                                                        ChunkIterator::APPEND_CHUNK |
                                                                        ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                        ChunkIterator::NO_EMPTY_CHECK);
            while (!updates.empty())
            {
                Value updateVal;

                updatePos[0] = updates.front().first;
                updateVal.setInt64(updates.front().second);
                chunkPosWriteIter->setPosition(updatePos);
                chunkPosWriteIter->writeItem(updateVal);

                updates.pop();
            }
            chunkPosWriteIter->flush();
            chunkPosWriteIter.reset();
        }

        // Goto next chunk
        ++(*arrayPosIter);
        ++(*arrayChunkIdIter);
        if (!arrayChunkIdIter->end())
        {
            chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
            chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
        }
    }

    return needsResort;
}


void RedimensionCommon::appendItemToBeforeRedistribution(
    ArrayCoordinatesMapper const& coordMapper,
    CoordinateCRange lows,
    CoordinateCRange intervals,
    Coordinates& tmp,
    position_t prevPosition,
    PointerRange< std::shared_ptr<ChunkIterator> const> chunkItersBeforeRedist,
    StateVector& stateVector)
{
    // Do nothing if stateVector has nothing in it
    if (stateVector.isValid())
    {
        coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, tmp);

        PointerRange<const Value> destItem = stateVector.get();
        for (size_t a = 0, s=chunkItersBeforeRedist.size(); a < s; ++a) {
            bool rc = chunkItersBeforeRedist[a]->setPosition(tmp);
            if (!rc) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(tmp);
            }
            chunkItersBeforeRedist[a]->writeItem(destItem[a]);
        }
    }
}

/**
 * A template function that reads a scidb::Value of any integral type, and returns an int64_t value.
 * This is needed when turning an arbitrary integral type attribute to a dimension.
 *
 * @param v   a scidb::Value object, storing a value of type T.
 * @return    the value of type int64_t.
 *
 * @note T==int8_t || T==int16_t || T==int32_t || T==int64_t || T==uint8_t || T==uint16_t || T==uint32_t || T==uint64_t
 *
 */
template<class T>
int64_t integerTypeToInt64(Value const& v)
{
    // This assert holds, or else we should have thrown SCIDB_LE_OP_REDIMENSION_ERROR2 during
    // LogicalRedimension::inferSchema().
    static_assert(std::is_signed<T>::value || std::numeric_limits<T>::digits < 64,
                  "Cannot safely convert uint64 attribute to int64 dimension.");

    return static_cast<int64_t>(v.get<T>());
}

typedef int64_t (*IntegerCoercion)(const Value&);

std::shared_ptr<Array> RedimensionCommon::redimensionArray(std::shared_ptr<Array>& srcArray,
                                                           std::shared_ptr<Query> const& query,
                                                           std::shared_ptr<PhysicalOperator> const& phyOp,
                                                           ElapsedMilliSeconds& timing,
                                                           RedistributeMode redistributeMode,
                                                           bool doPhase2Sort)
{
    PerfTimeScope pts_all(PTS_REDIM_ALL);

    // These should have been set up by now via setupMappingByFoo().
    SCIDB_ASSERT(!_dimMapping.empty());
    SCIDB_ASSERT(!_attrMapping.empty());
    SCIDB_ASSERT(_attrMapping.size() == _aggregates.size());

    // def of the meta data
    ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true);
    Attributes const& destAttrs = _schema.getAttributes(true);
    Dimensions const& destDims = _schema.getDimensions();
    SCIDB_ASSERT(_attrMapping.size() == destAttrs.size());
    SCIDB_ASSERT(_dimMapping.size() == destDims.size());

    // Does the dest array have a synthetic dimension?
    bool hasSynthetic = false;
    bool synthAutochunked = false;
    size_t dimSynthetic = 0;
    Coordinate dimStartSynthetic = CoordinateBounds::getMin();
    Coordinate dimEndSynthetic = CoordinateBounds::getMax();

    for (size_t i = 0; i < _dimMapping.size(); ++i) {
        if (_dimMapping[i] == SYNTHETIC) {
            LOG4CXX_DEBUG(logger, "redimensionArray() _dimMapping [" <<i<<"] is SYNTHETIC");
            hasSynthetic = true;
            dimSynthetic = i;
            dimStartSynthetic = destDims[i].getStartMin();
            synthAutochunked = destDims[i].isAutochunked();
            if (!synthAutochunked) {
                dimEndSynthetic = dimStartSynthetic + destDims[i].getChunkInterval() - 1;
            }
            SCIDB_ASSERT(dimEndSynthetic>=dimStartSynthetic);
            break;
        }
    }
    LOG4CXX_DEBUG(logger, "redimensionArray() hasSynthetic: " << hasSynthetic);

    // Does the dest array have any aggregate?
    bool hasAggregate = false;
    for (size_t i = 0; i < _aggregates.size(); ++i) {
        if (_aggregates[i]) {
            hasAggregate = true;
            break;
        }
    }

    bool hasOverlap = false;
    for (size_t i = 0; i < destDims.size(); i++) {
        // Does the dest array have any overlap?
        if (destDims[i].getChunkOverlap() != 0) {
            hasOverlap = true;
        }

        // Any pass-thru intervals we should fix up?
        if (destDims[i].getRawChunkInterval() == DimensionDesc::PASSTHRU) {
            size_t srcDim = _dimMapping[i];
            int64_t interval = DimensionDesc::UNINITIALIZED;
            if (isFlipped(srcDim)) {
                // This can happen, see SDB-6285.  We'll have to
                // autochunk this dimension, since there *is* no
                // corresponding source interval.
                interval = DimensionDesc::AUTOCHUNKED;
            } else {
                // Pass the source interval right on through.
                interval = srcArrayDesc.getDimensions()[srcDim].getChunkInterval();
            }
            _schema.getDimensions()[i].setRawChunkInterval(interval);
            SCIDB_ASSERT(destDims[i].getRawChunkInterval() == interval);
        }
    }

    // Maybe create dimensions using provisional chunk intervals.
    //
    // When autochunking, provDims are the provisional dimensions, and we will eventually compute
    // the finalDims.  When *not* autochunking, provDims and finalDims are the same: it's a
    // convenience to have them both to avoid unnecessarily complicating code paths below.
    //
    Dimensions finalDims(destDims);
    Dimensions provDims;
    bool autochunk = makeProvisionalChunking(hasSynthetic, dimSynthetic, provDims);
    SCIDB_ASSERT(autochunk == !provDims.empty());
    if (autochunk) {
        // The destDims (and hence the finalDims) are autochunked.  "Metric exchange" will fill in
        // the missing intervals below!
        LOG4CXX_INFO(logger, "Using provisional chunk intervals, old: [" << destDims
                     << "], new: [" << provDims << ']');
        _schema.setDimensions(provDims); // Will set them to finalDims later.
    } else {
        provDims = destDims;
    }

    // Initialize 'redimensioned' array
    std::shared_ptr<Array> redimensioned;
    vector< std::shared_ptr<ArrayIterator> > redimArrayIters(_arena);
    vector< std::shared_ptr<ChunkIterator> > redimChunkIters(_arena);
    OnScopeExit onExit([this, &redimChunkIters] () {
        unpinAllChunks(redimChunkIters, logger);
    });

    size_t redimChunkSize =
        Config::getInstance()->getOption<size_t>(CONFIG_REDIMENSION_CHUNKSIZE);
    LOG4CXX_DEBUG(logger, "[RedimensionArray] redimChunkSize: from CONFIG_REDIMENSION_CHUNKSIZE "
                          << redimChunkSize);

    redimensioned = initializeRedimensionedArray(query,
                                                 srcAttrs,
                                                 destAttrs,
                                                 redimArrayIters,
                                                 redimChunkIters,
                                                 redimChunkSize);

    SCIDB_ASSERT(redimArrayIters.size() == destAttrs.size() + 2);
    SCIDB_ASSERT(redimChunkIters.size() == destAttrs.size() + 2);

    timing.logTiming(logger, "[RedimensionArray] PHASE 0 'redimensioned' initialized");

    // PHASE 1 - convert to redimensioned form (but not order)
    PerfTimeScope pts_p1(PTS_REDIM_P1_FILL);

    // Iterate through the input array, generate the output data, and append to the MemArray.
    // Note: For an aggregate field, its source value (in the input array) is used.
    // Note: The synthetic dimension is not handled here. That is, multiple records that will be
    //       differentiated along the synthetic dimension are all appended to the 'redimensioned'
    //       array with the same 'position'.
    //
    size_t iterAttr = 0;    // one of the attributes from the input array that needs to be iterated

    // srcAttrs excludes the empty bitmap
    vector< std::shared_ptr<ConstArrayIterator> > srcArrayIterators(_arena,srcAttrs.size());
    vector< std::shared_ptr<ConstChunkIterator> > srcChunkIterators(_arena,srcAttrs.size());

    // A vector of functors, to be used to get value from an input array and return an int64_t value.
    vector<IntegerCoercion> functorsGetSourceValue(_arena,destDims.size());

    const auto& srcArrayAttrs = srcArray->getArrayDesc().getAttributes();

    // Initialize the source array iters
    for (const auto& dstAttr : destAttrs) {
        size_t j = _attrMapping[dstAttr.getId()];
        if (!isFlipped(j)) {
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            auto mappedAttrJ = srcArrayAttrs.find(safe_static_cast<AttributeID>(j));
            SCIDB_ASSERT(mappedAttrJ != srcArrayAttrs.end());
            srcArrayIterators[j] = srcArray->getConstIterator(*mappedAttrJ);
        }
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        size_t j = _dimMapping[i];
        if (isFlipped(j)) {
            j = turnOff(j, FLIP);
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            auto mappedAttrJ = srcArrayAttrs.find(safe_static_cast<AttributeID>(j));
            SCIDB_ASSERT(mappedAttrJ != srcArrayAttrs.end());
            srcArrayIterators[j] = srcArray->getConstIterator(*mappedAttrJ);

            // TODO:  Ugh.  Use FunctionLibrary::findConverter().  Example in ops/input/ChunkLoader.cpp.
            TypeId tid = srcAttrs.findattr(j).getType();
            if (tid == TID_INT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int8_t>;
            }
            else if (tid == TID_INT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int16_t>;
            }
            else if (tid == TID_INT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int32_t>;
            }
            else if (tid == TID_INT64) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
            }
            else if (tid == TID_UINT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint8_t>;
            }
            else if (tid == TID_UINT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint16_t>;
            }
            else if (tid == TID_UINT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint32_t>;
            }
            else if (tid == TID_UINT64) {
                // Should have caught this already in LogicalRedimension::inferSchema().
                ASSERT_EXCEPTION(false, "In RedimensionCommon::redimensionArray(), cannot safely convert"
                                 " uint64 attribute to int64 dimension.");
            }
            else {
                ASSERT_EXCEPTION(false, "In RedimensionCommon::redimensionArray(), src attr type must be "
                                 "of integer type.");
            }
        }
        else {
            functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
        }
    }
    if (!srcArrayIterators[iterAttr]) {
        // If no src attribute needs to be scanned, open one anyways.
        const auto& srcArrayAttrs = srcArray->getArrayDesc().getAttributes();
        SCIDB_ASSERT(iterAttr == srcArrayAttrs.firstDataAttribute().getId());
        srcArrayIterators[0] = srcArray->getConstIterator(srcArrayAttrs.firstDataAttribute());
    }

    // Start scanning the input -- iterating attribute, then chunk, then position
    // TODO:: pullSG's DeserializedArray requires iterating in order
    // (chunk, attribute, position) which is inefficient.
    // Operators need to be re-written to iterate by chunk, position, attribute
    // to avoid re-determining positions.

    std::shared_ptr<ArrayCoordinatesMapper> arrayCoordinatesMapper =
        std::make_shared<ArrayCoordinatesMapper>(provDims); // == destDims if !autochunk
    std::shared_ptr<ChunkIdMap> arrayChunkIdMap = createChunkIdMap(provDims, _arena);
    size_t redimCount = 0;      // Actually a count of cells written to the 1-D.

    LocalMetrics localMetrics(_schema, synthAutochunked);          // local metrics for autochunking
    Coordinates destPos(destDims.size());                          // in outermost loop to avoid mallocs
    vector<Value> valuesInRedimArray(_arena,destAttrs.size()+2);   // in outermost loop to avoid mallocs

    // This offset vector (if non-empty) will be added to the each
    // destination cell position.  Not implemented for the synthetic
    // dimension.
    Coordinates const& offset = _settings.getOffset();
    SCIDB_ASSERT(!hasSynthetic || offset.empty());
    SCIDB_ASSERT(offset.empty() || offset.size() == destDims.size());

    while (!srcArrayIterators[iterAttr]->end())
    {
        // Initialize src chunk iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                srcChunkIterators[i] = srcArrayIterators[i]->getChunk().getConstIterator();
            }
        }

        // Initialize the dest
        Coordinates chunkPos;

        // Loop through the chunks content
        while (!srcChunkIterators[iterAttr]->end()) {
            Coordinates const& srcPos = srcChunkIterators[iterAttr]->getPosition();

            // Get the destPos for this item -- for the SYNTHETIC dim, use the same value
            // (dimStartSynthetic) for all.
            size_t nDims = destDims.size();
            for (size_t i = 0; i < nDims; i++) {
                size_t j = _dimMapping[i];
                if (isFlipped(j)) {
                    Value const& value = srcChunkIterators[turnOff(j,FLIP)]->getItem();
                    if (value.isNull()) {
                        // a dimension is NULL. Just skip this item.
                        goto ToNextItem;
                    }
                    destPos[i] = (*functorsGetSourceValue[i])(value);
                } else if (j == SYNTHETIC) {
                    destPos[i] = dimStartSynthetic;
                } else {
                    destPos[i] = srcPos[j];
                }
                if (!offset.empty()) {
                    destPos[i] += offset[i];
                }
            }

            // Sanity check, and accumulate local metrics.
            for (size_t i=0; i < nDims; ++i) {
                if (destPos[i]<destDims[i].getStartMin() || destPos[i]>destDims[i].getEndMax()) {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION)
                        << CoordsToStr(destPos);
                }
            }

            chunkPos = destPos;
            _schema.getChunkPositionFor(chunkPos);

            // Build data (except the last two fields, i.e. position/chunkid) to be written
            for (const auto& dstAttr : destAttrs) {
                size_t j = _attrMapping[dstAttr.getId()];
                if ( isFlipped(j) ) { // if flipped from a dim
                    valuesInRedimArray[dstAttr.getId()].setInt64( srcPos[turnOff(j, FLIP)] );
                } else { // from an attribute
                    valuesInRedimArray[dstAttr.getId()] = srcChunkIterators[j]->getItem();
                }
            }

            if (autochunk) {
                localMetrics.accumulate(valuesInRedimArray, destPos);
            }

            // Set the last two fields of the data, and append to the redimensioned array.
            // When autochunking, the overlap regions will be generated later.
            if (hasOverlap && !autochunk) {
                // OverlappingChunksIterator iterates over the logical space.
                // Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
                // here is why it is ok.
                // If chunkOverlap = 0, there is only one chunk in the space so it is ok.
                // With non-zero chunkOverlaps, the space includes only the neighbor chunks that need to store
                // a copy of this record. We have no option but to iterate over all of them.
                //
                OverlappingChunksIterator allChunks(destDims, destPos);
                while (!allChunks.end()) {
                    Coordinates const& overlappingChunkPos = allChunks.getPosition();
                    position_t pos = arrayCoordinatesMapper->coord2pos(overlappingChunkPos, destPos);
                    valuesInRedimArray[destAttrs.size()].setInt64(pos);
                    position_t chunkId = arrayChunkIdMap->mapChunkPosToId(overlappingChunkPos);
                    valuesInRedimArray[destAttrs.size()+1].setInt64(chunkId);
                    appendItemToRedimArray(valuesInRedimArray,
                                           query,
                                           redimArrayIters,
                                           redimChunkIters,
                                           redimCount,
                                           redimChunkSize);

                    // Must increment *after* overlappingChunkPos is no longer needed, because
                    // the increment will clobber overlappingChunkPos.
                    ++allChunks;
                }
            } else {
                position_t pos = arrayCoordinatesMapper->coord2pos(chunkPos, destPos);
                valuesInRedimArray[destAttrs.size()].setInt64(pos);
                position_t chunkId = arrayChunkIdMap->mapChunkPosToId(chunkPos);
                valuesInRedimArray[destAttrs.size()+1].setInt64(chunkId);
                appendItemToRedimArray(valuesInRedimArray,
                                       query,
                                       redimArrayIters,
                                       redimChunkIters,
                                       redimCount,
                                       redimChunkSize);
            }

            // Advance chunk iterators
        ToNextItem:

            for (size_t i = 0; i < srcAttrs.size(); i++) {
                if (srcChunkIterators[i]) {
                    ++(*srcChunkIterators[i]);
                }
            }
        }

        // Advance array iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                ++(*srcArrayIterators[i]);
            }
        }
    } // while

    // If there are leftover values, flush the output iters one last time
    if (redimCount % redimChunkSize != 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); ++i)
        {
            redimChunkIters[i]->flush();
            redimChunkIters[i].reset();
        }
    }
    for (size_t i = 0; i < redimArrayIters.size(); ++i)
    {
        redimArrayIters[i].reset();
    }

    LOG4CXX_DEBUG(logger, "[RedimensionArray] phase 1 end, flushed and reset chunks iterators");

    timing.logTiming(logger, "[RedimensionArray] PHASE 1: conversion to redimensioned form (not order)");

    // If autochunking, we now have our partial <min,max,approxdc> metrics and can do "metric
    // exchange" to compute the final chunk intervals.  From here on, use only finalDims, *never*
    // provDims.
    if (autochunk) {
        try {
            exchangeMetrics(query,
                            phyOp,
                            localMetrics,
                            (hasSynthetic ? dimSynthetic : -1),
                            finalDims);

            // Pretend like we knew them all along!
            _schema.setDimensions(finalDims);
            SCIDB_ASSERT(!_schema.isAutochunked());
        }
        catch (Exception const& ex) {
            LOG4CXX_DEBUG(logger, "Exception from exchangeMetrics: " << ex.what());
            ex.raise();
        }
        catch (std::exception& ex) {
            LOG4CXX_DEBUG(logger, "std::exception from exchangeMetrics: " << ex.what());
            throw;
        }
        LOG4CXX_DEBUG(logger, "Metric exchange yields " << finalDims);
        timing.logTiming(logger, "[RedimensionArray] PHASE 1A: autochunking metric exchange complete");
    }

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned values: ");
    // redimensioned->printArrayToLogger();

    // PHASE 2 - sort "redimensioned" to global order
    PerfTimeScope pts_p2(PTS_REDIM_P2_SORT);

    // drop the source array
    redimChunkIters.clear();
    redimArrayIters.clear();
    srcChunkIterators.clear();
    srcArrayIterators.clear();
    srcArray.reset();

    // If autochunking (and if we have anything to actually work on, i.e. redimCount > 0), here we
    // wrap the 1-D array with a "remapper" that will transform the provisional coordinates into
    // final coordinates on the fly.
    std::shared_ptr<Remapper> remapper;
    if (autochunk && redimCount) {
        remapper.reset(new Remapper(*arrayCoordinatesMapper, arrayChunkIdMap, finalDims, _arena));
        std::shared_ptr<Array> remappedRedim;
        if (hasOverlap) {
            remappedRedim.reset(new OverlapRemapperArray(redimensioned, remapper, query));
        } else {
            remappedRedim.reset(new RemapperArray(redimensioned, remapper, query));
        }
        redimensioned = remappedRedim;
        LOG4CXX_DEBUG(logger, "[RedimensionArray] AUTOCHUNKING REMAPPER in use");
    }

    // Sort the redimensioned array based on the chunkid, followed by the position in the chunk
    //
    SortingAttributeInfos sortingAttributeInfos(2);
    SortingAttributeInfo k;
    k.columnNo = safe_static_cast<int>(destAttrs.size() + 1);
    k.ascent = true;
    sortingAttributeInfos[0] = k;
    k.columnNo = safe_static_cast<int>(destAttrs.size());
    k.ascent = true;
    sortingAttributeInfos[1] = k;

    // chunk size for redimension is critical.  if there are large string attributes, then using
    // too large a chunk size can create chunks that are too big to be copied into or written from
    // BufferMgr.  If so reduce CONFIG_REDIMENSION_CHUNKSIZE
    unique_ptr<SortArray> sorter(new SortArray(redimensioned->getArrayDesc(), _arena));
    sorter->setChunkSize(redimChunkSize);

    std::shared_ptr<TupleComparator> tcomp =
        std::make_shared<TupleComparator>(sortingAttributeInfos,
                                          redimensioned->getArrayDesc());
    if (redimCount)
    {
        std::shared_ptr<MemArray> sortedRedimensioned = sorter->getSortedArray(redimensioned, query, phyOp, tcomp);
        redimensioned = sortedRedimensioned;
    }

    // Sort done, and the resulting array has the final (chunkId,cellPos) pairs.  Extract and use
    // the corresponding ChunkIdMap.
    if (autochunk && redimCount) {
        SCIDB_ASSERT(remapper);
        arrayChunkIdMap = remapper->getFinalChunkIdMap();
        arrayCoordinatesMapper = std::make_shared<ArrayCoordinatesMapper>(finalDims);
        remapper.reset();       // releases provisional ChunkIdMap memory
        LOG4CXX_DEBUG(logger, "[RedimensionArray] AUTOCHUNKING REMAPPER in use, msg 2");
    } else {
        SCIDB_ASSERT(!remapper);
    }

    // reverse the direction of the chuk pos <=> id bijection
    arrayChunkIdMap->reverse();

    timing.logTiming(logger, "[RedimensionArray] PHASE 2A: redimensioned sort pass 1");

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned sorted values: ");
    // redimensioned->printArrayToLogger();

    // If hasSynthetic, each record with the same position get assigned a distinct value in the
    // synthetic dimension, effectively assigning a distinct position to every record.  After
    // updating the redimensioned array, it will need to be re-sorted.
    //
    // [mjl: Seems like we do this work twice: here localling, and again in SyntheticDimChunkMerger.
    // Would be better to do it just once *after* the SG.  Seems like a lot of wasted work,
    // especially since this will involve non-sequential chunk updates.]
    //
    if (hasSynthetic && redimCount) {
        LOG4CXX_DEBUG(logger, "[RedimensionArray] AUTOCHUNKING REMAPPER in use, msg 3");
        PerfTimeScope pts_p2_syn_upd(PTS_REDIM_P2_SYN_UPD); // synthetic update case

        bool updated = updateSyntheticDimForRedimArray(query,
                                                       *arrayCoordinatesMapper,
                                                       *arrayChunkIdMap,
                                                       dimSynthetic,
                                                       redimensioned);
        timing.logTiming(logger, "[RedimensionArray] PHASE 2B: redimensioned updateSynthetic");
        if(updated)
        {
            PerfTimeScope pts_p2_syn_resort(PTS_REDIM_P2_SYN_RESORT);  // updateSynthetic
            // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic before sort: ");
            // redimensioned->printArrayToLogger();

            std::shared_ptr<MemArray> sortedRedimSynthetic = sorter->getSortedArray(redimensioned, query, phyOp, tcomp);
            redimensioned = sortedRedimSynthetic;
        }
        // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic after sort2: ");
        // redimensioned->printArrayToLogger();
    }
    sorter.reset();

    timing.logTiming(logger, "[RedimensionArray] PHASE 2: complete");

    // PHASE 3 - aggregate into 'beforeRedistribution'
    PerfTimeScope pts_p3(PTS_REDIM_P3_TO_BEFORE);

    // Create a MemArray called 'beforeRedistribution'.
    //
    // The schema is adapted from destArrayDesc as follows:
    //    (a) For an aggregate field, the type is the 'State' of the aggregate, rather than the destination field type.
    //
    // The data is computed as follows:
    //    (a) For an aggregate field, the aggregate state, among all records with the same position, is stored.
    //    (b) If !hasAggregate and !hasSynthetic, for duplicates, only one record is kept.
    //
    // Also, the MemArray has the empty tag, regardless to what the input array has.
    //

    Attributes attrsBeforeRedistribution;

    if (hasAggregate) {
        // If there are any aggregates, then pass their attributes through, otherwise use the
        // original destAttrs.
        for (const auto& dstAttr : destAttrs) {
            const auto i = dstAttr.getId();
            if (_aggregates[i]) {
                attrsBeforeRedistribution.push_back(
                    AttributeDesc(dstAttr.getName(),
                                  _aggregates[i]->getStateType().typeId(),
                                  dstAttr.getFlags(),
                                  dstAttr.getDefaultCompressionMethod()));
            } else {
                attrsBeforeRedistribution.push_back(dstAttr);
            }
        }
    } else {
        attrsBeforeRedistribution = destAttrs;
    }

    SCIDB_ASSERT(finalDims == _schema.getDimensions());
    auto attrsSize = attrsBeforeRedistribution.size();
    std::shared_ptr<MemArray> beforeRedistribution =
        // ArrayDesc consumes the new copy, source is used later with the assumption that the
        // empty tag is not present.
       std::make_shared<MemArray>(ArrayDesc(_schema.getName(),
                                            attrsBeforeRedistribution.addEmptyTagAttribute(),
                                            _schema.getDimensions(),
                                            createDistribution(dtUndefined),
                                            _schema.getResidency()
                                            ),
                                  query);

    // Write data from the 'redimensioned' array to the 'beforeRedistribution' array
    //

    // Initialize iterators
    //
    vector<std::shared_ptr<ArrayIterator> > arrayItersBeforeRedistribution(_arena,attrsSize);
    vector<std::shared_ptr<ChunkIterator> > chunkItersBeforeRedistribution(_arena,attrsSize);
    OnScopeExit onExitBeforeRedistribution([this, &chunkItersBeforeRedistribution] () {
        unpinAllChunks(chunkItersBeforeRedistribution, logger);
    });

    // destAttrs doesn't include the empty bitmap
    for (const auto& attr : destAttrs)
    {
        arrayItersBeforeRedistribution[attr.getId()] = beforeRedistribution->getIterator(attr);
    }
    vector< std::shared_ptr<ConstArrayIterator> > redimArrayConstIters(_arena,destAttrs.size() + 2);
    vector< std::shared_ptr<ConstChunkIterator> > redimChunkConstIters(_arena,destAttrs.size() + 2);
    const auto& redimAttrs = redimensioned->getArrayDesc().getAttributes(true);
    for (const auto& attr : redimAttrs)
    {
        redimArrayConstIters[attr.getId()] = redimensioned->getConstIterator(attr);
    }

    // Initialize current chunk id to a value that is never in the map
    //
    size_t chunkIdAttr = redimArrayConstIters.size() - 1;
    size_t positionAttr = redimArrayConstIters.size() - 2;
    size_t nDestDims = _schema.getDimensions().size();
    size_t chunkId = arrayChunkIdMap->getUnusedId();

    // Coordinates outside of loops to reduce number of mallocs
    Coordinates lows(nDestDims);
    Coordinates intervals(nDestDims);
    Coordinates tmp(nDestDims);
    Coordinates outputCoord(nDestDims);

    // Init state vector and prev position
    StateVector stateVector(_arena, _aggregates);
    position_t prevPosition = -1;

    // Scan through the items, aggregate (if apply), and write to the MemArray.
    //
    vector<Value> destItem(_arena,destAttrs.size());  // moved outside inner loop to avoid repeated malloc
    const auto& fda = redimensioned->getArrayDesc().getAttributes().firstDataAttribute();
    while (!redimArrayConstIters[fda.getId()]->end())
    {
        // Set up chunk iters for the input chunk
        for (const auto& attr : redimAttrs)
        {
            redimChunkConstIters[attr.getId()] = redimArrayConstIters[attr.getId()]->getChunk().getConstIterator();
        }

        while (!redimChunkConstIters[fda.getId()]->end())
        {
            // Have we found a new output chunk?
            //
            SCIDB_ASSERT(redimensioned->getArrayDesc().getAttributes().hasAttribute((AttributeID)chunkIdAttr));
            size_t nextChunkId = redimChunkConstIters[chunkIdAttr]->getItem().getInt64();
            if (chunkId != nextChunkId)
            {
                // Write the left-over stateVector
                //
                appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // Flush current output iters
                //
                for (size_t i = 0; i < destAttrs.size(); ++i)
                {
                    if (chunkItersBeforeRedistribution[i])
                    {
                        chunkItersBeforeRedistribution[i]->flush();
                        chunkItersBeforeRedistribution[i].reset();
                    }
                }

                // Init the coordinate mapper for the new chunk
                //
                chunkId = nextChunkId;
                arrayCoordinatesMapper->chunkPos2LowsAndIntervals(arrayChunkIdMap->mapIdToChunkPos(chunkId),
                                                                  lows,
                                                                  intervals);

                // Create new chunks and get the iterators.
                // The first non-empty-tag attribute does NOT use NO_EMPTY_CHECK (so as to help
                // take care of the empty tag); others do.
                //
                int iterMode = ConstChunkIterator::SEQUENTIAL_WRITE;
                for (size_t i=0; i<destAttrs.size(); ++i)
                {
                    if (arrayItersBeforeRedistribution[i]) {
                        CoordinateCRange pos = arrayChunkIdMap->mapIdToChunkPos(chunkId);
                        tmp.assign(pos.begin(),pos.end());

                        Chunk& chunk = arrayItersBeforeRedistribution[i]->newChunk(tmp);
                        chunkItersBeforeRedistribution[i] = chunk.getIterator(query, iterMode);
                        iterMode |= ConstChunkIterator::NO_EMPTY_CHECK;
                    }
                }

                // Update prevPosition, reset state vector
                //
                prevPosition = -1;
                stateVector.init();
            }

            // When seeing the first item with a new position, the attribute values in the item are
            // populated into the destItem as follows.
            //  - For a scalar field, the value is copied.
            //  - For an aggregate field, the value is initialized and accumulated.
            //
            // When seeing subsequent items with the same position, the attribute values in the item
            // are populated as follows.
            //  - For a scalar field, the value is ignored (just select the first item).
            //  - For an aggregate field, the value is accumulated.
            //
            for (size_t i = 0; i < destAttrs.size(); ++i)
            {
                destItem[i] = redimChunkConstIters[i]->getItem();
            }

            position_t currPosition = redimChunkConstIters[positionAttr]->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                if (!hasAggregate) {
                    if (redistributeMode==VALIDATED) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_DATA_COLLISION)
                                                                    << CoordsToStr(outputCoord);
                    }
                    if (!_hasDataIntegrityIssue && logger->isWarnEnabled()) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_WARN(logger, "RedimensionCommon::redimensionArray: "
                                     << "Data collision is detected at cell position "
                                     << CoordsToStr(outputCoord)
                                     <<  " for attribute ID = " << positionAttr
                                     << ". Add log4j.logger.scidb.array.RedimensionCommon=TRACE to the log4cxx config file for more");
                        _hasDataIntegrityIssue=true;
                    } else if (_hasDataIntegrityIssue && logger->isTraceEnabled()) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_TRACE(logger, "RedimensionCommon::redimensionArray: "
                                      << "Data collision is detected at cell position "
                                      << CoordsToStr(outputCoord)
                                      <<  " for attribute ID = " << positionAttr);
                    }
                }
                stateVector.accumulate(destItem);
            }
            else
            {
                // Output the previous state vector.
                appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // record the new prevPosition
                prevPosition = currPosition;

                // Init and accumulate with the current item.
                stateVector.init();
                stateVector.accumulate(destItem);
            }

            // Advance chunk iterators
            for (size_t i = 0; i < redimChunkConstIters.size(); ++i)
            {
                ++(*redimChunkConstIters[i]);
            }
        } // while chunk iterator

        // Advance array iterators
        for (size_t i = 0; i < redimArrayConstIters.size(); ++i)
        {
            ++(*redimArrayConstIters[i]);
        }
    } // while array iterator

    arrayChunkIdMap->clear(); // ok, we're done with this now - release memory

    // Flush the leftover statevector
    appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                     lows, intervals, tmp,
                                     prevPosition,
                                     chunkItersBeforeRedistribution,
                                     stateVector);

    // Flush the chunks one last time
    for (size_t i=0; i<destAttrs.size(); ++i)
    {
        if (chunkItersBeforeRedistribution[i])
        {
            chunkItersBeforeRedistribution[i]->flush();
        }
        chunkItersBeforeRedistribution[i].reset();
    }

    for (size_t i=0; i<destAttrs.size(); ++i) {
        arrayItersBeforeRedistribution[i].reset();
        chunkItersBeforeRedistribution[i].reset();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 3: [aggregate] and build 'BeforeRedistribution'");

    // PHASE 4 redistribute

    // drop redimensioned
    redimChunkConstIters.clear();
    redimArrayConstIters.clear();
    redimensioned.reset();

    if( !hasAggregate && redistributeMode!=AGGREGATED) {
        SCIDB_ASSERT(!hasSynthetic);
        SCIDB_ASSERT(_schema.getResidency()->isEqual(beforeRedistribution->getArrayDesc().getResidency()));
        SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(beforeRedistribution->getArrayDesc().getDistribution())
                     || bool(findKeyword("_append")));  // No problem, _append_helper() will do an explicit SG.

        // return without redistributing : optimizer will have to insert SG
        timing.logTiming(logger, "[RedimensionArray] PHASE 4 w/out redist (not agg., not synth.), early return");
        return beforeRedistribution;
    }
    SCIDB_ASSERT(redistributeMode!=VALIDATED);

    ArrayDesc outSchema(_schema.getName(),
                        _schema.getAttributes(),
                        _schema.getDimensions(),
                        _schema.getDistribution(),
                        _schema.getResidency());

    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

    std::shared_ptr<Array> afterRedistribution;

    if(hasSynthetic) {
        PerfTimeScope pts_p3a(PTS_REDIM_P4_REDIST_SYN);
        LOG4CXX_DEBUG(logger, "[RedimensionArray] phase 4 redistributeWithSynthetic()");
        SyntheticDimChunkMerger::RedimInfo redimInfo(hasSynthetic,
                                                     safe_static_cast<AttributeID>(dimSynthetic),
                                                     finalDims[dimSynthetic]);
        std::shared_ptr<Array> input(beforeRedistribution);
        afterRedistribution = redistributeWithSynthetic(input, query, phyOp, &redimInfo);
    } else {
        LOG4CXX_DEBUG(logger, "[RedimensionArray] phase 4 redistributeWithAggregate()");
        PerfTimeScope pts_p3b(PTS_REDIM_P4_REDIST_AGG);
        const bool enforceDataIntegrity = (redistributeMode == VALIDATED);
        SCIDB_ASSERT(!enforceDataIntegrity);
        std::shared_ptr<Array> input(beforeRedistribution);

        afterRedistribution = redistributeWithAggregates(input,
                                                         outSchema,
                                                         query,
                                                         phyOp,
                                                         enforceDataIntegrity,
                                                         hasOverlap,
                                                         _aggregates);
    }

    // drop beforeRedistribution
    chunkItersBeforeRedistribution.clear();
    arrayItersBeforeRedistribution.clear();
    beforeRedistribution.reset();

    timing.logTiming(logger, "[RedimensionArray] PHASE 4: redistribution: full redistribution");

    assert(outSchema == afterRedistribution->getArrayDesc());

    return afterRedistribution;
}

std::shared_ptr<Array>
RedimensionCommon::redistributeWithSynthetic(std::shared_ptr<Array>& inputArray,
                                             const std::shared_ptr<Query>& query,
                                             const std::shared_ptr<PhysicalOperator>& phyOp,
                                             const SyntheticDimChunkMerger::RedimInfo* redimInfo)
{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    size_t numAttrs = desc.getAttributes().size();
    PartialChunkMergerList chunkMergers(numAttrs);
    for (AttributeID a=0; a < numAttrs; ++a) {
        std::shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
        std::make_shared<SyntheticDimChunkMerger>(redimInfo, query->getInstancesCount());
        chunkMergers[a] = merger;
    }

    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

    // regardless of user settings there should be no data collisions with a synthetic dimension
    const bool enforceDataIntegrity = true;
    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));
    return redistributeToRandomAccess(inputArray,
                                      _schema.getDistribution(),
                                      ArrayResPtr(), //default query residency
                                      query,
                                      phyOp,
                                      chunkMergers,
                                      enforceDataIntegrity );
}

/**
 * A partial chunk merger which uses an aggregate function to form a complete chunk.
 * It expects the partial chunks to contain aggreagte state values suitable for using with the Aggregate methods.
 */
class FinalAggregateChunkMerger : public AggregateChunkMerger
{
    const bool _hasOverlap;
    const ArrayDesc* _desc;
public:
    /// Constructor
    FinalAggregateChunkMerger(AggregatePtr const& agg,
                              const ArrayDesc*  desc,
                              bool isEmptyable,
                              bool hasOverlap)
    : AggregateChunkMerger(agg, isEmptyable), _hasOverlap(hasOverlap), _desc(desc) { assert(desc); }

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<MemChunk> chunk = AggregateChunkMerger::getMergedChunk(attId, query);
        std::shared_ptr<MemChunk> finalChunk = std::make_shared<MemChunk>();

        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalAggregateChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        Address addr(chunk->getAttributeDesc().getId(), chunk->getFirstPosition(false));
        finalChunk->initialize(&chunk->getArray(),
                   _desc,
                   addr,
                   chunk->getCompressionMethod());

        // src chunk
        std::shared_ptr<ConstChunkIterator> src =
            chunk->getConstIterator(ConstChunkIterator::DEFAULT);

        const size_t ebmSize(chunk->getBitmapSize());
        if (ebmSize>0) {
            const size_t off = chunk->getSize() - ebmSize;
            std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap =
               std::make_shared<ConstRLEEmptyBitmap>(
                   reinterpret_cast<const char*>(chunk->getConstData()) + off);
            finalChunk->setEmptyBitmap(emptyBitmap);
        } else {
            ASSERT_EXCEPTION(false, "Merged chunk has no emptybitmap");
            // Technically, it is not a problem if finalChunk is not a "closure" (i.e. emptybitmap is not appended to the data)
            // and the result of redistribute(redimension()) is materialized (into a MemArray).
            // However, we would like it to also work without full array materialization.
        }

        // dest chunk
        int destMode = ConstChunkIterator::SEQUENTIAL_WRITE |
                       ConstChunkIterator::NO_EMPTY_CHECK |
                       ConstChunkIterator::APPEND_EMPTY_BITMAP;
        std::shared_ptr<ChunkIterator> dst = finalChunk->getIterator(query, destMode);

        // copy
        Value result;
        size_t count = 0;
        while (!src->end()) {
            ++ count;
            Coordinates const& destPos = src->getPosition();
            bool rc = dst->setPosition(destPos);
            SCIDB_ASSERT(rc);
            _aggregate->finalResult(result, src->getItem());
            dst->writeItem(result);
            ++(*src);
        }
        src.reset();
        dst->flush();
        dst.reset();

        finalChunk->setEmptyBitmap(std::shared_ptr<ConstRLEEmptyBitmap>());
        finalChunk->setBitmapChunk(NULL);
        assert(finalChunk->getBitmapSize()>0);

        if (!_hasOverlap) { // the count should not include overlapped items; just leave as 0.
            finalChunk->setCount(count);
        } else {
            finalChunk->setCount(0);
        }
        return finalChunk;
    }
};

class FinalETChunkMerger : public MultiStreamArray::DefaultChunkMerger
{
    const ArrayDesc *_desc;
public:
    /// Constructor
    FinalETChunkMerger(const ArrayDesc* desc,
                       bool enforceDataIntegrity)
    : DefaultChunkMerger(enforceDataIntegrity), _desc(desc) { assert(desc); }

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const std::shared_ptr<Query>& query)
    {
        // TODO references ebm relative to size()
        assert(attId == _desc->getAttributes().size()-1);
        std::shared_ptr<MemChunk> chunk = MultiStreamArray::DefaultChunkMerger::getMergedChunk(attId, query);
        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalETChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        chunk->setArrayDesc(_desc);
        return chunk;
    }
};

std::shared_ptr<Array>
RedimensionCommon::redistributeWithAggregates(std::shared_ptr<Array>& inputArray,
                                              ArrayDesc const& outSchema,
                                              const std::shared_ptr<Query>& query,
                                              const std::shared_ptr<PhysicalOperator>& phyOp,
                                              bool enforceDataIntegrity,
                                              bool hasOverlap,
                                              PointerRange<const AggregatePtr> aggregates)

{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    const size_t numAttrs = desc.getAttributes().size();
    assert(numAttrs == (aggregates.size()+1));
    const bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);

    PartialChunkMergerList chunkMergers(numAttrs);

    std::shared_ptr<Array> withAggregatesArray = std::make_shared<MemArray>(outSchema,query);

    for (AttributeID a=0; a < (numAttrs-1); ++a) {
        if (aggregates[a]) {
            std::shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
                std::make_shared<FinalAggregateChunkMerger>(aggregates[a], &outSchema, isEmptyable, hasOverlap);
            chunkMergers[a] = merger;
        }
    }
    assert(chunkMergers.size() == numAttrs);
    assert(!chunkMergers[numAttrs-1]);
    chunkMergers[numAttrs-1] = std::make_shared<FinalETChunkMerger>(&outSchema, enforceDataIntegrity);

    redistributeToArray(inputArray,
                        withAggregatesArray,
                        chunkMergers,
                        NULL,
                        query,
                        phyOp,
                        enforceDataIntegrity );
     return withAggregatesArray;
}

/**
 * If any @c _schema dimension has an unspecified chunk interval, set up @c provDims with
 * provisional chunk intervals.
 *
 * Per wiki:Development/components/Rearrange_Ops/RedimWithAutoChunkingHLD, here we detect whether we
 * are doing autochunking, and if so we "make up from thin air" provisional chunk intervals to
 * (temporarily) use in place of the unspecified ones.  These are stored in @c provDims .
 *
 * @return true iff we saw an unspecified interval (i.e. we are in autochunking mode)
 */
bool RedimensionCommon::makeProvisionalChunking(bool hasSynth, size_t synthIndex, Dimensions& provDims)
{
    provDims.clear();

    // In this first pass, determine the product of the fixed chunk
    // intervals, so we know how much "room" we have to work with in
    // picking the provisional intervals.  (While theoretically any
    // provisional interval value would do, in practice the product of
    // all intervals---the logical chunk size--- has to fit inside a
    // signed 64-bit number, that is, inside a Coordinate!)
    //
    Coordinate const MAX_LOGICAL_CHUNK = std::numeric_limits<Coordinate>::max();
    bool needSynthInterval = false;
    int numAutochunked = 0;
    Coordinate logicalChunkSize = 1;
    ssize_t lastAcDim = -1;     // last non-synthetic autochunked dimension
    Dimensions const& destDims = _schema.getDimensions();
    for (size_t i = 0, n = destDims.size(); i < n; ++i) {
        DimensionDesc const& dim = destDims[i];
        int64_t chunkInterval = dim.getRawChunkInterval();
        if (chunkInterval == DimensionDesc::AUTOCHUNKED) {
            ++numAutochunked;
            if (provDims.empty()) {
                // Late initialization, only do it if we must.
                provDims = destDims;
            }
            if (hasSynth && i == synthIndex) {
                // It's autochunked, need to estimate it.
                needSynthInterval = true;
            } else {
                // Remember the last non-synthetic autochunked
                // dimension, in case we need to adjust it below.
                lastAcDim = i;
            }
        } else {
            if (logicalChunkSize >= (MAX_LOGICAL_CHUNK / chunkInterval)) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR,
                                     SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
            }
            logicalChunkSize *= chunkInterval;
        }
    }

    // If there were no autochunked (that is, unspecified) intervals, we
    // need not make any provisional guesses.
    //
    if (!numAutochunked) {
        SCIDB_ASSERT(provDims.empty());
        return false;
    }
    SCIDB_ASSERT(!provDims.empty());

    // Arbitrary, so keep it simple: about one million.  Make it look
    // funny for easy identification while debugging.
    //
    int64_t const MAX_PROVISIONAL_INTERVAL = 1012101;

    // Divide the remaining logical chunk space evenly among the
    // autochunked dimensions (but cap it at MAX_PROV...).
    //
    int64_t provInterval = MAX_LOGICAL_CHUNK / logicalChunkSize;
    int numNaturalDims = numAutochunked - int(needSynthInterval);
    if (numNaturalDims > 1) {
        provInterval = static_cast<int64_t>(
            ::floor(::pow(static_cast<double>(provInterval),
                          1.0 / numNaturalDims)));
    }
    provInterval = ::max(1L, ::min(provInterval, MAX_PROVISIONAL_INTERVAL));

    // What's the logical chunk size now?
    for (int i = 0; i < numNaturalDims; ++i) {
        logicalChunkSize *= provInterval;

        // Since provInterval is (1/N)-th the remaining chunk volume,
        // we can safely multiply it out by N without an overflow.
        SCIDB_ASSERT(logicalChunkSize > 0);
    }
    SCIDB_ASSERT(logicalChunkSize <= MAX_LOGICAL_CHUNK);

    // Picking an appropriate synthetic chunk interval is REALLY NOT
    // POSSIBLE without examining the data.  But still we try to make a
    // guess, adjusting the last "natural" autochunked dimension to
    // compensate if we have to.  If this fails, the user will just have
    // to explicitly pick something.
    //
    int64_t const MIN_SYNTHETIC_INTERVAL = 16;
    int64_t synthInterval = 128;
    int64_t lastAcInterval = provInterval;
    if (needSynthInterval) {
        // First try shortening the synthetic interval.
        // While synthInterval is too big...
        while (logicalChunkSize >= (MAX_LOGICAL_CHUNK / synthInterval)
               && synthInterval >= MIN_SYNTHETIC_INTERVAL) {
            synthInterval >>= 1;
        }
        if (synthInterval < MIN_SYNTHETIC_INTERVAL) {
            // Synthetic interval is as short as we'll let it go.
            // Adjust a non-synthetic autochunked interval if we can.
            synthInterval = MIN_SYNTHETIC_INTERVAL;
            if (lastAcDim < 0
                || (lastAcInterval / MIN_SYNTHETIC_INTERVAL) == 0)
            {
                // Can't adjust lastAcInterval?  I give up!!!  User
                // needs to divide one of the fixed chunk intervals by
                // MIN_SYNTHETIC_INTERVAL if they want to autochunk the
                // synthetic dimension.
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR,
                                     SCIDB_LE_OP_REDIMENSION_SYNTH_INTERVAL);
            }
            lastAcInterval /= MIN_SYNTHETIC_INTERVAL;
        }
    }

    // Finally we can set the provisional values into provDims.
    for (size_t i = 0, n = destDims.size(); i < n; ++i) {
        if (destDims[i].isAutochunked()) {
            if (hasSynth && synthIndex == i) {
                provDims[i].setChunkInterval(synthInterval);
            } else if (lastAcDim > -1 && static_cast<size_t>(lastAcDim) == i) {
                provDims[i].setChunkInterval(lastAcInterval);
            } else {
                provDims[i].setChunkInterval(provInterval);
            }
        }
    }

    // Got some provisional dimensions for you.
    return true;
}


/**
 *  Exchange and aggregate locally gathered shape metrics to obtain cluster-wide metrics.
 *
 *  @description We have seen the full coordinates of every locally produced output cell, and
 *  gathered metrics along each dimension: min, max, and approximate distinct count.  We also have
 *  local values for distinct count across *all* dimensions and an estimate of collisions along the
 *  synthetic dimension (if any).  Now we replicate these local findings, using the aggregation
 *  facility of SG, to come up with global values for these metrics.  The global metrics will be fed
 *  to the ChunkEstimator to arrive at reasonable, automatically computed chunk interval values.
 */
void RedimensionCommon::exchangeMetrics(std::shared_ptr<Query> const& query,
                                        std::shared_ptr<PhysicalOperator> const& phyOp,
                                        LocalMetrics& localMetrics,
                                        ssize_t synthDim,
                                        Dimensions& inOutDims)
{
    size_t const N_DEST_DIMS = _schema.getDimensions().size();
    SCIDB_ASSERT(localMetrics.dimCount() == N_DEST_DIMS);
    SCIDB_ASSERT(inOutDims.size() == N_DEST_DIMS);

    if (isDebug()) {
        // operator<< is expensive for LocalMetrics objects, so guard this with isDebug().
        LOG4CXX_DEBUG(logger, "Local redimension metrics: " << localMetrics);
    }

    // Spread those metrics around!
    localMetrics.redistribute(query, phyOp, _schema.getDistribution(), _schema.getResidency());

    if (isDebug()) {
        LOG4CXX_DEBUG(logger, "Global redimension metrics: " << localMetrics);
    }

    // Shove the global metric values into a chunk estimator.
    ChunkEstimator estimator(inOutDims);
    estimator.setLogger(logger)
        .setOverallDistinct(localMetrics.getOverallDistinct())
        .setBigAttributeSize(localMetrics.getLargestAttrAvgSize());
    for (size_t dim = 0; dim < N_DEST_DIMS; ++dim) {
        ChunkEstimator::Statistics stats;
        stats[ChunkEstimator::minimum].setInt64(localMetrics.getDimMin(dim));
        stats[ChunkEstimator::maximum].setInt64(localMetrics.getDimMax(dim));
        stats[ChunkEstimator::distinct].setUint64(localMetrics.getDimDistinctCount(dim));
        estimator.addStatistics(stats);
    }

    if (synthDim >= 0) {
        estimator.setSyntheticInterval(synthDim,
                                       localMetrics.getNumCollisions(),
                                       _settings.getCollisionRatio());
    }

    // Set cells-per-chunk or mibs-per-chunk goal, if given.
    if (_settings.getPhysChunkSize()) {
        estimator.setTargetMibCount(_settings.getPhysChunkSize());
    } else if (_settings.getCellsPerChunk()) {
        estimator.setTargetCellCount(_settings.getCellsPerChunk());
    }

    // Finally, estimate the chunk size(s).
    estimator.go();
}

} // namespace scidb
