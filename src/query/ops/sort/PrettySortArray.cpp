/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2014-2019 SciDB, Inc.
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
 * @file PrettySortArray.cpp
 * @author Mike Leibensperger
 */

#include "PrettySortArray.h"

#include <array/ArrayDistributionInterface.h> // for dtUndefined
#include <array/MemArray.h>
#include <array/Metadata.h>
#include <query/Query.h>

#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger ::getLogger("scidb.query.ops.sort"));
}

namespace scidb
{

PrettySortArray::PrettySortArray(ArrayDesc const& schema,
                                 std::shared_ptr<MemArray>& sortedArray,
                                 Dimensions const& originalDims,
                                 std::shared_ptr<Query> const& query)
    : SinglePassArray(schema)
    , _N_OUT_ATTRS(schema.getAttributes(/*exclude:*/false).size())
    , _OUT_EBM_ATTR(schema.getAttributes().getEmptyBitmapAttribute()->getId())
    , _PREFIX_COUNT(originalDims.size())
    , _N_IN_ATTRS(sortedArray->getArrayDesc().getAttributes(/*exclude:*/false).size())
    , _IN_EBM_ATTR(sortedArray->getArrayDesc().getAttributes().getEmptyBitmapAttribute()->getId())
    , _CELLPOS_ATTR(_IN_EBM_ATTR - 1)  // DJG this will be utterly broken when EBM is first
    , _CHUNKPOS_ATTR(_CELLPOS_ATTR - 1)  // DJG this will be utterly broken when EBM is first
    , _mapper(originalDims)
    , _arrayIters(_N_IN_ATTRS)
    , _chunks(_N_OUT_ATTRS * _HISTORY_SIZE)
{
    _query = query; // XXX HORROR: protected data member in Array base class

    pushPipe(sortedArray);
    SCIDB_ASSERT(getPipeCount() == 1);

    // These assertions are by way of checking that the sortedArray is
    // indeed SortArray-generated using the preservePositions option.
    // There should be two trailing int64 attributes, and we should
    // have a good _PREFIX_COUNT.

    if (isDebug()) {
        SCIDB_ASSERT(_PREFIX_COUNT > 0);
        SCIDB_ASSERT(_PREFIX_COUNT == (_N_OUT_ATTRS - 1) - _CHUNKPOS_ATTR);

        Attributes const& inAttrs =
            sortedArray->getArrayDesc().getAttributes(/*exclude:*/false);
        SCIDB_ASSERT(inAttrs.size() > 3);
        SCIDB_ASSERT(inAttrs.hasEmptyIndicator());
        SCIDB_ASSERT(inAttrs.findattr(inAttrs.size()-2).getType() == TID_INT64); // $cell_pos
        SCIDB_ASSERT(inAttrs.findattr(inAttrs.size()-3).getType() == TID_INT64); // $chunk_pos

        // Result schema must have right count of int64 "prefix" attributes.
        Attributes const& outAttrs = schema.getAttributes(/*exclude:*/true);
        for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
            SCIDB_ASSERT(outAttrs.findattr(i).getType() == TID_INT64);
        }
    }

    // Ensure horizontal access to avoid complications: we have N
    // dimension-as-attribute output attributes that depend on two
    // input attributes $chunk_pos and $cell_pos, so we can't let our
    // iterators get out of step.
    setEnforceHorizontalIteration(true);

    for (const auto& attr : sortedArray->getArrayDesc().getAttributes()) {
        _arrayIters[attr.getId()] = sortedArray->getConstIterator(attr);
    }

    LOG4CXX_TRACE(logger, __FUNCTION__ << " created with "
                  << _PREFIX_COUNT << " prefix dims-as-attrs, "
                  << _N_IN_ATTRS << " sorted input attrs (w/ EBM), "
                  << _N_OUT_ATTRS << " output attrs");
}


PrettySortArray::~PrettySortArray()
{
    LOG4CXX_TRACE(logger, "PrettySortArray destructor");
}


std::shared_ptr<MemArray> PrettySortArray::_sortedArray()
{
    return dynamic_pointer_cast<MemArray>(getPipe(0));
}


ArrayDesc PrettySortArray::makeOutputSchema(ArrayDesc const& inputSchema,
                                            string const& dimName,
                                            size_t chunkSize)
{
    assert(!dimName.empty());

    if (chunkSize == 0) {
        Config& cfg = *Config::getInstance();
        chunkSize = cfg.getOption<size_t>(CONFIG_TARGET_CELLS_PER_CHUNK);
    }
    assert(chunkSize);

    Dimensions const& inDims = inputSchema.getDimensions();
    assert(!inDims.empty());

    // Prefixed attributes, one for each input dimension.
    Attributes attrs;
    for (auto const& dim : inDims) {
        attrs.push_back(
            AttributeDesc(dim.getBaseName(),
                          TID_INT64,
                          0, // flags
                          CompressorType::NONE));
    }

    // Copy the genuine input attributes.
    Attributes const& inAttrs = inputSchema.getAttributes(/*exclude:*/true);
    for (size_t i = 0; i < inAttrs.size(); ++i) {
        auto const& inAttr = inAttrs.findattr(i);
        attrs.push_back(
            AttributeDesc(inAttr.getName(),
                          inAttr.getType(),
                          inAttr.getFlags(),
                          inAttr.getDefaultCompressionMethod(),
                          inAttr.getAliases(),
                          inAttr.getReserve(),
                          &inAttr.getDefaultValue(),
                          inAttr.getDefaultValueExpr(),
                          inAttr.getVarSize()));
    }

    Dimensions dims(1, DimensionDesc(dimName, 0, 0,
                                     CoordinateBounds::getMax(),
                                     CoordinateBounds::getMax(),
                                     chunkSize, 0));

    // ArrayDesc consumes the new copy, source is discarded.
    return ArrayDesc(inputSchema.getName(),
                     attrs.addEmptyTagAttribute(),
                     dims,
                     createDistribution(dtUndefined),
                     inputSchema.getResidency());
}


MemChunk& PrettySortArray::_chunkRef(AttributeID attrId, size_t row)
{
    size_t i = size_t(attrId) + (row % _HISTORY_SIZE) * _N_OUT_ATTRS;
    assert(i < _chunks.size());
    LOG4CXX_TRACE(logger, "_chunkRef(" << attrId
                  << ", " << row << ") --> [" << i << "] " << &_chunks[i]);
    return _chunks[i];
}


bool PrettySortArray::moveNext(size_t rowIdx)
{
    LOG4CXX_TRACE(logger, __FUNCTION__ << '(' << rowIdx
                  << "): from " << _currRowIndex);

    Query::getValidQueryPtr(_query);

    // A step too far?
    if (rowIdx > _currRowIndex + 1) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }

    // Already been here?
    if (rowIdx <= _currRowIndex) {
        // Assert we are within the history window.
        assert(rowIdx + _HISTORY_SIZE > _currRowIndex);
        LOG4CXX_TRACE(logger, __FUNCTION__ << ": Want " << rowIdx <<
                      ", at " << _currRowIndex << ", return true");
        return true;
    }

    // Going to try to really move forward!
    assert(rowIdx == _currRowIndex + 1);

    // But if we're at end, we can't.
    const auto& fda = _sortedArray()->getArrayDesc().getAttributes().firstDataAttribute();
    if (_arrayIters[fda.getId()]->end()) {
        LOG4CXX_TRACE(logger, __FUNCTION__ << ": Already at end()");
        return false;
    }

    // Pull next row of chunks!
    if (_currRowIndex) {
        LOG4CXX_TRACE(logger, __FUNCTION__ << ": Bump sub-iterators");
        for (auto& it : _arrayIters) {
            ++(*it);
        }

        if (_arrayIters[fda.getId()]->end()) {
            LOG4CXX_TRACE(logger, __FUNCTION__ << ": Reached end()");
            return false;
        }

    } // else first time, so don't move.  This fencepost error is an
      // integral part of current Array API "design", mumble.

    // Reached next row and not at end(), hooray!
    ++_currRowIndex;
    _makePrefixChunks();
    _makePassThruChunks();
    LOG4CXX_TRACE(logger, __FUNCTION__
                  << ": Made the chunks for row " << _currRowIndex);

    return true;
}


void PrettySortArray::_makePrefixChunks()
{
    // Make the prefix dimension-as-attribute chunks and corresponding
    // empty bitmap.

    LOG4CXX_TRACE(logger, __FUNCTION__
                  << ": Entered, _currRowIndex=" << _currRowIndex);

    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));

    // Initialize this row's prefix MemChunks (plus the empty bitmap chunk).
    vector<std::shared_ptr<ChunkIterator>> outChunkIters(_PREFIX_COUNT + 1);

    auto initChunk = [this, &outChunkIters, &query](AttributeID i) {
        Address addr;
        const auto& fda = _sortedArray()->getArrayDesc().getAttributes().firstDataAttribute();
        addr.coords = _arrayIters[fda.getId()]->getPosition();
        addr.attId = i;
        MemChunk& mc = _chunkRef(i, _currRowIndex);
        mc.initialize(this, &getArrayDesc(), addr,
                      getArrayDesc().getAttributes().findattr(i).getDefaultCompressionMethod());
        int const FLAGS =
            ChunkIterator::NO_EMPTY_CHECK |
            ChunkIterator::SEQUENTIAL_WRITE;
        if (!mc.getAttributeDesc().isEmptyIndicator()) {
            outChunkIters[i] = mc.getIterator(query, FLAGS);
        } else {
            outChunkIters.back() = mc.getIterator(query, FLAGS); // Empty bitmap
        }
        LOG4CXX_TRACE(logger, "MemChunk at " << &mc << " for attrId " << i);
    };

    for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
        initChunk(AttributeID(i));
    }
    initChunk(_OUT_EBM_ATTR);

    // Get read iterators for $chunk_pos and $cell_pos input chunks.
    vector<std::shared_ptr<ConstChunkIterator>> inChunkIters {
        _arrayIters[_CHUNKPOS_ATTR]->getChunk().getConstIterator(),
        _arrayIters[_CELLPOS_ATTR]->getChunk().getConstIterator(),
    };

    Value trueValue;
    trueValue.setBool(true);

    // Write the new chunks.
    Value v;
    Coordinates chunkPos(_PREFIX_COUNT);
    Coordinates cellPos(_PREFIX_COUNT);
    while (!inChunkIters[0]->end()) {

        // Derive coordinates from $chunk_pos and $cell_pos.
        _mapper.pos2chunkPos(inChunkIters[0]->getItem().getInt64(), chunkPos);
        _mapper.pos2coord(chunkPos, inChunkIters[1]->getItem().getInt64(), cellPos);

        // Write coordinates into corresponding output chunks.
        for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
            v.setInt64(cellPos[i]);
            outChunkIters[i]->writeItem(v);
        }
        // TODO, do we need a container for the chunk iterators so that we can address
        // the chunk iterators in a way that's consistent with how the attributes
        // are addressed?
        outChunkIters.back()->writeItem(trueValue); // empty bitmap

        // Increment!
        ++(*inChunkIters[0]);
        ++(*inChunkIters[1]);
        for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
            ++(*outChunkIters[i]);
        }
        ++(*outChunkIters.back());
    }

    // Close all chunks.
    for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
        outChunkIters[i]->flush();
        outChunkIters[i].reset();
    }
    outChunkIters.back()->flush();
    outChunkIters.back().reset();

    // Set empty bitmap chunk for prefix chunks.
    MemChunk& ebmChunk = _chunkRef(_OUT_EBM_ATTR, _currRowIndex);
    for (size_t i = 0; i < _PREFIX_COUNT; ++i) {
        AttributeID attrId = AttributeID(i);
        auto& mc = _chunkRef(attrId, _currRowIndex);
        mc.setBitmapChunk(&ebmChunk);
    }
}


void PrettySortArray::_makePassThruChunks()
{
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));

    // Number of genuine (pass-thru) attributes...
    size_t const N_TRUE_ATTRS = _OUT_EBM_ATTR - _PREFIX_COUNT;

    // Set up chunk iterators.
    Address addr;
    const auto& fda = _sortedArray()->getArrayDesc().getAttributes().firstDataAttribute();
    addr.coords = _arrayIters[fda.getId()]->getPosition();
    int const FLAGS =
        ChunkIterator::NO_EMPTY_CHECK |
        ChunkIterator::SEQUENTIAL_WRITE;
    vector<std::shared_ptr<ConstChunkIterator>> inChunkIters(N_TRUE_ATTRS);
    vector<std::shared_ptr<ChunkIterator>> outChunkIters(N_TRUE_ATTRS);
    for (size_t i = 0; i < N_TRUE_ATTRS; ++i) {
        // Input side...
        inChunkIters[i] = _arrayIters[i]->getChunk().getConstIterator();

        addr.attId = AttributeID(i + _PREFIX_COUNT);
        MemChunk& outChunk = _chunkRef(addr.attId, _currRowIndex);
        outChunk.initialize(this, &getArrayDesc(), addr,
                            getArrayDesc().getAttributes().findattr(addr.attId).getDefaultCompressionMethod());
        outChunkIters[i] = outChunk.getIterator(query, FLAGS);
    }

    // Copy values.  Wasteful, but apparently SinglePassArray-derived
    // classes cannot avoid it.  Minimize Value resizes by doing one
    // attribute at a time.

    for (AttributeID id = 0; id < N_TRUE_ATTRS; ++id) {
        while (!inChunkIters[id]->end()) {
            outChunkIters[id]->writeItem(inChunkIters[id]->getItem());
            ++(*inChunkIters[id]);
            ++(*outChunkIters[id]);
        }
    }

    // Close all chunks.
    for (AttributeID id = 0; id < N_TRUE_ATTRS; ++id) {
        outChunkIters[id]->flush();
        outChunkIters[id].reset();
    }

    // Set empty bitmap chunk for pass-thru chunks.
    MemChunk& ebmChunk = _chunkRef(_OUT_EBM_ATTR, _currRowIndex);
    for (size_t i = _PREFIX_COUNT; i < _OUT_EBM_ATTR; ++i) {
        AttributeID attrId = AttributeID(i);
        auto& mc = _chunkRef(attrId, _currRowIndex);
        mc.setBitmapChunk(&ebmChunk);
    }
}


ConstChunk const& PrettySortArray::getChunk(AttributeID attrId, size_t rowIdx)
{
    LOG4CXX_TRACE(logger, __FUNCTION__ << "(attr=" << attrId
                  << ", row=" << rowIdx << ')');

    Query::getValidQueryPtr(_query);

    // (Some gyrations here to avoid unsigned underflow.)
    if (rowIdx > _currRowIndex || rowIdx + _HISTORY_SIZE <= _currRowIndex) {
        ASSERT_EXCEPTION_FALSE("Requested row " << rowIdx
                               << " not in history cache range "
                               << (_currRowIndex > _HISTORY_SIZE
                                   ? _currRowIndex - _HISTORY_SIZE
                                   : 0)
                               << ".." << _currRowIndex);
    }

    MemChunk& chunk = _chunkRef(attrId, rowIdx);
    LOG4CXX_TRACE(logger, __FUNCTION__
                  << "(attr=" << attrId << ", row=" << rowIdx
                  << ") --> " << CoordsToStr(chunk.getFirstPosition(false)));

    return chunk;
}

} // namespace
