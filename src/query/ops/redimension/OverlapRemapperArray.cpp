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
 * @file OverlapRemapperArray.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "OverlapRemapperArray.h"

#include "ChunkIdMap.h"
#include "Remapper.h"

#include <array/MemChunk.h>
#include <query/Query.h>
#include <util/OverlappingChunksIterator.h>
#include <log4cxx/logger.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.redimension.overlapremapper"));

OverlapRemapperArray::OverlapRemapperArray(std::shared_ptr<Array> inputArray,
                                           std::shared_ptr<Remapper>& remap,
                                           std::shared_ptr<Query> const& query)
    : DelegateArray(inputArray->getArrayDesc(), inputArray, /*isClone:*/false)
    , _inputArrayAttrs(inputArray->getArrayDesc().getAttributes(false))
    , _remap(remap)
    , _N_ATTRS(_inputArrayAttrs.size())
    , _EBM_ATTR(_inputArrayAttrs.getEmptyBitmapAttribute()->getId())
    , _ID_ATTR(AttributeID(_EBM_ATTR - 1))  // TODO fix this once EBM moves to position zero
    , _POS_ATTR(AttributeID(_ID_ATTR - 1))  // TODO fix this once EBM moves to position zero
    , _INTERVAL(inputArray->getArrayDesc().getDimensions()[0].getChunkInterval())
    , _chunkEpoch(0UL)
    , _freshChunks(0L)
    , _sawEof(false)
    , _firstRow(true)
    , _firstChunks(true)
    , _inputArrayIters(_N_ATTRS)
    , _inputChunkIters(_N_ATTRS)
    , _inputValues(_N_ATTRS)
    , _chunks(_N_ATTRS)
    , _writeIters(_N_ATTRS)
{
    // At least one authentic attribute in the 1-D array.
    SCIDB_ASSERT(_POS_ATTR > 0);  // This should assert once the EBM moves to position zero, fix later.

    setQuery(query);
    for (const auto& aid : _inputArrayAttrs) {
        _inputArrayIters[aid.getId()] = getPipe(0)->getConstIterator(aid);
    }
}



DelegateArrayIterator* OverlapRemapperArray::createArrayIterator(const AttributeDesc& aid) const
{
    // I have no idea why the base class method needs to be const.
    OverlapRemapperArray* self = const_cast<OverlapRemapperArray*>(this);
    return new OverlapRemapperArrayIterator(*self, aid, _inputArrayIters[aid.getId()]);
}


bool OverlapRemapperArray::makeMoreChunks()
{
    // All previous chunks should have been consumed.
    SCIDB_ASSERT(_freshChunks == 0L);

    // Did getNextInputRow() encounter end-of-input?
    if (_sawEof) {
        return false;
    }

    // Make some MemChunks we can write into.
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    Address addr;
    addr.coords.push_back(_chunkEpoch * _INTERVAL);
    int const FLAGS =
        ChunkIterator::NO_EMPTY_CHECK |
        ChunkIterator::SEQUENTIAL_WRITE;
    for (const auto& attr : _inputArrayAttrs) {
        const auto i = attr.getId();
        _chunks[i].reset(new MemChunk());
        addr.attId = AttributeID(i);
        _chunks[i]->initialize(this, &getArrayDesc(), addr,
                               getArrayDesc().getAttributes().findattr(i).getDefaultCompressionMethod());
        _writeIters[i] = _chunks[i]->getIterator(query, FLAGS);
    }

    // Write the new chunks.
    Value emptyBitmapValue;
    emptyBitmapValue.setBool(true);
    Value vPos, vId;
    int64_t cellCount = 0;
    for ( ; cellCount < _INTERVAL; ++cellCount) {

        // Get next input cell, then figure out where all its copies should go.
        if (_clones.empty()) {
            if (!getNextInputRow()) {
                SCIDB_ASSERT(_sawEof); // End of input.
                break;
            }

            // Transform provisional (pos, id) to cell coordinates.  Result accessed via
            // getCellCoordinates() below.
            _remap->provPosIdToCoords(_inputValues[_POS_ATTR], _inputValues[_ID_ATTR]);

            // OverlappingChunksIterator iterates over the logical space.
            // Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
            // that's fine by the same reasoning given in RedimensionCommon.cpp:
            //
            //     If chunkOverlap = 0, there is only one chunk in the space so it is ok.  With
            //     non-zero chunkOverlaps, the space includes only the neighbor chunks that need to
            //     store a copy of this record. We have no option but to iterate over all of them.
            //
            OverlappingChunksIterator allChunks(_remap->getFinalDimensions(), _remap->getCellCoordinates());
            while (!allChunks.end()) {
                Coordinates const& overlappingChunkPos = allChunks.getPosition();
                _remap->chunkAndCellToPosId(overlappingChunkPos, _remap->getCellCoordinates(), vPos, vId);
                _clones.push_back(std::make_pair(vPos.getInt64(), vId.getInt64()));
                ++allChunks;
             }
            SCIDB_ASSERT(!_clones.empty());
        }

        // Now generate one cell copy.
        for (AttributeID aid = 0; aid < _POS_ATTR; ++aid) {
            _writeIters[aid]->writeItem(_inputValues[aid]);
            ++(*_writeIters[aid]);
        }

        vPos.setInt64(_clones.front().first);
        vId.setInt64(_clones.front().second);
        _clones.pop_front();

        _writeIters[_POS_ATTR]->writeItem(vPos);
        ++(*_writeIters[_POS_ATTR]);
        _writeIters[_ID_ATTR]->writeItem(vId);
        ++(*_writeIters[_ID_ATTR]);
        _writeIters[_EBM_ATTR]->writeItem(emptyBitmapValue);
        ++(*_writeIters[_EBM_ATTR]);
    }

    // Close all chunks.
    for (const auto& attr : _inputArrayAttrs) {
        const auto i = attr.getId();
        _writeIters[i]->flush();
        _writeIters[i].reset();
    }

    // It pays to know your bitmap.
    Chunk* bitmapChunk = _chunks[_EBM_ATTR].get();
    for (const auto& attr : getPipe(0)->getArrayDesc().getAttributes(true)) {
        _chunks[attr.getId()]->setBitmapChunk(bitmapChunk);
    }

    // Incrementing this signals our array iterators to come get the their chunk.
    if (!_firstChunks) {
        ++_chunkEpoch;
    } else {
        // These are the epoch 0 chunks, no increment.
        SCIDB_ASSERT(_chunkEpoch == 0);
        _firstChunks = false;
    }

    _freshChunks = cellCount ? _chunks.size() : 0;

    LOG4CXX_TRACE(logger, "After makeMoreChunks: fresh=" <<  _freshChunks
                 << ", cells=" << cellCount
                 << ", epoch=" << _chunkEpoch);

    return cellCount != 0;
}


bool OverlapRemapperArray::getNextInputRow()
{
    // Once EOF, always EOF.  Prevents gratuitous iterator increments.
    if (_sawEof) {
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Still at EOF.");
        return false;
    }

    // Do we need more input chunks?
    const auto& fda = _inputArrayAttrs.firstDataAttribute();
    if (!_inputChunkIters[fda.getId()] || _inputChunkIters[fda.getId()]->end()) {
        if (_firstRow) {
            _firstRow = false;
        } else {
            // Increment array iterators.
            for (auto& aIter : _inputArrayIters) {
                ++(*aIter);
            }
            LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": Bumped _inputArrayIters");
        }

        // Done?
        if (_inputArrayIters[fda.getId()]->end()) {
            LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": End of input!");
            _sawEof = true;
            return false;
        }

        // Not done, get chunk interators.
        for (const auto& aid : _inputArrayAttrs) {
            _inputChunkIters[aid.getId()] = _inputArrayIters[aid.getId()]->getChunk().getConstIterator();
        }
    }

    // Got chunks to work on!  Read the next cell.
    for (const auto& aid : _inputArrayAttrs) {
        _inputValues[aid.getId()] = _inputChunkIters[aid.getId()]->getItem();
        ++(*_inputChunkIters[aid.getId()]);
    }

    return true;
}

// ----------------------------------------------------------------------

ConstChunk const& OverlapRemapperArrayIterator::getChunk()
{
    if (_currPos[0] == 0 && !_chunkPtr) {
        // First fetch.
        LOG4CXX_TRACE(logger, __PRETTY_FUNCTION__ << ": First fetch");
        if (!_array._chunks[_attrId.getId()]) {
            // No chunks created yet, so we must be the first iterator asked to do a fetch.  Create
            // a batch of fresh chunks.  (This is why iterators should obey an open/close protocol.)
            SCIDB_ASSERT(_wantChunk == 0UL);
            SCIDB_ASSERT(_gotChunk == 0UL);
            SCIDB_ASSERT(_array._chunkEpoch == 0UL);
            SCIDB_ASSERT(_array._freshChunks == 0L);
            if (!_array.makeMoreChunks()) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
            }
            SCIDB_ASSERT(_array._freshChunks == ssize_t(_array._N_ATTRS));
            SCIDB_ASSERT(_array._chunks[_attrId.getId()]);
        }
        // Grab my fresh chunk.
        _chunkPtr = _array._chunks[_attrId.getId()];
        _array._chunks[_attrId.getId()].reset();
        SCIDB_ASSERT(_array._freshChunks > 0);
        --_array._freshChunks;
    } else if (_wantChunk != _gotChunk) {
        SCIDB_ASSERT(_gotChunk + 1 == _wantChunk);
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
    }
    SCIDB_ASSERT(_chunkPtr);
    return *_chunkPtr;
}


void OverlapRemapperArrayIterator::operator++()
{
    SCIDB_ASSERT(_wantChunk == _gotChunk);
    ++_wantChunk;

    // Maybe it's my turn to make the chunks?
    if (_wantChunk != _array._chunkEpoch) {

        // If these conditions don't hold, then someone is doing something wrong.  We require that
        // upstream callers walk our chunks horizontally in lock-step.
        SCIDB_ASSERT(_wantChunk == _array._chunkEpoch + 1);
        SCIDB_ASSERT(_array._freshChunks == 0);

        // If we fail to make chunks, leave _gotChunks != _wantChunks, i.e. we're at end().
        if (!_array.makeMoreChunks()) {
            SCIDB_ASSERT(end());
            return;
        }
        SCIDB_ASSERT(_array._freshChunks == ssize_t(_array._N_ATTRS));
        SCIDB_ASSERT(_array._chunks[_attrId.getId()]);
    }

    // Should be a nice fresh chunk waiting for me.
    SCIDB_ASSERT(_array._freshChunks > 0);
    _chunkPtr = _array._chunks[_attrId.getId()];
    _array._chunks[_attrId.getId()].reset();
    --_array._freshChunks;
    SCIDB_ASSERT(_chunkPtr);
    ++_gotChunk;
    _currPos[0] += _array._INTERVAL;
}


bool OverlapRemapperArrayIterator::end()
{
    if (_array._firstChunks) {
        // Haven't fetched anything yet, so optimistically assume there is something to fetch.
        return false;
    }
    return _gotChunk != _wantChunk;
}

} // namespace scidb
