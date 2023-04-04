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
 * @file SlowDfArray.cpp
 * @author Mike Leibensperger
 * @brief "Slow path" array-to-dataframe converter.
 */

#include "SlowDfArray.h"
#include "CellProvider.h"

#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.flatten.slowdfarray"));
}

namespace scidb
{

SlowDfArray::SlowDfArray(ArrayDesc const& outSchema,
                         std::shared_ptr<Array> inArray,
                         std::shared_ptr<Query> const& query)
    : DelegateArray(outSchema, inArray, /*clone:*/ false)
    , _DF_INPUT(inArray->getArrayDesc().isDataframe())
    , _N_IN_DIMS(inArray->getArrayDesc().getDimensions().size())
    , _N_IN_ATTRS(inArray->getArrayDesc().getAttributes(/*exclude:*/false).size())
    , _N_OUT_ATTRS(_N_IN_ATTRS // ...plus _N_IN_DIMS for arrays, not dataframes
                   + (_DF_INPUT ? 0 : _N_IN_DIMS))
    , _OUT_EBM_ATTR(AttributeID(_N_OUT_ATTRS - 1))
    , _localInstance(dataframePhysicalIdMode()
                     ? query->getPhysicalInstanceID()
                     : query->getInstanceID())
    , _currPos(DF_NUM_DIMS)
    , _chunks(_N_OUT_ATTRS)
    , _regionProvider(inArray)
    , _cellProvider(_regionProvider)
{
    setQuery(query);
    _currPos[DF_INST_DIM] = _localInstance;
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Convert from " << inArray->getArrayDesc());
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Convert  to  " << outSchema);
}

bool SlowDfArray::_makeChunks(size_t row)
{
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Entered, row " << row);

    // Bookkeeping...
    if (row == _currRow) {
        // Already there, no problem.
        LOG4CXX_TRACE(logger, _cls << __func__ << ": Early quit, row=" << row
                      << ", _currRow=" << _currRow);
        return true;
    }
    if (row != _currRow + 1) {
        // Somebody's out of step.
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }
    // Our turn to make the doughnuts.  Can we?
    if (_cellProvider.end()) {
        LOG4CXX_TRACE(logger, _cls << __func__
                      << ": Cell provider at eof, row=" << row
                      << ", _currRow=" << _currRow);
        return false;
    }

    // We can!
    Dimensions const& outDims = getArrayDesc().getDimensions();
    _prevPos = _currPos;
    _currPos[DF_SEQ_DIM] = _currRow * outDims[DF_SEQ_DIM].getChunkInterval();
    ++_currRow;

    LOG4CXX_TRACE(logger, _cls << __func__ << ": Making chunks, _currRow=" << _currRow);

    // Prepare iterator(s) to generate output chunks.
    vector<std::shared_ptr<ChunkIterator>> outIters(_N_OUT_ATTRS);
    int const FLAGS =
        ChunkIterator::NO_EMPTY_CHECK |
        ChunkIterator::SEQUENTIAL_WRITE;
    auto query = Query::getValidQueryPtr(_query);
    Address addr(0, _currPos);
    for (AttributeID id = 0; id < _N_OUT_ATTRS; ++id) {
        MemChunk& mc = _chunks[id];
        addr.attId = id;
        mc.initialize(this, &getArrayDesc(), addr, CompressorType::NONE);
        outIters[id] = mc.getIterator(query, FLAGS);
    }

    ssize_t const WANT_CELLS = outDims[DF_SEQ_DIM].getChunkInterval();
    Coordinates inCoords(_N_IN_DIMS);
    Cell inCell(_N_IN_ATTRS);
    bool more = false;
    Value v, trueVal;
    trueVal.setBool(true);

    LOG4CXX_TRACE(logger, _cls << __func__ <<
                  ": Need " << WANT_CELLS << " cells for this chunk");

    ssize_t gotCells = 0;
    while (gotCells < WANT_CELLS) {
        more = _cellProvider.next(inCell, inCoords);
        if (!more) {
            LOG4CXX_TRACE(logger, _cls << __func__ << ": EOF, partial chunk has "
                          << gotCells << " of " << WANT_CELLS << " cells");
            break;
        }
        ++gotCells;


        size_t id = 0;
        if (!_DF_INPUT) {
            // Write "prefix" attributes: the input cell's coordinates.
            SCIDB_ASSERT(_N_IN_DIMS == inCoords.size());
            for (id = 0; id < _N_IN_DIMS; ++id) {
                v.setInt64(inCoords[id]);
                outIters[id]->writeItem(v);
                ++(*outIters[id]);
            }
        }

        // Write the "pass-thru" attributes: the attributes of the
        // original array.
        SCIDB_ASSERT(id == 0 || id == _N_IN_DIMS);
        for (; id < _OUT_EBM_ATTR; ++id) {
            size_t inId = _DF_INPUT ? id : id - _N_IN_DIMS;
            outIters[id]->writeItem(inCell[inId]);
            ++(*outIters[id]);
        }

        // Write the outgoing empty bitmap attribute.
        outIters[_OUT_EBM_ATTR]->writeItem(trueVal);
        ++(*outIters[_OUT_EBM_ATTR]);
    }

    ASSERT_EXCEPTION(gotCells > 0, "No cells written but end() didn't trip?!");

    // Close chunks and set their empty bitmaps.
    ConstChunk* outEbmChunk = &_chunks[_OUT_EBM_ATTR];
    for (size_t i = 0; i < _N_OUT_ATTRS; ++i) {
        outIters[i]->flush();
        outIters[i].reset();
        MemChunk& mc = _chunks[i];
        mc.setBitmapChunk(dynamic_cast<Chunk*>(outEbmChunk));
        SCIDB_ASSERT(mc.getBitmapChunk() != nullptr);
    }

    LOG4CXX_TRACE(logger, _cls << __func__
                  << ": Done making chunks, _currRow=" << _currRow
                  << ", cellEnd=" << _cellProvider.end()
                  << ", wrote " << gotCells
                  << " of " << WANT_CELLS << " cells");

    return true;
}

void SlowDfArray::_restart()
{
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Called");
    _currRow = 0;
    _prevPos.clear();
    _currPos.clear();
    _currPos.resize(DF_NUM_DIMS);
    _currPos[DF_INST_DIM] = _localInstance;
    _cellProvider.restart();
}

DelegateArrayIterator*
SlowDfArray::createArrayIterator(AttributeDesc const& aDesc) const
{
    SlowDfArray* self = const_cast<SlowDfArray*>(this);
    return new SlowDfArrayIterator(*self, aDesc);
}

// ---- SlowDfArrayIterator --------------------------------------------

SlowDfArrayIterator::SlowDfArrayIterator(SlowDfArray& array,
                                         AttributeDesc const& outAttr)
    : super(array, outAttr, std::shared_ptr<ConstArrayIterator>())
    , _array(array)
{
    LOG4CXX_TRACE(logger, _cls << __func__
                  << ": Constructed for attrId=" << outAttr.getId());
}

bool SlowDfArrayIterator::setPosition(Coordinates const& pos)
{
    // We're a MULTI_PASS array which supposedly means we need not
    // implement setPosition().  Yet setPosition() is still called by
    // ConstChunk::getEmptyBitmap() anyhow.  So long as that's called
    // only on the EBM chunk itself, we can fake it.
    //
    // (Not all our attributes have corresponding inputIterators, so
    // we can't just call super::getPosition() with abandon... even if
    // we could, we'd have to translate DF coordinates to inputArray
    // coordinates, and that's hard when the DF is unbounded. A random
    // DF position may not even live on this instance, so we can't
    // solve the general case.)

    bool result = false;

    if (pos == _array._currPos) {
        _myRow = _array._currRow;     // If we weren't before, we are now.
        result = true;
        LOG4CXX_TRACE(logger, "SlowDfArrayIterator::setPosition("
                      << CoordsToStr(pos) << "): No problem, that's _currRow="
                      << _array._currRow << ", result=" << result);
    }
    else {
        LOG4CXX_WARN(logger, "SlowDfArrayIterator::setPosition(" << CoordsToStr(pos)
                     << "): Uh oh, _currRow=" << _array._currRow
                     << " but _currPos=" << CoordsToStr(_array._currPos)
                     << ", result=" << result);
        ASSERT_EXCEPTION_FALSE(__PRETTY_FUNCTION__ << ": No clue what to do");
    }

    return result;
}

void SlowDfArrayIterator::operator++()
{
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Attr " << attr.getId()
                  << " leaving row " << _myRow);
    ++_myRow;
}

void SlowDfArrayIterator::restart()
{
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Attr " << attr.getId()
                  << " restarting");
    _myRow = 1;
    if (attr.getId() == 0) {
        _array._restart();
    }
}

ConstChunk const& SlowDfArrayIterator::getChunk()
{
    bool more = _array._makeChunks(_myRow);
    if (!more) {
        LOG4CXX_TRACE(logger, _cls << __func__
                      << ": No chunks for attr=" << attr.getId()
                      << ", row=" << _myRow);
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }

    LOG4CXX_TRACE(logger, _cls << __func__
                  << ": Called for attr=" << attr.getId()
                  << ", row=" << _myRow);
    return _array._chunks[attr.getId()];
}

bool SlowDfArrayIterator::end()
{
    if (_myRow == _array._currRow) {
        // Can't be at end, since _array never moves _currRow beyond
        // the last row actually seen.
        LOG4CXX_TRACE(logger, _cls << __func__
                      << ": Looking at _array._currRow=" << _array._currRow
                      << ", attr=" << attr.getId() << ", so result=false");
        return false;
    }

    if (_array._currRow == 0) {
        ASSERT_EXCEPTION(_myRow == 1, "Out of step again, somehow."); // xxx
        // Initial conditions, _makeChunks() not yet called.  What
        // does the cell provider think?
        bool result = _array._cellProvider.end();
        LOG4CXX_TRACE(logger, _cls << __func__
                      << ": No output chunks yet, attr=" << attr.getId()
                      << ", cellProvider says eof=" << result);
        return result;
    }

    if (_myRow < _array._currRow) {
        // No longer initial conditions, so here we know we're not at eof.
        ASSERT_EXCEPTION(_myRow + 1 == _array._currRow, "Out of step, ouch."); // xxx
        LOG4CXX_TRACE(logger, _cls << __func__ << ": Attr=" << attr.getId()
                      << " chunks await me at row=" << _array._currRow
                      << ", eof=false");
        return false;
    }

    // _myRow > _array._currRow means that I've incremented, but not
    // yet asked for a chunk so no chunks have been made yet.  Ask the
    // cell provider whether there'll be new chunks forthcoming.

    bool result = _array._cellProvider.end();
    LOG4CXX_TRACE(logger, _cls << __func__
                  << ": Output chunks for row=" << _myRow
                  << " depend on cellProvider, which says eof=" << result);
    return result;
}

Coordinates const& SlowDfArrayIterator::getPosition()
{
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Called, attr=" << attr.getId()
                  << ", _myRow=" << _myRow
                  << ", _array._currRow=" << _array._currRow
                  << ", _array._currPos=" << CoordsToStr(_array._currPos));

    if (_myRow == _array._currRow) {
        return _array._currPos;
    }
    if (_myRow + 1 == _array._currRow) {
        SCIDB_ASSERT(!_array._prevPos.empty());
        return _array._prevPos;
    }

    // I'm out of step.
    LOG4CXX_TRACE(logger, _cls << __func__ << ": Step error, _myRow=" << _myRow
                  << ", _array._currRow=" << _array._currRow);
    throw SYSTEM_EXCEPTION(
        SCIDB_SE_EXECUTION,
        SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
}

} // namespace
