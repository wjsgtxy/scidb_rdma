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
 * @file FastDfArray.cpp
 * @author Mike Leibensperger
 * @brief "Fast path" array-to-dataframe converter.
 */

#include "FastDfArray.h"
#include <array/PinBuffer.h>
#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.flatten.fastdfarray"));
}

namespace scidb
{

FastDfArray::FastDfArray(ArrayDesc const& outSchema,
                         std::shared_ptr<Array> inArray,
                         std::shared_ptr<Query> const& query)
    : DelegateArray(outSchema, inArray, /*clone:*/ true)
    , _N_IN_DIMS(inArray->getArrayDesc().getDimensions().size())
    , _N_IN_ATTRS(inArray->getArrayDesc().getAttributes(/*exclude:*/false).size())
    , _OUT_EBM_ATTR(AttributeID(_N_IN_DIMS + _N_IN_ATTRS - 1))
    , _localInstance(dataframePhysicalIdMode()
                     ? query->getPhysicalInstanceID()
                     : query->getInstanceID())
    , _currPos(DF_NUM_DIMS)
    , _chunks(_N_IN_DIMS + 1)   // +1 in case we must copy EBM payloads (SDB-6298)
    , _mapper(inArray->getArrayDesc().getDimensions())
{
    setQuery(query);
    _currPos[DF_INST_DIM] = _localInstance;
    LOG4CXX_TRACE(logger, __func__ << ": Convert from " << inArray->getArrayDesc());
    LOG4CXX_TRACE(logger, __func__ << ": Convert  to  " << outSchema);
}

// These predicates are based on vector sizes fixed by the constructor.
bool FastDfArray::_isPrefixAttr(size_t attrId) const
{
    return attrId < _N_IN_DIMS;
}

bool FastDfArray::_isCloneAttr(size_t attrId) const
{
    return attrId >= _N_IN_DIMS && attrId <= _OUT_EBM_ATTR;
}

bool FastDfArray::_makeChunks(size_t row)
{
    LOG4CXX_TRACE(logger, __func__ << ": Entered, row " << row);

    // Bookkeeping...
    if (_end() || row == _currRow) {
        LOG4CXX_TRACE(logger, __func__ << ": Early quit, _atEnd=" << _atEnd
                      << ", row=" << row << ", _currRow=" << _currRow);
        // Already there, no problem.
        return !_atEnd;
    }
    if (row != _currRow + 1) {
        // Somebody's out of step.
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }
    Dimensions const& outDims = getArrayDesc().getDimensions();
    _prevPos = _currPos;
    _currPos[DF_SEQ_DIM] = _currRow * outDims[DF_SEQ_DIM].getChunkInterval();
    ++_currRow;

    LOG4CXX_TRACE(logger, __func__ << ": Making chunks, _currRow=" << _currRow);
    ASSERT_EXCEPTION(_inputEbmIter,
                     "Array iterator getChunk() called before end() check");

    // Prepare iterator(s) to generate prefix chunk(s).
    vector<std::shared_ptr<ChunkIterator>> outIters(_N_IN_DIMS);
    int const FLAGS =
        ChunkIterator::NO_EMPTY_CHECK |
        ChunkIterator::SEQUENTIAL_WRITE;
    auto query = Query::getValidQueryPtr(_query);
    Address addr(0, _currPos);
    for (AttributeID id = 0; id < _N_IN_DIMS; ++id) {
        MemChunk& mc = _chunks[id];
        addr.attId = id;
        mc.initialize(this, &getArrayDesc(), addr, CompressorType::NONE);
        outIters[id] = mc.getIterator(query, FLAGS);
    }

    // Iterate through input empty bitmap chunk and write cell
    // coordinates into prefix chunks.
    ConstChunk const& inEbmChunk = _inputEbmIter->getChunk();
    Coordinates chunkPos(inEbmChunk.getFirstPosition(/*overlap:*/false));
    Coordinates cellPos(chunkPos.size());
    assert(cellPos.size() == _N_IN_DIMS);
    ConstRLEEmptyBitmap ebm(inEbmChunk);
    auto ebmIter = ebm.getIterator();
    Value v;
    while (!ebmIter.end()) {
        _mapper.pos2coord(chunkPos, ebmIter.getLPos(), cellPos);
        LOG4CXX_TRACE(logger, __func__ << ": Cell at " << CoordsToStr(cellPos));
        for (size_t i = 0; i < _N_IN_DIMS; ++i) {
            v.setInt64(cellPos[i]);
            outIters[i]->writeItem(v);
            ++(*outIters[i]);
        }
        ++ebmIter;
    }

    // We have a ConstChunk inEbmChunk, but MemChunk::setBitmapChunk()
    // demands a Chunk.  Hopefully this const_cast is not too evil: we
    // don't plan to mutate inEbmChunk.
    //
    Chunk* theEbmChunk = nullptr;
    auto tmp = const_cast<ConstChunk*>(&inEbmChunk);
    theEbmChunk = dynamic_cast<Chunk*>(tmp);
    if (!theEbmChunk) {
        // Darn, we need a Chunk, so we'll have to copy it.
        LOG4CXX_TRACE(logger, __func__ << ": EBM payload copy, _currRow=" << _currRow);
        MemChunk& ebmCopy = _chunks.back();
        ebmCopy.initialize(inEbmChunk);
        ebmCopy.setPayload(&inEbmChunk);
        theEbmChunk = &ebmCopy;
    }

    // Close generated chunks and set their empty bitmaps.
    for (size_t i = 0; i < _N_IN_DIMS; ++i) {
        outIters[i]->flush();
        outIters[i].reset();
        MemChunk& mc = _chunks[i];
        mc.setBitmapChunk(theEbmChunk);
        SCIDB_ASSERT(mc.getBitmapChunk() != nullptr);
    }

    return true;
}

void FastDfArray::_restart()
{
    if (_currRow != 0) {
        LOG4CXX_TRACE(logger, __func__ << ": Array-level restart");
        _atEnd = false;
        _currRow = 0;
        _currPos[DF_SEQ_DIM] = 0;
        if (_inputEbmIter) {
            _inputEbmIter->restart();
        }
    } else {
        LOG4CXX_TRACE(logger, __func__ << ": Already restarted at array-level");
    }
}

bool FastDfArray::_end()
{
    if (_atEnd) {
        // Access is not RANDOM so it's OK to cache whether we've hit the end.
        return true;
    }
    if (!_inputEbmIter) {
        LOG4CXX_TRACE(logger, __func__ << ": Make _inputEbmIter");
        Attributes const& inAttrs = getPipe(0)->getArrayDesc().getAttributes();
        SCIDB_ASSERT(inAttrs.hasEmptyIndicator());
        const auto& ebmAttr = inAttrs.getEmptyBitmapAttribute();
        _inputEbmIter = getPipe(0)->getConstIterator(*ebmAttr);
    }
    if (_inputEbmIter->end()) {
        _atEnd = true;
        return true;
    }
    return false;
}

void FastDfArray::_bumped(AttributeDesc const& who, size_t row)
{
    // The attribute 'who' is now positioned on 'row'.
    if (row != _currRow && row != _currRow + 1) {
        // Yet another out-of-step error.
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }

    // Almost always it's the first non-EBM attribute that's checked
    // for end(), since that attribute is certain to exist.  So when
    // the corresponding array iterator gets bumped, we'll also bump
    // the backend _inputEbmIter.

    if (who.getId() == 0) {
        LOG4CXX_TRACE(logger, __func__
                      << ": AttrId=" << who.getId() << " arrives at row " << row
                      << ", increments _inputEbmIter");
        assert(_inputEbmIter);
        ++(*_inputEbmIter);
    }
}

DelegateArrayIterator*
FastDfArray::createArrayIterator(AttributeDesc const& aDesc) const
{
    FastDfArray* self = const_cast<FastDfArray*>(this);
    AttributeID id = aDesc.getId();

    // Generated coordinate-as-attribute?  Empty bitmap shifted along
    // $inst dimension?  Don't give it a back-end iterator.
    if (_isPrefixAttr(id)) {
        return new FastDfArrayIterator(
            *self, aDesc, std::shared_ptr<ConstArrayIterator>());
    }

    // Direct-mapped attribute payload (includes the empty bitmap)?
    assert(_isCloneAttr(id));
    AttributeID inputId = AttributeID(id - _N_IN_DIMS);
    Attributes const& inAttrs = getPipe(0)->getArrayDesc().getAttributes();
    const auto& inAttr = inAttrs.findattr(inputId);
    return new FastDfArrayIterator(
        *self, aDesc, getPipe(0)->getConstIterator(inAttr));
}

DelegateChunk*
FastDfArray::createChunk(DelegateArrayIterator const* it, AttributeID id) const
{
    // One of the chunks to be generated by _makeChunks()?  Don't
    // bother to allocate anything, FastDfArrayIterator will hand out
    // a chunk from _chunks[] when the time comes.
    if (_isPrefixAttr(id)) {
        return nullptr;
    }

    // Direct-mapped attribute payload?  We'll need to make sure it's
    // now associated with the outSchema, not the inSchema.  Prepare a
    // FastDfChunk to do that.
    assert(_isCloneAttr(id));
    auto fdai = dynamic_cast<FastDfArrayIterator const*>(it);
    assert(fdai);
    return new FastDfChunk(*this, *fdai, id);
}

DelegateChunkIterator*
FastDfArray::createChunkIterator(DelegateChunk const* dc, int mode) const
{
    auto chunkp = dynamic_cast<FastDfChunk const*>(dc);
    assert(chunkp);
    return new FastDfChunkIterator(*chunkp, mode);
}

// ---- FastDfArrayIterator --------------------------------------------

FastDfArrayIterator::FastDfArrayIterator(FastDfArray& array,
                                         AttributeDesc const& outAttr,
                                         std::shared_ptr<ConstArrayIterator> inIter)
    : super(array, outAttr, inIter)
    , _array(array)
    , _isClone(bool(inIter))
{
    SCIDB_ASSERT(_isClone == array._isCloneAttr(attr.getId()));
    LOG4CXX_TRACE(logger, __func__ << ": Constructed for attrId=" << outAttr.getId());
}

bool FastDfArrayIterator::setPosition(Coordinates const& pos)
{
    // We're a MULTI_PASS array which supposedly means we need not
    // implement setPosition().  Yet setPosition() is still called by
    // ConstChunk::getEmptyBitmap() anyhow.  So long as that's called
    // only on the EBM chunk itself, we can fake it.
    //
    // (Not all our attributes have corresponding inputIterators, so
    // we can't just call super::getPosition() with abandon... even if
    // we could, we'd have to translate DF coordinates to input array
    // (pipe 0) coordinates, and that's hard when the DF is unbounded.
    // (XXX T-or-F: A random DF position may not even live on this
    // instance, so we can't solve the general case.))

    bool result = false;

    if (pos == _array._currPos) {
        _myRow = _array._currRow;     // If we weren't before, we are now.
        result = true;
        LOG4CXX_TRACE(logger, "FastDfArrayIterator::setPosition("
                      << CoordsToStr(pos) << "): No problem, that's _currRow="
                      << _array._currRow << ", result=" << result);
    }
    else {
        LOG4CXX_WARN(logger, "FastDfArrayIterator::setPosition(" << CoordsToStr(pos)
                     << "): Uh oh, _currRow=" << _array._currRow
                     << " but _currPos=" << CoordsToStr(_array._currPos)
                     << ", result=" << result);
        ASSERT_EXCEPTION_FALSE(__PRETTY_FUNCTION__ << ": No clue what to do");
    }

    return result;
}

void FastDfArrayIterator::restart()
{
    LOG4CXX_TRACE(logger, __func__ << ": Called, attr=" << attr.getId());

    _array._restart();
    _myRow = 1;

    // Clones have to step their back-end iterators too.
    if (_isClone) {
        super::restart();
    }
}

void FastDfArrayIterator::operator++()
{
    LOG4CXX_TRACE(logger, __func__ << ": Attr " << attr.getId()
                  << " leaving row " << _myRow);

    ++_myRow;
    _array._bumped(attr, _myRow);

    // Clones have to step their back-end iterators too.
    if (_isClone) {
        super::operator++();
    }
}

ConstChunk const& FastDfArrayIterator::getChunk()
{
    bool more = _array._makeChunks(_myRow);
    if (!more) {
        LOG4CXX_ERROR(logger, __func__
                      << ": No chunks for attr=" << attr.getId());
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }

    // Clones can just call super::getChunk() because although
    // !chunkInitialized, the only initialization we'd do would be to
    // update the returned get{First,Last}Position() values.  That
    // happens anyway because of the FastDfChunk overrides for those
    // methods.  So the default getChunk() action is OK.

    LOG4CXX_TRACE(logger, __func__ << ": Called for attr=" << attr.getId()
                  << (_isClone ? " (clone)" : " (prefix)"));
    return _isClone ? super::getChunk() : _array._chunks[attr.getId()];
}

bool FastDfArrayIterator::end()
{
    bool result = _isClone ? super::end() : _array._end();
    LOG4CXX_TRACE(logger, __func__ << ": Called, attr=" << attr.getId()
                  << ", result=" << result);
    return result;
}

Coordinates const& FastDfArrayIterator::getPosition()
{
    if (isDebug()) {
        stringstream ss;
        if (_isClone) {
            ss << ", super::getPosition()="
               << CoordsToStr(super::getPosition());
        }
        LOG4CXX_TRACE(logger, __func__ << ": Called, attr=" << attr.getId()
                      << ", _isClone=" << _isClone
                      << ", _myRow=" << _myRow
                      << ", _array._currRow=" << _array._currRow
                      << ", _array._currPos=" << CoordsToStr(_array._currPos)
                      << ss.str());
    }

    if (_myRow == _array._currRow) {
        return _array._currPos;
    }
    if (_myRow + 1 == _array._currRow) {
        SCIDB_ASSERT(!_array._prevPos.empty());
        return _array._prevPos;
    }

    // I'm out of step.
    LOG4CXX_ERROR(logger, __func__ << ": Step error, _myRow=" << _myRow
                  << ", _array._currRow=" << _array._currRow);
    throw SYSTEM_EXCEPTION(
        SCIDB_SE_EXECUTION,
        SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
}

// ---- FastDfChunk ----------------------------------------------------

FastDfChunk::FastDfChunk(FastDfArray const& a,
                         FastDfArrayIterator const& it,
                         AttributeID outAttrId)
    : super(a, it, outAttrId, /*isClone:*/true)
    , _array(a)
    , _attrId(outAttrId)
    , _myRow(a._currRow)
    , _firstPos(DF_NUM_DIMS)
    , _lastPos(DF_NUM_DIMS)
{
    LOG4CXX_TRACE(logger, __func__ << ": Created for attrId=" << outAttrId);

    // A chunk is being created, so some FastDfArrayIterator has
    // called _makeChunks().  That means that _array._currRow is
    // correctly positioned, and we can derive our chunk boundary.

    _refreshBounds();
}

void FastDfChunk::_refreshBounds()
{
    LOG4CXX_TRACE(logger,  __func__ << ": attr=" << _attrId
                  << ", row=" << _array._currRow);
    _myRow = _array._currRow;

    Dimensions const& outDims = _array.getArrayDesc().getDimensions();
    int64_t const SEQ_INTERVAL = outDims[DF_SEQ_DIM].getChunkInterval();
    _firstPos[DF_INST_DIM] =  _array._localInstance;
    _firstPos[DF_SEQ_DIM] = (_array._currRow - 1) * SEQ_INTERVAL;  // Recall _currRow is 1-based
    _lastPos[DF_INST_DIM] = _array._localInstance;
    _lastPos[DF_SEQ_DIM] = _firstPos[DF_SEQ_DIM] + SEQ_INTERVAL - 1;
}

Coordinates const& FastDfChunk::getFirstPosition(bool /*withOverlap*/) const
{
    bool refreshed = false;
    if (_myRow != _array._currRow) {
        auto self = const_cast<FastDfChunk*>(this);
        self->_refreshBounds();
        refreshed = true;
    }

    LOG4CXX_TRACE(logger,  __func__ << ": attr=" << _attrId
                  << ", _firstPos=" << CoordsToStr(_firstPos)
                  << ", refreshed=" << refreshed);
    return _firstPos;
}

Coordinates const& FastDfChunk::getLastPosition(bool /*withOverlap*/) const
{
    bool refreshed = false;
    if (_myRow != _array._currRow) {
        auto self = const_cast<FastDfChunk*>(this);
        self->_refreshBounds();
        refreshed = true;
    }

    LOG4CXX_TRACE(logger, __func__ << ": attr=" << _attrId
                  << ", _lastPos=" << CoordsToStr(_lastPos)
                  << ", refreshed=" << refreshed);
    return _lastPos;
}

std::shared_ptr<ConstRLEEmptyBitmap> FastDfChunk::getEmptyBitmap() const
{
    ASSERT_EXCEPTION(_myRow == _array._currRow,
                     "Out of step in FastDfChunk::" << __func__);
    return _array._inputEbmIter->getChunk().getEmptyBitmap();
}

ConstChunk const* FastDfChunk::getBitmapChunk() const
{
    ASSERT_EXCEPTION(_myRow == _array._currRow,
                     "Out of step in FastDfChunk::" << __func__);
    return _array._inputEbmIter->getChunk().getBitmapChunk();
}

// ---- FastDfChunkIterator --------------------------------------------

FastDfChunkIterator::FastDfChunkIterator(FastDfChunk const& chunk, int mode)
    : super(&chunk,  mode)
    , _chunk(chunk)
    , _mapper(chunk._firstPos, chunk._lastPos)
{
    SCIDB_ASSERT(_mapper.getNumDims() == DF_NUM_DIMS);
}

Coordinates const& FastDfChunkIterator::getFirstPosition()
{
    return _chunk.getFirstPosition(false);
}

Coordinates const& FastDfChunkIterator::getLastPosition()
{
    return _chunk.getLastPosition(false);
}

Coordinates const& FastDfChunkIterator::getPosition()
{
    SCIDB_ASSERT(bool(inputIterator));

    // Extract logical position from input iterator (if indeed it
    // implements the method).
    position_t here = -1;
    if (_canGetLogicalPos) {
        try {
            here = inputIterator->getLogicalPosition();
        }
        catch (scidb::Exception const& ex) {
            if (ex.getLongErrorCode() != SCIDB_LE_UNREACHABLE_CODE) {
                LOG4CXX_ERROR(logger, __func__
                              << ": Unexpected throw for attr=" << _chunk._attrId
                              << ", reason: " << ex.what());
                ex.raise();
            }

            // The input chunk iterator didn't override getLogicalPosition().
            // No problem, we'll figure it out using the _mapper.
            LOG4CXX_TRACE(logger, __func__
                          << ": No getLogicalPosition() for attr="
                          << _chunk._attrId << ", using the coordinates mapper");
            here = -1;
            _canGetLogicalPos = false; // Failed; unlikely to succeed next time.
        }
    }

    // Looks like we have to do it ourselves?
    if (here < 0) {
        Coordinates inCoords = inputIterator->getPosition();
        Coordinates chunkPos = inputIterator->getChunk().getFirstPosition(/*overlap:*/false);
        FastDfArray const& fda = _chunk._array;
        here = fda._mapper.coord2pos(chunkPos, inCoords); // input schema mapper
    }

    // Convert logical position to output schema coordinates.
    SCIDB_ASSERT(here >= 0);
    _mapper.pos2coord(here, _coords); // output schema mapper (for this chunk)

    // Hah-cha-cha!
    LOG4CXX_TRACE(logger, __func__ << ": attr=" << _chunk._attrId
                  << " positioned at " << CoordsToStr(_coords));
    return _coords;
}

} // namespace
