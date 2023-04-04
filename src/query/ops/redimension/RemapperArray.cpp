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
 * @file RemapperArray.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "RemapperArray.h"

#include "ChunkIdMap.h"
#include "Remapper.h"

using namespace std;

namespace scidb {

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.redimension.remapperarray"));

RemapperArray::RemapperArray(std::shared_ptr<Array> inputArray,
                             std::shared_ptr<Remapper>& remap,
                             std::shared_ptr<Query> const& query)
    : DelegateArray(inputArray->getArrayDesc(), inputArray, /*isClone:*/false)
    , _remap(remap)
    , _query(query)
    , _N_ATTRS(inputArray->getArrayDesc().getAttributes(true).size())
    , _ID_ATTR(AttributeID(_N_ATTRS - 1))
    , _POS_ATTR(AttributeID(_ID_ATTR - 1))
    , _expectedAttrId(0)
{
    // At least one authentic attribute in the 1-D array.
    SCIDB_ASSERT(_POS_ATTR > 0);
}


DelegateArrayIterator* RemapperArray::createArrayIterator(const AttributeDesc& aid) const
{
    // I have no idea why the base class method needs to be const.  Buehler?  Anyone?
    RemapperArray& self = const_cast<RemapperArray&>(*this);

    if (aid.getId() == _POS_ATTR) {
        auto iter = new WrappedRemapperArrayIterator(self, aid, getPipe(0)->getConstIterator(aid));
        self._posArrayIter = iter->getInputIterator();
        return iter;
    } else if (aid.getId() == _ID_ATTR) {
        auto iter = new WrappedRemapperArrayIterator(self, aid, getPipe(0)->getConstIterator(aid));
        self._idArrayIter = iter->getInputIterator();
        return iter;
    } else {
        return new PassThruRemapperArrayIterator(self, aid, getPipe(0)->getConstIterator(aid));
    }
    SCIDB_UNREACHABLE();
}


/**
 *  Build the two remapped chunks.
 */
void RemapperArray::computeRemappedChunks()
{
    SCIDB_ASSERT(_posArrayIter);
    SCIDB_ASSERT(_idArrayIter);

    // Get hold of input chunks and their iterators.
    ConstChunk const& posChunk = _posArrayIter->getChunk();
    ConstChunk const& idChunk = _idArrayIter->getChunk();
    std::shared_ptr<ConstChunkIterator> posSrc = posChunk.getConstIterator();
    std::shared_ptr<ConstChunkIterator> idSrc = idChunk.getConstIterator();

    // Create output chunks.
    _posChunkPtr.reset(new MemChunk());
    _idChunkPtr.reset(new MemChunk());
    _posChunkPtr->initialize(posChunk);
    _idChunkPtr->initialize(idChunk);
    _posChunkPtr->setBitmapChunk((Chunk*)posChunk.getBitmapChunk()); // use source bitmap chunk during write
    _idChunkPtr->setBitmapChunk((Chunk*)idChunk.getBitmapChunk());

    // Create output iterators.
    int const FLAGS =
        ChunkIterator::NO_EMPTY_CHECK |
        ChunkIterator::SEQUENTIAL_WRITE |
        ChunkIterator::APPEND_EMPTY_BITMAP;  // attach empty bitmap to chunk
    Query::validateQueryPtr(_query); // paranoid
    std::shared_ptr<ChunkIterator> posDst = _posChunkPtr->getIterator(_query, FLAGS);
    std::shared_ptr<ChunkIterator> idDst = _idChunkPtr->getIterator(_query, FLAGS);

    // Fill the chunks with remapped values!
    Value vPos, vId;
    size_t count = 0;
    while (!posSrc->end()) {

        if (!posDst->setPosition(posSrc->getPosition()) ||
            !idDst->setPosition(idSrc->getPosition()))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED)
                << "setPosition() in RemapperArray::computeRemappedChunks()";
        }

        (*_remap)(posSrc->getItem(), idSrc->getItem(), vPos, vId);
        posDst->writeItem(vPos);
        idDst->writeItem(vId);

        ++count;
        ++(*posSrc);
        ++(*idSrc);
    }
    SCIDB_ASSERT(idSrc->end());

    _posChunkPtr->setCount(count);
    _idChunkPtr->setCount(count);
    posDst->flush();
    idDst->flush();

    // unhook the new chunks from the source bitmaps, since the new chunks should have bitmaps attached
    _posChunkPtr->setBitmapChunk(NULL);
    _idChunkPtr->setBitmapChunk(NULL);

    // Prepare for next call.
    ++(*_posArrayIter);
    ++(*_idArrayIter);
}

// ----------------------------------------------------------------------

/**
 *  Enforce horizontal access to attribute chunks.
 *
 *  @note Only the empty bitmap can be accessed out of order.
 */
void RemapperArrayIterator::checkAccessOrder()
{
    if (getAttrId() != _array._expectedAttrId) {
        // Either it's the empty bitmap attribute, or else downstream client is not accessing
        // attributes horizontally as we require.
        if (getAttrId() != _array._N_ATTRS) {
            stringstream ss;
            ss << "RemapperArray requires horizontal attribute traversal, expecting id "
               << _array._expectedAttrId << " but got " << getAttrId() << " instead.";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR) << ss.str();
        }
    }
    else if (++_array._expectedAttrId == _array._N_ATTRS) {
        _array._expectedAttrId = 0;
    }

    // Until the next operator++() call, allow getChunk() to go unchecked.
    _needOrderCheck = false;
}

// ----------------------------------------------------------------------

/**
 *  Get the next chunk for this wrapped array iterator.
 */
std::shared_ptr<MemChunk> WrappedRemapperArrayIterator::getNextChunkPtr()
{
    // Prove we are a WrappedRemapperArrayIterator and *not* a PassThruRemapperArrayIterator.
    SCIDB_ASSERT(getAttrId() == _array._ID_ATTR || getAttrId() == _array._POS_ATTR);

    // When getting the _POS_ATTR chunk, we compute both it and the _ID_ATTR chunk.  When getting
    // the _ID_ATTR chunk, we assume it has been pre-computed.  This assumption is why horizontal
    // access is a requirement.
    std::shared_ptr<MemChunk> result;
    if (getAttrId() == _array._POS_ATTR) {

        // Assert that previous calls consumed stashed chunks.
        SCIDB_ASSERT(!_array._posChunkPtr && !_array._idChunkPtr);

        // Assert that our wrapped iterators both have unread data.
        SCIDB_ASSERT(!_array._posArrayIter->end());
        SCIDB_ASSERT(!_array._idArrayIter->end());

        // Compute and stash new chunks.
        _array.computeRemappedChunks();
        SCIDB_ASSERT(_array._idChunkPtr && _array._posChunkPtr);

        // Consume the _POS_ATTR chunk now.
        result.swap(_array._posChunkPtr);
        SCIDB_ASSERT(!_array._posChunkPtr);

    } else {
        // Consume the stashed _ID_ATTR chunk.
        SCIDB_ASSERT(getAttrId() == _array._ID_ATTR);
        result.swap(_array._idChunkPtr);
        SCIDB_ASSERT(!_array._idChunkPtr);
    }

    return result;
}


ConstChunk const& WrappedRemapperArrayIterator::getChunk()
{
    LOG4CXX_TRACE(logger, "WrapperRemapperArrayIterator::getChunk [wantchunk=" << _wantChunk
                  << "][gotchunk=" << _gotChunk << "]");
    if (end()) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
    }
    if (_needOrderCheck) {
        checkAccessOrder();
    }
    if (_wantChunk > _gotChunk) {
        SCIDB_ASSERT(_gotChunk + 1 == _wantChunk);
        _chunkPtr = getNextChunkPtr();
        ++_gotChunk;
        LOG4CXX_TRACE(logger, "WrapperRemapperArrayIterator::getChunk got chunk pointer [gotchunk="
                      << _gotChunk << "]");
    } else {
        SCIDB_ASSERT(_gotChunk == _wantChunk);
    }
    SCIDB_ASSERT(_chunkPtr);
    return *_chunkPtr;
}


/**
 *  True iff we have walked past the last chunk.
 *
 *  @description We're past the last chunk iff we need a new chunk <i>and cannot get one</i>.
 *  That last subcondition is different depending on which wrapped attribute we are.  The
 *  _ID_ATTR iterator cannot get a new chunk if its input iterator is at its @c end() .  The
 *  _POS_ATTR iterator cannot get a new chunk if one was not left in _array._posChunkPtr by the
 *  _ID_ATTR iterator.
 */
bool WrappedRemapperArrayIterator::end()
{
    return _wantChunk > _gotChunk         // I need a new one...
        && (isPosAttr()
            ? getInputIterator()->end() // ...but no input to compute one with.
            : !haveIdChunk());          // ...but none precomputed for me.
}


void WrappedRemapperArrayIterator::operator++()
{
    SCIDB_ASSERT(_wantChunk == _gotChunk); // ...else getChunk() was never called!
    LOG4CXX_TRACE(logger, "WrapperRemapperArrayIterator::operator++ [wantchunk=" << _wantChunk
                  << "][gotchunk=" << _gotChunk << "]");

    // We don't touch the inputIterator here, instead we note that we want the next chunk and
    // getNextChunkPtr() will get it on the next getChunk() call.
    ++_wantChunk;
    _currPos[0] = _wantChunk * _INTERVAL;
    _needOrderCheck = true;
}

WrappedRemapperArrayIterator::~WrappedRemapperArrayIterator()
{
    LOG4CXX_TRACE(logger, "~WrapperRemapperArrayIterator [wantchunk=" << _wantChunk
                  << "][gotchunk=" << _gotChunk << "]");
}

} // namespace scidb
