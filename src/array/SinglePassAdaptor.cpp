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
 * @file SinglePassAdaptor.cpp
 * @author Mike Leibensperger
 * @brief Adaptor converts input array to a SinglePassArray.
 */

#include <array/SinglePassAdaptor.h>
#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.1PassAdaptor"));
  constexpr char const * const cls = "SinglePassAdaptor::";
}

namespace scidb
{

SinglePassAdaptor::SinglePassAdaptor(std::shared_ptr<Array> inArray,
                                     std::shared_ptr<Query> const& query)
    : super(inArray->getArrayDesc())
    , _N_IN_ATTRS(inArray->getArrayDesc().getAttributes(/*exclude:*/false).size())
    , _IN_EBM_ATTR(AttributeID(_N_IN_ATTRS - 1))
    , _inIters(_N_IN_ATTRS)
    , _chunks(_N_IN_ATTRS * _HISTORY_SIZE)
{
    Attributes const& myAttrs = getArrayDesc().getAttributes();
    SCIDB_ASSERT(myAttrs.hasEmptyIndicator());

    setQuery(query);
    pushPipe(inArray);
    SCIDB_ASSERT(getPipeCount() == 1);

    Attributes const& inAttrs = inArray->getArrayDesc().getAttributes();
    for (auto const& attr : inAttrs) {
        _inIters[attr.getId()] = inArray->getConstIterator(attr);
    }
}

size_t SinglePassAdaptor::getCurrentRowIndex() const
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": " << _currRow);
    return _currRow;
}

bool SinglePassAdaptor::moveNext(size_t rowIndex)
{
    if (rowIndex > _currRow + 1) {
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }
    if (rowIndex <= _currRow) {
        // Avoid underflow, don't use subtraction!
        ASSERT_EXCEPTION(rowIndex + _HISTORY_SIZE > _currRow, "Out of step");
        LOG4CXX_TRACE(logger, cls << __func__
                      << "(" << rowIndex << "): Row is in cache, more=true");
        return true;
    }

    if (_atEnd || _inIters[0]->end()) {
        LOG4CXX_TRACE(logger, cls << __func__
                      << "(" << rowIndex << "): At end");
        return false;
    }

    if (0 == _currRow++) {
        LOG4CXX_TRACE(logger, cls << __func__
                      << "(" << rowIndex << "): First call, no increment");
        _updateChunkCache();
        return true;
    }

    LOG4CXX_TRACE(logger, cls << __func__
                  << "(" << rowIndex << "): Advancing input iterators");
    for (auto& it : _inIters) {
        ++(*it);
    }

    _atEnd = _inIters[0]->end(); // update cached value
    if (_atEnd) {
        // Do *not* let _currRow point beyond the end, otherwise
        // SinglePassArray base class calls getChunk() and won't find
        // a current chunk.
        --_currRow;
        LOG4CXX_TRACE(logger, cls << __func__ << "(" << rowIndex
                      << "): Reached end, remain at _currRow=" << _currRow);
    } else {
        _updateChunkCache();
        LOG4CXX_TRACE(logger, cls << __func__ << "(" << rowIndex
                      << "): Chunk cache updated, _currRow=" << _currRow);
    }

    LOG4CXX_TRACE(logger, cls << __func__
                  << "(" << rowIndex << "): Return more=" << bool(!_atEnd));
    return !_atEnd;
}

ConstChunk const&
SinglePassAdaptor::getChunk(AttributeID attr,  size_t rowIndex)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": attr=" << attr << ", row=" << rowIndex);
    SCIDB_ASSERT(attr < _inIters.size());
    if (rowIndex > _currRow || rowIndex +_HISTORY_SIZE <= _currRow) {
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_INVALID_OPERATION_FOR_SEQUENTIAL_MODE);
    }
    return _chunks[_idx(attr, rowIndex)];
}

void SinglePassAdaptor::_updateChunkCache()
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": For row " << _currRow);

    // First pass: copy the payloads.  SAD!!!
    for (AttributeID attr = 0; attr < _N_IN_ATTRS; ++attr) {
        SCIDB_ASSERT(!_inIters[attr]->end());
        size_t idx = _idx(attr, _currRow);
        SCIDB_ASSERT(idx < _chunks.size());
        MemChunk& mc = _chunks[idx];
        ConstChunk const& chunk = _inIters[attr]->getChunk();
        mc.initialize(chunk);
        mc.setPayload(chunk.materialize()); // XXX data copy :-(
    }

    // Second pass: empty bitmap fun.
    MemChunk* ebm = &_chunks[_idx(_IN_EBM_ATTR, _currRow)];
    for (AttributeID attr = 0; attr < _N_IN_ATTRS - 1; ++attr) {
        size_t idx = _idx(attr, _currRow);
        _chunks[idx].setBitmapChunk(ebm);
        SCIDB_ASSERT(_chunks[idx].getBitmapChunk() != nullptr);
    }
}

} // namespace
