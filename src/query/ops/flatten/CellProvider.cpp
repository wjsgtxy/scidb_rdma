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
 * @file CellProvider.cpp
 * @author Mike Leibensperger
 * @brief Extract individual cell values from a Region.
 */

#include "CellProvider.h"

#include <array/ConstChunk.h>
#include <array/ConstChunkIterator.h>
#include <query/Value.h>
#include <system/Utils.h>

#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.flatten.cellprov"));
}

namespace scidb {

CellProvider::CellProvider(RegionProvider& source)
    : _source(source)
    , _mode(ConstChunkIterator::IGNORE_OVERLAPS)
{ }

CellProvider::CellProvider(RegionProvider& source, int mode)
    : _source(source)
    , _mode(mode)
{ }

CellProvider::Iters& CellProvider::_getInIters()
{
    // Lazy initialization of input chunk iterators, to avoid doing
    // too much work in the constructor.

    if (_first) {
        SCIDB_ASSERT(_inIters.empty());
        _first = false;
        bool more = _source.next(_region, _regPos);
        if (more) {
            _makeIters();
        }
    }
    return _inIters;
}

void CellProvider::_makeIters()
{
    SCIDB_ASSERT(_region.size() > 1); // at least EBM + 1 attribute
    _inIters.clear();
    _inIters.reserve(_region.size());
    for (auto const& chunkp : _region) {
        SCIDB_ASSERT(chunkp);
        _inIters.push_back(chunkp->getConstIterator(_mode));
    }
}

bool CellProvider::end() const
{
    // We may be at end-of-chunk and have to load the next region to
    // see if there are more cells.  Rather than sprinkle 'mutable'
    // everywhere, just wrap a non-const private method.

    auto self = const_cast<CellProvider*>(this);
    return self->_end();
}

bool CellProvider::_end()
{
    Iters& iters = _getInIters();
    if (iters.empty()) {
        return true;
    }
    if (iters[0]->end()) {
        for (auto& it : iters) {
            it.reset();
        }
        bool more = _source.next(_region, _regPos);
        if (!more) {
            iters.clear();
            return true;
        }
        _makeIters();
    }
    return false;
}

bool CellProvider::next(Cell& cell, Coordinates& where)
{
    if (end()) {
        return false;
    }

    Iters& iters = _getInIters();
    SCIDB_ASSERT(!iters.empty()); // since end() was false above
    cell.resize(iters.size());    // no-op if already right-sized

    where = iters[0]->getPosition();
    for (size_t i = 0; i < iters.size(); ++i) {
        cell[i] = iters[i]->getItem(); // XXX data copy :-(
        ++(*iters[i]);
    }
    return true;
}

void CellProvider::restart()
{
    _inIters.clear();
    _regPos.clear();
    _region.clear();
    _source.restart();
    _first = true;
}

} // namespace scidb
