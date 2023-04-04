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
 * @file RegionProvider.cpp
 * @author Mike Leibensperger
 * @brief Wrap the Array API to provide row-of-chunks sequential access.
 */

#include "RegionProvider.h"

#include <array/Array.h>
#include <array/ArrayDesc.h>
#include <array/Attributes.h>
#include <array/ConstArrayIterator.h>
#include <system/Utils.h>

#include <log4cxx/logger.h>

using namespace std;

// XXX Turn logging to TRACE before commit.

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.flatten.regprov"));

  scidb::Attributes const& getAttrs(scidb::Array const& a)
  {
      return a.getArrayDesc().getAttributes(/*exclude:*/false);
  }
}

namespace scidb {

RegionProvider::RegionProvider(std::shared_ptr<Array>& ary)
    : _array(ary)
{
    SCIDB_ASSERT(ary);
}

RegionProvider::Iters& RegionProvider::_getInIters()
{
    // Lazy initialization of input array iterators, to avoid doing
    // too much work in the constructor.

    if (_inIters.empty()) {
        Attributes const& attrs = getAttrs(*_array);
        _inIters.reserve(attrs.size());
        for (auto const& a : attrs) {
            LOG4CXX_DEBUG(logger, __func__ << ": Make attr=" << a.getId() << " iterator"
                          << (a.isEmptyIndicator() ? " (empty bitmap)" : ""));
            _inIters.push_back(_array->getConstIterator(a));
        }
    }
    return _inIters;
}

bool RegionProvider::end() const
{
    auto self = const_cast<RegionProvider*>(this);
    bool result = self->_getInIters()[0]->end();
    LOG4CXX_DEBUG(logger, __func__ << ": iter[0] returns " << result);
    return result;
}

bool RegionProvider::next(Region& r, Coordinates& where)
{
    if (end()) {
        LOG4CXX_DEBUG(logger, __func__ << ": Already at end, return false");
        return false;
    }

    // Move iterators if necessary.
    Iters& iters = _getInIters();
    SCIDB_ASSERT(!iters.empty());
    r.resize(iters.size());
    if (_first) {
        _first = false;
    } else {
        for (auto& it : iters) {
            ++(*it);
        }
    }
    if (end()) {
        LOG4CXX_DEBUG(logger, __func__ << ": Reached end, return false");
        for (auto& chunkp : r) {
            chunkp = nullptr;
        }
        where.clear();
        return false;
    }

    where = iters[0]->getPosition();

    // XXX This is a naive approach.  Calling getChunk() for
    // all of the (possibly hundreds of) attributes at once
    // risks excessive memory pressure, cache eviction of
    // buffers still needed, and other badness.  Ideally we
    // would have some kind of acquire/keep/release protocol
    // for chunks within the returned region (keep meaning "I
    // want to go on to the next region, but hold on to this
    // chunk from the current region for now").  Divorce chunk
    // descriptor acquisition from chunk buffer
    // filling/pinning/unpinning.
    //
    // But for now we just grab that chunk pointer (with the
    // above bad side effects) and press on.
    //
    Attributes const& attrs = getAttrs(*_array);
    SCIDB_ASSERT(attrs.size() == iters.size());
    SCIDB_ASSERT(attrs.size() == r.size());
    for (size_t i = 0; i < attrs.size(); ++i) {
        r[i] = &iters[i]->getChunk();
        LOG4CXX_DEBUG(logger, __func__ << ": iters[" << i << "] chunk at " << r[i]);
    }

    LOG4CXX_DEBUG(logger, __func__ << ": Region at " << CoordsToStr(where));
    return true;
}

void RegionProvider::restart()
{
    LOG4CXX_DEBUG(logger, __func__ << ": Restart!");
    for (auto& it : _getInIters()) {
        it->restart();
    }
    _first = true;
}

} // namespace scidb
