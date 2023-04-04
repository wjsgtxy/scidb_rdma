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

#include <array/SyntheticArray.h>

#include <array/Array.h>
#include <array/MemChunk.h>
#include <query/Query.h>
#include <query/TypeSystem.h>

#include <log4cxx/logger.h>

#include <ios>                  // for hex and dec

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.synthetic"));
}

namespace scidb {

SyntheticChunkIterator::SyntheticChunkIterator(std::shared_ptr<Array> parray,
                                               const AttributeDesc& attr,
                                               const SyntheticChunk& chunk,
                                               int iterationMode,
                                               std::shared_ptr<ConstChunkIterator> ebmChunkIter)
        : _parray(parray)
        , _attr(attr)
        , _chunk(chunk)
        , _cellCount(chunk.count())
        , _iterationMode(iterationMode)
        , _pos(_parray->getArrayDesc().getDimensions().size())
        , _ebmChunkIter(ebmChunkIter)
        , _bitmapIter(dynamic_cast<RLEBitmapChunkIterator*>(ebmChunkIter.get()))
{
    LOG4CXX_TRACE(logger, _cls << __func__
                  << ": attrId=" << attr.getId()
                  << ", mode=" << hex << _iterationMode << dec);

    ASSERT_EXCEPTION(_bitmapIter, "Expected an RLEBitmapChunkIterator?!");

    // For tile mode, one-time creation of the constant tiles we'll need.
    if (_iterationMode & TILE_MODE) {
        auto tileSize = _bitmapIter->getTileSize();
        SCIDB_ASSERT(tileSize);
        LOG4CXX_TRACE(logger, _cls << __func__
                      << ": tileMode, tileSize=" << tileSize
                      << ", _cellCount=" << _cellCount
                      << ", lastTileSize=" << (_cellCount % tileSize));
        _fullTile = makeTileConstant(
            _attr.getType(), _attr.getDefaultValue(), tileSize);
        _partialTile = makeTileConstant(
            _attr.getType(), _attr.getDefaultValue(), _cellCount % tileSize);
    }

    restart();
}

SyntheticChunkIterator::~SyntheticChunkIterator()
{
    // Nothing to do.
}

bool SyntheticChunkIterator::end()
{
    return _ebmChunkIter->end();
}

void SyntheticChunkIterator::operator ++()
{
    ++(*_ebmChunkIter);
}

Coordinates const& SyntheticChunkIterator::getPosition()
{
    return _ebmChunkIter->getPosition();
}

bool SyntheticChunkIterator::setPosition(Coordinates const& pos)
{
    return _ebmChunkIter->setPosition(pos);
}

void SyntheticChunkIterator::restart()
{
    _ebmChunkIter->restart();
}

int SyntheticChunkIterator::getMode() const
{
    return _iterationMode;
}

Value const& SyntheticChunkIterator::getItem()
{
    if ((_iterationMode & TILE_MODE) == 0) {
        // No tile mode, hooray.
        return _attr.getDefaultValue();
    }
    SCIDB_ASSERT(_cellCount > _bitmapIter->getTilePos());
    if (_cellCount - _bitmapIter->getTilePos() < _bitmapIter->getTileSize()) {
        return _partialTile;
    }
    return _fullTile;
}

bool SyntheticChunkIterator::isEmpty() const
{
    return _ebmChunkIter->isEmpty();
}

ConstChunk const& SyntheticChunkIterator::getChunk()
{
    return _chunk;
}

SyntheticChunk::SyntheticChunk(std::shared_ptr<Array> parray,
                               const AttributeDesc& attr)
        : _parray(parray)
        , _attr(attr)
{
    // Nothing to do.
}

SyntheticChunk::~SyntheticChunk()
{
}

const ArrayDesc& SyntheticChunk::getArrayDesc() const
{
    return _parray->getArrayDesc();
}

const AttributeDesc& SyntheticChunk::getAttributeDesc() const
{
    return _attr;
}

Coordinates const& SyntheticChunk::getFirstPosition(bool withOverlap) const
{
    SCIDB_ASSERT(_ebmChunk);
    return _ebmChunk->getFirstPosition(withOverlap);
}

Coordinates const& SyntheticChunk::getLastPosition(bool withOverlap) const
{
    SCIDB_ASSERT(_ebmChunk);
    return _ebmChunk->getLastPosition(withOverlap);
}

bool SyntheticChunk::isCountKnown() const
{
    SCIDB_ASSERT(_ebmChunk);
    return _ebmChunk->isCountKnown();
}

size_t SyntheticChunk::count() const
{
    SCIDB_ASSERT(_ebmChunk);
    return _ebmChunk->count();
}

std::shared_ptr<ConstChunkIterator> SyntheticChunk::getConstIterator(int iterationMode) const
{
    auto ebmChunkIter = _ebmChunk->getConstIterator(iterationMode);
    return std::make_shared<SyntheticChunkIterator>(_parray,
                                                    _attr,
                                                    *this,
                                                    iterationMode,
                                                    ebmChunkIter);
}

CompressorType SyntheticChunk::getCompressionMethod() const
{
    return _attr.getDefaultCompressionMethod();
}

Array const& SyntheticChunk::getArray() const
{
    return *_parray;
}

void SyntheticChunk::setEbmChunk(const ConstChunk* pebmChunk)
{
    _ebmChunk = pebmChunk;
}

SyntheticArrayIterator::SyntheticArrayIterator(std::shared_ptr<Array> parray,
                                               const AttributeDesc& attr,
                                               std::shared_ptr<Query> query)
    : ConstArrayIterator(*parray)
    , _parray(parray)
    , _chunk(parray, attr)
    , _ebmIter(nullptr)
{
    restart();
}

SyntheticArrayIterator::~SyntheticArrayIterator()
{
    // Nothing to do.
}

ConstChunk const& SyntheticArrayIterator::getChunk()
{
    const auto& ebmChunk = _ebmIter->getChunk();
    const auto* pebmChunk = &ebmChunk;
    _chunk.setEbmChunk(pebmChunk);
    return _chunk;
}

bool SyntheticArrayIterator::end()
{
    return !_ebmIter || _ebmIter->end();
}

void SyntheticArrayIterator::operator++()
{
    SCIDB_ASSERT(_ebmIter);
    ++(*_ebmIter);
}

Coordinates const& SyntheticArrayIterator::getPosition()
{
    SCIDB_ASSERT(_ebmIter);
    return _ebmIter->getPosition();
}

bool SyntheticArrayIterator::setPosition(Coordinates const& pos)
{
    SCIDB_ASSERT(_ebmIter);
    bool success = false;
    success = _ebmIter->setPosition(pos);
    return success;
}

void SyntheticArrayIterator::restart()
{
    const auto ebmAttr = _parray->getArrayDesc().getEmptyBitmapAttribute();
    if (ebmAttr) {
        _ebmIter = _parray->getConstIterator(*ebmAttr);
    }
}

}  // namespace scidb
