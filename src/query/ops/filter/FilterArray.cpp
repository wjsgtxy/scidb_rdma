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
 * FilterArray.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <memory>


#include "array/Array.h"
#include "query/ops/filter/FilterArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"
#include <log4cxx/logger.h>


using namespace std;

namespace scidb {

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.filter"));

    //
    // Filter chunk iterator methods
    //
    inline Value& FilterChunkIterator::evaluate()
    {
        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
                case BindInfo::BI_ATTRIBUTE:
                    _params[i] = _iterators[i]->getItem();
                    assert((_iterators[i]->getMode() & IGNORE_OVERLAPS)  ==
                           (inputIterator->getMode() & IGNORE_OVERLAPS));
                    break;

                case BindInfo::BI_COORDINATE:
                    if (_mode & TILE_MODE) {
                        _iterators[i]->getItem().getTile()->getCoordinates(
                            _array.getPipe(0)->getArrayDesc(),
                            _array.bindings[i].resolvedId,
                            _iterators[i]->getChunk().getFirstPosition(false),
                            _iterators[i]->getPosition(),
                            _params[i],
                            !(_mode & IGNORE_OVERLAPS));
                    } else {
                        _params[i].setInt64(inputIterator->getPosition()[_array.bindings[i].resolvedId]);
                    }
                    break;

                default:
                    break;
            }
        }
        auto& v = const_cast<Value&>(_array.expression->evaluate(_params));
        return v;
    }

    inline bool FilterChunkIterator::filter()
    {
        Value const& result = evaluate();
        return !result.isNull() && result.getBool();
    }

    void FilterChunkIterator::moveNext()
    {
        ++(*inputIterator);
        if (!inputIterator->end()) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    ++(*_iterators[i]);
                }
            }
        }
    }

    void FilterChunkIterator::nextVisible()
    {
        while (!inputIterator->end()) {
            if ((_mode & TILE_MODE) || filter()) {
                _hasCurrent = true;
                return;
            }
            moveNext();
        }
        _hasCurrent = false;
    }

    void FilterChunkIterator::restart()
    {
        inputIterator->restart();
        if (!inputIterator->end()) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    _iterators[i]->restart();
                }
            }
        }
        nextVisible();
    }

    bool FilterChunkIterator::end()
    {
        return !_hasCurrent;
    }

    Value const& FilterChunkIterator::getItem()
    {
        LOG4CXX_TRACE(logger, "FCI::getItem");
        if (_mode & TILE_MODE) {
            RLEPayload* newEmptyBitmap = evaluate().getTile();
            RLEPayload::iterator ei(newEmptyBitmap);
            Value const& value = inputIterator->getItem();
            RLEPayload* inputPayload = value.getTile();
            RLEPayload::iterator vi(inputPayload);

			// This needs to compare against getMaxLength() or multiple tests
			// will fail with scidb::SCIDB_SE_NETWORK::SCIDB_LE_CANT_SEND_RECEIVE.
            if (newEmptyBitmap->count() == CoordinateBounds::getMaxLength()) {
                assert(newEmptyBitmap->nSegments() == 1);
                if (ei.isNull() == false && ei.checkBit()) {
                    // empty bitmap containing all ones: just return original value
                    return value;
                }
                _tileValue.getTile()->clear();
                LOG4CXX_TRACE(logger, "FCI::getItem cleared tile value");
            } else {
                RLEPayload::append_iterator appender(_tileValue.getTile());
                Value v;
                while (!ei.end()) {
                    uint64_t count = ei.getRepeatCount();
                    LOG4CXX_TRACE(logger, "FCI::getItem seg count=" << count <<
                                  " isnull=" << ei.isNull() << " checkbit=" <<
                                  (ei.isNull() ? "<n/a>" :
                                   ei.checkBit() ? "1" : "0"));
                    if (ei.isNull() == false && ei.checkBit()) {
                        count = appender.add(vi, count);
                    } else {
                        vi += count;
                    }
                    ei += count;
                }
                appender.flush();
                LOG4CXX_TRACE(logger, "FCI::getItem constructed tile value");
            }
            LOG4CXX_TRACE(logger, "FCI::getItem returning tile value");
            return _tileValue;
        }
        LOG4CXX_TRACE(logger, "FCI::getItem returning inputIterator getItem");
        return inputIterator->getItem();
    }

    bool FilterChunkIterator::setPosition(Coordinates const& pos)
    {
        if (inputIterator->setPosition(pos)) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    if (!_iterators[i]->setPosition(pos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            return _hasCurrent = (_mode & TILE_MODE) || filter();
        }
        return _hasCurrent = false;
    }

    void FilterChunkIterator::operator ++()
    {
        moveNext();
        nextVisible();
    }

    FilterChunkIterator::FilterChunkIterator(FilterArrayIterator const& arrayIterator,
                                             DelegateChunk const* chunk,
                                             int iterationMode)
    : DelegateChunkIterator(chunk, iterationMode),
      _array((FilterArray&)arrayIterator.array),
      _iterators(_array.bindings.size()),
      _params(*_array.expression),
      _mode(iterationMode),
      _type(chunk->getAttributeDesc().getType()),
      _query(Query::getValidQueryPtr(_array._query))
    {
        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
              case BindInfo::BI_COORDINATE:
                if (_mode & TILE_MODE) {
                    if (arrayIterator.iterators[i] == arrayIterator.getInputIterator()) {
                        _iterators[i] = inputIterator;
                    } else {
                        _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(iterationMode);

                    }
                }
                break;
              case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID)_array.bindings[i].resolvedId == arrayIterator.inputAttrID) {
                    _iterators[i] = inputIterator;
                } else {
                    _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(
                        ((_mode & TILE_MODE) |
                         (inputIterator->getMode() & IGNORE_OVERLAPS)));
                }
                break;
              case BindInfo::BI_VALUE:
                _params[i] = _array.bindings[i].value;
                break;
              default:
                break;
            }
        }
        if (iterationMode & TILE_MODE) {
            _tileValue = Value(TypeLibrary::getType(chunk->getAttributeDesc().getType()),Value::asTile);
            if (arrayIterator.emptyBitmapIterator) {
                _emptyBitmapIterator =
                    arrayIterator.emptyBitmapIterator->getChunk().getConstIterator(TILE_MODE);
            } else {
                ArrayDesc const& arrayDesc = chunk->getArrayDesc();
                Address addr(arrayDesc.getEmptyBitmapAttribute()->getId(), chunk->getFirstPosition(false));
                _shapeChunk.initialize(&_array, &arrayDesc, addr, CompressorType::NONE);
                _emptyBitmapIterator = _shapeChunk.getConstIterator(TILE_MODE);
            }
        }
        nextVisible();
    }

    inline Value& FilterChunkIterator::buildBitmap()
    {
        Value& value = evaluate();
        RLEPayload* inputPayload = value.getTile();
        RLEPayload::append_iterator appender(_tileValue.getTile());
        RLEPayload::iterator vi(inputPayload);
        if (!_emptyBitmapIterator->setPosition(inputIterator->getPosition()))
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        RLEPayload* emptyBitmap = _emptyBitmapIterator->getItem().getTile();
        RLEPayload::iterator ei(emptyBitmap);

#ifndef NDEBUG
        position_t prevPos = 0;
#endif

        Value trueVal, falseVal;
        trueVal.setBool(true);
        falseVal.setBool(false);
        while (!ei.end()) {
#ifndef NDEBUG
            position_t currPos = ei.getPPos();
#endif
            assert (prevPos == currPos);
            uint64_t count;
            if (ei.checkBit()) {
                count = min(vi.getRepeatCount(), ei.getRepeatCount());
                appender.add((vi.isNull()==false && vi.checkBit()) ? trueVal : falseVal, count);
                vi += count;
            } else {
                count = ei.getRepeatCount();
                appender.add(falseVal, count);
            }
            ei += count;

#ifndef NDEBUG
            prevPos = currPos + count;
#endif
        }
        appender.flush();
        return _tileValue;
    }

    //
    // Exited bitmap chunk iterator methods
    //
    Value& ExistedBitmapChunkIterator::getItem()
    {
        if (_mode & TILE_MODE) {
            return buildBitmap();
        } else {
            _value.setBool(inputIterator->getItem().getBool() && filter());
            return _value;
        }
    }

    ExistedBitmapChunkIterator::ExistedBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : FilterChunkIterator(arrayIterator, chunk, iterationMode), _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // New bitmap chunk iterator methods
    //
    Value& NewBitmapChunkIterator::getItem()
    {
        return (_mode & TILE_MODE) ? buildBitmap() : evaluate();
    }

    NewBitmapChunkIterator::NewBitmapChunkIterator(FilterArrayIterator const& arrayIterator, DelegateChunk const* chunk, int iterationMode)
    : FilterChunkIterator(arrayIterator, chunk, iterationMode)
    {
    }

    //
    // Filter array iterator methods
    //

    ConstChunk const& FilterArrayIterator::getChunk()
    {
        _chunkPtr()->setInputChunk(inputIterator->getChunk());
        _chunkPtr()->overrideClone(false);
        return *_chunkPtr();
    }


    bool FilterArrayIterator::setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        if (inputIterator->setPosition(pos)) {
            for (size_t i = 0, n = iterators.size(); i < n; i++) {
                if (iterators[i]) {
                    if (!iterators[i]->setPosition(pos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            if (emptyBitmapIterator) {
                if (!emptyBitmapIterator->setPosition(pos))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            return true;
        }
        return false;
    }

    void FilterArrayIterator::restart()
    {
        chunkInitialized = false;
        inputIterator->restart();
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            if (iterators[i] && iterators[i] != inputIterator) {
                iterators[i]->restart();
            }
        }
        if (emptyBitmapIterator) {
            emptyBitmapIterator->restart();
        }
    }

    void FilterArrayIterator::operator ++()
    {
        chunkInitialized = false;
        ++(*inputIterator);
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            if (iterators[i] && iterators[i] != inputIterator) {
                ++(*iterators[i]);
            }
        }
        if (emptyBitmapIterator) {
            ++(*emptyBitmapIterator);
        }
    }

    FilterArrayIterator::FilterArrayIterator(FilterArray const& array,
                                             const AttributeDesc& outAttrID,
                                             const AttributeDesc& inAttrID)
    : DelegateArrayIterator(array, outAttrID, array.getPipe(0)->getConstIterator(inAttrID)),
      iterators(array.bindings.size()),
      inputAttrID(inAttrID.getId())
    {
        for (size_t i = 0, n = iterators.size(); i < n; i++) {
            switch (array.bindings[i].kind) {
              case BindInfo::BI_ATTRIBUTE:
                if ((AttributeID)array.bindings[i].resolvedId == inAttrID.getId()) {
                    iterators[i] = inputIterator;
                } else {
                    const auto& attr =
                        array.getPipe(0)->getArrayDesc().getAttributes().findattr(array.bindings[i].resolvedId);
                    iterators[i] = array.getPipe(0)->getConstIterator(attr);
                }
                break;
              case BindInfo::BI_COORDINATE:
                if (array._tileMode) {
                    AttributeDesc const* emptyAttr = array.getPipe(0)->getArrayDesc().getEmptyBitmapAttribute();
                    if (emptyAttr == NULL || emptyAttr->getId() == inputAttrID) {
                        iterators[i] = inputIterator;
                    } else {
                        assert(emptyAttr);
                        iterators[i] = array.getPipe(0)->getConstIterator(*emptyAttr);
                    }
                }
                break;
              default:
                break;
            }
        }
        if (array._tileMode) {
            const auto* emptyAttr = array.getPipe(0)->getArrayDesc().getEmptyBitmapAttribute();
            if (emptyAttr != NULL) {
                emptyBitmapIterator = array.getPipe(0)->getConstIterator(*emptyAttr);
            }
        }
    }

    //
    // Filter array methods
    //
    DelegateChunk* FilterArray::createChunk(DelegateArrayIterator const* iterator, AttributeID id) const
    {
        DelegateChunk* chunk = DelegateArray::createChunk(iterator, id);
        chunk->overrideClone(!chunk->getAttributeDesc().isEmptyIndicator());
        return chunk;
    }


    DelegateChunkIterator* FilterArray::createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        FilterArrayIterator const& arrayIterator = (FilterArrayIterator const&)chunk->getArrayIterator();
        AttributeDesc const& attr = chunk->getAttributeDesc();
        if (_tileMode/* && chunk->isRLE()*/) {
            iterationMode |= ChunkIterator::TILE_MODE;
        } else {
            iterationMode &= ~ChunkIterator::TILE_MODE;
        }
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;
        return attr.isEmptyIndicator()
            // TODO this is troublesome because, when all empty bitmaps are at position zero, it becomes
            // impossible to tell if, in this case, attr maps to an existing bitmap chunk iterator or
            // a new one needs to be created.  I suppose that it's always safe to create a new one, but is
            // likely inefficient.   I suppose we could check if attr.getId() is attribute zero in that case.
            //? (attr.getId() >= inputArray->getArrayDesc().getAttributes().size())
            //    ? (DelegateChunkIterator*)new NewBitmapChunkIterator(arrayIterator, chunk, iterationMode)
            //    : (DelegateChunkIterator*)new ExistedBitmapChunkIterator(arrayIterator, chunk, iterationMode)
            ? (DelegateChunkIterator*)new NewBitmapChunkIterator(arrayIterator, chunk, iterationMode)
            : (DelegateChunkIterator*)new FilterChunkIterator(arrayIterator, chunk, iterationMode);
    }

    DelegateArrayIterator* FilterArray::createArrayIterator(const AttributeDesc& attrID) const
    {
        AttributeID inputAttrID = attrID.getId();
        if (inputAttrID >= getPipe(0)->getArrayDesc().getAttributes().size()) {
            inputAttrID = 0;
            for (size_t i = 0, n = bindings.size(); i < n; i++) {
                if (bindings[i].kind == BindInfo::BI_ATTRIBUTE) {
                    inputAttrID = (AttributeID)bindings[i].resolvedId;
                    break;
                }
            }
        }
        const auto& inputAttr = getArrayDesc().getAttributes().findattr(inputAttrID);
        return new FilterArrayIterator(*this, attrID, inputAttr);
    }

    FilterArray::FilterArray(ArrayDesc const& desc, std::shared_ptr<Array> const& array,
                             std::shared_ptr< Expression> expr, std::shared_ptr<Query>& query,
                             bool tileMode)
    : DelegateArray(desc, array), expression(expr), bindings(expr->getBindings()), _tileMode(tileMode),
      cacheSize(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE)),
      emptyAttrID(desc.getEmptyBitmapAttribute()->getId())
    {
        assert(query);
        _query=query;
    }

}
