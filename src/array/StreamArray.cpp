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

/**
 * @file StreamArray.cpp
 *
 * @brief Array receiving chunks from abstract stream
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include <array/StreamArray.h>
#include <system/Exceptions.h>

#include <log4cxx/logger.h>

#ifndef SCIDB_CLIENT
// All so we can assert if someone attempts to use
// StreamArrayIterator on one of these subclasses.....
#include <array/SinglePassArray.h>
#include <query/RemoteMergedArray.h>
#include <query/PullSGArray.h>
#endif

namespace scidb
{
    using namespace std;
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.streamarray"));

    //
    // StreamArray
    //

    StreamArray::StreamArray(ArrayDesc const& arr, bool emptyable)
    : desc(arr), emptyCheck(emptyable), _iterators(arr.getAttributes().size()), currentBitmapChunk(NULL)
    {
    }

    string const& StreamArray::getName() const
    {
        return desc.getName();
    }

    ArrayID StreamArray::getHandle() const
    {
        return desc.getId();
    }

    ArrayDesc const& StreamArray::getArrayDesc() const
    {
        return desc;
    }

    std::shared_ptr<ArrayIterator> StreamArray::getIteratorImpl(const AttributeDesc& attId)
    {
        throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "StreamArray::getIterator()";
    }

    std::shared_ptr<ConstArrayIterator> StreamArray::getConstIteratorImpl(const AttributeDesc& attr) const
    {
        auto attId = attr.getId();
        if (!_iterators[attId]) {
            ((StreamArray*)this)->_iterators[attId] =
                std::shared_ptr<ConstArrayIterator>(new StreamArrayIterator(*(StreamArray*)this, attId));
        }
        return _iterators[attId];
    }


    //
    // Stream array iterator
    //

    StreamArrayIterator::StreamArrayIterator(StreamArray& arr, AttributeID attr)
        : ConstArrayIterator(arr)
        , _array(arr)
        , _attId(arr.getArrayDesc().getAttributes().findattr(attr))
        , _currentChunk(NULL)
    {
        LOG4CXX_TRACE(logger, "StreamArrayIterator::StreamArrayIterator "
                      << "attr=" << attr);
        moveNext();
    }

#define STRINGIFY(X) #X
#define OUTPUT(VAR) " " << STRINGIFY(VAR) << ": " << (VAR)

    void StreamArrayIterator::moveNext()
    {
        LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__
            << OUTPUT(_array.currentBitmapChunk)
            << OUTPUT(_attId.getId()));

        if (_array.currentBitmapChunk == NULL ||
            _attId.getId() == 0 ||
            _array.currentBitmapChunk->getAttributeDesc().getId() != _attId.getId()) {

            if (_array.currentBitmapChunk
                    && _array.currentBitmapChunk->getAttributeDesc().getId() != _attId.getId()) {
                LOG4CXX_TRACE(logger, "StreamArrayIterator::" << __func__
                              << OUTPUT(_array.currentBitmapChunk->getAttributeDesc().getId()));
            }
            LOG4CXX_TRACE(logger, "StreamArrayIterator::" << __func__
                          << " calling _array.nextChunk(" << OUTPUT(_attId)<<")");

            _currentChunk = _array.nextChunk(_attId, _dataChunk);
            if (_currentChunk != NULL && _array.emptyCheck) {
                AttributeDesc const* bitmapAttr = _array.desc.getEmptyBitmapAttribute();
                if (bitmapAttr != NULL && bitmapAttr->getId() != _attId.getId()) {
                    if (_array.currentBitmapChunk == NULL ||
                        (_array.currentBitmapChunk->getFirstPosition(false) !=
                         _currentChunk->getFirstPosition(false))) {
#ifndef SCIDB_CLIENT
                        assert(!dynamic_cast<scidb::RemoteMergedArray*>(&_array));
                        assert(!dynamic_cast<scidb::PullSGArray*>(&_array));
                        assert(!dynamic_cast<scidb::SinglePassArray*>(&_array));
#endif

                        LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__<< " calling _array.nextChunk("
                                                                     << OUTPUT(bitmapAttr->getId())<<")");
                        _array.currentBitmapChunk = _array.nextChunk(*bitmapAttr, _bitmapChunk);
                        if (!_array.currentBitmapChunk) {
                            throw USER_EXCEPTION(SCIDB_SE_EXECUTION,
                                                 SCIDB_LE_NO_CURRENT_BITMAP_CHUNK);
                        }
                    }
                    assert(_currentChunk == &_dataChunk);
                    if (_array.currentBitmapChunk->getFirstPosition(false) !=
                        _currentChunk->getFirstPosition(false)) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                                               SCIDB_LE_NO_ASSOCIATED_BITMAP_CHUNK);
                    }
                    _dataChunk.setBitmapChunk((Chunk*)_array.currentBitmapChunk);
                    LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__<< "() did setBitmapChunk");
                }
            }
        } else {
            LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__<< "() set _currentChunk to bitmap chunk");
            _currentChunk = _array.currentBitmapChunk;
        }
    }

    ConstChunk const& StreamArrayIterator::getChunk()
    {
        if (!_currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return *_currentChunk;
    }

    bool StreamArrayIterator::end()
    {
        LOG4CXX_TRACE(logger, "StreamArrayIterator::end _attId=" << _attId
                      << ", _currentChunk=" << _currentChunk);
        return _currentChunk == NULL;
    }

    void StreamArrayIterator::operator ++()
    {
        if (_currentChunk != NULL) {
            LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__<< " calling moveNext()");
            moveNext();
        } else {
            LOG4CXX_TRACE(logger, "StreamArrayIterator::"<<__func__<< " _currentChunk null, no moveNext()");
        }
    }

    Coordinates const& StreamArrayIterator::getPosition()
    {
        if (!_currentChunk) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_CHUNK);
        }
        return _currentChunk->getFirstPosition(false);
    }
}
