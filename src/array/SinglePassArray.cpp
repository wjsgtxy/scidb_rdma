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

#include <array/SinglePassArray.h>
#include <log4cxx/logger.h>

namespace {
  constexpr char const * const cls = "SinglePassArray::";
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.singlepass"));
}

namespace scidb {

SinglePassArray::SinglePassArray(ArrayDesc const& arr)
: StreamArray(arr, false),
  _enforceHorizontalIteration(false),
  _consumed(arr.getAttributes().size()),
  _rowIndexPerAttribute(arr.getAttributes().size(), 0)
{}

std::shared_ptr<ConstArrayIterator>
SinglePassArray::getConstIteratorImpl(const AttributeDesc& attr) const
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": attId=" << attr.getId());

    ScopedMutexLock cs(_mutex, PTW_SML_SINGLE_PASS_ARRAY);
    auto attId = attr.getId();
    if (_iterators[attId]) { return _iterators[attId]; }

    // Initialize all attribute iterators at once
    // to avoid unnecessary exceptions
    // (in case when getConstIterator(aid) are not called for all attributes before iteration)
    Exception::Pointer err;
    for (AttributeID a=0, n=safe_static_cast<AttributeID>(_iterators.size());
         a<n;
         ++a)
    {
        try {
            if (!_iterators[a]) {
                StreamArray* self = const_cast<StreamArray*>(static_cast<const StreamArray*>(this));
                std::shared_ptr<ConstArrayIterator> cai(new StreamArrayIterator(*self, a));
                const_cast< std::shared_ptr<ConstArrayIterator>& >(_iterators[a]) = cai;
            }
        } catch (const StreamArray::RetryException& e) {
            if (a == attId) {
                err = e.clone();
            }
        }
    }
    if (err) { err->raise(); }
    assert(_iterators[attId]);
    return _iterators[attId];
}

ConstChunk const*
SinglePassArray::nextChunk(const AttributeDesc& aDesc, MemChunk& chunk)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": attId=" << aDesc.getId()
                  << ", scratchChunk @" << &chunk);

    auto attId = aDesc.getId();
    ScopedMutexLock cs(_mutex, PTW_SML_SINGLE_PASS_ARRAY);

    // Ensure that attributes are consumed horizontally
    while(true) {

        ConstChunk const* result(NULL);
        assert(attId < _rowIndexPerAttribute.size());

        const size_t nAttrs = _rowIndexPerAttribute.size();
        const size_t currRowIndex = getCurrentRowIndex();

        size_t& chunkIndex = _rowIndexPerAttribute[attId];

        if (chunkIndex != currRowIndex) {
            // the requested chunk must be in the current row
            ASSERT_EXCEPTION((currRowIndex > chunkIndex), cls << __func__);
            ASSERT_EXCEPTION((chunkIndex == (currRowIndex-1)
                              || !_enforceHorizontalIteration), cls << __func__);

            result = &getChunk(attId, chunkIndex+1);
            ++chunkIndex;
            ++_consumed;
            assert(_consumed<=nAttrs || !_enforceHorizontalIteration);
            LOG4CXX_TRACE(logger, cls << __func__ << ": attId=" << attId
                          << ", row+1=" << chunkIndex
                          << ", chunk consumed!");
            return result;
        }

        if (_enforceHorizontalIteration && _consumed < nAttrs) {
            // the previous row has not been fully consumed
            LOG4CXX_TRACE(logger, cls << __func__
                          << ": Row " << chunkIndex << " not consumed, retry");
            throw RetryException(REL_FILE, __FUNCTION__, __LINE__);
        }

        assert(_consumed==nAttrs || !_enforceHorizontalIteration);

        if (!moveNext(chunkIndex+1)) {
            // no more chunks
            LOG4CXX_TRACE(logger, cls << __func__
                          << ": No more chunks at row=" << (chunkIndex + 1));
            return result;
        }

        // advance to the next row and get the chunk
        _consumed = 0;
        result = &getChunk(attId, chunkIndex+1);
        assert(result);
        ++chunkIndex;
        ++_consumed;

        if (hasValues(result)) {
            LOG4CXX_TRACE(logger, cls << __func__
                          << ": Return valuable attId=" << attId
                          << " chunk at row=" << chunkIndex);
            return result;
        }

        // run through the rest of the attributes discarding empty chunks
        LOG4CXX_TRACE(logger, cls << __func__ << ": Skipping empty chunks");
        const auto& attrs = getArrayDesc().getAttributes();
        for (const auto& attr : attrs) {
            if (attr.getId() == attId) { continue; }
            ConstChunk const* result = nextChunk(attr, chunk);
            ASSERT_EXCEPTION((!hasValues(result)), cls << __func__);
            assert(getCurrentRowIndex() == _rowIndexPerAttribute[attr.getId()]);
        }
        assert(_consumed == nAttrs);
        assert(getCurrentRowIndex() == _rowIndexPerAttribute[attId]);
    }

    ASSERT_EXCEPTION_FALSE(cls << __func__ << ": Unreachable return?!");
    return NULL;
}

bool
SinglePassArray::hasValues(const ConstChunk* chunk)
{
    bool isEmptyable = (getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool chunkHasVals = (!isEmptyable) || (!chunk->isEmpty());
    return (chunkHasVals && (chunk->getSize() > 0));
}

} // scidb
