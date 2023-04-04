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
 * @file SinglePassAdaptor.h
 * @author Mike Leibensperger
 * @brief Adaptor converts input to a SinglePassArray with horizontal iteration.
 *
 * @details You have an Array subclass that allows horizontal
 * (row-wise) chunk iteration, but you need to conform to the
 * SinglePassArray API.  This adaptor is here to help.
 */

#ifndef SINGLE_PASS_ADAPTOR_H
#define SINGLE_PASS_ADAPTOR_H

#include <array/SinglePassArray.h>

namespace scidb
{

/**
 *  Adaptor to get SinglePassArray behavior.
 */
class SinglePassAdaptor : public SinglePassArray
{
    using super = SinglePassArray;

public:
    // 'structors
    SinglePassAdaptor(std::shared_ptr<Array> inArray,
                      std::shared_ptr<Query> const& query);
    virtual ~SinglePassAdaptor() = default;

    // Non-copyable, non-assignable.
    SinglePassAdaptor(SinglePassAdaptor const&) = delete;
    SinglePassAdaptor& operator=(SinglePassAdaptor const&) = delete;

protected:
    // Customization points.
    size_t getCurrentRowIndex() const override;
    bool moveNext(size_t rowIndex) override;
    ConstChunk const& getChunk(AttributeID attr,  size_t rowIndex) override;

private:
    size_t const _N_IN_ATTRS;
    AttributeID const _IN_EBM_ATTR;
    mgd::vector<std::shared_ptr<ConstArrayIterator>> _inIters;
    size_t _currRow { 0 };
    bool _atEnd { false };

    // StreamArray::nextChunk() comment implies we must maintain a
    // cache.  Sad.
    static constexpr size_t _HISTORY_SIZE = 2;
    mgd::vector<MemChunk> _chunks;
    void _updateChunkCache();
    size_t _idx(AttributeID id, size_t row) const
    {
        return size_t(id) + (row % _HISTORY_SIZE) * _N_IN_ATTRS;
    }
};

} // namespace

#endif /* ! SINGLE_PASS_ADAPTOR_H */
