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
 * @file SlowDfArray.h
 * @author Mike Leibensperger
 * @brief "Slow path" array-to-dataframe converter.
 */

#ifndef SLOW_DF_ARRAY_H
#define SLOW_DF_ARRAY_H

#include "CellProvider.h"
#include <array/DelegateArray.h>

namespace scidb
{

/**
 * "Slow path" array-to-dataframe converter.
 *
 * @details Here, the desired dataframe chunk size does *not* match
 * the chunk volume of the input array.  Every chunk will have to be
 * rewritten on the way through.
 *
 * Enforces horizontal-by-chunk iteration, because additional chunks
 * must be generated for the original input dimension values.
 */
class SlowDfArray : public DelegateArray
{
    friend class SlowDfArrayIterator;
    static constexpr char const * const _cls = "SlowDfArray::";

public:
    // 'structors
    SlowDfArray(ArrayDesc const& outSchema,
                std::shared_ptr<Array> inArray,
                std::shared_ptr<Query> const& query);

    virtual ~SlowDfArray() = default;

    // Non-copyable, non-assignable.
    SlowDfArray(SlowDfArray const&) = delete;
    SlowDfArray& operator=(SlowDfArray const&) = delete;

    // No restart() implemented for SlowDfArray at this point, so
    // SINGLE_PASS for now.  @see FastDfArray::getSupportedAccess()
    Access getSupportedAccess() const override { return SINGLE_PASS; }

    // All your array iterator are belong-to-us.
    DelegateArrayIterator* createArrayIterator(AttributeDesc const& aDesc)
        const override;

private:
    bool const _DF_INPUT;
    size_t const _N_IN_DIMS;
    size_t const _N_IN_ATTRS;   // includes empty bitmap
    size_t const _N_OUT_ATTRS;  // includes empty bitmap
    AttributeID const _OUT_EBM_ATTR;

    InstanceID _localInstance;

    size_t _currRow { 0 };
    Coordinates _currPos;
    Coordinates _prevPos;       // Intentionally not initialized in ctor!
    mgd::vector<MemChunk> _chunks;

    RegionProvider _regionProvider;
    CellProvider _cellProvider;

    bool _makeChunks(size_t row);
    void _restart();
};


class SlowDfArrayIterator : public DelegateArrayIterator
{
    using super = DelegateArrayIterator;
    static constexpr char const * const _cls = "SlowDfArrayIterator::";

public:
    SlowDfArrayIterator(SlowDfArray& array, AttributeDesc const& outAttr);
    bool setPosition(Coordinates const& pos) override;
    void restart() override;
    void operator++() override;
    ConstChunk const& getChunk() override;
    bool end() override;
    Coordinates const& getPosition() override;

private:
    SlowDfArray& _array;
    size_t _myRow { 1 };        // ...to trigger initial _makeChunks() call.
};

} // namespace

#endif /* ! SLOW_DF_ARRAY_H */
