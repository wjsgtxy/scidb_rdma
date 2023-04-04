/**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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

#include <array/Dense1MChunkEstimator.h>


#include <array/DimensionDesc.h>
#include <system/Config.h>
#include <util/Utility.h>

using namespace std;

namespace scidb {

std::atomic<int64_t> Dense1MChunkEstimator::s_targetCellCount(0);

Dense1MChunkEstimator::Dense1MChunkEstimator()
    : _knownSize(1L)
    , _nKnown(0)
    , _nUnknown(0)
{
    if (s_targetCellCount.load() == 0) {
        // All threads will eventually get the same answer...
        size_t target = Config::getInstance()->getOption<size_t>(CONFIG_TARGET_CELLS_PER_CHUNK);
        if (!target) {
            target = 1000000UL; // See SDB-1819.
        }
        s_targetCellCount = safe_static_cast<int64_t>(target);
        SCIDB_ASSERT(s_targetCellCount.load() > 0);
    }
}

void Dense1MChunkEstimator::add(int64_t interval)
{
    if (interval > 0) {
        _knownSize *= interval;
        ++_nKnown;
    } else {
        ++_nUnknown;
    }
}

int64_t Dense1MChunkEstimator::estimate(unsigned numUnknown)
{
    SCIDB_ASSERT(_knownSize > 0);

    // Guard against "mixed mode" usage.
    SCIDB_ASSERT(!(numUnknown && _nUnknown));

    if (_nUnknown == 0) {
        _nUnknown = numUnknown;
    }
    if (_nUnknown == 0) {
        return 0;
    }

    int64_t r = static_cast<int64_t>(
        floor(pow(max(1L,s_targetCellCount.load()/_knownSize),
                  1.0/static_cast<double>(_nUnknown))));

    SCIDB_ASSERT(r > 0);
    return r;
}

int64_t Dense1MChunkEstimator::apply(Dimensions& dims)
{
    SCIDB_ASSERT((_nUnknown + _nKnown) == dims.size());
    int64_t result = estimate();
    if (result) {
        for (auto& dim : dims) {
            if (dim.isAutochunked()) {
                dim.setChunkInterval(result);
            }
        }
    }
    return result;
}

int64_t Dense1MChunkEstimator::estimate(Dimensions& dims)
{
    Dense1MChunkEstimator estimator;
    for (auto& dim : dims) {
        estimator.add(dim.getRawChunkInterval());
    }
    return estimator.apply(dims);
}

} // namespace
