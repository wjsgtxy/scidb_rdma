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

#ifndef DENSE_1M_CHUNK_ESTIMATOR_H
#define DENSE_1M_CHUNK_ESTIMATOR_H

#include <array/Dimensions.h>

#include <atomic>
#include <cstdint>

/**
 *  @file Dense1MChunkEstimator.h
 *
 *  @description
 *  "Dense1M" is a static computation used in some contexts to compute
 *  chunk intervals not explicitly specified by the user.  It assumes
 *  that the array for which intervals are to be computed will hold
 *  dense data, with a cell count per chunk of one million---hence
 *  "Dense1M".
 *
 *  Dense1M works like this.  Suppose you have a schema with n
 *  dimensions, of which m ≤ n are unspecified.  Each of those m
 *  dimensions will be given the same chunk interval, computed as the
 *  m-th root of one million divided by the product of the specified
 *  intervals.  (The physical chunk size is pegged at one million, and
 *  the unspecified dimensions each take an equal proportion (m-th
 *  root) of what's left of the space after the specified dimensions
 *  have been factored out.)
 *
 *  @note The magic 1,000,000 number is no longer hardcoded, but is
 *  taken from the target-cells-per-chunk configuration parameter.
 */

namespace scidb {

/**
 *  @brief Implement the "dense one million cell" chunk estimation computation.
 *
 *  @description
 *  Given a set of chunk intervals (possibly held in a Dimensions
 *  vector), compute the value of any unspecified chunk intervals.
 *  There are a couple of ways to do this.
 *
 *  If you have a Dimensions vector that may or may not have
 *  unspecified (that is, DimensionDesc::AUTOCHUNKED) intervals, you
 *  can estimate any missing intervals with a single call to the static
 *  estimate() method:
 *
 *  @code
 *  Dimensions& myDims = ... ;
 *  Dense1MChunkEstimator::estimate(myDims);
 *  @endcode
 *
 *  If you must loop through the dimensions anyway (or if you don't
 *  yet have DimensionDesc objects, as is the case in the AST
 *  Translator), you can add intervals as you learn them and then
 *  estimate a result.  If there were no unspecified intervals, the
 *  result will be zero.
 *
 *  @code
 *  Dense1MChunkEstimator estimator;
 *  for (auto& thing : Things) {
 *      estimator.add(thing.chunkInterval());
 *  }
 *  int64_t result = estimator.estimate();
 *  if (result) {
 *      Dimensions& myDims = ...;
 *      for (auto& dim : myDims) {
 *          if (dim.isAutochunked()) {
 *              dim.setChunkInterval(result);
 *          }
 *      }
 *  }
 *  @endcode
 *
 *  The apply() method does that last "if (result) ..." for-loop for
 *  you, so instead you can write
 *
 *  @code
 *  Dense1MChunkEstimator estimator;
 *  for (auto& thing : Things) {
 *      estimator.add(thing.chunkInterval());
 *  }
 *  Dimensions& myDims = ...;
 *  estimator.apply(myDims);
 *  @endcode
 */
class Dense1MChunkEstimator
{
public:
    Dense1MChunkEstimator();

    /**
     * Include another interval value, specified or not, in the computation.
     *
     * @param interval an interval value, or -1 (AUTOCHUNKED)
     */
    void add(int64_t interval);

    /**
     * Compute result, or zero if no unspecified intervals were added.
     *
     * @param unknown the number of unspecified chunk intervals
     * @return estimated chunk interval value, or zero if no estimate needed
     *
     * @note You can either call add() with @em all values including
     *       -1's, @em or you can call add() with only specified
     *       (positive) values and then estimate(m) for m > 0.  Trying
     *       to "mix modes" causes an assertion failure.
     */
    int64_t estimate(unsigned unknown = 0);

    /**
     * Compute result and replace unspecified intervals.
     *
     * @param dims dimension vector to update
     * @returns result from internal estimate() call
     */
    int64_t apply(Dimensions& dims);

    /**
     * Return product of intervals added so far.
     *
     * @description
     * When there are no unspecified intervals, the Translator still
     * wants to know the physical size of the chunk.  This accessor
     * returns that value.
     */
    int64_t getKnownSize() const { return _knownSize; }

    /**
     * Replace unspecified chunk intervals with estimates in-place.
     * @returns result from internal estimate() call
     */
    static int64_t estimate(Dimensions& dims);

private:
    int64_t     _knownSize;
    unsigned    _nKnown;
    unsigned    _nUnknown;

    static std::atomic<int64_t> s_targetCellCount;
};

} // namespace

#endif  /* ! DENSE_1M_CHUNK_ESTIMATOR_H */
