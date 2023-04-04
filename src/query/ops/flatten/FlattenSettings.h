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
 * @file FlattenSettings.h
 * @author Mike Leibensperger
 * @brief Settings definitions for the flatten() operator.
 */

#ifndef FLATTEN_SETTINGS_H
#define FLATTEN_SETTINGS_H

#include <query/OperatorParam.h>
#include <system/Utils.h>

namespace scidb {

class FlattenSettings
{
public:

    static constexpr char const * const KW_CELLS_PER_CHUNK = "cells_per_chunk";
    static constexpr char const * const KW_FAST = "_fast";

    FlattenSettings() = default;
    ~FlattenSettings() = default;

    FlattenSettings(std::string const& s) { fromString(s); }

    void init(ArrayDesc const& inSchema,
              Parameters const& params,
              KeywordParameters const& kwParams,
              std::shared_ptr<Query> query);

    std::string toString() const;
    void fromString(std::string const&);

    /// Did init() or fromString() get called?
    bool isValid() const { return _mode != UNDEF; }

    /**
     * @brief flatten(ARRAY [,cells_per_chunk:N])
     *
     * @details In this mode, we're transforming an array of any
     * dimensionality into a dataframe.
     */
    bool flattenMode() const
    {
        SCIDB_ASSERT(isValid());
        return _mode == FLATTEN;
    }

    /**
     * @brief flatten(DATAFRAME [,cells_per_chunk:N])
     *
     * @details In this mode, we already have a dataframe, but we wish to
     * ensure that it is dense (and possibly to dictate its chunk size).
     * We don't add any dimensions-as-attribute prefix: the input
     * dataframe doesn't have any nameable dimensions.
     *
     * @p In this mode we always use SlowDfArray, which produces dense
     * chunks.
     */
    bool rechunkMode() const
    {
        SCIDB_ASSERT(isValid());
        return _mode == RECHUNK;
    }

    /**
     * @brief flatten(ARRAY_OR_DATAFRAME, dimName [,cells_per_chunk:N])
     *
     * @details In this mode, we're turning our input into a 1-D array
     * with dimension dimName, as the old unpack() operator did.  If the
     * input is a dataframe, we don't prefix the dimensions-as-attributes.
     */
    bool unpackMode() const
    {
        SCIDB_ASSERT(isValid());
        return _mode == UNPACK;
    }

    bool inputIsDataframe() const { return _inputIsDataframe; }
    int64_t getChunkSize() const { return _cellsPerChunk; }
    std::string const& getDimName() const { return _dimName; }
    int getMode() const { return static_cast<int>(_mode); }
    bool canUseFastPath() const { return _fastpath; }

private:
    enum Mode { UNDEF, FLATTEN, RECHUNK, UNPACK };
    Mode _mode { UNDEF };
    bool _inputIsDataframe { false };
    int64_t _cellsPerChunk { 0 };
    std::string _dimName;
    bool _fastpath { false };
};

} // namespace

#endif /* !  FLATTEN_SETTINGS_H */
