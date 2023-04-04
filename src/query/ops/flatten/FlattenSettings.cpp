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
 * @file FlattenSettings.cpp
 * @author Mike Leibensperger
 * @brief Parameter parsing/validation for the flatten() operator.
 */

#include "FlattenSettings.h"

#include <array/Metadata.h>
#include <query/Expression.h>
#include <query/UserQueryException.h>
#include <system/Config.h>

#include <boost/lexical_cast.hpp>

using namespace std;

namespace scidb {

constexpr char const * const FlattenSettings::KW_CELLS_PER_CHUNK;
constexpr char const * const FlattenSettings::KW_FAST;

void FlattenSettings::init(ArrayDesc const& inSchema,
                           Parameters const& params,
                           KeywordParameters const& kwParams,
                           std::shared_ptr<Query> query)
{
    _inputIsDataframe = inSchema.isDataframe();

    if (params.empty()) {
        _dimName.clear();
    } else {
        SCIDB_ASSERT(params.size() == 1);
        auto const lexp = dynamic_cast<OperatorParamDimensionReference const *>(
            params[0].get());
        SCIDB_ASSERT(lexp);
        _dimName = lexp->getObjectName();
    }

    auto pos = kwParams.find(KW_CELLS_PER_CHUNK);
    if (pos == kwParams.end()) {
        _cellsPerChunk = 0;     // illegal value
    } else {
        _cellsPerChunk = paramToInt64(pos->second);
        if (_cellsPerChunk < 1) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER,
                                       pos->second->getParsingContext())
                << KW_CELLS_PER_CHUNK;
        }
    }

    // Infer the mode.
    if (!_dimName.empty()) {
        _mode = UNPACK;
    } else if (_inputIsDataframe) {
        _mode = RECHUNK;
    } else {
        _mode = FLATTEN;
    }

    // Configured to allow use of FastDfArray?
    _fastpath = Config::getInstance()->getOption<int32_t>(
        CONFIG_FLATTEN_FASTPATH);
    pos = kwParams.find(KW_FAST);
    if (pos != kwParams.end()) {
        Parameter p = pos->second;
        auto lexp = dynamic_cast<OperatorParamLogicalExpression*>(p.get());
        _fastpath = evaluate(lexp->getExpression(), TID_BOOL).getBool();
    }

    // Determine output chunk length if not explicitly supplied.
    if (_cellsPerChunk == 0) {
        if (_mode != FLATTEN) {
            // Either UNPACK or RECHUNK mode, and no explicit
            // chunkSize given.  Use the system default (but pare it
            // down if there's no way the input array is that big).

            _cellsPerChunk = Config::getInstance()->getOption<size_t>(
                CONFIG_TARGET_CELLS_PER_CHUNK);
            SCIDB_ASSERT(_cellsPerChunk > 0);

            //AP: If there's no way that the entire input array has one
            //million elements - reduce the chunk size further.  This is
            //ONLY done for aesthetic purposes - don't want to see one
            //million "()" on the screen.  In fact, sometimes it can
            //become a liability...

            uint64_t total = inSchema.getSize();
            if (total < static_cast<uint64_t>(_cellsPerChunk)) {
                _cellsPerChunk = std::max<size_t>(total, 1); // paranoid
            }
        }
        else if (inSchema.isAutochunked()) {
            // Input doesn't know it's chunk size, so for now, neither do we.
            _cellsPerChunk = DimensionDesc::AUTOCHUNKED;
        }
        else {
            // By flattening with a chunk size equal to the volume of
            // the input chunks, we can avoid rewriting their
            // payloads.  Row-major order of values in an MxN 2-D
            // chunk is the same as for a 1-D chunk of length MxN.
            // (This is the "fast" case in PhysicalFlatten.)

            _cellsPerChunk = getChunkVolume(inSchema.getDimensions());
            SCIDB_ASSERT(_cellsPerChunk > 0);
        }
    }
}


std::string FlattenSettings::toString() const
{
    ostringstream oss;
    oss << int(_mode)
        << ' ' << int(_fastpath)
        << ' ' << int(_inputIsDataframe)
        << ' ' << _cellsPerChunk
        << ' ' << _dimName
        ;
    return oss.str();
}


void FlattenSettings::fromString(std::string const& s)
{
    using boost::lexical_cast;
    using boost::bad_lexical_cast;

    istringstream iss(s);
    string token;
    try {
        iss >> token;
        _mode = Mode(lexical_cast<int>(token));
        iss >> token;
        _fastpath = bool(lexical_cast<int>(token));
        iss >> token;
        _inputIsDataframe = bool(lexical_cast<int>(token));
        iss >> token;
        _cellsPerChunk = lexical_cast<int64_t>(token);
        iss >> token;
        _dimName = token;
    }
    catch (bad_lexical_cast&) {
        ostringstream oss;
        oss << "Bad lexical cast in " << __PRETTY_FUNCTION__
            << ": token='" << token
            << "', s='" << s << '\'';
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNKNOWN_ERROR)
            << oss.str();
    }
}

} // namespace scidb
