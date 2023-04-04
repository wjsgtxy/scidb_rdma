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
 * @file RedimSettings.cpp
 * @brief Common keyword settings shared by repart() and redimension().
 */

#include "RedimSettings.h"

#include <query/Expression.h>
#include <system/Config.h>

namespace scidb {

const char* const RedimSettings::KW_CELLS_PER_CHUNK = "cells_per_chunk";
const char* const RedimSettings::KW_PHYS_CHUNK_SIZE = "phys_chunk_size";
const char* const RedimSettings::KW_COLLISION_RATIO = "_collision_ratio";
const char* const RedimSettings::KW_OFFSET = "_offset";

constexpr float RedimSettings::DEFAULT_COLLISION_RATIO; // linkage needed, sigh

namespace {
    int64_t evalInt64Expr(Parameter p, bool logical)
    {
        if (logical) {
            OperatorParamLogicalExpression* exp =
                dynamic_cast<OperatorParamLogicalExpression*>(p.get());
            SCIDB_ASSERT(exp != nullptr);
            return evaluate(exp->getExpression(), TID_INT64).getInt64();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            SCIDB_ASSERT(exp != nullptr);
            return exp->getExpression()->evaluate().getInt64();
        }
    }

    float evalFloatExpr(Parameter p, bool logical)
    {
        if (logical) {
            OperatorParamLogicalExpression* exp =
                dynamic_cast<OperatorParamLogicalExpression*>(p.get());
            SCIDB_ASSERT(exp != nullptr);
            return evaluate(exp->getExpression(), TID_FLOAT).getFloat();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            SCIDB_ASSERT(exp != nullptr);
            return exp->getExpression()->evaluate().getFloat();
        }
    }
}

void RedimSettings::init(std::string const& opName,
                         KeywordParameters const& kwParams,
                         bool logicalOp)
{
    auto const& cpc = kwParams.find(KW_CELLS_PER_CHUNK);
    auto const& pcs = kwParams.find(KW_PHYS_CHUNK_SIZE);
    auto const& cnr = kwParams.find(KW_COLLISION_RATIO);
    auto const& off = kwParams.find(KW_OFFSET);

    if (logicalOp) {
        // Do some error checking in the logical operator at inferSchema() time.  First,
        // cells_per_chunk and phys_chunk_size are mutually exclusive.
        if (cpc != kwParams.end() && pcs != kwParams.end()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_MUTUALLY_EXCLUSIVE_PARAMS)
                << opName << KW_CELLS_PER_CHUNK << KW_PHYS_CHUNK_SIZE;
        }
    }

    if (cpc != kwParams.end()) {
        _cellsPerChunk = evalInt64Expr(cpc->second, logicalOp);
        if (logicalOp && _cellsPerChunk < 1) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER)
                << KW_CELLS_PER_CHUNK;
        }
    }

    if (pcs != kwParams.end()) {
        _physChunkSize = evalInt64Expr(pcs->second, logicalOp);
        if (logicalOp && _physChunkSize < 1) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_PARAMETER_NOT_POSITIVE_INTEGER)
                << KW_PHYS_CHUNK_SIZE;
        }
    }

    if (cnr != kwParams.end()) {
        _collisionRatio = evalFloatExpr(cnr->second, logicalOp);
        if (logicalOp &&
            (_collisionRatio < 0.0 || _collisionRatio > DEFAULT_COLLISION_RATIO)) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_PARAMETER_OUT_OF_RANGE)
                << KW_COLLISION_RATIO << 0.0 << DEFAULT_COLLISION_RATIO;
        }
    }

    if (off != kwParams.end()) {
        // The length of the _offset vector needs to match the target
        // schema, but we can't check that here.  Just build the vector.
        auto group = dynamic_cast<OperatorParamNested*>(off->second.get());
        ASSERT_EXCEPTION(group, "Offset is not a nested parameter list?!");
        Parameters const& g = group->getParameters();
        _offset.resize(g.size());
        for (size_t i = 0; i < g.size(); ++i) {
            _offset[i] = evalInt64Expr(g[i], logicalOp);
        }
    }
}

} // namespace scidb
