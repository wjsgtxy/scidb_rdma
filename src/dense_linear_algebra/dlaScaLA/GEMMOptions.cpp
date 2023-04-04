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

// header groups:
// std C++
// std C
// de-facto standards
// SciDB
// MPI/ScaLAPACK
// local

#include "GEMMOptions.hpp"

#include <query/OperatorParam.h>
#include <query/Expression.h>

namespace scidb
{

GEMMOptions::GEMMOptions(KeywordParameters& kwParams, bool logicalOp)
           : transposeA(false), transposeB(false), alpha(1.0), beta(1.0)
{
    typedef std::shared_ptr<OperatorParamLogicalExpression> LExp;
    typedef std::shared_ptr<OperatorParamPhysicalExpression> PExp;

    auto kw = kwParams.find("transa");
    if (kw != kwParams.end()) {
        if (logicalOp) {
            transposeA = evaluate(reinterpret_cast<LExp&>(kw->second)->getExpression(), TID_BOOL).getBool();
        } else {
            transposeA = reinterpret_cast<PExp&>(kw->second)->getExpression()->evaluate().getBool();
        }
    }

    kw = kwParams.find("transb");
    if (kw != kwParams.end()) {
        if (logicalOp) {
            transposeB = evaluate(reinterpret_cast<LExp&>(kw->second)->getExpression(), TID_BOOL).getBool();
        } else {
            transposeB = reinterpret_cast<PExp&>(kw->second)->getExpression()->evaluate().getBool();
        }
    }

    kw = kwParams.find("alpha");
    if (kw != kwParams.end()) {
        if (logicalOp) {
            alpha = evaluate(reinterpret_cast<LExp&>(kw->second)->getExpression(), TID_DOUBLE).getDouble();
        } else {
            alpha = reinterpret_cast<PExp&>(kw->second)->getExpression()->evaluate().getDouble();
        }
    }

    kw = kwParams.find("beta");
    if (kw != kwParams.end()) {
        if (logicalOp) {
            beta = evaluate(reinterpret_cast<LExp&>(kw->second)->getExpression(), TID_DOUBLE).getDouble();
        } else {
            beta = reinterpret_cast<PExp&>(kw->second)->getExpression()->evaluate().getDouble();
        }
    }
}

} // namespace scidb
