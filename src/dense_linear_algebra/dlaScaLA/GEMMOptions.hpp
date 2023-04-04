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
#ifndef GEMMOPTIONS_HPP_
#define GEMMOPTIONS_HPP_

// header groups:
// std C++
// std C
// de-facto standards
// SciDB
#include <query/OperatorParam.h>
// MPI/ScaLAPACK
// local

namespace scidb
{

struct GEMMOptions {

    /**
     * @brief Evaluate options as described by the keyword parameters.
     * @param kwParams keyword parameter map
     * @param logicalOp true iff called from logical operator
     * @description
     * Set the public data members, either to defaults or to settings given by keyword parameter.
     */
    GEMMOptions(KeywordParameters& kwParams, bool logicalOp);

    bool        transposeA, transposeB;
    double      alpha, beta;
};

} // namespace scidb

#endif // GEMMOPTIONS_HPP_
