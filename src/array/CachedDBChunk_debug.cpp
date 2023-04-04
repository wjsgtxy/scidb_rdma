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
 * @file CachedDBChunk_debug.cpp
 *
 * @brief Persistent chunk methods that do not require test coverage
 *
 */

#include <log4cxx/logger.h>
#include <array/CachedDBChunk.h>

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.CachedDBChunk"));

void CachedDBChunk::trace() const
{
    // clang-format off
        LOG4CXX_TRACE(logger, "chunk trace [this=" << this << "] [accesscount=" <<
                      _accessCount << "]");
    // clang-format on
}

} // end namespace
