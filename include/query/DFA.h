/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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
 * @file query/DFA.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Wrap <util/DFA.h> for use with ParamPlaceholders
 *
 * @details Provide DFA_*() helper macros to integrate pure DFA code
 * into SciDB.
 *
 * @see util/DFA.h
 */

#ifndef QUERY_DFA_H
#define QUERY_DFA_H

#include <system/Exceptions.h>
#include <log4cxx/logger.h>

namespace scidb { namespace dfa {
  extern log4cxx::LoggerPtr logger;
} }

#define DFA_DBG(_shifty)                                \
do {                                                    \
    LOG4CXX_TRACE(scidb::dfa::logger, "DFA: "           \
                  << __FUNCTION__ << ':' << __LINE__    \
                  << ": " << _shifty);                  \
} while (0)

#define DFA_ERR(_shifty)                                \
do {                                                    \
    LOG4CXX_ERROR(scidb::dfa::logger, "DFA: "           \
                  << __FUNCTION__ << ':' << __LINE__    \
                  << ": " << _shifty);                  \
} while (0)

#define DFA_FAIL(_shifty)                                               \
do {                                                                    \
    std::stringstream ss;                                               \
    ss << "DFA: " << _shifty;                                           \
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)   \
        << ss.str();                                                    \
} while (0)

#define HAVE_DFA_HELPER_MACROS 1
#include <util/DFA.h>

#undef DFA_DBG
#undef DFA_ERR
#undef DFA_FAIL

#endif /* ! QUERY_DFA_H */
