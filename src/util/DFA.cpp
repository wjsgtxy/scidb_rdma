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
 * @file DFA.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Generic deterministic finite-state automaton (DFA) code
 *
 * @details This file contains the linker dependencies for
 * <query/DFA.h>.
 */

#include <log4cxx/logger.h>

namespace scidb { namespace dfa {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.util.dfa"));

} }
