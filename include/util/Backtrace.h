/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2018-2019 SciDB, Inc.
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
 * @file Backtrace.h
 * @brief Write stack backtraces to SciDB log file
 */

#ifndef BACKTRACE_H
#define BACKTRACE_H

#include <string>
#include <vector>

namespace scidb {

/** Default number of stack frames to log. */
constexpr int  BT_DEPTH = 25;

/** Container representing each line, each function call, in a backtrace */
using CallStack = std::vector<std::string>;

/**
 * @brief Capture a stack trace, line-by-line, in a CallStack container.
 *
 * @param tag    begin every line with this string
 * @param depth  count of frames to include, default BT_DEPTH
 * @return       the stack of function calls
 *
 * @note Backtraces are suppressed in non-Debug builds.
 */
CallStack fetchBacktrace(const char* tag = nullptr, int depth = BT_DEPTH);

/**
 * @brief Log a stack trace at INFO level.
 *
 * @param tag    begin every log line with this string
 * @param depth  count of frames to include, default BT_DEPTH
 *
 * @note Backtraces are suppressed in non-Debug builds.
 */
void logBacktrace(const char* tag = nullptr, int depth = BT_DEPTH);

} // namespace scidb

#endif /* ! BACKTRACE_H */
