/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file RTLog.h
 * @brief cheap real-time logging for debugging a mutex hang problem
 */

#ifndef RTLOG_H_
#define RTLOG_H_

#include <atomic>
#include <array>

namespace scidb
{

class RTLog
{
public:
    RTLog() = delete ;              // all methods are static, do not construct

    /// @param msg message to add to the next position in _msgMemory
    static void log(const char *msg);  // maximum length enforced.

    /// invoke from gdb as follows
    //  "set lang c++"
    //  "call scidb::RTLog::dump()
    static void dump();             // writes it to <a fixed file location>
private:
    static const size_t BYTES_PER_MESSAGE = 120;
    static const size_t NUM_MESSAGES = 1000*1000;
    using msg_buffers_t = std::array<char, BYTES_PER_MESSAGE*NUM_MESSAGES> ; // 1M messages, 120 chars each

    static msg_buffers_t            _msgBuffers ;
    static std::atomic<std::size_t> _msgIndexAtomic ;
};

} //namespace

#endif /* RTLOG_H_ */
