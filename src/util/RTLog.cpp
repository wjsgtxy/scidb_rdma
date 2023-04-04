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
 * @file RTLog.cpp
 */
#include <util/RTLog.h>            // test our header for standalone inclusion

#include <fstream>
#include <thread>

#include <string.h>

namespace scidb
{
using namespace std;

RTLog::msg_buffers_t     RTLog::_msgBuffers ;
std::atomic<std::size_t> RTLog::_msgIndexAtomic(0);


void RTLog::log(const char* msg)
{
    std::thread::id tid = std::this_thread::get_id();

    size_t myIndex = _msgIndexAtomic.fetch_add(1);   // returns prior value
    myIndex = myIndex % NUM_MESSAGES;    // @ up to 90 cycles, but that's still <= 45ns typically

    char * destRecord = & _msgBuffers[myIndex*BYTES_PER_MESSAGE];

    memcpy(destRecord, &tid, sizeof(tid));
    char * destMsg = destRecord + sizeof(tid);

    const size_t MAX_STRNCPY = BYTES_PER_MESSAGE-1-sizeof(pthread_t) ;
    strncpy(destMsg, msg, MAX_STRNCPY);
    destMsg[BYTES_PER_MESSAGE-1] = 0 ;    // terminating null if string is longer than MAX_STRNCPY

    // at this point, each string less than _msgIndexAtomic can be dumped by dump()
}

void RTLog::dump()
{
    // call from gdb as follows:
    //
    // "set lang c++"
    // "call scidb::RTLog::dump()"
    //
    // outputs to /tmp/rtlog.txt at the moment

    ofstream output("/tmp/rtlog.txt");    // for write
    for(size_t idx=0; idx< _msgIndexAtomic && idx < NUM_MESSAGES; idx++) {
        const char* rec = &_msgBuffers[idx*BYTES_PER_MESSAGE];

        pthread_t tid;
        memcpy(&tid, rec, sizeof(tid));
        const char * msg = rec +  sizeof(tid);

        output << std::dec << idx << ": " << "[0x" << std::hex << tid << "] " << msg << std::endl ;
    }
}

} //namespace
