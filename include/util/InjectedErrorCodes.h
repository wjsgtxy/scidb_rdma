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
 * @file InjectedErrorCodes.h
 */

#ifndef INJECTEDERRORCODES_H_
#define INJECTEDERRORCODES_H_

namespace scidb
{

enum class InjectErrCode : long
{
    LEGACY_INITIALIZE=0,
    FIRST_USEABLE=1,
    WRITE_CHUNK=FIRST_USEABLE,

    REPLICA_SEND=2,
    REPLICA_WAIT=3,
    OPERATOR=4,
    THREAD_START=5,
    DATA_STORE=6,
    QUERY_BROADCAST=7,
    FINAL_BROADCAST=8,
    CHUNK_EXISTS=9,
    INVALID_QUERY1=10,
    INVALID_QUERY2=11,
    CHUNK_OUT_OF_BOUNDS=12,
    NO_CURRENT_CHUNK=13,
    CANNOT_GET_CHUNK=14,
    ROCKS_GET_FAIL=15,
    UNUSED_AVAILABLE_01=16,
    RESERVE_SPACE_FAIL=17,
    BLOCK_LIST_END=18,
    DI_FAIL_DELETE_RECORD=19,
    DI_FAIL_PIN_VALUE=20,
    ALLOC_BLOCK_BASE_FAIL_ABH=21,
    ALLOC_BLOCK_BASE_FAIL_PBL=22,
    ALLOC_BLOCK_BASE_FAIL_GCB=23,
    SLOT_GENCOUNT_CHANGE=24,
    SLOT_OTHER_THREAD_LOAD=25,
    SLOT_GENCOUNT_CHANGE_RBFC =26,
    ALLOC_BLOCK_BASE_FAIL_WSU=27,
    NUM_CODES
};

} //namespace scidb

#endif
