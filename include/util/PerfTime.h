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
 * @file PerfTime.h
 *
 * @brief timing methods for performance reporting
 */

#ifndef PERF_TIME_H_
#define PERF_TIME_H_

#include <algorithm>
#include <atomic>

#include <sys/time.h>             // linux specific
#include <sys/resource.h>         // linux specific

namespace scidb
{

/// enumerations for scope categories, used for summarizing per-scope data
#define PTSC_TABLE_ENTRIES \
    PTSC_ENTRY(PTSC_INVALID,                 "timings outside of scope required to record it"), \
    PTSC_ENTRY(PTSC_CMHJ,                    "ClientMessageHandleJob methods: client top-level threads."), \
    PTSC_ENTRY(PTSC_SMHJ,                    "ServerMessageHandleJob methods: server to server services."), \
    PTSC_ENTRY(PTSC_FET,                     "ServerMessageHandleJob methods: chunk fetch."), \
    PTSC_ENTRY(PTSC_REDIM,                   "Redimension{,Common} methods."), \
    PTSC_ENTRY(PTSC_STORE,                   "PhysicalStore methods."), \
    PTSC_ENTRY(PTSC_JOB,                     "Job::run() scopes"), \
    PTSC_ENTRY(PTSC_OTHER,                   "other timing scopes")  // no comma

/// expand PTSC_TABLE_ENTRIES into a set of enums
#define PTSC_ENTRY(PTSC, MEANING) PTSC
enum perfTimeScopeCategory {         // the actual enumeration type
    PTSC_TABLE_ENTRIES,              // expansion of list above
    // these two added enumerations for iterating
    PTSC_NUM,                        // must follow PTSC_TABLE_ENTRIES
    PTSC_FIRST=0
};
using perfTimeScopeCategory_e = enum perfTimeScopeCategory ;
#undef PTSC_ENTRY

/// enumerations for scope enumerations, argument to PerfTimeScope ctor
#define PTS_TABLE_ENTRIES \
    PTS_ENTRY(PTS_INVALID,                   PTSC_INVALID, "no PerfTimeScope, investigation needed"), \
    PTS_ENTRY(PTS_OTHER,                     PTSC_OTHER,   "miscellaneous not broken out below"), \
    PTS_ENTRY(PTS_REDIM_ALL,                 PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P1_FILL,             PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P2_SORT,             PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P2_SYN_UPD,          PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P2_SYN_RESORT,       PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P3_TO_BEFORE,        PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P4_REDIST_SYN,       PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_REDIM_P4_REDIST_AGG,       PTSC_REDIM,   "RedimensionCommon::redimensionArray()"), \
    PTS_ENTRY(PTS_STORE_ARRAY_FLUSH,         PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    PTS_ENTRY(PTS_STORE_COPY_CHUNKS,         PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    PTS_ENTRY(PTS_STORE_REMOVE_DEAD_CHUNKS,  PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    PTS_ENTRY(PTS_STORE_REMOVE_INBOUND_QUEUE,PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    PTS_ENTRY(PTS_STORE_REPLICATION_SYNC,    PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    PTS_ENTRY(PTS_STORE_UPDATE_SCHEMA_BOUND, PTSC_STORE,   "PhysicalStore::copyChunks()"), \
    \
    PTS_ENTRY(PTS_CMHJ_COMP_EXE_QUERY,       PTSC_CMHJ,    "ClientMessageHandleJob::completeExecuteQuery()"), \
    PTS_ENTRY(PTS_CMHJ_COMP_QUERY,           PTSC_CMHJ,    "ClientMessageHandleJob::completeQuery()"), \
    PTS_ENTRY(PTS_CMHJ_EXE_CLI_QUERY,        PTSC_CMHJ,    "ClientMessageHandleJob::executeClientQuery()"), \
    PTS_ENTRY(PTS_CMHJ_FETCH_CHUNK,          PTSC_CMHJ,    "ClientMessageHandleJob::fetchChunk()"), \
    PTS_ENTRY(PTS_CMHJ_FETCH_MRG_CHUNK,      PTSC_CMHJ,    "ClientMessageHandleJob::fetchMergedChunk()"), \
    PTS_ENTRY(PTS_CMHJ_PREP_CLI_QUERY,       PTSC_CMHJ,    "ClientMessageHandleJob::prepareClientQuery()"), \
    PTS_ENTRY(PTS_CMHJ_RETRY_EXE_QUERY,      PTSC_CMHJ,    "ClientMessageHandleJob::retryExecuteQuery()"), \
    PTS_ENTRY(PTS_CMHJ_RETRY_PREP_QUERY,     PTSC_CMHJ,    "ClientMessageHandleJob::retryPrepareQuery()"), \
    \
    PTS_ENTRY(PTS_SMHJ_FET_CHUNK,            PTSC_SMHJ,    "ServerMessageHandleJob::handleFetch(), REMOTE_ARRAY"),\
    PTS_ENTRY(PTS_SMHJ_FET_MCHUNK,           PTSC_SMHJ,    "ServerMessageHandleJob::handleFetch(), MERGED_ARRAY"),\
    PTS_ENTRY(PTS_SMHJ_FET_CHUNK_COMMON,     PTSC_SMHJ,    "ServerMessageHandleJob::handleFetch(), common code"), \
    PTS_ENTRY(PTS_SMHJ_FET_SGCHUNK,          PTSC_SMHJ,    "ServerMessageHandleJob::handleSGFetchChunk()"), \
    PTS_ENTRY(PTS_SMHJ_HC,                   PTSC_SMHJ,    "ServerMessageHandleJob::handleCommit()"), \
    PTS_ENTRY(PTS_SMHJ_HPPP,                 PTSC_SMHJ,    "ServerMessageHandleJob::handlePreparePhysicalPlan()"),\
    PTS_ENTRY(PTS_SMHJ_HQR,                  PTSC_SMHJ,    "ServerMessageHandleJob::handleQueryResult"), \
    \
    PTS_ENTRY(PTS_JOB_RUN_PREFETCH,          PTSC_JOB,     "ParallelAccumulatorArray::ChunkPrefetchJob::run()"), \
    PTS_ENTRY(PTS_JOB_RUN_SORT,              PTSC_JOB,     "SortArray::SortJob::run()"), \
    PTS_ENTRY(PTS_JOB_RUN_MERGE,             PTSC_JOB,     "SortArray::MergeJob::run() "), \
    PTS_ENTRY(PTS_GET_SORTED_ARRAY,          PTSC_JOB,     "SortArray::SortJob::run()")     // last, no comma

/// expand PTS_TABLE_ENTRIES into a set of enums
#define PTS_ENTRY(PTS, PTC, MEANING) PTS
enum perfTimeScope {    // the actual enumeration type
    PTS_TABLE_ENTRIES,  // expansion of list above
    // these two added enumerations for iterating
    PTS_NUM,           // must follow PTS_TABLE_ENTRIES
    PTS_FIRST=0,
};
#undef PTS_ENTRY
using perfTimeScope_e = enum perfTimeScope;

/**
 * @brief categories of accumulated time
 * the following macro defines a string with all the information required
 * to generate enumerations and a table containing a stringification and their meaning
 */

#define PTWC_TABLE_ENTRIES \
    PTWC_ENTRY(PTWC_INVALID,                 "waits that are not being categorized correctly"), \
    PTWC_ENTRY(PTWC_PG,                      "waits for postgres queries"), \
    /* FileIO */ \
    PTWC_ENTRY(PTWC_FS_RD,                   "waits for read"),                 \
    PTWC_ENTRY(PTWC_FS_WR,                   "waits for write"),                \
    PTWC_ENTRY(PTWC_FS_WR_SYNC,              "waits for write with O_SYNC"),    \
    PTWC_ENTRY(PTWC_FS_FL,                   "waits for write flush"),          \
    PTWC_ENTRY(PTWC_FS_PH,                   "waits for fallocate punch hole"), \
    PTWC_ENTRY(PTWC_SM_LOAD,                 "waits for chunk load"),           \
    PTWC_ENTRY(PTWC_BF_RD,                   "waits for buffered file read"),   \
    /* storage */ \
    PTWC_ENTRY(PTWC_SM_CMEM,                 "waits for chunk mem (unpins & writes)"), /* PTC_ZERO? */ \
    PTWC_ENTRY(PTWC_BUF_CACHE,               "waits for space in the buffer cache"), \
    PTWC_ENTRY(PTWC_SM_OTHER,                "waits for other storage manager reasons"), \
    /* networking */ \
    PTWC_ENTRY(PTWC_REP,                     "waits for replication (throttling)"), \
    PTWC_ENTRY(PTWC_NET_RCV,                 "waits for point-to-point receive"),   \
    PTWC_ENTRY(PTWC_NET_SND,                 "waits for point-to-point send"), \
    PTWC_ENTRY(PTWC_NET_RCV_R,               "waits for remote array)"), \
    PTWC_ENTRY(PTWC_NET_RCV_C,               "waits for rcv-from-client)"), \
    PTWC_ENTRY(PTWC_SG_RCV,                  "waits for SG chunks receive"),  \
    PTWC_ENTRY(PTWC_SG_BAR,                  "waits for SG sync barrier"), \
    PTWC_ENTRY(PTWC_BAR,                     "waits for other (spmd/bsp) barriers"), \
    /* client communication manager*/                                                     \
    PTWC_ENTRY(PTWC_CCM_SESS,                "waits for client communication manager session management"), \
    /*other */ \
    PTWC_ENTRY(PTWC_SORT_JOB,                "waits for sort jobs"), \
    PTWC_ENTRY(PTWC_MISC_JOB,                "waits for miscellaneous jobs"), \
    PTWC_ENTRY(PTWC_EXT,                     "waits for managed processes"), \
    PTWC_ENTRY(PTWC_SEMA,                    "waits for other sempahores"),  \
    PTWC_ENTRY(PTWC_EVENT,                   "waits for other events"), \
    PTWC_ENTRY(PTWC_LATCH,                   "waits for data latches/locks"), \
    PTWC_ENTRY(PTWC_RARE,                    "waits that are rare"), \
    PTWC_ENTRY(PTWC_TESTING,                 "waits used for unit testing, not user functionality"), \
    PTWC_ENTRY(PTWC_IGNORE,                  "e.g. some waits for query arrival"), /* not attributable to a query */ \
    PTWC_ENTRY(PTWC_ZERO,                    "waits that should equal 0") /*no comma */

/**
 * enumerations for categories of wait time.
 * the more detailed PTW_ enumerations are grouped into these categories.
 */
#define PTWC_ENTRY(PTWC, MEANING) PTWC
enum perfTimeWaitCategory {         // enumerations of wait categories
    PTWC_TABLE_ENTRIES,
    PTWC_NUM,                    // must follow _TABLE_ENTRIES
    PTWC_FIRST=0
};
using perfTimeWaitCategory_e = enum perfTimeWaitCategory ;
#undef PTWC_ENTRY

/**
 * explanation of PTW_ prefixes
 *
 * PTW_SPCL_  waits calculated without using Scoped{WaitTimer,MutexLock}, RWLock, Semaphore, or Event
 * PTW_SWT_   measured using ScopedWaitTime
 * PTW_SML_   measured using ScopedMutexLock
 * PTW_RWL_   measured using ReadWrite
 * PTW_SEM_   measured with Semaphore
 * PTW_EVENT  measured with Event
 * PTW_MUTEX  measured with Mutex
 */

#define PTW_TABLE_ENTRIES \
    PTW_ENTRY(PTW_UNTIMED,                      PTWC_IGNORE,     "not actually perf blocking, e.g. no client"), \
    PTW_ENTRY(PTW_SWT_PG1,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG2,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG3,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG4,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG5,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG6,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG7,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG8,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG9,                      PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG10,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG11,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG12,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG13,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG14,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG15,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG16,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG17,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG18,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG19,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG20,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG21,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG22,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG23,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG24,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG25,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG26,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG26_A,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG27,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG28,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG29,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_A,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_B,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_C,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_D,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_E,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_F,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_G,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_H,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_I,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG30_J,                   PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG31,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG32,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG33,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG34,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG35,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG36,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG37,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SWT_PG38,                     PTWC_PG,         "communicating with postgres"), \
    PTW_ENTRY(PTW_SML_PG,                       PTWC_PG,         "latches during comm. with postgres s.b. 0"),\
    /* File IO */ \
    PTW_ENTRY(PTW_SWT_FS_RD,                    PTWC_FS_RD,      "file system read"), \
    PTW_ENTRY(PTW_SWT_FS_WR,                    PTWC_FS_WR,      "file system write"), \
    PTW_ENTRY(PTW_SWT_FS_WR_SYNC,               PTWC_FS_WR_SYNC, "file system write with O_SYNC"), \
    PTW_ENTRY(PTW_SWT_FS_FL,                    PTWC_FS_FL,      "file system flush"), \
    PTW_ENTRY(PTW_SWT_FS_PH,                    PTWC_FS_PH,      "file system fallocate punch hole"), \
    PTW_ENTRY(PTW_EVENT_SM_LOAD,                PTWC_SM_LOAD,    "wait on smgr thread to load chunk"), \
    PTW_ENTRY(PTW_EVENT_SM_CMEM,                PTWC_SM_CMEM,    "wait for freed memory (unpin & write)"), \
    PTW_ENTRY(PTW_EVENT_BFI_GET,                PTWC_BF_RD,      "wait for BufferedFileInput reader thread"), \
    PTW_ENTRY(PTW_EVENT_BFI_RA,                 PTWC_BF_RD,      "wait for BufferedFileINput read ahead"), \
    /* Storage */ \
    PTW_ENTRY(PTW_SML_BM_ALLOC,                 PTWC_SM_OTHER,   "wait for a BufferMgr::allocateBuffer()"), \
    PTW_ENTRY(PTW_SML_BM_DISCARD,               PTWC_SM_OTHER,   "wait in BufferMgr::discardBuffer()"), \
    PTW_ENTRY(PTW_SML_BM_PIN_LOCKED,            PTWC_SM_OTHER,   "wait in BufferMgr::pinLocked"), \
    PTW_ENTRY(PTW_SML_BM__PIN,                  PTWC_SM_OTHER,   "wait in BufferMgr::_pinBuffer()"), \
    PTW_ENTRY(PTW_SML_BM__UNPIN,                PTWC_SM_OTHER,   "wait in BufferMgr::_unpinBuffer()"), \
    PTW_ENTRY(PTW_COND_BM_PW__PIN,              PTWC_SM_OTHER,   "wait for COND PW in BufferMgr::_pinBuffer()"), \
    PTW_ENTRY(PTW_COND_BM_PW__PIN_LOCKED,       PTWC_SM_OTHER,   "wait COND PW in BufferMgr::_pinBufferLocked()"),\
    PTW_ENTRY(PTW_COND_BM_PW_REMOVE,            PTWC_SM_OTHER,   "wait for COND in BufferMgr::remove()"), \
    PTW_ENTRY(PTW_COND_BM_PWB_FLUSHALL,         PTWC_SM_OTHER,   "wait for COND PWB in BufferMgr::flushall"), \
    PTW_ENTRY(PTW_COND_BM_RESERVEQ,             PTWC_SM_OTHER,   "wait for COND in BufferMgr::reserve()"), \
    PTW_ENTRY(PTW_SWT_BUF_CACHE,                PTWC_BUF_CACHE,  "wait for space in the buffer cache"), \
    PTW_ENTRY(PTW_SWT_CDBC_PIN,                 PTWC_SM_OTHER,   "wait for a CachedDBChunk::pin()"), \
    PTW_ENTRY(PTW_SWT_CDBC_UNPIN,               PTWC_SM_OTHER,   "wait for a CachedDBChunk::unPin()"), \
    PTW_ENTRY(PTW_SML_DB_ARRAY_ITER,            PTWC_SM_OTHER,   "wait for a DBArrayIterator::operator++()"),  \
    PTW_ENTRY(PTW_SML_MEM_ARRAY_PIN,            PTWC_SM_OTHER,   "wait for a MemArray::pinChunk()"),  \
    PTW_ENTRY(PTW_SML_MEM_ARRAY_UNPIN,          PTWC_SM_OTHER,   "wait for a MemArray::unpinChunk()"), \
    PTW_ENTRY(PTW_SML_MEM_ARRAY_ITER,           PTWC_SM_OTHER,   "wait for a MemArrayIter latch"), /* class */ \
    /* query thread wait for other threads */ \
    PTW_ENTRY(PTW_JOB_CHUNK_PREFETCH,           PTWC_MISC_JOB,   "wait for chunk prefetch job"), \
    PTW_ENTRY(PTW_JOB_DATASTORE_FLUSH_STOP,     PTWC_MISC_JOB,   "wait for datastore flush job to stop"), \
    PTW_ENTRY(PTW_JOB_FILEREAD,                 PTWC_MISC_JOB,   "wait for file read job"), \
    PTW_ENTRY(PTW_JOB_KENDALL,                  PTWC_MISC_JOB,   "wait for kendall job"), \
    PTW_ENTRY(PTW_JOB_PEARSON,                  PTWC_MISC_JOB,   "wait for pearson job"), \
    PTW_ENTRY(PTW_JOB_MERGE_FINAL,              PTWC_SORT_JOB,   "wait for sort's last merge"), \
    PTW_ENTRY(PTW_JOB_SORT,                     PTWC_SORT_JOB,   "wait for sort job"), \
    PTW_ENTRY(PTW_JOB_SORT_STOPPED,             PTWC_SORT_JOB,   "wait for stopped sort job"), \
    /* networking */ \
    PTW_ENTRY(PTW_SWT_NET_RCV,                  PTWC_NET_RCV,    "wait for receive pt-to-pt message"), \
    PTW_ENTRY(PTW_SWT_NET_SND,                  PTWC_NET_SND,    "wait to enqueue pt-to-pt message for send"), \
    PTW_ENTRY(PTW_SEM_NET_RCV_R,                PTWC_NET_RCV_R,  "wait for remote array"),  \
    PTW_ENTRY(PTW_SEM_NET_RCV_C,                PTWC_NET_RCV_C,  "wait for client communication"),  \
    /* Client Communication Manager */                                                       \
    PTW_ENTRY(PTW_CCM_CACHE,                    PTWC_CCM_SESS,   "wait for Ccm session cache"), \
    /* SG */ \
    PTW_ENTRY(PTW_SEM_SG_RCV,                   PTWC_SG_RCV,     "wait for SG receive data sema"), \
    PTW_ENTRY(PTW_EVENT_SG_PULL,                PTWC_SG_RCV,     "wait for SG receive attribute event"), \
    PTW_ENTRY(PTW_SWT_PULL_BARRIER_0,           PTWC_SG_RCV,     "wait for pullRedistribute() barrier 0"), \
    PTW_ENTRY(PTW_SWT_PULL_BARRIER_1,           PTWC_SG_RCV,     "wait for pullRedistribute() barrier 1"), \
    PTW_ENTRY(PTW_SEM_QUERY_BAR,                PTWC_SG_BAR,     "wait for query::barrier(), syncBarrier()"), \
    /*          PTWC_REP */ \
    PTW_ENTRY(PTW_SEM_REP,                      PTWC_REP,        "wait for replication sema"), /* ~6 locs */ \
    PTW_ENTRY(PTW_EVENT_REP,                    PTWC_REP,        "wait for replication event"), /* 1 loc */ \
    /*          PTWC_EXT */ \
    PTW_ENTRY(PTW_EVENT_EXT_LAUNCH,             PTWC_EXT,        "wait for managed process to start"), \
    PTW_ENTRY(PTW_SWT_EXT,                      PTWC_EXT,        "wait for managed process to 'return' status"), \
    /* misc. barriers */ \
    PTW_ENTRY(PTW_SML_BAR_DEFAULT,              PTWC_BAR,        "wait for unspecified barrier"), \
    PTW_ENTRY(PTW_SEM_JOB_DONE,                 PTWC_BAR,        "wait at end of job"), \
    PTW_ENTRY(PTW_SEM_RESULTS_QP,               PTWC_BAR,        "QueryProcessor results.enter()"), /* 2 locs */ \
    PTW_ENTRY(PTW_SEM_RESULTS_EX,               PTWC_BAR,        "executor results.enter()"), /* 1 loc */ \
    /* misc semaphores */ \
    PTW_ENTRY(PTW_SEM_THREAD_TERM,              PTWC_SEMA,       "wait for thread term"), \
    PTW_ENTRY(PTW_SEM_RESOURCES,                PTWC_SEMA,       "wait for a Resources semaphore" ), \
    /* short-term data locks */ \
    PTW_ENTRY(PTW_MUTEX,                        PTWC_LATCH,      "wait for a non-specific Mutex"), /* (default parameter) */ \
    PTW_ENTRY(PTW_RWL_MODULE,                   PTWC_LATCH,      "wait for a module RWLock"), \
    PTW_ENTRY(PTW_SML_AGGREGATE_LIBRARY,        PTWC_LATCH,      "wait for AggregateLibrary latch"), /* class */ \
    PTW_ENTRY(PTW_SML_ARENA,                    PTWC_LATCH,      "wait for ThreadedArena latch"), /* class */ \
    PTW_ENTRY(PTW_SML_AUTH_MESSAGE_HANDLE_JOB,  PTWC_LATCH,      "wait for AuthMessageHandleJob latch"), /*class*/\
    PTW_ENTRY(PTW_SML_BEST_MATCH_ARRAY,         PTWC_LATCH,      "wait for a BestMatchArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_BFI,                      PTWC_LATCH,      "wait for a BufferedFileInput latch"), /*class*/\
    PTW_ENTRY(PTW_SML_CACHE_PTR,                PTWC_LATCH,      "wait for a CachePtr latch"), /* class */ \
    PTW_ENTRY(PTW_SML_CLUSTER,                  PTWC_LATCH,      "wait for a Cluster latch"), /* class */ \
    PTW_ENTRY(PTW_SML_CON,                      PTWC_LATCH,      "wait for a Connection latch"), /* class */ \
    PTW_ENTRY(PTW_SML_CONFIG_BASE,              PTWC_LATCH,      "wait for a ConfigBase latch"), /* class */ \
    PTW_ENTRY(PTW_SML_COUNTER_STATE,            PTWC_LATCH,      "wait for a CounterState latch"), /* class */ \
    PTW_ENTRY(PTW_SML_DFLT,                     PTWC_LATCH,      "wait for unspecified ScopedMutexLock"),\
    PTW_ENTRY(PTW_SML_DISK_INDEX,               PTWC_LATCH,      "wait for a Disk Index"), /* class */ \
    PTW_ENTRY(PTW_SML_DS,                       PTWC_LATCH,      "wait for a Datastore latch"), /* class */ \
    PTW_ENTRY(PTW_SML_ERRORS_LIBRARY,           PTWC_LATCH,      "wait for a ErrorsLibrary latch"), /* class */ \
    PTW_ENTRY(PTW_SML_FILE_MANAGER,             PTWC_LATCH,      "wait for a FileManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_GROUP_RANK_ARRAY,         PTWC_LATCH,      "wait for a GroupRankArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_INJECTED_ERROR_LISTENER,  PTWC_LATCH,      "wait for a InjectedErrorLister latch"), \
    PTW_ENTRY(PTW_SML_JOB_QUEUE,                PTWC_LATCH,      "wait for a JobQue latch"), \
    PTW_ENTRY(PTW_SML_JOB_XOQ,                  PTWC_LATCH,      "wait for a JobXoq latch"), \
    PTW_ENTRY(PTW_SML_LOCK_MANAGER,             PTWC_LATCH,      "wait for a LockManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_MATERIALIZED_ARRAY,       PTWC_LATCH,      "wait for a MaterializedArray latch"), /*class*/ \
    PTW_ENTRY(PTW_SML_MA,                       PTWC_LATCH,      "wait for a MemArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_MATCH_ARRAY,              PTWC_LATCH,      "wait for a MatchArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_MPI_OP_CONTEXT,           PTWC_LATCH,      "wait for an MPIOperatorContext latch"), /* class */ \
    PTW_ENTRY(PTW_SML_NOTI,                     PTWC_LATCH,      "wait for a Notification latch"), /* class */ \
    PTW_ENTRY(PTW_SML_NM,                       PTWC_LATCH,      "wait for a NetworkManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_MPIL,                     PTWC_LATCH,      "wait for an MPILauncher latch"), /* class */ \
    PTW_ENTRY(PTW_SML_MPIM,                     PTWC_LATCH,      "wait for an MPIManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_OPERATOR,                 PTWC_LATCH,      "wait for an Operator latch"), /* 1 loc */ \
    PTW_ENTRY(PTW_SML_ORDERED_BCAST_TEST,       PTWC_LATCH,      "wait for an OrderedBcastTestPhysical"), \
    PTW_ENTRY(PTW_SML_P4_INSTANCE_LIVENESS,     PTWC_LATCH,      "wait for a p4 InstanceLivenessManager latch"), \
    PTW_ENTRY(PTW_SML_P4_OP_STATS_QUERY,        PTWC_LATCH,      "wait for a p4 OpStatsQuery latch"), /* class */ \
    PTW_ENTRY(PTW_SML_P4_PAM_AUTHENTICATOR,     PTWC_LATCH,      "wait for a p4 PamAuthenticator latch"), \
    PTW_ENTRY(PTW_SML_PLUGIN_MANAGER,           PTWC_LATCH,      "wait for a PluginManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_PM,                       PTWC_LATCH,      "wait for an PluginManager latch"), /* class */ \
    PTW_ENTRY(PTW_SML_QUANTILE,                 PTWC_LATCH,      "wait for a Quantile latch"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_A,            PTWC_LATCH,      "wait for Query errorMutex a"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_B,            PTWC_LATCH,      "wait for Query errorMutex b"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_C,            PTWC_LATCH,      "wait for Query errorMutex c"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_D,            PTWC_LATCH,      "wait for Query errorMutex d"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_E,            PTWC_LATCH,      "wait for Query errorMutex e"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_F,            PTWC_LATCH,      "wait for Query errorMutex f"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_G,            PTWC_LATCH,      "wait for Query errorMutex g"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_H,            PTWC_LATCH,      "wait for Query errorMutex h"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_I,            PTWC_LATCH,      "wait for Query errorMutex i"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_J,            PTWC_LATCH,      "wait for Query errorMutex j"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_K,            PTWC_LATCH,      "wait for Query errorMutex k"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_L,            PTWC_LATCH,      "wait for Query errorMutex l"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_M,            PTWC_LATCH,      "wait for Query errorMutex m"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_N,            PTWC_LATCH,      "wait for Query errorMutex n"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_O,            PTWC_LATCH,      "wait for Query errorMutex o"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_P,            PTWC_LATCH,      "wait for Query errorMutex p"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_Q,            PTWC_LATCH,      "wait for Query errorMutex q"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_R,            PTWC_LATCH,      "wait for Query errorMutex r"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_S,            PTWC_LATCH,      "wait for Query errorMutex s"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_T,            PTWC_LATCH,      "wait for Query errorMutex t"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_U,            PTWC_LATCH,      "wait for Query errorMutex u"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_V,            PTWC_LATCH,      "wait for Query errorMutex v"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_W,            PTWC_LATCH,      "wait for Query errorMutex w"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_X,            PTWC_LATCH,      "wait for Query errorMutex x"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_Y,            PTWC_LATCH,      "wait for Query errorMutex y"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_Z,            PTWC_LATCH,      "wait for Query errorMutex z"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AA,           PTWC_LATCH,      "wait for Query errorMutex aa"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AB,           PTWC_LATCH,      "wait for Query errorMutex ab"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AC,           PTWC_LATCH,      "wait for Query errorMutex ac"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AD,           PTWC_LATCH,      "wait for Query errorMutex ad"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AE,           PTWC_LATCH,      "wait for Query errorMutex ae"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AF,           PTWC_LATCH,      "wait for Query errorMutex af"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AG,           PTWC_LATCH,      "wait for Query errorMutex ag"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AH,           PTWC_LATCH,      "wait for Query errorMutex ah"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AI,           PTWC_LATCH,      "wait for Query errorMutex ai"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AJ,           PTWC_LATCH,      "wait for Query errorMutex aj"), \
    PTW_ENTRY(PTW_SML_QUERY_ERROR_AK,           PTWC_LATCH,      "wait for Query errorMutex ak"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_A,            PTWC_LATCH,      "wait for Query _mutex a"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_B,            PTWC_LATCH,      "wait for Query _mutex b"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_C,            PTWC_LATCH,      "wait for Query _mutex c"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_D,            PTWC_LATCH,      "wait for Query _mutex d"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_E,            PTWC_LATCH,      "wait for Query _mutex e"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_F,            PTWC_LATCH,      "wait for Query _mutex f"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_G,            PTWC_LATCH,      "wait for Query _mutex g"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_H,            PTWC_LATCH,      "wait for Query _mutex h"), \
    PTW_ENTRY(PTW_SML_QUERY_MUTEX_I,            PTWC_LATCH,      "wait for Query _mutex i"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_A,          PTWC_LATCH,      "wait for Query queriesMutex a"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_B,          PTWC_LATCH,      "wait for Query queriesMutex b"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_C,          PTWC_LATCH,      "wait for Query queriesMutex c"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_D,          PTWC_LATCH,      "wait for Query queriesMutex d"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_E,          PTWC_LATCH,      "wait for Query queriesMutex e"), \
    PTW_ENTRY(PTW_SML_QUERY_QUERIES_F,          PTWC_LATCH,      "wait for Query queriesMutex f"), \
    PTW_ENTRY(PTW_SML_QUERY_WARN_A,             PTWC_LATCH,      "wait for Query _warningsMutex a"), \
    PTW_ENTRY(PTW_SML_QUERY_WARN_B,             PTWC_LATCH,      "wait for Query _warningsMutex b"), \
    PTW_ENTRY(PTW_SML_QUERY_WARN_C,             PTWC_LATCH,      "wait for Query _warningsMutex c"), \
    PTW_ENTRY(PTW_SML_RA,                       PTWC_LATCH,      "wait for a RemoteArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_RECEIVE_MUTEX,            PTWC_LATCH,      "wait for a Query _recevieMutex latch"), \
    PTW_ENTRY(PTW_SML_REP,                      PTWC_LATCH,      "wait for a Replication latch"), \
    PTW_ENTRY(PTW_SML_ROCKS_INDEX_MAP,          PTWC_LATCH,      "wait for a RocksIndexMap latch"), /* class */ \
    PTW_ENTRY(PTW_SML_ROW_COLLECTION,           PTWC_LATCH,      "wait for a RowCollection latch"), /* class */ \
    PTW_ENTRY(PTW_SML_RWL,                      PTWC_LATCH,      "wait for a RWLock latch"), \
    PTW_ENTRY(PTW_SML_SORT_ARRAY,               PTWC_LATCH,      "wait for a SortArray latch"), /* class */ \
    PTW_ENTRY(PTW_SML_SERVER_MESSAGE_HANDLE_JOB,PTWC_LATCH,      "wait for ServerMessageHandleJob latch"), \
    PTW_ENTRY(PTW_SML_SINGLE_PASS_ARRAY,        PTWC_LATCH,      "wait for a SinglePassArray latch"), /* class */\
    PTW_ENTRY(PTW_SML_SINGLETON,                PTWC_LATCH,      "wait for a Singleton latch"), /* class */ \
    PTW_ENTRY(PTW_SML_SG_PULL,                  PTWC_LATCH,      "wait for SG PULL latch"), /*  */ \
    PTW_ENTRY(PTW_SML_SR,                       PTWC_LATCH,      "wait for SciDBRemote latch"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_A,                   PTWC_LATCH,      "wait for a Storage latch a"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_B,                   PTWC_LATCH,      "wait for a Storage latch b"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_D,                   PTWC_LATCH,      "wait for a Storage latch d"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_E,                   PTWC_LATCH,      "wait for a Storage latch e"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_F,                   PTWC_LATCH,      "wait for a Storage latch f"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_G,                   PTWC_LATCH,      "wait for a Storage latch g"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_H,                   PTWC_LATCH,      "wait for a Storage latch h"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_I,                   PTWC_LATCH,      "wait for a Storage latch i"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_J,                   PTWC_LATCH,      "wait for a Storage latch j"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_K,                   PTWC_LATCH,      "wait for a Storage latch k"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_L,                   PTWC_LATCH,      "wait for a Storage latch l"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_M,                   PTWC_LATCH,      "wait for a Storage latch m"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_N,                   PTWC_LATCH,      "wait for a Storage latch n"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_O,                   PTWC_LATCH,      "wait for a Storage latch o"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_P,                   PTWC_LATCH,      "wait for a Storage latch p"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_Q,                   PTWC_LATCH,      "wait for a Storage latch q"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_R,                   PTWC_LATCH,      "wait for a Storage latch r"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_S,                   PTWC_LATCH,      "wait for a Storage latch s"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_T,                   PTWC_LATCH,      "wait for a Storage latch t"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_U,                   PTWC_LATCH,      "wait for a Storage latch u"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_V,                   PTWC_LATCH,      "wait for a Storage latch v"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AA,                  PTWC_LATCH,      "wait for a Storage latch aa"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AC,                  PTWC_LATCH,      "wait for a Storage latch ac"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AE,                  PTWC_LATCH,      "wait for a Storage latch ae"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AF,                  PTWC_LATCH,      "wait for a Storage latch af"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AG,                  PTWC_LATCH,      "wait for a Storage latch ag"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AH,                  PTWC_LATCH,      "wait for a Storage latch ah"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AI,                  PTWC_LATCH,      "wait for a Storage latch ai"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AJ,                  PTWC_LATCH,      "wait for a Storage latch aj"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AK,                  PTWC_LATCH,      "wait for a Storage latch ak"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AL,                  PTWC_LATCH,      "wait for a Storage latch al"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AM,                  PTWC_LATCH,      "wait for a Storage latch am"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AN,                  PTWC_LATCH,      "wait for a Storage latch an"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AO,                  PTWC_LATCH,      "wait for a Storage latch ao"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AP,                  PTWC_LATCH,      "wait for a Storage latch ap"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AQ,                  PTWC_LATCH,      "wait for a Storage latch aq"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AR,                  PTWC_LATCH,      "wait for a Storage latch ar"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AS,                  PTWC_LATCH,      "wait for a Storage latch as"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AT,                  PTWC_LATCH,      "wait for a Storage latch at"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AU,                  PTWC_LATCH,      "wait for a Storage latch au"), /* class */ \
    PTW_ENTRY(PTW_SML_STOR_AV,                  PTWC_LATCH,      "wait for a Storage latch av"), /* class */ \
    PTW_ENTRY(PTW_SML_THREAD_POOL,              PTWC_LATCH,      "wait for a ThreadPool latch"), /* class */ \
    PTW_ENTRY(PTW_SML_THROTTLED_SCHEDULER,      PTWC_LATCH,      "wait for a ThrottledScheduler latch"), \
    PTW_ENTRY(PTW_SML_TS,                       PTWC_LATCH,      "wait for a TypeSystem latch"), /* class */ \
    PTW_ENTRY(PTW_SML_WQ,                       PTWC_LATCH,      "wait for a WorkQueue latch"), /* class */ \
    PTW_ENTRY(PTW_SML_RESOURCES,                PTWC_LATCH,      "wait for a Resources latch"), /* class */ \
    /* UNIT TESTING */ \
    PTW_ENTRY(PTW_SWT_TEST_SLEEP,               PTWC_TESTING,    "wait for perfTestSleep() UDF"), /* function */ \
    PTW_ENTRY(PTW_SWT_TEST_SPIN,                PTWC_TESTING,    "wait for perfTestSpin() UDF"), /* function */ \
    /* PTWC_RARE */ \
    PTW_ENTRY(PTW_EVENT_MATCH,                  PTWC_RARE,	     "wait in example operator Match" ), \
    PTW_ENTRY(PTW_EVENT_BESTMATCH,              PTWC_RARE,	     "wait in example operator BestMatch" ),  \
    /* PTWC_ZERO, for these the explanation is can be used as a warning if they occur */ \
    PTW_ENTRY(PTW_SEM_BAR_DEFAULT,              PTWC_ZERO,       "debits outer PTW_SML_BAR_DEFAULT"),\
    PTW_ENTRY(PTW_SEM_NET_RCV,                  PTWC_ZERO,       "debits outer PTW_SWT_NET_RCV") /*no comma*/

/**
 * enumerations of finer-grained 'sub-categories' of wait time.
 * eventually someday replaced by registered file and line number locations in the code
 * these are grouped into PTWC_ categories for coarse-grained reporting
 * their direct use is intended for debugging
 */
#define PTW_ENTRY(PTW, PTC, MEANING) PTW
enum perfTimeWait {
    PTW_TABLE_ENTRIES,
    PTW_NUM,           // must follow _TABLE_ENTRIES
    PTW_FIRST=0
};
#undef PTW_ENTRY
using perfTimeWait_e = enum perfTimeWait ;


/// a convenience function.
/// @return: a value such that differences measure the passage of real time
///         (not guaranteed to be coincident with wall-clock time)
/// @note:  noexcept so query execution immune from timing failures
double perfTimeGetReal() noexcept;

/// a convenience function
///
/// @param: usr the user time in seconds used by the current thread
/// @param: sys the system time in seconds used by the current thread
/// @note:  noexcept so query execution immune from timing failures
void   perfTimeGetThreadTimes(double& usr, double& sys) noexcept;

/// Get the real (not necessarily synchronized with wall-clock) time in microseconds
/// @note:  noexcept so query execution immune from timing failures
///
uint64_t perfTimeGetElapsedInMicroseconds() noexcept;


/// Get combined system and user time usage measures in microseconds
/// @param who one of the following {RUSAGE_SELF, RUSAGE_THREAD, RUSAGE_CHILDREN}
/// See man rusage for more information
/// @note:  noexcept so query execution immune from timing failures
uint64_t perfTimeGetCpuInMicroseconds(int who) noexcept;

/// @class ScopedWaitTimer Measures time between construction and destruction.
///        At destruction adds the time to PerfTimeAggData
///
/// @todo JHM: move to separate .h,.cpp
///
class ScopedWaitTimer {
public:
    /// @param ptw enumerator of the wait scope
    /// @param logOnCompletion emit TRACE message on destruction
    /// @note:  noexcept so query execution immune from timing failures
    ///
    ScopedWaitTimer(perfTimeWait_e ptw, bool logOnCompletion = false) noexcept;

    ~ScopedWaitTimer();

    /**
     * @note:  called from runSciDB else
     *         infinite recursion (through mutex) can occur.
     */
    static void          adjustWaitTimingEnabled() ;
private:
    void                 init() noexcept ;
    static bool          isWaitTimingEnabled() noexcept ;

    perfTimeWait_e       _ptw;
    double               _secStartReal;
    bool                 _isEnabled;
    static bool          _isWaitTimingEnabled;
    bool                 _logOnCompletion;

    static thread_local  uint64_t _nestingDepth;  // std::atomic_unit_fast32_t
                                                  // if it were not thread_local
};


/// @param ptw a PTW_ enum relating to the code location
/// @return the PTWC_ enum for the category that the PTW_ is in
perfTimeWaitCategory_e ptwToCategory(const perfTimeWait_e ptw);

/// @param pts a PTS_ enum relating to the code location
/// @return the PTSC_ enum for the category that the PTS_ is in
perfTimeScopeCategory_e ptsToCategory(const perfTimeScope_e pts);

/// @param ptw a PTW_ enum relating to the code location
/// @return a char* string representation of the enumeration
const char* ptwName(const perfTimeWait_e ptw);

/// @param pts a PTS_ enum relating to a timing activity
/// @return a char* string representation of the enumeration
const char* ptsName(const perfTimeScope_e pts);

/// @param ptwc a PTWC_ enum
/// @return a char* string representation of the enumeration
const char* ptwcName(const perfTimeWaitCategory_e ptwc);

/// @param ptwc a PTWC_ enum
/// @return a char* string representation of the long name (message) explaining the enumeration
const char* ptwcMessage(const perfTimeWaitCategory_e ptwc);

/// @param ptsc a PTSC_ enum
/// @return a char* string representation of the enumeration
const char* ptscName(const perfTimeScopeCategory_e ptsc);

} //namespace

#endif /* PERF_TIME_H_ */
