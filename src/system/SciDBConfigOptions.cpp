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

#include "SciDBConfigOptions.h"

#include "stdint.h"

#include "system/Config.h"
#include "system/Constants.h"
#include <unistd.h>

using namespace std;

namespace scidb
{

void configHook(int32_t configOption)
{
    switch (configOption)
    {
        case CONFIG_CONFIG:
            Config::getInstance()->setConfigFileName(
                Config::getInstance()->getOption<string>(CONFIG_CONFIG));
            break;

        case CONFIG_HELP:
            cout << "Available options:" << endl
                << Config::getInstance()->getDescription() << endl;
            exit(0);
            break;

        case CONFIG_VERSION:
            cout << SCIDB_BUILD_INFO_STRING() << endl;
            exit(0);
            break;
    }
}
// 初始化配置文件
void initConfig(int argc, char* argv[])
{
    Config *cfg = Config::getInstance();

    // WARNING: When using the EIC multipliers (KiB, MiB, GiB, etc.)
    // or any other size_t 64-bit value, make sure to use Config::SIZE
    // rather than Config::INTEGER.  Otherwise at runtime you'll get a
    // boost::bad_any_cast exception when reading the option value
    // (cannot extract 64-bit unsigned size_t into 32-bit signed int).

    // ANOTHER WARNING: If you don't use the EIC multipliers but do
    // use Config::SIZE, make sure to use the UL suffix after your
    // constants (or indicate somehow else to the compiler that the
    // constants are not of type int).  Failure to do so can prevent
    // scidb from starting.

    // YEAH, YEAH, YEAH: When adding a Config::BOOLEAN, be sure to
    // update the BOOL_OPTIONS list in utils/scidblib/scidbctl_common.py.

    cfg->addOption
        (CONFIG_CATALOG, 'c', "catalog", "CATALOG", "", Config::STRING,
            "Catalog connection string, but without any password.  "
            "See https://www.postgresql.org/docs/9.5/static/libpq-connect.html#LIBPQ-CONNSTRING for guidance.")
        (CONFIG_LOGCONF, 'l', "logconf", "LOG_PROPERTIES", "",
            Config::STRING, "Log4cxx properties file.", string(""), false)
        (CONFIG_PORT, 'p', "port", "PORT", "", Config::INTEGER, "Set port for instance.", 0, false)
        // CONFIG_HOST uses 'i' for backward compatibility.  Unfortunately 'h' and 'H' are taken.
        (CONFIG_HOST, 'i', "host", "SCIDB_HOST", "", Config::STRING,
         "Host name or address for listening connections.", string("0.0.0.0"), false)
        (CONFIG_REGISTER, 'r', "register", "", "", Config::BOOLEAN,
            "Register instance in system catalog.", false, false)
        // "online" means "when instance first joined the membership".  XXX Rename, perhaps to "first-joined".
        (CONFIG_ONLINE, 'o', "online", "ONLINE", "", Config::STRING, "Time when instance was registered",
                string("now"), false)
        (CONFIG_REDUNDANCY, 0, "redundancy", "", "", Config::SIZE,
            "Level of redundancy.", 0UL, false)
        (CONFIG_INITIALIZE, 0, "initialize", "", "", Config::BOOLEAN,
            "Initialize cluster.", false, false)
        (CONFIG_STORAGE, 's', "storage", "STORAGE", "", Config::STRING, "Storage URL.",
                string("./storage.scidb"), false)
        (CONFIG_PLUGINSDIR, 'u', "pluginsdir", "PLUGINS", "", Config::STRING, "Plugins folder.",
            string(SCIDB_INSTALL_PREFIX()) + string("/lib/scidb/plugins"), false)
        (CONFIG_SMGR_CACHE_SIZE, 'm', "smgr-cache-size", "CACHE", "", Config::INTEGER,
            "Size of storage cache (Mb).", 256, false)
        (CONFIG_CONFIG, 'f', "config", "", "", Config::STRING,
                "Instance configuration file.", string(""), false)
        (CONFIG_HELP, 'h', "help", "", "", Config::BOOLEAN, "Show this text.",
                false, false)
        (CONFIG_STRING_SIZE_ESTIMATION, 0, "string-size-estimation", "STRING_SIZE_ESTIMATION", "", Config::INTEGER,
            "Average string size (bytes).", DEFAULT_STRING_SIZE_ESTIMATION, false)
        (CONFIG_STORAGE_MIN_ALLOC_SIZE_BYTES, 0, "storage-min-alloc-size-bytes", "STORAGE_MIN_ALLOC_SIZE_BYTES", "", Config::INTEGER,
         "Size of minimum allocation chunk in storage file.", 512, false)
        (CONFIG_DAEMON_MODE, 'd', "daemon-mode", "", "", Config::BOOLEAN, "Run scidb in background.",
                false, false)
        (CONFIG_MEM_ARRAY_THRESHOLD, 'a', "mem-array-threshold", "MEM_ARRAY_THRESHOLD", "", Config::SIZE,
                "Maximal size of memory used by temporary in-memory array (MiB)", DEFAULT_MEM_THRESHOLD, false)
        (CONFIG_REDIM_CHUNK_OVERHEAD_LIMIT, 0, "redim-chunk-overhead-limit-mb",
         "REDIM_CHUNK_OVERHEAD_LIMIT", "", Config::SIZE,
         "Redimension memory usage for chunk headers will be limited to this "
         "value in MiB (0 disables check)", 0*MiB, false)
        (CONFIG_TARGET_CELLS_PER_CHUNK, 0, "target-cells-per-chunk", "TARGET_CELLS_PER_CHUNK", "", Config::SIZE,
         "Desired number of cells per chunk", 1000000UL, false)
        (CONFIG_TARGET_MB_PER_CHUNK, 0, "target-mb-per-chunk", "TARGET_MB_PER_CHUNK", "", Config::SIZE,
         "Desired physical size of chunks in MiB", 0UL, false)
        (CONFIG_CHUNK_SIZE_LIMIT, 0, "chunk-size-limit-mb",
         "CHUNK_SIZE_LIMIT", "", Config::SIZE,
         "Maximum allowable chunk size in MiB (0 disables check)", 0*MiB, false)
        (CONFIG_RESULT_PREFETCH_THREADS, 't', "result-prefetch-threads", "EXEC_THREADS", "", Config::INTEGER,
                "Number of execution threads for concurrent processing of chunks of one query", 4, false)
        (CONFIG_RESULT_PREFETCH_QUEUE_SIZE, 'q', "result-prefetch-queue-size", "PREFETCHED_CHUNKS", "", Config::INTEGER,
                "Number of prefetch chunks for each query", 4, false)
        (CONFIG_EXECUTION_THREADS, 'j', "execution-threads", "MAX_THREADS", "", Config::INTEGER,
         "Max. number of queries that can be processed in parallel", 6, false)
        (CONFIG_CLIENT_QUERIES, 0, "client-queries", "CLIENT_QUERIES", "", Config::INTEGER,
                "Number of queries a coordinator instance can initiate in parallel; if 0, execution-threads is used instead", 0, false)
        (CONFIG_ADMIN_QUERIES, 0, "admin-queries", "ADMIN_QUERIES", "", Config::INTEGER,
                "Number of queries a coordinator instance can initiate in parallel with administrative priority", 1, false)
        (CONFIG_OPERATOR_THREADS, 'x', "operator-threads", "USED_CPU_LIMIT", "", Config::INTEGER,
                "Max. number of threads for concurrent processing of one chunk", 0, false)
        (CONFIG_MERGE_SORT_BUFFER, 0, "merge-sort-buffer", "MERGE_SORT_BUFFER", "", Config::INTEGER,
                "Maximal size for in-memory sort buffer (Mb)", 128, false)
        (CONFIG_MERGE_SORT_NSTREAMS, 0, "merge-sort-nstreams", "MERGE_SORT_NSTREAMS", "", Config::INTEGER,
                "Number of streams to merge at each level of sort", 8, false)
        (CONFIG_MERGE_SORT_JOBS, 0, "merge-sort-jobs", "MERGE_SORT_JOBS", "", Config::INTEGER,
                "Number of parallel sort jobs to use per instance (when possible)", 1, false)
        (CONFIG_MERGE_SORT_PIPELINE_LIMIT, 0, "merge-sort-pipeline-limit", "MERGE_SORT_PIPELINE_LIMIT", "", Config::INTEGER,
         "Max number of outstanding sorted runs before merging", 32, false)
        (CONFIG_CHUNK_RESERVE, 0, "chunk-reserve", "CHUNK_RESERVE", "", Config::INTEGER, "Percent of chunks size preallocated for adding deltas", 0, false)
        (CONFIG_VERSION, 'V', "version", "", "", Config::BOOLEAN, "Version.",
                false, false)
        (CONFIG_LOG_LEVEL, 0, "log-level", "LOG_LEVEL", "LOG_LEVEL", Config::STRING,
         "Level for basic log4cxx logger. Ignored if log-properties option is used. Default level is ERROR", string("error"), false)
        (CONFIG_X_CATALOG_DELAY_SECS, 0, "x-catalog-delay-seconds", "", "", Config::INTEGER,
         "During startup, maximum delay time used to stagger catalog access", 5, false)
        (CONFIG_LIVENESS_TIMEOUT, 0, "liveness-timeout", "LIVENESS_TIMEOUT", "", Config::INTEGER, "Time in seconds to wait before declaring a network-silent instance dead.",
       120, false)
        (CONFIG_DEADLOCK_TIMEOUT, 0, "deadlock-timeout", "DEADLOCK_TIMEOUT", "", Config::INTEGER,
         "Time in seconds to wait before declaring a query deadlocked.", 30, false)
        (CONFIG_NO_WATCHDOG, 0, "no-watchdog", "NO_WATCHDOG", "", Config::BOOLEAN, "Do not start a watch-dog process.",
                false, false)
        (CONFIG_TILE_SIZE, 0, "tile-size", "TILE_SIZE", "", Config::INTEGER, "Size of tile", 10000, false)
        (CONFIG_TILES_PER_CHUNK, 0, "tiles-per-chunk", "TILES_PER_CHUNK", "", Config::INTEGER, "Number of tiles per chunk", 100, false)
        (CONFIG_SYNC_IO_INTERVAL, 0, "sync-io-interval", "SYNC_IO_INTERVAL", "", Config::INTEGER, "Interval of time for io synchronization (milliseconds)", 0, false)
        (CONFIG_MULTISTREAM_NEXT_POS_THROW_NS, 0, "multistream-next-pos-throw-ns",
        "MULTISTREAM_NEXT_POS_THROW_NS", "", Config::INTEGER,
        "sleep in MultiStreamArray::nextChunk when no nextChunkPos (nanosec)", -1, false)
        (CONFIG_MAX_MEMORY_LIMIT, 0, "max-memory-limit", "MAX_MEMORY_LIMIT", "", Config::INTEGER, "Maximum amount of memory the scidb process can take up (mebibytes)", -1, false)
        (CONFIG_BUFFERMGR_SLOTS_MAX, 0, "buffermgr-slots-max", "BUFFERMGR_SLOTS_MAX", "", Config::INTEGER, "Max number of buffers in the buffer cache", 10*1000, false)
        (CONFIG_SMALL_MEMALLOC_SIZE, 0, "small-memalloc-size", "SMALL_MEMALLOC_SIZE", "", Config::SIZE, "Maximum size of a memory allocation request which is considered small (in bytes). Larger memory allocation requests may be allocated according to a different policy.", 64*KiB, false)
        (CONFIG_LARGE_MEMALLOC_LIMIT, 0, "large-memalloc-limit", "LARGE_MEMALLOC_LIMIT", "", Config::INTEGER, "Maximum number of large  (vs. small) memory allocations. The policy for doing large memory allocations may be different from the (default) policy used for small memory allocations. This parameter limits the number of outstanding allocations performed using the (non-default) large-size allocation policy.", std::numeric_limits<int>::max(), false)
        (CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE, 0, "replication-receive-queue-size", "REPLICATION_RECEIVE_QUEUE_SIZE", "", Config::INTEGER, "The length of incoming replication queue (across all connections)", 64, false)
        (CONFIG_REPLICATION_SEND_QUEUE_SIZE, 0, "replication-send-queue-size", "REPLICATION_SEND_QUEUE_SIZE", "", Config::INTEGER, "The length of outgoing replication queue (across all connections)", 4, false)
        (CONFIG_SG_RECEIVE_QUEUE_SIZE, 0, "sg-receive-queue-size", "SG_RECEIVE_QUEUE_SIZE", "", Config::INTEGER, "The length of incoming sg queue (across all connections)", 8, false)
        (CONFIG_SG_SEND_QUEUE_SIZE, 0, "sg-send-queue-size", "SG_SEND_QUEUE_SIZE", "", Config::INTEGER, "The length of outgoing sg queue (across all connections)", 16, false)
        (CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT, 0, "array-emptyable-by-default", "ARRAY_EMPTYABLE_BY_DEFAULT", "", Config::BOOLEAN, "By default arrays are emptyable", true, false)
        (CONFIG_LOAD_SCAN_BUFFER, 0, "load-scan-buffer", "LOAD_SCAN_BUFFER", "", Config::INTEGER, "Number of MB for one input buffer used in InputScanner", 1, false)
        (CONFIG_MATERIALIZED_WINDOW_THRESHOLD, 0, "materialized-window-threshhold", "MATERIALIZED_WINDOW_THRESHHOLD", "", Config::INTEGER, "Size in Mebibytes above which we will not materialize the input chunk to a window(...) operation", 128, false)
        (CONFIG_MPI_DIR, 0, "mpi-dir", "MPI_DIR", "", Config::STRING, "Location of MPI installation.", DEFAULT_MPI_DIR(), false)
        (CONFIG_MPI_IF, 0, "mpi-if", "MPI_IF", "", Config::STRING, "Network interface to use for MPI traffic", string(""), false)
        (CONFIG_MPI_TYPE, 0, "mpi-type", "MPI_TYPE", "", Config::STRING, "MPI installation type [mpich2scidb | mpich | openmpi].", DEFAULT_MPI_TYPE(), false)
        (CONFIG_MPI_SHM_TYPE, 0, "mpi-shm-type", "MPI_SHM_TYPE", "", Config::STRING, "MPI shared memory type [SHM | FILE].", string("SHM"), false)
        (CONFIG_CATALOG_RECONNECT_TRIES, 0, "catalog-reconnect-tries", "CONFIG_CATALOG_RECONNECT_TRIES", "", Config::INTEGER, "Count of tries of catalog reconnection", 5, false)
        (CONFIG_QUERY_MAX_SIZE, 0, "query-max-size", "CONFIG_QUERY_MAX_SIZE", "", Config::SIZE, "Max number of bytes in query string", 16*MiB, false)
        (CONFIG_REQUESTS, 0, "requests", "MAX_REQUESTS", "", Config::INTEGER,
         "Max. number of client query requests queued for execution. Any requests in excess of the limit are returned to the client with an error.", 256, false)
        (CONFIG_ENABLE_CATALOG_UPGRADE, 0, "enable-catalog-upgrade", "ENABLE_CATALOG_UPGRADE", "", Config::BOOLEAN, "Set to true to enable the automatic upgrade of SciDB catalog", false, false)
        (CONFIG_REDIMENSION_CHUNKSIZE, 0, "redimension-chunksize", "REDIMENSION_CHUNKSIZE", "", Config::SIZE, "Chunksize for internal intermediate array used in operator redimension", 10*KiB, false)
        (CONFIG_MAX_OPEN_FDS, 0, "max-open-fds", "MAX_OPEN_FDS", "", Config::INTEGER, "Maximum number of fds that will be opened by the storage manager at once", 256, false)
        (CONFIG_PREALLOCATE_SHARED_MEM, 0, "preallocate-shared-mem", "PREALLOCATE_SHM", "", Config::BOOLEAN, "Make sure shared memory backing (e.g. /dev/shm) is preallocated", true, false)
        (CONFIG_INSTALL_ROOT, 0, "install-root", "INSTALL_ROOT", "", Config::STRING, "The installation directory from which SciDB runs", string(SCIDB_INSTALL_PREFIX()), false)
        (CONFIG_INPUT_DOUBLE_BUFFERING, 0, "input-double-buffering", "INPUT_DOUBLE_BUFFERING", "", Config::BOOLEAN,
         "Use double buffering where possible in input and load operators", true, false)
        (CONFIG_SECURITY, 'S', "security", "", "", Config::STRING,
                "Security mode.", string("trust"), false)
        (CONFIG_ENABLE_CHUNKMAP_RECOVERY, 0, "enable-chunkmap-recovery", "ENABLE_CHUNKMAP_RECOVERY", "", Config::BOOLEAN, "Set to true to enable recovery of corrupt chunk-map entires on startup.", false, false)
        (CONFIG_SKIP_CHUNKMAP_INTEGRITY_CHECK, 0, "skip-chunkmap-integrity-check", "SKIP_CHUNKMAP_INTEGRITY_CHECK", "", Config::BOOLEAN, "Set to true to skip all chunkmap integrity checks on startup.", false, false)
        (CONFIG_OLD_OR_NEW_WINDOW, 0, "window-old-or-new", "OLD_OR_NEW_WINDOW", "", Config::BOOLEAN, "Set to true to have OLD window algorithm, false to get the NEW window algorithm.", false, false)
        (CONFIG_AUTOCHUNK_MAX_SYNTHETIC_INTERVAL, '\0', "autochunk-max-synthetic-interval",
         "AUTOCHUNK_MAX_SYNTHETIC_INTERVAL", "", Config::SIZE,
         "Largest chunk interval to allow for the synthetic dimension if that dimension is autochunked.", 20UL, false)
        (CONFIG_PERTURB_ARR_RES, 0, "perturb-array-residency", "PERTURB_ARR_RES", "", Config::STRING, "Generate non-default array residencies when storing an array", std::string(""), false)
        (CONFIG_RESOURCE_MONITORING, 'R', "resource-monitoring", "", "", Config::BOOLEAN,
                "Resource monitoring.", false, false)
        (CONFIG_STATS_QUERY_HISTORY_SIZE, 'H', "stats-query-history-size", "", "", Config::INTEGER,
                "Statistics query history size .", 32, false)
        (CONFIG_POOLED_ALLOCATION, 0, "pooled-allocation", "POOLED_ALLOCATION", "", Config::BOOLEAN,
                "Allocate scidb::Value objects in individual query arenas", true, false)
        (CONFIG_PERF_WAIT_TIMING, 0, "perf-wait-timing", "PERF_WAIT_TIMING", "", Config::INTEGER,
                "Enable timing of wait timing", 0, false) // note: default of false with Config::BOOLEAN causes complaint about no such option "--timing"
        (CONFIG_SWAP_TEMP_METADATA, 0, "swap-temp-metadata", "SWAP_TEMP_METADATA", "",
         Config::INTEGER, "Enable swapping of metadata for temp arrays.", 0, false)
        (CONFIG_FILTER_USE_RTREE, 0, "filter-use-rtree", "FILTER_USE_RTREE", "", Config::BOOLEAN,
                "Use Rtree to speedup filter and cross_between in presence of many ranges", true, false)
        (CONFIG_FLATTEN_FASTPATH, 0, "flatten-fastpath", "flatten-fastpath", "FLATTEN_FASTPATH", Config::INTEGER,
         "Enable experimental fast path in flatten() operator", 0, false)
        (CONFIG_PGPASSFILE, 0, "pgpassfile", "PGPASSFILE", "", Config::STRING,
         "Location of file to search for Postgres password", std::string(""), false)
        (CONFIG_IO_PATHS_LIST, 0, "io-paths-list", "", "", Config::STRING,
         "Absolute paths of directories that non-administative users may save to or load from.", string(), false)
        (CONFIG_CLIENT_AUTH_TIMEOUT, 0, "client-auth-timeout", "CLIENT_AUTH_TIMEOUT", "", Config::INTEGER,
         "Seconds to wait before disconnecting clients who are slow to authenticate", 30, false)
        (CONFIG_PAM_OPTIONS, 0, "pam-options", "", "", Config::STRING,
         "Pluggable authentication module token list, for example 'setcred,session'", std::string(""), false)
        (CONFIG_MAX_ARENA_PAGE_SIZE, 0, "max-arena-page-size", "MAX_ARENA_PAGE_SIZE", "", Config::INTEGER,
         "Maximum arena page size in MiB", -1, false)
        (CONFIG_X_DATAFRAME_PHYS_IID, 0, "x-dataframe-phys-iid", "X_DATAFRAME_PHYSICAL_IID", "", Config::INTEGER,
         "Experimental, enabled by default.  Make dataframes use physical instance ID for $inst dimension",
         1, false)
        (CONFIG_DATASTORE_PUNCH_HOLES, 0, "datastore-punch-holes", "DATASTORE_PUNCH_HOLES", "", Config::INTEGER,
                "Enable release of datastore blocks to filesystem during remove_versions()", 1, false)
        // The following 3 settings are for regression testing, not supportable for end-users
        // note: prefer CONFIG_X_DFLT_DIST, but scidbctl prohibits any from being a prefix of another
        (CONFIG_X_DFLT_DIST_OTHER, 0, "x-dflt-dist-other", "X_DFLT_DIST_OTHER", "", Config::INTEGER,
        "DistType returned from defaultDistType()", 1, false)   // the value 1 == dtHashedParititioning
        (CONFIG_X_DFLT_DIST_INPUT, 0, "x-dflt-dist-input", "X_DFLT_DIST_INPUT", "", Config::INTEGER,
        "DistType returned from defaultDistTypeInput()", 1, false)   // the value 1 == dtHashedParititioning
        (CONFIG_X_DFLT_DIST_ROOT, 0, "x-dflt-dist-root", "X_DFLT_DIST_ROOT", "", Config::INTEGER,
        "DistType returned from defaultDistTypeRoot()", 1, false)   // the value 1 == dtHashedParititioning
        (CONFIG_CCM_PORT, 0, "ccm-port", "CCM_PORT", "", Config::INTEGER,
         "Listening port for Client Communication.", 5239, false)
        (CONFIG_CCM_TLS, 0, "ccm-use-tls", "CCM_USE_TLS", "", Config::INTEGER,
         "Use TLS for Client Communication", 0, false)
        (CONFIG_CCM_SESSION_TIME_OUT, 0, "ccm-session-time-out", "CCM_SESSION_TIME_OUT", "", Config::INTEGER,
         "Maximum time between Client Sessions Messages (in seconds) before timeout.", 2700, false)
        (CONFIG_CCM_READ_TIME_OUT, 0, "ccm-read-time-out", "CCM_READ_TIME_OUT", "", Config::INTEGER,
         "Maximum time (in seconds) to allow for reading  Client Message Requests.", 10, false)
        (CONFIG_DISKINDEX_RENUMBER_EBM, 0, "diskindex-renumber-ebm", "DISKINDEX_RENUMBER_EBM", "", Config::INTEGER,
         "Renumber the EBM at-rest in the disk index from attribute N to EBM_ATTRIBUTE_ID", 1, false)
        (CONFIG_USE_RDMA, 'D', "use-rdma", "", "", Config::BOOLEAN, "use rdma or not.", false, false);

    cfg->addHook(configHook);
    cfg->parse(argc, argv, "");
}

} // namespace
