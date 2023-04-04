/* -*- C++ -*-
 *
 *  MessageTypes.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 * Instead you should include some file that includes this file, such
 * as NetworkMessage.h.
 *
 *  WHEN EDITING THIS FILE:
 *
 *  0. Begin unused names with "mtUnused", see MessageDesc::validate().
 *
 *  1. You'll probably have to increment NET_PROTOCOL_CURRENT_VER, see
 *     src/network/proto/scidb_msg.proto .
 *
 *  2. Going forward, code numbers are important for binary compatibility of
 *     client APIs, so please do not renumber.
 *
 *  3. Keep message types numerically sorted by code.
 *
 *  4. JDBC depends on this but at present the build dependency is
 *     broken, so you'll need to run.py cleanup/setup/make to get a
 *     good JDBC connector.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */
/* Name                             Code      Description */
X(mtNone,                           0,        "No message type.")
X(mtExecuteQuery,                   1,        "(Describe me!)")
X(mtPreparePhysicalPlan,            2,        "Request to begin the 2PC voting phase with a query to execute")
X(mtHangup,                         3,        "Stop reading on outbound instance-to-instance connection")
X(mtFetch,                          4,        "(Describe me!)")
X(mtChunk,                          5,        "(Describe me!)")
X(mtChunkReplica,                   6,        "(Describe me!)")
X(mtRecoverChunk,                   7,        "(Describe me!)")
X(mtReplicaSyncRequest,             8,        "(Describe me!)")
X(mtReplicaSyncResponse,            9,        "(Describe me!)")
X(mtUnusedPlus10,                   10,       "Be sure MessageDesc::validate() knows this is UNUSED.")
X(mtQueryResult,                    11,       "(Describe me!)")
X(mtError,                          12,       "Aborts query, indicates 2PC \"vote\" for failure")
X(mtSyncRequest,                    13,       "(Describe me!)")
X(mtSyncResponse,                   14,       "(Describe me!)")
X(mtCancelQuery,                    15,       "(Describe me!)")
X(mtRemoteChunk,                    16,       "(Describe me!)")
X(mtNotify,                         17,       "(Describe me!)")
X(mtWait,                           18,       "(Describe me!)")
X(mtBarrier,                        19,       "(Describe me!)")
X(mtBufferSend,                     20,       "(Describe me!)")
X(mtAlive,                          21,       "(Describe me!)")
X(mtPrepareQuery,                   22,       "(Describe me!)")
X(mtResourcesFileExistsRequest,     23,       "(Describe me!)")
X(mtResourcesFileExistsResponse,    24,       "(Describe me!)")
X(mtAbortRequest,                   25,       "Request to begin the 2PC completion phase with an abort")
X(mtCommitRequest,                  26,       "Request to begin the 2PC completion phase with a commit")
X(mtCompleteQuery,                  27,       "2PC voting phase completed successfully")
X(mtControl,                        28,       "(Describe me!)")
X(mtUpdateQueryResult,              29,       "(Describe me!)")
X(mtAuthLogon,                      30,       "Authentication handshake begins, client or peer requests logon")
X(mtAuthChallenge,                  31,       "Server requests proof of identity")
X(mtAuthResponse,                   32,       "Client or peer proves its identity")
X(mtAuthComplete,                   33,       "Server allows/denies logon")
X(mtAuthError,                      34,       "Error during authentication (ordinary mtError cannot be used)")
X(mtCommitResponse,                 35,       "Successful 2PC query commit completion acknowledgment")
X(mtAbortResponse,                  36,       "Successful 2PC query abort completion acknowledgment")

/* Marks last MessageType used by SciDB core. */
X(mtSystemMax,                      50,       "MUST BE LAST!  Renumber as needed, not used for real messages")

/*
 *  (Some) message types used by P4 plugins.
 *
 *  BEWARE!  There are other message types used by the plugins; these
 *  are just the ones that the SciDB core must know about.
 */
X(mtMpiSlaveHandshake,              51,       "(Describe me!)")
X(mtMpiSlaveResult,                 52,       "(Describe me!)")
X(mtMpiSlaveCommand,                53,       "(Describe me!)")
X(mtOBcastRequest,                  54,       "(Describe me!)")
X(mtOBcastReply,                    55,       "(Describe me!)")
X(mtLiveness,                       56,       "(Describe me!)")
X(mtLivenessAck,                    57,       "(Describe me!)")

/* Marks last P4 plugin MessageType known to the SciDB engine. */
X(mtPluginMax,                      58,       "MUST BE LAST!  Renumber as needed, not used for real messages.")
