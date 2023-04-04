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

/*  CcmErrorCode.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  These messages define the Error IDs used in The Client Communications manager
 *
 *  Note about modifying this file:
 *   - Add the new enumeration at the end of this. Do NOT Skip any number.
 *   - The Code for an error  should NOT change. Clients use these codes.
 *   - Do NOT remove any CcmErrorCodes, but alter the name to UNKNOWN_X, X, 'Unknown Key X' for
 *     the code that is being removed.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 *
 */

// clang-format off

/* Name                      Code  Description */
X(GENERAL_ERROR,             0,    "General unspecified error")
X(INVALID_MSG_HEADER,        1,    "Invalid message header")
X(MALFORMED_REQUEST,         2,    "Malformed protocol buffer payload")
X(INVALID_REQUEST,           3,    "Invalid mesasge type.")
X(READ_TIMEOUT,              4,    "Input timer for reading expired")
X(RESTART_HANDSHAKE,         5,    "Authentication must be (re)started from the beginning of the 4-way handshake.")
X(UNEXPECTED_MSGTYPE,        6,    "Unexpected message request Received")
X(UNAUTH_EXECUTE,            7,    "Unable to execute query. Session is unauthenticated or session timed out.")
X(UNAUTH_FETCH,              8,    "Unable to fetch data. Session is unauthenticated or session timed out.")
X(AUTH_COOKIE_MISMATCH,      9,    "Authentication token mismatch")
X(EXEC_IN_START,            10,    "Unable to execute query. Session is still authenticating.")
X(FETCH_IN_START,           11,    "Unable to Fetch data. Session is still authenticating.")
X(INVALID_QUERY,            12,    "Invalid query string")
X(BUSY,                     13,    "A query is already active for this session.")
X(NOQUERY_FETCH,            14,    "No active query from which to fetch data.")
X(FETCH_BAD_ID,             15,    "ccmConversation identifier mismatch.")
// clang-format on

// Local Variables:
// mode: c++
// End:
