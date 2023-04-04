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

// clang-format off

/*  Ccm.inc.h
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  These messages define the Ids of the Protocol Buffer messages defined in
 *  Ccm.proto. The code is the value given in the CcmMsgHeader.msgtype.
 *
 *  Note about modifying this file:
 *   - There MUST be a corresponding Protocol Buffer message in Ccm.proto
 *   - Add the new enumeration before MSGMAX
 *   - Update the MSGMAX code value to be one more than the largest code value of the other
 *     messages.
 *   - DO NOT LEAVE any gaps in the Code sequence. If a message is removed, then update the
 *     the Name to an "Unused_X, X, "Unused Message S" format (where X is the code of the
 *     removed message.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 *
 */

/* Name                      Code      Description */
X(ErrorMsg,                  0,        "Error Response Message")
X(AuthLogonMsg,              1,        "AuthLogon Request (part 1) of handshake.")
X(AuthChallengeMsg,          2,        "AuthChallenge Response (part 2) of handshake.")
X(AuthResponseMsg,           3,        "AuthResponse Request (part 3) of handshake.")
X(AuthCompleteMsg,           4,        "AuthComplete Response (part 4) of handshake.")
X(ExecuteQueryMsg,           5,        "ExecuteQuery Request")
X(ExecuteQueryResponseMsg,   6,        "ExecuteQuery Response")
X(FetchIngotMsg,             7,        "FetchIngot Request")
X(FetchIngotResponseMsg,     8,        "FetchIngot Response")
// add new values here.
X(MSGMAX,                    9,        "Sentinel Enum value.. THIS MUST BE LAST")
// clang-format on

// Local Variables:
// mode: c++
// End:
