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
 * @file NetworkMessage.h
 * @brief Common network related types.
 */

#ifndef NETWORK_MESSAGE_H_
#define NETWORK_MESSAGE_H_

#include <memory>
#include <google/protobuf/message.h>

namespace scidb
{
    typedef ::google::protobuf::Message Message;
    typedef std::shared_ptr<Message> MessagePtr;
    typedef uint16_t MessageID;

    /**
     * Message types
     */
    enum MessageType
    {
#       define X(_name, _code, _desc)    _name = _code ,
#       include <util/MessageTypes.inc>
#       undef X
    };

    std::string strMsgType(MessageID);

    /** @brief True iff msgId is part of the authentication handshake. */
    inline bool isAuthMessage(MessageID msgId)
    {
        return msgId == mtAuthLogon
            || msgId == mtAuthChallenge
            || msgId == mtAuthResponse
            || msgId == mtAuthComplete
            || msgId == mtAuthError;
    }

    /** @brief True iff this is a SciDB message type (i.e. not from a plugin) */
    inline bool isScidbMessage(MessageID msgId)
    {
        return msgId < mtSystemMax;
    }

    /**
     * @brief True if this could be a P4 plugin message type.
     *
     * @description The message could be a plugin message, or it could
     * be bogus.  Either way, if this returns true then look for
     * handlers in the NetworkMessageFactory (rather than among the
     * system message handlers).  Does not depend on mtPluginMax, see
     * <util/MessageTypes.inc>.  Also, recall that mtSystemMax itself
     * is unused.
     */
    inline bool isPluginMessage(MessageID msgId)
    {
        return msgId > mtSystemMax;
    }
}

#endif /* NETWORK_MESSAGE_H_ */
