#ifndef NETWORKMESSAGEFACTORY_H_
#define NETWORKMESSAGEFACTORY_H_
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

#include <network/NetworkMessage.h> // for MessageID
#include <functional> // dz 为了编译通过
namespace scidb {

class MessageDescription;

   /**
    * Network message factory allows for addition of network message handlers
    * on SciDB server (only).
    */
   class NetworkMessageFactory
   {
   public:
      typedef std::function< MessagePtr(MessageID) > MessageCreator;
      typedef std::function< void(const std::shared_ptr<MessageDescription>& ) > MessageHandler; // dz：不同类型消息处理回调函数      

      virtual bool isRegistered(const MessageID& msgId) = 0;
      virtual bool addMessageType(const MessageID& msgId,
                                  const MessageCreator& msgCreator,
                                  const MessageHandler& msgHandler) = 0;
      virtual bool removeMessageType(const MessageID& msgId) = 0;
      virtual MessagePtr createMessage(const MessageID& msgId) = 0;
      virtual MessageHandler getMessageHandler(const MessageID& msgId) = 0;
      virtual ~NetworkMessageFactory() {}
   };

}  // namespace scidb
#endif
