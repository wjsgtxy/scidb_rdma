#ifndef MESSAGEDESCRIPTION_H_
#define MESSAGEDESCRIPTION_H_
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

#include <network/NetworkMessage.h>  // for MessageID, MessagePtr
#include <query/InstanceID.h>
#include <boost/asio.hpp>

namespace scidb {

class SharedBuffer;
class QueryID;

   /**
    * Network message descriptor
    */
   class MessageDescription
   {
   public:
      virtual InstanceID getSourceInstanceID() const = 0;
      virtual MessagePtr getRecord() = 0;
      virtual MessageID getMessageType() const = 0;
      virtual boost::asio::const_buffer getBinary() = 0;
      virtual std::shared_ptr<scidb::SharedBuffer> getMutableBinary() = 0;
      virtual ~MessageDescription() {}
      virtual QueryID getQueryId() const = 0;
   };

}  // namespace scidb
#endif
