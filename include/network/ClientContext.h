#ifndef CLIENTCONTEXT_H_
#define CLIENTCONTEXT_H_
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


namespace scidb
{

class Query;
class QueryID;

   /**
    * Abstract client context
    */
   class ClientContext
   {
   public:
       typedef std::shared_ptr<ClientContext> Ptr;

       /// Client connection disconnect handler
       typedef std::function<void(const std::shared_ptr<Query>&)> DisconnectHandler;

       /**
        * Attach a query specific handler for client's disconnect
        * @param queryID query ID
        * @param dh disconnect handler
        */
       virtual void attachQuery(QueryID queryID, DisconnectHandler& dh) = 0;

       /**
        * Detach a query specific handler for client's disconnect
        * @param queryID query ID
        */
       virtual void detachQuery(QueryID queryID) = 0;

       /**
        * Indicate that the context should no longer be used
        */
       virtual void disconnect() = 0;

       virtual ~ClientContext() {}
   };

}  // namespace scidb
#endif
