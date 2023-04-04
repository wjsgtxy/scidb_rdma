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

/*
 * MonitorCommunicator.h
 *
 *  Created on: Apr 28, 2016
 *      Author: mcorbett@paradigm4.com
 */

#ifndef MONITOR_PLUGIN_COMMUNICATOR_H_
#define MONITOR_PLUGIN_COMMUNICATOR_H_

#include <log4cxx/logger.h>

namespace scidb
{
    class Query;

    namespace monitor
    {
        static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.monitorPluginComm"));

        /**
         * Communication interface between the namespaces plugin and SciDB.
         */
        namespace Communicator
        {
            /**
             * Add the query specified to the monitoring history
             * @param query The query to add
             */
            void addQueryInfoToHistory(Query* query);

        } // namespace Communicator
    } // namespace monitor
} // namespace scidb

#endif // MONITOR_PLUGIN_COMMUNICATOR_H_
