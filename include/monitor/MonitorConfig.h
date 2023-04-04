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
 * MonitorConfig.h
 *      Author: marty.corbett@gmail.com
 *      Description: Provides class for storing config.ini resource monitoring variables
 */

#ifndef MONITOR_CONFIG_H_
#define MONITOR_CONFIG_H_

//#include <atomic>
#include <functional>
#include <system/Config.h>
#include "util/Mutex.h"
#include <util/Singleton.h>

namespace scidb
{
    /**
     * Captures the variables located in config.ini for resource monitoring during the
     * constructor.  If _setopt is subsequently called on one of these variables a callback
     * function connnected with the Config class will be notified.  This callback function
     * and will then dispatch to other callback functions that have registered for notification
     * of the particular variable of interest.
     */
    class MonitorConfig : public Singleton<MonitorConfig>
    {
    private:

        /// Tracks the "resource-monitoring" as set in the config.ini or via setopt
        bool        _enabled;

        /// Tracks the "stats-query-history-size" as set in the config.ini or via setopt
        uint32_t    _queryHistorySize;

        /// Save the config instance pointer to minimize calls to Config::getInstance().
        scidb::Config *_config;

        /**
         * Retrieve the value for the "resource-monitoring" config variable
         */
        bool getConfigEnabled()
        {
            return _config->getOption<bool>(CONFIG_RESOURCE_MONITORING);
        }

        /**
         * Retrieve the value for the "stats-query-history-size" config variable
         */
        uint32_t getConfigHistorySize()
        {
            return  static_cast<uint32_t>(
                        _config->getOption<int>(
                            CONFIG_STATS_QUERY_HISTORY_SIZE));
        }

        void testQueryHistorySize(uint32_t queryHistorySize);

    public:
        /**
         * Constructor - sets up the callback hook with the Config class
         */
        MonitorConfig();

        /**
         * The Config class requires a static callback function.  This method provides for that
         * callback and redirects the callback to the singleton setConfigOption method.
         *
         * @param configOption - the option that is being changed
         */
        static void onConfigSetOption(int32_t configOption);
        void onSetOption(int32_t configOption);

        /**
         * Static callback function for when _setopt is called with 'resource-monitoring'
         */
        static void onConfigResourceMonitoringChanged();
        void onResourceMonitoringChanged();

        /**
         * Static callback function for when _setopt is called with 'stats-query-history-size'
         */
        static void onConfigQueryHistorySizeChanged();
        void onQueryHistorySizeChanged();


        /**
         * Declare whether resource-monitoring is enabled
         */
        bool isEnabled() const
        {
            return _enabled;
        }

        /**
         * Retrieve the value for the QueryHistorySize
         */
        uint32_t getQueryHistorySize() const
        {
            return _queryHistorySize;
        }
    };  // class MonitorConfig
} // namespace scidb

#endif // MONITOR_CONFIG_H_

