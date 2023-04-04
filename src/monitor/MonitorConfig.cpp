/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
#include <monitor/MonitorConfig.h>

#include <log4cxx/logger.h>
#include <util/Notification.h>

//#include <fstream>

namespace scidb
{
    // Logger for network subsystem. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.impl_monitor"));

    MonitorConfig::MonitorConfig()
        : _enabled(false)
        , _queryHistorySize(32)
        , _config(scidb::Config::getInstance())
    {
        SCIDB_ASSERT(_config);

        // Get the initial values
        _enabled            = getConfigEnabled();
        _queryHistorySize   = getConfigHistorySize();

        // Register a hook with the Config class for callbacks
        _config->addHook(&onConfigSetOption);

        _config->addHookSetOpt(
            &onConfigResourceMonitoringChanged,
            "resource-monitoring");

        _config->addHookSetOpt(
            &onConfigQueryHistorySizeChanged,
            "stats-query-history-size");
    }

    void MonitorConfig::onConfigSetOption(int32_t configOption)
    {
        MonitorConfig::getInstance()->onSetOption(configOption);
    }

    void MonitorConfig::onConfigResourceMonitoringChanged()
    {
        MonitorConfig::getInstance()->onResourceMonitoringChanged();
    }

    void MonitorConfig::onConfigQueryHistorySizeChanged()
    {
        MonitorConfig::getInstance()->onQueryHistorySizeChanged();
    }

    void MonitorConfig::testQueryHistorySize(uint32_t queryHistorySize)
    {
        if(_queryHistorySize & (1 << 31))
        {
            LOG4CXX_ERROR(logger, "QueryHistorySize exceeds max value=" << _queryHistorySize);
            SCIDB_ASSERT(!(_queryHistorySize & (1 << 31)));
        }
    }

    void MonitorConfig::onSetOption(int32_t configOption)
    {
        switch (configOption)
        {
            case CONFIG_RESOURCE_MONITORING:
            {
                _enabled = getConfigEnabled();
            } break;

            case CONFIG_STATS_QUERY_HISTORY_SIZE:
            {
                _queryHistorySize = getConfigHistorySize();
                testQueryHistorySize(_queryHistorySize);
            } break;
        }
    }

    void MonitorConfig::onResourceMonitoringChanged()
    {
        _enabled = getConfigEnabled();
    }

    void MonitorConfig::onQueryHistorySizeChanged()
    {
        _queryHistorySize = getConfigHistorySize();
        testQueryHistorySize(_queryHistorySize);
    }

} // namespace scidb
