//
// @P4COPYRIGHT_SINCE:2008@
//
#include <util/logVmSizes.h>

#include <system/ErrorCodes.h>
#include <system/Exceptions.h>

#include <fcntl.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <unistd.h>

using namespace std;

namespace scidb {

DECLARE_SYSTEM_EXCEPTION_SUBCLASS(CannotParseStatusException,
                                  SCIDB_SE_INTERNAL,
                                  SCIDB_LE_PROC_STATUS_PARSE_ERROR);

long long getValueFromString(log4cxx::LoggerPtr logger, const char* label, const char* haystack)
{
    // repeating pattern, factor to function
    const char * start = strstr(haystack, label);
    if (!start) {
        // LOG4CXX_ERROR(logger, "scidb::logVmSizes: could not find " << label << " in " << haystack);
        // throw SYSTEM_EXCEPTION_SUBCLASS(CannotParseStatusException) << label;
        // dz szu lab Ubuntu1604 cat/pid/status 没有RssAson, RssFile, RssShmem这三个参数，所以这里会抛异常，这个只是用于统计，所以先屏蔽了
        return 0ll; // dz 先直接返回0
    }
    long long value;
    int numItems = sscanf(start+strlen(label), ": %lld kB", &value);
    if (numItems <= 0) {
        LOG4CXX_ERROR(logger, "scidb::logVmSizes: could not parse the " << label << " field in " << haystack);
        throw SYSTEM_EXCEPTION_SUBCLASS(CannotParseStatusException) << label;
    }
    return value;
}

void logVmSizes(log4cxx::LoggerPtr logger, const std::string& msg, const ::log4cxx::spi::LocationInfo& location) {

    LOG4CXX_TRACE(logger, "scidb::logVmSizes: caller msg: " << msg);

    // want /proc/pid/status
    // lines 12 (VmPeak) - 24 (VmLib)
    // for now, just read the whole thing

    char pathBuffer[1000];
    sprintf(pathBuffer, "/proc/%d/status", getpid());

    int fd = open(pathBuffer, O_RDONLY);
    if (fd == -1) {
        LOG4CXX_ERROR(logger, "could not open " << pathBuffer);
        return; // not a throw: diagnostic logging avoid changing execution of caller
    }
    char readBuffer[10000];
    ssize_t bytes = read(fd, readBuffer, sizeof(readBuffer)-1);
    if (bytes <= 0) {
        LOG4CXX_ERROR(logger, "could not read from " << pathBuffer);
        return; // not a throw: diagnostic logging avoid changing execution of caller
    }
    close(fd);
    readBuffer[bytes]=0;

    LOG4CXX_TRACE(logger, "scidb::logVmSizes: pathBuffer: " << pathBuffer);
    LOG4CXX_TRACE(logger, "scidb::logVmSizes: eadBytes: " << bytes);
    LOG4CXX_TRACE(logger, "scidb::logVmSizes: readBuffer: " << readBuffer);

    // extract 5 values from readBuffer for logging
    long long vmHwm = 0;
    long long vmRss = 0;
    long long rssAnon = 0;
    long long rssFile = 0;
    long long rssShmem = 0;
    try {
        vmHwm = getValueFromString(logger, "VmHWM", readBuffer);
        LOG4CXX_TRACE(logger, "VmHWM: " << vmHwm);

        vmRss = getValueFromString(logger, "VmRSS", readBuffer);
        LOG4CXX_TRACE(logger, "VmRss: " << vmRss);

        rssAnon = getValueFromString(logger, "RssAnon", readBuffer);
        LOG4CXX_TRACE(logger, "RssAnon: " << rssAnon);

        rssFile = getValueFromString(logger, "RssFile", readBuffer);
        LOG4CXX_TRACE(logger, "RssFile: " << rssFile);

        rssFile = getValueFromString(logger, "RssShmem", readBuffer);
        LOG4CXX_TRACE(logger, "RssShmem: " << rssFile);

    } catch(CannotParseStatusException&) {
        return ;    // error already logged by getValueFromString
    }

    std::stringstream ss;
    ss << "VmHWM: " << vmHwm;
    ss << " VmRSS: " << vmRss;
    ss << " RssAnon: " << rssAnon;
    ss << " RssFile: " << rssFile;
    ss << " RssShmem: " << rssShmem;
    ss << " " << msg;

    logger->forcedLog(::log4cxx::Level::getDebug(), ss.str(), location);
}

} // namespace scidb
