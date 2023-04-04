//
// @P4COPYRIGHT_SINCE:2008@
//
#ifndef LOGVMSIZES_H
#define LOGVMSIZES_H

#include <log4cxx/logger.h>
#include <string>


/// Logs a message to a specified logger with the DEBUG level.
/// @param logger the logger to be used.
/// @param message the message string to log.
#define SDB_LOG4_VM_SIZES(logger, message) \
    { \
        if (LOG4CXX_UNLIKELY(logger->isDebugEnabled())) { \
            ::log4cxx::helpers::MessageBuffer oss_; \
            scidb::logVmSizes(logger, oss_.str(oss_ << message), LOG4CXX_LOCATION); \
        } \
    }


namespace scidb {

/// Logs a message to a specified logger with the DEBUG level.
/// Intention is that it is only used through the SDB_LOG4_VM_SIZES macro, above
/// @param logger the logger to be used.
/// @param msg the message string to log.
/// @param location the log4 LocationInformation about the call site
void logVmSizes(log4cxx::LoggerPtr logger, const std::string& msg, const log4cxx::spi::LocationInfo& location);

}

#define LOGVMSIZES() ;

#endif // LOGVMSIZES_H
