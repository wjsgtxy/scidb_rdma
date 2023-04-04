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

#ifndef USEREXCEPTION_H_
#define USEREXCEPTION_H_

#include <system/Exceptions.h>

namespace scidb {

#define USER_EXCEPTION(_short_error, _long_error)               \
    scidb::UserException(REL_FILE, __FUNCTION__, __LINE__,      \
                         CORE_ERROR_NAMESPACE,                  \
                         _short_error, _long_error,             \
                         #_short_error, #_long_error)

#define USER_EXCEPTION_SPTR(_short_error, _long_error)  \
    std::make_shared<scidb::UserException>(             \
        REL_FILE, __FUNCTION__, __LINE__,               \
        CORE_ERROR_NAMESPACE,                           \
        int(_short_error), int(_long_error),            \
        #_short_error, #_long_error)

#define USER_QUERY_EXCEPTION(_short_error, _long_error, parsing_ctx)    \
    scidb::UserQueryException(REL_FILE, __FUNCTION__, __LINE__,         \
                              CORE_ERROR_NAMESPACE,                     \
                             _short_error, _long_error,                 \
                              #_short_error, #_long_error, parsing_ctx)

#define PLUGIN_USER_EXCEPTION(_err_space, _short_error, _long_error)    \
    scidb::UserException(REL_FILE, __FUNCTION__, __LINE__, _err_space,  \
                         _short_error, _long_error,                     \
                         #_short_error, #_long_error)

/**
 * Exceptions caused by user actions.
 * @see scidb::Exception
 */
class PLUGIN_EXPORT UserException : public Exception
{
public:
    UserException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const QueryID& query_id = INVALID_QUERY_ID);

    COMMON_EXCEPTION_METHODS(UserException)
};

COMMON_EXCEPTION_FREE_FUNCTIONS(UserException)

}  // namespace scidb

#endif
