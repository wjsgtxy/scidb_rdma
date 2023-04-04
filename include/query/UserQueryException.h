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

#ifndef USERQUERYEXCEPTION_H_
#define USERQUERYEXCEPTION_H_

#include <system/UserException.h>

namespace scidb {

class ParsingContext;

#define CONV_TO_USER_QUERY_EXCEPTION(_ex, _parsing_context) \
    *UserQueryException::create(_ex, _parsing_context)

#define USER_QUERY_EXCEPTION_SPTR(short_error_code, long_error_code, parsing_context) \
    std::make_shared<scidb::UserQueryException>(                        \
        REL_FILE, __FUNCTION__, __LINE__, CORE_ERRORS_NAMESPACE,        \
        int(short_error_code), int(long_error_code),                    \
        #short_error_code, #long_error_code, parsing_context)

#define PLUGIN_USER_QUERY_EXCEPTION(error_namespace, short_error_code, long_error_code, parsing_context)\
    scidb::UserQueryException(REL_FILE, __FUNCTION__, __LINE__, error_namespace, short_error_code,\
        long_error_code, #short_error_code, #long_error_code, parsing_context)

/**
 * Exceptions caused by wrong queries
 * @see scidb::Exception
 */
class PLUGIN_EXPORT UserQueryException : public UserException
{
    using super = UserException;

public:
    typedef std::shared_ptr<UserQueryException> Pointer;

    UserQueryException(const char* file, const char* function, int32_t line,
        const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
        const char* stringified_short_error_code, const char* stringified_long_error_code,
        const std::shared_ptr<ParsingContext>& parsingContext, const QueryID& query_id = INVALID_QUERY_ID);

    static std::shared_ptr<UserQueryException> create(Exception const& ex,
                                                      std::shared_ptr<ParsingContext> ctx);

    std::shared_ptr<ParsingContext> getParsingContext() const;

    COMMON_EXCEPTION_METHODS(UserQueryException)

protected:
    std::string format() override;

private:
    std::shared_ptr<ParsingContext> _parsingContext;
};

COMMON_EXCEPTION_FREE_FUNCTIONS(UserQueryException)

}  // namespace scidb

#endif
