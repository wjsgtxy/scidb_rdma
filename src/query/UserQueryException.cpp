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

#include <query/UserQueryException.h>

#include <query/ParsingContext.h>
#include <system/ErrorsLibrary.h>

using std::string;
using std::stringstream;
using std::endl;

namespace scidb {
/*
 * UserQueryException
 */
UserQueryException::UserQueryException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    const std::shared_ptr<ParsingContext>& parsingContext, const QueryID& query_id):
        UserException(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id),
        _parsingContext(parsingContext)
{
}

/*
 *  "Carrots are divine
 *  You get a dozen for a dime
 *  It's magic!
 *  They fry, a song begins;
 *  They roast, and I hear violins
 *  It's magic!"
 *      -- Bugs Bunny
 */
void carrots(string &str, size_t line, size_t start, size_t end)
{
    const string tmp = str;
    str = "";
    const size_t len = ( end > start ) ? end - start : 1;
    const string carrots = string(start - 1, ' ') + string(len, '^');

    bool done = false;
    size_t lineCounter = 1;
    for (string::const_iterator it = tmp.begin(); it != tmp.end(); ++it)
    {
        str += *it;
        if (*it == '\n')
            ++lineCounter;
        if (lineCounter == line + 1)
        {
            const string tail = string(it, tmp.end());
            str += carrots + tail;//(tail != "" ? "\n" + tail : "");
            done = true;
            break;
        }
    }
    if (!done)
        str += '\n' + carrots;
}

string UserQueryException::format()
{
    stringstream ss;
    ss << super::format();

    if (_parsingContext) {
        string query(_parsingContext->getQueryString());
        carrots(query,
                _parsingContext->getLineStart(),
                _parsingContext->getColStart(),
                _parsingContext->getColEnd());
        ss << '\n' << query;
    }

    return ss.str();
}

std::shared_ptr<ParsingContext> UserQueryException::getParsingContext() const
{
    return _parsingContext;
}

std::shared_ptr<UserQueryException>
UserQueryException::create(Exception const& ex, std::shared_ptr<ParsingContext> ctx)
{
    auto result =
        std::make_shared<UserQueryException>(ex.getFile().c_str(),
                                             ex.getFunction().c_str(),
                                             ex.getLine(),
                                             ex.getErrorsNamespace().c_str(),
                                             ex.getShortErrorCode(),
                                             ex.getLongErrorCode(),
                                             ex.getStringifiedShortErrorCode().c_str(),
                                             ex.getStringifiedLongErrorCode().c_str(),
                                             ctx,
                                             ex.getQueryId());
    result->setInternalState(ex._what_str,
                             (ex._formatted_msg.empty() // Must "materialize" the _formatted_msg here,
                              ? ex.getErrorMessage()    // else the boost::format machinery in the new
                              : ex._formatted_msg),     // object will expect some operator<<() calls.
                             ex.getInstanceId());
    return result;
}

}  // namespace scidb
