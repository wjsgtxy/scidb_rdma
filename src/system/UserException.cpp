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


#include <system/UserException.h>
#include <system/ErrorsLibrary.h>

using std::stringstream;
using std::endl;
namespace scidb {
/*
 * UserException
 */
UserException::UserException(const char* file, const char* function, int32_t line,
    const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
    const char* stringified_short_error_code, const char* stringified_long_error_code,
    const QueryID& query_id):
        Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
            stringified_short_error_code, stringified_long_error_code, query_id)
{
}

}  // namespace scidb
