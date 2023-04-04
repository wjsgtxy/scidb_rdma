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

/**
 * @file Exceptions.h
 *
 * @author: Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Exceptions which thrown inside SciDB
 */

#include <system/Exceptions.h>

#ifndef SCIDB_CLIENT
#  include <storage/StorageMgr.h>
#endif
#include <system/ErrorsLibrary.h>
#include <system/Utils.h>
#include <util/Utility.h>

#include <iostream>
#include <sstream>

using namespace std;

namespace scidb
{

/*
 * Exception
 */
Exception::Exception(const char* file, const char* function, int32_t line,
                     const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
                     const char* stringified_short_error_code, const char* stringified_long_error_code,
                     const QueryID& query_id)
    : _file(file)
    , _function(function)
    , _line(line)
    , _errors_namespace(errors_namespace)
    , _short_error_code(short_error_code)
    , _long_error_code(long_error_code)
    , _stringified_short_error_code(stringified_short_error_code)
    , _stringified_long_error_code(stringified_long_error_code)
    , _query_id(query_id)
#ifdef SCIDB_CLIENT
    , _inst_id(CLIENT_INSTANCE)
#else
    , _inst_id(StorageMgr::getInstance()->getInstanceId())
#endif
{ }

void Exception::setInternalState(string const& what,
                                 string const& formatted,
                                 InstanceID origin)
{
    // For a target Exception object derived from an mtError message
    // or from a source Exception object, this sets the internal data
    // members that SHOULD NOT BE RECOMPUTED.
    //
    // For example, the target instance's boost::format object is
    // still in its initial state and likely expecting some
    // operator<<() calls.  These won't happen on the receiving
    // instance!  See getErrorMessage() above.
    //
    // Similarly, the _inst_id is only meaningful if it reflects where
    // the source object was constructed.

    _what_str = what;
    _formatted_msg = formatted;
    _inst_id = origin;
}

boost::format& Exception::getMessageFormatter() const
{
    SCIDB_ASSERT(_formatted_msg.empty());
    if (_formatter.size() == 0) {
        _formatter = boost::format(ErrorsLibrary::getInstance()->getLongErrorMessage(_errors_namespace,
                                                                                     _long_error_code));
    }
    assert(_formatter.size() != 0);
    return _formatter;
}

const std::string& Exception::getWhatStr() const
{
    if (_what_str.empty()) {
        try {
            Exception* self = const_cast<Exception*>(this);
            self->_what_str = self->format();
        } catch (...) {}
    }
    return _what_str;
}

std::string Exception::getErrorMessage() const
{
    if (!_formatted_msg.empty()) {
        return _formatted_msg;
    }
    try
    {
        _formatted_msg = boost::str(getMessageFormatter());
        return _formatted_msg;
    }
    catch(std::exception& e) {
        string s("!!!Cannot format error message: ");
        s += e.what();
        s += "Check argument count for ";
        s += getErrorId();
        s += "!!!";
        assert(false);
        return s;
    }
    catch(...)
    {
        string s("!!!Cannot format error message. Check argument count for ");
        s += getErrorId();
        s += "!!!";
        assert(false);
        return s;
    }
}

string Exception::getClassName() const
{
    string s(demangle_cpp(typeid(*this).name()));
    string::size_type p = s.find_last_of(":");
    return p == string::npos ? s : s.substr(p + 1);
}

string Exception::format()
{
    // The test harness scrapes away 'line: .*$' before running diff,
    // so changes in either _line or _inst_id won't require
    // re-recording captured test output.

    stringstream ss;
    ss << getClassName()
       << " in file: " << _file
       << " function: " << _function
       << " line: " << _line;
    if (isValidPhysicalInstance(_inst_id)) {
        ss << " instance: " << Iid(_inst_id);
    }
    ss << "\nError id: " << _errors_namespace
       << "::" << _stringified_short_error_code
       << "::" << _stringified_long_error_code
        // ----------------
       << "\nError description: "
       << ErrorsLibrary::getInstance()->getShortErrorMessage(_short_error_code)
       << ". " << getErrorMessage()
       << '.'; // SHOULD NOT dictate end punctuation here, but to
               // avoid re-recording a lot of tests, keep it for now.

    if (_query_id.isValid()) {
        ss << "\nFailed query id: " << _query_id ;
    }

    return ss.str();
}

const string& Exception::getErrorsNamespace() const
{
    return _errors_namespace;
}

const char* Exception::what() const throw()
{
    return getWhatStr().c_str();
}

const std::string& Exception::getFile() const
{
	return _file;
}

const std::string& Exception::getFunction() const
{
	return _function;
}

int32_t Exception::getLine() const
{
	return _line;
}

int32_t Exception::getShortErrorCode() const
{
	return _short_error_code;
}

int32_t Exception::getLongErrorCode() const
{
    return _long_error_code;
}

const std::string& Exception::getStringifiedShortErrorCode() const
{
    return _stringified_short_error_code;
}

const std::string& Exception::getStringifiedLongErrorCode() const
{
    return _stringified_long_error_code;
}

const string Exception::getErrorId() const
{
    stringstream ss;
    ss << _errors_namespace << "::" << _short_error_code << "::" << _long_error_code;
    return ss.str();
}

const string Exception::getStringifiedErrorId() const
{
    return _errors_namespace + "::" + _stringified_short_error_code + "::" + _stringified_long_error_code;
}

InstanceID Exception::getInstanceId() const
{
    return _inst_id;
}

const QueryID& Exception::getQueryId() const
{
    return _query_id;
}

void Exception::setQueryId(const QueryID& queryId)
{
    _query_id = queryId;
}

/*
 * SystemException
 */
SystemException::SystemException(const char* file, const char* function, int32_t line,
                                 const char* errors_namespace, int32_t short_error_code, int32_t long_error_code,
                                 const char* stringified_short_error_code, const char* stringified_long_error_code,
                                 const QueryID& query_id):
Exception(file, function, line, errors_namespace, short_error_code, long_error_code,
          stringified_short_error_code, stringified_long_error_code, query_id)
{
}

}
