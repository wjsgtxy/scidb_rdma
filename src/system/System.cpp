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

#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <util/FileIO.h>
#include <system/Utils.h>
#include <system/Exceptions.h>

#ifdef COVERAGE
extern "C" void __gcov_flush(void);
#endif

/****************************************************************************/
namespace scidb {
/****************************************************************************/

namespace arena {extern void onForkOfChild();}

pid_t fork()
{
    if (int e = pthread_atfork(NULL,NULL,&arena::onForkOfChild))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,SCIDB_LE_SYSCALL_ERROR)
            << "pthread_atfork" << e << errno << ::strerror(errno) << "";
    }

    return ::fork();
}

void exit(int status)
{
#ifdef COVERAGE
    __gcov_flush();
#endif
#if !defined(NDEBUG) && defined(CLEAN_EXIT)
    ::exit(status);
#endif
    ::_exit(status);
}

std::string getDir(const std::string& filePath)
{
    size_t found = filePath.find_last_of("/");

    if (found == std::string::npos)
    {
        return ".";
    }

    if (found == 0)
    {
        return "/";
    }

    return filePath.substr(0,found);
}

std::string getFile(const std::string& filePath)
{
    size_t found = filePath.find_last_of("/");

    if (found == std::string::npos)
    {
        return filePath;
    }
    ++found;
    assert(found<=filePath.length());
    return filePath.substr(found);
}

bool isFullyQualified(const std::string& filePath)
{
    return !filePath.empty() && filePath[0]=='/';
}

FILE* openMemoryStream(char const* ptr,size_t size)
{
    FILE* f = tmpfile();

    if (NULL == f)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_FAILED) << "tmpfile";
    }

    size_t rc = scidb::fwrite(ptr,1,size,f);

    if (rc != size)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,SCIDB_LE_OPERATION_FAILED) << "fwrite";
    }

    fseek(f,0,SEEK_SET);
    return f;
}

void bad_dynamic_cast(const std::type_info& b,const std::type_info& d)
{
    std::stringstream s;                                // Formats the message

    s << " safe_dynamic_cast: bad cast from "           // Insert message text
      << b.name()   << " to "                           // ...and source type
      << d.name();                                      // ...and target type

    ASSERT_EXCEPTION(false,s.str());                    // And report failure
}

void bad_static_cast(const std::type_info& to,const std::type_info& from)
{
    std::stringstream s;

    s << " safe_static_cast: bad cast from "
      << from.name()   << " to "
      << to.name();

    ASSERT_EXCEPTION(false,s.str());
}

/****************************************************************************/
}
/****************************************************************************/
