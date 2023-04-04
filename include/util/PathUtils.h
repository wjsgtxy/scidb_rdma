/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file PathUtils.h
 * @brief Miscellaneous file pathname utilities.
 *
 * @details These are in addition to boost::filesystem::path, which
 * you should look at first if what you need is not here yet.
 */

#ifndef PATH_UTILS_H
#define PATH_UTILS_H

#include <util/Mutex.h>

#include <fcntl.h>
#include <new>                  // for std::nothrow_t
#include <string>
#include <sys/stat.h>
#include <vector>

namespace boost { namespace filesystem { class path; } }

namespace scidb {

class Query;

namespace path {

/**
 * @brief Module initialization.
 *
 * @details Initializes internal data structures needed to implement
 * the expandForFoo() routines below.  Throws on bad configuration.
 */
void initialize();

/**
 * @brief Super-mkdir, create leaf directory and all intermediate ones.
 * @throw SYSTEM_EXCEPTION on error.
 */
void makedirs(std::string const& path, int mode);
void makedirs(boost::filesystem::path const& path, int mode);

/**
 * @brief Super-mkdir, create leaf directory and all intermediate ones.
 * @return errno on error
 */
int makedirs(std::string const& path, int mode, std::nothrow_t const&);
int makedirs(boost::filesystem::path const& path, int mode, std::nothrow_t const&);

/**
 * @brief Return true iff left is an ancestor of right.
 * @note Strict: a path is not its own ancestor.
 */
bool isAncestor(boost::filesystem::path const& left,
                boost::filesystem::path const& right);

/**
 * @brief Compute sanitized filename for save() and other output operators.
 *
 * @param filename the original filename
 * @param query query context containing session information
 *
 * @details Sanitize an output filename.  Non-administrative SciDB users
 * can only write to per-user subdirectories of the instance data
 * directory (with the exception of paths into directories in the
 * config.ini io-paths-list).  Disallow tricks with ".." or symbolic
 * links.
 *
 * @p Differs from expandForRead() in that the per-user directory and
 * any subdirectories are created if missing.
 *
 * @see https://paradigm4.atlassian.net/wiki/display/DEV/Limiting+File+System+Access+in+17.x
 *
 * @note This must be called on all instances (presumably by the
 *       physical operator) to ensure that per-user directories are
 *       created everywhere prior to parallel save-like operations.
 *       See PhysicalSave::execute().
 */
std::string expandForSave(std::string const& filename, Query const& query);

/**
 * @brief Compute sanitized filename for load() and other input operators.
 *
 * @param filename the original filename
 * @param query query context containing session information
 *
 * @details Sanitize an input filename.  Non-administrative SciDB users
 * can only read from per-user subdirectories of the instance data
 * directory (with the exception of paths into directories in the
 * config.ini io-paths-list).  Disallow tricks with ".." or symbolic
 * links.
 *
 * @see https://paradigm4.atlassian.net/wiki/display/DEV/Limiting+File+System+Access+in+17.x
 */
std::string expandForRead(std::string const& filename, Query const& query);

/**
 * @brief Temporarily set the file mode creation mask with umask(2).
 * @note Sets the mask for the whole process, hence the mutex.
 */
class ScopedUmask
{
    static Mutex s_mutex;
    mode_t _saved;

public:

    explicit ScopedUmask(mode_t mode)
    {
        s_mutex.lock(PTW_MUTEX);
        _saved = ::umask(mode);
    }

    ~ScopedUmask()
    {
        (void) ::umask(_saved);
        s_mutex.unlock();
    }
};

} } // namespaces

#endif /* ! PATH_UTILS_H */
