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
 * @file PathUtils.cpp
 * @brief Miscellaneous file pathname utilities.
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <util/PathUtils.h>

#include <query/Query.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>
#include <system/Config.h>
#include <util/PerfTime.h>

#include <boost/algorithm/string.hpp>
#define BOOST_FILESYSTEM_NO_DEPRECATED 1
#include <boost/filesystem.hpp>
#include <log4cxx/logger.h>
#include <sys/stat.h>

using namespace std;
using namespace scidb;
namespace bfs = boost::filesystem;
using NsComm = namespaces::Communicator;

namespace {

log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.util"));

char const * const USERS_SUBDIR = "users";

bool initialized = false;
vector<bfs::path> ioPathsList;
bfs::path instDataDir;

bool isIoPathChild(bfs::path const& filename, bfs::path* ancestor = nullptr)
{
    if (filename == "/dev/null") {
        if (ancestor) {
            *ancestor = "/dev/null";
        }
        return true;
    }
    for (auto const& p : ioPathsList) {
        if (scidb::path::isAncestor(p, filename)) {
            if (ancestor) {
                *ancestor = p;
            }
            return true;
        }
    }
    return false;
}

} // anonymous namespace

namespace scidb { namespace path {

Mutex ScopedUmask::s_mutex;

void initialize()
{
    // Just being paranoid here.  This routine is called exactly once
    // while the instance is still single-threaded.
    if (initialized) {
        LOG4CXX_DEBUG(logger, "PathUtils module already initialized");
        return;
    }
    initialized = true;

    // The instance data directory is the current directory.  Get it
    // and figure out a few derived values.
    char buf[PATH_MAX];
    char* cp = ::getcwd(buf, PATH_MAX);
    ASSERT_EXCEPTION(cp, "Cannot get current working directory?!");
    instDataDir = buf;
    bfs::path basePath(instDataDir.parent_path().parent_path());
    string instId = instDataDir.filename().native();
    string srvrId = instDataDir.parent_path().filename().native();
    LOG4CXX_DEBUG(logger, "Base path " << basePath
                  << ", server " << srvrId << ", instance " << instId);

    // Get and parse io-paths-list from the configuration.
    Config& config = *Config::getInstance();
    vector<string> dirs;
    boost::split(dirs, config.getOption<string>(CONFIG_IO_PATHS_LIST),
                 boost::is_any_of(":"));

    // The configured io-paths-list might contain symbolic links.
    // Canonicalize them now; later we'll be comparing them against
    // other canonical, symlink-free expanded paths.
    for (string& d : dirs) {
        boost::trim(d);
        bfs::path canon;
        bfs::path dpath(d);
        try {
            if (!dpath.is_absolute()) {
                LOG4CXX_WARN(logger, "Ignoring "
                             << config.getOptionName(CONFIG_IO_PATHS_LIST)
                             << " config.ini entry " << dpath << " (" << canon
                             << "): Not an absolute path");
                continue;
            }
            canon = bfs::canonical(dpath);
            if (!bfs::is_directory(canon)) {
                LOG4CXX_WARN(logger, "Ignoring "
                             << config.getOptionName(CONFIG_IO_PATHS_LIST)
                             << " config.ini entry " << dpath << " (" << canon
                             << "): Not a directory");
                continue;
            }
        }
        catch (bfs::filesystem_error& ex) {
            LOG4CXX_WARN(logger, "Ignoring "
                     << config.getOptionName(CONFIG_IO_PATHS_LIST)
                     << " config.ini entry " << dpath << ": " << ex.what());
            continue;
        }
        ioPathsList.push_back(canon); // ...*not* dpath!
    }

    // Calls to bfs::canonical() etc. might throw, and we need to translate
    // that exception into SciDBspeak.
    try {

        // What's the status of the USERS_SUBDIR?
        bfs::path usersDir(instDataDir);
        usersDir /= USERS_SUBDIR;
        boost::system::error_code ec;
        bfs::file_status fs = bfs::status(usersDir, ec);
        if (ec) {
            if (ec != boost::system::errc::no_such_file_or_directory) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_CANT_STAT_FILE)
                    << usersDir << ec.message() << ec.value();
            }

            // Perhaps it is a symlink with a missing target?
            fs = bfs::symlink_status(usersDir, ec);
            if (!ec) {
                // Symlink with missing target.  We could delete it
                // and press on, BUT that's probably NOT the intention
                // of the sysadmin who set up the symlink!
                stringstream ss;
                ss << usersDir << " symbolic link target "
                   << bfs::read_symlink(usersDir) << " is missing";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_UNKNOWN_ERROR)
                    << ss.str();
            }

            // It's missing altogether.  Fix that.
            makedirs(usersDir, 0700);
        }
        else if (fs.type() != bfs::directory_file) {
            stringstream ss;
            ss << "Problem with " << usersDir << ": Not a directory";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }

        // The prospective USERS_SUBDIR cannot be covered by the
        // io-paths-list, otherwise anyone can still read or clobber
        // anything!
        bfs::path canonUsersDir(bfs::canonical(usersDir));
        bfs::path parent;
        if (isIoPathChild(canonUsersDir, &parent)) {
            stringstream ss;
            ss << usersDir << " is invalid because directory " << parent
               << " on the " << config.getOptionName(CONFIG_IO_PATHS_LIST)
               << " covers it, making user subdirectories writeable by anyone";
            throw SYSTEM_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }

        // Establish ownership of USERS_SUBDIR and complain if there's
        // a conflict.  This is especially necessary if the userDir is
        // a symbolic link set up by a sysadmin.
        string const OWNER_PREFIX(".scidb_");
        string paint(OWNER_PREFIX);
        paint += SystemCatalog::getInstance()->getClusterUuid();
        paint += "_s" + srvrId + "_i" + instId;
        bfs::path paintFile = usersDir / paint;
        bfs::copy_file("/dev/null", paintFile, ec);
        if (ec && ec != boost::system::errc::file_exists) {
            stringstream ss;
            ss << "Cannot create " << paintFile << ": " << ec.message();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }
        for (auto pos = bfs::directory_iterator(usersDir);
             pos != bfs::directory_iterator();
             ++pos)
        {
            string const entry = pos->path().filename().native();
            if (entry.find(OWNER_PREFIX) == 0 && entry != paint) {
                bfs::remove(paintFile);
                stringstream ss;
                ss << usersDir << ": Directory already in use by " << entry;
                throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_UNKNOWN_ERROR)
                    << ss.str();
            }
        }
    }
    catch (bfs::filesystem_error& fe) {
        stringstream ss;
        ss << "Error during scidb::path module initialization: " << fe.what();
        throw SYSTEM_EXCEPTION(SCIDB_SE_INITIALIZATION, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
}


void makedirs(string const& path, int mode)
{
    bfs::path p(path);
    makedirs(p, mode);
}

int makedirs(string const& path, int mode, nothrow_t const& noThrow)
{
    bfs::path p(path);
    return makedirs(p, mode, noThrow);
}

void makedirs(bfs::path const& path, int mode)
{
    int rc = makedirs(path, mode, nothrow_t());
    if (rc) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_CANT_CREATE_DIRECTORY)
            << path.native() << ::strerror(rc);
    }
}

int makedirs(bfs::path const& path, int mode, nothrow_t const&)
{
    ScopedWaitTimer timer(PTW_SWT_FS_WR);
    ScopedUmask m(mode ^ 0777);
    boost::system::error_code ec;
    bfs::create_directories(path, ec);
    return ec.value();
}


bool isAncestor(bfs::path const& left, bfs::path const& right)
{
    bfs::path::iterator lpos = left.begin();
    bfs::path::iterator rpos = right.begin();
    for (;; ++lpos, ++rpos) {
        if (rpos == right.end()) {
            // Right is shorter, left cannot be ancestor.  Or, if left
            // is also at end, a path cannot be its own ancestor, so
            // also false.
            return false;
        }
        if (lpos == left.end()) {
            // Left is shorter and we matched so far, so yes, left is
            // an ancestor of right.
            return true;
        }
        if (*lpos != *rpos) {
            return false;
        }
    }
    SCIDB_UNREACHABLE();
}


namespace {

// Canonicalize, but don't insist that the last (filename) component
// must exist.  Ordinary bfs::canonical() requires that the file
// actually exist, but we need to be more forgiving when we are trying
// to canonicalize an output file that hasn't yet been created.
//
bfs::path softCanonical(bfs::path const& input)
{
    boost::system::error_code ec;
    bfs::path result = bfs::canonical(input, ec);
    if (ec) {
        result = bfs::canonical(input.parent_path());  // may throw
        result /= input.filename();
    }
    return result;
}


string expandCommon(string const& filename, scidb::Query const& query, bool writing)
{
    SCIDB_ASSERT(initialized);

    // Filename expansion depends on who you are.  Dig into the Query to
    // find the logged-in user's UserDesc.
    ASSERT_EXCEPTION(!filename.empty(), "Empty filename not allowed");
    std::shared_ptr<const Session> sess = query.getSession();
    if (!sess) {
        if (query.isFake()) {
            // User is running show() or _explain_physical() or the
            // like, so there's no associated session.  No need to
            // modify the filename.
            return filename;
        }
        ASSERT_EXCEPTION_FALSE("Query has no authenticated session");
    }
    UserDesc const& user = sess->getUser();
    ASSERT_EXCEPTION(user.getId() != rbac::NOBODY, "Unresolved username");

    bfs::path fname(filename);  // Enter the bfs world ...

    // Administrator can use whatever path they want without modification.
    if (user.getId() == rbac::DBA_ID) {
        if (writing && !fname.parent_path().empty()) {
            makedirs(fname.parent_path(), 0700);
        }
        return filename;
    }

    // For non-administrators, absolute filenames must evaluate to
    // something covered by the io-paths-list.  If not, then the user
    // has to belong to the admin group (with same powers as DBA).
    //
    if (fname.is_absolute()) {
        bfs::path canon(writing ? softCanonical(fname) : bfs::canonical(fname));
        if (isIoPathChild(canon)) {
            // Anyone can write/read filenames covered by the io-paths-list.
            return filename;
        }

        // Only hope now is to belong to the admin group.
        rbac::RightsMap rights;
        rights.upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
        NsComm::checkAccess(sess.get(), &rights); // throws unless admin member
        return filename;
    }

    // Relative filename.  What we do with it depends on whether this
    // user is in the admin group.
    try {
        rbac::RightsMap rights;
        rights.upsert(rbac::ET_DB, "", rbac::P_DB_ADMIN);
        NsComm::checkAccess(sess.get(), &rights); // throws unless admin member

        // Admin group user.  Create directories if needed and use the
        // relative filename as-is.
        if (writing && !fname.parent_path().empty()) {
            makedirs(fname.parent_path(), 0700);
        }
        return filename;
    }
    catch (Exception& ex) {
        if (logger->isTraceEnabled()) {
            LOG4CXX_TRACE(logger, __FUNCTION__ << ": " << user.getName()
                          << " is not an administrator: " << ex.what());
        }
    }

    // Ordinary user.  Simply cannot use "..", end of story.
    for (auto const& component : fname) {
        if (component == "..") {
            stringstream ss;
            ss << "Illegal path component \"..\" in " << fname;
            throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }
    }

    // Build the expanded relative filename.
    bfs::path newName(USERS_SUBDIR);
    newName /= user.getName();
    if (!bfs::exists(newName)) {
        // Create per-user directory on the fly, for both reads and writes.
        makedirs(newName, 0700);
    }
    newName /= fname;
    if (writing && !bfs::exists(newName.parent_path())) {
        makedirs(newName.parent_path(), 0700);
    }

    LOG4CXX_DEBUG(logger, __FUNCTION__
                  << ": Expanded " << filename << " to " << newName);

    return newName.native();
}

} // anonymous namespace


string expandForSave(string const& path, Query const& query)
{
    try {
        return expandCommon(path, query, true);
    }
    catch (bfs::filesystem_error& fe) {
        stringstream ss;
        ss << "Error expanding output path \"" << path << "\": " << fe.what();
        throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
    SCIDB_UNREACHABLE();
}

string expandForRead(string const& path, Query const& query)
{
    try {
        return expandCommon(path, query, false);
    }
    catch (bfs::filesystem_error& fe) {
        stringstream ss;
        ss << "Error expanding input path \"" << path << "\": " << fe.what();
        throw USER_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
    SCIDB_UNREACHABLE();
}

} } // namespaces
