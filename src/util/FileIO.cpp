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

#include <util/FileIO.h>

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <dirent.h>
#include <string.h>

#include <boost/scoped_array.hpp>

#include <linux/falloc.h>

#include <util/PerfTime.h>
#include <log4cxx/logger.h>
#include <system/UserException.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <util/OnScopeExit.h>

using namespace std;

namespace scidb
{
    // Logger for operator. static to prevent visibility of variable outside of file
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.fileio"));

    // Number of retries on EAGAIN error. We dont use O_NONBLOCK on files,
    // but we have seen EAGAIN error codes returned. Since the EAGAINs are unexpected
    // and we sleep between the retries, the numbers are not high.
    const size_t MAX_READ_RETRIES  = 3;
    const size_t MAX_WRITE_RETRIES = 10;
    // Number of retries on EINTR error from IO syscalls.
    // Generally, there should not be reason to give up on EINTR, but to avoid infinite loops we do.
    // we retry with no back off, so the value is somewhat high.
    const size_t MAX_EINTR_RETRIES = 1000;

    /* Stat a file
     */
    int
    File::stat(char const* filePath, struct stat* st, bool raise)
    {
        assert(filePath);
        LOG4CXX_TRACE(logger, "File::stat: " << filePath);
        int err = 0;
        int rc = ::stat(filePath, st);
        if (rc < 0)
        {
            err = errno;
            if (raise) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                       << "stat" << rc << err << ::strerror(err) << filePath);
            }
        }
        return err;
    }

    /* Remove (unlink) a file
     */
    int
    File::remove(char const* filePath, bool raise)
    {
        ScopedWaitTimer timer(PTW_SWT_FS_WR);

        assert(filePath);
        LOG4CXX_TRACE(logger, "File::remove: " << filePath);
        int err = 0;
        int rc = ::unlink(filePath);
        if (rc < 0) {
            err = errno;
            if (raise) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                       << "unlink" << rc << err << ::strerror(err) << filePath);
            }
        }
        return err;
    }

    /* Close a directory
     */
    int
    File::closeDir(const char* dirName, DIR *dirp, bool raise)
    {
        ScopedWaitTimer timer(PTW_SWT_FS_RD);              // directories are not modified via DIR*

        int err = 0;
        int rc = ::closedir(dirp);
        if (rc!=0) {
            err = errno;
            LOG4CXX_ERROR(logger, "closedir("<<dirName<<") failed: " <<
                          ::strerror(errno) << " (" << errno << ')');
            if (raise) {
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                       << "closedir" << rc << err << ::strerror(err) << dirName);
            }
        }
        return err;
    }

    /* Callback used by readdir to accumulate dir entries
     */
    void
    File::readDirCb(std::list<std::string>& entries,
                    struct dirent* de)
    {
        entries.push_back(std::string(de->d_name));
    }

    /* Read the contents of a directory
     */
    void
    File::readDir(const char* dirName, std::list<std::string>& entries)
    {
        ScopedWaitTimer timer(PTW_SWT_FS_RD);

        LOG4CXX_TRACE(logger, "File::readDir: " << dirName);

        std::function<void(struct dirent * de)> cb = std::bind(&File::readDirCb,
                                                               entries,
                                                               std::placeholders::_1);
        File::processDir(dirName, cb);
    }

    /* Invoke a callback function for each element in a directory
     */
    void
    File::processDir(const char* dirName,
                     std::function<void (struct dirent* de)>& cb)
    {
        LOG4CXX_TRACE(logger, "File::processDir: " << dirName);

        DIR* dirp = ::opendir(dirName); // closedir

        if (dirp == NULL) {
            int err = errno;
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "opendir" << "NULL" << err << ::strerror(err)
                   << dirName);
        }

        scidb::OnScopeExit dirCloser([dirName, dirp] () {
                File::closeDir(dirName, dirp, false);
            });

        struct dirent entry;
        memset(&entry, 0, sizeof(entry));
        assert((pathconf(dirName, _PC_NAME_MAX) + 1) == sizeof(entry.d_name));

        while (true) {

            struct dirent *result(NULL);

            int rc = ::readdir_r(dirp, &entry, &result);
            if (rc != 0) {
                int err = errno;
                throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                                        SCIDB_LE_SYSCALL_ERROR)
                       << "readdir_r" << rc << err << ::strerror(err)
                       << dirName);
            }
            if (result == NULL) {
                // EOF
                return;
            }
            assert(result == &entry);

            cb(&entry);
        }
    }

    /* Create a directory
     */
    int
    File::createDir(const std::string& dirPath)
    {
        ScopedWaitTimer timer(PTW_SWT_FS_WR);

        assert(!dirPath.empty());
        int rc = ::mkdir(dirPath.c_str(), (S_IRUSR|S_IWUSR|S_IXUSR));
        if (rc==0) {
            return 0;
        }
        if (errno==EEXIST) {
            rc = ::chmod(dirPath.c_str(), (S_IRUSR|S_IWUSR|S_IXUSR));
            return rc ? errno : 0;
        }
        return errno;
    }

    /* Close file descriptor (restarting after signal interrupt if necessary)
     */
    int
    File::closeFd(int fd)
    {
        int rc=0;
        size_t eintrRetries=0;
        do {
            rc = ::close(fd);
        } while( (rc != 0) &&
                 (errno == EINTR) &&
                 (++eintrRetries < MAX_EINTR_RETRIES));
        // TODO: This appears to violate the warning in the close(2) man page.
        //             Note that the return value should be used only for
        //             diagnostics.  In particular close() should not be retried
        //             after an EINTR since this may cause a reused descriptor
        //             from another thread to be closed.
        return rc;

    }

    /* Open a file (restarting after signal interrupt if necessary)
     */
    int
    File::openFile(const std::string& fileName, int flags)
    {
        // three possible time categories
        // FS_RD, FS_WR_SYNC, FS_WR

        auto tc = PTW_SWT_FS_RD; // default is read
        if (flags & (O_WRONLY|O_RDWR)) {
            if( flags & O_SYNC) {
                tc = PTW_SWT_FS_WR_SYNC;
            } else {
                tc = PTW_SWT_FS_WR;
            }
        }
        ScopedWaitTimer timer(tc);

        int fd = -1;
        size_t eintrRetries=0;
        while (true) {
            fd = ::open(fileName.c_str(), flags,
                        (S_IRUSR|S_IWUSR));
            if (fd < 0) {
                if ((errno == EINTR) &&
                    (++eintrRetries < MAX_EINTR_RETRIES)) {
                    continue;
                }
                return -1;
            }
            break;
        }
        assert(fd>=0);
        return fd;
    }


    /* Write data to the file
     */
    void
    File::writeAll(const void* data, size_t size, uint64_t offs)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to write the data, retrying if we are interrupted by signals
         */

        // NOTE O_SYNC files have a separate time category
        auto tc = (_flags & O_SYNC) ?  PTW_SWT_FS_WR_SYNC : PTW_SWT_FS_WR ;
        ScopedWaitTimer timer(tc); // after FileMonitor
        const char* src = (const char*)data;
        size_t nRetries = 0;
        size_t eintrRetries = 0;
        while (size != 0) {
            ssize_t rc = ::pwrite(_fd, src, size, offs);
            if (rc <= 0) {
                if ((rc < 0) && (errno == EINTR) && (++eintrRetries < MAX_EINTR_RETRIES)) {
                    nRetries = 0;
                    continue;
                }
#ifdef NDEBUG
                if ((rc == 0 || errno == EAGAIN) && ++nRetries < MAX_WRITE_RETRIES)
#else
                if (rc == 0 || errno == EAGAIN)
#endif
                {
                    LOG4CXX_DEBUG(logger, "pwrite wrote nothing, path=" << _path
                                  <<" src=" << size_t(src)
                                  <<" size="<<size<<" offs="<<offs<<" rc="<<rc<<" errno="<<errno
                                  <<" retries="<<nRetries);
                    sleep(1);
                    eintrRetries=0;
                    continue;
                }
                LOG4CXX_DEBUG(logger, "pwrite failed, path=" << _path << " src=" << size_t(src)
                              <<" size="<<size<<" offs="<<offs<<" rc="<<rc<<" errno="<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PWRITE_ERROR)
                    << size << offs << _path << ::strerror(errno) << errno;
            } else {
                nRetries = 0;
                eintrRetries = 0;
            }
            src += rc;
            size -= rc;
            offs += rc;
        }
    }


    /* Write vector of data to the file (gather write)
     */
    void
    File::writeAllv(const struct iovec* iovs, int niovs, uint64_t offs)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to write the data, retrying if we are interrupted by signals
         */
        ssize_t totalSize = 0;
        ssize_t bytesWritten = 0;
        ssize_t rc = 0;

        /* Clone the iovecs in case we need to update the pointers
         */
	boost::scoped_array<struct iovec> myiovs(new struct iovec[niovs]);
        int myniovs = niovs;
        for (int i = 0; i < niovs; i++)
        {
            totalSize +=  iovs[i].iov_len;
            myiovs[i] = iovs[i];
        }

        ScopedWaitTimer timer(PTW_SWT_FS_WR);  // after FileMonitor
        size_t nRetries = 0;
        size_t eintrRetries = 0;
        while (totalSize != 0) {
            rc = ::pwritev(_fd, myiovs.get(), myniovs, offs);
            if (rc <= 0) {
                if ((rc < 0) && (errno == EINTR) && (++eintrRetries < MAX_EINTR_RETRIES))
                {
                    LOG4CXX_DEBUG(logger, "pwritev error EINTR retry, path=" << _path <<" size="
                                  <<totalSize<<" offs="<<offs<<" rc="<<rc<<" errno="<<errno
                                  <<" eintrretries="<<eintrRetries);
                    nRetries = 0;
                    continue;
                }
#ifdef NDEBUG
                if ((rc == 0 || errno == EAGAIN) && ++nRetries < MAX_WRITE_RETRIES)
#else
                if (rc == 0 || errno == EAGAIN)
#endif
                {
                    LOG4CXX_DEBUG(logger, "pwritev wrote nothing, path=" << _path <<" size="
                                  <<totalSize<<" offs="<<offs<<" rc="<<rc<<" errno="<<errno
                                  <<" retries="<<nRetries);
                    sleep(1);
                    eintrRetries=0;
                    continue;
                }
                LOG4CXX_DEBUG(logger, "pwritev failed, path=" << _path <<" size="<<totalSize
                              <<" offs="<<offs<<" rc="<<rc<<" errno="<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PWRITE_ERROR)
                    << totalSize << offs << _path << ::strerror(errno) << errno;
            } else {
                nRetries = 0;
                eintrRetries = 0;
            }
            totalSize -= rc;
            bytesWritten += rc;
            offs += rc;

            if (totalSize > 0)
            {
                /* retry request, picking up from where we left off
                 */
                int i = 0;
                int j = 0;
                ssize_t skip = bytesWritten;
                while ( skip > 0 && (ssize_t)iovs[i].iov_len < skip)
                {
                    skip -= (ssize_t)iovs[i++].iov_len;
                }
                assert(niovs > i);
                myniovs = niovs - i;
                myiovs[0].iov_base = (char*)iovs[i].iov_base + skip;
                myiovs[0].iov_len = iovs[i].iov_len - skip;
                while (++j < myniovs)
                {
                    myiovs[j] = iovs[++i];
                }
            }
        }
    }


    /* Read data from the file
     */
    void
    File::readAll(void* data, size_t size, uint64_t offs)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to read the data, retrying if we are interrupted by signals
         */
        ScopedWaitTimer timer(PTW_SWT_FS_RD); // after FileMonitor
        char* dst = (char*)data;
        size_t nRetries = 0;
        size_t eintrRetries = 0;
        while (size != 0) {
            ssize_t rc = ::pread(_fd, dst, size, offs);
            if (rc <= 0)
            {
                if ((rc < 0) && (errno == EINTR) && (++eintrRetries < MAX_EINTR_RETRIES)) {
                    nRetries = 0;
                    continue;
                }
#ifdef NDEBUG
                if (rc < 0 && errno == EAGAIN && ++nRetries < MAX_READ_RETRIES)
#else
                if (rc < 0 && errno == EAGAIN)
#endif
                {
                    LOG4CXX_DEBUG(logger, "pread received EAGAIN, path=" << _path << " dst=" << size_t(dst)
                                  <<" size="<<size<<" offs="<<offs<<" retries="<<nRetries);
                    eintrRetries = 0;
                    sleep(1);
                    continue;
                }
                LOG4CXX_DEBUG(logger, "pread failed path=" << _path << " dst=" << size_t(dst)
                              <<" size="<<size<<" offs="<<offs<<" rc="<<rc<<" errno="<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PREAD_ERROR)
                    << size << offs << _path << rc << ::strerror(errno) << errno;
            } else {
                nRetries = 0;
                eintrRetries = 0;
            }

            dst += rc;
            size -= rc;
            offs += rc;
        }
    }


    /* Read vector of data to the file (scatter read)
     */
    void
    File::readAllv(const struct iovec* iovs, int niovs, uint64_t offs)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to read the data, retrying if we are interrupted by signals
         */
        ssize_t totalSize = 0;
        ssize_t bytesRead = 0;
        ssize_t rc = 0;

        /* Clone the iovecs in case we need to update the pointers
         */
	boost::scoped_array<struct iovec> myiovs(new struct iovec[niovs]);
        int myniovs = niovs;
        for (int i = 0; i < niovs; i++)
        {
            totalSize +=  iovs[i].iov_len;
            myiovs[i] = iovs[i];
        }

        ScopedWaitTimer timer(PTW_SWT_FS_RD);
        size_t nRetries = 0;
        size_t eintrRetries = 0;
        while (totalSize != 0) {
            rc = ::preadv(_fd, myiovs.get(), myniovs, offs);
            if (rc <= 0) {
                if ((rc < 0) && (errno == EINTR) && (++eintrRetries < MAX_EINTR_RETRIES)) {
                    nRetries = 0;
                    continue;
                }
#ifdef NDEBUG
                if (rc < 0 && errno == EAGAIN && ++nRetries < MAX_READ_RETRIES)
#else
                if (rc < 0 && errno == EAGAIN)
#endif
                {
                    LOG4CXX_DEBUG(logger, "preadv received EAGAIN, path=" << _path <<" size="
                                  <<totalSize<<" offs="<<offs<<" retries="<<nRetries);
                    sleep(1);
                    eintrRetries=0;
                    continue;
                }
                LOG4CXX_DEBUG(logger, "preadv failed, path=" << _path <<" size="<<totalSize
                              <<" offs="<<offs<<" rc="<<rc<<" errno="<<errno);
                throw SYSTEM_EXCEPTION(SCIDB_SE_IO, SCIDB_LE_PREAD_ERROR)
                    << totalSize << offs << _path << rc << ::strerror(errno) << errno;
            } else {
                nRetries = 0;
                eintrRetries = 0;
            }

            totalSize -= rc;
            bytesRead += rc;
            offs += rc;

            if (totalSize > 0)
            {
                /* retry request, picking up from where we left off
                 */
                int i = 0;
                int j = 0;
                ssize_t skip = bytesRead;
                while ( skip > 0 && (ssize_t)iovs[i].iov_len < skip)
                {
                    skip -= (ssize_t)iovs[i++].iov_len;
                }
                assert(niovs > i);
                myniovs = niovs - i;
                myiovs[0].iov_base = (char*)iovs[i].iov_base + skip;
                myiovs[0].iov_len = iovs[i].iov_len - skip;
                while (++j < myniovs)
                {
                    myiovs[j] = iovs[++i];
                }
            }
        }
    }


    /* Try to read from the file -- retry on EINTR and EAGAIN
     */
    size_t
    File::read(void* data, size_t size, uint64_t offs)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to read the data, retrying if we are interrupted by signals
         */
        ScopedWaitTimer timer(PTW_SWT_FS_RD);

        ssize_t rc = 0;
        char* dst = (char*)data;
        size_t nRetries = 0;
        size_t eintrRetries = 0;
        while (size != 0) {
            rc = ::pread(_fd, dst, size, offs);
            if (rc <= 0)
            {
                if ((rc < 0) && (errno == EINTR) && (++eintrRetries < MAX_EINTR_RETRIES)) {
                    nRetries = 0;
                    continue;
                }
#ifdef NDEBUG
                if (rc < 0 && errno == EAGAIN && ++nRetries < MAX_READ_RETRIES)
#else
                if (rc < 0 && errno == EAGAIN)
#endif
                {
                    LOG4CXX_DEBUG(logger, "pread received EAGAIN, path=" << _path << " dst="
                                  << size_t(dst) <<" size="<<size<<" offs="<<offs
                                  <<" retries="<<nRetries);
                    eintrRetries = 0;
                    sleep(1);
                    continue;
                }
                break;
            } else {
                nRetries = 0;
                eintrRetries = 0;
            }

            dst += rc;
            size -= rc;
            offs += rc;
        }
        return rc;
    }

    /* Fsync a file (restarting after signal interrupt if necessary)
     */
    int
    File::fsync()
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to fsync the file
         */
        ScopedWaitTimer timer(PTW_SWT_FS_FL); // after FileMonitor

        int rc = 0;

        do
        {
            rc = ::fsync(_fd);
        } while (rc != 0 && errno == EINTR);
        return rc;
    }

    /* Fdatasync a file (restarting after signal interrupt if necessary)
     */
    int
    File::fdatasync()
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to fdatasync the file
         */
        ScopedWaitTimer timer(PTW_SWT_FS_FL); // after FileMonitor
        int rc = 0;

        do
        {
            rc = ::fdatasync(_fd);
        } while (rc != 0 && errno == EINTR);
        return rc;
    }


    /* ftruncate a file (restarting after signal interrupt in necessary)
     */
    int
    File::ftruncate(off_t len)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to ftruncate the file
         */
       ScopedWaitTimer timer(PTW_SWT_FS_WR); // after FileMonitor
        int rc = 0;

        do
        {
            rc = ::ftruncate(_fd, len);
        } while (rc != 0 && errno == EINTR);
        return rc;
    }

    int File::fallocPunchHole(off_t offset, off_t len)
    {
        // for debugging
        // TEMPORARY: unsure if offset + len > st_size can occur, so assert it does not
        const bool punchSizeCheck = false;
        if(isDebug() && punchSizeCheck) { // avoid cost of fstat() for production builds
            struct stat statVals;
            fstat(&statVals); // for st.st_size
            SCIDB_ASSERT(offset + len <= statVals.st_size);
            // if this assert occurs, save repro steps and inform jmcq
        }

        // for debugging
        // normally-disabled code that can simulate (offset,len) crossing eof
        const bool debugSimulateCrossingEof = false;   // can be enabled in dev build to validate
        SCIDB_ASSERT(!debugSimulateCrossingEof);       // but never check in enabled
        if(isDebug() && debugSimulateCrossingEof) {
            len = File::_fallocPunchHoleSimulateLenCrossingEof(offset, len);
        }

        // Verify that the fd is open
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        // Try to fallocate the file
        ScopedWaitTimer timer(PTW_SWT_FS_PH); // after FileMonitor
        int rc = 0;
        do {
            rc = ::fallocate(_fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, offset, len);
            //  fallocate(2) man page says that ...PUNCH_HOLE must be ored with ...KEEP_SIZE
            //  and this has been validated.  Without ...KEEP_SIZE, fails with EINVAL.
        } while (rc != 0 && errno == EINTR);

        // for debugging
        // normally-disabled code to detect behavior of (offset,len) crossing eof
        if(debugSimulateCrossingEof) {
            _fallocPunchHoleReportOnSimulatedCrossing(rc);
        }

        return rc;
    }

    // normally-disabled "punch past eof inducing" code ...
    // i.e. validate FALLOC_FL_KEEP_SIZE allows off+len to exceed
    // stat.st_size without penalty.
    //
    // Combine this with a test that recovers all cells in the last
    // chunk of an array. This code has been used to demonstrate
    // that that offset+len == st_size can be made to occur,
    // and that even with len artificially increased, the hole
    // is punched in the file up to eof, with no error returned.
    off_t File::_fallocPunchHoleSimulateLenCrossingEof(off_t offset, off_t len)
    {
        struct stat st;
        fstat(&st); // st.st_size;
        LOG4CXX_WARN(logger, "File::fallocPunchHole():"
                             << " offset: " << offset
                             << " + len: " << len
                             << " = " << offset+len
                             << " vs st_size: " << st.st_size);
        if(offset + len == st.st_size) {
            LOG4CXX_WARN(logger, "File::fallocPunchHole(): @@@@@@@@@@@@@@@@@@@@@@@@");
            LOG4CXX_WARN(logger, "File::fallocPunchHole(): changing old len: " << len);
            len += 4096;
            LOG4CXX_WARN(logger, "File::fallocPunchHole(): new: " << len);
        }

        return len;
    }

    // post fallocate() companion to _fallocPunchHoleSimulateLenCrossingEof()
    void File::_fallocPunchHoleReportOnSimulatedCrossing(int rc) {
        LOG4CXX_WARN(logger, "File::fallocPunchHole(): punch past eof exercised, rc: " << rc);
        if (rc) {
            LOG4CXX_ERROR(logger, "File::fallocPunchHole(): punch past eof error, rc: " << rc << " errno: " << errno);
        }
    }

    /* Set an advisory lock on the file (restarting after signal intr)
     */
    int
    File::fsetlock(struct flock* flc)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to set the lock on the file
         */
        int rc = 0;

        do
        {
            rc = ::fcntl(_fd, F_SETLK, flc);
        } while (rc != 0 && errno == EINTR);

        return rc;
    }

    /* Stat the file
     */
    int
    File::fstat(struct stat* st)
    {
        /* Verify that the fd is open
         */
        checkClosedByUser();
        FileMonitor fm(_fm, *this);

        assert(_fd >= 0);
        assert(_pin);

        /* Try to stat the file
         */
        int rc = 0;

        rc = ::fstat(_fd, st);

        return rc;
    }

    /* Mark file to be removed on last close
     */
    void
    File::removeOnClose()
    {
        _remove = true;
    }

    /* Close the file immediately -- file object cannot be used again
     */
    int
    File::close()
    {
        /* Note: no need for a monitor... if the file is already inactive
           (closed) we will just remove it from the FileManager...
         */
        int ret = 0;

        if (!_closed)
        {
            /* Take this entry out of the list (lru or closed)
             */
            _fm->forgetFd(*this);

            /* Close the file (if necessary)
             */
            if (_fd >= 0)
            {
                // three possible time categories
                // FS_RD, FS_WR_SYNC, FS_WR
                auto tc = PTW_SWT_FS_RD; // default is read
                if (_flags & (O_WRONLY|O_RDWR)) {
                    if( _flags & O_SYNC) {
                        tc = PTW_SWT_FS_WR_SYNC;
                    } else {
                        tc = PTW_SWT_FS_WR;
                    }
                }
                ScopedWaitTimer timer(tc);
                ret = File::closeFd(_fd);
            }
            if (!ret)
            {
                _closed = true;
            }
            else
            {
                LOG4CXX_ERROR(logger, "close("<< _path <<") failed: " <<
                              ::strerror(errno) << " (" << errno << ')');
            }
            if (_closed && _remove)
            {
                ret = ::unlink(_path.c_str());
                if (ret)
                {
                    LOG4CXX_ERROR(logger, "unlink("<< _path <<") failed: " <<
                                  ::strerror(errno) << " (" << errno << ')');
                }
            }
        }
        return ret;
    }

    /* Constructor
     */
    File::File(int fd, const std::string path, int flags, bool temp) :
        _fd(fd),
        _path(path),
        _flags(flags),
        _remove(temp),
        _closed(false),
        _pin(0)
    {
        _fm = FileManager::getInstance();
    }

    /* Destructor (closes the file descriptor)
     */
    File::~File()
    {
        close();
    }

    /* Check if the file had been explicitly close and throw if so
     */
    void
    File::checkClosedByUser()
    {
        if (_closed)
        {
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_SYSCALL_ERROR)
                   << "close called multiple times" << -1 << EBADFD << ::strerror(EBADFD) << _path);
        }
    }

    /* Stack allocated helper which ensures the fd is open and pinned upon
       construction
    */
    File::FileMonitor::FileMonitor(FileManager* fm, File& f) :
        _fm(fm),
        _f(f)
    {
        _fm->checkActive(_f);
    }

    /* Destructor ensures the file is unpinned
     */
    File::FileMonitor::~FileMonitor()
    {
        --(_f._pin);
    }

    /* FileManager implementation
     */

    /* Create a (temp) file object (file is removed on close)
       if filePath is specified, use it for the path of the file
     */
    File::FilePtr
    FileManager::createTemporary(std::string const& arrName, char const* filePath)
    {
        std::string dir;
        int fd;

        /* Try to create the temp file
         */
        if (filePath == NULL) {
            dir = getTempDir();
            if (dir.length() != 0 && dir[dir.length()-1] != '/') {
                dir += '/';
            }
            dir += "scidb_";
            dir += arrName;
            dir += ".XXXXXX";
            filePath = dir.c_str();
            fd = ::mkstemp((char*)filePath);
        } else {
            fd = ::open(filePath, O_RDWR|O_TRUNC|O_EXCL|O_CREAT|O_LARGEFILE, 0666);
        }
        if (fd < 0) {
            // For certain types of transient errors, we can throw a USER EXCEPTION
            if (errno == EMFILE)
            {
                throw USER_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_TOO_MANY_OPEN_FILES);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE)
                << filePath << ::strerror(errno) << errno;
        }

        /* Create the file object and put it on the lru
         */
        File::FilePtr ret(new File(fd, filePath, O_RDWR|O_LARGEFILE, true));
        addFd(*ret);

        return ret;
    }


    /* Open a file (restarting after signal interrupt if necessary)
       Returns shared pointer to file object or NULL on error
     */
    File::FilePtr
    FileManager::openFileObj(const std::string& fileName, int flags)
    {
        int fd = -1;
        File::FilePtr ret;

        /* Try to open the file
         */
        fd = File::openFile(fileName, flags);

        /* Create the file object and put it on the lru
         */
        if (fd >= 0)
        {
            flags = flags & (~O_CREAT) & (~O_EXCL) & (~O_TRUNC);
            ret.reset(new File(fd, fileName, flags, false));
            addFd(*ret);
        }

        return ret;
    }

    /* Constructor -- need to ensure that everything in the temp dir
     * is wiped out (in case we crashed previously)
     */
    FileManager::FileManager()
        : _maxLru(1)
    {
        LOG4CXX_TRACE(logger, "FileManager::FileManager");

        /* Try to open the temp dir
         */
        std::string dir = getTempDir();
        if (dir.length() != 0 && dir[dir.length()-1] != '/') {
            dir += '/';
        }

        DIR* dirp = ::opendir(dir.c_str());

        if (dirp == NULL)
        {
            LOG4CXX_TRACE(logger, "FileManager::FileManager: failed to open tmp dir, creating it");
            int rc = ::mkdir(dir.c_str(), (S_IRUSR|S_IWUSR|S_IXUSR));
            if (rc != 0)
            {
                LOG4CXX_ERROR(logger, "FileManager::FileManager: failed to create tmp dir, error: "
                              << strerror(errno));
            }
            return;
        }

        scidb::OnScopeExit dirCloser([&dir, dirp] () {
                File::closeDir(dir.c_str(), dirp, false);
            });

        /* For each entry in the temp dir
         */
        struct dirent entry;
        struct dirent *result = NULL;
        while (::readdir_r(dirp, &entry, &result) == 0 && result)
        {
            assert(result == &entry);

            LOG4CXX_TRACE(logger, "FileManager::FileManager: found entry " << entry.d_name);

            /* If it's a temp file, go ahead and try to remove it
             */
            if (strncmp(entry.d_name, "scidb_", 6) == 0)
            {
                LOG4CXX_TRACE(logger, "FileManager::FileManager: deleting entry " << entry.d_name);
                std::string fullpath = dir + "/" + entry.d_name;
                File::remove(fullpath.c_str(), false);
            }
        }

        /* Check that the static state has been initialized
         */
#ifndef SCIDB_CLIENT
        Config *cfg = Config::getInstance();
        assert(cfg);

        int configuredMaxLru = cfg->getOption<int>(CONFIG_MAX_OPEN_FDS);
        assert(configuredMaxLru > 0);

        /* If the max LRU size read from the configuration is
         * zero, then set it to one and report an error.
         */
        if (configuredMaxLru <= 0) {
            ostringstream oss;
            oss << "max-open-fds set to invalid value of " << configuredMaxLru;
            configuredMaxLru = 1;
            oss << ", using " << configuredMaxLru << " instead" << endl;
            LOG4CXX_ERROR(logger, oss.str().c_str());
        }

        _maxLru = static_cast<uint32_t>(configuredMaxLru);
#endif
    }

    /* Add a new open entry to the lru
     */
    void
    FileManager::addFd(File& file)
    {
        ScopedMutexLock scm(_fileLock, PTW_SML_FILE_MANAGER);

        checkLimit();

        /* Add to the LRU
         */
        _lru.push_front(&file);
        file._listPos = _lru.begin();
    }


    /* Remove an open entry from the lru or closed list
     */
    void
    FileManager::forgetFd(File& file)
    {
        ScopedMutexLock scm(_fileLock, PTW_SML_FILE_MANAGER);

        if (file._fd >= 0)
        {
            _lru.erase(file._listPos);
        }
        else
        {
            _closed.erase(file._listPos);
        }
    }


    /* Check if the entry is open and on the lru list---
       if not re-open it
       @throws SystemException if the lru limit is reached and the whole
               lru list is pinned.
     */
    void
    FileManager::checkActive(File& file)
    {
        ScopedMutexLock scm(_fileLock, PTW_SML_FILE_MANAGER);
        assert(*(file._listPos) == &file);

        /* Check if the entry is already open, if so, update the lru
           and mark it pinned
         */
        if (file._fd >= 0)
        {
            _lru.erase(file._listPos);
            _lru.push_front(&file);
            file._listPos = _lru.begin();
            file._pin++;
            return;
        }

        /* Make sure there is room in the lru
         */
        checkLimit();

        /* Try to open the file using the saved flags
         */
        file._fd = File::openFile(file._path, file._flags);

        /* Remove from the closed list and add to the lru
         */
        file._pin++;
        _closed.erase(file._listPos);
        _lru.push_front(&file);
        file._listPos = _lru.begin();
    }


    /* Check if we have reached the limit of the lru list---
       if so close the lru element
       @pre _fileLock is locked
     */
    void
    FileManager::checkLimit()
    {
        if (_lru.size() >= _maxLru)
        {
            if (_lru.back()->_pin > 0)
            {
                throw USER_EXCEPTION(SCIDB_SE_INTERNAL,
                                     SCIDB_LE_TOO_MANY_OPEN_FILES);
            }

            /* Get the victim from the lru, close it and add it
               to the closed list.
             */
            File* victim = _lru.back();

            _lru.pop_back();
            File::closeFd(victim->_fd);
            victim->_fd = -1;
            _closed.push_front(victim);
            victim->_listPos = _closed.begin();
        }
    }

    /* REturn the full path of the temp directory
     */
    std::string
    FileManager::getTempDir()
    {
        string storageConfigPath;
#ifndef SCIDB_CLIENT
        storageConfigPath = Config::getInstance()->getOption<string>(CONFIG_STORAGE);
#endif
        std::string storageConfigDir = getDir(storageConfigPath);
        std::string tmpDir = storageConfigDir += "/tmp";
        return tmpDir;
    }


//
// begin stdio wrappers
// NOTE: these add wait-timing to stdio functions while still matching
//       the call signatures as they were used in the import/export
//       code.
//

FILE* fopen(const char *path, const char *mode)
{
    auto tc = PTW_SWT_FS_RD; // default is read
    if (mode[0] == 'w' || mode[0] == 'a') {
        tc = PTW_SWT_FS_WR;
    }
    ScopedWaitTimer timer(tc);
    return ::fopen(path, mode);
}

FILE* fdopen(int fd, const char *mode)
{
    auto tc = PTW_SWT_FS_RD; // default is read
    if (mode[0] == 'w' || mode[0] == 'a') {
        tc = PTW_SWT_FS_WR;
    }
    ScopedWaitTimer timer(tc);
    return ::fdopen(fd, mode);
}

FILE* freopen(const char *path, const char *mode, FILE *stream)
{
    auto tc = PTW_SWT_FS_RD; // default is read
    if (mode[0] == 'w' || mode[0] == 'a') {
        tc = PTW_SWT_FS_WR;
    }
    ScopedWaitTimer timer(tc);
    return ::freopen(path, mode, stream);
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    ASSERT_EXCEPTION_FALSE("should not be called, use scidb::fread_unlocked()");
}

size_t fread_unlocked(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    ScopedWaitTimer timer(PTW_SWT_FS_RD);
    return ::fread_unlocked(ptr, size, nmemb, stream);
}

size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    ScopedWaitTimer timer(PTW_SWT_FS_WR);
    return ::fwrite(ptr, size, nmemb, stream);
}

size_t fwrite_unlocked(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    ScopedWaitTimer timer(PTW_SWT_FS_WR);
    return ::fwrite_unlocked(ptr, size, nmemb, stream);
}

int fprintf(FILE* stream, const char* format, ...)
{
    ScopedWaitTimer timer(PTW_SWT_FS_WR);
    va_list args;
    va_start(args, format);
    auto result = ::vfprintf(stream, format, args);
    va_end(args);
    return result;
}

int fflush(FILE *stream)
{
    ScopedWaitTimer timer(PTW_SWT_FS_WR);
    return ::fflush(stream);
}

int fclose(FILE *fp)
{
    auto tc = PTW_SWT_FS_WR; // assume the file was opened for write

    // attempt to correct the assumption
    // but if there is an error, go on anyway
    // as it is far more important to close files to avoid resource leaks
    // than to make the timing breakdown be perfect
    auto flags = ::fcntl(::fileno(fp), F_GETFL);
    if (flags != -1 && ((flags & O_RDONLY))) {
        tc = PTW_SWT_FS_RD;    // actually, it was opened for read
    }
    ScopedWaitTimer timer(tc);
    return ::fclose(fp);
}

} // namespace scidb
