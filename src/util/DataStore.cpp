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
 * @file DataStore.cpp
 * @brief Implementation of array data file
 * @author sfridella@paradigm4.com
 */

/* Implementation notes:

   DataStore file is divided into power-of-two sized chunks. Some important
   invariants:

   1) If the file is non-zero length, then there are always valid chunks at
      offset 0, and offset filesize/2.  We never have a file with a single
      chunk that spans the entire file.
 */

#include <util/DataStore.h>

#include <log4cxx/logger.h>
#include <util/Platform.h>
#include <util/FileIO.h>
#include <util/OnScopeExit.h>
#include <system/Config.h>
#include <system/UserException.h>
#include <errno.h>

#include <limits>

namespace scidb
{

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.smgr.datastore"));

/* ChunkHeader special values */
const size_t DataStore::DiskChunkHeader::usedValue = 0xfeedfacefeedface;
const size_t DataStore::DiskChunkHeader::freeValue = 0xdeadbeefdeadbeef;

/* Construct an flb structure from a bucket on the free list
 */
DataStore::FreelistBucket::FreelistBucket(size_t key, std::set<off_t>& bucket)
{
    size_t bucketsize =
        (2 * sizeof(size_t)) +
        (bucket.size() * sizeof(off_t)) +
        sizeof(uint32_t);
    size_t bufsize = bucketsize + sizeof(size_t);

    _buf.reset(new char[bufsize]);

    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */

    char* pos;

    pos = _buf.get();
    _size = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _key = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _nelements = reinterpret_cast<size_t*>(pos);
    pos += sizeof(size_t);
    _offsets = reinterpret_cast<off_t*>(pos);
    pos += (bucket.size() * sizeof(off_t));
    _crc = reinterpret_cast<uint32_t*>(pos);

    *_size = bucketsize;
    *_key = key;
    *_nelements = bucket.size();

    std::set<off_t>::iterator bucket_it;
    off_t offset = 0;

    for (bucket_it = bucket.begin();
         bucket_it != bucket.end();
         ++bucket_it)
    {

        _offsets[offset++] = *bucket_it;
    }

    *_crc = calculateCRC32((void*)_key, *_size - sizeof(uint32_t));
}

/* Construct an flb by reading it from a file
 */
DataStore::FreelistBucket::FreelistBucket(File::FilePtr& f, off_t offset)
{
    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */

    size_t bufsize;
    size_t bucketsize;

    f->readAll(&bucketsize, sizeof(size_t), offset);
    bufsize = bucketsize + sizeof(size_t);
    offset += sizeof(size_t);

    _buf.reset(new char[bufsize]);

    f->readAll(_buf.get() + sizeof(size_t), bucketsize, offset);

    char* pos = _buf.get();

    _size = (size_t*) pos;
    *_size = bucketsize;
    pos += sizeof(size_t);
    _key = (size_t*) pos;
    pos += sizeof(size_t);
    _nelements = (size_t*) pos;
    pos += sizeof(size_t);
    _offsets = (off_t*) pos;
    pos += (*_nelements * sizeof(off_t));
    _crc = (uint32_t*) pos;

    if (*_crc != calculateCRC32(_key, *_size - sizeof(uint32_t)))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_CORRUPT_FREELIST)
            << f->getPath();
    }
}

/* Serialize the flb to a file
 */
void
DataStore::FreelistBucket::write(File::FilePtr& f, off_t offset)
{
    /* Format of bucket:
       <size><key><nelements><offset 1>...<offset n><crc>
    */
    f->writeAll(_buf.get(), (*_size + sizeof(size_t)), offset);
}

/* Unserialize the flb into the freelist
 */
void
DataStore::FreelistBucket::unload(DataStoreFreelists& fl)
{
    LOG4CXX_TRACE(logger, "DataStore: unloading bucket with key " << *_key <<
                  " and size " << *_nelements);

    for (size_t offset = 0; offset < *_nelements; ++offset)
    {
        fl[*_key].insert(_offsets[offset]);
    }
}

/* Find space for the chunk of indicated size in the DataStore.
 */
off_t
DataStore::allocateSpace(size_t requestedSize, size_t& allocatedSize)
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);
    off_t ret = 0;
    DataStores* dsm = DataStores::getInstance();

    LOG4CXX_TRACE(logger, "datastore: allocate space " << requestedSize << " for " <<
                  _filename);

    invalidateFreelistFile();


    /* Round up required size to next power-of-two
     */
    size_t requiredSize = requestedSize + sizeof(DiskChunkHeader);
    if (requiredSize < dsm->getMinAllocSize())
        requiredSize = dsm->getMinAllocSize();
    requiredSize = roundUpPowerOf2(requiredSize);

    /* Check if the free lists have a chunk of the proper size
     */
    if (requiredSize > _largestFreeChunk)
    {
        makeMoreSpace(requiredSize);
    }
    SCIDB_ASSERT(requiredSize <= _largestFreeChunk);

    /* Look in the freelist to find a chunk to allocate.
     */
    ret = searchFreelist(requiredSize);
    allocatedSize = requiredSize;

    /* Update the largest free chunk
     */
    calcLargestFreeChunk();

    LOG4CXX_TRACE(logger, "datastore: allocate space " << requestedSize << " for "
                  << _filename << " returned " << ret);

    return ret;
}

/* Write bytes to the DataStore, to a location that is already
   allocated
 */
void
DataStore::writeData(off_t off,
                     void const* buffer,
                     size_t len,
                     size_t allocatedSize)
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);
    DataStores* dsm = DataStores::getInstance();

    checkFileOpen();
    SCIDB_ASSERT(_file);

    DiskChunkHeader hdr(false, allocatedSize);
    struct iovec iovs[2];

    /* Set up the iovecs
     */
    iovs[0].iov_base = (char*) &hdr;
    iovs[0].iov_len = sizeof(DiskChunkHeader);
    iovs[1].iov_base = (char*) buffer;
    iovs[1].iov_len = len;

    /* Issue the write
     */
    SCIDB_ASSERT(_file);
    _file->writeAllv(iovs, 2, off);

    /* Update the dirty flag and schedule flush if necessary
     */
    if (!_dirty)
    {
        _dirty = true;
        dsm->getFlusher().add(_dsk);
    }
}

/* Read a chunk from the DataStore
 */
size_t
DataStore::readData(off_t off, void* buffer, size_t len)
{
    LOG4CXX_TRACE(logger, "Reading data [dsk:" << _dsk.toString() << "]"
                          << ", [off:" << off << "]"
                          << ", [len: " << len << "]")
    DiskChunkHeader hdr;
    struct iovec iovs[2];

    checkFileOpen();
    SCIDB_ASSERT(_file);

    /* Set up the iovecs
     */
    iovs[0].iov_base = (char*) &hdr;
    iovs[0].iov_len = sizeof(DiskChunkHeader);
    iovs[1].iov_base = (char*) buffer;
    iovs[1].iov_len = len;

    /* Issue the read
     */
    _file->readAllv(iovs, 2, off);

    /* Check validity of header
     */
    if (!hdr.isValid())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_DATASTORE_CHUNK_CORRUPTED)
            << _file->getPath() << off;
    }

    return hdr.size;
}

/* Flush dirty data and metadata for the DataStore
 */
void
DataStore::flush()
{
    LOG4CXX_TRACE(logger, "DataStore::flush for ds " << _filename);

    ScopedMutexLock sm(_dslock, PTW_SML_DS);

    if (_dirty)
    {
        SCIDB_ASSERT(_file);
        LOG4CXX_TRACE(logger, "DataStore::flushing data for ds " << _filename);
        if (_file->fsync() != 0)
        {
            throw USER_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) <<
                "fsync " + _file->getPath();
        }
        _dirty = false;
    }
    if (_fldirty)
    {
        LOG4CXX_TRACE(logger, "DataStore::flushing metadata for ds " << _filename);
        persistFreelists();
    }
}

/* Mark chunk as free both in the free lists and on
   disk
 */
void
DataStore::freeChunk(off_t off, size_t allocated)
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);

    LOG4CXX_TRACE(logger, "datastore: free chunk " << off << " for " <<
                  _filename);

    invalidateFreelistFile();

    /* Update the free list --- first check if a parent block is already
       on the freelist (possibly in crash recovery case).  If so, we don't
       need to do anything.
    */
    if (!isParentBlockFree(off, allocated))
    {
        addToFreelist(allocated, off);

        /* Occaisionally we should check the integrity of
           the freelist (only in DEBUG)
        */
        if (isDebug())
        {
            if (++_frees % 10000 == 0)
            {
                verifyFreelistInternal();
            }
        }
    }
    calcLargestFreeChunk();
}


/* Return size information about the data store
 */
void
DataStore::getSizes(off_t& filebytes, off_t& filefree)
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);

    if(_isPunchHoleEnabled) {
        checkFileOpen(); // st_blksize required, so file must be created to obtain st.st_blksize
    }

    struct stat st;
    st.st_blocks = 0;  // If file has not been created no reason to stat it
    st.st_blksize = 0; // to check later assertion

    if (_isFileCreated)
    {
        /* Get the file length information
         */
        File::stat(_filename.c_str(), &st);
        ASSERT_EXCEPTION((size_t)st.st_size == _allocatedSize || !_file,
                         "Inconsistent datastore size in getSizes");
    }

    filebytes = st.st_blocks*512; // NOTE: st_blocks are always 512 bytes, regardless of st_blksize
    filefree = 0;

    /* Calc the number of free bytes in the file
     */
    if(_isPunchHoleEnabled) {
        ASSERT_EXCEPTION(st.st_blksize, "DataStore::getSizes(), st_blksize not available");

        for (auto dsit = _freelists.begin(); dsit != _freelists.end(); ++dsit) {    // lists of sizes
            // if there are free lists, file needs to be created because
            // we need st_blksize to determine physically unused space in the file.

            off_t len = dsit->first;
            for (auto offsetIt = dsit->second.begin(); offsetIt != dsit->second.end(); ++ offsetIt) {
                off_t start = *offsetIt;
                if (!_isFileCreated) {
                    LOG4CXX_ERROR(logger, "DataStore::getSizes(): !_isFileCreated but  _isstart: " << start << " len " << len);
                }

                off_t startCeiling  = ceiling(start, st.st_blksize);
                off_t endFloor      = floor(start+len, st.st_blksize);
                filefree += (startCeiling - start);
                filefree += ((start+len) - endFloor);
            }
        }
    } else {
        // indeterminate
        // 1. when a block was never written, the above is correct
        // 2. if an entire block has been written and returned to the free list
        // the line below is correct.
        // 3. We can't distinguish (1) from (2) with the existing data structures
        // 4. There are cases outside (1) and (2) such as if part of a binary buddy
        //    was written but then the full allocation returned to the free list
        LOG4CXX_INFO (logger, "DataStore::getSizes(): cannot determine filefree()"); // NOCHECKIN, s.b. DEBUG
        filefree = -1; // off_t is signed, this signifies the error
        LOG4CXX_INFO (logger, "DataStore::getSizes(): filefree set to " << filefree); // NOCHECKIN, s.b. DEBUG
    }
}

/* Persist free lists to disk
   @pre caller has locked the DataStore
 */
void
DataStore::persistFreelists()
{
    /* If we have yet to create the backing file, then there is no
       reason to persist the free lists.
     */
    if (!_isFileCreated)
    {
        return;
    }

    /* Open and truncate the freelist file
     */
    File::FilePtr flfile;
    std::string filename;

    filename = _filename + ".fl";
    flfile = FileManager::getInstance()->openFileObj(filename, O_CREAT | O_TRUNC | O_RDWR);
    if (!flfile)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_PATH)
            << filename;
    }

    /* Iterate the freelists, writing as we go
       File format:
       <# of buckets><bucket 1>...<bucket n>
     */
    off_t fileoff = 0;
    size_t nbuckets = _freelists.size();
    DataStoreFreelists::iterator freelist_it;
    std::set<off_t>::iterator bucket_it;

    LOG4CXX_TRACE(logger, "datastore: persisting freelist for " <<
                  _filename << " buckets " << nbuckets);

    flfile->writeAll((void*)&nbuckets, sizeof(size_t), fileoff);
    fileoff += sizeof(size_t);

    for (freelist_it = _freelists.begin();
         freelist_it != _freelists.end();
         ++freelist_it)
    {
        std::set<off_t>& bucket = freelist_it->second;
        if (bucket.size() == 0)
            continue;

        FreelistBucket flb(freelist_it->first, bucket);

        flb.write(flfile, fileoff);
        fileoff += flb.size();
    }

    flfile->fsync();
    _fldirty = false;
}

/* Initialize the free list and allocated size
   based on the size of the data store
*/
void
DataStore::initializeFreelist()
{
    struct stat st;
    DataStores* dsm = DataStores::getInstance();

    if (!_isFileCreated)
    {
        st.st_size = 0;
    }
    else
    {
        File::stat(_filename.c_str(), &st);
    }
    _allocatedSize = roundUpPowerOf2(st.st_size);

    /* If the file is empty, initialize with a single free
       block of the minimum size.
    */
    if (_allocatedSize < dsm->getMinAllocSize())
    {
        _allocatedSize = dsm->getMinAllocSize();
        _allocatedSize = roundUpPowerOf2(_allocatedSize);
        _freelists[_allocatedSize].insert(0);
    }
    else
    {
        readFreelistFromFile();
    }
    calcLargestFreeChunk();
}

/* Read free lists from disk file
   @returns number of buckets successfully read
*/
size_t
DataStore::readFreelistFromFile()
{
    /* Try to open the freelist file
     */
    File::FilePtr flfile;
    std::string filename;

    filename = _filename + ".fl";
    flfile = FileManager::getInstance()->openFileObj(filename, O_RDONLY);
    if (!flfile)
    {
        return 0;
    }

    /* Sanity check:  make sure its not empty
     */
    struct stat st;

    if (flfile->fstat(&st) || (st.st_size == 0))
    {
        LOG4CXX_ERROR(logger, "DataStore: found empty freelist file for " <<
                      _filename);
        return 0;
    }

    /* Try to parse the contents
       File format:
       <# of buckets><bucket 1>...<bucket n>
     */
    off_t fileoff = 0;
    size_t nbuckets = 0;
    size_t current;

    flfile->readAll(&nbuckets, sizeof(size_t), fileoff);
    fileoff += sizeof(size_t);

    LOG4CXX_TRACE(logger, "DataStore: reading " << nbuckets <<
                  " for freelist");

    for (current = 0; current < nbuckets; ++current)
    {
        try
        {
            FreelistBucket flb(flfile, fileoff);

            fileoff += flb.size();
            flb.unload(_freelists);
        }
        catch (SystemException const& x)
        {
            LOG4CXX_ERROR(logger, "DataStore: failed to read freelist for " <<
                          _filename << ", error (" << x.getErrorMessage() << ")");
            _freelists.clear();
            return 0;
        }
    }

    return nbuckets;
}

/* Invalidate the free-list file on disk
   @pre caller has locked the DataStore
 */
void
DataStore::invalidateFreelistFile()
{
    /* If we haven't created a backing file yet, then there is no freelist
       file on disk
     */
    if (!_isFileCreated)
    {
        return;
    }

    if (!_fldirty)
    {
        File::FilePtr flfile;
        std::string filename;
        size_t nbuckets = 0;
        DataStores* dsm = DataStores::getInstance();

        LOG4CXX_TRACE(logger, "datastore: invalidating freelist for " << _filename);

        filename = _filename + ".fl";
        flfile = FileManager::getInstance()->openFileObj(filename, O_CREAT | O_TRUNC | O_RDWR);
        if (!flfile)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_CANT_OPEN_PATH)
                << filename;
        }

        /* This is one vulnerable spot... after truncate, but before we write the zero byte
         */
        dsm->getErrorListener().throwif(__LINE__, __FILE__);

        flfile->writeAll((void*)&nbuckets, sizeof(size_t), 0);
        flfile->fsync();
        _fldirty = true;
        dsm->getFlusher().add(_dsk);
    }
}

/* Dump the free list to the log for debug
 */
void
DataStore::dumpFreelist()
{
    DataStoreFreelists::iterator fl_it = _freelists.begin();

    LOG4CXX_DEBUG(logger, "Freelists for datastore " <<
                  _file->getPath() << ": ");

    while (fl_it != _freelists.end())
    {
        set<off_t>::iterator bucket_it = fl_it->second.begin();

        LOG4CXX_DEBUG(logger, "   bucket [ " <<
                      fl_it->first << " ] :");

        while (bucket_it != fl_it->second.end())
        {
            LOG4CXX_DEBUG(logger, "     offset : " << *bucket_it);
            ++bucket_it;
        }
        ++fl_it;
    }
}

/* Check whether any parent blocks are on the freelist
   pre: lock is held
 */
bool
DataStore::isParentBlockFree(off_t off, size_t size)
{
    DataStoreFreelists::iterator fl_it =
        _freelists.upper_bound(size);

    while (fl_it != _freelists.end())
    {
        const size_t bucketSize = fl_it->first;
        const set<off_t>& bucket = fl_it->second;
        const off_t parentOff = off - (off % bucketSize);
        if (bucket.find(parentOff) != bucket.end())
        {
            return true;
        }
        ++fl_it;
    }
    return false;
}

/* Verify the integrity of the free list and throw exception
 * if there is a problem
 */
void
DataStore::verifyFreelist()
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);
    verifyFreelistInternal();
}

/* Verify the integrity of the free list when lock is already held
 */
void
DataStore::verifyFreelistInternal()
{
    DataStoreFreelists::iterator fl_it = _freelists.begin();

    while (fl_it != _freelists.end())
    {
        set<off_t>::iterator bucket_it = fl_it->second.begin();
        while (bucket_it != fl_it->second.end())
        {
            if (isParentBlockFree(*bucket_it, fl_it->first))
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                       SCIDB_LE_DATASTORE_CORRUPT_FREELIST)
                    << _file->getPath();
            }
            ++bucket_it;
        }
        ++fl_it;
    }
}

/* Remove the free-list file from disk
   @pre caller has locked the DataStore
 */
void
DataStore::removeFreelistFile()
{
    // The backing _file object is closed (and possibly removed) when
    // this DataStore is destroyed, but we must make special
    // arrangements for the corresponding freelist file.

    std::string filename = _filename + ".fl";
    File::remove(filename.c_str(), /*raise:*/ false);
}

/* Set the data store to be removed from disk on close
 */
void
DataStore::removeOnClose()
{
    ScopedMutexLock sm(_dslock, PTW_SML_DS);
    _isRemoveSet = true;
    if (_isFileCreated)
    {
        checkFileOpen();
        SCIDB_ASSERT(_file);
    }
}

/* Destroy a DataStore object
 */
DataStore::~DataStore()
{
    LOG4CXX_TRACE(logger, "Destroy DataStore for dsk=" << _dsk.toString());
}

/* Construct a new DataStore object
 */
DataStore::DataStore(char const* filename,
                     DataStoreKey dsk) :
    _dslock(),
    _dsk(dsk),
    _filename(filename),
    _isFileCreated(false),
    _isRemoveSet(false),
    _file(NULL),
    _frees(0),
    _largestFreeChunk(0),
    _dirty(false),
    _fldirty(false),
    _isPunchHoleEnabled(static_cast<bool>(Config::getInstance()->getOption<int>(CONFIG_DATASTORE_PUNCH_HOLES)))
{
    struct stat st;

    /* First check if the backing file has been created or not.
       We try to open the file lazily, but it is useful to know
       if the file has been created or not.
    */
    int err = File::stat(_filename.c_str(), &st, false);
    if (err == 0)
    {
        _isFileCreated = true;
    }
    else
    {
        if (err != ENOENT)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_SYSCALL_ERROR)
                << "fstat" << -1 << err << ::strerror(err) << _filename;
        }
    }

    /* Try to initialize the free lists from the free-list file.
     */
    initializeFreelist();

    LOG4CXX_DEBUG(logger, "DataStore: _isPunchHoleEnabled: " << _isPunchHoleEnabled);
}

/* Check if the backing file is open and if not, open it
   pre:  called under the mutex
 */
void
DataStore::checkFileOpen()
{
    if (!_file)
    {
        /* Open the file and make sure its length is at least the allocated
           size
         */
        _file =
            FileManager::getInstance()->openFileObj(_filename.c_str(),
                                                    O_LARGEFILE | O_RDWR | O_CREAT);
        if (_file.get() == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_CANT_OPEN_FILE)
                << _filename << ::strerror(errno) << errno;
        }

        struct stat st;

        if (_file->fstat(&st) != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_SYSCALL_ERROR)
                << "fstat" << -1 << errno << ::strerror(errno)
                << _file->getPath();
        }
        if (static_cast<size_t>(st.st_size) < _allocatedSize)
        {
            if (_file->ftruncate(_allocatedSize) != 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                       SCIDB_LE_SYSCALL_ERROR)
                    << "ftruncate" << -1 << errno << ::strerror(errno)
                    << _file->getPath();
            }
        }
        LOG4CXX_TRACE(logger, "datastore: new ds opened file " << _filename);
        _isFileCreated = true;
    }

    /* If remove-on-close was requested set it now...
     */
    SCIDB_ASSERT(_file);
    if (_isRemoveSet)
    {
        _file->removeOnClose();
    }
}


/* Round up size_t value to next power of two (static)
 */
size_t
DataStore::roundUpPowerOf2(size_t size)
{
    // TODO: incorrect.
    // size_t is 64 bits wide
    // this works only up to 32 bits
    // chunks have no such 32-bit limit

    // TODO: implement by right shifting
    // size (in a loop) by 1 bit position
    // until it becomes zero.  the count
    // of shifts required is where the high
    // bit was.  form the power of two
    // with 1 << count (+/- 1)

    size_t roundupSize = size;
    --roundupSize;
    roundupSize |= roundupSize >> 1;
    roundupSize |= roundupSize >> 2;
    roundupSize |= roundupSize >> 4;
    roundupSize |= roundupSize >> 8;
    roundupSize |= roundupSize >> 16;
    roundupSize |= roundupSize >> 32;
    ++roundupSize;

    return roundupSize;
}

/* Round a number down to multiple of factor
 */
size_t
DataStore::floor(size_t number, size_t factor)
{
    ASSERT_EXCEPTION(factor >= 1, "factor must be greater than 0");
    size_t result = (number/factor)*factor;

    SCIDB_ASSERT(result <= number);
    SCIDB_ASSERT(result % factor == 0);
    return result;
}

/* Round a number up to multiple of factor
 */
size_t
DataStore::ceiling(size_t number, size_t factor)
{
    ASSERT_EXCEPTION(factor >= 1, "factor must be greater than 0");
    ASSERT_EXCEPTION(number < numeric_limits<size_t>::max()-factor, "number must be less than max(size_t)-factor");
    size_t result = floor(number+(factor-1), factor);

    SCIDB_ASSERT(result >= number);
    return result;
}

/* Allocate more space into the data store to handle the requested chunk
 */
void
DataStore::makeMoreSpace(size_t request)
{
    SCIDB_ASSERT(request > _largestFreeChunk);
    SCIDB_ASSERT(_allocatedSize >= _largestFreeChunk);

    while (request > _largestFreeChunk)
    {
        _freelists[_allocatedSize].insert(_allocatedSize);
        _largestFreeChunk = _allocatedSize;
        _allocatedSize *= 2;
        if (_file)
        {
            if (_file->ftruncate(_allocatedSize) != 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                       SCIDB_LE_SYSCALL_ERROR)
                    << "ftruncate" << -1 << errno << ::strerror(errno)
                    << _file->getPath();
            }
        }
    }
}

/* Iterate the free lists and find a free chunk of the requested size
   @pre caller has locked the DataStore
 */
off_t
DataStore::searchFreelist(size_t request)
{
    off_t ret = 0;

    SCIDB_ASSERT(request <= _largestFreeChunk);

    /* Base case:  the target bucket contains a free chunk
     */
    DataStoreFreelists::iterator it =
        _freelists.find(request);
    if (it != _freelists.end())
    {
        assert(it->second.size() > 0);
        ret = *(it->second.begin());
        it->second.erase(ret);
        if (it->second.size() == 0)
        {
            _freelists.erase(it);
        }
    }
    /* Recursive case:  we have to get a free chunk by breaking
       up a larger free chunk
     */
    else
    {
        ret = searchFreelist(request * 2);
        _freelists[request].insert(ret + request);
    }

    return ret;
}

/* Add block to free list and try to consolidate buddy blocks
 */
void
DataStore::addToFreelist(size_t bucket, off_t off)
{
    SCIDB_ASSERT(roundUpPowerOf2(bucket) == bucket);
    SCIDB_ASSERT(off % bucket == 0);

    /* Calc the buddy block
     */
    size_t parent = bucket * 2;
    off_t buddy;

    if (off % parent == 0)
    {
        buddy = off + bucket;
    }
    else
    {
        buddy = off - bucket;
    }

    /* Check if the buddy is free
     */
    DataStoreFreelists::iterator it =
        _freelists.find(bucket);
    if (it != _freelists.end())
    {
        std::set<off_t>::iterator bucket_it;

        bucket_it = (it->second).find(buddy);
        if (bucket_it != (it->second).end())
        {
            /* Merge with the buddy
             */
            off_t merged = (off < buddy) ? off : buddy;

            (it->second).erase(bucket_it);
            if ((it->second).size() == 0)
            {
                _freelists.erase(it);
            }
            addToFreelist(parent, merged);
            return;
        }
    }

    /* Buddy is not free, just insert into
       free list
     */
    _freelists[bucket].insert(off);

    if(_isPunchHoleEnabled) { // return freed disk blocks to the filesystem for use by other datastores
        checkFileOpen();
        // return the disk blocks to the filesystem for use by other arrays
        int error = _file->fallocPunchHole(off, bucket);
        LOG4CXX_TRACE(logger, "DataStore::addToFreeList:"
                               << " fallocPunchHole(off " << off << ", bucket " << bucket << ")"
                               << " returns: " << error);
        if(error) {
            LOG4CXX_ERROR(logger, "DataStore: could not punch hole at off:" << off << ", size:" << bucket);
            char msgbuf[120];
            LOG4CXX_ERROR(logger, "DataStore: could not punch hole, errno:" << errno
                                  << ", " << strerror_r(errno, msgbuf, sizeof(msgbuf)));
            // NOTE: we do not throw, because this call can fail on filesystems other than
            //       ext4 and xfs.
            //       On such systems one should not set the config variable, but it does
            //       not seem appropriate to cause the query to error if such a mistake were made
        }
    }
}

/* Update the largest free chunk member
 */
void
DataStore::calcLargestFreeChunk()
{
    DataStoreFreelists::const_iterator it;

    it = _freelists.end();
    _largestFreeChunk =
        (it == _freelists.begin()) ?
        0 :
        (--it)->first;
}

/* Initialize the global DataStore state
 */
void
DataStores::initDataStores(char const* basepath)
{
    ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);

    if (_theDataStores == NULL)
    {
        SCIDB_ASSERT(_theNameSpaceLabelMap == NULL);
        SCIDB_ASSERT(_theNameSpaceIdMap == NULL);

        _basePath = basepath;
        _basePath += "/";
        _minAllocSize =
            Config::getInstance()->getOption<int> (
                CONFIG_STORAGE_MIN_ALLOC_SIZE_BYTES
                );

        /* Create the datastore directory if necessary
         */
        if (int rc = File::createDir(_basePath))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_CANT_CREATE_DIRECTORY)
                << _basePath << ::strerror(rc);
        }

        /* Initialize the namespace map
         */
        _theNameSpaceLabelMap = new NameSpaceLabelMap();
        _theNameSpaceIdMap = new NameSpaceIdMap();

        std::function<void(struct dirent * de)> cb = std::bind(&DataStores::namespaceCb,
                                                               this,
                                                               std::placeholders::_1);
        File::processDir(_basePath.c_str(), cb);

        /* Start background flusher
         */
        int syncMSeconds =
            Config::getInstance()->getOption<int>(
                CONFIG_SYNC_IO_INTERVAL
                );
        if (syncMSeconds > 0)
        {
            _dsflusher.start(syncMSeconds);
        }

        _theDataStores = new DataStoreMap();

        /* Start error listener
         */
        _listener.start();
    }
}

/* Callback for processing namespaces
 */
void
DataStores::namespaceCb(struct dirent* de)
{
    SCIDB_ASSERT(de);

    /* If entry is not a dir, skip it
     */
    if (de->d_type != DT_DIR)
    {
        return;
    }

    /* Parse the name field -- label will be null-terminated
     */
    uint nsId;
    std::string label;
    char* labelstart = strchr(de->d_name, '_');
    if (!labelstart)
    {
        return;
    }
    nsId = static_cast<DataStore::NsId>(strtol(de->d_name, NULL, 0));
    label = ++labelstart;

    /* Record the namespace information
     */
    if (_theNameSpaceIdMap->size() < nsId + 1)
    {
        _theNameSpaceIdMap->resize(nsId + 1);
    }
    (*_theNameSpaceIdMap)[nsId] = label;
    _theNameSpaceLabelMap->insert(make_pair(label,nsId));
}

/* Open a namespace
   Note: namespace is created if it does not already exist
 */
DataStore::NsId
DataStores::openNamespace(std::string label)
{
    ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);
    DataStore::NsId nsId = 0;
    NameSpaceLabelMap::iterator nslit;

    SCIDB_ASSERT(_theNameSpaceIdMap);
    SCIDB_ASSERT(_theNameSpaceLabelMap);

    /* Find out if the namespace is already there
     */
    nslit = _theNameSpaceLabelMap->find(label);
    if (nslit == _theNameSpaceLabelMap->end())
    {
        /* Create the new namespace and add it to the map
         */
        stringstream nsDirName;

        nsId = static_cast<DataStore::NsId>(_theNameSpaceIdMap->size());
        nsDirName << _basePath << nsId << '_' << label;
        if (int rc = File::createDir(nsDirName.str()))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE,
                                   SCIDB_LE_CANT_CREATE_DIRECTORY)
                << _basePath << ::strerror(rc);
        }

        _theNameSpaceIdMap->push_back(label);
        _theNameSpaceLabelMap->insert(make_pair(label,nsId));
    }
    else
    {
        nsId = nslit->second;
    }

    /* Return the ndId
     */
    return nsId;
}

/* Get a reference to a specific DataStore
 */
std::shared_ptr<DataStore>
DataStores::getDataStore(const DataStore::DataStoreKey& dsk)
{
    DataStoreMap::iterator it;
    DataStore::NsId nsid = dsk.getNsid();
    DataStore::DsId dsid = dsk.getDsid();
    std::shared_ptr<DataStore> retval;
    ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);

    SCIDB_ASSERT(_theDataStores);
    SCIDB_ASSERT(_theNameSpaceIdMap);
    SCIDB_ASSERT(_theNameSpaceLabelMap);

    /* Check the map
     */
    it = _theDataStores->find(dsk);
    if (it != _theDataStores->end())
    {
        return it->second;
    }

    /* Not found, figure out the path based on the namespace
     */
    stringstream filepath;

    ASSERT_EXCEPTION(nsid < _theNameSpaceIdMap->size(),
                     "Invalid name space id.");
    filepath << _basePath << nsid << '_'
             << (*_theNameSpaceIdMap)[nsid] << '/'
             << dsid << ".data";

    retval = std::make_shared<DataStore>(filepath.str().c_str(),
                                           dsk);
    (*_theDataStores)[dsk] = retval;

    return retval;
}

/* Remove a data store from memory and disk
 */
void
DataStores::closeDataStore(DataStore::DataStoreKey& dsk, bool remove)
{
    DataStoreMap::iterator it;
    DataStore::NsId nsid = dsk.getNsid();
    DataStore::DsId dsid = dsk.getDsid();
    ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);

    SCIDB_ASSERT(_theDataStores);

    /* Check the map
     */
    it = _theDataStores->find(dsk);
    if (it == _theDataStores->end())
    {
        /* It isn't in the map... maybe it hasn't been opened this time.  If remove
           is specified we need to open it so we can remove it from disk.
         */
        if (remove)
        {
            LOG4CXX_TRACE(logger, "closeDataStore(dsk=" << dsk.toString()
                          << "): Reopen for removal!");

            stringstream filepath;

            ASSERT_EXCEPTION(nsid < _theNameSpaceIdMap->size(),
                             "Invalid name space id.");
            filepath << _basePath << nsid << '_'
                     << (*_theNameSpaceIdMap)[nsid] << '/'
                     << dsid << ".data";

            it =
                _theDataStores->insert(
                    make_pair(
                        dsk,
                        std::make_shared<DataStore>(filepath.str().c_str(),
                                                      dsk)
                        )
                    ).first;
        }
        else
        {
            return;
        }
    }

    /* Flush it
     */
    it->second->flush();

    /* Remove it from the map
     */
    if (remove)
    {
        it->second->removeOnClose();
        it->second->removeFreelistFile();
    }
    _theDataStores->erase(it);
}

/* Flush all DataStore objects
 */
void
DataStores::flushAllDataStores()
{
    DataStoreMap::iterator it;
    std::shared_ptr<DataStore> current;
    DataStore::DataStoreKey lastDsk;

    while (true)
    {
        {
            ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);

            SCIDB_ASSERT(_theDataStores);

            it = _theDataStores->upper_bound(lastDsk);
            if (it == _theDataStores->end())
            {
                break;
            }
            current = it->second;
            lastDsk = it->first;
        }

        current->flush();
        current.reset();
    }
}

/* Clear all datastore files from the basepath
 */
void
DataStores::clearAllDataStores()
{
    /* Try to open the base dir
     */
    DIR* dirp = ::opendir(_basePath.c_str());

    if (dirp == NULL)
    {
        LOG4CXX_ERROR(logger, "DataStores::clearAllDataStores: failed to open base dir, aborting clearAll");
        return;
    }

    OnScopeExit dirCloser([this, dirp] () { File::closeDir(_basePath.c_str(), dirp, false); });

    struct dirent entry;
    memset(&entry, 0, sizeof(entry));

    /* For each entry in the base dir
     */
    while (true)
    {
        struct dirent *result(NULL);

        int rc = ::readdir_r(dirp, &entry, &result);
        if (rc != 0 || result == NULL)
        {
            return;
        }
        assert(result == &entry);

        LOG4CXX_TRACE(logger, "DataStores::clearAllDataStores: found entry " << entry.d_name);

        /* If its a datastore or fl file, go ahead and try to remove it
         */
        size_t entrylen = strlen(entry.d_name);
        size_t fllen = strlen(".fl");
        size_t datalen = strlen(".data");
        const char* entryend = entry.d_name + entrylen;

        /* Check if entry ends in ".fl" or ".data"
         */
        if (((entrylen > fllen) &&
             (strcmp(entryend - fllen, ".fl") == 0)) ||
            ((entrylen > datalen) &&
             (strcmp(entryend - datalen, ".data") == 0))
            )
        {
            LOG4CXX_TRACE(logger, "DataStores::clearAllDataStores: deleting entry " << entry.d_name);
            std::string fullpath = _basePath + "/" + entry.d_name;
            File::remove(fullpath.c_str(), false);
        }
    }
}

/* List information about all datastores using the builder
 */
void
DataStores::visitDataStores(Visitor visit) const
{
    ScopedMutexLock sm(_dataStoreLock, PTW_SML_DS);
    DataStoreMap::iterator it = _theDataStores->begin();

    while (it != _theDataStores->end())
    {
        LOG4CXX_TRACE(logger, "visit data stores visiting: " <<
                      it->second->getDsk().toString());
        visit(*(it->second));
        ++it;
    }
}

/* Destroy the DataStores object
 */
DataStores::~DataStores()
{
    _dsflusher.stop();
    _listener.stop();
}

/* Get current time in nanosecond resolution
 */
inline int64_t getTimeNanos()
{
    struct timeval tv;
    gettimeofday(&tv,0);
    return ((int64_t) tv.tv_sec) * 1000000000 + ((int64_t) tv.tv_usec) * 1000;
}

/* Main loop for DataStoreFlusher
 */
void
DataStoreFlusher::FlushJob::run()
{
    while (true)
    {
        int64_t totalSyncTime= 0;
        {
            /* Collect the datastores we need to flush
             */
            set<DataStore::DataStoreKey>::iterator it;
            set<DataStore::DataStoreKey> dss;
            {
                ScopedMutexLock cs(_flusher->_lock, PTW_SML_DS);
                if ( (_flusher->_running) == false)
                {
                    return;
                }

                for (it = _flusher->_datastores.begin();
                     it != _flusher->_datastores.end();
                     ++it)
                {
                    dss.insert(*it);
                }
                _flusher->_datastores.clear();
            }

            /* Flush the collected data stores
             */
            DataStores* dsm = DataStores::getInstance();
            for (it = dss.begin(); it != dss.end(); ++it)
            {
                std::shared_ptr<DataStore> ds;

                int64_t t0 = getTimeNanos();
                ds = dsm->getDataStore(*it);
                ds->flush();
                int64_t t1 = getTimeNanos();
                totalSyncTime = totalSyncTime + t1 - t0;
            }
        }

        if ( totalSyncTime < _timeIntervalNanos )
        {
            uint64_t sleepTime = _timeIntervalNanos - totalSyncTime;
            struct timespec req;
            req.tv_sec= sleepTime / 1000000000;
            req.tv_nsec = sleepTime % 1000000000;
            while (::nanosleep(&req, &req) != 0)
            {
                if (errno != EINTR)
                {
                    LOG4CXX_ERROR(logger, "DataStoreFlusher: nanosleep fail errno "<<errno);
                }
            }
        }
    }
}

/* Start the data store flusher
 */
void
DataStoreFlusher::start(int timeIntervalMSecs)
{
    ScopedMutexLock cs(_lock, PTW_SML_DS);
    if (_running)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_STORAGE, SCIDB_LE_OPERATION_FAILED) <<
            "DataStoreFlusher: error on start; already running";
    }

    _running = true;

    if (!_threadPool->isStarted())
    {
        _threadPool->start();
    }
    _myJob.reset(new FlushJob(timeIntervalMSecs, this));
    _queue->pushJob(_myJob);
}

/* Shut down the data store flusher
 */
void
DataStoreFlusher::stop()
{
    {
        ScopedMutexLock cs(_lock, PTW_SML_DS);
        if (_running)
        {
            _running = false;
        }
        else
        {
            return;
        }
    }

    if(!_myJob->wait(PTW_JOB_DATASTORE_FLUSH_STOP))
    {
        LOG4CXX_ERROR(logger, "DataStoreFlusher: error on stop.");
    }

    _datastores.clear();
}

/* Schedule a data store to be flushed
 */
void
DataStoreFlusher::add(DataStore::DataStoreKey& dsk)
{
    ScopedMutexLock cs(_lock, PTW_SML_DS);
    if(_running)
    {
        _datastores.insert(dsk);
    }
}

/* Destroy the flusher
 */
DataStoreFlusher::~DataStoreFlusher()
{
    stop();
}

} // namespace scidb
