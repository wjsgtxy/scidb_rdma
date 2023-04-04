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

/****************************************************************************/

#include <atomic>

#include <pthread.h>                                     // For pthread_mutex
#include <errno.h>                                       // For the code EBUSY

#include "Platform.h"                                    // For getBlockSize()
#include "ArenaDetails.h"                                // For implementation
#include <system/Config.h>                               // For CONFIG_POOLED_ALLOCATION

/****************************************************************************/
namespace scidb { namespace arena { namespace {
/****************************************************************************/

std::atomic<size_t> _bytesShared(0);    // bytes allocated (excludes in-malloc data structres)
std::atomic<size_t> _peakShared(0);     // High water mark of _bytesShared
std::atomic<size_t> _limitShared(SIZE_MAX);     // Max bytes allowed
std::atomic<size_t> _blocksShared(0);   // Num blocks allocated (questionable value)
// TODO: to be removed later (postponed from release branch changes)
pthread_mutex_t _mutex  = PTHREAD_MUTEX_INITIALIZER;     // Guards counters

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Release the (copy of) the root arena mutex when forking a child process.
 *
 *  Call from the one and only child process thread immediately upon forking.
 *
 *  @see http://cppwisdom.quora.com/Why-threads-and-fork-dont-mix
 */
void onForkOfChild()    // probably meant onExec because fork is a duplicate process
{
    // TODO: change name later (postponed from release branch changes)
    // TODO: to be removed later (postponed from release branch changes)
    int e = pthread_mutex_trylock(&_mutex);              // But do not block

    if (e==0 || e==EBUSY)                                // So we acquired it?
    {
        if (pthread_mutex_unlock(&_mutex) == 0)          // ...we released it?
        {
         /* So we now know for sure that the (copy of) the root arena _mutex
            is definately not locked within the one and only thread that now
            runs in the forked child process...*/

            _bytesShared.store(0);                       // re-initialize
            _peakShared.store(0);                        // re-intialize
            _blocksShared.store(0);                      // re-initialize
            _limitShared.store(SIZE_MAX);                // re-iinitialize
            return;                                      // ...and we're good
        }
    }

    abort();                                             // Malloc is unusable
}

/**
 *  Return the maximum memory limit in bytes. An attempt to allocate more than
 *  this number of bytes should fail; malloc() and friends return 0, while the
 *  root arena and operator new throw a std::bad_alloc exception.
 */
size_t getMemoryLimit()
{
    return _limitShared.load();                          // The current limit
}

/**
 *  Assign the maximum memory limit in bytes. An attempt to allocate more than
 *  this number of bytes should fail; malloc() and friends return 0, while the
 *  root arena and operator new throw a std::bad_alloc exception.
 *  TODO: Setting or not setting the limit based on whether we are already over the
 *  limit is of dubious value.  It may well be better to set the limit even if we
 *  are already over what is desired.  To refuse to set the limit in that case
 *  may be to cause the user further delay in stopping the growth of a scidb
 *  process.
 */
bool setMemoryLimit(size_t limit)
{
    if (_bytesShared.load() <= _limitShared.load())      // Not yet exceeded at a single moment?
    {
        _limitShared.store(limit);                       // ...ok, then set it
                                                         // note this is not atomic w.r.t.
                                                         // _bytesShared which is lock-free
                                                         // so it is possible that when the limit
                                                         // is set, we are over the limit.
                                                         // But no further allocations take
                                                         // place once _limitShared is set.

        return true;                                     // ...set succeeded
    }

    return false;                                        // Assignment failed
                                                         // meaning for sure we knew we were
                                                         // over the limit for one moment in time
}

//
//  High performance replacements for the pthread_mutex that used to protect
//  _bytesShared and _peakShared during [de]allocations.
//
//  This technique saved over 5% _real time_ query throughput in some important
//  commercial cases as measured in a 15.6 code base.
//
//  For example using CAS ( Compare And Swap, in this case, std::atomic<>::compare_exchange )
//  is 200-2000 times faster than a mutex on x86_64 Linux in the no-contention
//  case, and the overhead of a Mutex is typically larger than the time to execute the
//  underlying malloc code.
//
/**
 *  This function is used _prior_ to each allocation
 *  returns non-zero if a new high-watermark could be required
 *  as a consequence.
 */
inline bool publishBytesIncreaseIfAllowed(size_t increase, size_t limit, size_t& newPeak)
{
    assert(_bytesShared.is_lock_free());

    size_t bytesSample;
    size_t moreBytes;
    do {
        bytesSample = _bytesShared.load();
        moreBytes = bytesSample + increase;
        if(moreBytes > limit) { // overlimit failure
            newPeak=0;
            return false;
        }
    } while(!_bytesShared.compare_exchange_weak(bytesSample, moreBytes));

    newPeak=moreBytes;
    return true;
}

/**
 *  This function is used only to correct round-up ammounts that
 *  cannot be known until after an allocation succeeds (since the malloc
 *  api does not provide any limit on round-up in adance of allocation).
 *  If it turns out we are over, it makes little sense to back out, as the
 *  amount of overage would likely be small for any desireable malloc.
 *  But rather than ingnore the overage, we publish it.
 */
inline void publishBytesIncrease(size_t increase, size_t& newPeak)
{
    if (increase > 0) {
        newPeak = _bytesShared += increase;
    } else {
        newPeak = 0;
    }
}

/**
 *  This function is used _after_ deallocation
 *  or a failed allocation that was pre-reserved
 *  via publishBytesIncrease() and must be withdrawn
 */
inline void publishBytesDecrease(size_t decrease)
{
    assert(_bytesShared.load() >= decrease);    // true even when _bytes changes in other threads
    if(decrease > 0) {
        _bytesShared -= decrease;
    }
}

/**
 *  High performance replacement for using a lock to protect
 *  a maximum shared value.
 *
 *  to understand this implementation, and to validate it,
 *  you need to be familiar with how to correctly use
 *  std::atomic<>::compare_exchange_{weak,strong}
 *
 */
template<typename T>
void atomicEnsureMax(std::atomic<T>& maxShared, T val)
{
    assert(maxShared.is_lock_free());
    T maxSample;
    do {
        maxSample = maxShared.load();
        if(val <= maxSample) { break ;}    // maxShared is already >= val
    } while(!maxShared.compare_exchange_weak(maxSample, val));
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/malloc
 *
 *  note that malloc(0) is legitimate and should return NULL or
 *  a pointer that works with free() depending on the underlying
 *  implementation
 */
void* malloc(size_t size)
{
    //
    // atomically check that this thread may increase memory usage by size
    // and publish the new number of bytes used
    //
    size_t possiblePeak1;
    if(!publishBytesIncreaseIfAllowed(size, _limitShared.load(), possiblePeak1)) {
        return nullptr;                                     // ...don't even try
    }

    //
    // attempt the allocation
    //
    void* p = std::malloc(size);
    if (!p) {
        publishBytesDecrease(size);
        return nullptr;
    }

    //
    // alloc successful
    // but the request is rounded up to some granularity (not necessarily
    // a multiple of bytes).  Those bytes have been accounted for too
    // despite the extra accounting cost of doing so.
    // Note that does not include other memory used within the alloc
    // implementation we are calling.
    //
    size_t blockSize = getBlockSize(p);                 // actual size
    assert(blockSize >= size);
    size_t extra  = blockSize - size;                  // amount not already accounted for

    assert(blockSize == size + extra);
    size_t possiblePeak2;
    publishBytesIncrease(extra, possiblePeak2);
    atomicEnsureMax(_peakShared, std::max(possiblePeak1, possiblePeak2));
    _blocksShared++;

    return p;                                   // ...the allocation
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/calloc
 */
void* calloc(size_t count, size_t sz)
{
    assert(count < SIZE_MAX/sz);                       // Beware of overlow
    size_t size = count*sz;    // makes remaining code nearly identical with malloc, above

    //
    // atomically check that this thread may increase memory usage by size
    // and publish the new number of bytes used
    //
    size_t possiblePeak1;
    if(!publishBytesIncreaseIfAllowed(size, _limitShared.load(), possiblePeak1)) {
        return NULL;                                     // ...don't even try
    }

    void* p = std::calloc(count, sz);               // Allocate as normal

    // shoud be just like malloc from here on
    if (!p) {
        publishBytesDecrease(size);
        return NULL;
    }

    //
    // alloc successful
    // but the request is rounded up to some granularity (not necessarily
    // a multiple of bytes).  Those bytes have been accounted for too
    // despite the extra accounting cost of doing so.
    // Note that does not include other memory used within the alloc
    // implementation we are calling.
    //
    size_t blockSize = getBlockSize(p);                 // actual size
    assert(blockSize >= size);
    size_t extra  = blockSize - size;                  // amount not already accounted for

    assert(blockSize == size + extra);
    size_t possiblePeak2;
    publishBytesIncrease(extra, possiblePeak2);
    atomicEnsureMax(_peakShared, std::max(possiblePeak1, possiblePeak2));
    _blocksShared++;

    return p;                                   // ...the allocation
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/realloc
 *
 *  Note especially that according to specifications
 *  1) realloc(0, size!=0) is equivlaent to malloc(size)
 *  2) realloc(ptr!=0, 0) is equivalent to free(ptr)
 */
void* realloc(void* ptr, size_t size)
{
    size_t blockSize1 = getBlockSize(ptr);          // Fetch current size
    assert(blockSize1 <= _bytesShared.load());      // Verify accounting

    if (size > blockSize1) { // growing, have to pre-reserve the increase
        size_t growthEstimate = size - blockSize1;

        size_t possiblePeak1;
        if(!publishBytesIncreaseIfAllowed(growthEstimate, _limitShared.load(), possiblePeak1)) {
            return NULL;                                     // ...don't even try
        }

        void* p = std::realloc(ptr, size);         // growing case
        if (!p) {
            publishBytesDecrease(growthEstimate);
            return NULL; // failure of realloc
        }

        size_t blockSize2 = getBlockSize(p);       // actual size
        assert(blockSize2 >= blockSize1);          // must not have shrunk

        size_t growth  = blockSize2 - blockSize1;  // actual growth
        ssize_t extra = growth - growthEstimate;   // growth beyond prediction

        size_t possiblePeak2;
        publishBytesIncrease(extra, possiblePeak2);
        atomicEnsureMax(_peakShared, std::max(possiblePeak1, possiblePeak2));
        if(!ptr && size) {
            _blocksShared++;    // "equivalent to malloc(size)"
        }

        return p;

    } else { // shrinking or at least logically shrinking (or no change)
        void* p = std::realloc(ptr, size);
        if (!p) {
            if(ptr && !size) {
                _blocksShared--;    // "equivalent to free"
            }
            return NULL;
        }
        size_t blockSize2 = getBlockSize(p);       // actual size

        if(blockSize1 >= blockSize2) {          // it did not grow
            size_t shrinkage  = blockSize1 - blockSize2;  // actual shrink
            publishBytesDecrease(shrinkage);
        } else {
            // [ infrequent case (did not happen with the code-freeze checkin tests)
            //   can happen e.g. with MALLOC_CHECK_=3 and MALLOC_PERTURB_=127 set
            //   in the environment, and glibC providing std::malloc()]
            //
            // size is smaller than blockSize1, so we expected blockSize2
            // to be the same or smaller, but actually it was larger.
            //
            size_t extra = blockSize2 - blockSize1;
            size_t possiblePeak;
            publishBytesIncrease(extra, possiblePeak);
            atomicEnsureMax(_peakShared, possiblePeak);
            // do not modify _blocksShared in this case
        }
        return p;
    }

    assert(false); // NOTREACHED
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/free
 */
void free(void* payload)
{
    size_t blockSize = getBlockSize(payload);            // Fetch actual size
    if (blockSize)
    {
        assert(_blocksShared.load() > 0);
        assert(blockSize <= _bytesShared.load());

        std::free(payload);                              // ...free as normal
        publishBytesDecrease(blockSize);
        _blocksShared-- ;
    }
}

/// @return whether the SciDB server is configured to use pooled allocation.
/// If true, SciDB will allocate scidb::Value objects from the query arenas.
inline bool pooledAllocation()
{
    static const bool use = Config::getInstance()->getOption<bool>(CONFIG_POOLED_ALLOCATION);
    return use;
}

/// A wrapper over pooledAllocation().
/// The motivation of introducing the wrapper is to enable entry.cpp to indirectly call pooledAllocation(),
/// without sacrificing the ability for the compiler to make it inline to speedup mallocTLS(), etc. here.
bool pooledAllocationWrapper()
{
    return pooledAllocation();
}

void* mallocTLS(std::size_t size)
{
    if (Arena::getArenaTLS() && pooledAllocation()) {
        return Arena::getArenaTLS()->malloc(size);
    }
    return arena::malloc(size);
}

void* callocTLS(std::size_t num, size_t size)
{
    if (Arena::getArenaTLS() && pooledAllocation()) {
        return Arena::getArenaTLS()->calloc(num, size);
    }
    return arena::calloc(num, size);
}

void  freeTLS(void* ptr, size_t size)
{
    if (Arena::getArenaTLS() && pooledAllocation()) {
        Arena::getArenaTLS()->free(ptr, size);
        return;
    }
    arena::free(ptr);
}

/**
 *  Return the root arena,  the singleton %arena from which every other %arena
 *  ultimately obtains its memory.
 *
 *  The root arena forms the root of a parent-child tree that runs through all
 *  other arenas up to this singleton instance,  and is automatically supplied
 *  as the  default when constructing an %arena for  which no other parent has
 *  been specified.
 *
 *  It's implemented as a stateless wrapper around the global counters defined
 *  above, delegating to arena::malloc() and arena::free() to manipulate these
 *  counters and so adhere to the maximum memory limit installed at startup.
 */
ArenaPtr getRootArena()
{
    static struct RootArena : Arena
    {
        name_t      name()                         const {return "root";}
        size_t      available()                    const {return _limitShared.load() - _bytesShared.load();}
        size_t      allocated()                    const {return _bytesShared.load();}
        size_t      peakusage()                    const {return _peakShared.load();}
        size_t      allocations()                  const {return _blocksShared.load();}
        features_t  features()                     const {return finalizing|recycling|threading;}

        void* doMalloc(size_t size)
        {
            assert(size != 0);                           // Validate arguments

            if (void* p = arena::malloc(size))           // Use our own malloc
            {
                return p;                                // ...yes, succeeded
            }

            this->exhausted(size);                       // Failed to allocate
        }

        void doFree(void* payload,size_t size)
        {
            assert(aligned(payload));                    // Validate argument
            assert(size <= getBlockSize(payload));       // Validate its size

            arena::free(payload);                        // Use our own free

            (void)size;                                  // Unused in release
        }

    } theRootArena;                                      // The singleton root

    return ArenaPtr(static_cast<Arena*>(&theRootArena),null_deleter());
}

/****************************************************************************/
}}
/****************************************************************************/
