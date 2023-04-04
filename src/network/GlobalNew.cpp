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

#include <new>                                           // For bad_alloc etc.
#include <util/arena/Malloc.h>                           // For malloc() etc.

/****************************************************************************/

/**
 *  Implements global operator new by delegating to arena::malloc().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20new
 */
void* operator new(std::size_t size)  _GLIBCXX_THROW(std::bad_alloc)
{
    if (void* p = scidb::arena::malloc(size))            // Use our own malloc
    {
        return p;                                        // ...the allocation
    }

    throw std::bad_alloc();                              // Failed to allocate
}

/**
 *  Implements global operator new by delegating to arena::malloc().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20new
 */
void* operator new(std::size_t size,const std::nothrow_t&) _GLIBCXX_USE_NOEXCEPT
{
    return scidb::arena::malloc(size);                   // Use our own malloc
}

/**
 *  Implements global operator new[] by delegating to arena::malloc().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20new%5B%5D
 */
void* operator new[](std::size_t size) _GLIBCXX_THROW(std::bad_alloc)
{
    return ::operator new(size);                         // Use our own malloc
}

/**
 *  Implements global operator new[] by delegating to arena::malloc().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20new%5B%5D
 */
void* operator new[](std::size_t size,const std::nothrow_t&) _GLIBCXX_USE_NOEXCEPT
{
    return scidb::arena::malloc(size);                   // Use our own malloc
}

/**
 *  Implements global operator delete by delegating to arena::free().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20delete
 */
void operator delete(void* payload) _GLIBCXX_USE_NOEXCEPT
{
    scidb::arena::free(payload);                         // Use our own free
}

/**
 *  Implements global operator delete by delegating to arena::free().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20delete
 */
void operator delete(void* payload,const std::nothrow_t&) _GLIBCXX_USE_NOEXCEPT
{
    scidb::arena::free(payload);                         // Use our own free
}

/**
 *  Implements global operator delete[] by delegating to arena::free().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20delete%5B%5D
 */
void operator delete[](void* payload) _GLIBCXX_USE_NOEXCEPT
{
    scidb::arena::free(payload);                         // Use our own free
}

/**
 *  Implements global operator delete[] by delegating to arena::free().
 *
 *  @see http://www.cplusplus.com/reference/new/operator%20delete%5B%5D
 */
void operator delete[](void* payload,const std::nothrow_t&) _GLIBCXX_USE_NOEXCEPT
{
    scidb::arena::free(payload);                         // Use our own free
}

/****************************************************************************/
