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

#ifndef UTIL_ARENA_MALLOC_H_
#define UTIL_ARENA_MALLOC_H_

#include <cstddef>  // for size_t

/****************************************************************************/
namespace scidb { namespace arena {
/****************************************************************************/
/**
 *  @brief      Arena-compatible versions of the standard freestore functions.
 *
 *  @details    These versions  of the standard freestore functions  cooperate
 *              with the root arena to maintain statistics about the number of
 *              heap allocations that remain live, and enforce a maximum upper
 *              limit on the amount of memory that can be allocated by clients
 *              of the arena library.
 *
 *              Together, they form the underlying implementation for both the
 *              root arena and the global operator new.
 *
 *  @author     jbell@paradigm4.com.
 */

void*   malloc  (std::size_t );
void*   calloc  (std::size_t,std::size_t);
void*   realloc (void*,std::size_t);
void    free    (void*);

void*   mallocTLS  (std::size_t );
void*   callocTLS  (std::size_t,std::size_t);
void    freeTLS    (void*, std::size_t);

/****************************************************************************/
}}
/****************************************************************************/
#endif
/****************************************************************************/
