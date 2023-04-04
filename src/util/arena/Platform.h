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

#ifndef UTIL_ARENA_PLATFORM_H_
#define UTIL_ARENA_PLATFORM_H_

/****************************************************************************/

/**
 *  Return the actual usable size of a block allocated by std::malloc().
 *
 *  The implementation is non-standard; most heap implementations do provide a
 *  version of this API in some form or other, however.
 */
#if defined(__GNUC__)
# include <malloc.h>
# define getBlockSize   malloc_usable_size
#else
# pragma message "no definition for getBlockSize supplied"
#endif

/****************************************************************************/
#endif
/****************************************************************************/
