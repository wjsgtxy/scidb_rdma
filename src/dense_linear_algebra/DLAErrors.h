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

/*
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Error codes for dense linear algebra plugin
 */

#ifndef DENSE_LINEAR_ERRORS_H_
#define DENSE_LINEAR_ERRORS_H_

#include <system/ErrorCodes.h>

#define DLANameSpace "DLA"

enum
{
    DLA_PREVIOUS_ERROR = SCIDB_USER_ERROR_CODE_START - 1,
#   define X(_name, _msg)  _name ,
#   include "DLAErrors.inc"
#   undef X
};

#endif /* DENSE_LINEAR_ERRORS_H_ */
