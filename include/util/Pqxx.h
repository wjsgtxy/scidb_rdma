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
 * @file Pqxx.h
 * @brief Wrap libpqxx header files, and provide version-specific tailoring.
 *
 * @description
 * Libpqxx is the C++ Postgres client library.  This header serves as
 * a central place to tailor our libpqxx usage, especially when we
 * need to navigate API differences across library versions shipped
 * with different Linux distros.
 */

#ifndef UTIL_PQXX_H
#define UTIL_PQXX_H

#include <pqxx/connection>
#include <pqxx/transaction>
#include <pqxx/prepared_statement>
#include <pqxx/except>
#include <pqxx/binarystring>
#include <pqxx/version>
#include <libpq-fe.h>

static_assert(PQXX_VERSION_MAJOR >= 4,
              "Building SciDB requires at least version 4 of libpqxx.");

#if PQXX_VERSION_MAJOR >= 5
    /* No more PGSTD (supported older compilers). */
#   define PGSTD std
#endif

#endif  // ! UTIL_PQXX_H
