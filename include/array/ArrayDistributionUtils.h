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
#ifndef ARRAY_DISTRIBUTION_UTILS_H_
#define ARRAY_DISTRIBUTION_UTILS_H_

#include <iostream>
#include <memory>
#include <sstream>
#include <vector>
#include <system/Exceptions.h>

namespace scidb
{
namespace dist
{
/// A simple stream de-serializer for a vector of integers.
/// The integers are expected to be comma separated.
template <typename Int>
void readInts(std::istream& sin, std::vector<Int>& nums)
{
    const size_t origSize = nums.size();
    while(sin.good()) {

        Int n(~0);
        char comma('\0');
        sin >> n;

        if (sin.fail()) {

            if (sin.eof() && (nums.size()>origSize)) {
                break;
            }
            ASSERT_EXCEPTION_FALSE("readInts: conversion from CSV failed: IO error");
        }
        nums.push_back(n);

        if (sin.eof()) {
            break;
        }

        sin >> comma;

        if (sin.bad()) {
            ASSERT_EXCEPTION_FALSE("readInts: conversion from CSV failed: IO error");
        }
        if (!sin.fail()) {
            ASSERT_EXCEPTION( (comma ==','),
                              "readInts: conversion from CSV failed: expected comma");
        }
    }
    if (!sin.eof()) {
        ASSERT_EXCEPTION_FALSE("readInts: conversion from CSV failed: IO error");
    }
    ASSERT_EXCEPTION(origSize < nums.size(),
                     "readInts: conversion from CSV failed: no data");
}


/// A simple stream serializer for a vector of integers.
/// The integers are written to a stream separated by commas.
template <typename Int>
void writeInts(std::ostream& sout, const std::vector<Int>& nums)
{
    for(typename std::vector<Int>::const_iterator i = nums.begin();
        i < nums.end(); ++i) {
        sout << *i << ',';
    }
    ASSERT_EXCEPTION((!sout.fail()), "writeInts: conversion to CSV failed");
}
}  // namespace dist
} // namespace scidb

#endif // ARRAY_DISTRIBUTION_UTILS_H_
