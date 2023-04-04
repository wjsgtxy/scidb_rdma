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
 * @file ConstArrayIterator.cpp
 */

#include <array/ConstArrayIterator.h>

#include <array/Array.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <util/Utility.h>

#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstArrayIter"));
}

namespace scidb
{
    Array const& ConstArrayIterator::getArray() const
    {
        SCIDB_ASSERT(_array.isAlive());
        return _array;
    }

    bool ConstArrayIterator::setPosition(Coordinates const& pos)
    {
        stringstream ss;
        ss << "ConstArrayIterator::setPosition(" << CoordsToStr(pos)
           << ") not implemented by " << demangle_cpp(typeid(*this).name());
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED)
           << ss.str();
        SCIDB_UNREACHABLE();
    }

    void ConstArrayIterator::restart()
    {
        stringstream ss;
        ss << "ConstArrayIterator::restart() not implemented by "
           << demangle_cpp(typeid(*this).name());
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_NOT_IMPLEMENTED)
            << ss.str();
    }
}
