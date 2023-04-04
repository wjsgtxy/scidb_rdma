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
 * @file AllocationBuffer.cpp
 *
 * @brief class AllocationBuffer
 */

#include <array/AllocationBuffer.h>

#include <log4cxx/logger.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.AllocationBuffer"));

    AllocationBuffer::AllocationBuffer(const arena::ArenaPtr& arena)
                    : _arena(arena),
                      _data(0),
                      _size(0)
    {}

    AllocationBuffer::~AllocationBuffer()
    {
        this->free();
    }

    const void* AllocationBuffer::getConstData() const
    {
        return _data;
    }

    void* AllocationBuffer::getWriteData()
    {
        return _data;
    }

    size_t AllocationBuffer::getSize() const
    {
        return _size;
    }

    bool AllocationBuffer::pin() const
    {
        return false;
    }

    void AllocationBuffer::unPin() const
    {}

    void AllocationBuffer::allocate(size_t n)
    {
        _data = _arena->allocate(_size = n);
    }

    void AllocationBuffer::free()
    {
        _arena->recycle(_data);
        _data = 0;
    }
}
