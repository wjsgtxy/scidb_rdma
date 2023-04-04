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
 * @file ConstChunkIterator.cpp
 *
 * @brief class ConstChunkIterator
 */

#include <array/ConstChunkIterator.h>

#include <log4cxx/logger.h>

#include <array/ConstChunk.h>
#include <system/Exceptions.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstChunkIterator"));

    Coordinates const& ConstChunkIterator::getFirstPosition()
    {
        return getChunk().getFirstPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

    Coordinates const& ConstChunkIterator::getLastPosition()
    {
        return getChunk().getLastPosition((getMode() & IGNORE_OVERLAPS) == 0);
    }

        bool ConstChunkIterator::forward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& last = getLastPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (++pos[i] > last[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }

        bool ConstChunkIterator::backward(uint64_t direction)
    {
        Coordinates pos = getPosition();
        Coordinates const& first = getFirstPosition();
        do {
            for (size_t i = 0; direction != 0; i++, direction >>= 1) {
                if (direction & 1) {
                    if (--pos[i] < first[i]) {
                        return false;
                    }
                }
            }
        } while (!setPosition(pos));
        return true;
    }
}
