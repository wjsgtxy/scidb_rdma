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
 * @file ConstItemIterator.cpp
 *
 * @brief class ConstItemIterator
 *
 */
#include <array/ConstItemIterator.h>

#include <log4cxx/logger.h>

#include <array/Array.h>
#include <array/ArrayDesc.h>
#include <array/ConstArrayIterator.h>
#include <array/ConstChunk.h>

#include <system/Exceptions.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.array.ConstItemIterator"));

    int ConstItemIterator::getMode() const
    {
        return iterationMode;
    }

    Value const& ConstItemIterator::getItem()
    {
        return chunkIterator->getItem();
    }

    bool ConstItemIterator::isEmpty() const
    {
        return chunkIterator->isEmpty();
    }

    ConstChunk const& ConstItemIterator::getChunk()
    {
        return chunkIterator->getChunk();
    }

    bool ConstItemIterator::end()
    {
        return !chunkIterator || chunkIterator->end();
    }

    void ConstItemIterator::operator ++()
    {
        ++(*chunkIterator);
        while (chunkIterator->end()) {
            chunkIterator.reset();
            ++(*arrayIterator);
            if (arrayIterator->end()) {
                return;
            }
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    Coordinates const& ConstItemIterator::getPosition()
    {
        return chunkIterator->getPosition();
    }

    bool ConstItemIterator::setPosition(Coordinates const& pos)
    {
        if (!chunkIterator || !chunkIterator->setPosition(pos)) {
            chunkIterator.reset();
            if (arrayIterator->setPosition(pos)) {
                chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
                return chunkIterator->setPosition(pos);
            }
            return false;
        }
        return true;
    }

    void ConstItemIterator::restart()
    {
        chunkIterator.reset();
        arrayIterator->restart();
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(iterationMode);
        }
    }

    ConstItemIterator::ConstItemIterator(Array const& array, const AttributeDesc& attrID, int mode)
    : arrayIterator(array.getConstIterator(attrID)),
      iterationMode(mode)
    {
        if (!arrayIterator->end()) {
            chunkIterator = arrayIterator->getChunk().getConstIterator(mode);
        }
    }
}
