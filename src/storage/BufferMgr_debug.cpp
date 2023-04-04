/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2019 SciDB, Inc.
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
 * @file BufferMgr_debug.cpp
 *
 * @brief Implementation of buffer manager classes.
 */

#include <storage/BufferMgr.h>
#include <system/Config.h>
#include <system/Constants.h>
#include <array/RLE.h>
#include <util/compression/Compressor.h>

namespace scidb {

//
// ostream friend overloads
//
std::ostream& operator<<(std::ostream& os, BufferMgr::BufferKey const& bk)
{
    os << "{\"dsk\":" << bk.getDsk().toString()
       << ", \"off\":" << bk.getOffset()
       << ", \"csz\":" << bk.getCompressedSize()
       << ", \"sz\":" << bk.getSize()
       << ", \"asz\":" << bk.getAllocSize()
       << '}';
    return os;
}

std::ostream& operator<<(std::ostream& os, BufferMgr::BufferHandle const& bh)
{
    if (bh.isNull()) {
        os << "{null";
    } else {
        os << "{\"off\":" << bh.offset()
           << ", \"csz\":" << bh.compressedSize()
           << ", \"sz\":" << bh.size()
           << ", \"asz\":" << bh.allocSize();
    }
    os << ", \"slot\":" << bh.getSlot()
       << ", \"gen\":" << bh.getGenCount()
       << ", \"key\":" << bh.getKey()
       << ", \"cType\": " << static_cast<int16_t>(bh.getCompressorType())
       << '}';
    return os;
}

std::string BufferMgr::BlockHeader::toString() const
{
    std::ostringstream os;
    os << "{"
       << " \"priority\": " << priority
       << " , \"dirty\": " << _dirty
       << " , \"pending\": " << pending
       << " , \"wp\": " << wp
       << " , \"pinCount\": " << pinCount
       << " , \"dsk\": " << dsk.toString()
       << " , \"offset\": " << offset
       << " , \"size\": " << size
       << " , \"compressedSize\": " << compressedSize
       << " , \"compressorType\": " << static_cast<uint32_t>(compressorType)
       << " , \"allocSize\": " << allocSize
       << " , \"next\": \"0x" << std::hex << _next << "\""
       << " , \"prev\": \"0x" << std::hex << _prevDirtyOrClean << "\""
       << " , \"genCount\": " << std::dec << genCount
       << " , \"blockBase\": \"0x" << std::hex << static_cast<void*>(blockBase) << "\""
       << " , \"compressedBlockBase\": \"0x" << std::hex << static_cast<void*>(compressedBlockBase) << "\""
       << " }";
    return os.str();
}

}  // namespace scidb
