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

#ifndef UNPIN_ALL_CHUNKS_H_
#define UNPIN_ALL_CHUNKS_H_

#include <log4cxx/logger.h>

namespace scidb
{
    template <typename ContainerT>
    void unpinAllChunks(const ContainerT& chunkIterators,
                        log4cxx::LoggerPtr logger)
    {
        assert(logger);

        for (const auto& chunkIter : chunkIterators) {
            try {
                if (chunkIter) {
                    chunkIter->unpin();
                }
            }
            catch (const std::exception& e) {
                auto what = e.what();
                LOG4CXX_WARN(logger, "Error unpinning chunk: " << what);
            }
            catch (...) {
                LOG4CXX_WARN(logger, "Unknown error unpinning chunk");
            }
        }
    }
}  // namespace scidb

#endif  // UNPIN_ALL_CHUNKS_H_
