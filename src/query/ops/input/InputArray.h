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
 * InputArray.h
 *
 *  Created on: Sep 23, 2010
 */
#ifndef INPUT_ARRAY_H
#define INPUT_ARRAY_H

#include <memory>
#include <string>

#include <log4cxx/logger.h>

#include <array/ArrayDistributionInterface.h>
#include <array/SinglePassArray.h>

namespace scidb
{
    class ChunkLoader;
    class PhysicalOperator;

    class InputArray : public SinglePassArray
    {
        friend class ChunkLoader;

      public:

        static log4cxx::LoggerPtr s_logger;

        /// Constructor
        InputArray(ArrayDesc const& desc,
                   ArrayDistPtr const& distribForCoordSequence,
                   std::string const& format,
                   std::shared_ptr<Query>& query,
                   const std::shared_ptr<PhysicalOperator>& phyOp,
                   bool emptyMode,
                   bool enforceUniqueness,
                   int64_t maxCnvErrors,
                   bool parallelLoad);
        /// Destructor
        virtual ~InputArray();

        void openFile(std::string const& fileName);
        void openString(std::string const& dataString);

        bool inEmptyMode() const { return state == S_Empty; }

        /// Upcalls from the _chunkLoader
        /// @{
        void handleError(Exception const& x,
                         std::shared_ptr<ChunkIterator> cIter,
                         AttributeID i);
        void countCell() { ++nLoadedCells; lastBadAttr = -1; }
        /// @}

        /// @see Array::hasInputPipe
        bool hasInputPipe() const override { return false; }

        /// @returns true iff the named format is supported
        static bool isSupportedFormat(std::string const& format);

    protected:
        /// @see SinglePass::getCurrentRowIndex()
        virtual size_t getCurrentRowIndex() const { return _currChunkIndex; }
        /// @see SinglePass::moveNext()
        virtual bool moveNext(size_t rowIndex);
        /// @see SinglePass::getChunk()
        virtual ConstChunk const& getChunk(AttributeID attr, size_t rowIndex);

    private:
        std::weak_ptr<PhysicalOperator> _phyOp;
        ChunkLoader*    _chunkLoader;
        size_t          _currChunkIndex;

        Value strVal;
        AttributeID emptyTagAttrID;
        uint64_t nLoadedCells;
        uint64_t nLoadedChunks;
        size_t nErrors;
        size_t maxErrors;
        enum State
        {
            S_Normal,           // Chunks expected/permitted
            S_Empty,            // No chunks ever expected this instance
            S_Done              // No more chunks expected, terminal
        };
        State state;
        MemChunk tmpChunk;
        size_t nAttrs;
        int lastBadAttr;
        InstanceID myInstanceID;
        bool parallelLoad;
        bool _enforceDataIntegrity;
    };

} //namespace scidb

#endif /* INPUT_ARRAY_H */
