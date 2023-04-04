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
 * InputArray.cpp
 *
 *  Created on: Sep 23, 2010
 */

// The declarations that are implemented in this file.
#include "InputArray.h"

// other includes from this module
#include "ChunkLoader.h"

// other scidb modules
#include <query/PhysicalUpdate.h>
#include <query/Query.h>


namespace scidb
{
    using namespace std;

    log4cxx::LoggerPtr InputArray::s_logger(log4cxx::Logger::getLogger("scidb.qproc.ops.inputarray"));
    static log4cxx::LoggerPtr& logger(InputArray::s_logger);

InputArray::InputArray(ArrayDesc const& array,
                       ArrayDistPtr const& distribForNextChunk,
                       string const& format,
                       std::shared_ptr<Query>& query,
                       const std::shared_ptr<PhysicalOperator>& phyOp,
                       bool emptyMode,
                       bool enforceDataIntegrity,
                       int64_t maxCnvErrors,
                       bool parallel)
:     SinglePassArray(array),
      _phyOp(phyOp),
      _chunkLoader(ChunkLoader::create(format)),
      _currChunkIndex(0),
      strVal(TypeLibrary::getType(TID_STRING)),
      emptyTagAttrID(array.getEmptyBitmapAttribute() != NULL
                     ? array.getEmptyBitmapAttribute()->getId()
                     : INVALID_ATTRIBUTE_ID),
      nLoadedCells(0),
      nLoadedChunks(0),
      nErrors(0),
      maxErrors(maxCnvErrors),
      state(emptyMode ? S_Empty : S_Normal),
      nAttrs(array.getAttributes(true).size()),
      parallelLoad(parallel),
      _enforceDataIntegrity(enforceDataIntegrity)
    {
        SCIDB_ASSERT(query);
        _query=query;

        myInstanceID = query->getInstanceID();

        SCIDB_ASSERT(_chunkLoader);   // else inferSchema() messed up
        LOG4CXX_TRACE(logger, "InputArray::InputArray() DistType: "
                      << array.getDistribution()->getDistType());

        _chunkLoader->bindArray(this, distribForNextChunk, query);

        // only after the bind() call the schema can be adjusted
        // (to match PhysicalInput::getOutputDistribution())
        // because some chunk loaders use the distribution specified by the target array
        // See LogicalInput
        // NOTE: separate distribForNextChunk gives actual distribution of the data from the file when the
        // schema in base SinglePassArray is set to dtUndefined (when forcing SGs)

        // NOTE: whenever the schema is not dtLocalInstance, this code sets the distribution right back to dtUndefined.
        //  a. this deserves another printout
        //  b. (TODO) managing this more cleanly is a worthy goal
        //  c. schema 'desc' is a protected member of base SinglePassArray's base StreamArray
        if (not isLocal(getArrayDesc().getDistribution()->getDistType())) {
            desc.setDistribution(createDistribution(dtUndefined));  // where is desc?
            LOG4CXX_WARN(logger, "InputArray::InputArray() forcing desc to dtUndefined");
        }
    }

    bool InputArray::isSupportedFormat(string const& format)
    {
        std::unique_ptr<ChunkLoader> cLoader(ChunkLoader::create(format));
        return cLoader.get() != 0;
    }

    void InputArray::openFile(std::string const& fileName)
    {
        SCIDB_ASSERT(_chunkLoader);
        SCIDB_ASSERT(state != S_Empty); // Don't tell empty-mode InputArrays to be opening stuff!

        int rc = _chunkLoader->openFile(fileName);
        if (rc != 0) {
            LOG4CXX_WARN(logger, "Failed to open file " << fileName <<
                         " for input: " << ::strerror(rc) << " (" << rc << ')');
            state = S_Empty;
            if (!parallelLoad) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE)
                    << fileName << ::strerror(rc) << rc;
            }
        }
    }

    void InputArray::openString(std::string const& dataString)
    {
        SCIDB_ASSERT(_chunkLoader);
        _chunkLoader->openString(dataString);
    }

    void InputArray::handleError(Exception const& x,
                                 std::shared_ptr<ChunkIterator> cIter,
                                 AttributeID i)
    {
        SCIDB_ASSERT(_chunkLoader);
        string const& msg = x.getErrorMessage();
        Attributes const& attrs = desc.getAttributes();
        LOG4CXX_ERROR(logger, "Failed to convert attribute " << attrs.findattr(i).getName()
                      << " at position " << _chunkLoader->getFileOffset()
                      << " line " << _chunkLoader->getLine()
                      << " column " << _chunkLoader->getColumn() << ": " << msg);

        if (++nErrors > maxErrors) {
            if (maxErrors) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
            } else {
                // If no maxErrors limit was set, show the original error.
                x.raise();
            }
        }

        Value errVal;
        if (attrs.findattr(i).isNullable()) {
            errVal.setNull();
        } else {
            errVal.setSize<Value::IGNORE_DATA>(TypeLibrary::getType(attrs.findattr(i).getType()).byteSize());
            errVal = TypeLibrary::getDefaultValue(attrs.findattr(i).getType());
        }
        cIter->writeItem(errVal);
    }

    bool InputArray::moveNext(size_t chunkIndex)
    {
        bool more = false;
        LOG4CXX_TRACE(logger, "InputArray::moveNext: chunkIndex= " << chunkIndex);
        try {

            if (chunkIndex > _currChunkIndex+1) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR1);
            }
            std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            if (chunkIndex <= _currChunkIndex) {
                return true;
            }
            if (state == S_Empty) {
                state = S_Done;
                return false;
            }
            if (state == S_Done) {
                return false;
            }

            assert(state == S_Normal);
            more = _chunkLoader->loadChunk(query, chunkIndex);
            if (more) {
                nLoadedChunks += 1;
                LOG4CXX_TRACE(logger, "Loading of " << desc.getName()
                              << " is in progress: load at this moment " << nLoadedChunks
                              << " chunks and " << nLoadedCells
                              << " cells with " << nErrors << " errors");

                _currChunkIndex += 1;
            } else {
                state = S_Done;
            }

            LOG4CXX_TRACE(logger, "Finished scan of chunk number " << _currChunkIndex << ", more=" << more);
        }
        catch(Exception const& x)
        {
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << _chunkLoader->filePath()
                << myInstanceID
                << getName()
                << _chunkLoader->getLine()
                << _chunkLoader->getColumn()
                << _chunkLoader->getFileOffset()
                << debugEncode(_chunkLoader->getBadField())
                << x.getErrorMessage();
        }
        catch(std::exception const& x)
        {
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << _chunkLoader->filePath()
                << myInstanceID
                << getName()
                << _chunkLoader->getLine()
                << _chunkLoader->getColumn()
                << _chunkLoader->getFileOffset()
                << debugEncode(_chunkLoader->getBadField())
                << x.what();
        }
        return more;
    }

    InputArray::~InputArray()
    {
        delete _chunkLoader;
        LOG4CXX_INFO(logger, "Loading of " << desc.getName()
                     << " is completed: loaded " << nLoadedChunks
                     << " chunks and " << nLoadedCells
                     << " cells with " << nErrors << " errors");
    }


    ConstChunk const& InputArray::getChunk(AttributeID attr, size_t chunkIndex)
    {
        LOG4CXX_TRACE(logger, "InputArray::getChunk: currChunkIndex=" << _currChunkIndex
                      << " attr=" << attr
                      << " chunkIndex=" << chunkIndex);

        Query::getValidQueryPtr(_query);
        if (chunkIndex > _currChunkIndex
            || chunkIndex + ChunkLoader::LOOK_AHEAD <= _currChunkIndex)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR11);
        }
        MemChunk& chunk = _chunkLoader->getLookaheadChunk(attr, chunkIndex);
        if (emptyTagAttrID != attr && emptyTagAttrID != INVALID_ATTRIBUTE_ID) {
            MemChunk& bitmapChunk = _chunkLoader->getLookaheadChunk(emptyTagAttrID, chunkIndex);
            chunk.setBitmapChunk(&bitmapChunk);
        }

        LOG4CXX_TRACE(logger, "InputArray::getChunk: currChunkIndex=" << _currChunkIndex
                      << " attr=" << attr
                      << " chunkIndex=" << chunkIndex
                      << " pos=" << CoordsToStr(chunk.getFirstPosition(false)));

        return chunk;
    }
}
