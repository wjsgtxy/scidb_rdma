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

#ifndef CHUNK_BUFFER_H_
#define CHUNK_BUFFER_H_

#include <array/SharedBuffer.h>
#include <util/Backtrace.h>

#include <memory>

namespace scidb
{

class ConstRLEEmptyBitmap;
class ChunkBuffer : public SharedBuffer
{
    /** true when getConstData has been called */
    mutable bool _hasBeenRead;

    /** will contain a call stack for the last location
        from which getConstData is called */
    mutable CallStack _lastReadFrom;

    /** will contain a call stack for the last location
        from which getWriteData is called */
    mutable CallStack _lastWrittenFrom;

public:
    /** Default constructor
     */
    ChunkBuffer();

    /** Copy constructor
     */
    ChunkBuffer(const ChunkBuffer& other);

    /** Destructor
     */
    virtual ~ChunkBuffer() = default;

    /**
     * @return pointer to binary buffer.
     */
    virtual void* getWriteData() override final;

    /**
     * @return a const pointer to binary buffer.
     * This may not be called on a buffer on which
     * getWriteData has already been called.
     */
    virtual void const* getConstData() const override final;

    /**
     * @return true if there is data available in the buffer
     */
    bool isPresent() const;

    /**
     * Copy the data pointed at by getConstData() into the destination
     * buffer.
     * @param destPtr The pointer to the buffer into which to copy the
     * chunk data.
     * @param bytes the number of bytes from the source chunk to copy,
     * defaults to the result of getSize()
     */
    void cloneConstData(void* destinationPtr, size_t bytes = 0) const;

    /**
     * @param offset into chunk over which to create empty bitmap
     * @return a new instance of a ConstRLEEmptyBitmap constructed from the
     * chunk data pointed at by getConstData() at the given offset
     */
    std::shared_ptr<ConstRLEEmptyBitmap> createEmptyBitmap(size_t offset = 0) const;

protected:
    /**
     * Clears the _hasBeenRead flag.
     */
    void clearReadBit();

    /**
     * Derived classes override this to provide the behavior
     * required to return the read/write pointer via the
     * getWriteData interface.
     * @return pointer to data
     */
    virtual void* getWriteDataImpl() = 0;

    /**
     * Derived classes override this to provide the behavior
     * required to return the read-only pointer via the
     * getWriteData interface.
     * @return pointer to const data
     */
    virtual void const* getConstDataImpl() const = 0;
};

} // namespace
#endif /* CHUNK_BUFFER_H_ */
