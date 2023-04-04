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
#ifndef FITS_INPUT_ARRAY_H
#define FITS_INPUT_ARRAY_H

#include "array/Array.h"
#include "array/MemArray.h"

#include "../common/FITSParser.h"


namespace scidb
{

class FITSInputArrayIterator;

/* FITSInputArray */

class FITSInputArray : public Array
{
public:
    FITSInputArray(ArrayDesc const& desc, std::string const& filePath, uint32_t hdu, std::shared_ptr<Query>& query);

    virtual ArrayDesc const&                        getArrayDesc() const;
    std::shared_ptr<ConstArrayIterator>   getConstIteratorImpl(const AttributeDesc& attr) const override;

    /**
     * Get the least restrictive access mode that the array supports.
     * @return SINGLE_PASS
     */
    virtual Access getSupportedAccess() const
    {
        return SINGLE_PASS;
    }

    ConstChunk*                                     getChunkByIndex(size_t index, AttributeID attr);

private:
    // A window is used to keep the last 'kWindowSize' chunks in memory.
    // Its size must be at least 2 because different attribute iterators
    // may be simultaneously requesting chunk N as well as chunk (N - 1).
    static const size_t                             kWindowSize = 2;

    struct CachedChunks {
        MemChunk chunks[kWindowSize];
    };

    void                                            initValueHolders();

    bool                                            validSchema();
    bool                                            validDimensions();

    void                                            initChunkPos();
    bool                                            advanceChunkPos();

    void                                            calculateLength();
    void                                            readChunk();
    void                                            initMemChunks(std::shared_ptr<Query>& query);
    void                                            flushMemChunks();

    void                                            readShortInts(size_t n);
    void                                            readShortIntsAndScale(size_t n);
    void                                            readInts(size_t n);
    void                                            readIntsAndScale(size_t n);
    void                                            readFloats(size_t n);

    FITSParser                                      parser;
    uint32_t                                        hdu;
    ArrayDesc                                       desc;
    Dimensions const&                               dims;
    size_t                                          nDims;
    const size_t                                          nAttrs;
    std::vector<Value>                                   values;
    std::vector<CachedChunks>                            chunks;
    std::vector<std::shared_ptr<ChunkIterator> >       chunkIterators;
    size_t                                          chunkIndex;
    Coordinates                                     chunkPos;
    size_t                                          nConsecutive;
    size_t                                          nOuter;
    std::weak_ptr<Query>                          query;
};

/* FITSInputArrayIterator */

class FITSInputArrayIterator : public ConstArrayIterator
{
public:
    FITSInputArrayIterator(FITSInputArray& array, AttributeID attr);

    bool                                    end() override;
    void                                    operator ++() override;
    Coordinates const&                      getPosition() override;
    bool                                    setPosition(Coordinates const& pos) override;
    void                                    restart() override;
    ConstChunk const&                       getChunk() override;

private:
    FITSInputArray&                                 array;
    AttributeID                                     attr;
    ConstChunk const*                               chunk;
    size_t                                          chunkIndex;
    bool                                            chunkRead;
};

}

#endif /* FITS_INPUT_ARRAY_H */
