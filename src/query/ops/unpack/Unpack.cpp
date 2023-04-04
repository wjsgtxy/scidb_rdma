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
 * @file Unpack.cpp
 * @brief Implementation details for unpack-like operators.
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include "Unpack.h"

#include <array/ConstArrayIterator.h>
#include <array/ConstChunk.h>
#include <array/MemArray.h>
#include <array/MemoryBuffer.h>
#include <network/Network.h>
#include <query/PhysicalBoundaries.h>
#include <query/Query.h>

using namespace std;

namespace scidb { namespace unpack {

    Coordinate ArrayInfo::getOutputPos (Coordinates const& inputChunkPos) const
    {
        SCIDB_ASSERT(inputChunkPos.size() == _nDims);
        ChunkAddress addr;
        addr.inputChunkPos = inputChunkPos;
        Super::const_iterator iter = Super::lower_bound(addr);
        if (iter == end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "Can't find coordinates " << CoordsToStr(inputChunkPos) << " in set";
        }

        Coordinates const& iterPos = iter->inputChunkPos;
        for (size_t i=0; i<_nDims; i++)
        {
            if(iterPos[i] != inputChunkPos[i])
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                    << "Can't find coordinates " << CoordsToStr(inputChunkPos) << " in set";
            }
        }
        return iter->outputPos;
    }

    size_t ArrayInfo::getBinarySize() const
    {
        return ChunkAddress::marshalledSize(_nDims) * Super::size() + sizeof(size_t);
    }

    void ArrayInfo::marshall(void* buf) const
    {
        size_t *sPtr = reinterpret_cast<size_t *> (buf);
        *sPtr = Super::size();
        ++sPtr;
        void *res = reinterpret_cast<char *>(sPtr);
        for(Super::iterator i = Super::begin(); i!=Super::end(); ++i)
        {
            res = i->marshall(res, _nDims);
        }
        SCIDB_ASSERT(res == static_cast<uint8_t*>(buf) + getBinarySize());
    }

    void ArrayInfo::unMarshall(void const* buf)
    {
        size_t const* sPtr = reinterpret_cast<size_t const*>(buf);
        size_t numEntries = *sPtr;
        ++sPtr;
        void const* res = reinterpret_cast<void const *>(sPtr);
        for(size_t k =0; k<numEntries; k++)
        {
            ChunkAddress addr;
            res = addr.unMarshall(res, _nDims);
            Super::iterator i = Super::find(addr);
            if( i == Super::end())
            {
                Super::insert(addr);
            }
            else
            {
                //Don't call me with partially filled chunks, buddy :)
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                    << "Unpack chunk info encountered the same chunk multiple times";
            }
        }
    }


    bool outputFullChunks(ArrayDesc const& inSchema, ArrayDesc const& outSchema)
    {
        if (inSchema.getEmptyBitmapAttribute())
        {   //input is emptyable - all bets are off
            return false;
        }
        Coordinate inputChunkSize = 1;
        for (size_t i=0, j=inSchema.getDimensions().size(); i<j; i++)
        {
            inputChunkSize *= inSchema.getDimensions()[i].getChunkInterval();
        }
        if (inputChunkSize == outSchema.getDimensions()[0].getChunkInterval())
        {
            return true;
        }
        return false;
    }


    PhysicalBoundaries getOutputBoundaries(PhysicalBoundaries const& inBounds,
                                           ArrayDesc const& outSchema)
    {
        // We don't use PhysicalBounadries::reshape() here because it doesn't play well with
        // "unbounded" dimensions, see SDB-5688 and SDB-5689.  Instead we hand-craft the 1-D
        // result boundaries.

        Coordinates start = { outSchema.getDimensions()[0].getCurrStart() };
        Coordinate nCells = static_cast<Coordinate>(
            ::min(inBounds.getNumCells(),
                  static_cast<uint64_t>(std::numeric_limits<Coordinate>::max())));
        Coordinates end(start);
        if (CoordinateBounds::getMax() - nCells < start[0]) {
            end[0] = CoordinateBounds::getMax();  // would overflow, cap it
        } else {
            end[0] += nCells;
        }
        return PhysicalBoundaries(start, end);
    }


    void collectChunkInfo(std::shared_ptr<Array> const& inputArray, ArrayInfo& info)
    {
        ArrayDesc const& desc = inputArray->getArrayDesc();
        const AttributeDesc* victimAttribute = nullptr;
        if (desc.getEmptyBitmapAttribute())
        {
            victimAttribute = desc.getEmptyBitmapAttribute();
        }
        else
        {
            auto minAttrSize = static_cast<size_t>(-1);
            for (const auto& attr : desc.getAttributes())
            {
                if (attr.getSize() > 0 && attr.getSize() < minAttrSize)
                {
                    minAttrSize = attr.getSize();
                    victimAttribute = &attr;
                }
            }
        }
        assert(victimAttribute);

        std::shared_ptr<ConstArrayIterator> iter = inputArray->getConstIterator(*victimAttribute);
        while (!iter->end())
        {
            ChunkAddress addr;
            addr.inputChunkPos = iter->getPosition();
            ConstChunk const& chunk = iter->getChunk();
            addr.elementCount = chunk.count();
            addr.outputPos = 0;
            info.insert(addr);
            ++(*iter);
        }
    }


    void exchangeChunkInfo(ArrayInfo& info, std::shared_ptr<Query>& query)
    {
        const size_t nInstances = query->getInstancesCount();
        if (! query->isCoordinator())
        {
            std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, info.getBinarySize()));
            info.marshall(reinterpret_cast<void*>(buf->getWriteData()));
            info.clear();
            BufSend(query->getCoordinatorID(), buf, query);
            buf = BufReceive(query->getCoordinatorID(), query);
            info.unMarshall(buf->getConstData());
        }
        else
        {
            InstanceID myId = query->getInstanceID();
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    std::shared_ptr<SharedBuffer> buf = BufReceive(i,query);
                    info.unMarshall(buf->getConstData());
                }
            }
            std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, info.getBinarySize()));
            info.marshall(reinterpret_cast<void*>(buf->getWriteData()));
            for(InstanceID i=0; i<nInstances; i++)
            {
                if(i != myId)
                {
                    BufSend(i, buf, query);
                }
            }
        }
    }


    void computeGlobalChunkInfo(std::shared_ptr<Array> const& inputArray,
                                std::shared_ptr<Query>& query,
                                ArrayInfo& info)
    {
        collectChunkInfo(inputArray, info);
        exchangeChunkInfo(info, query);
        Coordinate startingPosition = 0;
        for (auto& chunkAddr : info)
        {
            chunkAddr.outputPos = startingPosition;
            startingPosition += chunkAddr.elementCount;
        }
    }

    std::shared_ptr<Array> fillOutputArray(std::shared_ptr<Array> const& inputArray,
                                           ArrayDesc const& outSchema,
                                           ArrayInfo const& chunkInfo,
                                           std::shared_ptr<Query> &query)
    {
        std::shared_ptr<Array> result = std::make_shared<MemArray>(outSchema, query);
        ArrayDesc const& inputSchema = inputArray->getArrayDesc();
        size_t nSrcDims = inputSchema.getDimensions().size();
        bool isDataframe = inputSchema.isDataframe();
        int64_t const OUTPUT_CHUNK_SIZE = outSchema.getDimensions()[0].getChunkInterval();

        // Remember that (for non-dataframes only!) the first attributes of dst are dimensions from src.
        size_t startOfAttributes = isDataframe ? 0 : inputSchema.getDimensions().size();

        Coordinates outputChunkPos(0);
        Coordinates outputCellPos(1,0);

        //source array and chunk iters
        vector<std::shared_ptr<ConstArrayIterator> > saiters (inputSchema.getAttributes(true).size());
        vector<std::shared_ptr<ConstChunkIterator> > sciters (inputSchema.getAttributes(true).size());

        //destination array and chunk iters
        vector<std::shared_ptr<ArrayIterator> > daiters (outSchema.getAttributes(true).size());
        vector<std::shared_ptr<ChunkIterator> > dciters (outSchema.getAttributes(true).size());

        for (const auto& srcAttr : inputSchema.getAttributes(true))
        {
            saiters[srcAttr.getId()] = inputArray->getConstIterator(srcAttr);
        }
        for (const auto& destAttr : outSchema.getAttributes(true))
        {
            daiters[destAttr.getId()] = result->getIterator(destAttr);
        }

        Value buf;
        const auto& ifda = inputSchema.getAttributes(true).firstDataAttribute();
        while (!saiters[ifda.getId()]->end())
        {
            Coordinates const& chunkPos = saiters[0]->getPosition();
            outputCellPos[0] = chunkInfo.getOutputPos(chunkPos);
            for (const auto& srcAttr : inputSchema.getAttributes(true))
            {
                sciters[srcAttr.getId()] = saiters[srcAttr.getId()]->getChunk().getConstIterator();
            }
            while(!sciters[ifda.getId()]->end())
            {
                //can't go backwards!
                SCIDB_ASSERT(outputChunkPos.size() == 0 || outputCellPos[0] >= outputChunkPos[0] );
                if(outputChunkPos.size() == 0 || outputCellPos[0] > (outputChunkPos[0] + OUTPUT_CHUNK_SIZE) - 1)
                {
                    // First time, or we've crossed an output chunk boundary.
                    // Flush old iterators and get new ones!

                    for (const auto& destAttr : outSchema.getAttributes(true)) {
                        if (dciters[destAttr.getId()]) {
                            dciters[destAttr.getId()]->flush();
                            dciters[destAttr.getId()].reset();
                        }
                    }

                    outputChunkPos = outputCellPos;
                    outSchema.getChunkPositionFor(outputChunkPos);
                    int chunk_mode = ChunkIterator::SEQUENTIAL_WRITE;
                    for (const auto& destAttr : outSchema.getAttributes(true))
                    {
                        Chunk& outChunk = daiters[destAttr.getId()]->newChunk(outputChunkPos);
                        dciters[destAttr.getId()] = outChunk.getIterator(query, chunk_mode);
                        chunk_mode |= ChunkIterator::NO_EMPTY_CHECK;
                    }
                }
                Coordinates const& inputCellPos = sciters[0]->getPosition();
                if (!isDataframe) {
                    for(size_t i=0; i<nSrcDims; i++)
                    {
                        if (dciters[i]) {
                            dciters[i]->setPosition(outputCellPos);
                            buf.setInt64(inputCellPos[i]);
                            dciters[i]->writeItem(buf);
                        }
                    }
                }
                for (const auto& srcAttr : inputSchema.getAttributes(true))
                {
                    dciters[srcAttr.getId()+startOfAttributes]->setPosition(outputCellPos);
                    dciters[srcAttr.getId()+startOfAttributes]->writeItem(sciters[srcAttr.getId()]->getItem());
                }
                outputCellPos[0]++;

                for (const auto& srcAttr : inputSchema.getAttributes(true)) {
                    ++(*sciters[srcAttr.getId()]);
                }
            }

            for (const auto& srcAttr : inputSchema.getAttributes(true)) {
                ++(*saiters[srcAttr.getId()]);
            }
        }

        for (const auto& destAttr : outSchema.getAttributes(true)) {
            if (dciters[destAttr.getId()]) {
                dciters[destAttr.getId()]->flush();
                dciters[destAttr.getId()].reset();
            }
        }

        return result;
    }

std::ostream& operator<<(std::ostream& stream, const unpack::ArrayInfo& info)
{
    for (auto const& chunkAddr : info)
    {
        stream << CoordsToStr(chunkAddr.inputChunkPos)
               << ',' << chunkAddr.elementCount
               << ',' << chunkAddr.outputPos
               << ' ';
    }
    return stream;
}

} // unpack

}  // scidb
