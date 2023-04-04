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

#ifndef UNPACK_IMPL_H
#define UNPACK_IMPL_H

/**
 * @file Unpack.h
 * @brief Implementation details for unpack-like operators.
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include <array/Coordinate.h>
#include <system/Utils.h>

namespace scidb {

class Array;
class ArrayDesc;
class PhysicalBoundaries;
class Query;

namespace unpack {

/**
 * A simple marshallable struct combining the coordinates of a chunk, the number of elements the
 * chunk contains and the starting position of the chunk in the output array.
 */
struct ChunkAddress
{
    /**
     * Position of the input chunk.
     */
    Coordinates inputChunkPos;

    /**
     * Number of elements in the chunk.
     */
    size_t elementCount;

    /**
     * The starting position of the chunk in the output 1D array.
     *
     * @note Using mutable because we store these in an std::set<>,
     *       which would normally be forbidden... but it's OK because
     *       this field isn't used by the ChunkAddressLess comparator.
     * @see unpack::ChunkAdressLess
     */
    mutable Coordinate outputPos;

    /**
     * Compute the marshalled size (in bytes) of any unpack::ChunkAddress for a given
     * number of dimensions.
     * @param[in] ndDims the number of dimensions in the input array
     */
    inline static size_t marshalledSize(size_t const nDims)
    {
        return sizeof(Coordinate) * (nDims + 1) + sizeof(size_t);
    }

    /**
     * Marshall this into a buffer. The structure will occupy exactly marshalledSize(nDims) bytes.
     * @param[in|out] buf a pointer to memory where to start writing the structure. Better not be
     *                    null and have enough size to hold all data
     * @param[in] nDims the number of dimensions; provided externally for performance
     * @return ((uint8_t *)buf) + marshalledSize(nDims)
     */
    inline void* marshall(void *buf, size_t const nDims) const
    {
        Coordinate *cPtr = reinterpret_cast<Coordinate *>(buf);
        for(size_t i=0; i<nDims; i++)
        {
            *cPtr = inputChunkPos[i];
            ++cPtr;
        }

        size_t *sPtr = reinterpret_cast<size_t *>(cPtr);
        *sPtr = elementCount;
        ++sPtr;

        cPtr = reinterpret_cast<Coordinate *>(sPtr);
        *cPtr = outputPos;
        ++cPtr;

        void *res = reinterpret_cast<void *> (cPtr);
        SCIDB_ASSERT(res == static_cast<uint8_t*>(buf) + marshalledSize(nDims));
        return res;
    }

    /**
     * Unmarshall this from a buffer.
     * @param[in] buf a pointer to memory where to read from
     * @param[in] nDims the number of dimensions
     * @return ((uint8_t *)buf) + marshalledSize(nDims).
     */
    inline void const* unMarshall(void const* buf, size_t const nDims)
    {
        if (inputChunkPos.size())
        {
            inputChunkPos.clear();
        }

        Coordinate const* cPtr = reinterpret_cast<Coordinate const*>(buf);
        for(size_t i=0; i<nDims; i++)
        {
            inputChunkPos.push_back(*cPtr);
            ++cPtr;
        }

        size_t const* sPtr = reinterpret_cast<size_t const*>(cPtr);
        elementCount = *sPtr;
        ++sPtr;

        cPtr = reinterpret_cast<Coordinate const*>(sPtr);
        outputPos = *cPtr;
        ++cPtr;

        void const* res = reinterpret_cast<void const*> (cPtr);
        SCIDB_ASSERT(res == static_cast<uint8_t const*>(buf) + marshalledSize(nDims));
        return res;
    }
};

/**
 * A comparator for unpack::ChunkAddresses.
 * We use the inputChunkPos coordinates for to keep addresses in sorted order.
 */
struct ChunkAddressLess : public CoordinatesLess
{
    /**
     * Compare two addresses.
     * @return true if c1 is less than c2, false otherwise
     */
    bool operator()(const ChunkAddress& c1, const ChunkAddress& c2) const
    {
        return CoordinatesLess::operator()(c1.inputChunkPos, c2.inputChunkPos);
    }
};

/**
 * A marshallable set of unpack::ChunkAddress-es whereby the starting position of
 * an output chunk can be looked up.
 */
class ArrayInfo : public std::set <ChunkAddress, ChunkAddressLess>
{
private:
    /**
     * Number of dimensions in the input array.
     */
    size_t _nDims;

public:
    using Super = std::set<ChunkAddress, ChunkAddressLess>;

    /**
     * Create an empty info.
     * @param[in] nDims the number of dimensions in the input array
     */
    ArrayInfo(size_t const nDims):
        Super(),
        _nDims(nDims)
    {}

    /**
     * Given the position of the chunk in the input array - determine the position
     * of the starting element in this chunk in the output array. Throws if there is no
     * info for this chunk in this.
     * @param inputChunkPos the coordinates of the input chunk
     * @return the position of the first element of this chunk in the output array
     */
    Coordinate getOutputPos (Coordinates const& inputChunkPos) const;

    /**
     * Compute the marshalled size of the entire structure.
     * @return the number of bytes needed to marshall this
     */
    size_t getBinarySize() const;

    /**
     * Write all the data into a preallocated buffer.
     * @param[in|out] buf a pointer to allocated memory; must be at least getBinarySize() bytes
     */
    void marshall(void* buf) const;

    /**
     * Read marshalled data from the buffer and add it to this.
     * @param[in] buf a pointer to memory that contains a marshalled unpack::ArrayInfo
     */
    void unMarshall(void const* buf);
};


/**
 * Determine whether the operator outputs full chunks.
 * @param inSchema the shape of the input array
 * @param outSchema the shape of the output array
 * @return true if input is not emptyable and input chunk size matches output;
 *         false otherwise
 */
bool outputFullChunks(ArrayDesc const& inSchema, ArrayDesc const& outSchema);


/**
 * Compute the boundaries of the output array.
 * @return the input boundaries reshaped around a single dimension. Often an over-estimate.
 */
PhysicalBoundaries getOutputBoundaries(PhysicalBoundaries const& inBounds,
                                       ArrayDesc const& outSchema);

/**
 * Perform a single pass over some attribute of the inputArray and populate info with data about
 * the array.
 * @param[in] inputArray the array to iterate over
 * @param[out] info the structure to populate
 */
void collectChunkInfo(std::shared_ptr<Array> const& inputArray,
                      ArrayInfo& info);

/**
 * Send info to the coordinator; merge all sent data at coordinator and send
 * data back to all instances; rebuild info with data from all instances.
 * @param[in|out] info the structure to populate
 * @param[in] query the query context
 */
void exchangeChunkInfo(ArrayInfo& info, std::shared_ptr<Query>& query);

/**
 * Build a unpack::ArrayInfo from the local array, then exchange data with other nodes, then
 * compute the starting positions for each of the chunks.
 * @param[in] inputArray the array to scan
 * @param[in] query the query context
 * @param[out] info the data to collect
 */
void computeGlobalChunkInfo(std::shared_ptr<Array> const& inputArray,
                            std::shared_ptr<Query>& query,
                            ArrayInfo& info);

/**
 * Given an input array and an unpack::ArrayInfo - create an outputArray by opening each chunk of
 * the input, looking up the corresponding position for the data in the output array and
 * appending it.
 * @param[in] inputArray the array to take data from
 * @param[in] outSchema the output schema
 * @param[in] chunkInfo the information about where to place which chunks
 * @param[in] query the query context
 * @return the output MemArray with partially filled chunks wherein all the data elements are at
 *         the right place
 */
std::shared_ptr<Array> fillOutputArray(std::shared_ptr<Array> const& inputArray,
                                       ArrayDesc const& outSchema,
                                       ArrayInfo const& chunkInfo,
                                       std::shared_ptr<Query> &query);

/**
 * Print an unpack::ArrayInfo into a text stream. Used for logging.
 * @param os the output stream
 * @param info the data to print
 * @return the stream with the output added
 *
 * @note For argument-dependent-lookup reasons, since ArrayInfo is-a
 * std::set<>, you must add (the equivalent of) the following snippet
 * anywhere you wish to call this overload:
 * @code
 * namespace scidb { using unpack::operator<<; }
 * @endcode
 */
std::ostream& operator<<(std::ostream& os, const ArrayInfo& info);

} // unpack

}  // scidb

#endif /* ! UNPACK_IMPL_H */
