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
 * @file Array.h
 *
 * @brief the Array class
 *
 * Note: Array.h was split into
 *   8 main classes:
 *     ConstIterator.h ConstArrayIterator.h ArrayIterator.h ConstItemIterator.h
 *     ConstChunk.h Chunk.h
 *     ConstChunkIterator.h ChunkIterator.h
 *   and 5 ancillary classes:
 *     CompressedBuffer.h MemoryBuffer.h SharedBuffer.h
 *     PinBuffer.h UnPinner.h
 * Use "git blame -C" to show the code motion of the splitup.
 */

#ifndef ARRAY_H_
#define ARRAY_H_

#include <set>                          // CoordinateSet

#include <array/ArrayDesc.h>
#include <array/ArrayID.h>
#include <array/AttributeID.h>

#include <array/ConstChunkIterator.h>   // (grr) only due to use of a un unfactored default for getItemIterator()

namespace scidb
{
class Array;
class ArrayIterator;
class AttributeDesc;
class ConstArrayIterator;
class ConstItemIterator;
class Query;

using ArrayPtr = std::shared_ptr<Array>;
using CoordinateSet = std::set<Coordinates, CoordinatesLess>;

/**
 * The array interface provides metadata about the array, including its handle, type and
 * array descriptors.
 * To access the data in the array, a constant (read-only) iterator can be requested, or a
 * volatile iterator can be used.
 */
class Array : public std::enable_shared_from_this<Array>
{
public:

    /**
     * An enum that defines three levels of Array read access policy, ranging from most to least restrictive.
     */
    enum Access
    {
        /**
         * Most restrictive access policy wherein the array can only be iterated over one time.
         * If you need to read multiple attributes, you need to read all of the attributes horizontally, at the same time.
         * Imagine that reading the array is like scanning from a pipe - after a single scan, the data is no longer available.
         * This is the only supported access mode for InputArray and MergeSortArray.
         * Any SINGLE_PASS array must inherit from scidb::SinglePassArray if that array is ever returned from PhysicalOperator::execute().
         * The reason is that the sg() operator can handle only the SINGLE_PASS arrays conforming to the SinglePassArray interface.
         */
        SINGLE_PASS = 0,

        /**
         * A policy wherein the array can be iterated over several times, and various attributes can be scanned independently,
         * but the ArrayIterator::setPosition() function is not supported.
         * This is less restrictive than SINGLE_PASS.
         * This is the least restrictive access mode supported by ConcatArray.
         */
        MULTI_PASS  = 1,

        /**
         * A policy wherein the client of the array can use the full functionality of the API.
         * This is the least restrictive access policy and it's supported by the vast majority of Array subtypes.
         */
        RANDOM      = 2
    };

    Array() = default;
    explicit Array(ArrayPtr const& ap);              // Calls pushPipe() once, see below.
    Array(ArrayPtr const& ap1, ArrayPtr const& ap2); // Calls pushPipe() twice, see below.

    virtual ~Array() { _magic = 0; }

    /**
     *  For paranoid detection of stale references.
     */
    bool isAlive() const { return _magic == MAGIC; }

    /**
     * Get array name
     */
    virtual std::string const& getName() const;

    /**
     * Get class name of most-derived subclass.
     *
     * @details The return value is used by Array::printPipe() for
     * displaying the execution pipeline.  Subclasses can override
     * Array::pipeInfo() to add supplemental debug information.
     *
     * @returns demangled name from typeid(*this).name()
     * @see Array::pipeInfo()
     * @see Array::printPipe()
     */
    std::string getClassName() const;

    /**
     * Get array identifier
     */
    virtual ArrayID getHandle() const;

    /**
     * Determine if this array has an easily accessible list of chunk positions. In fact, a set of chunk positions can
     * be generated from ANY array simply by iterating over all of the chunks once. However, this function will return true
     * if retrieving the chunk positions is a separate routine that is more efficient than iterating over all chunks.
     * All materialized arrays can and should implement this function.
     * @return true if this array supports calling getChunkPositions(). false otherwise.
     */
    virtual bool hasChunkPositions() const
    {
        return false;
    }

    /**
     * Build and return a list of the chunk positions. Only callable if hasChunkPositions() returns true, throws otherwise.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual std::shared_ptr<CoordinateSet> getChunkPositions() const;

    /**
     * If hasChunkPositions() is true, return getChunkPositions(); otherwise build a list of chunk positions manually
     * by iterating over the chunks of one of the array attributes. The attribute to iterate over is chosen according to a heuristic,
     * using empty_tag if available, otherwise picking the smallest fixed-size attribute. The array getSupportedAccess() must be
     * at least MULTI_PASS.
     * @return the sorted set of coordinates, containing the first coordinate of every chunk present in the array
     */
    virtual std::shared_ptr<CoordinateSet> findChunkPositions() const;

    /**
     * Determine if the array is materialized; which means all chunks are populated either memory or on disk, and available on request.
     * This returns false by default as that is the case with all arrays. It returns true for MemArray, etc.
     * @return true if this is materialized; false otherwise
     */
    virtual bool isMaterialized() const
    {
        return false;
    }

    /**
     * Get the least restrictive access mode that the array supports. The default for the abstract superclass is RANDOM
     * as a matter of convenience, since the vast majority of our arrays support it. Subclasses that have access
     * restrictions are responsible for overriding this appropriately.
     * @return the least restrictive access mode
     */
    virtual Access getSupportedAccess() const
    {
        return RANDOM;
    }

    /**
     * Extract subarray between specified coordinates into the buffer.
     * @param attrID extracted attribute of the array (should be fixed size)
     * @param buf buffer preallocated by caller which should be preallocated by called and be large enough
     *        to fit all data.
     * @param first minimal coordinates of extract box
     * @param last maximal coordinates of extract box
     * @param init EXTRACT_INIT_ZERO or EXTRACT_INIT_NAN
     *             if buf is floating-point, EXTRACT_INIT_NAN writes a NaN in
     *             buf for each cell that was empty; otherwise a zero.
     *             the second option is only meaningful for extracting to arrays of float or double
     * @param null EXTRACT_NULL_AS_EXCEPTION or EXTRACT_NULL_AS_NAN
     *             if buf is floating-point, EXTRACT_NULL_AS_NAN writes a NaN in
     *             buf for each null; otherwise a null is an exception.
     *             if a floating-point array, whether it should be extracted as a NaN
     * @return number of extracted chunks
     */
    enum extractInit_t { EXTRACT_INIT_ZERO=0, EXTRACT_INIT_NAN };
    enum extractNull_t { EXTRACT_NULL_AS_EXCEPTION=0, EXTRACT_NULL_AS_NAN };
    virtual size_t extractData(AttributeID attrID, void* buf, Coordinates const& first, Coordinates const& last,
                               extractInit_t init=EXTRACT_INIT_ZERO,
                               extractNull_t null=EXTRACT_NULL_AS_EXCEPTION) const;

    /**
     * Append data from the array, all attribute-chunks
     * before advancing the chunk position ("horizontal")
     * @param[in] input source array
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void appendHorizontal(ArrayPtr const& input, CoordinateSet* newChunkCoordinates = NULL);

    /**
     * Append data from the array, all chunks positions of one attribute
     * before advancing to the next attribute ("vertical") [Deprecated, use appendHorizontal]
     * @param[in] input source array
     * @param[out] newChunkCoordinates if set - the method shall insert the coordinates of all appended chunks into the set pointed to.
     */
    virtual void appendVertical(ArrayPtr const& input,  CoordinateSet* newChunkCoordinates = NULL);

    /**
     * Get array descriptor
     */
    virtual ArrayDesc const& getArrayDesc() const = 0;

    /**
     * Get read-write iterator
     * @param attr attribute ID
     * @return iterator through chunks of spcified attribute
     */
    std::shared_ptr<ArrayIterator> getIterator(const AttributeDesc& attr);

    virtual std::shared_ptr<ArrayIterator> getIteratorImpl(const AttributeDesc& attr);

    /**
     * Get read-only iterator
     * @param attr attribute ID
     * @return read-only iterator through chunks of spcified attribute
     */
    std::shared_ptr<ConstArrayIterator> getConstIterator(const AttributeDesc& attr) const;

    virtual std::shared_ptr<ConstArrayIterator> getConstIteratorImpl(const AttributeDesc& attr) const = 0;

    /**
     * Get read-only iterator thtough all array elements
     * @param attr attribute ID
     * @param iterationMode chunk iteration mode
     */
    std::shared_ptr<ConstItemIterator> getItemIterator(const AttributeDesc& attr,
                                                       int iterationMode =
                                                           ConstChunkIterator::IGNORE_OVERLAPS) const;
    virtual std::shared_ptr<ConstItemIterator> getItemIteratorImpl(const AttributeDesc& attr,
                                                                   int iterationMode) const;

    /**
     * Scan entire array and print contents to logger
     * DEBUG build only.  Otherwise a nullop
     */
    void printArrayToLogger() const;

    void setQuery(std::shared_ptr<Query> const& query) {_query = query;}

    /**
     * If count() can return its result in O(1) [or O(nExistingChunks) time if from a
     * fast in-memory index such as a chunkMap], then
     * this method should return true to indicate it is "reasonably" fast
     * Otherwse, this should return false, for example, when chunks themselves would return false
     * from their isCountKnown() method.  If there are gray areas in between these cases
     * O(1) or O(nExistingChunks) vs O(nNonNullCells), then the API of isCountKnown()
     * will either need definition refinement and/or API revision
     * @return true if count() is sufficiently fast by the above criteria
     */
    virtual bool isCountKnown() const;

    /**
     * While we would like all arrays to do this in O(1) time or O(nChunks) time,
     * some cases still require traversal of all the chunks of one attribute of the array.
     * If that expense is too much, then isCountKnown() should return false, and you
     * should avoid calling count().
     * @return the count of all non-empty cells in the array
     */
    virtual size_t count() const; // logically const, even if an result is cached.


    /**
     * Given the coordinate set liveChunks - remove the chunk version for
     * every chunk that is in the array and NOT in liveChunks. This is used
     * by overriding-storing ops to ensure that new versions of arrays do not
     * contain chunks from older versions unless explicitly added.
     * @param query  shared pointer to query context
     * @param liveChunks the set of chunks that should NOT be tombstoned
     */
    virtual void removeDeadChunks(
        std::shared_ptr<Query>& query,
        std::set<Coordinates, CoordinatesLess> const& liveChunks)
        {}

    /**
     * Insert a tombstone locally for a single position for all attributes
     * @param query  shared pointer to query context
     * @param coords  position at which to remove
     */
    virtual void removeLocalChunk(
        std::shared_ptr<Query> const& query,
        Coordinates const& coords)
        {}

    /**
     * Flush all changes to the physical device(s) for the array, if required.
     */
    virtual void flush()
        {}

    /**
     * Remove all versions prior to lastLiveArrId from the array. If
     * lastLiveArrId is 0, removes all versions.
     * @param query  shared pointer to query context
     * @param lastLiveArrId the Versioned Array ID of last version to preserve
     */
    virtual void removeVersions(
        std::shared_ptr<Query>& query,
        ArrayID lastLiveArrId)
        {}

    /**
     * Attach an input pipe/array.
     *
     * @details "Pipe" refers to an input array and all of @i its
     * input arrays, recursively to whatever depth.  Input arrays are
     * managed here in the Array base class, allowing general code
     * (for example, Array::printPipe()) to traverse the entire
     * pipeline.
     */
    void pushPipe(ArrayPtr const& pipe);

    /** Retrieve input array by index. */
    ArrayPtr const& getPipe(size_t index) const;

    /** Overwrite input pipe. */
    void setPipe(size_t index, ArrayPtr const& pipe);

    /** Number of input pipes. */
    size_t getPipeCount() const { return _inputArrays.size(); }

    /** Format this array and (recursively) its inputs for debug output. */
    std::string printPipe(unsigned int indent = 0) const;

    /**
     * Return true iff the array has one or more input arrays.
     *
     * @details This method is a safety check to ensure that all
     * arrays having backing input arrays store them in base class
     * data members, where they can be found by printPipe() (and
     * eventually, by more sophisticated pipeline management code).
     *
     * If your array subclass has backing input array(s), then
     * - use Array::pushPipe() or the appropriate constructor to the
     *   input array smart pointer, and
     * - use Array::getPipe() to access your input arrays.
     * Examples: CrossJoinArray, FilterArray.
     *
     * If your array does @i not have backing input arrays, then
     * override this method to return false.  Examples: InputArray,
     * BuildArray.
     */
    virtual bool hasInputPipe() const { return true; }

protected:
    /// The query context for this array
    std::weak_ptr<Query> _query;                // TODO: make private by providing a setter method
                                                //       (currently directly set up to three levels
                                                //       of deriviation away, e.g. in PullSGArray)

    /**
     * Provides additional information for inclusion in printPipe() output.
     *
     * @details Prefer to avoid newlines so that indentation doesn't
     * get messed up.  Gets inserted immediately after the class name,
     * so something enclosed in parens will look good....
     */
    virtual std::string pipeInfo() const { return {}; }

private:
    /**
     * private to force the use of append{Vertical,Horizontal} so that we can find any remaining
     * uses of appendVertical in the code, which are problematic for pullSG
     * @param[in] input source array
     * @param[in] vertical
     *            If false - append the first chunk for all attributes, then the second chunk...
     * @param[out] newChunkCoordinates if not NULL, the method shall insert the coordinates of all
     *            appended chunks into the specified CoordinateSet.
     */
    void append(ArrayPtr const& input, bool vertical, CoordinateSet* newChunkCoordinates = NULL);

    /// The input arrays, if any
    std::vector<ArrayPtr> _inputArrays;

    static constexpr int MAGIC = 0xBADC0FEE;
    int _magic { MAGIC };
};

} // namespace

#endif /* ARRAY_H_ */
