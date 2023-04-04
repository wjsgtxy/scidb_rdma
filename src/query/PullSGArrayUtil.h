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
 * @file PullSGArrayUtil.h
 *
 * @brief Helper classes and routines used in pull-SG
 */
#ifndef PULL_SG_ARRAY_UTIL_H_
#define PULL_SG_ARRAY_UTIL_H_

#include <query/PullSGArray.h>
#include <query/PullSGContext.h>

namespace scidb
{
namespace sg
{

/// Functor class that copies a given chunk into a given array
class WriteChunkToArrayFunc
{
public:
    /**
     * Constructor
     * @param outputArray to absorb the chunks
     * @param newChunkCoords a set of chunk coordinates to populate during the operation of this functor,
     * each chunk position will be recorded in it
     */
    WriteChunkToArrayFunc(const std::shared_ptr<Array>& outputArray,
                          std::set<Coordinates, CoordinatesLess>* newChunkCoords,
                          bool enforceDataIntegrity);
    /**
     * Write a chunk into the internally stored array.
     * All chunks must have unique positions.
     * @param attId chunk attribute ID
     * @param chunk
     * @param query
     */
    void operator() (const AttributeID attId,
                     const ConstChunk& chunk,
                     const std::shared_ptr<Query>& query);

    void verifyBitmap(ConstChunk const& dataChunk, ConstChunk const& ebmChunk);

private:
    std::shared_ptr<Array> _outputArray;
    std::set<Coordinates, CoordinatesLess>* _newChunkCoords;
    std::vector<std::shared_ptr<ArrayIterator> > _outputIters;
    bool _enforceDataIntegrity;
    /// true if a data integrity issue has been found
    bool _hasDataIntegrityIssue;
};

/**
 * An Array that serializes the attributes of the input array.
 * It converts the input attribute IDs into the coordinate values in the extra (last) dimension.
 * It has one attribute (+empty bit map), and # of input dimensions + 1.
 * It is designed strictly for the internal use with PullSG and does not support the full-fledged array API.
 * Its purpose is to minimize the buffer space needed for a streaming SG. In particular, if the redistributed array is
 * SINGLE_PASS array. This array serializes the input attributes into a single "pseudo" attribute (according to a given order),
 * whereby requiring only a single buffer queue rather than one for each of the original attributes.
 * @note NOT thread-safe
 */
class SerializedArray : public StreamArray
{
public:

    static const char* SERIALIZED_DIM_NAME;
    static const char* SERIALIZED_ATTR_NAME;

    /// Constructor
    /// @param array to serialize
    /// @param attributeOrdering the (sub)set of attributes to serialize in the order specified
    explicit SerializedArray(std::shared_ptr<Array> const& array,
                             AttributeOrdering const& attributeOrdering) ;
    /// Destructor
    virtual ~SerializedArray() {}

    /**
     * Convert a "serialized" chunk of the pseudo attribute 0 into a chunk
     * with the attribute ID equal to the last coordinate. The last dimension is removed.
     * The data payload of either chunk is untouched.
     * @param multiArray[in] the array to which the multi-attribute chunk belongs
     * @param singleAttrChunk[in] - serialized chunk
     * @param multiDesc[in] - multi-attribute array descriptor
     * @param multiAttrChunk[in,out] - multi-attribute chunks
     * @param tmpChunkAddr - scratch space, value is undefined
     */
    static void toMultiAttribute(const Array* multiArray,
                                 const ConstChunk* singleAttrChunk,
                                 const ArrayDesc& multiDesc,
                                 MemChunk* multiAttrChunk,
                                 Address& tmpMemChunkAddr) ;
    /**
     * A helper class that is used as a MultiStreamArray::PartialChunkMerger and a PullSGArrayBlocking::ChunkHandler.
     * Its purpose is to hide the fact the chunks used by the redistribute code are serialized
     * (i.e. all of the same pseudo attribute 0, with the last coordinate == original attribute ID).
     * from the consumer of the chunks. When the chunks are merged or consumed using this class,
     * their metadata corresponds to the array metadata specified by the caller of pullRedistributeXXX().
     * However, for the purposes of PuSGArray/PullSGContext their metadata is converted into the serialized form.
     *
     * NOTE: not thread-safe
     * NOTE: the public methods must not be executed by multiple threads concurrently
     */
    class SerializedChunkMerger : public MultiStreamArray::PartialChunkMerger
    {
    public:

        /// Constructor
        /// @param[in] multiDesc multi-attribute/deserialized descriptor
        /// @param[in] singleDesc single-attribute/serialized descriptor
        /// @param[in,out] mergers if not NULL, will contain NULL pointers on return
        /// @param[in,out] handler empty on return
        explicit SerializedChunkMerger(const ArrayDesc& multiDesc,
                                 const ArrayDesc& singleDesc,
                                 PartialChunkMergerList* mergers,
                                 PullSGArrayBlocking::ChunkHandler& handler,
                                 bool enforceDataIntegrity) ;

        /// @see MultiStreamArray::PartialChunkMerger::mergerPartialChunk()
        virtual bool mergePartialChunk(size_t stream,
                                       AttributeID attId,
                                       std::shared_ptr<MemChunk>& partialChunk,
                                       const std::shared_ptr<Query>& query) ;

        /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunks
        virtual void mergePartialChunks(AttributeID attId,
                                        std::vector<std::shared_ptr<MemChunk>> partialChunks,
                                        std::vector<size_t> streams,
                                        std::shared_ptr<Query> const& query) override;

        /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk()
        virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                           const std::shared_ptr<Query>& query) ;

        /// Conforms to PullSGArrayBlocking::ChunkHandler
        void handleChunk (const AttributeID attId,
                          const ConstChunk& chunk,
                          const std::shared_ptr<Query>& query) ;

private:

        static bool isInitialized(const ArrayDesc& desc)
        {
            return !desc.getDimensions().empty();
        }

        /**
         * Helper to convert a normal/deserialized chunk metadata to its serialized form.
         * The attribute of the output chunk is 0 and its coordinates are
         * {the input coordinates, the input attribute ID}
         * @param[in] attId deserialized attribute ID
         * @param[in,out] chunk to convert
         * @param[in] singleDesc serialized array descriptor
         * @param[in,out] scratch space to avoid memory allocation, value is undefined
         * @param[in] query the current Query context
         */
        static void toSingleAttribute(AttributeID attId,
                                      MemChunk* chunk,
                                      const ArrayDesc& singleDesc,
                                      Address& tmpMemChunkAddr,
                                      const std::shared_ptr<Query>& query) ;
        /**
         * Helper to convert a serialized chunk metadata to its normal/deserialized form.
         * The attribute of the output chunk==last coordinate of the input and its coordinates are
         * the input coordinates without the last one.
         * @param[in] attId asserted to be 0, otherwise ignored
         * @param[in] multiDesc deserialized array descriptor
         * @param[in,out] chunk to convert
         * @param[in,out] scratch space to avoid memory allocation
         * @param[in] query the current Query context
         */
        static void toMultiAttribute(AttributeID attId,
                                     const ArrayDesc& multiDesc,
                                     MemChunk* chunk,
                                     Address& tmpMemChunkAddr,
                                     const std::shared_ptr<Query>& query) ;

        const ArrayDesc _multiDesc;
        ArrayDesc _finalMultiDesc;
        const ArrayDesc _singleDesc;
        PullSGArrayBlocking::ChunkHandler _handler;
        AttributeID _multiAttId;
        Address _tmpMemChunkAddr;
        std::vector<std::shared_ptr<PartialChunkMerger> > _chunkMergers;
    };

    /**
     *  Determine the destination instance for "serialized" chunk Coordinates
     *
     * @param query
     * @param chunkPosition for the serialized chunk
     * @param arrayDesc for the serialized chunk
     * @param multiAttrDesc - array descriptor with multiple attributes
     * @param multiAttrChunkPos - scratch coordinates object to generate
     *                            the multi-attribute position
     * @param inputInstLocator - the locator to use for the multi-attribute position
     */
    static InstanceID instanceForChunk(const std::shared_ptr<Query>& query,
                                       const Coordinates& chunkPosition,
                                       const ArrayDesc& arrayDesc,
                                       const ArrayDesc& multiAttrDesc,
                                       Coordinates& multiAttrChunkPos,
                                       SGInstanceLocator& inputInstLocator);
protected:

    /**
     * @see StreamArray::nextChunk
     * @throws scidb::StreamArray::RetryException
     * @note This method is NOT thread-safe.
     */
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

private:

    /// Convert a standard (possibly) multi-attribute descriptor into
    /// a "serialized" single-attribute one
    /// @param desc "normal" multi-attribute descriptor
    /// @return an array descriptor with a single "void" attribute and
    ///         input_dimensions+1 dimensions
    static ArrayDesc getSingleAttributeDesc(const ArrayDesc& desc);

    /// A chunk wrapper that makes the input chunk look like
    /// it has an extra coordinate corresponding to the attribute ID.
    /// For internal use with PullSGArray/PullSGContext.
    class ChunkImpl : public ConstChunk
    {
    public:

        ChunkImpl(const Array* outputArray, const ConstChunk* inputChunk)
        {
            initialize(outputArray, inputChunk);
        }

        ChunkImpl() : _inputChunk(NULL), _outputArray(NULL), _emptyBitmapChunk(false)
        {}

        virtual ~ChunkImpl() {}

        void initialize(const Array* outputArray, const ConstChunk* inputChunk);

        virtual Coordinates const& getFirstPosition(bool withOverlap) const ;

        virtual Coordinates const& getLastPosition(bool withOverlap) const ;

        virtual bool isMaterialized() const
        {
            return _inputChunk->isMaterialized();
        }

        size_t getBitmapSize() const
        {
            return _inputChunk->getBitmapSize();
        }

        virtual const ArrayDesc& getArrayDesc() const
        {
            return _outputArray->getArrayDesc();
        }

        virtual const AttributeDesc& getAttributeDesc() const
        {
            return _outputArray->getArrayDesc().getAttributes().firstDataAttribute();
        }

        virtual size_t count() const
        {
            return _inputChunk->count();
        }

        virtual bool isCountKnown() const
        {
            return _inputChunk->isCountKnown();
        }

        size_t getNumberOfElements(bool withOverlap) const
        {
            return _inputChunk->getNumberOfElements(withOverlap);
        }

        bool isSolid() const
        {
            return _inputChunk->isSolid();
        }


        virtual CompressorType getCompressionMethod() const
        {
            return _inputChunk->getCompressionMethod();
        }

        virtual void compress(CompressedBuffer& buf,
                              std::shared_ptr<ConstRLEEmptyBitmap>& emptyBitmap,
                              bool forceUncompressed) const
        {
            _inputChunk->compress(buf, emptyBitmap, forceUncompressed);
        }

        virtual size_t getSize() const { return _inputChunk->getSize(); }
        virtual bool pin() const       { return _inputChunk->pin(); }
        virtual void unPin() const     {_inputChunk->unPin(); }

        /** Get the Empty Bitmap Attribute for the InputChunk.
         * @return the empty bitmap of the _inputChunk iff this chunk itself has not been
         *         marked as the empty bitmap chunk else return a null pointer.
         */
        virtual std::shared_ptr<ConstRLEEmptyBitmap> getEmptyBitmap() const
        {
            if (! isEmptyBitMapChunk())
            {
                return _inputChunk->getEmptyBitmap();
            }
            return std::shared_ptr<ConstRLEEmptyBitmap>(nullptr);
        }

        bool isEmpty(bool withOverlap=true) const
        {
            return _inputChunk->isEmpty(withOverlap);
        }

        bool contains(Coordinates const& pos, bool withOverlap) const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::contains");
            return false;
        }

        virtual std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::getConstIterator");
            return  std::shared_ptr<ConstChunkIterator>();
        }

        ConstChunkIterator* getConstIteratorPtr(int iterationMode)
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::getConstIteratorPtr");
            return NULL;
        }

        virtual Array const& getArray() const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::getArray");
            Array* dummy(NULL);
            return (*dummy);
        }

        void makeClosure(Chunk& closure,
                         std::shared_ptr<ConstRLEEmptyBitmap> const& emptyBitmap) const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::makeClosure");
        }

        virtual ConstChunk const* getBitmapChunk() const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::getBitmapChunk");
            return NULL;
        }

        virtual ConstChunk* materialize() const
        {
            ASSERT_EXCEPTION_FALSE("SerializedArray::ChunkImpl::materialize");
            return NULL;
        }

        /**
         * Specify that this chunk is already an Empty Bitmap (EBM) chunk so that
         * no additional empty bitmap(s) will be attached to this chunk.
         *
         * @param val set(true)/unset(false) the _emptyBitmapChunk flag
         */
        void setEmptyBitmapChunk(bool val)
        {
            _emptyBitmapChunk = val;
        }

        /**
         * Is this chunk marked as the empty bitmap chunk.
         *
         * @see ChunkImpl::getEmptyBitmap() const
         */
        bool isEmptyBitMapChunk() const
        {
            return _emptyBitmapChunk;
        }

    private:

        const ConstChunk* _inputChunk;
        const Array* _outputArray;
        Coordinates _outputFirstPosition;
        Coordinates _outputLastPosition;
        Coordinates _outputFirstPositionWOverlap;
        Coordinates _outputLastPositionWOverlap;
        bool _emptyBitmapChunk;    //!< flag indicating if this chunk is for the empty bitmap attribute.
    };

private:

    SerializedArray();
    SerializedArray(const SerializedArray&);
    SerializedArray& operator=(const SerializedArray&);

private:

    std::shared_ptr<Array> _inputArray;
    AttributeOrdering _attributeOrdering;
    AttributeOrdering::const_iterator _currAttribute;
    std::vector< std::shared_ptr<ConstArrayIterator> > _inputIterators;
    ChunkImpl _currChunk;
};

/**
 * An array that can be used in conjunction with SerializedArray to "deserialize" single attribute chunks
 * It is a stream whose chunks must be consumed (one at a time) in the order specified in its constructor.
 * It was once based on StreamArray but now is based on SinglePassArray so that pullSG does not have to
 * materialize full arrays.
 * @note NOT thread-safe
 */
class DeserializedArray : public SynchableArray, public StreamArray
{
public:

    /// Constructor
    explicit DeserializedArray(std::shared_ptr<PullSGArrayBlocking> const& array,
                               ArrayDesc const& multiAttrDesc,
                               AttributeOrdering const& attributeOrdering)
    : StreamArray(multiAttrDesc, false),
      _inputArray(array),
      _attributeOrdering(attributeOrdering),
      _currAttribute(_attributeOrdering.begin())
    { }

    /// Destructor
    virtual ~DeserializedArray() {}

    /// @see scidb::SynchableArray
    virtual void sync() { _inputArray->sync(); }

protected:

    /**
     * @see StreamArray::nextChunk
     * @throws scidb::StreamArray::RetryException
     * @note This method is NOT thread-safe.
     */
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;

private:

    /**
     * Copy an input single-attribute (serialized) chunk to a multi-attribute(deserialized) chunk
     * @param inputChunk single-attribute chunk
     * @param chunk multi-attribute chunk
     */
    void deserialize(ConstChunk const* inputChunk, MemChunk& chunk);

private:

    DeserializedArray();
    DeserializedArray(const DeserializedArray&);
    DeserializedArray& operator=(const DeserializedArray&);

private:

    std::shared_ptr<PullSGArrayBlocking> _inputArray;
    AttributeOrdering _attributeOrdering;
    AttributeOrdering::const_iterator _currAttribute;
    std::shared_ptr<ConstArrayIterator> _inputIterator;
    Address _tmpMemChunkAddr;
};

struct FailOnInvocation
{
    void operator() (AttributeID, const ConstChunk&, const std::shared_ptr<Query>&)
    {
        ASSERT_EXCEPTION_FALSE("SerializedArray::SerializedChunkMerger<FailOnInvocation> must not be called");
    }
};

/**
 * Wrap the input array into a SerializedArray and return the result of pullRedistribute()
 * For the description of parameters @see scidb::pullRedistributeInAttributeOrder()
 * @return a SerializedArray to be pulled or inputArray if the distribution is not needed
 */
std::shared_ptr<Array> getSerializedArray(std::shared_ptr<Array>& inputArray,
                                          AttributeOrdering& attributeOrdering,
                                          const ArrayDistPtr& outputArrayDist,
                                          const ArrayResPtr&  outputArrayRes,
                                          const std::shared_ptr<Query>& query,
                                          const std::shared_ptr<PhysicalOperator>& physOp,
                                          bool enforceDataIntegrity);
/**
 * Redistribute inputArray applying chunkHandler on every redistributed chunk
 * For the description of parameters @see scidb::pullRedistributeToRandomAccess()
 * @param chunkHandler chunk handler to apply on every redistributed chunk
 * @return either inputArray if no redistribution is necessary or a pointer to an undefined array
 */
std::shared_ptr<Array> redistributeWithCallback(std::shared_ptr<Array>& inputArray,
                                                PullSGArrayBlocking::ChunkHandler& chunkHandler,
                                                PartialChunkMergerList* mergers,
                                                const ArrayDistPtr& outputArrayDist,
                                                const ArrayResPtr& outputArrayRes,
                                                const std::shared_ptr<Query>& query,
                                                const std::shared_ptr<PhysicalOperator>& physOp,
                                                bool enforceDataIntegrity);

} //namespace

} // namespace
#endif
