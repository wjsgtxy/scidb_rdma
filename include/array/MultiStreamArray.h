#ifndef MULTISTREAMARRAY_H
#define MULTISTREAMARRAY_H
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

#ifndef SCIDB_CLIENT

#include <queue>

#include <array/StreamArray.h>

namespace scidb {

/**
 * MultiStreamArray merges chunks from different streams.
 *
 * The subclasses implementing the streams via nextChunkPos() and
 * nextChunkBody() are allowed to throw RetryException indicating that
 * nextChunk() should be called again to obtain the chunk
 */
class MultiStreamArray : public StreamArray
{
  public:

    MultiStreamArray(size_t nStreams,
                     size_t localStream,
                     ArrayDesc const& arr,
                     bool enforceDataIntegrity,
                     std::shared_ptr<Query>const& query);

    virtual ~MultiStreamArray() {}

    /// @return true if a data collision/unordered data would cause an exception
    bool isEnforceDataIntegrity() { return _enforceDataIntegrity; }

    /**
     * An abstract interface for customizing the way remote partial chunks are merged together
     */
    class PartialChunkMerger
    {
    public:
        virtual ~PartialChunkMerger() {}
        /**
         * Handle a remote partial chunk
         * @param stream chunk's logical instance of origin
         * @param attId chunk's attribute ID
         * @param chunk [in/out] partial chunk to merge. If NULL upon return, the chunk is no longer
         *        "owned" by the caller e.g. the partial chunk may be "borrowed" and used as merged chunk
         * @param query the current query context
         * @return  chunk!=NULL
         */
        virtual bool mergePartialChunk(size_t stream,
                                       AttributeID attId,
                                       std::shared_ptr<MemChunk>& chunk,
                                       std::shared_ptr<Query> const& query) = 0;

        /**
         * Handle a list of remote partial chunks
         * @param attId chunk's attribute ID
         * @param [in/out] partialChunks list of partial chunks to merge. If NULL upon return,
         *        e.g. the first partial chunk may be "borrowed" and used as merged chunk
         * @param streams chunk's stream number (logical instance of the source of the data)
         * @param query Current query
         */
        virtual void mergePartialChunks(AttributeID attId,
                                        std::vector<std::shared_ptr<MemChunk>> partialChunks,
                                        std::vector<size_t> streams,
                                        std::shared_ptr<Query> const& query) = 0;
        /**
         * Get a complete local chunk after merging all the partial chunks.
         * Upon return this merger must be prepared to handle a partial chunks for a new position.
         * @param attId chunk's attribute ID
         * @param query the current query context
         * @return merged chunk
         */
        virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                         std::shared_ptr<Query> const& query) = 0;
    protected:
        PartialChunkMerger() {}
    private: // disallow
        PartialChunkMerger(const PartialChunkMerger&);
        PartialChunkMerger& operator=(const PartialChunkMerger&);
    };

    /**
     * A default partial chunk merger which adds new cell values and overwrites the existing ones.
     * It can enforce data integrity checks and error out in case cell collisions (i.e. will not
     * overwrite existing cells).
     * @note If the data integrity checks are not enabled and the data cells collisions are possible,
     * the merged chunk data values are undefined (i.e. any colliding cell value for a given coordinate
     * may be chosen).
     */
    class DefaultChunkMerger : public MultiStreamArray::PartialChunkMerger
    {
    private:
        const bool _isEnforceDataIntegrity;
        // the order of definitions below must be preserved (chunk first, iterator second)
        std::shared_ptr<MemChunk> _mergedChunk;
        /// true if a data integrity issue has been found
        bool _hasDataIntegrityIssue;
        size_t _numElems;
        size_t _chunkSizeLimitMiB;

        static bool isEmptyBitMap(const std::shared_ptr<MemChunk>& chunk);

    public:
        /**
         * Constructor
         * @param isEnforceDataIntegrity flag for turning on/off data integrity checks
         */
        DefaultChunkMerger (bool isEnforceDataIntegrity);

        /// Destructor
        virtual ~DefaultChunkMerger () {}

        /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
        virtual bool mergePartialChunk(size_t stream,
                                       AttributeID attId,
                                       std::shared_ptr<MemChunk>& partialChunk,
                                       const std::shared_ptr<Query>& query);

        /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunks
        virtual void mergePartialChunks(AttributeID attId,
                                        std::vector<std::shared_ptr<MemChunk>> partialChunks,
                                        std::vector<size_t> streams,
                                        std::shared_ptr<Query> const& query) override;

        /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
        virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                           const std::shared_ptr<Query>& query);
    };

    /**
     * Customize the partial chunk merger for a given attribute.
     * @note NOT thread-safe. So, it should be called before starting to use this array.
     * @param attId attribute ID to which apply the merger
     * @param chunkMerger [in/out] merger to set, set to NULL upon return
     */
    void setPartialChunkMerger(AttributeID attId,
                               std::shared_ptr<PartialChunkMerger>& chunkMerger)
    {
        assert(chunkMerger);
        assert(attId < _chunkMergers.size());
        _chunkMergers[attId].swap(chunkMerger);
        chunkMerger.reset();
    }

protected:
    /**
     * @see StreamArray::nextChunk
     * @throws scidb::StreamArray::RetryException if the next chunk is not ready and nextChunk() needs
     *  to be called again
     * @note if at the time of throwing RetryException, MultiStreamArray cannot fully determine the next position
     * of a chunk destined for this instance, then it is not in the send queue of at least one other instance,
     * and nextChunk will, prior to the throw, sleep briefly, to throttle the rate of additional polling, which
     * can slow progress of that instance sending to yet other instances.
     * @note This method is NOT thread-safe.
     * Technically, this method should work correctly if the invocations are serialized per attribute
     * (i.e. different attributes are accessed in different threads).
     * However, MultiStream is a SINGLE_PASS array and has to be scanned horizontally
     * (all attributes at the same position before going to the next position),
     * so accessing different attributes asynchronously (say in different threads) may result in RetryException
     */
    ConstChunk const* nextChunk(const AttributeDesc& attId, MemChunk& chunk) override;
    virtual ConstChunk const* nextChunkBody(size_t stream, AttributeID attId, MemChunk& chunk) = 0;
    virtual bool nextChunkPos(size_t stream,
                              AttributeID attId,
                              Coordinates& pos,
                              size_t& destStream) = 0 ;

    size_t getStreamCount() const { return _nStreams; }
    size_t getLocalStream() const { return _localStream; }

private:
    /// A pair of a source stream and a destination stream.
    /// The destination stream may not match the local stream.
    /// When it does not, the source stream indicates the position of the next chunk it is sending
    /// (to the specified destination),
    /// so that this array potentially could make progress in selecting/constructing the next output chunk.
    /// Even in the absence of chunk data from the source the next output chunk can be produced if other streams
    /// provide chunks with 'lesser' coordinates.
    class SourceAndDest
    {
    public:
    SourceAndDest(Coordinates const& coords,
                      size_t src, size_t dest)
    : _coords(coords), _src(src), _dest(dest) {}
        Coordinates const& getCoords() const { return _coords; }
        size_t getSrc() const  { return _src; }
        size_t getDest() const { return _dest; }
        bool operator< (SourceAndDest const& right) const
        {
            scidb::CoordinatesLess comp;
            return comp(right.getCoords(), _coords); // reverse order
        }
    private:
        SourceAndDest();
        // relying on the default destructor, copy constructor & assignment
        Coordinates _coords;
        size_t _src;
        size_t _dest;
    };
    typedef std::priority_queue<SourceAndDest> PositionMap;

    void getAllStreamPositions(PositionMap& readyPos,
                               std::list<size_t>& notReadyPos,
                               const AttributeID attId);
    void mergePartialStreams(PositionMap& readyPos,
                             std::list<size_t>& notReadyPos,
                             std::list<size_t>& currPartialStreams,
                             const AttributeID attId);
    void getNextStreamPositions(PositionMap& readyPos,
                                std::list<size_t>& notReadyPos,
                                std::list<size_t>& currPartialStreams,
                                const AttributeID attId);
    void logReadyPositions(PositionMap& readyPos,
                           const AttributeID attId);

    const size_t _nStreams;
    const size_t _localStream;
    const bool   _enforceDataIntegrity;
    std::vector<std::shared_ptr<MemChunk> > _resultChunks;
    std::vector<std::shared_ptr<PartialChunkMerger> > _chunkMergers;
    std::vector<PositionMap> _readyPositions;
    std::vector<std::list<size_t> > _notReadyPositions;
    std::vector<std::list<size_t> > _currPartialStreams;

    /// true if a data integrity issue has been found
    bool _hasDataIntegrityIssue;

    /// true iff we are multistreaming a dataframe
    bool _isDataframe;

protected:
    /// A hint to the derived classes specifying the coordinates of the next output chunk (in progress)
    std::vector<Coordinates> _currMinPos;
private:
    size_t _nextChunkPosThrowDelayInNanosec ;
};

}  // namespace
#endif //SCIDB_CLIENT

#endif // endif ! MULTISTREAMARRAY_H
