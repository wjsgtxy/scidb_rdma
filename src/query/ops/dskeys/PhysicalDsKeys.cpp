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

#include <array/MemArray.h>
#include <query/InstanceID.h>
#include <query/PhysicalOperator.h>
#include <array/DBArray.h>
#include <storage/PersistentIndexMap.h>

#include <fstream>

namespace scidb
{

class PhysicalDsKeys : public PhysicalOperator
{
public:
    PhysicalDsKeys(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema)
        : PhysicalOperator(logicalName,
                           physicalName,
                           parameters,
                           schema)
    { }

private:
    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/,
                                      size_t /*depth*/) const override
    {
        _schema.setDistribution(createDistribution(dtUndefined));
        return _schema.getDistribution()->getDistType();
    }

    /// @see OperatorDist
    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        SCIDB_ASSERT(numChildren == 1);
        return {true};
    }

    /// @see OperatorDist
    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren) const
    {
        SCIDB_ASSERT(numChildren == 1);

        // Replication is required so that all instances get a copy of
        // the array containing the UAIDs.
        return {dtReplication};
    }

    /// @see PhysicalOperator
    RedistributeContext getOutputDistribution(
        std::vector<RedistributeContext> const& /*inputDistrib*/,
        std::vector<ArrayDesc> const& /*inputSchemas*/) const override
    {
        _schema.setDistribution(createDistribution(dtUndefined));
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    using ArrayIterators = std::vector<std::shared_ptr<ArrayIterator>>;
    using ChunkIterators = std::vector<std::shared_ptr<ChunkIterator>>;

    /**
     * Create the next chunk for writing.
     *
     * @param oaiters[in,out] The output array iterators.
     * @param ociters[in,out] The output array chunk iterators.
     * @param coords The new coordinate at which to open new chunks.
     * @param query The pointer to the query context.
     */
    void _advance(ArrayIterators& oaiters,
                  ChunkIterators& ociters,
                  const Coordinates& coords,
                  const std::shared_ptr<Query>& query) const
    {
        for (const auto& attr : _schema.getAttributes(/*excludeEbm:*/true)) {
            ociters[attr.getId()]->flush();

            // populate empty tag from attr 0 implicitly
            int mode = ChunkIterator::SEQUENTIAL_WRITE;
            if (attr.getId()) {
                mode |= ChunkIterator::NO_EMPTY_CHECK;
            }

            ociters[attr.getId()] =
                oaiters[attr.getId()]->newChunk(coords).getIterator(query, mode);
        }
    }

    /**
     * Write a uint32_t into an output chunk referenced by the iterator.
     *
     * @param iter The iterator over the output chunk.
     * @param val The value to write.
     */
    void _writeUint32(const std::shared_ptr<ChunkIterator>& iter,
                      uint32_t val) const
    {
        SCIDB_ASSERT(iter);
        Value buf;
        buf.setUint32(val);
        iter->writeItem(buf);
    }

    /**
     * Write a uint64_t into an output chunk referenced by the iterator.
     *
     * @param iter The iterator over the output chunk.
     * @param val The value to write.
     */
    void _writeUint64(const std::shared_ptr<ChunkIterator>& iter,
                      uint64_t val) const
    {
        SCIDB_ASSERT(iter);
        Value buf;
        buf.setUint64(val);
        iter->writeItem(buf);
    }

    /**
     * Write a string into an output chunk referenced by the iterator.
     *
     * @param iter The iterator over the output chunk.
     * @param val The value to write.
     */
    void _writeString(const std::shared_ptr<ChunkIterator>& iter,
                      const std::string& val) const
    {
        SCIDB_ASSERT(iter);
        Value buf;
        buf.setString(val);
        iter->writeItem(buf);
    }

    /**
     * Walk rocksdb for matching nsid and dsid entries, writing those into
     * the output chunk iterators.
     *
     * @param nsid The datastore namespace ID.
     * @param dsid The datastore ID, commonly the same as the unversioned
     *     array ID.
     * @param query The query context.
     * @param oaiters[in,out] The output array attribute iterators.
     * @param ociters[in,out] The output array chunk iterators.
     * @param coords[in,out] The current coordinate in the output, updated by
     *     this method.
     */
    void _walk(const DataStore::NsId nsid,
               const DataStore::DsId dsid,
               std::shared_ptr<Query> query,
               ArrayIterators& oaiters,
               ChunkIterators& ociters,
               Coordinates& coords) const
    {
        DataStore::DataStoreKey dskey(nsid, dsid);
        DbAddressMeta am;
        RocksIndexMap<DbAddressMeta> index(dskey, am, scidb::arena::getArena());

        for (auto it = index.begin(); !it->isEnd(); ++(*it)) {
            const DbAddressMeta::Key* key = it->getKey();

            ociters[0]->setPosition(coords);
            if (ociters[0]->end()) {
                // All chunks are in lockstep with consistent state, so
                // the first at the end implies all at the end.
                _advance(oaiters, ociters, coords, query);
            }

            // nsid
            _writeUint32(ociters[0], nsid);

            // dsid
            ociters[1]->setPosition(coords);
            _writeUint64(ociters[1], dsid);

            // ndims
            ociters[2]->setPosition(coords);
            _writeUint64(ociters[2], key->_nDims);

            // attid
            ociters[3]->setPosition(coords);
            _writeString(ociters[3], displayAttributeID(key));

            // coords
            ociters[4]->setPosition(coords);
            // CoordsToStr is obsolete name for CoordinateCRange, prefer
            // the latter.
            CoordinateCRange coordRange(key->_coords,
                                        &(key->_coords[key->_nDims]));
            std::stringstream ss;
            ss << coordRange;
            _writeString(ociters[4], ss.str());

            // arrverid
            ociters[5]->setPosition(coords);
            _writeUint64(ociters[5], key->_arrVerId);

            // The first dimension is this instance's ID and that won't change,
            // only move along the second dimension.
            ++coords[1];
        }
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(inputArrays.size() == 1);

        const auto& inAttrs = inputArrays[0]->getArrayDesc().getAttributes();
        auto inputAttrIter = inputArrays[0]->getConstIterator(inAttrs.firstDataAttribute());
        const auto NUM_OUTPUT_ATTRIBUTES = _schema.getAttributes(/*excludeEbm:*/true).size();
        std::vector<std::shared_ptr<ArrayIterator>> oaiters(NUM_OUTPUT_ATTRIBUTES);
        std::vector<std::shared_ptr<ChunkIterator>> ociters(NUM_OUTPUT_ATTRIBUTES);
        auto outputArray = std::make_shared<MemArray>(_schema, query);

        // The initial coordinate is {my_instance_id, start_min} where my_instance_id
        // is the logical instance ID.
        Coordinates coords(2, 0);
        coords[0] = query->getInstanceID();
        coords[1] = _schema.getDimensions()[1].getStartMin();

        // Initialize all of the output array iterators and the first set of
        // output chunks and chunk iterators.
        for (const auto& attr : _schema.getAttributes(/*excludeEbm:*/true)) {
            oaiters[attr.getId()] = outputArray->getIterator(attr);

            // populate empty tag from attr 0 implicitly
            int mode = ChunkIterator::SEQUENTIAL_WRITE;
            if (attr.getId()) {
                mode |= ChunkIterator::NO_EMPTY_CHECK;
            }

            ociters[attr.getId()] = oaiters[attr.getId()]->newChunk(coords).getIterator(query, mode);
        }

        const DataStore::NsId nsid = DataStores::getInstance()->openNamespace("persistent");

        // Walk the input array, each cell containing the DSID of an entry
        // in rocksdb that the user is interested in.
        while (!inputAttrIter->end()) {
            auto inputChunkIter = inputAttrIter->getChunk().getConstIterator();
            while (!inputChunkIter->end()) {
                auto dsid = static_cast<DataStore::DsId>(inputChunkIter->getItem().getUint64());
                _walk(nsid, dsid, query, oaiters, ociters, coords);
                ++(*inputChunkIter);
            }
            ++(*inputAttrIter);
        }

        for (size_t oatt = 0; oatt < NUM_OUTPUT_ATTRIBUTES; ++oatt) {
            ociters[oatt]->flush();
        }

        return outputArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDsKeys, "_dskeys", "PhysicalDsKeys")

}
