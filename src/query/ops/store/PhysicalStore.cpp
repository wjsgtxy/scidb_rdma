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


#include <log4cxx/logger.h>

#include <array/Array.h>
#include <array/ArrayName.h>
#include <array/DBArray.h>
#include <array/DelegateArray.h>

#include <array/ParallelAccumulatorArray.h>
#include <array/TransientCache.h>
#include <query/Expression.h>
#include <query/PhysicalUpdate.h>
#include <query/QueryProcessor.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <util/PerfTimeScope.h>
#include <util/Timing.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physical_store"));

// main operator

class PhysicalStore: public PhysicalUpdate
{
    using super = PhysicalUpdate;
    static constexpr const char * const cls = "PhysicalStore::";

public:
    /// @see PhysicalOperator::PhysicalOperator()
    PhysicalStore(const string& logicalName, const string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema);

    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren) const override;

    /// @see PhysicalOperator::getDistributionRequirement()
    DistributionRequirement getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas)
        const override;

    /// @see PhysicalOperator::getOutputBoundaries()
    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas)
        const override { return inputBoundaries.front(); }

    /// @see PhysicalOperator::acceptsPullSG()
    bool acceptsPullSG(size_t input) const override { return true; }

    /// @see PhysicalOperator::requiresRedimensionOrPartition()
    void requiresRedimensionOrRepartition(vector<ArrayDesc> const& inputSchemas,
                                          vector<ArrayDesc const*>& modifiedPtrs) const override
    { repartForStoreOrInsert(inputSchemas, modifiedPtrs); }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& inputDistrib, size_t depth)
        const override;

    /// @see PhysicalOperator::getOutputDistribution()
    RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDists,
                                              std::vector<ArrayDesc> const& inputSchemas)
        const override;

    /// @see PhysicalOperator::isSingleThreaded()
    bool isSingleThreaded() const override { return false; }

    // preSingleExecute not overridden

    /// @see PhysicalOperator::execute()
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override;
private:

    // TODO: consider other names prefixed by execute
    static PhysicalBoundaries copyChunks(const std::shared_ptr<Array>& dstArray,
                                         const std::shared_ptr<Array>& srcArray,
                                         std::shared_ptr<Query> query,
                                         std::set<Coordinates, CoordinatesLess>& createdChunksOut) ;
};

//
// begin implementation
//

constexpr const char * const PhysicalStore::cls;

// ctor helper
static string getArrayName(const Parameters& parameters) {
    SCIDB_ASSERT(!parameters.empty());
    return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
}

// ctor
PhysicalStore::PhysicalStore(const string& logicalName, const string& physicalName,
                             const Parameters& parameters, const ArrayDesc& schema)
:
    PhysicalUpdate(logicalName, physicalName, parameters, schema,
                   getArrayName(parameters))
{
    LOG4CXX_DEBUG(logger, "PhysicalStore::ctor() _schema distType: "
                           << distTypeToStr(_schema.getDistribution()->getDistType()));
}

// inheritance
std::vector<DistType> PhysicalStore::inferChildInheritances(DistType inherited, size_t numChildren) const
{
    LOG4CXX_TRACE(logger, "PhysicalStore::inferChildInheritances: " << " inherited " << distTypeToStr(inherited));
    // common logic is in getOutputDistribution()
    std::vector<RedistributeContext> emptyRC;
    std::vector<ArrayDesc> emptyAD;
    auto context = getOutputDistribution(emptyRC, emptyAD);
    auto outDistType = context.getArrayDistribution()->getDistType();

    std::vector<DistType> result(numChildren, outDistType);
    SCIDB_ASSERT(numChildren==1);
    LOG4CXX_TRACE(logger, "PhysicalSTore::inferChildInheritances: result[0] = " << distTypeToStr(result[0]));

    return result;
}

// getDistributionRequirement
DistributionRequirement
PhysicalStore::getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const
{
        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);
        SCIDB_ASSERT(not isUninitialized(arrDist->getDistType()));
        SCIDB_ASSERT(not isUndefined(arrDist->getDistType()));

        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);
        SCIDB_ASSERT(_schema.getResidency());
        SCIDB_ASSERT(_schema.getResidency()->size() > 0);

        //XXX TODO: for now just check that all the instances in the residency are alive
        //XXX TODO: once we allow writes in a degraded mode, this call might have more semantics
        query->isDistributionDegradedForWrite(_schema);

        // make sure PhysicalStore informs the optimizer about the actual array residency
        ArrayResPtr arrRes = _schema.getResidency();
        SCIDB_ASSERT(arrRes);

        RedistributeContext distro(arrDist, arrRes);
        LOG4CXX_TRACE(logger, "store() input req distro: "<< distro);

        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder,
                                       vector<RedistributeContext>(1, distro));
}

DistType
PhysicalStore::inferSynthesizedDistType(std::vector<DistType> const& inputDistrib, size_t depth) const
{
    // Call common logic from getOutputDistribution().

    std::vector<RedistributeContext> emptyRC;
    std::vector<ArrayDesc> emptyAD;
    auto context = getOutputDistribution(emptyRC, emptyAD);
    DistType result = context.getArrayDistribution()->getDistType();
    LOG4CXX_TRACE(logger, cls << __func__ << ": Returning distType=" << result);
    return result;
}

/// @see PhysicalOperator::getOutputDistribution()
RedistributeContext
PhysicalStore::getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                     std::vector<ArrayDesc> const& inputSchemas) const
{
    ArrayDistPtr dist = _schema.getDistribution();
    if (_schema.isDataframe() && not isReplicated(dist->getDistType())) {
        // We are storing a non-replicated dataframe that was not pre-created.
        // Earlier we may have assumed defaultDistType(), so fix it.
        if (isDataframe(dist->getDistType())) {
            LOG4CXX_DEBUG(logger, "Schema already using dtDataframe, no need to overwrite");
        } else {
            LOG4CXX_DEBUG(logger, "Clobber " << distTypeToStr(dist->getDistType())
                          << " schema distribution, must use dtDataframe");
            dist = createDistribution(dtDataframe,
                                      _schema.getDistribution()->getRedundancy());

            // Clobber the _schema distribution too, so optimizer won't
            // try to SG the output (that we probably won't produce anyway
            // since we're likely not in "_fetch:1" mode).
            PhysicalStore* self = const_cast<PhysicalStore*>(this);
            self->_schema.setDistribution(dist);
        }
    }

    RedistributeContext distro(dist, _schema.getResidency());
    LOG4CXX_TRACE(logger, "store() output distro: "<< distro);
    return distro;
}

// getOutputBoundaries not overridden


// requiresRedimensionOrRepartition not overridden

// preSingleExecute not overridden

// execute
std::shared_ptr<Array>
PhysicalStore::execute(vector< std::shared_ptr<Array> >& inputArrays,
                       std::shared_ptr<Query> query)
{
        SCIDB_ASSERT(inputArrays.size() == 1);
        executionPreamble(inputArrays[0], query);

        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == getVersionFromName(_schema.getQualifiedArrayName())); // paranoid

        bool fetch = DEFAULT_FETCH_UPDATE_RESULTS;
        Parameter p = findKeyword("_fetch");
        if (p) {
            auto pexp = dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            fetch = pexp->getExpression()->evaluate().getBool();
        }

        string nsName, arrayName;
        query->getNamespaceArrayNames(getArrayName(_parameters), nsName, arrayName);
        SCIDB_ASSERT(isNameUnversioned(arrayName));

        //
        // pre lock transient case
        //
        if (_schema.isTransient()) {
            // no pre-lock transient case for insert
        }

        //
        // acquire locks (nearly identical to insert)
        //
        if (!_lock) {
            SCIDB_ASSERT(!query->isCoordinator());
            const LockDesc::LockMode lockMode =
                _schema.isTransient() ? LockDesc::XCL : LockDesc::WR;

             std::string namespaceName = _schema.getNamespaceName();
            _lock = std::shared_ptr<LockDesc>(
                make_shared<LockDesc>(
                    nsName,
                    arrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    LockDesc::WORKER,
                    lockMode));

            if (lockMode == LockDesc::WR) {
                SCIDB_ASSERT(!_schema.isTransient());
                _lock->setArrayVersion(version);
                std::shared_ptr<Query::ErrorHandler> ptr(make_shared<UpdateErrorHandler>(_lock));
                query->pushErrorHandler(ptr);
            }

            Query::Finalizer f = std::bind(&UpdateErrorHandler::releaseLock,
                                           _lock,
                                           std::placeholders::_1);
           query->pushVotingFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker(std::bind(&Query::validate, query));
           if (!SystemCatalog::getInstance()->lockArray(_lock, errorChecker)) {
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << _lock->toString();
           }
           SCIDB_ASSERT(_lock->getLockMode() == lockMode);
           LOG4CXX_DEBUG(logger, cls << __func__ << ": output array lock acquired");
        }

        //
        // post lock transient case
        //

        if (_schema.isTransient()) {
            SCIDB_ASSERT(_lock->getLockMode() == LockDesc::XCL);

            MemArrayPtr        outArray = std::make_shared<MemArray>(_schema,query); // materialized copy
            PhysicalBoundaries bounds(PhysicalBoundaries::createEmpty(_schema.getDimensions().size()));

            const size_t nAttrs = outArray->getArrayDesc().getAttributes().size();
            std::shared_ptr<Array> srcArray(inputArrays[0]);
            ArrayDesc const& srcArrayDesc(srcArray->getArrayDesc());
            // Cater to the case where the input array does not contain the coordinates
            // list. This happens as the result of an aggregate operator, for
            // example. See- Ticket SDB-5490. Use the same code from below (in the the
            // non-Transient Case) to add the coordinates list.
            // The "Right Thing To Do (tm)" is to eradicate "non-empty-able" arrays altogether.
            if (nAttrs > srcArrayDesc.getAttributes().size()) {
                SCIDB_ASSERT(nAttrs == srcArrayDesc.getAttributes().size() + 1);
                srcArray = std::make_shared<NonEmptyableArray>(inputArrays[0]);
            }

            //
            // DELEGATE APPEND  -- transient case only
            //
            LOG4CXX_DEBUG(logger, cls << __func__ << " transient case, using array->appendHorizontal()");
            outArray->appendHorizontal(srcArray);          // ...materialize it

            /* Run back over the chunks one more time to compute the physical bounds
            of the array...*/

            LOG4CXX_DEBUG(logger, cls << __func__ <<
                          " transient case, using bounds.updateFromChunk() on outArray chunks");
            const auto& fda = outArray->getArrayDesc().getAttributes().firstDataAttribute();
            for (std::shared_ptr<ConstArrayIterator> i(outArray->getConstIterator(fda)); !i->end(); ++(*i))
            {
                bounds.updateFromChunk(&i->getChunk());       // ...update bounds
            }

            LOG4CXX_DEBUG(logger, cls << __func__ << " transient case, updating schema boundaries");
            updateSchemaBoundaries(_schema, bounds, query);
            query->pushVotingFinalizer(std::bind(&PhysicalUpdate::recordTransient,
                                                 this,
                                                 outArray,
                                                 std::placeholders::_1));
            getInjectedErrorListener().throwif(__LINE__, __FILE__);

            // EARLY RETURN
            return fetch ? outArray : std::shared_ptr<Array>();
        } // end transient case

        //////////////////////////////////////////////////////////////////////
        //
        // NOT TRANSIENT CASE
        //
        //  seems like there is probably a lot of code duplication vs above
        //

        std::shared_ptr<Array>  srcArray    (inputArrays[0]);
        ArrayDesc const&   srcArrayDesc(srcArray->getArrayDesc());
        std::shared_ptr<Array>  dstArray    (DBArray::createDBArray(_schema, query));
        ArrayDesc const&   dstArrayDesc(dstArray->getArrayDesc());
        std::string descArrayName = dstArrayDesc.getName();
        std::string descNamespaceName = dstArrayDesc.getNamespaceName();
        SCIDB_ASSERT(dstArrayDesc == _schema);

        // Prepare to catch incoming replica chunks for dstArray.
        // See wiki:How+Chunk+Replication+Works
        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(), dstArray);

        const size_t nAttrs = dstArrayDesc.getAttributes().size();

        if (nAttrs == 0) {
            // EARLY RETURN
            return fetch ? dstArray : std::shared_ptr<Array>();
        }

        // See comment for similar code above for transient case, SDB-5490.
        if (nAttrs > srcArrayDesc.getAttributes().size()) {
            assert(nAttrs == srcArrayDesc.getAttributes().size()+1);
            srcArray = std::make_shared<NonEmptyableArray>(srcArray);
        }

        set<Coordinates, CoordinatesLess> createdChunkCoords;   // return values

        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, using copyChunks");
        PhysicalBoundaries bounds = copyChunks(dstArray, srcArray, query, createdChunkCoords);

        // Insert tombstone entries
        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, converting dead chunks to tombstones");
        PerfTimeScope ptsRemove(PTS_STORE_REMOVE_DEAD_CHUNKS);
        dstArray->removeDeadChunks(query, createdChunkCoords);

        // Update boundaries
        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, updating schema boundaries");
        PerfTimeScope ptsUpdate(PTS_STORE_UPDATE_SCHEMA_BOUND);
        updateSchemaBoundaries(_schema, bounds, query);

        // Sync replication
        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, waiting for replication");
        PerfTimeScope ptsRepSync(PTS_STORE_REPLICATION_SYNC);
        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());

        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, remove inbound queue");
        PerfTimeScope ptsRemoveInbound(PTS_STORE_REMOVE_INBOUND_QUEUE);
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());

        // Flush data and metadata for the array
        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, ptsFlush");
        PerfTimeScope ptsFlush(PTS_STORE_ARRAY_FLUSH);
        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, dstArray->flush");
        dstArray->flush();

        getInjectedErrorListener().throwif(__LINE__, __FILE__);

        LOG4CXX_DEBUG(logger, cls << __func__ << " non-transient case, returning");
        return fetch ? dstArray : std::shared_ptr<Array>();
}
//
// end execute
//


//
// was StoreJob::chunkHasValues in prior versions
//
// XXX TODO: until we have the single RLE? data format,
//           we need to filter out other depricated formats (e.g. dense/sparse/nonempyable)
static bool chunkHasValues(const std::shared_ptr<Array>& srcArray, ConstChunk const& srcChunk)
{
    bool isSrcEmptyable = (srcArray->getArrayDesc().getEmptyBitmapAttribute() != NULL);
    bool chunkHasVals = (!isSrcEmptyable) || (!srcChunk.isEmpty());
    return chunkHasVals;
}

//
// was StoreJob::run() in prior versions
//
PhysicalBoundaries
PhysicalStore::copyChunks(const std::shared_ptr<Array>& dstArray,
                          const std::shared_ptr<Array>& srcArray,
                          std::shared_ptr<Query> query,
                          std::set<Coordinates, CoordinatesLess>& createdChunksOut)
{
    PerfTimeScope pts(PTS_STORE_COPY_CHUNKS); // PhysicalStore::copyChunks()

    const auto& dstArrayDesc = dstArray->getArrayDesc();
    const auto nDims = dstArrayDesc.getDimensions().size();
    const auto nAttrs = dstArrayDesc.getAttributes().size();

    LOG4CXX_DEBUG(logger, "PhysicalStore::copyChunks(): begin");
    // from StoreJob::StoreJob in prior versions
    ASSERT_EXCEPTION(nAttrs <= std::numeric_limits<AttributeID>::max(),
                     "Attribute count exceeds max addressable iterator");

    PhysicalBoundaries bounds(PhysicalBoundaries::createEmpty(nDims));

    std::vector<std::shared_ptr<ArrayIterator> >      dstArrayIterators(nAttrs);
    std::vector<std::shared_ptr<ConstArrayIterator> > srcArrayIterators(nAttrs);
    for (const auto& attr : dstArrayDesc.getAttributes()) {
        LOG4CXX_TRACE(logger, "PhysicalStore::copyChunks(): assigning attrs: " <<
                              attr.getId() << " of " << nAttrs);
        dstArrayIterators[attr.getId()] = dstArray->getIterator(attr);
        srcArrayIterators[attr.getId()] = srcArray->getConstIterator(attr);
    }
    // end from StoreJob::StoreJob


    // TODO, see comment in parameter list, why is nAttrs passed instead of the following?
    // size_t nAttrs = dstArrayDesc.getAttributes().size();
    const auto& fda = srcArray->getArrayDesc().getAttributes().firstDataAttribute();
    while (!srcArrayIterators[fda.getId()]->end()) {
        bool chunkHasElems(true);
        for (const auto& attr : dstArrayDesc.getAttributes()) {
            LOG4CXX_TRACE(logger, "PhysicalStore::copyChunks(): attr " << attr.getId());
            ConstChunk const& srcChunk = srcArrayIterators[attr.getId()]->getChunk();
            Coordinates srcPos         = srcArrayIterators[attr.getId()]->getPosition();
            if (!dstArrayDesc.contains(srcPos)) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                    << CoordsToStr(srcPos) << dstArrayDesc.getDimensions();
            }
            //
            //   Only insert the logical chunk boundaries into the list of
            //  created chunks once ...
            if (attr.getId() == fda.getId()) {
                chunkHasElems = chunkHasValues(srcArray, srcChunk);
                if (chunkHasElems) {
                    createdChunksOut.insert(srcPos);
                }
            } else {
                SCIDB_ASSERT(chunkHasElems == chunkHasValues(srcArray, srcChunk));
            }
            if (chunkHasElems) {
                //
                //  If this is ...
                //  i. The last time we're going through a logical chunk's
                //     cells (that is, if we're dealing with the coordinates
                //     list / isEmpty attribute) and ...
                // ii. ... this logical chunk's boundaries are outside of the
                //     bounds we've gathered about what parts of the array have
                //     been seen so far ...
                //
                //  .. then track details about this chunk's MBR and use this
                // information to track the boundaries of the array being
                // stored, otherwise, just copy the chunk as best we can.
                bool isInside = bounds.isInsideBox(srcChunk.getFirstPosition(false)) &&
                                bounds.isInsideBox(srcChunk.getLastPosition(false));

                if (( attr.isEmptyIndicator() ) && (! isInside )) {
                    Coordinates chunkStart(srcPos.size());  // code motion per DG
                    Coordinates chunkEnd(srcPos.size());
                    dstArrayIterators[attr.getId()]->copyChunk( srcChunk, &chunkStart, &chunkEnd );
                    bounds = bounds.unionWith( PhysicalBoundaries ( chunkStart, chunkEnd ) );
                } else {
                    LOG4CXX_TRACE(logger, "PhysicalStore::copyChunks(): dstArryIter[attr.getId()]->copyChunks()");
                    dstArrayIterators[attr.getId()]->copyChunk(srcChunk);
                }
            } else {
                LOG4CXX_TRACE(logger, "PhysicalStore::copyChunks(): !chunkHasElems");
            }
            Query::validateQueryPtr(query);

        } // for

        //
        // advance to next chunk
        //
        for (const auto& attr : dstArrayDesc.getAttributes()) {
            if (!srcArrayIterators[attr.getId()]->end()) {
                ++(*srcArrayIterators[attr.getId()]);
            }
        }
        LOG4CXX_TRACE(logger, "PhysicalStore::copyChunks(): advanced to next chunk");
    } // while

    LOG4CXX_DEBUG(logger, "PhysicalStore::copyChunks(): end");
    return bounds;
}


DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
