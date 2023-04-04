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
 * PhysicalDelete.cpp
 *
 *  Created on: January 19, 2017
 *  @author Donghui Zhang
 */

#include <array/Array.h>
#include <array/ArrayName.h>
#include <array/DBArray.h>
#include <array/DelegateArray.h>
#include <array/Dense1MChunkEstimator.h>
#include <array/TransientCache.h>
#include <log4cxx/logger.h>
#include <query/Multiquery.h>
#include <query/PhysicalOperator.h>
#include <query/PhysicalUpdate.h>
#include <query/QueryProcessor.h>
#include <query/TypeSystem.h>
#include <storage/StorageMgr.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <util/SpatialType.h>
#include "../filter/FilterArray.h"

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.delete"));

class PhysicalDelete: public PhysicalUpdate
{
    mutable unique_ptr<ArrayDesc> _prevDesc;

    static const string& getArrayName(const Parameters& parameters)
    {
        SCIDB_ASSERT(!parameters.empty());
        return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
    }

    /**
    * Find the descriptor for the previous version and populate placeHolder with it.
    * @param[out] placeholder the returned descriptor
    */
    ArrayDesc const& getPreviousDesc() const
    {
        if (_prevDesc) {
            return *_prevDesc;
        }
        _prevDesc.reset(new ArrayDesc());

        const string& arrayNameOrig = getArrayName(_parameters);
        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = _prevDesc.get();
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.throwIfNotFound = true;
        if (_schema.getId() != _schema.getUAId()) {
            //new version was already created; locate the previous
            SCIDB_ASSERT(_schema.getId() > _schema.getUAId());
            SCIDB_ASSERT(_schema.getVersionId() > 0);
            args.versionId = _schema.getVersionId() - 1;
        } else {
            // new version (our version) was not created yet
            args.versionId = LAST_VERSION;
        }
        SystemCatalog::getInstance()->getArrayDesc(args);

        return *_prevDesc;
    }

    /**
     * Clip the new boundaries to stay inside the old ones.
     *
     * @details Per step 4 (see "Algorithm:" below), if a chunk's
     * range is outside the dimRanges we'll continue, keeping the
     * chunk.  But we don't actually count the cells in that chunk, we
     * merely assume that its "range" is fully dense and call
     * updateFromRange() to include it in the bounds.  *BUT* it might
     * not be dense, it might have had some cells removed by an
     * earlier delete.  If it was a boundary chunk, then mysteriously
     * those deleted cells reappear in the new boundaries, because we
     * wrongly assumed a full chunk.  So we call this method to ensure
     * that delete() causes boundaries only to shrink, never to grow.
     */
    void clip(PhysicalBoundaries& bounds, ArrayDesc const& prevVersionDesc)
    {
        // Recall transient arrays are not versioned.
        const bool isTempArray = prevVersionDesc.isTransient();
        if (prevVersionDesc.getVersionId() == 0 && !isTempArray) {
            // Nothing previously stored, the bounds are the bounds.
            return;
        }

        Coordinates low = prevVersionDesc.getLowBoundary();
        Coordinates high = prevVersionDesc.getHighBoundary();
        bounds.intersectWith(PhysicalBoundaries(low, high));
    }

public:
    PhysicalDelete(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
      : PhysicalUpdate(logicalName, physicalName, parameters, schema, getArrayName(parameters))
    {}

    /**
     * Get the estimated upper bound of the output array for the optimizer.
     * @note Unlike in insert(inputArray, dstArrayName), here in delete() there is no input array.
     *       So this function ignores the input.
     */
    PhysicalBoundaries getOutputBoundaries(
        const std::vector<PhysicalBoundaries>&,
        const std::vector<ArrayDesc>&) const override
    {
        // Recall transient arrays are not versioned.
        ArrayDesc const& prevVersionDesc = getPreviousDesc();
        if (prevVersionDesc.getVersionId() == 0 && !prevVersionDesc.isTransient())
        {
            return PhysicalBoundaries();
        }

        Coordinates currentLo = prevVersionDesc.getLowBoundary();
        Coordinates currentHi = prevVersionDesc.getHighBoundary();
        return PhysicalBoundaries(currentLo, currentHi);
    }

    RedistributeContext
    getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                          std::vector<ArrayDesc> const& inputSchemas) const override
    {
        // Use whatever distribution we got from the catalog.
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * Algorithm:
     *  1. Parse expr --> dimRanges && otherConditions
     *  2. Create a FilterArray using notExpr
     *  3. for all chunkCoords in the array:
     *  4.   if dimRanges exists and the chunk's range is outside dimRanges, continue;
     *  5.   Pull a bitmap chunk from the FilterArray.
     *  6.   if #cells in the bitmap == 0
     *  7.     for all attributes, mark the chunk as a tombstone.
     *  8.   else if #cells in the bitmap != #cells in the original chunk
     *  9.     for all attributes, pull from FilterArray a chunk and write as a new version.
     * 10.   endif
     * 11. endfor
     */
    std::shared_ptr<Array> performDeletion(std::shared_ptr<Query>& query)
    {
        // If the array is auto-chunked, there is nothing to delete from it.
        // Just fix up the auto-chunked dimensions and return an empty array,
        // as PhysicalScan::execute() does.
        if (_schema.isAutochunked())
        {
            Dense1MChunkEstimator::estimate(_schema.getDimensions());
            return std::make_shared<MemArray>(_schema, query);
        }

        std::shared_ptr<Array> dstArray;
        const size_t numDims = _schema.getDimensions().size();
        const AttributeID numAttrs =
            static_cast<AttributeID>(_schema.getAttributes().size());  // including empty bitmap

        PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(numDims);

        // Will be filled by the code block below with the set of chunkCoords for chunks that are fully deleted.
        set<Coordinates, CoordinatesLess> chunkCoordsToRemove;

        // The write iterators to modify dstArray.
        vector<std::shared_ptr<ArrayIterator> > outputIters(numAttrs);

        // In the following code block: the set of all chunkCoords that exist.
        // After the code block: one code path will subtract those coords from chunkCoordsToRemove.
        std::shared_ptr<CoordinateSet> chunkCoordsSet;

        // The code block is introduced to free local variables, in particular read iterators of dstArray.
        {
            // 1. Parse expr --> spatialRangesPtr and hasOtherClauses
            auto expr = dynamic_pointer_cast<OperatorParamPhysicalExpression>(_parameters[1])->getExpression();
            auto notExpr = expr->negate();
            bool hasOtherClauses;
            auto spatialRangesPtr = std::make_shared<SpatialRanges>(numDims);
            expr->extractSpatialConstraints(spatialRangesPtr, hasOtherClauses);

            // 2. Create a FilterArray using notExpr
            if (_schema.isTransient())
            {
                dstArray = transient::lookup(_schema,query);
                transient::remove(_schema);
                query->pushVotingFinalizer(std::bind(&PhysicalUpdate::recordTransient,
                                                     this,
                                                     static_pointer_cast<MemArray>(dstArray),
                                                     std::placeholders::_1));
            }
            else
            {
                dstArray = DBArray::createDBArray(_schema, query);
            }

            SCIDB_ASSERT(dstArray->getArrayDesc().getId()   == _schema.getId());
            SCIDB_ASSERT(dstArray->getArrayDesc().getUAId() == _schema.getUAId());

            query->getReplicationContext()->enableInboundQueue(_schema.getId(), dstArray);

            auto filterArray = std::make_shared<FilterArray>(
                _schema,
                dstArray,  // The dstArray output array is also the input array for filtering.
                notExpr,
                query, _tileMode);
            vector<std::shared_ptr<ConstArrayIterator>> filterArrayIters(numAttrs);
            for (const auto& attr : _schema.getAttributes()) {
                filterArrayIters[attr.getId()] = filterArray->getConstIterator(attr);
            }

            // 3. For all chunkCoords in the array.
            chunkCoordsSet = dstArray->findChunkPositions();
            SpatialRange range(numDims);
            for (const auto& attr : _schema.getAttributes()) {
                outputIters[attr.getId()] = dstArray->getIterator(attr);
            }
            auto const& dims = _schema.getDimensions();
            auto const& ranges = spatialRangesPtr->ranges();
            for (auto it = chunkCoordsSet->begin(); it != chunkCoordsSet->end(); ++it) {
                auto const& chunkCoords = *it;

                // 4. if dimRanges exists and the chunk's range is outside dimRanges, continue;
                range._low = chunkCoords;
                for (size_t i=0; i < range._low.size(); ++i) {
                    range._high[i] = min(range._low[i] + dims[i].getChunkInterval() - 1, dims[i].getEndMax());
                }
                size_t hint = 0;
                if (!ranges.empty() && !spatialRangesPtr->findOneThatIntersects(range, hint)) {
                    bounds.updateFromRange(range);
                    continue;
                }

                // 5. Pull a bitmap chunk from the FilterArray.
                size_t filterChunkCount = 0;
                const auto& ebmAttr = *_schema.getAttributes().getEmptyBitmapAttribute();
                const auto ebmAttrId = ebmAttr.getId();
                if (filterArrayIters[ebmAttrId]->setPosition(chunkCoords) && !filterArrayIters[ebmAttrId]->end()) {
                    auto const*const ebmChunk = filterArrayIters[ebmAttrId]->getChunk().materialize();
                    SCIDB_ASSERT(ebmChunk);
                    if (!ebmChunk->isEmpty()) {
                        filterChunkCount = ebmChunk->count();
                    }
                }

                // 6. if #cells in the bitmap == 0
                // 7.   for all attributes, mark the chunk as a tombstone.
                if (filterChunkCount == 0) {
                    chunkCoordsToRemove.insert(chunkCoords);
                }

                // 8. else if #cells in the bitmap != #cells in the original chunk
                // 9.   for all attributes, pull from FilterArray a chunk and write as a new version.
                else {
                    auto origArrayIter = dstArray->getConstIterator(ebmAttr);
                    const bool status = origArrayIter->setPosition(chunkCoords);
                    SCIDB_ASSERT(status);
                    ConstChunk const& origChunk = origArrayIter->getChunk();
                    if (filterChunkCount == origChunk.count()) {
                        bounds.updateFromChunk(&origChunk);
                        continue;
                    }

                    // Make a copy of all chunks from the FilterArray, but don't write to dstArray yet.
                    // The rationale is that in order for the FilterArray to generate an empty-bitmap chunk,
                    // it needs to access some data chunks in the underlying dstArray (to evaluate notExpr).
                    // But at the same time if dstArray's data chunks are changed, there will be inconsistency.
                    vector<vector<char>> chunks(numAttrs);
                    for (const auto& attr : _schema.getAttributes()) {
                        bool status = filterArrayIters[attr.getId()]->setPosition(chunkCoords);
                        SCIDB_ASSERT(status);
                        ConstChunk const*const matChunk = filterArrayIters[attr.getId()]->getChunk().materialize();
                        const size_t size = matChunk->getSize();
                        void const* data = matChunk->getConstData();
                        chunks[attr.getId()].resize(size);
                        memcpy(&chunks[attr.getId()][0], data, size);
                    }

                    // Now write the cached chunks to dstArray.
                    for (const auto& attr : _schema.getAttributes()) {
                        const auto attrId = attr.getId();
                        auto& outputChunk = outputIters[attrId]->newChunk(chunkCoords);
                        outputChunk.allocateAndCopy(&chunks[attrId][0], chunks[attrId].size(), filterChunkCount, query);
                        if (attr.isEmptyIndicator()) {
                            bounds.updateFromChunk(&outputChunk);
                        }
                    }
                }  // if (filterChunkCount == 0) {
            }  // for (auto it = chunkCoordsSet.begin(); ...
        }  // end code block

        // Remove dead chunks
        if (_schema.isTransient()) {
            for (auto it = chunkCoordsToRemove.begin(); it != chunkCoordsToRemove.end(); ++it) {
                for (const auto& attr : _schema.getAttributes()) {
                    auto status = outputIters[attr.getId()]->setPosition(*it);
                    SCIDB_ASSERT(status);
                }
                for (const auto& attr : _schema.getAttributes()) {
                    Chunk& chunk = *const_cast<Chunk*>(reinterpret_cast<Chunk const*>(&outputIters[attr.getId()]->getChunk()));
                    outputIters[attr.getId()]->deleteChunk(chunk);
                }
            }
        } else {
            for (auto it = chunkCoordsToRemove.begin(); it != chunkCoordsToRemove.end(); ++it) {
                chunkCoordsSet->erase(*it);
            }
            dstArray->removeDeadChunks(query, *chunkCoordsSet);

            query->getReplicationContext()->replicationSync(_schema.getId());
            query->getReplicationContext()->removeInboundQueue(_schema.getId());
            dstArray->flush();
        }

        clip(bounds, getPreviousDesc());
        updateSchemaBoundaries(_schema, bounds, query);

        // @see the implementation of QueryProcessorImpl::execute()
        SCIDB_ASSERT(!dstArray->getArrayDesc().isAutochunked());
        return dstArray;
    }

    std::shared_ptr<Array> execute(vector<std::shared_ptr<Array>>&, std::shared_ptr<Query> query) override
    {
        // The doxygen comment at PhysicalUpdate::executionPreamble() requires executionPreamble()
        // to be called here, first thing in execute().
        std::shared_ptr<Array> dummy;
        executionPreamble(dummy, query);

        bool fetch = DEFAULT_FETCH_UPDATE_RESULTS;
        Parameter p = findKeyword("_fetch");
        if (p) {
            auto pexp = dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            fetch = pexp->getExpression()->evaluate().getBool();
        }

        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == getVersionFromName(_schema.getName()));

        string nsName, arrayName;
        query->getNamespaceArrayNames(getArrayName(_parameters), nsName, arrayName);
        SCIDB_ASSERT(isNameUnversioned(arrayName));

        if (!_lock)
        {
            SCIDB_ASSERT(!query->isCoordinator());
            const LockDesc::LockMode lockMode =
                _schema.isTransient() ? LockDesc::XCL : LockDesc::WR;

            _lock = std::shared_ptr<LockDesc>(
                std::make_shared<LockDesc>(
                    nsName,
                    arrayName,
                    mst::getLockingQueryID(query),
                    Cluster::getInstance()->getLocalInstanceId(),
                    LockDesc::WORKER,
                    lockMode));
            if (lockMode == LockDesc::WR) {
                SCIDB_ASSERT(!_schema.isTransient());
                _lock->setArrayVersion(version);
                std::shared_ptr<Query::ErrorHandler> ptr = std::make_shared<UpdateErrorHandler>(_lock);
                query->pushErrorHandler(ptr);
            }

            Query::Finalizer f = std::bind(&UpdateErrorHandler::releaseLock,
                                           _lock,
                                           std::placeholders::_1);
           query->pushVotingFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker(std::bind(&Query::validate, query));
           if (!SystemCatalog::getInstance()->lockArray(_lock, errorChecker)) {
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)<< _lock->toString();
           }
        }

        std::shared_ptr<Array> dstArray = performDeletion(query);

        getInjectedErrorListener().throwif(__LINE__, __FILE__);

        mst::installFinalizers(query, _lock);

        return fetch ? dstArray : std::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalDelete, "delete", "physicalDelete")

}  // namespace scidb
