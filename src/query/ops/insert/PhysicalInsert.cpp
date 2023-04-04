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

#include <array/Array.h>
#include <array/ArrayName.h>
#include <array/DBArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <log4cxx/logger.h>
#include <query/Expression.h>
#include <query/Multiquery.h>
#include <query/PhysicalUpdate.h>
#include <query/QueryProcessor.h>
#include <query/TypeSystem.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <system/SystemCatalog.h>
#include <util/SpatialType.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_insert"));

// main operator

class PhysicalInsert: public PhysicalUpdate
{
public:
    /// @see PhysicalOperator::PhysicalOperator()
    PhysicalInsert(const string& logicalName, const string& physicalName,
                   const Parameters& parameters, const ArrayDesc& schema);

    /// @see PhysicalOperator::getDistributionRquirement()
    DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const override;

    /// @see PhysicalOperator::getOutputBoundaries()
    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const override;

    /// @see PhysicalOperator::acceptsPullSG()
    virtual bool acceptsPullSG(size_t input) const { return true; }

    /// @see PhysicalOperator::requiresRedimensionOrPartition()
    void requiresRedimensionOrRepartition(vector<ArrayDesc> const&   inputSchemas,
                                          vector<ArrayDesc const*>&  modifiedPtrs) const override;

    /// @see PhysicalOperator::getOutputDistribution()
    RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                              std::vector<ArrayDesc> const& inputSchemas) const override;

    // isSingleThreaded


    // preSingleExecute


    /// @see PhysicalOperator::execute()
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override;
private:
    /**
    * Find the descriptor for the previous version and populate placeHolder with it.
    * @param[out] placeholder the returned descriptor
    */
    void getPreviousDesc(ArrayDesc& previousDesc) const;

    /**
     * Internal helper: write a cell from sourceIter to outputIter at pos and set flag to true.
     * @param sourceIter a chunk iterator to write from
     * @param outputIter a chunk iterator to write to
     * @param pos the position where to write the element
     * @param flag variable that is set to true after writing
     */
    void writeFrom(std::shared_ptr<ConstChunkIterator>& sourceIter,
                   std::shared_ptr<ChunkIterator>& outputIter,
                   Coordinates const* pos, bool& flag) ;
    /**
     * Merge previous version chunk with new chunk and insert result into the target chunk.
     * @param query the query context
     * @param materializedInputChunk a materialized chunk from input
     * @param existingChunk an existing chunk from the previous version
     * @param newChunk the newly created blank chunk to be written
     * @param nDims the number of dimensions
     */
    void insertMergeChunk(std::shared_ptr<Query>& query,
                          ConstChunk* materializedInputChunk,
                          ConstChunk const& existingChunk,
                          Chunk& newChunk,
                          size_t nDims,
                          Coordinates * chunkStart = nullptr, Coordinates * chunkEnd = nullptr);
    /**
     * Insert inputArray into a new version based on _schema, update catalog boundaries.
     * @param inputArray the input to insert
     * @param query the query context
     * @param currentLowBound the current lower-bound coordinates of the data in the previous version
     * @param currentHiBound the current hi-bound coordinates of the data in the previous version
     */
    std::shared_ptr<Array> performInsertion(std::shared_ptr<Array>& inputArray,
                                       std::shared_ptr<Query>& query,
                                       Coordinates const& currentLowBound,
                                       Coordinates const& currentHiBound,
                                       size_t const nDims);

    /**
     * Get writeable chunk at chunkPos, whether it already exists or not.
     *
     * @details Try to allocate a new chunk at the given position.  If
     * that fails because a chunk already exists at that position,
     * then pin that (writeable) ConstChunk and upcast it so we can
     * udpate it.
     */
    Chunk&
    getNewChunk(const Coordinates& chunkPos,
                const std::shared_ptr<ArrayIterator> & outputIter);

//
// private data
//
    ArrayDesc _previousVersionDesc; // Descriptor of previous version. Not initialized if not applicable.
};

//
// begin implementation
//

// ctor helper
static string getArrayName(const Parameters& parameters) {
    SCIDB_ASSERT(!parameters.empty());
    return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
}

// ctor
PhysicalInsert::PhysicalInsert(const string& logicalName, const string& physicalName,
                               const Parameters& parameters, const ArrayDesc& schema)
:
    PhysicalUpdate(logicalName, physicalName, parameters, schema,
                   getArrayName(parameters))
{}

// getDistributionRequirement
DistributionRequirement
PhysicalInsert::getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const
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
        LOG4CXX_TRACE(logger, "insert() input req distro: "<< distro);

        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder,
                                       vector<RedistributeContext>(1, distro));
}

// getDistType not overridden

/// @see PhysicalOperator::getOutputDistribution()
RedistributeContext
PhysicalInsert::getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                      std::vector<ArrayDesc> const& inputSchemas) const
{
        // the schema is determined by the catalog
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
}

/// @see PhyiscalOperator::getOutuputBoundaries()
PhysicalBoundaries
PhysicalInsert::getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                    const std::vector< ArrayDesc> & inputSchemas) const
{
        ArrayDesc prevVersionDesc;
        getPreviousDesc(prevVersionDesc);
        if (prevVersionDesc.getVersionId() == 0)
        {
            return inputBoundaries[0];
        }
        else
        {
            Coordinates currentLo = prevVersionDesc.getLowBoundary();
            Coordinates currentHi = prevVersionDesc.getHighBoundary();
            PhysicalBoundaries currentBoundaries(currentLo, currentHi);
            return currentBoundaries.unionWith(inputBoundaries[0]);
        }
}

/// @see PhyiscalOperator::requiresRedimensionOrRepartition()
void PhysicalInsert::requiresRedimensionOrRepartition(vector<ArrayDesc> const&   inputSchemas,
                                                      vector<ArrayDesc const*>&  modifiedPtrs) const
{
        repartForStoreOrInsert(inputSchemas, modifiedPtrs);
}

// preSingleExecute not overridden

// execute
std::shared_ptr<Array>
PhysicalInsert::execute(vector< std::shared_ptr<Array> >& inputArrays,
                        std::shared_ptr<Query> query)
{
        SCIDB_ASSERT(inputArrays.size() == 1);
        executionPreamble(inputArrays[0], query);

        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == getVersionFromName(_schema.getName())); // paranoid

        bool fetch = DEFAULT_FETCH_UPDATE_RESULTS;
        Parameter p = findKeyword("_fetch");
        if (p) {
            auto pexp = dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            fetch = pexp->getExpression()->evaluate().getBool();
        }

        string nsName, arrayName;
        query->getNamespaceArrayNames(getArrayName(_parameters), nsName, arrayName);
        SCIDB_ASSERT(isNameUnversioned(arrayName)); // paranoid

        //
        // pre lock transient case
        //
        if (_schema.isTransient())
        {
            std::shared_ptr<Array> materialized(new MemArray(inputArrays[0]->getArrayDesc(), query));
            materialized->appendHorizontal(inputArrays[0]);
            inputArrays[0] = materialized;
        }

        //
        // acquire locks (nearly identical to store)
        //
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
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << _lock->toString();
           }
           SCIDB_ASSERT(_lock->getLockMode() == lockMode);
        }

        //
        // post lock transient case
        //
        if (_schema.isTransient())
        {
            // no post lock transient case for insert
        }
        // end transient

        size_t nDims = _schema.getDimensions().size();

        //
        // boundary calculation
        //

        // Start with an empty box
        Coordinates currentLo(nDims, CoordinateBounds::getMax());
        Coordinates currentHi(nDims, CoordinateBounds::getMin());

        if(query->isCoordinator())
        {
            // update the empty box with boundaries from the previous array version
            if(_previousVersionDesc.getUAId() == 0) {
                getPreviousDesc(_previousVersionDesc);
            }

            if(_previousVersionDesc.getVersionId() != 0) {
                currentLo = _previousVersionDesc.getLowBoundary();
                currentHi = _previousVersionDesc.getHighBoundary();
            }
        }
        // at the end of performInsertion all of the local boundaries will be combined
        // to form the new global boundaries by the coordinator

        //
        // DELEGATE INSERTION
        //
        std::shared_ptr<Array> dstArray = performInsertion(inputArrays[0], query, currentLo, currentHi, nDims);

        getInjectedErrorListener().throwif(__LINE__, __FILE__);

        mst::installFinalizers(query, _lock);

        return fetch ? dstArray : std::shared_ptr<Array>();
}

//
// private methods
//

void PhysicalInsert::getPreviousDesc(ArrayDesc& previousDesc) const
{
        //XXX TODO: avoid these catalog calls by getting the latest version in LogicalInsert
        const string& arrayNameOrg = getArrayName(_parameters);

        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        SystemCatalog::GetArrayDescArgs args;
        args.result = &previousDesc;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.throwIfNotFound = true;
        if (_schema.getId() != _schema.getUAId()) {
            //new version was already created; locate the previous
            SCIDB_ASSERT(_schema.getId() > _schema.getUAId());
            args.versionId = _schema.getVersionId() - 1;
            if (args.versionId == 0) {
                return;
            }
        } else {
            // new version (our version) was not created yet
            args.versionId = LAST_VERSION;
        }
        SystemCatalog::getInstance()->getArrayDesc(args);
}

void PhysicalInsert::writeFrom(std::shared_ptr<ConstChunkIterator>& sourceIter,
                               std::shared_ptr<ChunkIterator>& outputIter,
                               Coordinates const* pos, bool& flag)
{
        outputIter->setPosition(*pos);
        outputIter->writeItem(sourceIter->getItem());
        flag = true;
}

void PhysicalInsert::insertMergeChunk(std::shared_ptr<Query>& query,
                                      ConstChunk* materializedInputChunk,
                                      ConstChunk const& existingChunk,
                                      Chunk& newChunk,
                                      size_t nDims,
                                      Coordinates * chunkStart,
                                      Coordinates * chunkEnd)
{
        std::shared_ptr<ConstChunkIterator> inputCIter =
            materializedInputChunk->getConstIterator(ConstChunkIterator::DEFAULT);
        std::shared_ptr<ConstChunkIterator> existingCIter =
            existingChunk.getConstIterator(ConstChunkIterator::DEFAULT);
        std::shared_ptr<ChunkIterator> outputCIter =
            newChunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);

        Coordinates const* inputPos    = inputCIter->end() ? nullptr : &inputCIter->getPosition();
        Coordinates const* existingPos = existingCIter->end() ? nullptr : &existingCIter->getPosition();

        const Coordinates& firstPos = existingChunk.getFirstPosition(false/*no overlap*/);
        const Coordinates& lastPos  = existingChunk.getLastPosition(false/*no overlap*/);
        SpatialRange chunkDataBounds( firstPos, lastPos );
        bool hasOverlap = existingChunk.getArrayDesc().hasOverlap();

        SCIDB_ASSERT ( ((chunkStart == nullptr ) == ( chunkEnd == nullptr )) );

        bool const wantMBR = ( chunkStart != nullptr ) && ( chunkEnd != nullptr );

        if ( wantMBR ) {
            setCoordsMax ( *chunkStart );
            setCoordsMin ( *chunkEnd );
        }

        while ( inputPos || existingPos )
        {
            bool nextInput    = false;
            bool nextExisting = false;

            if (inputPos == nullptr)
            {
                writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
            }
            else if (existingPos == nullptr)
            {
                writeFrom(inputCIter, outputCIter, inputPos, nextInput);
            }
            else
            {
                int64_t res = coordinatesCompare(*inputPos, *existingPos);
                if ( res < 0 )
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                }
                else if ( res > 0 )
                {
                    writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
                }
                else
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                    nextExisting = true;
                }
            }
            //
            //  If ...
            //    ( a ) we care about tracking the boundaries ...
            //    ( b ) ... and we're dealing with a cell from the input side ...
            //    ( c ) ... and that cell is not in an overlap region ...
            //
            //  then use the cell's position to update this chunk's MBR.
            //
            if (( wantMBR ) &&
                ( nullptr != inputPos ) &&
                ( !hasOverlap || chunkDataBounds.contains(*inputPos))) {
                resetCoordMin ( *chunkStart, *inputPos );
                resetCoordMax ( *chunkEnd, *inputPos );
            }

            //
            //  Pick the next cell(s) to process ...
            if(inputPos && nextInput)
            {
                ++(*inputCIter);
                inputPos = inputCIter->end() ? nullptr : &inputCIter->getPosition();
            }
            if(existingPos && nextExisting)
            {
                ++(*existingCIter);
                existingPos = existingCIter->end() ? nullptr : &existingCIter->getPosition();
            }
        }

        outputCIter->flush();
}


std::shared_ptr<Array>
PhysicalInsert::performInsertion(std::shared_ptr<Array>& inputArray,
                                 std::shared_ptr<Query>& query,
                                 Coordinates const& currentLowBound,
                                 Coordinates const& currentHiBound,
                                 size_t const nDims)
{
        const size_t nAttrs = _schema.getAttributes().size();
        std::shared_ptr<Array> dstArray;


        if (_schema.isTransient()) {  // obtain the special MemArray
            LOG4CXX_DEBUG(logger, __func__ << "transient output special case"); // why?
            dstArray = transient::lookup(_schema,query);
            transient::remove(_schema);
            query->pushVotingFinalizer(std::bind(&PhysicalUpdate::recordTransient,
                                                 this,
                                                 static_pointer_cast<MemArray>(dstArray),
                                                 std::placeholders::_1));
        } else {
            dstArray = DBArray::createDBArray(_schema, query);
        }

        SCIDB_ASSERT(dstArray->getArrayDesc().getAttributes(true).size() ==
                     inputArray->getArrayDesc().getAttributes(true).size());
        SCIDB_ASSERT(dstArray->getArrayDesc().getId()   == _schema.getId());
        SCIDB_ASSERT(dstArray->getArrayDesc().getUAId() == _schema.getUAId());

        query->getReplicationContext()->enableInboundQueue(_schema.getId(), dstArray);

        PhysicalBoundaries bounds(currentLowBound, currentHiBound);

        if (inputArray->getArrayDesc().getEmptyBitmapAttribute() == nullptr && _schema.getEmptyBitmapAttribute()) {
            inputArray = std::make_shared<NonEmptyableArray>(inputArray);
        }

        vector<std::shared_ptr<ConstArrayIterator> > inputIters(nAttrs);    // for input
        vector<std::shared_ptr<ConstArrayIterator> > existingIters(nAttrs); // for existing data in output
        vector<std::shared_ptr<ArrayIterator> > outputIters(nAttrs);        // for writing output

        for (const auto& attr : inputArray->getArrayDesc().getAttributes()) {
            inputIters[attr.getId()] = inputArray->getConstIterator(attr);
            existingIters[attr.getId()] = dstArray->getConstIterator(attr);
            outputIters[attr.getId()] = dstArray->getIterator(attr);
        }

        while(!inputIters[0]->end()) {                             // while more chunks
            Coordinates const& pos = inputIters[0]->getPosition();
            if (!_schema.contains(pos)) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                    << CoordsToStr(pos) << _schema.getDimensions();
            }

            Coordinates chunkStart(pos.size());                    // for bounds tracking
            Coordinates chunkEnd(pos.size());

            bool hasExistingChunk = existingIters[0]->setPosition(pos);  // merging vs adding a chunk

            for (AttributeID i = 0; i < nAttrs; i++) {      // for all attributes in current chunk
                if ( hasExistingChunk && i != 0 ) {         // [0] already set
                    existingIters[i]->setPosition(pos);    // mmm... why is this existing rather than outputIters?
                }

                ConstChunk const& inputChunk = inputIters[i]->getChunk();
                ConstChunk* matChunk = inputChunk.materialize();
                if(matChunk->count() == 0) {
                    break;
                }

                //  IF ( last attribute of the chunk (position/empty info) AND
                //       chunk is not wholly within the existing chunk )
                //  THEN boundary requires updating
                const bool isInside = bounds.isInsideBox ( inputChunk.getFirstPosition(/* ignore overlap*/false) ) &&
                                      bounds.isInsideBox ( inputChunk.getLastPosition(/* ignore overlap*/ false) );
                const bool doBoundUpdating = (i == nAttrs-1) && (!isInside);

                if (hasExistingChunk) {    // requires insertion
                    if (doBoundUpdating) {
                        insertMergeChunk(query, matChunk, existingIters[i]->getChunk(),
                                         getNewChunk(pos,outputIters[i]), nDims,
                                         &chunkStart, &chunkEnd );
                    } else {
                        insertMergeChunk(query, matChunk, existingIters[i]->getChunk(),
                                          getNewChunk(pos,outputIters[i]), nDims);
                    }
                } else {
                   if (doBoundUpdating) {
                       outputIters[i]->copyChunk(*matChunk, &chunkStart, &chunkEnd );
                   } else {
                       outputIters[i]->copyChunk(*matChunk);
                   }
                }

                if (doBoundUpdating) {
                    bounds = bounds.unionWith( PhysicalBoundaries ( chunkStart, chunkEnd ) );
                }
            }

            // advance iterators
            for(auto curIter : inputIters) {
                (*curIter).operator++();
            }
        }

        // Update boundaries
        updateSchemaBoundaries(_schema, bounds, query);

        if (!_schema.isTransient())
        {
            query->getReplicationContext()->replicationSync(_schema.getId());
            query->getReplicationContext()->removeInboundQueue(_schema.getId());
            dstArray->flush();
        }

        return dstArray;
}

Chunk&
PhysicalInsert::getNewChunk(const Coordinates& chunkPos,
                            const std::shared_ptr<ArrayIterator> & outputIter)
{
        Chunk* chunk = nullptr;
        try {
            chunk = &outputIter->newChunk(chunkPos);
            SCIDB_ASSERT(chunk);
        } catch (const SystemException& err) {
            if (err.getLongErrorCode() != SCIDB_LE_CHUNK_ALREADY_EXISTS
                    || !_schema.isTransient()) {
                err.raise();
            }
            bool rc = outputIter->setPosition(chunkPos);
            ASSERT_EXCEPTION(rc, "PhysicalInsert::getNewChunk");
            chunk = &outputIter->updateChunk();
            SCIDB_ASSERT(chunk);
        }
        return *chunk;
}

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInsert, "insert", "physicalInsert")

}  // namespace ops
