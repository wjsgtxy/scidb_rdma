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
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include <query/PhysicalOperator.h>

#include <array/MemArray.h>
#include <network/MessageDesc.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Aggregate.h>
#include <query/OperatorLibrary.h>
#include <query/UserQueryException.h>
#include <system/Config.h>
#include <system/SystemCatalog.h>
#include <util/Indent.h>
#include <util/InjectedError.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));


DistributionRequirement::DistributionRequirement (reqType rt,
                                                  std::vector<RedistributeContext> specificRequirements)
:   _requiredType(rt),
    _specificRequirements(specificRequirements)
{
    if ((_requiredType == SpecificAnyOrder || _specificRequirements.size() != 0) &&
        (_requiredType != SpecificAnyOrder || _specificRequirements.size() <= 0))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_SPECIFIC_DISTRIBUTION_REQUIRED);
    }
    // up to here, relocated from header roughly at sha ccd501bff

    // specific requirements must not contain dtUndfined, as that is not satsfiable
    for (const RedistributeContext& rc : _specificRequirements) {
        ASSERT_EXCEPTION(not rc.isUndefined(), "distribution requirement dtUndefined not satisfiable");
    }
}

PhysicalOperator::PhysicalOperator(std::string const& logicalName,
                                   std::string const& physicalName,
                                   Parameters const& parameters,
                                   ArrayDesc const& schema)
:
    _parameters(parameters),
    _schema(schema),
    _tileMode(false),
    _logicalName(logicalName),
    _physicalName(physicalName)
{
    LOG4CXX_TRACE(logger, "PhysicalOperator " << physicalName
                       << " Distribution" << distTypeToStr(_schema.getDistribution()->getDistType()));
}

const std::string& PhysicalOperator::getPhysicalName() const { return _physicalName; }
const std::string& PhysicalOperator::getOperatorName() const { return getPhysicalName(); }

PhysicalOperator::~PhysicalOperator()
{ }

void PhysicalOperator::setKeywordParameters(KeywordParameters& kwParams)
{
    // Just take them, physical plan creator doesn't need them anymore.
    _kwParameters.swap(kwParams);
    setKeywordParamHook();
}

void PhysicalOperator::setOperatorContext(std::shared_ptr<OperatorContext> const& opContext,
                                         std::shared_ptr<JobQueue> const& jobQueue)
{
    static const char * funcName = "PhysOp::setOperatorContext()" ;
    SCIDB_ASSERT(opContext);
    SCIDB_ASSERT(!_operatorContext);

    _operatorContext = opContext;

    LOG4CXX_TRACE(logger, funcName << " this " << static_cast<void*>(this)
                                   << " getPhysicalName() " << getPhysicalName()
                                   << " getOperatorID() " << getOperatorID()
                                   << " operatorContext " << static_cast<void*>(_operatorContext.get()));

    if(false){ // TODO: need to eliminate need for operators to
               //       to start the SG queue ... could run
               //       for all query duration
        // NOTE: query->startSGQueue() is still called externally
        // NOTE: sort depends on the current behavior
        assert(_query.lock()->getSGQueue());
        _query.lock()->getSGQueue()->start(jobQueue);
    }
}

std::shared_ptr<OperatorContext> PhysicalOperator::getOperatorContext() const
{
    return _operatorContext;
}

void PhysicalOperator::unsetOperatorContext()
{
    static const char * funcName = "PhysOp::unsetOperatorContext()" ;

    LOG4CXX_TRACE(logger, funcName << " this " << static_cast<void*>(this)
                                   << " getPhysicalName() " << getPhysicalName()
                                   << " getOperatorID() " << getOperatorID()
                                   << " operatorContext " << static_cast<void*>(_operatorContext.get()));

    if (_operatorContext) {
        _operatorContext.reset();
    } else {
        LOG4CXX_TRACE(logger, funcName << " returning early, nothing to unset");
    }

    if(false){ // TODO: need to eliminate need to stop the SG queue
               //       see setOperatorContext
        // NOTE: query->stopSGQueue() still called externally
        // NOTE: sort might depend on the current behavior
        assert(_query.lock()->getSGQueue());
        _query.lock()->getSGQueue()->stop();
    }
}

void PhysicalOperator::setQuery(const std::shared_ptr<Query>& query)
{
    auto myQuery = _query.lock();
    if(!myQuery) {  // have never registered myself for message lookup
        LOG4CXX_TRACE(logger, "PhysOp::setQuery(): new getOperatorID() " << getOperatorID() );
        query->registerPhysicalOperator(shared_from_this());

        arena::Options options;                              // Arena ctor args
        options.name  (_physicalName.c_str());               // Use operator name
        options.parent(query->getArena());                   // Attach to query
        options.threading(!isSingleThreaded());              // the op may be multi-threaded
        _arena = arena::newArena(options);                   // Create new Arena

        _query = query; // note that setQuery has now been called once
    } else {
        SCIDB_ASSERT(myQuery.get() == query.get()); // must match on subsequent calls
    }
}

OperatorID PhysicalOperator::getOperatorID() const
{
    return _operatorID ;
}

void PhysicalOperator::setOperatorID(const OperatorID& opID)
{
    _operatorID = opID;
}

void PhysicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "schema " <<_schema<<"\n";
}

void PhysicalOperator::dumpArrayToLog(std::shared_ptr<Array> const& input, log4cxx::LoggerPtr& logger)
{
    ArrayDesc const& schema = input->getArrayDesc();
    Attributes const& attrs = schema.getAttributes(true);
    AttributeID const nAttrs = safe_static_cast<AttributeID>(attrs.size());
    vector<FunctionPointer> converters(nAttrs,NULL);
    FunctionLibrary *functionLib = FunctionLibrary::getInstance();
    vector<std::shared_ptr<ConstArrayIterator> > aiters (nAttrs);
    vector<std::shared_ptr<ConstChunkIterator> > citers (nAttrs);
    for (const auto& attr : attrs)
    {
        TypeId const& typeId = attr.getType();
        converters[attr.getId()] = functionLib->findConverter(typeId, TID_STRING, false, false, NULL);
        aiters[attr.getId()] = input->getConstIterator(attr);
    }
    const auto& fda = attrs.firstDataAttribute();
    while (!aiters[fda.getId()]->end())
    {
        for (const auto& attr : attrs)
        {
            citers[attr.getId()] =
                aiters[attr.getId()]->getChunk().getConstIterator(ConstChunkIterator::DEFAULT);
        }
        while (!citers[fda.getId()]->end())
        {
            Coordinates const& position = citers[fda.getId()]->getPosition();
            ostringstream out;
            out<<CoordsToStr(position)<<" ";
            for (const auto& attr : attrs)
            {
                Value const& v = citers[attr.getId()]->getItem();
                if (v.isNull())
                {
                    if(v.getMissingReason() == 0)
                    {
                        out<<"[null]";
                    }
                    else
                    {
                        out<<"[?"<<v.getMissingReason()<<"]";
                    }
                }
                else if (converters[attr.getId()])
                {
                    Value const* input = &v;
                    Value result;
                    converters[attr.getId()](&input, &result, NULL);
                    out<<result.getString();
                }
                else
                {
                    out<<"[nct]";
                }
                out<<",";
            }
            LOG4CXX_DEBUG(logger, out.str());
            for (const auto& attr : attrs)
            {
                ++(*citers[attr.getId()]);
            }
        }
        for (const auto& attr : attrs)
        {
            ++(*aiters[attr.getId()]);
        }
    }
}

std::shared_ptr<Array>
PhysicalOperator::ensureRandomAccess(std::shared_ptr<Array>& input,
                                     std::shared_ptr<Query> const& query)
{
    if (input->getSupportedAccess() != Array::RANDOM)
    {
        LOG4CXX_DEBUG(logger, "Query "<<query->getQueryID()<<
                      " materializing input "<<input->getArrayDesc());
        auto memArr = std::make_shared<MemArray>(input->getArrayDesc(), query);
        SCIDB_ASSERT(memArr);
        memArr->appendHorizontal(input);
        input = memArr;
    }

    return input;
}

RedistributeContext
PhysicalOperator::getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                        std::vector<ArrayDesc> const& inputSchemas) const
{
    if (inputDistributions.empty()) {

        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);
        return RedistributeContext(createDistribution(getSynthesizedDistType()),
                                   query->getDefaultArrayResidency());
    } else {
        // derive it from the first input distribution
        _schema.setResidency(inputDistributions[INPUT_INDEX_LEFT].getArrayResidency());
        ArrayDistPtr dist = convertToDefaultRedundancy(inputDistributions[INPUT_INDEX_LEFT].getArrayDistribution());

        // ensure replicated is not output when not accepted on the leftmost input
        // see OperatorDist::inferSynthesizedDistType
        auto curDistType = dist->getDistType();
        if(isReplicated(curDistType)) {   // not the same as !isPartition()
            // must check whether we can even allow replicated input
            bool isReplicatedOk = isReplicatedInputOk(inputDistributions.size())[INPUT_INDEX_LEFT];
            if (not isReplicatedOk) {
                dist = createDistribution(defaultDistType()); // always a partition
                LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesized dist type: operator " << getOperatorName()
                                      << " " << distTypeToStr(curDistType) << " not acceptable,"
                                      << " changed to defaultDistType() " << distTypeToStr(defaultDistType()));
            }
        }

        _schema.setDistribution(dist);

        // NOTE: we might think _schema's DistType would always agree with
        //       getSynthesizedDistType() or getOutputDistribution()'s but that is not true.
        //       1. During failover, PhysicalScan returns dtUndefined, but cannot change
        //          _schema to that, because the storage manager does not allow that
        //          even for reading.
        //          TODO JHM: fix this before next major release.  Note that PhysicalScan
        //                is confusing its input schema (from the storage manager) from its output
        //                _schema, which must be allowed to change to dtUndefined during failover,
        //                as that is the truth.
        //       2. redistribute outputs dtLocalDist to force Sg insertion as a side effect
        //
        if(_schema.getDistribution()->getDistType() != getSynthesizedDistType() &&
           not isUndefined(getSynthesizedDistType())) {
            LOG4CXX_ERROR(logger, "PhysicalOperator::getOutputDistribution: mismatch operator: "<< getPhysicalName());
            LOG4CXX_ERROR(logger, "PhysicalOperator::getOutputDistribution: _schema.getDistribution()->getDistType() "
                                   << distTypeToStr(_schema.getDistribution()->getDistType()));
            LOG4CXX_ERROR(logger, "PhysicalOperator::getOutputDistribution: getSynthesizedDistType()"
                                   << distTypeToStr(getSynthesizedDistType()));
        }
        ASSERT_EXCEPTION(_schema.getDistribution()->getDistType() == getSynthesizedDistType() ||
                         isUndefined(getSynthesizedDistType()),
                         "_schema and getSynthesizedDistType mismatch, see scidb.log");

        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }
}

ArrayDistPtr
PhysicalOperator::convertToDefaultRedundancy(ArrayDistPtr const& inputDist)
{
    if ( inputDist->getRedundancy() != DEFAULT_REDUNDANCY) {
        size_t instanceShift(0);
        ArrayDistributionFactory::getTranslationInfo(inputDist.get(), instanceShift);

        ArrayDistPtr dist =
        ArrayDistributionFactory::getInstance()->construct(inputDist->getDistType(),
                                                           DEFAULT_REDUNDANCY,
                                                           inputDist->getState(),
                                                           instanceShift);
        return dist;
    }
    return inputDist;
}

/**
 * Broadcast a "sync" message and wait for an acknowledgement from every instance.
 */
void sync(NetworkManager* networkManager, const std::shared_ptr<Query>& query, uint64_t instanceCount, bool isSendLocal=false)
{
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtSyncRequest);
    std::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(query->getQueryID());

    LOG4CXX_DEBUG(logger, "sync: Broadcast");
    networkManager->broadcastLogical(msg);
    if (isSendLocal) {
         networkManager->sendLocal(query, msg);
        ++instanceCount;
    }

    LOG4CXX_DEBUG(logger, "sync: Waiting for " << instanceCount-1 << " others");
    Semaphore::ErrorChecker ec = std::bind(&Query::validate, query);
    query->syncSG.enter(instanceCount - 1, ec, PTW_SEM_SG_RCV);

    LOG4CXX_DEBUG(logger, "sync: done.")
}

void barrier(uint64_t barrierId,
             NetworkManager* networkManager,
             const std::shared_ptr<Query>& query,
             uint64_t instanceCount)
{
    std::shared_ptr<MessageDesc> barrierMsg = std::make_shared<MessageDesc>(mtBarrier);
    std::shared_ptr<scidb_msg::DummyQuery> barrierRecord = barrierMsg->getRecord<scidb_msg::DummyQuery>();
    barrierMsg->setQueryID(query->getQueryID());
    barrierRecord->set_payload_id(barrierId);

    LOG4CXX_DEBUG(logger, "barrier: Broadcast");
    networkManager->broadcastLogical(barrierMsg);

    LOG4CXX_DEBUG(logger, "barrier: Waiting waiting for " << instanceCount - 1 << " others");
    Semaphore::ErrorChecker ec = std::bind(&Query::validate, query);
    query->semSG[barrierId].enter(instanceCount - 1, ec, PTW_SEM_QUERY_BAR);

    LOG4CXX_DEBUG(logger, "barrier: done.")
}

/**
 * This can be used as a barrier mechanism across the cluster in a blocking/materializing operator
 * @note: redistributeXXX() uses the same mechanism.
 * @todo: make a method directly on query?
 * @note: These methods on operator could remain (if needed).
 *        They would just be wrappers then.
 */
void syncBarrier(uint64_t barrierId, const std::shared_ptr<Query>& query)
{
    LOG4CXX_DEBUG(logger, "syncBarrier: barrierId = " << barrierId);
    SCIDB_ASSERT(query);
    NetworkManager* networkManager = NetworkManager::getInstance();
    SCIDB_ASSERT(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    SCIDB_ASSERT(instanceCount > 0);
    barrier(barrierId%MAX_BARRIERS, networkManager, query, instanceCount);
    LOG4CXX_DEBUG(logger, "syncBarrier: returning");
}

void syncSG(const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(query);
    LOG4CXX_DEBUG(logger, "syncSG: queryID="<<query->getQueryID());
    NetworkManager* networkManager = NetworkManager::getInstance();
    SCIDB_ASSERT(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    SCIDB_ASSERT(instanceCount > 0);
    sync(networkManager, query, instanceCount, true);
    LOG4CXX_DEBUG(logger, "syncSG: returning");
}

AggregatePtr resolveAggregate(std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                              Attributes const& inputAttributes,
                              AttributeDesc* inputAttributeID,
                              string* outputName)
{
    const std::shared_ptr<OperatorParam> &acParam = aggregateCall->getInputAttribute();

    try
    {
        if (PARAM_ASTERISK == acParam->getParamType())
        {
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), TypeLibrary::getType(TID_VOID));

            if (inputAttributeID)
            {
                *inputAttributeID = AttributeDesc();
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : agg->getName();
            }
            return agg;
        }
        else if (PARAM_ATTRIBUTE_REF == acParam->getParamType())
        {
            const std::shared_ptr<OperatorParamAttributeReference> &ref = (const std::shared_ptr<OperatorParamAttributeReference>&) acParam;

            AttributeDesc const& inputAttr = inputAttributes.findattr(ref->getObjectNo());
            Type const& inputType = TypeLibrary::getType(inputAttr.getType());
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), inputType);

            if (inputAttributeID)
            {
                *inputAttributeID = inputAttr;
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : inputAttr.getName() + "_" + agg->getName();
            }
            return agg;
        }
        else
        {
            // All other cases must have been thrown already during translation.
            // (If assertions disabled, on to throw statement at end of function.)
            SCIDB_ASSERT(0);
        }
    }
    catch(const UserException &e)
    {
        if (SCIDB_LE_AGGREGATE_NOT_FOUND == e.getLongErrorCode())
        {
            throw CONV_TO_USER_QUERY_EXCEPTION(e, acParam->getParsingContext());
        }

        e.raise();
    }

    SCIDB_ASSERT(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE)
        << __FUNCTION__;
    return AggregatePtr();
}

void addAggregatedAttribute (
        std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
        ArrayDesc const& inputDesc,
        ArrayDesc& outputDesc,
        bool operatorDoesAggregationInOrder)
{
    string outputName;

    AggregatePtr agg = resolveAggregate(aggregateCall, inputDesc.getAttributes(), nullptr, &outputName);

    if ( !operatorDoesAggregationInOrder && agg->isOrderSensitive() ) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << agg->getName();
    }

    outputDesc.addAttribute(
        AttributeDesc(
            outputName,
            agg->getResultType().typeId(),
            AttributeDesc::IS_NULLABLE,
            CompressorType::NONE));
}

void BasePhysicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addPhysicalOperatorFactory(this);
}


namespace {
    scidb::InjectedErrorListener theListener(InjectErrCode::OPERATOR);
}

InjectedErrorListener& PhysicalOperator::getInjectedErrorListener()
{
    theListener.start();
    return theListener;
}

std::shared_ptr<ThreadPool> PhysicalOperator::_globalThreadPoolForOperators;
std::shared_ptr<JobQueue> PhysicalOperator::_globalQueueForOperators;
Mutex PhysicalOperator::_mutexGlobalQueueForOperators;

std::shared_ptr<JobQueue> PhysicalOperator::getGlobalQueueForOperators()
{
    ScopedMutexLock cs(_mutexGlobalQueueForOperators, PTW_SML_OPERATOR);
    if (!_globalThreadPoolForOperators) {
        _globalQueueForOperators = std::shared_ptr<JobQueue>(new JobQueue("GlobalJobQueueForOperators"));
        _globalThreadPoolForOperators = std::shared_ptr<ThreadPool>(
                new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_THREADS),
                               _globalQueueForOperators,
                               "globalOperatorThreadPool"));
        _globalThreadPoolForOperators->start();
    }
    return _globalQueueForOperators;
}


void PhysicalOperator::checkOrUpdateIntervals(ArrayDesc& checkMe, std::shared_ptr<Array>& input)
{
    checkOrUpdateIntervals(checkMe, input->getArrayDesc().getDimensions());
}

void PhysicalOperator::checkOrUpdateIntervals(ArrayDesc& checkMe, Dimensions const& inDims)
{
    // If the input schema was autochunked, we must now fix it up with actual intervals on *all* the
    // instances.  If *not* autochunked... well, we need to scan the dimensions anyway to find that
    // out... so we might as well do this inexpensive check in that case as well.

    Dimensions& cDims = checkMe.getDimensions();
    const size_t N_DIMS = cDims.size();
    SCIDB_ASSERT(inDims.size() == N_DIMS);

    size_t const NONE = std::numeric_limits<size_t>::max();
    size_t failedDim = NONE;

    for (size_t i = 0; i < N_DIMS; ++i) {
        int64_t hardInterval = inDims[i].getChunkInterval();
        int64_t softInterval = cDims[i].getRawChunkInterval();
        if (! cDims[i].isIntervalResolved()) {
            cDims[i].setChunkInterval(hardInterval);
        } else if (softInterval != hardInterval) {
            // Remember problem dimension, we'll throw *after* we've
            // fixed the other unresolved ones.
            if (failedDim == NONE) {
                failedDim = i;
            }
        }
    }

    if (failedDim != NONE) {
        // By throwing here, we'll have wasted our earlier call to catalog->getNextArrayId(), and
        // burned an array id.  Oh well.  (This is re. calls from PhysicalUpdate.)

        // With autochunking, it's the optimizer (or something that should eventually be in the
        // optimizer) that's responsible for chunk size mismatches getting resolved.  So we use
        // that "short" error code when doing the late re-checking of the intervals.
        throw USER_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DIMENSIONS_DONT_MATCH)
            << inDims[failedDim] << cDims[failedDim];
    }
}


void PhysicalOperator::repartByLeftmost(
    vector<ArrayDesc> const& inputSchemas,
    vector<ArrayDesc const*>& modifiedPtrs) const
{
    const size_t N_SCHEMAS = inputSchemas.size();
    SCIDB_ASSERT(N_SCHEMAS > 1); // ... else you are calling the wrong canned implementation.
    SCIDB_ASSERT(N_SCHEMAS == modifiedPtrs.size());
    // NOTE:  N_SCHEMAS may (someday) be  > 2

    // Find leftmost *non-autochunked* schema, use *that* as the exemplar for
    // the following aspects of dimensions: chunkInterval and chunkOverlap.  The
    // names, start indices, and end indices of dimensions are preserved for each
    // of the input schemas.
    size_t exemplarIndex = 0;
    for (ArrayDesc const& schema : inputSchemas) {
        if (schema.isAutochunked()) {
                ++exemplarIndex;
        }
        else {
            break;
        }
    }
    // At least one of the input schemas must be non-autochunked
    if (exemplarIndex == N_SCHEMAS) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
            << getLogicalName();
    }

    // We don't expect to be called twice, but that may change later on:
    //  wipe any previous result.
    _redimRepartSchemas.clear();

    // Do not repartition leftmost (non-autochunked) input array.
    modifiedPtrs[exemplarIndex] = nullptr;

    ArrayDesc const& exemplarSchema = inputSchemas[exemplarIndex];
    const size_t     nDimensions    = exemplarSchema.getDimensions().size();
    // Check the other input schemas, adding a desired schema to modifiedPtrs
    // for each input that needs to have an automatic repart operator added.
    for (size_t iSchema = 0; iSchema < N_SCHEMAS; ++iSchema)
    {
        if (iSchema == exemplarIndex) {
            // This is the exemplar Schema. There is no need to insert a
            // repart/redimension to make the partitioning match itself.
            continue;
        }

        ArrayDesc const& targetSchema = inputSchemas[iSchema];
        if (exemplarSchema.samePartitioning(targetSchema)) {
            // Already has correct chunkSize and overlap, do nothing.
            modifiedPtrs[iSchema] = nullptr;
        }
        else {
            // Clone this schema and adjust dimensions according to exemplar.
            Dimensions const& exemplarDimensions = exemplarSchema.getDimensions();
            // maintain the names of the dimensions by starting with the
            // original dimension information from the input schema
            Dimensions targetDimensions(targetSchema.getDimensions());
            SCIDB_ASSERT(targetDimensions.size() == nDimensions);
            for (size_t iDim = 0; iDim < nDimensions; ++iDim)
            {
                DimensionDesc const & exemplarDim = exemplarDimensions[iDim];
                DimensionDesc       & targetDim   = targetDimensions[iDim];

                targetDim.setChunkInterval(exemplarDim.getChunkInterval());

                // Take smallest overlap since we can't (easily) conjure up cells that aren't there.
                targetDim.setChunkOverlap(min(exemplarDim.getChunkOverlap(),
                                              targetDim.getChunkOverlap()));
            }

            _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(
                                              targetSchema.getName(),
                                              targetSchema.getAttributes(),
                                              targetDimensions,
                                              targetSchema.getDistribution(),
                                              targetSchema.getResidency()));
            modifiedPtrs[iSchema] = _redimRepartSchemas.back().get();
        }
    }
    if (_redimRepartSchemas.empty()) {
        // Assertions elsewhere hate an all-NULLs vector here.
        modifiedPtrs.clear();
    }
}


void PhysicalOperator::repartForStoreOrInsert(
    vector<ArrayDesc> const& inputSchemas,
    vector<ArrayDesc const*>& modifiedPtrs) const
{
    SCIDB_ASSERT(inputSchemas.size() == 1);
    SCIDB_ASSERT(modifiedPtrs.size() == 1);

    // If schema matches input array, no problem.  Loose comparison allows
    // unspecified dimensions to match.
    if (_schema.sameLoosePartitioning(inputSchemas[INPUT_INDEX_LEFT])) {
        modifiedPtrs.clear();
        return;
    }

    // A repart is needed.  Start with a copy of _schema, the stored-into
    // target array, because the input must be repartitioned to match it.
    _redimRepartSchemas.clear();
    _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(_schema));

    // If any target dimension's chunk interval is unresolved, take it from
    // the input as-is.
    Dimensions const& srcDims = inputSchemas[INPUT_INDEX_LEFT].getDimensions();
    Dimensions& dstDims = _redimRepartSchemas.back()->getDimensions();
    size_t const N_DIMS = srcDims.size();
    SCIDB_ASSERT(dstDims.size() == N_DIMS);
    for (size_t d = 0; d < N_DIMS; ++d) {
        if (!dstDims[d].isIntervalResolved()) {
            dstDims[d].setRawChunkInterval(srcDims[d].getRawChunkInterval());
        }
    }

    // Request the repartition.
    modifiedPtrs[INPUT_INDEX_LEFT] = _redimRepartSchemas[INPUT_INDEX_LEFT].get();
}


void
PhysicalOperator::assertLastVersion(std::string const& nsName,
                                    std::string const& arrayName,
                                    bool arrayExists,
                                    ArrayDesc const& desc)
{
    if (isDebug()) {
        ArrayDesc tmpDesc;
        SystemCatalog::GetArrayDescArgs args;
        args.result = &tmpDesc;
        args.nsName = nsName;
        args.arrayName = arrayName;
        args.throwIfNotFound = false;
        bool found = SystemCatalog::getInstance()->getArrayDesc(args);

        SCIDB_ASSERT(arrayExists == found);

        if (arrayExists) {
            SCIDB_ASSERT(desc == tmpDesc);
        }
    }
}

void
PhysicalOperator::assertConsistency(ArrayDesc const& inputSchema,
                                    RedistributeContext const& inputDistribution)
{
   if (isDebug()) {
       bool equalResidency = inputSchema.getResidency()->isEqual(inputDistribution.getArrayResidency());
       if (!equalResidency) {
           LOG4CXX_ERROR(logger, "PhysicalOperator::assertConsistency() inputSchema residency "
                                 << inputSchema.getResidency()
                                 << " != "
                                 << inputDistribution.getArrayResidency());
       }
       SCIDB_ASSERT(equalResidency);
       //    if not a schema without data &&  dtUndefined ...
       if (inputSchema.getId() > 0 && isUndefined(inputDistribution.getArrayDistribution()->getDistType())) {
           // in degraded mode scan() reports dtUndefined but
           // it has to preserve the original distribution for SMGR to make sense of the chunks
           // so the check below is bypassed in this case
       } else {
           bool compatible = inputSchema.getDistribution()->checkCompatibility(inputDistribution.getArrayDistribution());
           if (!compatible) {
               LOG4CXX_ERROR(logger, "PhysicalOperator::assertConsistency() inputSchema (DistType "
                                     << distTypeToStr(inputSchema.getDistribution()->getDistType()) << ")"
                                     << " .getId() " << inputSchema.getId()
                                     << " is not compatible with inputDistribution (DistType "
                                     << distTypeToStr(inputDistribution.getArrayDistribution()->getDistType())
                                     << ")");
           }
           SCIDB_ASSERT(compatible);
       }
   }
}


void PhysicalOperator::inspectLogicalOp(LogicalOperator const& lop)
{
    setControlCookie(lop.getInspectable());
}

} //namespace
