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

#include <query/PhysicalUpdate.h>

#include <array/ArrayName.h>
#include <array/TransientCache.h>
#include <network/MessageDesc.h>
#include <network/NetworkManager.h>
#include <network/proto/scidb_msg.pb.h>
#include <query/Multiquery.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.physicalupdate"));

PhysicalUpdate::PhysicalUpdate(const string& logicalName,
                               const string& physicalName,
                               const Parameters& parameters,
                               const ArrayDesc& schema,
                               const std::string& catalogArrayName):
PhysicalOperator(logicalName, physicalName, parameters, schema),
_preambleDone(false),
_deferLockUpdate(false),
_unversionedArrayName(catalogArrayName),
_arrayUAID(0),
_arrayID(0),
_lastVersion(0)
{}

std::vector<uint8_t> PhysicalUpdate::isReplicatedInputOk(size_t numChildren) const
{
    // replicated input needed when output (_schema) is replicated
    bool isOutputReplicated = isReplicated(_schema.getDistribution()->getDistType());
    // SDB-1929: Don't return empty vector!  (Formerly relied on bad vector<bool> behavior.)
    vector<uint8_t> result(numChildren ? numChildren : 1, false);

    // delete operator is a use case with numChildren==0
    if (numChildren >= 1) {
        result[0] = isOutputReplicated;
    }
    return result;
}

//
// synthesis
//
DistType PhysicalUpdate::inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const
{
    LOG4CXX_TRACE(logger, "PhysicalUpdate::inferSynthesizedDistType returning: " << _schema.getDistribution()->getDistType());
    return _schema.getDistribution()->getDistType();
}

void PhysicalUpdate::preSingleExecute(std::shared_ptr<Query> query)
{
    // Figure out the array descriptor for the new array
    // Several options:
    // 1. _schema represents the unversioned array already in the catalog (obtained by the LogicalOperator)
    // 2. _schema represents the input array and the output array does not yet exist in the catalog
    // 3. _schema represents the input array and the output array may/not exist in the catalog
    //    (in case of PhysicalInput, user-inserted SG)
    // Transient arrays do not have versions; for the persistent arrays we need to update _schema
    // to represent a new version array.
    //
    // Life gets complicated when the input array is autochunked, that is, its chunk intervals are
    // not yet known and won't be known until execute() time.  In that case, we defer the work that
    // depends on having known intervals until executionPreamble() is called.
    //
    // ALL SUBCLASS execute() METHODS >>>MUST<<< CALL executionPreamble() FIRST.

    bool rc = false;

    SystemCatalog* catalog = SystemCatalog::getInstance();
    SCIDB_ASSERT(catalog);
    SCIDB_ASSERT(!_unversionedArrayName.empty());

    // throw away the alias name, and restore the catalog name
    std::string arrayName;
    std::string namespaceName;
    query->getNamespaceArrayNames(_unversionedArrayName, namespaceName, arrayName);
    _schema.setName(arrayName);
    _unversionedArrayName = arrayName;
    LOG4CXX_DEBUG(logger, "PhysicalUpdate::preSingleExecute begin: "
        << " namespaceName=" << namespaceName
        << " arrayName=" << arrayName
        << " schema: "<< _schema);

    bool isArrayInCatalog = (_schema.getId() > 0);
    if (!isArrayInCatalog) {
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);
        SystemCatalog::GetArrayDescArgs args;
        args.result = &_unversionedSchema;
        args.nsName = namespaceName;
        args.arrayName = arrayName;
        args.catalogVersion = arrayId;
        isArrayInCatalog = catalog->getArrayDesc(args);
        assertLastVersion(namespaceName, arrayName, isArrayInCatalog, _unversionedSchema);
    } else {
        SCIDB_ASSERT(_schema.getId() != INVALID_ARRAY_ID);
        SCIDB_ASSERT(_schema.getId() == _schema.getUAId());
        // LogicalOperator already obtained the unversioned schema
        _unversionedSchema = _schema;
    }

    // set up error handling
    const LockDesc::LockMode lockMode =
        _unversionedSchema.isTransient()
            ? LockDesc::XCL
            : LockDesc::WR;
    _lock = std::make_shared<LockDesc>(
        namespaceName,
        arrayName,
        mst::getLockingQueryID(query),
        Cluster::getInstance()->getLocalInstanceId(),
        LockDesc::COORD,
        lockMode);

    {  //XXX HACK to make sure we got the right (more restrictive) lock
       //XXX (the array may have been removed/inserted after we decided which lock to acquire)
        _lock->setLockMode(LockDesc::RD);
        std::shared_ptr<LockDesc> resLock = query->requestLock(_lock);
        SCIDB_ASSERT(resLock);
        _lock->setLockMode(resLock->getLockMode());
        if (_lock->getLockMode() !=  lockMode) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT) <<
            string("Transient array with name: \'") + _lock->getArrayName() +
            string("\' cannot be removed/inserted concurrently with another store/insert");
        }
    }

    if (lockMode == LockDesc::WR) {
        std::shared_ptr<Query::ErrorHandler> ptr = std::make_shared<UpdateErrorHandler>(_lock);
        query->pushErrorHandler(ptr);
    }

    const size_t redundancy = Config::getInstance()->getOption<size_t>(CONFIG_REDUNDANCY);

    if (!isArrayInCatalog) {
        // so, we need to add the unversioned array as well
        // NOTE: that transient arrays are only added by the user (via create array ...)
        SCIDB_ASSERT(_schema.getId() == 0);
        _lock->setLockMode(LockDesc::CRT);
        rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);

        LOG4CXX_TRACE(logger, "PhysicalUpdate::preSingleExecute  _schema DistType: " << _schema.getDistribution()->getDistType());
        LOG4CXX_TRACE(logger, "PhysicalUpdate::preSingleExecute !isArrayInCatalog: getSynthesizedDistType() : " << getSynthesizedDistType());
        SCIDB_ASSERT(_schema.getDistribution()->getDistType() == getSynthesizedDistType());
        SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(createDistribution(getSynthesizedDistType(), redundancy)));  // UPDATE

        if (isDebug()) {
            bool isRandomRes = (Config::getInstance()->getOption<std::string>(CONFIG_PERTURB_ARR_RES) == "random");
            SCIDB_ASSERT(isRandomRes ||_schema.getResidency()->isEqual(query->getDefaultArrayResidencyForWrite()));
        }
        _unversionedSchema = _schema;
        ArrayID uAId = catalog->getNextArrayId();
        _unversionedSchema.setIds(uAId, uAId, VersionID(0));

    } else if (_unversionedSchema.isTransient()) {

        _schema = _unversionedSchema;

        _lock->setArrayId       (_arrayUAID   = _unversionedSchema.getUAId());
        _lock->setArrayVersion  (_lastVersion = 0);
        _lock->setArrayVersionId(_arrayID     = _unversionedSchema.getId());
        rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);
        query->isDistributionDegradedForWrite(_unversionedSchema);
        return;

    } else {
        // in the catalog but not transient, so get its latest version
        _lastVersion = catalog->getLastVersion(_unversionedSchema.getId());
    }
    SCIDB_ASSERT(_unversionedSchema.getUAId() == _unversionedSchema.getId());

    // Not doing the more expensive check of counting the servers
    // because StorageManager must do it anyway.
    if (_unversionedSchema.getDistribution()->getRedundancy() >=
        _unversionedSchema.getResidency()->size()) {

        ServerCounter sc;
        const size_t resSize = _unversionedSchema.getResidency()->size();
        for (size_t i=0; i < resSize; ++i) {
            sc(_unversionedSchema.getResidency()->getPhysicalInstanceAt(i));
        }
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_INVALID_REDUNDANCY)
                << _unversionedSchema.getDistribution()->getRedundancy()
                << sc.getCount()
                << MAX_REDUNDANCY;
    }

    //XXX TODO: for now just check that all the instances in the residency are alive
    //XXX TODO: once we allow writes in a degraded mode, this call might have more semantics
    query->isDistributionDegradedForWrite(_unversionedSchema);

    // All aspects of conformity should be checked at the physical stage, but we must postpone the
    // interval check if we don't have the interval(s) yet.
    _deferLockUpdate = _schema.isAutochunked();
    unsigned flags = _deferLockUpdate ? ArrayDesc::IGNORE_INTERVAL : 0;
    ArrayDesc::checkConformity(_schema, _unversionedSchema, flags);

    _arrayUAID = _unversionedSchema.getId();

    LOG4CXX_TRACE(logger, "PhysicalUpdate::preSingleExecute end1: getSynthesizedDistType() : " << getSynthesizedDistType());
    _schema = ArrayDesc(
        namespaceName,
        makeVersionedName(arrayName, _lastVersion+1),
        _schema.getAttributes(),
        _schema.getDimensions(),
        _schema.getDistribution(),
        _schema.getResidency());

    for (DimensionDesc& d : _schema.getDimensions())
    {
        d.setCurrStart(CoordinateBounds::getMax());
        d.setCurrEnd(CoordinateBounds::getMin());
    }

    LOG4CXX_TRACE(logger, "PhysicalUpdate::preSingleExecute end2: getSynthesizedDistType() : " << getSynthesizedDistType());
    LOG4CXX_TRACE(logger, "PhysicalUpdate::preSingleExecute end3: _schema distribution: " << _schema.getDistribution()->getDistType());

    SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(createDistribution(getSynthesizedDistType(), redundancy)));

    _arrayID = catalog->getNextArrayId();

    _schema.setIds(_arrayID, _arrayUAID,(_lastVersion+1));

    _lock->setArrayId(_arrayUAID);
    _lock->setArrayVersion(_lastVersion+1);
    _lock->setArrayVersionId(_arrayID);

    // Do not update the array lock until the intervals have been checked.
    if (!_deferLockUpdate) {
        rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);
    }
}


void PhysicalUpdate::executionPreamble(std::shared_ptr<Array>& input, std::shared_ptr<Query>& query)
{
    // Catch bad subclasses that don't make this call!
    SCIDB_ASSERT(!_preambleDone);
    _preambleDone = true;

    if (input) {
        checkOrUpdateIntervals(_schema, input);
    }
    if (!_unversionedSchema.getAttributes().empty()) {
        // On the coordinator, sometimes _schema receives values from _unversionedSchema and
        // sometimes it's the other way around.  We must reconcile autochunked dimensions in both.
        SCIDB_ASSERT(query->isCoordinator());
        if (input) {
            checkOrUpdateIntervals(_unversionedSchema, input);
        }
    }

    // Finally the catalog can be updated.
    if (_deferLockUpdate) {
        SCIDB_ASSERT(query->isCoordinator());
        SystemCatalog* catalog = SystemCatalog::getInstance();
        SCIDB_ASSERT(catalog);
        bool rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);
    }

    // Everybody get together!
    // XXX Very heavyweight in terms of number of messages, suggestions welcome.
    // XXX What would be nice is an per-operator-object base barrier number (perhaps assigned by
    //     QueryProcessor::execute()?).  Without that, we need two calls here.
    // XXX Tigor says that workers might not really need to wait, so long as the lock is updated on
    //     the coordinator before execute()... i.e. we *might* not need to defer updating the lock.
    //     Worth testing at some point.
    syncBarrier(0, query);
    syncBarrier(1, query);
}

void PhysicalUpdate::postSingleExecute(std::shared_ptr<Query> query)
{
    // Some assertions to ensure that all PhysicalUpdate instances ran
    // the execute preamble.
    ASSERT_EXCEPTION(_preambleDone, "PhysicalUpdate subclass failed to run executionPreamble()");

    if (_arrayID != 0 && !_unversionedSchema.isTransient()) {
        SCIDB_ASSERT(_lock);
        SCIDB_ASSERT(_arrayID > 0);
        SCIDB_ASSERT(_arrayUAID < _arrayID);
        // NOTE: the physical operator must outlive the finalizer
        // (and it does because the physical plan is never destroyed)
        // This finalizer should run after the 2PC completion step and before
        // coordinator array locks release.
        query->pushCompletionFinalizer(std::bind(&PhysicalUpdate::recordPersistent,
                                                 this,
                                                 std::placeholders::_1));
    }
}

/**
 * Implements a callback that is suitable for use as a query finalizer.
 * Adds the new array/version to the catalog transactionally.
 */
void PhysicalUpdate::recordPersistent(const std::shared_ptr<Query>& query)
{
    if (!query->wasCommitted()) {
        return;
    }
    LOG4CXX_DEBUG(logger, "PhysicalUpdate::recordPersistent: start: "<< _schema);
    SCIDB_ASSERT(!_schema.isTransient());
    SCIDB_ASSERT(_arrayUAID == _unversionedSchema.getId());
    SCIDB_ASSERT(_arrayUAID == _schema.getUAId());
    SCIDB_ASSERT(_arrayID   == _schema.getId());
    SCIDB_ASSERT(_schema.getNamespaceName() == _unversionedSchema.getNamespaceName());
    SCIDB_ASSERT(query->getSession());

    // if the catalog update fails (for a reason other than disconnect)
    // the finalizer code exercising this routine will abort() the process
    if (_lock->getLockMode()==LockDesc::CRT) {
        SystemCatalog::getInstance()->addArrayVersion(&_unversionedSchema, _schema);
    } else {
        SCIDB_ASSERT(_lock->getLockMode()==LockDesc::WR);
        SystemCatalog::getInstance()->addArrayVersion(NULL, _schema);
    }
    LOG4CXX_DEBUG(logger, "PhysicalUpdate::recordPersistent: recorded: "<< _schema);
}

void PhysicalUpdate::recordTransient(const std::shared_ptr<Array>& t,
                                     const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(_schema.isTransient());
    SCIDB_ASSERT(_schema.getId() == _schema.getUAId());

    // if the catalog update fails (for a reason other than disconnect)
    // the finalizer code exercising this routine will abort() the process

    if (query->wasCommitted()) {
        LOG4CXX_DEBUG(logger, "PhysicalUpdate::recordTransient: start: "<< _schema);

        transient::record(t);

        if (query->isCoordinator()) {
            // 2/25/2015 NOTE: this is the last usage of updateArrayBoundaries()
            // ...because only transient arrays update array_dimensions records in place.
            // Persistent arrays create new records for successive VAIDs.
            PhysicalBoundaries bounds(_schema.getLowBoundary(), _schema.getHighBoundary());
            SystemCatalog::getInstance()->updateArrayBoundariesAndIntervals(_schema, bounds);
        }
        LOG4CXX_DEBUG(logger, "PhysicalUpdate::recordTransient: recorded: "<< _schema);
    }
}

void PhysicalUpdate::updateTransient(const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(_schema.isTransient());
    SCIDB_ASSERT(_schema.getId() == _schema.getUAId());

    // if the catalog update fails (for a reason other than disconnect)
    // the finalizer code exercising this routine will abort() the process

    if (query->wasCommitted() && query->isCoordinator()) {
        SystemCatalog::getInstance()->updateTransientAttributes(_schema);
    }
}

void
PhysicalUpdate::updateSchemaBoundaries(ArrayDesc& schema,
                                       PhysicalBoundaries& bounds,
                                       std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(schema.getId()>0);
    Dimensions const& dims = schema.getDimensions();
    SCIDB_ASSERT(!dims.empty());

    // These conditions should have been caught by now.
    SCIDB_ASSERT(bounds.isEmpty() ||
                 (schema.contains(bounds.getStartCoords()) &&
                  schema.contains(bounds.getEndCoords())));

    if (query->isCoordinator()) {

        SCIDB_ASSERT(schema.getUAId() <= schema.getId());

        LOG4CXX_DEBUG(logger, "PhysicalUpdate::updateSchemaBoundaries: schema on coordinator: "<< schema);

        Coordinates start(dims.size());
        Coordinates end(dims.size());

        for (InstanceID fromInstance=0; fromInstance < query->getInstancesCount(); ++fromInstance) {

            if (fromInstance == query->getInstanceID()) {
                continue;
            }
            std::shared_ptr<MessageDesc> resultMessage;
            NetworkManager::getInstance()->receive(fromInstance, resultMessage, query);
            SCIDB_ASSERT(resultMessage);
            ASSERT_EXCEPTION(resultMessage->getMessageType() == mtUpdateQueryResult, "UNKNOWN MESSAGE");

            std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
            ASSERT_EXCEPTION(queryResultRecord->array_name() == schema.getName(),
                             string("Message for unknown array ")+queryResultRecord->array_name());

            namespace gp = google::protobuf;
            const gp::RepeatedPtrField<scidb_msg::QueryResult_DimensionDesc>& entries = queryResultRecord->dimensions();
            ASSERT_EXCEPTION(static_cast<size_t>(entries.size()) == dims.size(), "INVALID MESSAGE");

            size_t i=0;
            for(gp::RepeatedPtrField<scidb_msg::QueryResult_DimensionDesc>::const_iterator iter = entries.begin();
                iter != entries.end();
                ++iter, ++i) {

                const scidb_msg::QueryResult_DimensionDesc& dimension = (*iter);
                SCIDB_ASSERT(dimension.has_curr_start());
                start[i] = dimension.curr_start();
                SCIDB_ASSERT(dimension.has_curr_end());
                end[i] = dimension.curr_end();
                if (isDebug()) {
                    SCIDB_ASSERT(dimension.has_name());
                    SCIDB_ASSERT(dimension.name() == dims[i].getBaseName());
                    SCIDB_ASSERT(dimension.has_start_min());
                    SCIDB_ASSERT(dimension.start_min() == dims[i].getStartMin());
                    SCIDB_ASSERT(dimension.has_end_max());
                    SCIDB_ASSERT(dimension.end_max() == dims[i].getEndMax());
                    SCIDB_ASSERT(dimension.has_chunk_interval());
                    SCIDB_ASSERT(dimension.chunk_interval() == dims[i].getRawChunkInterval());
                    SCIDB_ASSERT(dimension.has_chunk_overlap());
                    SCIDB_ASSERT(dimension.chunk_overlap() == dims[i].getChunkOverlap());
                }
            }
            PhysicalBoundaries currBounds(start, end);
            bounds.unionWith(currBounds);
        }

        SCIDB_ASSERT(start.size() == end.size());
        SCIDB_ASSERT(start.size() == dims.size());
        Dimensions newDims(dims);
        for (size_t i=0, n=newDims.size(); i<n; ++i) {
            newDims[i].setCurrStart(bounds.getStartCoords()[i]);
            newDims[i].setCurrEnd(bounds.getEndCoords()[i]);
        }
        schema.setDimensions(newDims);
        LOG4CXX_DEBUG(logger, "Dimension boundaries on coordinator updated: "<< schema);

    } else {

        LOG4CXX_DEBUG(logger, "PhysicalUpdate::updateSchemaBoundaries: schema on worker: "<< schema);

        // Creating message with result for sending to coordinator
        std::shared_ptr<MessageDesc> resultMessage = std::make_shared<MessageDesc>(mtUpdateQueryResult);
        std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
        resultMessage->setQueryID(query->getQueryID());

        queryResultRecord->set_array_name(schema.getName());

        const Coordinates & start = bounds.getStartCoords();
        const Coordinates & end = bounds.getEndCoords();
        SCIDB_ASSERT(start.size() == end.size());
        SCIDB_ASSERT(start.size() == dims.size());

        for (size_t i = 0; i < start.size(); i++)
        {
            scidb_msg::QueryResult_DimensionDesc* dimension = queryResultRecord->add_dimensions();

            dimension->set_name(dims[i].getBaseName());
            dimension->set_start_min(dims[i].getStartMin());
            dimension->set_end_max(dims[i].getEndMax());
            dimension->set_chunk_interval(dims[i].getRawChunkInterval());
            dimension->set_chunk_overlap(dims[i].getChunkOverlap());

            dimension->set_curr_start(start[i]);
            dimension->set_curr_end(end[i]);
        }
        const InstanceID coordLogicalId = query->getCoordinatorID();
        NetworkManager::getInstance()->send(coordLogicalId, resultMessage);

        LOG4CXX_DEBUG(logger, "PhysicalUpdate::updateSchemaBoundaries: schema on worker updated: "<< schema);
    }
}

} //namespace
