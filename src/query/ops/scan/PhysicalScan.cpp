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
 * PhysicalScan.cpp
 *
 *  Created on: Oct 28, 2010
 *      Author: knizhnik@garret.ru
 */

#include <array/DBArray.h>
#include <array/Dense1MChunkEstimator.h>
#include <array/MemArray.h>
#include <array/TransientCache.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("physical_scan"));

class PhysicalScan: public  PhysicalOperator
{
  public:
    PhysicalScan(const std::string& logicalName,
                 const std::string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
    :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        LOG4CXX_TRACE(logger, "PhysicalScan::PhysicalScan: created with schema.getDistribution()->getDistType() "
                              << schema.getDistribution()->getDistType());

        LOG4CXX_TRACE(logger, "PhysicalScan::PhysicalScan: created with schema.getDistribution()->getRedundancy(): "
                             << _schema.getDistribution()->getRedundancy());

        _arrayName = dynamic_pointer_cast<OperatorParamReference>(parameters[0])->getObjectName();
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        LOG4CXX_TRACE(logger, "PhysicalScan::inferSynthesizedDistType: schema.getDistribution()->getRedundancy(): "
                             << _schema.getDistribution()->getRedundancy());

        ArrayDistPtr schemaDist = _schema.getDistribution();
        SCIDB_ASSERT(schemaDist);
        LOG4CXX_TRACE(logger, "PhysicalScan::inferSynthesizedDistType: _schema.getDistribution()->getDistType() "
                              << schemaDist->getDistType());

        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);
        if (query->isDistributionDegradedForRead(_schema)) {
            LOG4CXX_TRACE(logger, "PhysicalScan::inferSynthesizedDistType: isDistributionDegradedForRead() is true");
            // see getOutputDistribution() ... this is analagous behavior for this case
            ASSERT_EXCEPTION(not isReplicated(schemaDist->getDistType()),
                             "Arrays with replicated distribution in degraded mode are not supported");
            LOG4CXX_TRACE(logger, "PhysicalScan::inferSynthesizedDistType: returning dtUndefined: " << dtUndefined);
            return dtUndefined; // matches what the getOutputDistribution does in this case
        }
        return _schema.getDistribution()->getDistType();
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & /*inSchemas*/) const override
    {
        LOG4CXX_TRACE(logger, "PhysicalScan::getOutputDistribution: created with schema.getDistribution()->getRedundancy(): "
                             << _schema.getDistribution()->getRedundancy());

        SCIDB_ASSERT(_schema.getDistribution());
        SCIDB_ASSERT(not isUninitialized(_schema.getDistribution()->getDistType()));
        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);

        if (query->isDistributionDegradedForRead(_schema)) {
            LOG4CXX_TRACE(logger, "PhysicalScan::getOutputDistribution: isDistributionDegradedForRead() is true");

            LOG4CXX_TRACE(logger, "PhysicalScan::getOutputDistribution1: _schema.getDistribution()->getDistType() "
                                  << _schema.getDistribution()->getDistType());
            LOG4CXX_TRACE(logger, "PhysicalScan::getOutputDistribution1: but returning a RedistContext(dtUndefined)");
            //XXX TODO: dtReplication declared as dtUndefined would confuse SG because most of the data would collide.
            //XXX TODO: One option is to take the intersection between the array residency and the query live set
            //XXX TODO: (i.e. the default array residency) and advertize that as the new residency (with psReplicated)...
            ASSERT_EXCEPTION((not isReplicated(_schema.getDistribution()->getDistType())),
                             "Arrays with replicated distribution in degraded mode are not supported");

            // Return a faked distribution.  The original distribution in _schema must remain, because
            // DBArrayChunks::initialize() and onward cannot handle a dtUndefined chunk.
            // To allow it to do so requires invesitigating whether we should be processing a dtUndefined
            // chunk prior to getResponsibleInstance() etc, because that is not defined for a dtUndefined
            // chunk. (One might ask, then how does load() work? because it outputs dtUndefined chunks.
            // Load outputs memChunks(), not DBArray chunks, so it can avoid the problem that way.)
            // But PhysicalScan does not have that option -- its chunks *are* storage manager chunks.
            // So here we will assert that _schema is not dtUndefined even as we return dtUndefined
            // in order that the proper converters are inserted to handle this degradedRead case
            ASSERT_EXCEPTION(not isUndefined(_schema.getDistribution()->getDistType()), "");
            return RedistributeContext(createDistribution(dtUndefined), _schema.getResidency());
        }

        LOG4CXX_TRACE(logger, "PhysicalScan::getOutputDistribution2: _schema.getDistribution()->getDistType() "
                              << _schema.getDistribution()->getDistType());
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector<ArrayDesc> & inputSchemas) const
    {
        Coordinates lowBoundary = _schema.getLowBoundary();
        Coordinates highBoundary = _schema.getHighBoundary();

        return PhysicalBoundaries(lowBoundary, highBoundary);
    }

    virtual void preSingleExecute(std::shared_ptr<Query> query)
    {
        if (_schema.isTransient())
        {
            query->isDistributionDegradedForWrite(_schema);
        }
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays,
                                      std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(!_arrayName.empty());

        // Get worker lock for transient arrays.
        if (_schema.isTransient() && !query->isCoordinator())
        {
            std::string arrayName;
            std::string namespaceName;
            query->getNamespaceArrayNames(_arrayName, namespaceName, arrayName);

            std::shared_ptr<LockDesc> lock(
                make_shared<LockDesc>(
                    namespaceName,
                    arrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    LockDesc::WORKER,
                    LockDesc::XCL));

            Query::Finalizer f = std::bind(&UpdateErrorHandler::releaseLock,
                                           lock,
                                           std::placeholders::_1);
            query->pushVotingFinalizer(f);
            SystemCatalog::ErrorChecker errorChecker(std::bind(&Query::validate, query));
            if (!SystemCatalog::getInstance()->lockArray(lock, errorChecker)) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)<< lock->toString();
            }
        }

        if (_schema.isAutochunked())
        {
            // Whether transient or not, scanning an array that is autochunked
            // in the system catalog gets you a non-autochunked empty MemArray.

            Dense1MChunkEstimator::estimate(_schema.getDimensions());
            return std::make_shared<MemArray>(_schema, query);
        }
        else if (_schema.isTransient())
        {
            MemArrayPtr a = transient::lookup(_schema,query);
            ASSERT_EXCEPTION(a.get()!=nullptr, string("Temp array ")+_schema.toString()+string(" not found"));
            return a;                                   // ...temp array
        }
        else
        {
            assert(_schema.getId() != 0);
            assert(_schema.getUAId() != 0);
            return std::shared_ptr<Array>(DBArray::createDBArray(_schema, query));
        }
    }

  private:
    string _arrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalScan, "scan", "physicalScan")

} //namespace scidb
