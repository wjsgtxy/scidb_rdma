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
 * @file PhysicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of SAVE operator for saving data from text file
 * which is located on coordinator
 */

#include "Save.h"

#include <array/ArrayWriter.h>
#include <array/MemArray.h>
#include <array/SynchableArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <util/PathUtils.h>

#include <boost/lexical_cast.hpp>
#include <log4cxx/logger.h>

using namespace std;
using namespace scidb;

// Useful local shorthand (for positional parameters only).
#define Parm(_n) \
    ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[(_n)])
#define ParmExpr(_n)    (Parm(_n)->getExpression())

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physical_save"));

class PhysicalSave: public PhysicalOperator
{
public:
    PhysicalSave(const std::string& logicalName,
                 const std::string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(
        const std::vector<PhysicalBoundaries> & inputBoundaries,
        const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    InstanceID getSourceInstanceID() const
    {
        // Precomputed in LogicalSave; retrieve it.
        try {
            return boost::lexical_cast<InstanceID>(getControlCookie());
        }
        catch (std::exception const& ex) {
            stringstream ss;
            ss << "Control cookie '" << getControlCookie()
               << "' cannot be cast to InstanceID: " << ex.what();
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNKNOWN_ERROR)
                << ss.str();
        }
    }

    bool isParallel() const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        bool result = sourceInstanceID == ALL_INSTANCE_MASK;
        return result;
    }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (isParallel()) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::getDistributionRequirement()] returns Any");
            return DistributionRequirement(DistributionRequirement::Any);
        }

        stringstream ss;
        ss << Query::getValidQueryPtr(_query)->getInstanceID();
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance, NO_REDUNDANCY,
                                                                                    ss.str());
        vector<RedistributeContext> requiredDistContext;
        requiredDistContext.push_back(RedistributeContext(localDist, _schema.getResidency()));

        LOG4CXX_TRACE(logger, "[PhysicalSave::getDistributionRequirement()] returns Specific dtLocalInstance");
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistContext);
    }

    //
    // inheritance
    //
    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren)
        const override
    {
        // to be consistent with getDistribution requirement
        // when !parallel, override the default implementation of inferChildInheritance to return
        // dtLocalInstance.
        if (isParallel()) {
            return PhysicalOperator::inferChildInheritances(inherited, numChildren);
        }

        LOG4CXX_TRACE(logger, "PhysicalSave::inferChildInheritances: returning dtLocalInstance==" << dtLocalInstance);
        SCIDB_ASSERT(numChildren == 1);

        std::vector<DistType> result(numChildren);
        result[0] = dtLocalInstance;
        return result;
    }

    string _getFormat() const
    {
        // factored from execute() for use by _isOpaque()
        string format = "store";
        Parameter fmtParam = findKeyword(KW_FORMAT);
        if (!fmtParam && _parameters.size() >= 3) {
            fmtParam = Parm(2);
        }
        if (fmtParam) {
            format = paramToString(fmtParam);
        }

        return format;
    }

    bool _isOpaque() const
    {
        // factored from execute() for use by acceptsPullSG()
        const string format = _getFormat();
        const bool isOpaque = (compareStringsIgnoreCase(format, "opaque") == 0);

        return isOpaque;
    }

    virtual bool acceptsPullSG(size_t input) const override
    {
        return _isOpaque();
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        assert(_parameters.size() >= 1);
        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        const string fileName = path::expandForSave(ParmExpr(0)->evaluate().getString(), *query);

        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] COORDINATOR_INSTANCE_MASK");
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        } else if (sourceInstanceID != ALL_INSTANCE_MASK) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] != ALL_INSTANCE_MASK");
            // convert from physical to logical
            sourceInstanceID = query->mapPhysicalToLogical(sourceInstanceID);
        }
        LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] sourceInstanceID " << sourceInstanceID);

        const ArrayDesc& inputArrayDesc = inputArrays[0]->getArrayDesc();
        std::shared_ptr<Array> tmpRedistedInput = inputArrays[0];

        if (isParallel()) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute() a] isParallel()");
        } else {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute() b] not isParallel()");

            auto inputPS = inputArrays[0]->getArrayDesc().getDistribution()->getDistType();
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute() b] inputPS: " << inputPS);
            SCIDB_ASSERT(isLocal(inputPS));

            const Attributes& attribs = inputArrayDesc.getAttributes();
            AttributeOrdering attributeOrdering;
            const bool isOpaque = _isOpaque();
            for (size_t i = 0; i < attribs.size(); ++i) {
                const auto* a = &attribs.findattr(i);
                // Exclude the EBM on all but opaque saves.
                if (!a->isEmptyIndicator() || isOpaque) {
                    SCIDB_ASSERT(inputArrayDesc.getEmptyBitmapAttribute()==NULL ||
                                 isOpaque ||
                                 // if emptyable, make sure the attribute is not the last
                                 // the last must be the empty bitmap
                                 (a->getId()+1) != attribs.size());
                    attributeOrdering.insert(*a);
                }
            }

            stringstream ss;
            ss << sourceInstanceID;
            ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(dtLocalInstance,
                                                                                        DEFAULT_REDUNDANCY,
                                                                                        ss.str());

            if (! inputArrays[0]->getArrayDesc().getDistribution()->isCompatibleWith(localDist)) {
                LOG4CXX_TRACE(logger, "[PhysicalSave::execute() b] NOT COMPATIBLE");

                // TODO: why does pullRedistributeInAttributeOrder not optimize for isCompatibleWith?
                //       perhaps that can be done in the future?
                tmpRedistedInput = pullRedistributeInAttributeOrder(inputArrays[0],
                                                                    attributeOrdering,
                                                                    localDist,
                                                                    ArrayResPtr(), //default query residency
                                                                    query,
                                                                    shared_from_this());
            }
        }
        const bool wasPullRedistribution = (tmpRedistedInput != inputArrays[0]) ;
        LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] wasPullRedistribution: " << wasPullRedistribution);

        const InstanceID myInstanceID = query->getInstanceID();
        const string format = _getFormat();
        if (isParallel()) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] isParallel()");
            ArrayWriter::save(*tmpRedistedInput, fileName, query, format, ArrayWriter::F_PARALLEL);
        } else if (sourceInstanceID == myInstanceID) {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] not isParallel() but we are the source instance");
            ArrayWriter::save(*tmpRedistedInput, fileName, query, format, 0/*not parallel*/);
        } else {
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] not isParallel() and not a participant()");
        }

        if (wasPullRedistribution) { // only when pullRedistribute did something (sometimes optimized away)
            LOG4CXX_TRACE(logger, "[PhysicalSave::execute()] post pullRedistribute sync required");
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }

        return std::make_shared<MemArray>(inputArrayDesc, query); //empty array
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSave, "save", "impl_save")

} //namespace
