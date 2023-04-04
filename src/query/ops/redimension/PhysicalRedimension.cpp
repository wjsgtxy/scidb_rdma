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
 * PhysicalRedimension.cpp
 *
 *  Created on: Apr 16, 2010
 *  @author Knizhnik
 *  @author poliocough@gmail.com
 */

#include "RedimensionCommon.h"

#include <query/Expression.h>
#include <query/Query.h>

namespace scidb
{

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.redimension"));

/**
 * Redimension operator
 */
class PhysicalRedimension: public RedimensionCommon
{
public:
    /**
     * Vanilla.
     * @param logicalName the name of operator "redimension"
     * @param physicalName the name of the physical counterpart
     * @param parameters the operator parameters - the output schema and optional aggregates
     * @param schema the result of LogicalRedimension::inferSchema
     */
    PhysicalRedimension(const string& logicalName,
                        const string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
        : RedimensionCommon(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Check if we are dealing with aggregates or a synthetic dimension.
     * @return true iff this redimension either uses a synthetic dimension or has at least one aggregate
     */
    bool haveAggregatesOrSynthetic(ArrayDesc const& srcDesc) const
    {
        if (_parameters.size() > 2)
        {
            return true; //aggregate
        }

        if (_parameters.size() == 2 &&
            _parameters[1]->getParamType() == PARAM_AGGREGATE_CALL) {
            return true;
        }

        ArrayDesc dstDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        for (const DimensionDesc &dstDim : dstDesc.getDimensions())
        {
            for (const auto& srcAttr : srcDesc.getAttributes())
            {
                if (dstDim.hasNameAndAlias(srcAttr.getName()))
                {
                    goto NextDim;
                }
            }
            for (const DimensionDesc &srcDim : srcDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstDim.getBaseName()))
                {
                    goto NextDim;
                }
            }
            return true; //synthetic
            NextDim:;
        }
        return false;
    }

    bool canToggleStrict() const override
    {
        return true;            // Yes, optimizer can call getIsStrict().
    }

    /**
     * @return false if the isStrict parameter is specified and is equal to false; true otherwise
     */
    bool getIsStrict() const
    {
        bool isStrict = true;
        if (_parameters.size() == 2 && _parameters[1]->getParamType() == scidb::PARAM_PHYSICAL_EXPRESSION) {
            OperatorParamPhysicalExpression* paramExpr =
                static_cast<OperatorParamPhysicalExpression*>(_parameters[1].get());
            SCIDB_ASSERT(paramExpr->isConstant());
            isStrict = paramExpr->getExpression()->evaluate().getBool();
        }
        return isStrict;
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext>const& inputDistributions,
                                                      vector<ArrayDesc>const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);

        if (haveAggregatesOrSynthetic(inputSchemas[0])) {
            // With aggregations or synthetic dimension,
            // redistribution happens within the execute() method.
            // (jhm: why?)
            // so it is efficient to try to produce what the parent desires.
            auto myType = getInheritedDistType();
            if (!isParameterless(myType)) {
                myType = defaultDistType(); // since this is a general case implementation
                                                // and an SG might need to follow, use one that
                                                // is parameter free.
                LOG4CXX_DEBUG(logger, "PhysicalRedimension::getOutputDistribution(): changed myType to: " << myType);
            }
            SCIDB_ASSERT(isParameterless(myType));
            SCIDB_ASSERT(getSynthesizedDistType() == myType); // should agree with what inferSynthesized returned earlier
            _schema.setDistribution(createDistribution(myType));

            SCIDB_ASSERT(_schema.getResidency()->isEqual(Query::getValidQueryPtr(_query)->getDefaultArrayResidency()));
        } else {
            // the optimizer needs to deal with the fallout,
            // i.e. redistributing (from dtUndefined) and merging partial chunks
            SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
            _schema.setResidency(inputDistributions[0].getArrayResidency());
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /// @see OperatorDist
    /// @note This is one of two cases where we need the extra argument from the 3-arg version of this method.
    ///       If operators could examine their children directly, would we not have to pass inputSchemas,
    ///       and we could eliminate this problem.
    DistType inferSynthesizedDistType(std::vector<DistType> const& inDist,
                                      std::vector<ArrayDesc>const & inSchema, size_t /*depth*/) const
    {
        SCIDB_ASSERT(inDist.size() == 1);
        SCIDB_ASSERT(inSchema.size() == 1);

        // mimics the (pre-existing) behavior of getOutputDistribution (though this method called first).
        if (haveAggregatesOrSynthetic(inSchema[0])) {
            // With aggregations or synthetic dimension,
            // redistribution happens within the execute() method.
            // (not sure why we don't let SG do it --jhm)
            // so long as that is the case, we should output what the parent wants
            auto myType = getInheritedDistType();
            if (!isParameterless(myType)) {
                myType = defaultDistType(); // unless we are pepared to provide arbitrary
                                                // redistribute arguments, use a parameter-free distribution.
                LOG4CXX_DEBUG(logger, "PhysicalRedimension::inferSynthesizedDistType(): changed myType to: " << myType);
            }
            SCIDB_ASSERT(isParameterless(myType));
            return myType;
        } else {
            // the optimizer needs to deal with the fallout,
            // i.e. redistributing (from dtUndefined) and merging partial chunks
            SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
            return dtUndefined;
        }
    }

    /// @see OperatorDist
    void checkInputDistAgreement(std::vector<DistType> const& inputDistrib, size_t /*depth*/) const
    {
        SCIDB_ASSERT(inputDistrib.size() == 1);
        // no restriction on inputDistrib values
    }

    /**
     * @see PhysicalOperator::outputFullChunks
     */
    virtual bool outputFullChunks(std::vector<ArrayDesc>const& inputSchemas) const
    {
        return haveAggregatesOrSynthetic(inputSchemas[0]);
    }

    /**
     * @see PhysicalOperator::execute
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        std::shared_ptr<Array>& srcArray = inputArrays[0];
        ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();

        Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.
        Dimensions const& destDims  = _schema.getDimensions();

        if (getControlCookie() == "repart") {
            // We are acting as a repart() operator on behalf of the optimizer.
            setupMappingsByOrder(destAttrs, destDims);
        } else {
            setupMappingsByName(srcArrayDesc, destAttrs, destDims);
        }

        ElapsedMilliSeconds timing;

        RedistributeMode redistributeMode(AUTO);
        if (haveAggregatesOrSynthetic(srcArrayDesc)) {
            redistributeMode = AGGREGATED;
        } else if ( getIsStrict()) {
            redistributeMode = VALIDATED;
        }
        std::shared_ptr<Array> resultArray =
           redimensionArray(srcArray, query, shared_from_this(), timing, redistributeMode, true);
        SCIDB_ASSERT(!inputArrays[0]); // should be dropped by now
        return resultArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRedimension, "redimension", "PhysicalRedimension")
}  // namespace ops
