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

#include "SGParams.h"

#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sg"));

// main operator

class PhysicalSG : public PhysicalOperator
{
public:
    /// @see PhysicalOperator::PhysicalOperator()
    PhysicalSG(const string& logicalName, const string& physicalName,
               const Parameters& parameters, const ArrayDesc& schema);

    // PhysicalOperator::getDistributionRquirement() not overridden

    bool outputFullChunks(std::vector< ArrayDesc> const&) const override { return true; }

    /// @see PhysicalOperator::getOutputDistribution()
    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                              const std::vector< ArrayDesc> & inputSchemas) const override;

    /// @see PhysicalOperator::getOutputBoundaries()
    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector<ArrayDesc> & inputSchemas) const override
    { return inputBoundaries[0]; }

    // requiresRedimensionOrPartition


    // getOutputDistribution


    // isSingleThreaded

    // preSingleExecute

    /// @see PhysicalOperator::execute()
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query) override ;

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& inDist, size_t depth) const override;
private:
    DistType getDistType() const ;

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const ;

};

//
// begin implementation
//

// ctor
PhysicalSG::PhysicalSG(const string& logicalName, const string& physicalName,
                       const Parameters& parameters, const ArrayDesc& schema)
:
    PhysicalOperator(logicalName, physicalName, parameters, schema)
{
    LOG4CXX_TRACE(logger, "PhysicalSG::PhysicalSG(): _schema.getDistribution()->getDistType() ="
                          << _schema.getDistribution()->getDistType());
}

// getDistributionRequirement

// getDistType
DistType PhysicalSG::getDistType() const
{
        // The operator parameters at this point are not used to determine the behavior of SG.
        // It is _schema which determines behavior:
        // it should be correctly populated either by the optimizer or by LogicalSG.

        // Here, we just check _schema is consistent with the parameters.
        SCIDB_ASSERT(_parameters[SGPARAM_DISTRIBUTION]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_DISTRIBUTION].get());
        DistType dt = static_cast<DistType>(pExp->getExpression()->evaluate().getInt32());
        LOG4CXX_TRACE(logger, "PhysicalSG::getDistType(): dt from _parameters is: "<< dt);
        LOG4CXX_TRACE(logger, "PhysicalSG::getDistType(): dt from _schema is: "<< _schema.getDistribution()->getDistType());
        SCIDB_ASSERT(dt == _schema.getDistribution()->getDistType());
        return dt;
}

DistType PhysicalSG::inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t depth) const
{
    // returns the inherited distribution, which should equal the one in _schema

    auto result = getDistType();
    if (depth > 0) {  // TODO ... do not check at the root
        // NOTE: one might think its always error to add an SG where the inputDisrib is the same
        //       as the synthesized(), but that would interfere with some existing tests.
        //       so we do not apply this test where _sg() is at the root.
        // TODO: consider eliminating those tests instead, and removing this depth>0 condition
        //
        auto inherited = getInheritedDistType();
        if (result != inherited) {
            // TODO we would like to replace this with a SCIDB_ASSERT or ASSSERT_EXCEPTION,
            //      but there is at least one test (consume.test) that uses the query consume(_sg(build(())
            //      so sometimes this is still happening at depth==1
            //      We have to decide whether we will disallow that, or whether we will track whether
            //      an sg is auto-inserted or manually inserted, and not check the manual cases.
            LOG4CXX_WARN(logger, "PhysicalSG::inferSynthesizedDistType(): result: " << result << " != inherited: " << inherited);
            LOG4CXX_WARN(logger, "PhysicalSG::inferSynthesizedDistType(): this is unexpected"
                                  << "except for manually inserted sg for testing purposes");
            // TODO: find away to distinguish manually inserted sgs (e.g. testing) from
            //       those inserted by Habilis, so that an assertion can be placed here instead
        }
    }

    LOG4CXX_TRACE(logger, "PhysicalSG::inferSynthesizedDistType(): returning getInheritedDistType:"
                          << " depth " << depth
                          << " result " << result
                          << "_schema dt =" << _schema.getDistribution()->getDistType());
    return result;
}

/// @see PhysicalOperator::getOutputDistribution()
RedistributeContext
PhysicalSG::getOutputDistribution(const std::vector<RedistributeContext> & /*inputDistrib*/,
                                  const std::vector< ArrayDesc> & /*inputSchemas*/) const
{
        if (isDebug()) {
            // Verify that _schema & _parameters are in sync
            ArrayDistPtr arrDist = _schema.getDistribution();
            SCIDB_ASSERT(arrDist);
            SCIDB_ASSERT(not isUninitialized(arrDist->getDistType()));
            SCIDB_ASSERT(not isUndefined(arrDist->getDistType()));
            std::shared_ptr<Query> query(_query);
            SCIDB_ASSERT(query);

            DistType dt = getDistType();
            SCIDB_ASSERT(arrDist->getDistType() == dt);

            SCIDB_ASSERT(not isLocal(dt) ||
                         getInstanceId(query) == InstanceID(atol(arrDist->getState().c_str())));
        }

        if(isDebug()) {
            InstanceID  schemaShift;
            ArrayDistributionFactory::getTranslationInfo(_schema.getDistribution().get(),
                                                         schemaShift);
        }
        RedistributeContext distro(_schema.getDistribution(),
                                   _schema.getResidency());
        LOG4CXX_TRACE(logger, "PhysicalSG::getOutputDistribution(): output distro is: "<< distro);
        return distro;
}

// getOutputBoundaries not overridden

// requiresRedimensionOrRepartition not overridden

// preSingleExecute not overridden

// execute
std::shared_ptr<Array>
PhysicalSG::execute(vector< std::shared_ptr<Array> >& inputArrays,
                    std::shared_ptr<Query> query)
{
        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);

        std::shared_ptr<Array> srcArray = inputArrays[0];

        bool enforceDataIntegrity=false;
        if (_parameters.size() > SGPARAM_IS_STRICT) { // isStrict param present
            assert(_parameters[SGPARAM_IS_STRICT]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_IS_STRICT].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_STRICT << " isStrict present: " << enforceDataIntegrity);
        } else {
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_STRICT << " isStrict not present!");
        }

        bool isPullSG=false;
        if (_parameters.size() > SGPARAM_IS_PULL) { // isPull param present
            LOG4CXX_TRACE(logger, "PhysicalSG::execute(): isPullSG parameter present");
            assert(_parameters[SGPARAM_IS_PULL]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_IS_PULL].get());
            isPullSG = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_PULL << " isPullSG present: " << isPullSG);
        } else {
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), param " << SGPARAM_IS_PULL << " isPullSG not present!");
        }
        LOG4CXX_DEBUG(logger, "PhysicalSG::execute(): isPullSG: " << isPullSG);

        {   // to temporarily preserve existing indentation
            // despite elimination of storing-sg functionality

            std::shared_ptr<Array> outputArray;

            if (isPullSG) {  // to use pullRedistribute instead of the usual redistributeToRandomAccess
                LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1a. !storeResult, isPullSG, dist: "<<
                                      RedistributeContext(arrDist, query->getDefaultArrayResidency()));

                const ArrayDesc& inputArrayDesc = srcArray->getArrayDesc();
                const bool isOpaque = true; // disable the inner stuff that cares about EmptyIndicator's attr order
                {
                    // not sure if this way of converting attributes to an ordering is required
                    // when save (which is row-oriented) is not involved
                    const Attributes& attribs = inputArrayDesc.getAttributes();
                    AttributeOrdering attributeOrdering;
                    for  ( size_t b = 0; b < attribs.size(); ++b) {
                        const auto* a = &attribs.findattr(b);
                        if (!a->isEmptyIndicator() || isOpaque) {
                            SCIDB_ASSERT(inputArrayDesc.getEmptyBitmapAttribute()==NULL ||
                                         isOpaque ||
                                         // if emptyable, make sure the attribute is not the last
                                         // the last must be the empty bitmap
                                         (a->getId()+1) != attribs.size());
                            attributeOrdering.insert(*a);
                        }
                    }

                    // arguments here taken from the else case below, note localDist (original) not needed here
                    LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B, !store, isPullSG, "
                                       << " pullRedistributeInAttributeOrder, enforce=" << enforceDataIntegrity);
                    outputArray  = pullRedistributeInAttributeOrder(srcArray,
                                                                    attributeOrdering,
                                                                    arrDist,
                                                                    _schema.getResidency(), //default query residency
                                                                    query,
                                                                    shared_from_this(),
                                                                    enforceDataIntegrity);
                }
                // TOOD: does the user of outputArray need to do an outputArray->sync() ?  who does it?

            } else {  // !isPullSG
                LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B. !store, !pullSG pre redistributeToRandomAccess into distro: "<<
                                      RedistributeContext(arrDist, query->getDefaultArrayResidency()));

                outputArray = redistributeToRandomAccess(srcArray,
                                           arrDist,
                                           _schema.getResidency(),
                                           query,
                                           shared_from_this(),
                                           enforceDataIntegrity);

            }
            LOG4CXX_DEBUG(logger, "PhysicalSG::execute(), 1B, !storeResult executed SG dist: "
                                  << RedistributeContext(outputArray->getArrayDesc().getDistribution(),
                                                         outputArray->getArrayDesc().getResidency())
                                  << " enforce= " << enforceDataIntegrity);

            SCIDB_ASSERT(_schema.getResidency()->isEqual(outputArray->getArrayDesc().getResidency()));
            SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(outputArray->getArrayDesc().getDistribution()));
            inputArrays[0].reset(); // hopefully, drop the input array
            return outputArray;
        }
}
//
// end execute
//

InstanceID PhysicalSG::getInstanceId(const std::shared_ptr<Query>& query) const
{
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() > SGPARAM_INSTANCE_ID ) {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[SGPARAM_INSTANCE_ID].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        if (instanceId == COORDINATOR_INSTANCE_MASK) {
            instanceId = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }
        return instanceId;
}

// Some tests rely on these names, be sure to change them if you rename here.
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "_sg", "impl_sg")

} //namespace
