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
 * @file PhysicalTestSG.cpp
 *
 * @author
 *
 */

#include <array/MemArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>
#include <query/PullSGArrayUtil.h>

using namespace std;
using namespace scidb;

namespace scidb
{
class PhysicalTestSG: public PhysicalOperator
{
public:
    PhysicalTestSG(const string& logicalName, const string& physicalName,
                   const Parameters& parameters, const ArrayDesc& schema)
    :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t depth) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & /*inputDistributions*/,
                                              const std::vector< ArrayDesc> & /*inputSchemas*/) const override
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        std::shared_ptr<Array> srcArray = inputArrays[0];

        std::string sgMode = getSGMode(_parameters);
        if (sgMode.empty()) {
            sgMode = "parallel";
        }

        bool enforceDataIntegrity=false;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[3].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
        }

        ArrayDistPtr  arrDist = _schema.getDistribution();
        ArrayResPtr arrRes; //default query residency

        // TODO: this (a) should be an integer switch [why is sgMode a string ... pu-lease, lets stop being so wasteful]
        //            (b) why so many calls have to be known on the "outside" ... maybe it should be redistribute(how, SrcArray, arrDist, ...)
        //                since they *all share a common signature!*
        //            (c) evidence that redistributeXXXX() is too much an implemenation-artifact rather than a stable "api"
        //                for operators to use.
        if (sgMode == "randomRes") {
            arrRes = _schema.getResidency();
            return redistributeToRandomAccess(srcArray,
                                              arrDist,
                                              arrRes,
                                              query,
                                              shared_from_this(),
                                              enforceDataIntegrity);
        } else if (sgMode == "parallel") {
            return redistributeToRandomAccess(srcArray,
                                              arrDist,
                                              arrRes,
                                              query,
                                              shared_from_this(),
                                              enforceDataIntegrity);
        } else if (sgMode == "serial") {
            return redistributeSerially(srcArray,
                                        arrDist,
                                        arrRes,
                                        query,
                                        shared_from_this(),
                                        enforceDataIntegrity);
        }

        ASSERT_EXCEPTION_FALSE("Bad SG mode");

        return std::shared_ptr<Array>();
    }

    std::shared_ptr<Array>
    redistributeSerially(std::shared_ptr<Array>& inputArray,
                         const ArrayDistPtr& arrDist,
                         const ArrayResPtr& arrRes,
                         const std::shared_ptr<Query>& query,
                         const std::shared_ptr<PhysicalOperator>& phyOp,
                         bool enforceDataIntegrity)
    {
        inputArray = ensureRandomAccess(inputArray, query);

        const ArrayDesc& inputArrayDesc = inputArray->getArrayDesc();

        ArrayDesc outputArrayDesc(inputArray->getArrayDesc());
        outputArrayDesc.setDistribution(arrDist);
        if (!arrRes) {
            outputArrayDesc.setResidency(query->getDefaultArrayResidency());
        } else {
            outputArrayDesc.setResidency(arrRes);
        }

        std::shared_ptr<MemArray> outputArray = std::make_shared<MemArray>(outputArrayDesc, query);

        // set up redistribute without the empty bitmap
        AttributeOrdering attributeOrdering;

        const auto& attrs = inputArrayDesc.getAttributes(true);
        for (const auto& attr : attrs) {
            attributeOrdering.insert(attr);
        }
        redistributeAttributes(inputArray,
                               outputArrayDesc.getDistribution(),
                               outputArrayDesc.getResidency(),
                               query,
                               phyOp,
                               enforceDataIntegrity,
                               attributeOrdering,
                               outputArray);

        // now redistribute the empty bitmap only
        attributeOrdering.clear();
        attributeOrdering.insert(*inputArrayDesc.getEmptyBitmapAttribute());
        redistributeAttributes(inputArray,
                               outputArrayDesc.getDistribution(),
                               outputArrayDesc.getResidency(),
                               query,
                               phyOp,
                               enforceDataIntegrity,
                               attributeOrdering,
                               outputArray);

        return std::shared_ptr<Array>(outputArray);
    }

    void redistributeAttributes(std::shared_ptr<Array>& inputArray,
                                const ArrayDistPtr& arrDist,
                                const ArrayResPtr& arrRes,
                                const std::shared_ptr<Query>& query,
                                const std::shared_ptr<PhysicalOperator>& phyOp,
                                bool enforceDataIntegrity,
                                AttributeOrdering& attributeOrdering,
                                const std::shared_ptr<MemArray>& outputArray)
    {
        std::shared_ptr<Array> tmpRedistedInput =
        pullRedistributeInAttributeOrder(inputArray,
                                         attributeOrdering,
                                         arrDist,
                                         arrRes,
                                         query,
                                         phyOp);

        // only when redistribute was actually done (sometimes optimized away)
        const bool wasConverted = (tmpRedistedInput != inputArray) ;

        // drain into outputArray
        const ArrayDesc& inputArrayDesc = inputArray->getArrayDesc();
        const Attributes& attributes = inputArrayDesc.getAttributes();

        sg::WriteChunkToArrayFunc chunkWriter(outputArray, NULL, enforceDataIntegrity);
        std::vector<std::shared_ptr<ConstArrayIterator> > inputArrIters(attributes.size());
        const auto& attId = (*attributeOrdering.begin());
        while (!inputArrIters[attId.getId()] || !inputArrIters[attId.getId()]->end()) {

            for  (auto attr = attributeOrdering.begin();
                  attr != attributeOrdering.end(); ++attr ) {
                const auto& a = *attr;

                if (!inputArrIters[a.getId()]) {
                    inputArrIters[a.getId()] = tmpRedistedInput->getConstIterator(a);
                    if (inputArrIters[a.getId()]->end()) {
                        continue;
                    }
                }

                ConstArrayIterator* arrIter = inputArrIters[a.getId()].get();
                SCIDB_ASSERT(arrIter != NULL);
                const ConstChunk& chunk = arrIter->getChunk();

                chunkWriter(a.getId(), chunk, query);
            }

            // need to bump them all at once
            for  (auto attr = attributeOrdering.begin();
                  attr != attributeOrdering.end(); ++attr ) {
                const auto& a = *attr;
                if (inputArrIters[a.getId()]) {
                    ++(*inputArrIters[a.getId()]);
                }
            }
        }

        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
    }

private:
    string getSGMode(const Parameters& parameters)
    {
        if (parameters.size() < 3) {
            return string();
        }
        SCIDB_ASSERT(parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        return static_cast<OperatorParamPhysicalExpression*>(parameters[2].get())->getExpression()->evaluate().getString();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTestSG, "test_sg", "PhysicalTestSG");
} //namespace
