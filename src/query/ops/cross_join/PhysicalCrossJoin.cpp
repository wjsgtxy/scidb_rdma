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

///
/// @file PhysicalCrossJoin.cpp
///

#include "CrossJoinArray.h"

#include <query/AutochunkFixer.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>

using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.cross_join"));

inline OperatorParamDimensionReference* dimref_cast( const std::shared_ptr<OperatorParam>& ptr )
{
    return dynamic_cast<OperatorParamDimensionReference*>(ptr.get());
}

class PhysicalCrossJoin: public PhysicalOperator
{
public:
    PhysicalCrossJoin(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema)
    :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    // required to allow replicated input
    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        vector<uint8_t> result(numChildren, false);
        SCIDB_ASSERT(numChildren==2);
        result[1] = true;   // permitted on the right-hand input
        return result;
    }

    virtual PhysicalBoundaries getOutputBoundaries(
            std::vector<PhysicalBoundaries> const& inputBoundaries,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty() || inputBoundaries[1].isEmpty()) {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }

        size_t numLeftDims = inputSchemas[0].getDimensions().size();
        size_t numRightDims = inputSchemas[1].getDimensions().size();

        Coordinates leftStart = inputBoundaries[0].getStartCoords();
        Coordinates rightStart = inputBoundaries[1].getStartCoords();
        Coordinates leftEnd = inputBoundaries[0].getEndCoords();
        Coordinates rightEnd = inputBoundaries[1].getEndCoords();

        Coordinates newStart, newEnd;

        size_t ldi;
        for (ldi = 0; ldi < numLeftDims; ldi++)
        {
            const DimensionDesc &lDim = inputSchemas[0].getDimensions()[ldi];
            size_t pi;
            for (pi = 0; pi < _parameters.size(); pi += 2)
            {
                const string &lJoinDimName = dimref_cast(_parameters[pi])->getObjectName();
                const string &lJoinDimAlias = dimref_cast(_parameters[pi])->getArrayName();
                if (lDim.hasNameAndAlias(lJoinDimName, lJoinDimAlias))
                {
                    const string &rJoinDimName = dimref_cast(_parameters[pi+1])->getObjectName();
                    const string &rJoinDimAlias = dimref_cast(_parameters[pi+1])->getArrayName();
                    for (size_t rdi = 0; rdi < numRightDims; rdi++)
                    {
                        if (inputSchemas[1].getDimensions()[rdi].hasNameAndAlias(rJoinDimName, rJoinDimAlias))
                        {
                            newStart.push_back(leftStart[ldi] < rightStart[rdi] ?  rightStart[rdi] : leftStart[ldi]);
                            newEnd.push_back(leftEnd[ldi] > rightEnd[rdi] ? rightEnd[rdi] : leftEnd[ldi]);
                            break;
                        }
                    }
                    break;
                }
            }

            if (pi>=_parameters.size())
            {
                newStart.push_back(leftStart[ldi]);
                newEnd.push_back(leftEnd[ldi]);
            }
        }

        for(size_t i=0; i<numRightDims; i++)
        {
            const DimensionDesc &dim = inputSchemas[1].getDimensions()[i];
            bool found = false;

            for (size_t pi = 0; pi < _parameters.size(); pi += 2)
            {
                const string &joinDimName = dimref_cast(_parameters[pi+1])->getObjectName();
                const string &joinDimAlias = dimref_cast(_parameters[pi+1])->getArrayName();
                if (dim.hasNameAndAlias(joinDimName, joinDimAlias))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                newStart.push_back(rightStart[i]);
                newEnd.push_back(rightEnd[i]);
            }
        }
        return PhysicalBoundaries(newStart, newEnd);
    }

    //
    // inheritance
    //
    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren) const
    {
        LOG4CXX_TRACE(logger, "PhysicalCrossJoin::inferChildInheritances: operator " << getOperatorName());
        LOG4CXX_TRACE(logger, "PhysicalCrossJoin::inferChildInheritances: inherited " << inherited);
        SCIDB_ASSERT(numChildren==2);

        std::vector<DistType> _result(numChildren);

        SCIDB_ASSERT(not isUninitialized(inherited));
        // Because cross_join outputs dtUndefined, there is no advantage to any particular
        // distribution on the LHS.  The RHS should be replicated.

        std::vector<DistType> childInheritance = {defaultDistType(), // TODO: we need a way to indicate
                                                                     //       "don't care/no preference"
                                                                     //       this could eliminate another SG
                                                  dtReplication};    // always, this is a requirement

        LOG4CXX_TRACE(logger, "PhysicalCrossJoin::inferChildInheritances: [0] " << childInheritance[0]
                                                                     << " [1] " << childInheritance[1]);
        return childInheritance;
    }

    /// see OperatorDist
    void checkInputDistAgreement(std::vector<DistType> const& inDist, size_t /*depth*/) const
    {
        SCIDB_ASSERT(inDist.size() == 2);
        // input[0] can have arbitrary distribution
        SCIDB_ASSERT(isReplicated(inDist[1])); // required
    }


    //
    // synthesis
    //
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return dtUndefined;     // cross_join creates new chunks with different coords than its inputs
    }

    virtual DistributionRequirement getDistributionRequirement(const std::vector<ArrayDesc> & inputSchemas) const
    {
        // on the LHS, we have no requirement
        // the purpose of the method is to force psRedistributed on the RHS only

        // we cannot just force the inherited input DistType on the LHS because that is overly restrictive
        // and cannot work in some cases
        auto lhsDistrib = inputSchemas[0].getDistribution();

        vector<RedistributeContext> redistContext;

        // lhs
        if(isReplicated(lhsDistrib->getDistType()) ||   // cross_join was incorrect on replicated LHS
           isLocal(lhsDistrib->getDistType()) ||
           isUndefined(lhsDistrib->getDistType())) {
            redistContext.push_back(RedistributeContext(createDistribution(defaultDistType()),
                                                        _schema.getResidency()));
        } else {
            redistContext.push_back(RedistributeContext(lhsDistrib, _schema.getResidency()));
        }

        // rhs
        redistContext.push_back(RedistributeContext(createDistribution(dtReplication), _schema.getResidency()));

        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, redistContext);
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);
        assertConsistency(inputSchemas[1], inputDistributions[1]);

        // no matter what we requested of our chldren, we are going to produce dtUndefined
        ASSERT_EXCEPTION( _schema.getDistribution()->getDistType() == dtUndefined, "");
        // why? because we produce more dimensions than our input

        _schema.setResidency(inputDistributions[0].getArrayResidency());

        RedistributeContext distro(_schema.getDistribution(),
                                   _schema.getResidency());
        LOG4CXX_TRACE(logger, "PhysicalCrossJoin::getOutputDistribution() output distro: "<< distro);
        return distro;
    }

    /**
     * Ensure input array chunk sizes and overlaps match along join-dimension pairs.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        assert(inputSchemas.size() == 2);
        assert(modifiedPtrs.size() == 2);

        // Only one input, at most, can be autochunked.
        if (inputSchemas[0].isAutochunked() && inputSchemas[1].isAutochunked()) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
                << getLogicalName();
        }

        // We don't expect to be called twice, but that may change later on:
        // wipe any previous result.
        _redimRepartSchemas.clear();

        Dimensions leftDims = inputSchemas[0].getDimensions();
        Dimensions rightDims = inputSchemas[1].getDimensions();
        // For each pair of join dimensions, make certain the chunkInterval and
        // chunkOverlap match the exemplar (left most non-autochunked array).
        // If these aspects do not match, then we need to build a repartSchema
        // for the non-exemplar schema to make them match.
        //
        // Only one schema, at most, can be autochunked. See check above.  If
        // the left is autochunked, then any required repartSchema will be built
        // for the left schema, otherwise, any required repartSchema will be for
        // the right schema.
        bool needRepart = false;
        bool leftIsAutochunked = inputSchemas[0].isAutochunked();
        for (size_t p = 0, np = _parameters.size(); p < np; p += 2)
        {
            const OperatorParamDimensionReference *lDim = dimref_cast(_parameters[p]);
            const OperatorParamDimensionReference *rDim = dimref_cast(_parameters[p+1]);
            ssize_t l = inputSchemas[0].findDimension(lDim->getObjectName(), lDim->getArrayName());
            ssize_t r = inputSchemas[1].findDimension(rDim->getObjectName(), rDim->getArrayName());
            assert(l >= 0); // was already checked in Logical...::inferSchema()
            assert(r >= 0); // ditto

            // Only one of the inputs (at most) can be autochunked. See previous check.
            if (rightDims[r].getRawChunkInterval() != leftDims[l].getRawChunkInterval()) {
                LOG4CXX_TRACE(logger, "PhysicalCrossJoin::requiresRedimOrRepart():"
                                      << " needRepart because leftCI: " << leftDims[l].getRawChunkInterval()
                                      << " != rightCI: " << rightDims[r].getRawChunkInterval());
                if (leftIsAutochunked) {
                    leftDims[l].setChunkInterval(rightDims[r].getChunkInterval());
                }
                else {
                    rightDims[r].setChunkInterval(leftDims[l].getChunkInterval());
                }
                needRepart = true;
            }
            if (rightDims[r].getChunkOverlap() != leftDims[l].getChunkOverlap()) {
                LOG4CXX_TRACE(logger, "PhysicalCrossJoin::requiresRedimOrRepart():"
                                      << " needRepart because leftOL: " << leftDims[l].getChunkOverlap()
                                      << " != rightOL: " << rightDims[r].getChunkOverlap());
                int64_t newOverlap = min(leftDims[l].getChunkOverlap(),
                                         rightDims[r].getChunkOverlap());
                if (leftIsAutochunked) {
                    leftDims[l].setChunkOverlap(newOverlap);
                }
                else {
                    rightDims[r].setChunkOverlap(newOverlap);
                }
                needRepart = true;
            }
        }

        LOG4CXX_TRACE(logger, "PhysicalCrossJoin::requiresRedimOrRepart():"
                              << " needRepart: " << needRepart);
        if (needRepart) {
            // Some of the input dimensions may not have obtained an exemplar
            // chunkOverlap if that dimension is not part of a joining pair.
            // Any such dimensions which are specified as "autochunked" need to
            // have the chunkinterval set to DimensionDesc::PASSTHRU. This is
            // done via setPassThroughChunkIntervals().
            if (leftIsAutochunked) {
                setPassthruIfAutochunked(leftDims);
                // Create copy of left array schema, with newly tweaked dimens ions.
                _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(inputSchemas[0]));
                _redimRepartSchemas.back()->setDimensions(leftDims);
            }
            else {
                setPassthruIfAutochunked(rightDims); // if necessary.
                // Create copy of right array schema, with newly tweaked dimensions.
                _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(inputSchemas[1]));
                _redimRepartSchemas.back()->setDimensions(rightDims);
            }
            // Leave "left-most non-autochunked" array alone, and repartition other
            // array.
            size_t unchangedIndex = 0;
            size_t needsRepartIndex = 1;
            if (leftIsAutochunked) {
                needsRepartIndex = 0;
                unchangedIndex = 1;
            }
            modifiedPtrs[unchangedIndex] = nullptr;
            modifiedPtrs[needsRepartIndex] = _redimRepartSchemas.back().get();
            LOG4CXX_TRACE(logger, "PhysicalCrossJoin::requiresRedimOrRepart():"
                                  << " modifiedPtrs[0]: " << modifiedPtrs[0]
                                  << " modifiedPtrs[1]: " << modifiedPtrs[1]);
        } else {
            // The preferred way of saying "no repartitioning needed" is to
            // clear the modifiedPtrs.
            modifiedPtrs.clear();
            LOG4CXX_TRACE(logger, "PhysicalCrossJoin::requiresRedimOrRepart(): modifiedPtrs cleared");
        }
    }

    /**
     * Join is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(
            vector< std::shared_ptr<Array> >& inputArrays,
            std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);

        AutochunkFixer af(getControlCookie());
        af.fix(_schema, inputArrays);


        size_t lDimsSize = inputArrays[0]->getArrayDesc().getDimensions().size();
        size_t rDimsSize = inputArrays[1]->getArrayDesc().getDimensions().size();

        vector<int> ljd(lDimsSize, -1);
        vector<int> rjd(rDimsSize, -1);

        for (size_t p = 0, np = _parameters.size(); p < np; p += 2)
        {
            const std::shared_ptr<OperatorParamDimensionReference> &lDim =
                (std::shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            const std::shared_ptr<OperatorParamDimensionReference> &rDim =
                (std::shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];

            rjd[rDim->getObjectNo()] = lDim->getObjectNo();
        }

        size_t k=0;
        for (size_t i = 0; i < rjd.size(); i++)
        {
            if (rjd[i] != -1)
            {
                ljd [ rjd[i] ] = safe_static_cast<int>(k);
                k++;
            }
        }

        SCIDB_ASSERT(inputArrays[0]->getArrayDesc().getResidency()->isEqual(_schema.getResidency()));
        bool isReplicated = inputArrays[1]->getArrayDesc().getDistribution()->checkCompatibility(createDistribution(dtReplication));
        ASSERT_EXCEPTION(isReplicated, "Right hand input should already be replicated by inserted converter");

        // RHS might or might not already be replicated already
        std::shared_ptr<Array> replicatedAndRandom;
        if (!isReplicated) {
            LOG4CXX_TRACE(logger, "PhysicalCrossJoin::execute redistributing RHS");
            replicatedAndRandom = redistributeToRandomAccess(inputArrays[1],
                                                             createDistribution(dtReplication),
                                                             _schema.getResidency(),
                                                             query,
                                                             shared_from_this());
        } else {
            // already replicated, ensure RandomAccess
            LOG4CXX_TRACE(logger, "PhysicalCrossJoin::execute RHS already replicated, just ensuring random");
            replicatedAndRandom = PhysicalOperator::ensureRandomAccess(inputArrays[1], query);
        }

        return std::make_shared<CrossJoinArray>(_schema, inputArrays[0], replicatedAndRandom, ljd, rjd);
    }

private:

    /**
     * Set ChunkIntervals of dimensions which are autochunked to be PASSTHRU.
     *
     * @param[in,out] the dimensions to set as PASSTHRU
     *
     */
    void setPassthruIfAutochunked(Dimensions& dims) const
    {
        for (DimensionDesc& dim : dims) {
            if (dim.isAutochunked()) {
                dim.setRawChunkInterval(DimensionDesc::PASSTHRU);
            }
        }
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCrossJoin, "cross_join", "physicalCrossJoin")

}  // namespace scidb
