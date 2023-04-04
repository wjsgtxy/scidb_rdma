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
 * @file PhysicalFlatten.cpp
 * @date Aug 10, 2018
 * @author Mike Leibensperger
 */

#include "FastDfArray.h"
#include "FlattenSettings.h"
#include "SlowDfArray.h"
#include "../unpack/Unpack.h"

#include <array/Metadata.h>
#include <array/SinglePassAdaptor.h>
#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.flatten"));
}

namespace scidb {

class PhysicalFlatten : public PhysicalOperator
{
    using super = PhysicalOperator;
    static constexpr char const * const cls = "PhysicalFlatten::";

public:
    PhysicalFlatten(string const& logicalName,
                    string const& physicalName,
                    Parameters const& params,
                    ArrayDesc const& schema);

    DistType
    inferSynthesizedDistType(std::vector<DistType> const& inDist,
                             size_t depth)
        const override;

    DistributionRequirement
    getDistributionRequirement(std::vector<ArrayDesc> const& sourceSchemas)
        const override;

    RedistributeContext
    getOutputDistribution(std::vector<RedistributeContext> const& inDists,
                          std::vector<ArrayDesc> const& inSchemas)
        const override;

    PhysicalBoundaries
    getOutputBoundaries(std::vector<PhysicalBoundaries> const& inputBoundaries,
                        std::vector<ArrayDesc> const& inputSchemas)
        const override;

    std::shared_ptr<Array> execute(vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
        override;

private:
    FlattenSettings _settings;
    std::shared_ptr<Array> _unpack(std::shared_ptr<Array>& inArray,
                                   std::shared_ptr<Query>& query);
};


PhysicalFlatten::PhysicalFlatten(string const& logicalName,
                                 string const& physicalName,
                                 Parameters const& params,
                                 ArrayDesc const& schema)
    : PhysicalOperator(logicalName, physicalName, params, schema)
{ }


DistType
PhysicalFlatten::inferSynthesizedDistType(std::vector<DistType> const& inDist,
                                          size_t depth) const
{
    FlattenSettings settings(getControlCookie());
    if (settings.unpackMode()) {
        return _schema.getDistribution()->getDistType();
    } else {
        return dtDataframe;
    }
}

DistributionRequirement
PhysicalFlatten::getDistributionRequirement(std::vector<ArrayDesc> const& sourceSchemas) const
{
    auto inputDist = sourceSchemas[0].getDistribution();
    auto inputDistType = inputDist->getDistType();

    auto requiredDistType = inputDistType;
    if (isReplicated(inputDistType)) {
        if (sourceSchemas[0].isDataframe()) {
            LOG4CXX_DEBUG(logger, cls << __func__ <<
                          ": Replicated dataframe, so require unreplicated");
            requiredDistType = dtDataframe;
        } else {
            LOG4CXX_DEBUG(logger, cls << __func__ <<
                          ": Replicated array, so require default distribution");
            requiredDistType = defaultDistType();
        }
    }
    else if (isUndefined(inputDistType)) {
        LOG4CXX_DEBUG(logger, cls << __func__ <<
                      ": Undefined distribution, so require default distribution");
        requiredDistType = defaultDistType();
    }

    vector<RedistributeContext> requiredDistribution;
    auto dist = createDistribution(requiredDistType);
    requiredDistribution.push_back(RedistributeContext(dist, _schema.getResidency()));
    return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
}


RedistributeContext
PhysicalFlatten::getOutputDistribution(vector<RedistributeContext> const& inDists,
                                       vector<ArrayDesc> const& inSchemas) const
{
    assertConsistency(inSchemas[0], inDists[0]);
    _schema.setResidency(inDists[0].getArrayResidency());

    FlattenSettings settings(getControlCookie());
    auto dt = settings.unpackMode() ? dtUndefined : dtDataframe;
    return RedistributeContext(
        createDistribution(dt, _schema.getDistribution()->getRedundancy()),
        _schema.getResidency());
}


PhysicalBoundaries
PhysicalFlatten::getOutputBoundaries(std::vector<PhysicalBoundaries> const& inBounds,
                                     std::vector<ArrayDesc> const& inSchemas) const
{
    FlattenSettings settings(getControlCookie());
    if (settings.unpackMode()) {
        return unpack::getOutputBoundaries(inBounds[0], _schema);
    } else {
        return super::getOutputBoundaries(inBounds, inSchemas);
    }
}


std::shared_ptr<Array>
PhysicalFlatten::execute(vector<std::shared_ptr<Array>>& inputArrays,
                         std::shared_ptr<Query> query)
{
    assert(inputArrays.size() == 1);
    auto& inArray = inputArrays[0];
    ArrayDesc const& inSchema = inArray->getArrayDesc();
    _settings.fromString(getControlCookie());

    int64_t inVolume = getChunkVolume(inSchema.getDimensions());
    int64_t chunkSize = _settings.getChunkSize();
    if (chunkSize == DimensionDesc::AUTOCHUNKED) {
        chunkSize = inVolume;
    }

    // Chunk size is now known, fix the schema.
    if (_schema.isAutochunked()) {
        _schema.getDimensions()[DF_SEQ_DIM].setChunkInterval(chunkSize);
    }

    if (_settings.unpackMode()) {
        return _unpack(inArray, query);
    }

    LOG4CXX_DEBUG(logger, cls << __func__ << ": Fast path is "
                  << (_settings.canUseFastPath() ? "enabled" : "disabled"));

    std::shared_ptr<Array> df;
    if (_settings.canUseFastPath()
            && chunkSize == inVolume
            && !inSchema.hasOverlap()
            && !_settings.rechunkMode()) {

        // Flattening.  We can use the existing inArray payloads,
        // including the empty bitmap!
        LOG4CXX_DEBUG(logger, cls << __func__ << ": Fast path, chunkSize=" << chunkSize
                      << ", inVolume=" << inVolume << ", overlap=" << inSchema.hasOverlap()
                      << ", mode=" << _settings.getMode());
        df = std::make_shared<FastDfArray>(_schema, inArray, query);
    }
    else {
        // Flattening or rechunking; we must rewrite all the payloads.
        LOG4CXX_DEBUG(logger, cls << __func__ << ": Slow path, chunkSize=" << chunkSize
                      << ", inVolume=" << inVolume << ", overlap=" << inSchema.hasOverlap()
                      << ", mode=" << _settings.getMode());
        df = std::make_shared<SlowDfArray>(_schema, inArray, query);
    }

    return std::make_shared<SinglePassAdaptor>(df, query);
}


std::shared_ptr<Array>
PhysicalFlatten::_unpack(std::shared_ptr<Array>& inArray,
                         std::shared_ptr<Query>& query)
{
    std::shared_ptr<Array> ary = ensureRandomAccess(inArray, query);
    Dimensions const& dims = ary->getArrayDesc().getDimensions();

    unpack::ArrayInfo info(dims.size());
    unpack::computeGlobalChunkInfo(ary, query, info);
    return unpack::fillOutputArray(ary, _schema, info, query);
}


DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalFlatten, "flatten", "flatten_impl")

}  // namespace scidb
