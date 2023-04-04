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
 * PhysicalUnpack.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 *      Author: poliocough@gmail.com
 */

#include "Unpack.h"

#include <array/MemArray.h>
#include <network/Network.h>
#include <query/PhysicalOperator.h>
#include <query/Query.h>

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.unpack"));

namespace scidb
{

using unpack::operator<<;

/**
 * The Unpack Physical Operator.
 */
class PhysicalUnpack: public PhysicalOperator
{
private:
    Coordinate _outputChunkSize;

public:
    /**
     * Create the operator.
     * @param logicalName name of the logical op
     * @param physicalName name of the physical op
     * @param parameters all the parameters the op was called with
     * @param schema the result of LogicalUnpack::inferSchema
     */
    PhysicalUnpack(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema),
        _outputChunkSize(schema.getDimensions()[0].getChunkInterval())
    {}

    /**
     * Determine whether the operator outputs full chunks.
     * @param inputSchemas the shapes of all the input arrays; one in our case
     * @return true if input is not emptyable and input chunk size matches output;
     *         false otherwise
     */
    virtual bool outputFullChunks(std::vector<ArrayDesc> const& inputSchemas) const
    {
        return unpack::outputFullChunks(inputSchemas[0], _schema);
    }

    /// @see PhysicalOperator
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                                                      std::vector<ArrayDesc> const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);

        _schema.setResidency(inputDistributions[0].getArrayResidency());
        SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));

        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    /**
     * Compute the boundaries of the output array.
     * @return the input boundaries reshaped around a single dimension. Often an over-estimate.
     */
    virtual PhysicalBoundaries getOutputBoundaries(std::vector<PhysicalBoundaries> const& inputBoundaries,
                                                   std::vector<ArrayDesc> const& inputSchemas) const
    {
        return unpack::getOutputBoundaries(inputBoundaries[0], _schema);
    }

    /**
     * Given the input array, first build an UnpackArrayInfo of how many elements each chunk has,
     * then redistribute the info to the coordinator, merge it, and use it to compute a place in the
     * output array for each chunk in the input; construct a MemArray with partially filled chunks
     * where each element is in the proper dense position. The operator will complete when the
     * optimizer inserts redistribute after the operator and merges the partially-filled chunks
     * together.
     * @param[in] inputArrays only care about the first
     * @param[in] query the query context
     * @return the MemArray with partially filled chunks wherein each element is in the correct place
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        Dimensions const& dims = inputArray->getArrayDesc().getDimensions();

        unpack::ArrayInfo info(dims.size());
        unpack::computeGlobalChunkInfo(inputArray, query, info);
        LOG4CXX_TRACE(logger, "Computed global chunk info "<<info);
        return unpack::fillOutputArray(inputArray, _schema, info, query);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnpack, "unpack", "physicalUnpack")

}  // namespace scidb
