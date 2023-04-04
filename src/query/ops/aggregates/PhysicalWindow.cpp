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
 * PhysicalWindow.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik, poliocough@gmail.com,
 *              Paul Brown <paulgeoffreybrown@gmail.com>
 */
#include "query/PhysicalOperator.h"

#include "array/Array.h"
#include "WindowArray.h"

namespace scidb {

using namespace std;

class PhysicalWindow: public  PhysicalOperator
{
private:
    vector<WindowBoundaries> _window;

public:

    PhysicalWindow(const string& logicalName, const string& physicalName,
                   const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        size_t nDims = _schema.getDimensions().size();
        _window = vector<WindowBoundaries>(nDims);
        for (size_t i = 0, size = nDims * 2, boundaryNo = 0; i < size; i+=2, ++boundaryNo)
        {
            _window[boundaryNo] = WindowBoundaries(
                    ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i])->getExpression()->evaluate().getInt64(),
                    ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+1])->getExpression()->evaluate().getInt64()
                    );
        }
    }

    /**
     * @see PhysicalOperator::requiresRedimensionOrRepartition()
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        SCIDB_ASSERT(inputSchemas.size() == 1);
        SCIDB_ASSERT(modifiedPtrs.size() == 1);

        if (inputNeedsRepart(inputSchemas[0])) {
            // OK, we're free to choose our own repartitioning!
            modifiedPtrs[0] = getRedimensionOrRepartitionSchema(inputSchemas[0]);
        } else {
            // All good as-is.
            modifiedPtrs.clear();
        }
    }

    ArrayDesc* getRedimensionOrRepartitionSchema(
        ArrayDesc const& inputSchema) const
    {
        _redimRepartSchemas.clear(); // Forget previous results, if any.
        Attributes attrs = inputSchema.getAttributes();

        Dimensions dims;
        for (size_t i =0; i<inputSchema.getDimensions().size(); i++)
        {
            DimensionDesc inDim = inputSchema.getDimensions()[i];

            int64_t overlap = inDim.getChunkOverlap();
            int64_t const neededOverlap = std::max(_window[i]._boundaries.first, _window[i]._boundaries.second);
            if ( neededOverlap > inDim.getChunkOverlap())
            {
                overlap = neededOverlap;
            }

            int64_t rawChunkInterval = inDim.getRawChunkInterval();
            if (inDim.isAutochunked()) {
                rawChunkInterval = DimensionDesc::PASSTHRU;
                // Note: If the child is a repart/redimension this will throw in the optimizer
                // since the overlap in the explicit repart/redimension will not
                // match this needed overlap.
                //
                // Other operators will get a  repart with a passthrough chunk specified
                // that will be handled in the redimension/repart execute().
            }

            dims.push_back( DimensionDesc(inDim.getBaseName(),
                                          inDim.getNamesAndAliases(),
                                          inDim.getStartMin(),
                                          inDim.getCurrStart(),
                                          inDim.getCurrEnd(),
                                          inDim.getEndMax(),
                                          rawChunkInterval,
                                          overlap));
        }

        _redimRepartSchemas.push_back(std::make_shared<ArrayDesc>(inputSchema.getName(), attrs, dims,
                                                                  inputSchema.getDistribution(),
                                                                  inputSchema.getResidency()));
        return _redimRepartSchemas.back().get();
    }

    bool inputNeedsRepart(ArrayDesc const& input) const
    {
        Dimensions const& dims = input.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& srcDim = dims[i];
            bool justOneChunk = !srcDim.isAutochunked() &&
                static_cast<uint64_t>(srcDim.getChunkInterval()) == srcDim.getLength();
            if (!justOneChunk &&
                srcDim.getChunkOverlap() < std::max(_window[i]._boundaries.first, _window[i]._boundaries.second))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * window(...) is a pipelined operator, hence it executes by returning an
     * iterator-based array to the consumer that overrides the chunkiterator
     * method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        ArrayDesc const& inDesc = inputArray->getArrayDesc();
        if (inputNeedsRepart(inDesc)) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_WINDOW_ERROR2);
        }

        vector<AttributeID> inputAttrIDs;
        vector<AggregatePtr> aggregates;
        string method;

        //
        //  Probe the list of operator parameters for aggregates and
        // the optional "method" argument. Note that checks about the
        // correctness of these arguments (valid aggregate names,
        // valid method names) have already occurred in LogicalWindow.
        for (size_t i = inDesc.getDimensions().size() * 2, size = _parameters.size(); i < size; i++)
        {
            std::shared_ptr<scidb::OperatorParam> & param = _parameters[i];

            if (PARAM_AGGREGATE_CALL == param->getParamType())
            {
                AttributeDesc inAttId;
                AggregatePtr agg = resolveAggregate((std::shared_ptr <OperatorParamAggregateCall> const&) _parameters[i],
                                                inDesc.getAttributes(),
                                                &inAttId,
                                                0);

                aggregates.push_back(agg);

                if (inAttId == AttributeDesc())
                {
                    //for count(*); optimize later
                    inputAttrIDs.push_back(inDesc.getAttributes().firstDataAttribute().getId());
                } else
                {
                    inputAttrIDs.push_back(inAttId.getId());
                }
            } else if (PARAM_PHYSICAL_EXPRESSION == param->getParamType())
            {
                method = ((std::shared_ptr<OperatorParamPhysicalExpression>&)param)->getExpression()->evaluate().getString();
            }
        }

        return std::shared_ptr<Array>(new WindowArray(_schema, inputArray, _window, inputAttrIDs, aggregates, method));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalWindow, "window", "physicalWindow")

}  // namespace scidb
