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
 * @file AggregateUtils.cpp
 * @brief Some internal utility functions for working with aggregates.
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <query/AggregateUtils.h>

#include <array/MemArray.h>
#include <query/Aggregate.h>
#include <query/PhysicalOperator.h>     // for redistributeToRandomAccess

using namespace std;

namespace scidb {

/**
 * The idea is to spread the states around by redistributing a 1-D
 * array that collides along the lone dimension, merging the states
 * and getting the results distributed to all callers.
 */
std::vector<Value>
redistributeAggregateStates(vector<AggregatePtr> const& aggs,
                            vector<Value> const& inStates,
                            shared_ptr<Query> const& query,
                            shared_ptr<PhysicalOperator>const & phyOp,
                            ArrayDistPtr const& distribution,
                            ArrayResPtr const& residency)
{
    SCIDB_ASSERT(!aggs.empty());
    SCIDB_ASSERT(aggs.size() == inStates.size());

    // Chunk::aggregateMerge() insists on IS_NULLABLE.
    int16_t const NULLABLE = AttributeDesc::IS_NULLABLE;

    // Build a MemArray to hold the input states.
    stringstream attrName;
    Attributes attrs;
    for (AttributeID aid = 0; aid < aggs.size(); ++aid) {
        attrName.str("");
        attrName << "a_" << aid;
        attrs.push_back(AttributeDesc(attrName.str(),
                                      aggs[aid]->getStateType().typeId(),
                                      NULLABLE,
                                      CompressorType::NONE));
    }
    attrs.addEmptyTagAttribute();
    Dimensions dims;
    dims.push_back(DimensionDesc("_row", 0, 0, 1, 0));
    ArrayDesc inDesc("_myAggs", attrs, dims, distribution, residency);
    shared_ptr<MemArray> statesArray = std::make_shared<MemArray>(inDesc, query);

    // Load inStates into statesArray.  This is a novel Array API idiom
    // for writing a single-cell array, let's see if it works!
    Coordinates cellCoords(1);
    assert(cellCoords[0] == 0);
    unsigned chunkMode = ChunkIterator::SEQUENTIAL_WRITE;
    const auto& statesArrayAttrs = statesArray->getArrayDesc().getAttributes();
    for (AttributeID aid = 0; aid < aggs.size(); ++aid) {  // DJG
        auto aidIter = statesArrayAttrs.find(aid);
        SCIDB_ASSERT(aidIter != statesArrayAttrs.end());
        shared_ptr<ArrayIterator> aIter = statesArray->getIterator(*aidIter);
        Chunk& chunk = aIter->newChunk(cellCoords);
        shared_ptr<ChunkIterator> cIter = chunk.getIterator(query, chunkMode);
        chunkMode |= ChunkIterator::NO_EMPTY_CHECK;
        cIter->writeItem(inStates[aid]);
        cIter->flush();
    }

    // For inexplicable reasons, the redistributeFoo API wants an
    // aggregates vector with one extra nil entry on the end.  Does it
    // expect client code to do some aggregation on the empty bitmap
    // attribute?  If so, why does it insist that the entry be null??
    //
    vector<AggregatePtr> aggsAndOneMoreNil(aggs);
    aggsAndOneMoreNil.push_back(AggregatePtr());

    // Replicate and merge.  We want merged states and not final
    // results, so there is no need for a modified output schema (as
    // with redistributeToArray).  Some or all of the aggregates may
    // be CompositeAggregates, which have no final result.
    //
    shared_ptr<Array> inArray = statesArray;
    shared_ptr<Array> outArray =
        redistributeToRandomAccess(inArray,
                                   createDistribution(dtReplication),
                                   residency,
                                   query,
                                   phyOp,
                                   aggsAndOneMoreNil,
                                   true); // enforceDataIntegrity

    // Pull!  Just one cell.
    vector<Value> results(aggs.size());
    const auto& outArrayAttrs = outArray->getArrayDesc().getAttributes();
    for (AttributeID aid = 0; aid < aggs.size(); ++aid) {
        auto aidIter = outArrayAttrs.find(aid);
        SCIDB_ASSERT(aidIter != outArrayAttrs.end());
        shared_ptr<ConstArrayIterator> aIter = outArray->getConstIterator(*aidIter);
        ConstChunk const& chunk = aIter->getChunk();
        shared_ptr<ConstChunkIterator> cIter = chunk.getConstIterator();
        results[aid] = cIter->getItem();
    }

    return results;
}

} // namespace
