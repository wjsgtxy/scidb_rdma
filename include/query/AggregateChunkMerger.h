#ifndef AGGREGATECHUNKMERGER_H_
#define AGGREGATECHUNKMERGER_H_
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

#include <array/MultiStreamArray.h>

#include <array/ArrayDistributionInterface.h> // For typedef ArrayDistPtr;

namespace scidb {

class Aggregate;
class PhysicalOperator;

/**
 * A partial chunk merger which uses an aggregate function to form the complete chunk.
 * It expects the partial chunks to contain aggregate state values suitable for using with the Aggregate methods.
 */
class AggregateChunkMerger : public MultiStreamArray::PartialChunkMerger
{
protected:
    std::shared_ptr<Aggregate> const _aggregate;
private:
    bool const _isEmptyable;
    std::shared_ptr<MemChunk> _mergedChunk;

public:
    /// Constructor
    AggregateChunkMerger(std::shared_ptr<Aggregate> const& agg,
                         bool isEmptyable);

    /// Destructor
    ~AggregateChunkMerger();

    /// Clear the internal state in preparation for the next chunk (position)
    void clear();

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunk
    virtual bool mergePartialChunk(size_t stream,
                                   AttributeID attId,
                                   std::shared_ptr<MemChunk>& chunk,
                                   const std::shared_ptr<Query>& query);

    /// @see MultiStreamArray::PartialChunkMerger::mergePartialChunks
    virtual void mergePartialChunks(AttributeID attId,
                                    std::vector<std::shared_ptr<MemChunk>> partialChunks,
                                    std::vector<size_t> streams,
                                    std::shared_ptr<Query> const& query) override;

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                     const std::shared_ptr<Query>& query);
};

}  // namespace scidb

#endif
