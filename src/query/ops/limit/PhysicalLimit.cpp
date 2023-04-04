/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
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

#include <array/MemArray.h>
#include <network/Network.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>

#include <boost/scope_exit.hpp>

#include <log4cxx/logger.h>

#include <limits>

using std::shared_ptr;
using std::make_shared;

using namespace std;

namespace scidb
{

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.limit"));

    using namespace scidb;

    template <class T>
    inline void resetIterators(vector<std::shared_ptr<T> > & iterators)
    {
        for (size_t i=0; i<iterators.size(); i++)
        {
            if(iterators[i].get())
            {
                iterators[i]->flush();
                iterators[i].reset();
            }
        }
    }


    class PhysicalLimit: public PhysicalOperator
    {
    public:
        PhysicalLimit(std::string const& logicalName, std::string const& physicalName,
                      Parameters const& parameters, ArrayDesc const& schema) :
            PhysicalOperator(logicalName, physicalName, parameters, schema)
        {}

        size_t countCellsInRange(shared_ptr<Array>& input, size_t const count, size_t const offset)
        {
            size_t num_cells = 0;
            size_t end_pt = count + offset;
            // TODO references ebm relative to size()
            const auto* ebmAttr = input->getArrayDesc().getAttributes().getEmptyBitmapAttribute();
            if (ebmAttr) {
                auto saiter = input->getConstIterator(*ebmAttr);

                while(!saiter->end() && num_cells < end_pt) {
                    num_cells += saiter->getChunk().count();
                    ++(*saiter);
                }

                if (num_cells > offset) {
                    num_cells = num_cells - offset;
                }
            }
            return num_cells > count ? count : num_cells;
        }

        shared_ptr<Array> makeLimitedArray(shared_ptr<Array>& input, shared_ptr<Query>& query, size_t const count, size_t const offset, size_t& actualCount)
        {
            shared_ptr<Array> result = std::make_shared<MemArray>(_schema,query);
            const auto& schemaAttrs = _schema.getAttributes(true);
            size_t const nSrcAttrs = schemaAttrs.size();
            vector<shared_ptr<ConstArrayIterator> > saiters (nSrcAttrs); //source array and chunk iters
            vector<shared_ptr<ConstChunkIterator> > sciters (nSrcAttrs);
            vector<shared_ptr<ArrayIterator> > daiters (nSrcAttrs);      //destination array and chunk iters
            vector<shared_ptr<ChunkIterator> > dciters (nSrcAttrs);
            BOOST_SCOPE_EXIT((&dciters))
            {
                resetIterators(dciters);
            } BOOST_SCOPE_EXIT_END

            for (const auto& attr : schemaAttrs)
            {
                saiters[attr.getId()] = input->getConstIterator(attr);
                daiters[attr.getId()] = result->getIterator(attr);
            }

            actualCount = 0;
            const auto& fda = schemaAttrs.firstDataAttribute();
            while ( !saiters[fda.getId()]->end() && actualCount < count)
            {
                Coordinates const& chunkPos = saiters[fda.getId()]->getPosition();
                for (AttributeID i = 0; i < nSrcAttrs; i++)
                {
                    sciters[i] = saiters[i]->getChunk().getConstIterator();
                    //populate empty tag from attr 0 implicitly
                    int mode = ChunkIterator::SEQUENTIAL_WRITE;
                    if (i) {
                        mode |= ChunkIterator::NO_EMPTY_CHECK;
                    }
                    dciters[i] = daiters[i]->newChunk(chunkPos).getIterator(query, mode);
                }
                // Skip the offset number of cells
                for (size_t j = 0; j < offset; j++)
                {
                    for(size_t i=0; i<nSrcAttrs; i++)
                    {
                        ++(*sciters[i]);
                    }
                }
                while(!sciters[fda.getId()]->end() && actualCount < count)
                {
                    Coordinates const inputCellPos = sciters[fda.getId()]->getPosition();
                    for (const auto& attr : schemaAttrs)
                    {
                        dciters[attr.getId()]->setPosition(inputCellPos);
                        dciters[attr.getId()]->writeItem(sciters[attr.getId()]->getItem());
                        ++(*sciters[attr.getId()]);
                    }
                    actualCount++;
                }
                for (const auto& attr : schemaAttrs)
                {
                    dciters[attr.getId()]->flush();
                    dciters[attr.getId()].reset();
                    ++(*saiters[attr.getId()]);
                }
            }
            resetIterators(dciters);
            return result;
        }

        shared_ptr<Array> execute(vector<std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
        {
            size_t count = std::numeric_limits<ssize_t>::max();
            size_t offset = 0;

            Parameter countParam = findKeyword("count");
            if (_parameters.size() >= 1) {
                ASSERT_EXCEPTION(!countParam, "Conflicting positional and keyword count parameters!");
                countParam = _parameters[0];
            }

            Value const& limVal = ((std::shared_ptr<OperatorParamPhysicalExpression>&)countParam)->getExpression()->evaluate();
            if (!limVal.isNull() && limVal.getUint64() < static_cast<size_t>(std::numeric_limits<ssize_t>::max()))
            {
                count = limVal.getUint64();
            }

            Parameter offsetParam = findKeyword("offset");
            if (!offsetParam)
            {
                if (_parameters.size() >= 2)
                {
                    offsetParam = _parameters[1];
                }
                else
                {
                    SCIDB_ASSERT(!offsetParam);
                }
            }
            if (offsetParam) {
                offset = ((std::shared_ptr<OperatorParamPhysicalExpression>&)offsetParam)->getExpression()->evaluate().getUint64();
            }

            size_t availableCount = countCellsInRange(inputArrays[0], count, offset);
            InstanceID const myId    = query->getInstanceID();
            InstanceID const coordId = query->getCoordinatorID() == INVALID_INSTANCE ? myId : query->getCoordinatorID();
            size_t const numInstances = query->getInstancesCount();
            size_t requiredCount = 0;
            if(myId != coordId) //send my count to the coordinator
            {
                shared_ptr<SharedBuffer> buffer(new MemoryBuffer(&availableCount, sizeof(availableCount)));
                BufSend(coordId, buffer, query);
                buffer = BufReceive(coordId, query);
                requiredCount = *(reinterpret_cast<const size_t*>(buffer->getConstData()));
            }
            else //on coordinator, lower everyone's count as needed
            {
                vector<size_t> requiredCounts(numInstances);
                for(size_t i =0; i<numInstances; ++i)
                {
                    if(i==myId)
                    {
                        requiredCounts[myId] = availableCount;
                        continue;
                    }
                    shared_ptr<SharedBuffer> buffer = BufReceive(i, query);
                    requiredCounts[i] = *(reinterpret_cast<const size_t*>(buffer->getConstData()));
                }
                size_t totalCount =0;
                for(size_t i=0; i<numInstances; ++i)
                {
                    if(requiredCounts[i] > count)
                    {
                        requiredCounts[i] = count;
                    }
                    else if(totalCount + requiredCounts[i] > count)
                    {
                        // Need (count - totalCount), guarding against underflow.
                        requiredCounts[i] = static_cast<ssize_t>(count) - static_cast<ssize_t>(totalCount) > 0 ? count - totalCount : 0;
                    }
                    totalCount += requiredCounts[i];
                }
                for(size_t i =0; i<numInstances; ++i)
                {
                    if(i==myId)
                    {
                        requiredCount = requiredCounts[myId];
                        continue;
                    }
                    shared_ptr<SharedBuffer> buffer(new MemoryBuffer(&(requiredCounts[i]), sizeof(size_t)));
                    BufSend(i, buffer, query);
                }
            }
            if(requiredCount == 0)
            {
                return shared_ptr<Array>(new MemArray(_schema, query));
            }
            else
            {
                return makeLimitedArray(inputArrays[0], query, requiredCount, offset, availableCount);
            }
        }
    };

    DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLimit, "limit", "PhysicalLimit")

}
// end namespace scidb
