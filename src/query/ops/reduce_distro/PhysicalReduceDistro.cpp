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
 * PhysicalProject.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <array/DelegateArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>

#include <log4cxx/logger.h>

namespace scidb
{

using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.reduce_distro"));

class ReduceDistroArrayIterator : public DelegateArrayIterator
{
public:
   ReduceDistroArrayIterator(const std::shared_ptr<Query>& query,
                             DelegateArray const& delegate,
                             const AttributeDesc& attrID,
                             std::shared_ptr<ConstArrayIterator> inputIterator,
                             DistType dt):
   DelegateArrayIterator(delegate, attrID, inputIterator),
   _dt(dt), _myInstance(query->getInstanceID()), _nextChunk(0), _query(query)
    {
        findNext();
    }


    virtual ~ReduceDistroArrayIterator()
    {}

    virtual void findNext()
    {
        while (!inputIterator->end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
            if (getInstanceForChunk(pos,
                                    array.getArrayDesc().getDimensions(),
                                    array.getArrayDesc().getDistribution(),
                                    array.getArrayDesc().getResidency(),
                                    query) == _myInstance)
            {
                _nextChunk = &inputIterator->getChunk();
                _hasNext = true;
                return;
            }

            ++(*inputIterator);
        }
        _hasNext = false;
    }

    void restart() override
    {
        chunkInitialized = false;
        inputIterator->restart();
        findNext();
    }

    bool end() override
    {
        return _hasNext == false;
    }

    void operator ++() override
    {
        chunkInitialized = false;
        ++(*inputIterator);
        findNext();
    }

    bool setPosition(Coordinates const& pos) override
    {
        chunkInitialized = false;
        std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        if (getInstanceForChunk(pos,
                                array.getArrayDesc().getDimensions(),
                                array.getArrayDesc().getDistribution(),
                                array.getArrayDesc().getResidency(),
                                query) == _myInstance &&
            inputIterator->setPosition(pos))
        {
            _nextChunk = &inputIterator->getChunk();
            return _hasNext = true;
        }

        return _hasNext = false;
    }

    ConstChunk const& getChunk() override
    {
        if (!chunkInitialized)  {
            _chunkPtr()->setInputChunk(*_nextChunk);
            chunkInitialized = true;
        }
        return *_chunkPtr();
    }

private:
    bool _hasNext;
    DistType _dt;
    InstanceID _myInstance;
    ConstChunk const* _nextChunk;
    std::weak_ptr<Query> _query;
};



class ReduceDistroArray: public DelegateArray
{
public:
    ReduceDistroArray(const std::shared_ptr<Query>& query,
                      ArrayDesc const& desc,
                      std::shared_ptr<Array> const& array,
                      DistType dt):
    DelegateArray(desc, array, true), _dt(dt)
    {
        assert(query);
        _query = query;
    }

    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override
    {
        return new ReduceDistroArrayIterator(Query::getValidQueryPtr(_query),
                                             *this, id, getPipe(0)->getConstIterator(id), _dt);
    }

private:
   DistType _dt;
};

class PhysicalReduceDistro: public  PhysicalOperator
{
public:
    PhysicalReduceDistro(const string& logicalName,
                         const string& physicalName,
                         const Parameters& parameters,
                         const ArrayDesc& schema)
        :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    DistType myDistType() const
    {
        DistType result = (DistType)((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        return result;
    }

    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        DistType dt = myDistType();

        LOG4CXX_TRACE(logger, "PhysicalReduceDistro::inferSynthesizedDistType: dt=" << dt);
        LOG4CXX_TRACE(logger, "PhysicalReduceDistro::inferSynthesizedDistType: schema dt="
                      << _schema.getDistribution()->getDistType());
        SCIDB_ASSERT(_schema.getDistribution()->getDistType() == dt);

        return dt;
    }

    virtual RedistributeContext
    getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                          std::vector<ArrayDesc> const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);
        DistType dt = myDistType();

        LOG4CXX_TRACE(logger, "PhysicalReduceDistro::getOutputDist: dt=" << dt);
        LOG4CXX_TRACE(logger, "PhysicalReduceDistro::getOutputDist: schema dt="
                      << _schema.getDistribution()->getDistType());
        SCIDB_ASSERT(_schema.getDistribution()->getDistType() == dt);

        ASSERT_EXCEPTION(_schema.getResidency()->isEqual(inputDistributions[0].getArrayResidency()),
                         "reduce_distro must not change residency");

        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * Project is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    virtual std::shared_ptr<Array>
    execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        DistType dt = (DistType)((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();

        // Can only reduce to a dataframe if the input itself is a (replicated) dataframe.
        // Otherwise, only flatten() can produce dataframe distributions, not the reducer.  We can't
        // check this at inferSchema time because the optimizer can insert a physical reducer node
        // directly.
        ASSERT_EXCEPTION(not isDataframe(dt) || inputArrays[0]->getArrayDesc().isDataframe(),
                         "Only flatten() may convert arrays to the dtDataframe distribution type");

        return std::shared_ptr<Array>(new ReduceDistroArray(query, _schema, inputArrays[0], dt));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalReduceDistro, "_reduce_distro", "physicalReduceDistro")

}  // namespace scidb
