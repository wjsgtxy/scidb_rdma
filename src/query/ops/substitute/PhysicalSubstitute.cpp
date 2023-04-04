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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */


#include <array/ConstItemIterator.h>
#include <array/DelegateArray.h>

#include <array/StreamArray.h>

#include <query/PhysicalOperator.h>

#include <util/Utility.h>


using namespace std;

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.substitute"));

class SubstituteArray;

class SubstituteChunkIterator : public DelegateChunkIterator
{
  public:
    virtual int getMode()
    {
        return mode;
    }

    virtual Value const& getItem();

    SubstituteChunkIterator(SubstituteArray& arr, DelegateChunk const* chunk, int iterationMode);

  private:
    SubstituteArray& array;
    std::shared_ptr<ConstItemIterator> itemIterator;
    int mode;
    Coordinates pos;
};

class SubstituteArray : public DelegateArray
{
  public:
    virtual DelegateChunkIterator* createChunkIterator(DelegateChunk const* chunk, int iterationMode) const
    {
        if(substituteAttrs[chunk->getAttributeDesc().getId()])
        {
            return new SubstituteChunkIterator(*(SubstituteArray*)this, chunk, iterationMode);
        }

        return DelegateArray::createChunkIterator(chunk, iterationMode);
    }

    SubstituteArray(ArrayDesc const& desc, std::shared_ptr<Array> input, std::shared_ptr<Array> subst, vector<uint8_t> const& substAttrs)
    : DelegateArray(desc, input, false),
      substArray(subst),
      substituteAttrs(substAttrs)
    {
    }

    std::shared_ptr<Array> substArray;

  private:
    vector<uint8_t> substituteAttrs;

};

SubstituteChunkIterator::SubstituteChunkIterator(SubstituteArray& arr, DelegateChunk const* chunk, int iterationMode)
: DelegateChunkIterator(chunk, iterationMode & ~IGNORE_NULL_VALUES),
  array(arr),
  itemIterator(arr.substArray->getItemIterator(arr.substArray->getArrayDesc().getAttributes().firstDataAttribute())),
  mode(iterationMode),
  pos(1)
{
}


Value const& SubstituteChunkIterator::getItem()
{
    Value const& val = inputIterator->getItem();
    if (val.isNull()) {
        pos[0] = val.getMissingReason();
        if (!itemIterator->setPosition(pos))
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_SUBSTITUTE_FAILED) << pos[0]; // TODO: bad exception substitution
        return itemIterator->getItem();
    }
    return val;
}


class PhysicalSubstitute: public PhysicalOperator
{
public:
	PhysicalSubstitute(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    // required to allow replicated input
    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        vector<uint8_t> result(numChildren, false);
        SCIDB_ASSERT(numChildren==2);
        result[1] = true;   // permitted on the right-hand input
        return result;
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    /// see OperatorDist
    void checkInputDistAgreement(std::vector<DistType> const& inputDistrib, size_t /*depth*/) const
    {
        SCIDB_ASSERT(inputDistrib.size() == 2);
        // no restriction on inputDistribution values
    }

	/***
	 * Substitute is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
	 * that overrides the chunkiterator method.
	 */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        //if no parameters are given, we assume we are substituting all nullable attributes
        vector<uint8_t> substituteAttrs (inputArrays[0]->getArrayDesc().getAttributes().size(), _parameters.size() == 0 ? true : false);
        for (size_t i = 0, n = _parameters.size(); i < n; i++)
        {
            size_t attId = ((std::shared_ptr<OperatorParamReference>&)_parameters[i])->getObjectNo();
            substituteAttrs[attId] = true;
        }

        // RHS might or might not already be replicated already
        bool isReplicated = inputArrays[1]->getArrayDesc().getDistribution()->checkCompatibility(createDistribution(dtReplication));
        std::shared_ptr<Array> input1;
        if (!isReplicated) {
            LOG4CXX_TRACE(logger, "PhysicalSubstitute::execute redistributing RHS");
            input1 = redistributeToRandomAccess(inputArrays[1],
                                                createDistribution(dtReplication),
                                                inputArrays[0]->getArrayDesc().getResidency(),
                                                query,
                                                shared_from_this());
        } else {
            // already replicated, ensure RandomAccess
            LOG4CXX_TRACE(logger, "PhysicalSubstitute::execute RHS already replicated, just ensuring random");
            input1 = PhysicalOperator::ensureRandomAccess(inputArrays[1], query);
        }

        return std::make_shared<SubstituteArray>(_schema, inputArrays[0], input1, substituteAttrs);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSubstitute, "substitute", "physicalSubstitute")

}  // namespace scidb
