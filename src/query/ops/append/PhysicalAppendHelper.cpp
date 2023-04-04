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
 * @file PhysicalAppendHelper.cpp
 * @date 2018-11-01
 * @author Mike Leibensperger
 */

#include "../reshape/ShiftArray.h"
#include "../repart/PhysicalRepart.h"
#include <network/Network.h>
#include <query/PhysicalOperator.h>

using namespace std;

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.append"));
}

namespace scidb {

class PhysicalAppendHelper : public PhysicalOperator
{
    constexpr static char const * const cls = "PhysicalAppendHelper::";
    using super = PhysicalOperator;

public:
    PhysicalAppendHelper(string const& logicalName,
                         string const& physicalName,
                         Parameters const& parameters,
                         ArrayDesc const& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    void setControlCookie(string const& cookie) override;

    PhysicalBoundaries getOutputBoundaries(const vector<PhysicalBoundaries>&,
                                           const vector<ArrayDesc>&)
        const override;

    void requiresRedimensionOrRepartition(vector<ArrayDesc> const&,
                                          vector<ArrayDesc const*>&)
        const override;

    bool outputFullChunks(vector<ArrayDesc> const&) const override;

    std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array>>& inputArrays,
            std::shared_ptr<Query> query)
        override;

private:
    PhysicalBoundaries _exchangeBounds(PhysicalBoundaries const& localBounds,
                                       std::shared_ptr<Query>& query);
    Parameter _makeConstant(int64_t x);

    /// Populated by parsing the control cookie:
    /// @{
    size_t _appendDim { ~0ULL };
    Coordinate _appendAt { CoordinateBounds::getMin() };
    Coordinate _offset { 0 };
    /// @}
};


void PhysicalAppendHelper::setControlCookie(string const& cookie)
{
    // Called exactly once, after construction but before plan
    // optimization.  See LogicalAppendHelper::getInspectable().
    SCIDB_ASSERT(_appendDim == ~0ULL);
    super::setControlCookie(cookie);

    istringstream iss(cookie);
    iss >> _appendDim;
    iss >> _appendAt;
    iss >> _offset;

    SCIDB_ASSERT(_appendDim != ~0ULL);
}


PhysicalBoundaries
PhysicalAppendHelper::getOutputBoundaries(const vector<PhysicalBoundaries>& inputBoundaries,
                                          const vector<ArrayDesc>& inputSchemas) const
{
    SCIDB_ASSERT(_appendDim != ~0ULL); // Should have parsed the cookie by now.
    Coordinate const MAX_STAR = CoordinateBounds::getMax();

    // The helper's boundaries are just the input boundaries, shifted
    // by _offset along _appendDim.  We know the _offset for
    // dataframes, BUT NOT FOR ARRAYS.  However, currently that
    // doesn't matter because the boundaries are only being used to
    // estimate data width, which will be the same no matter what the
    // _offset.
    //
    Coordinates start = inputBoundaries[0].getStartCoords();
    Coordinates end = inputBoundaries[0].getEndCoords();
    start[_appendDim] += _offset;
    if (end[_appendDim] != MAX_STAR) {
        end[_appendDim] += _offset;
    }

    // Whether for dataframes or arrays, the inferred boundaries must
    // fit within our output schema, otherwise they confuse the
    // getDataWidth() call made by the optimizer.  If they don't fit,
    // it's probably because we're appending data from an array with
    // an _appendDim dimension range that doesn't overlap our output
    // schema's.  Shift everything to fit (because eventually it
    // *will* fit, or else fail at execute time).
    //
    DimensionDesc const& outDim = _schema.getDimensions()[_appendDim];
    if (start[_appendDim] < outDim.getStartMin()) {
        Coordinate offset = outDim.getStartMin() - start[_appendDim];
        start[_appendDim] += offset;
        if (end[_appendDim] != MAX_STAR) {
            end[_appendDim] += offset;
            if (end[_appendDim] > outDim.getEndMax()) {
                stringstream ss;
                ss << "Inferred boundaries [" << CoordsToStr(start) << ", " << CoordsToStr(end)
                   << "] won't fit in output dimension " << outDim;
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                    << ss.str();
            }
        }
    }
    else if (end[_appendDim] != MAX_STAR && end[_appendDim] > outDim.getEndMax()) {
        Coordinate offset = end[_appendDim] - outDim.getEndMax();
        if (start[_appendDim] < outDim.getStartMin() + offset) {
            stringstream ss;
            ss << "Inferred boundaries [" << CoordsToStr(start) << ", " << CoordsToStr(end)
               << "] left-shifted by " << offset << " along dimension " << _appendDim
               << " won't fit in output dimension " << outDim;
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << ss.str();
        }
        start[_appendDim] -= offset;
        end[_appendDim] -= offset;
    }

    return PhysicalBoundaries(start, end);
}


void PhysicalAppendHelper::requiresRedimensionOrRepartition(vector<ArrayDesc> const& inputSchemas,
                                                            vector<ArrayDesc const*>& modifiedPtrs) const
{
    // We're feeding into insert(), and our inferred schema is the
    // same as the insertion target, so any needed repartitioning will
    // be the same as for an insert().

    if (inputSchemas[0].isDataframe()) {
        repartForStoreOrInsert(inputSchemas, modifiedPtrs);
    } else {
        // I'll be doing the repart() myself from ...::execute()!
        modifiedPtrs.clear();
    }
}


bool
PhysicalAppendHelper::outputFullChunks(vector<ArrayDesc> const& inSchemas) const
{
    // Dataframe append() will produce full chunks, but for arrays we
    // need an SG.
    return inSchemas[0].isDataframe();
}


std::shared_ptr<Array>
PhysicalAppendHelper::execute(vector<std::shared_ptr<Array>>& inputArrays,
                              std::shared_ptr<Query> query)
{
    std::shared_ptr<Array>& inArray = inputArrays[0]; // must be ref!
    try {
        checkOrUpdateIntervals(_schema, inArray);
    } catch (scidb::Exception const& e) {
        if (_schema.isDataframe()) {
            e.raise();
        }
        // Going to repart() anyway, so interval mismatch is not a problem.
        LOG4CXX_DEBUG(logger, cls << __func__ <<
                      ": Ignore chunk interval mismatch: " << e.what());
    }

    ArrayDesc const& inDesc = inArray->getArrayDesc();
    Dimensions const& inDims = inDesc.getDimensions();
    Dimensions const& outDims = _schema.getDimensions();
    SCIDB_ASSERT(inDims.size() == outDims.size());
    size_t const N_DIMS = inDims.size();
    LOG4CXX_DEBUG(logger, __func__ << ":  inDim " << inDims[_appendDim]);
    LOG4CXX_DEBUG(logger, __func__ << ": outDim " << outDims[_appendDim]);
    SCIDB_ASSERT(_appendDim < N_DIMS);

    Coordinates shiftCoords(N_DIMS, 0);

    // For dataframes, life is easy.  We obtained the target array's
    // "right edge" chunk boundary in inferSchema() and we can append
    // at that chunk boundary since no one cares about any empty cells
    // that may result.  ShiftArray does what we need!
    //
    if (isDataframe(outDims)) {
        shiftCoords[_appendDim] = _offset;
        LOG4CXX_DEBUG(logger, cls << __func__ <<
                      ": Shifting dataframe by " << CoordsToStr(shiftCoords));
        return std::make_shared<ShiftArray>(_schema, inArray, shiftCoords);
    }

    // Appending arrays.  We must learn the position of the "left
    // edge" *cell* (not chunk!) of the input.  To do that we must
    // examine the position of every cell.  Later we'll pass the input
    // to repart(), so we'll need at least MULTI_PASS access.

    if (inArray->getSupportedAccess() < Array::MULTI_PASS) {
        inArray = ensureRandomAccess(inArray, query);
    }

    // Compute the local bounding box.  We only care about cell
    // position, so just work with the empty bitmap attribute.
    Coordinates low(N_DIMS, CoordinateBounds::getMax());
    Coordinates high(N_DIMS, CoordinateBounds::getMin());
    AttributeDesc const* ebmAttr = inDesc.getEmptyBitmapAttribute();
    if (!ebmAttr) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR)
            << "append() input must have an empty bitmap attribute";
    }
    auto aIter = inArray->getConstIterator(*ebmAttr);
    while (!aIter->end()) {
        ConstChunk const& chunk = aIter->getChunk();
        auto cIter = chunk.getConstIterator();
        while (!cIter->end()) {
            Coordinates const& coords = cIter->getPosition();
            for (size_t i = 0; i < N_DIMS; ++i) {
                if (coords[i] < low[i]) {
                    low[i] = coords[i];
                }
                if (coords[i] > high[i]) {
                    high[i] = coords[i];
                }
            }
            ++(*cIter);
        }
        cIter.reset();
        ++(*aIter);
    }
    aIter->restart();
    aIter.reset();

    // Exchange local bounds info to obtain global input array bounds.
    PhysicalBoundaries bounds(low, high);
    bounds = _exchangeBounds(bounds, query);

    // Figure out the offset vector to add to each input coordinate.
    shiftCoords[_appendDim] = _appendAt - bounds.getStartCoords()[_appendDim];

    // Now that we finally know the bounding box *and* the offset
    // vector, we can check whether the input is going to fit into the
    // destination array or not... avoid work that's doomed to fail.
    //
    auto destBounds = PhysicalBoundaries::createFromFullSchema(_schema);
    bounds.shiftBy(shiftCoords); // Clobbers bounds!
    if (!destBounds.contains(bounds)) {
        // Some chunk somewhere will be out of bounds, though we don't
        // know which one at this point.
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
            << bounds << destBounds;
    }

    // Build offset vector into a keyword parameter for repart().
    KeywordParameters kwParams;
    Parameter p;
    auto group = new OperatorParamNested();
    p.reset(group);
    for (size_t i = 0; i < shiftCoords.size(); ++i) {
        Parameter c(_makeConstant(shiftCoords[i]));
        group->push_back(c);
    }
    kwParams.insert(make_pair(RedimSettings::KW_OFFSET, p));

    // Tell repart() to cut us a break w.r.t. certain assertions.
    kwParams.insert(make_pair("_append", _makeConstant(1)));

    // Destination array schema is repart()'s lone positional parameter.
    Parameters posParams(1);
    posParams[0].reset(new OperatorParamSchema(
                           _parameters[0]->getParsingContext(), _schema));

    // Physical repart() does the rest of the work!  (Must allocate
    // the operator on the heap, else buried SG-related calls to
    // shared_from_this() will crash.)
    LOG4CXX_DEBUG(logger, cls << __func__ << ": Begin repart()");
    std::shared_ptr<PhysicalOperator> repart(
        new PhysicalRepart(
            getLogicalName(), getPhysicalName(), posParams, _schema));
    repart->setKeywordParameters(kwParams);
    repart->setOperatorID(getOperatorID());
    repart->setQuery(query);
    auto result = repart->execute(inputArrays, query);
    LOG4CXX_DEBUG(logger, cls << __func__ << ": Done repart()");
    return result;
}


// Workers send their local boundaries to the coordinator.
// Coordinator computes the union and replies to the workers.  All
// instances then return the global bounds, used to compute the
// repart() offset.
//
PhysicalBoundaries
PhysicalAppendHelper::_exchangeBounds(PhysicalBoundaries const& localBounds,
                                      std::shared_ptr<Query>& query)
{
    PhysicalBoundaries bounds(localBounds);
    bool isCoord = query->isCoordinator();

    LOG4CXX_DEBUG(logger, cls << __func__ <<
                  (isCoord ? ": Coordinator" : ": Worker") <<
                  " local input boundaries: " << bounds);

    if (isCoord) {
        size_t const N_INSTANCES = query->getInstancesCount();
        if (N_INSTANCES > 1) {
            InstanceID myId = query->getInstanceID();
            for (size_t id = 0; id < N_INSTANCES; ++id) {
                if (id == myId) {
                    continue;
                }
                auto buf = BufReceive(id, query);
                PhysicalBoundaries b(PhysicalBoundaries::deSerialize(buf));
                bounds.unionWith(b);
            }
            BufBroadcast(bounds.serialize(), query);
        } // ...else just me, nothing to do.
    } else {
        InstanceID cid = query->getCoordinatorID();
        BufSend(cid, bounds.serialize(), query);
        auto buf = BufReceive(cid, query);
        bounds = PhysicalBoundaries::deSerialize(buf);
    }

    LOG4CXX_DEBUG(logger, cls << __func__ <<
                  ": Global input boundaries: " << bounds);

    return bounds;
}


Parameter PhysicalAppendHelper::_makeConstant(int64_t x)
{
    Value v;
    v.setInt64(x);

    auto exp = std::make_shared<Expression>();
    exp->compileConstant(/*tile:*/false, TID_INT64, v);

    auto ctx = _parameters[0]->getParsingContext(); // as good as any

    Parameter p;
    p.reset(new OperatorParamPhysicalExpression(ctx, exp, /*isConst:*/true));
    return p;
}


DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAppendHelper,
                                  "_append_helper", "append_helper_impl")

}  // namespace scidb
