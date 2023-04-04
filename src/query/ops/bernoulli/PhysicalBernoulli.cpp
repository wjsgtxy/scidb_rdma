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
 * PhysicalBernoulli.cpp
 *
 *  Created on: Feb 15, 2010
 *      Author: Knizhnik
 *  Re-worked:  November 15, 2016
 *      Author: Paul G. Brown
 *
 *   The operator/array/iterator code in this file implements a bernoulli
 *  random sampling filter.
 *
 *    See: https://en.wikipedia.org/wiki/Bernoulli_sampling
 *
 *   The purpose of this operator is to cut the number of data values that
 *  need to be processed down to a representative sample. In abstract terms,
 *  the bernoulli sampler tosses a (heavily weighted) coin once at each cell.
 *  If the toss comes up 'heads' then the cell's value is included in the
 *  sample result. If (as is almost always the case) the toss is 'tails',
 *  then the cell is discarded.
 *
 *   This has the effect of dramatically reducing the amount of data (number
 *  of values) that need to be processed so as to speed up (at some loss of
 *  precision) the calculation of statistical results like count, mean,
 *  variance, and so on.
 *
 *   A naive physical implementation of a bernoulli sampler would toss a coin
 *  once for each cell. But that would be highly inefficient. So instead, the
 *  procedure we adopt is taken from:
 *
 *  http://www.ittc.ku.edu/~jsv/Papers/Vit87.RandomSampling.pdf
 *
 *   The basic idea is to avoid tossing a coin N times, once for each cell.
 *  Instead we generate a random number to tell us how many times we would
 *  have to have tossed the coin before it came up 'head'. Having generated
 *  this number (referred to as the 'Step Length' in the code blow) we then
 *  leap ahead in the scan by that many cells. If 'p' is sufficiently small,
 *  this might actually result in us stepping over entire chunks.
 *
 */

#include <array/DelegateArray.h>
#include <query/Expression.h>
#include <query/PhysicalOperator.h>

#include <log4cxx/logger.h>

#include <random>


using namespace std;

//
// Logger for operator. static to prevent visibility outside file
static log4cxx::LoggerPtr bernoulliLogger(log4cxx::Logger::getLogger("scidb.query.ops.bernoulli"));

namespace scidb {

/**
 *  @see DelegateArrayIterator
 */
class BernoulliArrayIterator : public DelegateArrayIterator
{

  friend class BernoulliChunkIterator;

  public:

    BernoulliArrayIterator( DelegateArray const& array,
                            const AttributeDesc& attrID,
                            std::shared_ptr<ConstArrayIterator> inputIterator,
                            double prob,
                            int32_t rndGenSeed )
    : DelegateArrayIterator(array, attrID, inputIterator),
      _seed(rndGenSeed),
      _rng_gen(rndGenSeed),
      _geom_distr(prob),
      _inputArrayDesc(array.getPipe(0)->getArrayDesc()),
      _stepLength(0),
      _elemsInThisChunk(0),
      _isPlainArray(_inputArrayDesc.getEmptyBitmapAttribute() == NULL)
    {
        //
        //  If the attrID passed in is larger than or equal to the number of
        // attributes in the input array, then we're being asked to construct
        // a new isEmpty bit map (Coordinates list).
        _isNewEmptyIndicator = ( attrID.getId() >= ( array.getPipe(0)->getArrayDesc().getAttributes().size()));
        restart();
    }

    //
    //  This implementation features a complex little dance that involves
    // the ArrayIterator, and the ChunkIterator. In order to keep the two
    // seperate, when the ChunkIterator is constructed state from the
    // ArrayIterator is used to set up the ChunkIterator, which can then
    // proceed independently of the ArrayIterator.
    //
    //  Then when the ArrayIterator is incremented, it needs to step through
    // whatever its "current" chunk contains (in exactly the same sequence
    // of random steps as the chunks followed) before stepping to the "next"
    // chunk.
    void operator++() override
    {
        SCIDB_ASSERT ( !inputIterator->end() );

        //
        //  Step through the "current" chunk's cells before ...
        stepThroughChunk();
        //
        //  ... stepping to the next input chunk.
        ++(*inputIterator);
        //
        //  And of course, in case the next step length is larger than the
        // number of cells in the "next" chunk, try to step to whatever
        // is chunk that actually contains the "next" cell.
        stepToTargetChunk();
    }

    bool setPosition(Coordinates const& pos) override
    {
        bool result = false;                       // result
        CoordinatesLess less;
        Coordinates targetPos = pos;
        Coordinates currentPos;
        _inputArrayDesc.getChunkPositionFor(targetPos);

        //
        //  If we're at the end of an Iterator, or if the input Iterator's
        // position is *behind* the current position, then we need to reset
        // ourselves all the way back to the start of the entire Array scan
        // before working through the input again. This reset to the
        // beginning again is necessary to guarantee the stability of the
        // result, but restarting is obviously very expensive.
        //
        //  Fortunately, probing backwards rarely happens.
        if ( end() || less(targetPos, inputIterator->getPosition()))
        {
            restart();
        }

        //
        //  Step through the chunks in the inputIterator to the chunk that
        // contains the "next" cell.
        while ( !end() && less(inputIterator->getPosition(), targetPos) ) {
            //
            //  The "next" cell is in a chunk ahead of the current input
            // position. So we need to step through the "current" chunk ...
            stepThroughChunk();
            //
            //  ... step to the "next" input chunk ...
            ++(*inputIterator);

            //
            //  ... and in case the "next" cell is not in the "next" chunk,
            //  try searching ahead for it.
            stepToTargetChunk();
        }

        //
        //  Return true iif. we're not at the end of the input scan, and
        // the chunk we wanted is the chunk we've arrived at. Note that
        // this does not guarantee that the target "cell" is actually in
        // the chunk. Only that the chunk is here.
        if ( !end() && (targetPos == inputIterator->getPosition())) {
            result = true;
        }

        return result;
    }

    void restart() override
    {
        //
        // Restart the input iterator ...
        inputIterator->restart();

        //
        //  We need to reset the rng to use the seed we were passed when
        // we were started, and step to the "first" cell in the sample.
        _rng_gen.seed(_seed);
        _stepLength = getStep();

        //
        //  Step to the chunk from the input iterator that holds that holds
        // a value that will be passed to the sample output.
        stepToTargetChunk();
    }

  private:

    //
    //   Use the geometric distribution generator to calculate how many
    //  cells should be stepped over before we accept one into the sample.
    //  Must be at least one.
    size_t getStep()
    {
        return _geom_distr(_rng_gen) + 1;
    }

    //
    //  Step through a chunk "as if" you were sampling from it, but don't
    // pull values from the input.
    void stepThroughChunk()
    {
        while (_stepLength < _elemsInThisChunk) {
          _stepLength += getStep();
        }
        _stepLength -= _elemsInThisChunk;
    }

    //
    //   The sampling procedure has taken us beyond the "previous" chunk. Step
    //  through the input iterator until we findg the chunk that contains
    //  the next value to be included in the sample. NOTE: This may (should)
    //  lead to us skipping entire chunks if the 'p' is low enough.
    void stepToTargetChunk()
    {
      //
      //  Step over enough chunks to get to the chunk at the appropriate
      // offset.
      while (!inputIterator->end()) {
        _elemsInThisChunk = inputIterator->getChunk().count();
        if (_stepLength < _elemsInThisChunk) {
          break;
        }
        _stepLength -= _elemsInThisChunk;
        ++(*inputIterator);
      }
    }

    int32_t                                   _seed; // The RNG seed ...
    std::mt19937                           _rng_gen; // Standard uniform RNG
    std::geometric_distribution<size_t> _geom_distr; // Geometric distribution

    ArrayDesc const&                _inputArrayDesc;

    size_t                              _stepLength; // Size of 'next' step to
                                                     // take.
    size_t                        _elemsInThisChunk; // Number of cells in
                                                     // the current chunk.
    bool                              _isPlainArray;
    bool                       _isNewEmptyIndicator;
};

/**
 *  @see DelegateChunkIterator
 */
class BernoulliChunkIterator : public DelegateChunkIterator
{
  public:

    //
    // Lots of local values to be initialized ...
    BernoulliChunkIterator(DelegateChunk const* chunk, int iterationMode)
      : DelegateChunkIterator(chunk, ConstChunkIterator::IGNORE_OVERLAPS),
        _lastOffsetIntoChunk(0),
        _chunk_start_rng_gen(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._rng_gen),
        _rng_gen(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._rng_gen),
        _chunk_start_stepLength(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._stepLength),
        _stepLength(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._stepLength),
        _geom_distr(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._geom_distr.p()),
        _attr(chunk->getAttributeDesc()),
        _elemsInThisChunk(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._elemsInThisChunk),
        _isPlainArray(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._isPlainArray),
        _isNewEmptyIndicator(((BernoulliArrayIterator&)(chunk->getArrayIterator()))._isNewEmptyIndicator)
    {
      SCIDB_ASSERT ( _stepLength < _elemsInThisChunk );

      setSamplePositionWithinChunk();
      _trueValue.setBool(true);
    }

    //
    //   Restart the ChunkIterator.
    void restart() override
    {
       //
       //  Restart the input iterator ...
       inputIterator->restart();

       //
       //   Reset the RNG and the _stepLength to the point they were at the
       //  time we began working with this chunk.
       _rng_gen                   =        _chunk_start_rng_gen;
       _stepLength                =     _chunk_start_stepLength;
       _lastOffsetIntoChunk       =                           0;
       _hasCurrentItem            =       !inputIterator->end();
    }

    //
    //  Take a single step through the cells in a chunk. Might find a
    // cell, or might reach the end of the chunk.
    void operator++() override
    {
      if (!_hasCurrentItem)
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

      _stepLength += getStep();

      if ( _stepLength < _elemsInThisChunk) {
        //
        //  We have stepped to the 'next' cell, and it is within 'this' chunk ...
        setSamplePositionWithinChunk();
      } else {
        //
        //  We have stepped outside of this chunk ...
        _hasCurrentItem = false;
      }
    }

    //
    // True iif we're at the end of cells from the chunk ...
    bool end() override
    {
      return !_hasCurrentItem;
    }

    Value const& getItem() override
    {
      SCIDB_ASSERT(_hasCurrentItem);
      return _isNewEmptyIndicator ? _trueValue : DelegateChunkIterator::getItem();
    }

    bool setPosition ( Coordinates const& targetPos )
    {
      bool result = false;
      CoordinatesLess less;

      //
      //  If we're going backwards in the chunk, then we need to start at
      // the beginning of the chunk scan again.
      if ( !end() && less ( targetPos, inputIterator->getPosition()))  {
        restart();
        setSamplePositionWithinChunk();
      }

      //
      // Then step to the cell we want ...
      while ( !end() && less(inputIterator->getPosition(), targetPos) ) {
        _stepLength += getStep();
        setSamplePositionWithinChunk();
      }

      //
      //  ... and set result to 'true' if it is.
      if ( !end() && (targetPos == inputIterator->getPosition()) ) {
        result = true;
      }
      return result;
    }

    private:

    //
    //   Use the geometric distribution generator to calculate how many
    //  cells should be stepped over before we accept one into the sample.
    size_t getStep()
    {
        return _geom_distr(_rng_gen) + 1;
    }

    //
    //  Find the location of the next cell in the sample within 'this' chunk.
    void setSamplePositionWithinChunk()
    {
      //
      //   If the input is not a materialized array, then we need to step
      //  through the chunk's inputIterator _stepLength times. But that's a
      //  bit tricky, as we need to track both how far we've travelled
      //  through the current chunk independently of where we are in the
      //  overall sample scan.
      if (!_isPlainArray) {
        //
        //   Step through the chunk arrayIterator._stepLength times after the
        //  current offset into the chunk.
        size_t stepCnt = _stepLength - _lastOffsetIntoChunk;

        //
        // Step through the input chunk, one cell at a time ...
        while ( stepCnt > 0 && !inputIterator->end()) {
          ++(*inputIterator);
          stepCnt--;
        }
        //
        //  ... now, either ( a ) we're at the end of the chunk in which case
        //                        we need to calculate a new _stepLength and
        //                        set the _lastOffsetIntoChunk to zero. OR
        //                  ( b ) we're at some point within the chunk, in which
        //                        case we set the _lastOffsetIntoChunk to
        //                        _stepLength.
        if ( 0 == stepCnt ) {
           //
           //  Within the chunk ...
           _lastOffsetIntoChunk = _stepLength;
        } else {
           //
           // At the end of the chunk
           _lastOffsetIntoChunk = 0;
        }

        //
        //  ... and check that we aren't at the end of the chunk.
        _hasCurrentItem = !inputIterator->end();

      } else {
        //
        //  With a plain array, we can probe the input to a particular position
        // based on the _stepLength.
        Coordinates pos         = chunk->getFirstPosition(false);
        Coordinates const& last = chunk->getLastPosition(false);

        //
        //  Calculate the position within the chunk, using the arrayIterator._stepLength
        for (int64_t i = static_cast<int64_t>(pos.size()); --i >= 0; ) {
          size_t length = last[i] - pos[i] + 1;
          pos[i] += _stepLength % length;
          _stepLength /= length;
        }
        SCIDB_ASSERT( _stepLength == 0 );
        _hasCurrentItem = inputIterator->setPosition(pos);
      }
    }

    size_t                     _lastOffsetIntoChunk; // Current position within the chunk ...

    Value                                _trueValue; // Simply a scidb::Value holder for "true" ...
    bool                            _hasCurrentItem; // True if the value is in the chunk ...

    std::mt19937               _chunk_start_rng_gen; // The state of the RNG
                                                     // at the time we loaded
                                                     // 'this' chunk.
    std::mt19937                           _rng_gen; // Current RNG ...

    size_t                  _chunk_start_stepLength; // Step length when
                                                     // iterator created.
    size_t                              _stepLength; // Current step length

    std::geometric_distribution<size_t> _geom_distr; // Standard geomtric

    AttributeDesc                             _attr;

    size_t                        _elemsInThisChunk; // Number of cells in
                                                     // the current chunk.

    bool                              _isPlainArray; //
    bool                       _isNewEmptyIndicator; //
};

/**
 *  @see DelegateArray
 */
class BernoulliArray : public DelegateArray
{

  public:

    virtual DelegateChunkIterator* createChunkIterator( DelegateChunk const* chunk,
                                                        int iterationMode) const
    {
        return new BernoulliChunkIterator(chunk, iterationMode);
    }

    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override
    {
        const auto& attrs = getArrayDesc().getAttributes();
        const auto& fda = attrs.firstDataAttribute();
        return new BernoulliArrayIterator( *this, id,
                                           getPipe(0)->getConstIterator(id.getId() < _nAttrs ? id : fda),
                                           _probability, _seed);
    }

    BernoulliArray( ArrayDesc const& desc, std::shared_ptr<Array> input,
                    std::shared_ptr<Query> query,
                    double prob,
                    int32_t rndGenSeed )
    : DelegateArray(desc, input),
      _query (query),
      _nAttrs(input->getArrayDesc().getAttributes().size()),
      _probability(prob),
      _seed(rndGenSeed)
    {}

  private:
    std::shared_ptr<Query>            _query; // Handle to query.
    size_t                           _nAttrs; // Number of attributes in array
    double                      _probability; // Marginal inclusion probability
    int32_t                            _seed; // Initial Seed value for RNG
};

/**
 *  @see PhysicalOperator
 */
class PhysicalBernoulli: public PhysicalOperator
{
  private:

    //
    //   Calculate a seed value. If one is provided, then use it. Otherwise, use the
    //  C++ language standard machinery to generate one.
    //
    //   Note that we want to produce a different seed for each instance. The
    //  use of the std::random_device guarantees that for the non-specified
    //  seed.
    int32_t calcSeed( std::shared_ptr<Query> query ) const
    {
      int32_t res = 0;
      Parameter p = findKeyword("seed");
      if (p)
      {
        auto pexp = (std::shared_ptr<OperatorParamPhysicalExpression>&) p;
        res = pexp->getExpression()->evaluate().getInt32();
        res ^= (int32_t)query->getInstanceID();
      }
      else if ( (_parameters.size() >= 2) )
      {
        auto pexp = (std::shared_ptr<OperatorParamPhysicalExpression>&) _parameters[1];
        res = pexp->getExpression()->evaluate().getInt32();
        res ^= (int32_t)query->getInstanceID();
      } else {
        std::random_device r;
        res = abs((int32_t)r());
      }
      return res;
    }

  public:
    PhysicalBernoulli( const string& logicalName,    const string& physicalName,
                       const Parameters& parameters, const ArrayDesc& schema ) :
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    //
    //  Bernoulli is a pipelined operator, hence it executes by returning an
    // iterator-based array to the consumer that overrides the chunkiterator method.
    std::shared_ptr<Array> execute( vector< std::shared_ptr<Array> >& inputArrays,
                                    std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);
        SCIDB_ASSERT(_parameters.size() <= 2);

        checkOrUpdateIntervals( _schema, inputArrays[0]);
        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);

        double  probability = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                               _parameters[0])->getExpression()->evaluate().getDouble();
        int32_t seed = calcSeed(query);

        if (seed < 0)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR1);

        if (probability <= 0 || probability > 1)
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_SAMPLE_ERROR2);

        return std::shared_ptr<Array>(new BernoulliArray(getSchema(), inputArray, query, probability, seed));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalBernoulli, "bernoulli", "physicalBernoulli")

}  // namespace scidb
