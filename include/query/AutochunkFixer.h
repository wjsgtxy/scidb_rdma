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

#include <array/Dimensions.h>
#include <array/ArrayDesc.h>

/**
 *  @file AutochunkFixer.h
 *
 *  @author Mike Leibensperger <mjl@paradigm4.com>
 *
 *  @description
 *  When a query involves autochunked redimension() operators, the
 *  actual chunk sizes (aka chunk intervals) cannot be known until the
 *  physical operator's execute() method.  At that time, the physical
 *  operator's _schema MUST be repaired using these actual intervals.
 *  Otherwise, the operator could produce a result Array with an
 *  autochunked schema---and that is forbidden: it breaks the "all
 *  chunk intervals are known at execute() time" invariant for the
 *  parent operator(s).
 *
 *  For a given operator, the derivation of the output array's chunk
 *  intervals may be arbitrarily complex.  Duplicating that
 *  LogicalOperator logic in the PhysicalOperator is a code
 *  maintenance nightmare.  But by using this AutochunkFixer class
 *  appropriately, a logical operator can record the derivations of
 *  output intervals and transmit them to PhyiscalOperators on the
 *  workers.  The workers can then build their own AutochunkFixer from
 *  the derivations, and use it early in execute() to fix their chunk
 *  intervals.
 */
#ifndef AUTOCHUNK_FIXER_H
#define AUTOCHUNK_FIXER_H

#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace scidb {

class Array;
class ArrayDesc;

class AutochunkFixer
{
public:
    AutochunkFixer() : _state(OUTDIM) {}
    AutochunkFixer(const std::string& encodedDerivations)
        : _state(OUTDIM)
    {
        fromString(encodedDerivations);
    }

    /**
     * "Named parameter idiom" setters to load and record a derivation.
     *
     * @note These methods MUST be called in order, since
     * fromDimension() has the side effect of actually recording the
     * derivation.
     *
     * @{
     */
    AutochunkFixer& takeDimension(size_t d) {
        assert(_state == OUTDIM);
        _next.outDim = d;
        _state = INARRAY;
        return *this;
    }
    AutochunkFixer& fromArray(size_t a) {
        assert(_state == INARRAY);
        _next.inArray = a;
        _state = INDIM;
        return *this;
    }
    AutochunkFixer& fromDimension(size_t d) {
        assert(_state == INDIM);
        _next.inDim = d;
        _derivations.push_back(_next);
        _state = OUTDIM;
        return *this;
    }
    /**@}*/

    /**
     * Take all chunk interval derivations from a given schema.
     *
     * @description This is the moral equivalent of
     * PhysicalOperator::checkOrUpdateIntervals().  It's needed
     * because some physical operators (e.g. those in
     * src/query/ops/aggregate) share the same execute() method, but
     * have different logical inferSchema() methods.  Thus the shared
     * execute() code needs to perform different repairs depending on
     * what logicalInferSchema() did.  The checkOrUpdateIntervals()
     * method can't do that, but an AutochunkFixer can.
     *
     * @note This method assumes that @c inputSchema will correspond
     *       to inputArrays[0] when execute() is finally called.
     *
     * @{
     */
    void takeAllDimensions(ArrayDesc const& inputSchema)
        { takeAllDimensions(inputSchema.getDimensions()); }
    void takeAllDimensions(Dimensions const& inputDims);
    /**@}*/

    /// A clear() method is needed since inferSchema() is called several times.
    void clear() { _state = OUTDIM; _derivations.clear(); }

    /// Return encoded derivations as a string.
    std::string str() const;

    /// Set derivation vector from encoded string.
    void fromString(const std::string& s);

    /**
     *  Fix the schema according to the derivations and the inputs.
     *
     *  @param[in,out] schema schema whose chunk intervals are to be repaired
     *  @param[in]     inputs input arrays with concrete chunk intervals
     *  @throws SYSTEM_EXCEPTION if outDim is not autochunked, yet does not match inDim
     *
     *  @description Following the instructions in the derivations
     *  vector, make the schema's chunk intervals conform to those in
     *  the inputs vector.
     */
    void fix(ArrayDesc& schema, std::vector<std::shared_ptr<Array>>& inputs) const;

private:
    // "Output dimension outDim was derived from inArray's inDim dimension."
    struct Derivation {
        size_t  outDim;
        size_t  inArray;
        size_t  inDim;
    };
    std::vector<Derivation> _derivations;
    Derivation _next;

    // Bookkeeping to catch out-of-order calls to setters.
    enum State { OUTDIM, INARRAY, INDIM };
    State _state;
};

} // namespace scidb

#endif /* ! AUTOCHUNK_FIXER_H */
