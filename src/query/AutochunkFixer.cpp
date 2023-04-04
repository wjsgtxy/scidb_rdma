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
 *  @file AutochunkFixer.cpp
 *  @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <query/AutochunkFixer.h>

#include <array/Array.h>
#include <system/Exceptions.h>

#include <boost/lexical_cast.hpp>
#include <sstream>

namespace scidb {

using namespace std;

// Take all chunk interval derivations from a given schema.
void AutochunkFixer::takeAllDimensions(Dimensions const& inputDims)
{
    clear();
    for (size_t i = 0, n = inputDims.size(); i < n; ++i) {
        if (inputDims[i].isAutochunked()) {
            takeDimension(i).fromArray(0).fromDimension(i);
        }
    }
}

// Return encoded derivations as a string.
string AutochunkFixer::str() const
{
    // No partial derivations please!
    assert(_state == OUTDIM);

    // Keep it simple until it needs to get complex (JSON?).  Just
    // space-delimited numbers.

    stringstream ss;
    for (auto const& d : _derivations) {
        ss << ' ' << d.outDim << ' ' << d.inArray << ' ' << d.inDim;
    }
    return ss.str();
}

// Set derivation vector from encoded string.
void AutochunkFixer::fromString(const string& s)
{
    // The inverse of str().  Could behave VERY badly if you pass in a
    // string that was *not* encoded by str(), but... we're keeping it simple.

    using boost::lexical_cast;
    using boost::bad_lexical_cast;

    clear();

    if (s.empty()) {
        // A no-op fixer is possible: the aggregate operators share a
        // single execute() method and have different fixer needs.
        return;
    }

    istringstream iss(s);
    string token;
    try {
        while (!iss.eof()) {
            iss >> token;
            takeDimension(lexical_cast<size_t>(token));
            iss >> token;
            fromArray(lexical_cast<size_t>(token));
            iss >> token;
            fromDimension(lexical_cast<size_t>(token));
        }
    }
    catch (bad_lexical_cast&) {
        stringstream ss;
        ss << "Internal error detected in " << __PRETTY_FUNCTION__
           << ": Bad lexical cast, token='" << token
           << "', inputString='" << s << '\'';
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNKNOWN_ERROR)
            << ss.str();
    }
}

// Fix the schema according to the derivations and the inputs.
void AutochunkFixer::fix(ArrayDesc& schema, vector<std::shared_ptr<Array>>& inputs) const
{
    // No partial derivations please!
    assert(_state == OUTDIM);

    Dimensions& outDims = schema.getDimensions();
    for (auto const& d : _derivations) {
        DimensionDesc const& inDim =
            inputs[d.inArray]->getArrayDesc().getDimensions()[d.inDim];
        if (outDims[d.outDim].isAutochunked()) {
            outDims[d.outDim].setChunkInterval(inDim.getChunkInterval());
        }
        else if (outDims[d.outDim].getChunkInterval() != inDim.getChunkInterval()) {
            // This is just extra paranoia.  For non-autochunked
            // dimensions, logical inferSchema() should have gotten it
            // right!  If not, someone (the optimizer?) messed up somewhere.
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                << outDims[d.outDim] << inDim;
        }
    }
}

} // namespace scidb
