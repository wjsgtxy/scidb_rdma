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

/// @file OperatorDist.h
#ifndef OPERATOR_DIST_H
#define OPERATOR_DIST_H

#include <array/ArrayDistributionInterface.h>

namespace scidb
{

class ArrayDesc;

/// @see https://paradigm4.atlassian.net/wiki/spaces/DEV/pages/531562523/Multiple+Distributions
class OperatorDist
{
public:
    OperatorDist();


    // "I need to produce 'inherited'.  To do that, what do I need my
    // children to produce?"  In particular, what should the child
    // produce that would cause me to do the least work?
                        // precedes synthesis step
                        // if done well, only system operators will need to override
    virtual std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren) const;

                        // follows inheritance step
                        // only Redimension and Repart should override this method
                        // others see the one-arg version, below
    virtual DistType    inferSynthesizedDistType(std::vector<DistType> const& inDist,
                                                 std::vector<ArrayDesc> const& inSchema, size_t depth) const;

                        // follows inheritance step
                        // this is the form that operators should override if they can't use the default
                        // version, which returns whatever the inheritance requests
    virtual DistType    inferSynthesizedDistType(std::vector<DistType> const& inDist, size_t depth) const;

                        /// @note: must be overridden for any n-ary operator that does not require colocation
                        ///       (e.g. cross_join),as the default is to check for colocation
    virtual void        checkInputDistAgreement(std::vector<DistType> const& inDist, size_t depth) const;

    void                setInheritedDistType(DistType type);    // for saving value of inferChildInheritances()
    virtual DistType    getInheritedDistType() const;           // temporarily virtual as a workaround
                                                                // TODO: eliminate the need for virtual

    void                setSynthesizedDistType(DistType type);  // for saving value of inferSynthesizedDistType()
    virtual DistType    getSynthesizedDistType() const;         // temporarily virtual as a workaround
                                                                // TODO: eliminate the need for virtual


    virtual const std::string&  getOperatorName() const = 0;

    /// Whether dtReplication input is acceptable.
    /// Operators that can process replicated input correctly must set this true.
    /// Examples: update,cross_join,spgemm,index_lookup,substitute.
    ///
    /// Operators that could produce non-identical output on different instances
    /// must set this false, otherwise data will be inconsistent.
    /// Example: apply (because of random(), instanceid(), or similar).
    ///
    /// For now we provide a default of false.  At some point, this default may be
    /// removed (and this method made pure virtual).
    ///
    /// @note MUST return a non-empty vector even if numChildren is zero.
    virtual std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const;

private:
    DistType            _inheritedDistType;
    DistType            _synthesizedDistType;
};

const size_t INPUT_INDEX_LEFT = 0;
const size_t INPUT_INDEX_RIGHT = 1;


} // namespace

#endif /* OPERATOR_DIST_H */
