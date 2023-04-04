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

/// @file OperatorDist.cpp

#include <query/OperatorDist.h>

#include <array/ArrayDesc.h>
#include <array/ArrayDistributionInterface.h>
#include <system/Utils.h>

#include <log4cxx/logger.h>

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("OperatorDist"));

OperatorDist::OperatorDist()
:
    _inheritedDistType(dtUninitialized),
    _synthesizedDistType(dtUninitialized)
{
}

void OperatorDist::setInheritedDistType(DistType type)
{
    _inheritedDistType = type;

    LOG4CXX_TRACE(logger, "OperatorDist::setInheritedDistType: operator " << getOperatorName()
                           << " _inheritedDistType " << distTypeToStr(_inheritedDistType));
}

void OperatorDist::setSynthesizedDistType(DistType type)
{
    LOG4CXX_TRACE(logger, "OperatorDist::setSynthesizedDistType("<<distTypeToStr(type)
                           << "), operator " << getOperatorName());
    _synthesizedDistType = type;
}

DistType OperatorDist::getInheritedDistType() const
{
    return _inheritedDistType;
}

DistType OperatorDist::getSynthesizedDistType() const
{
    LOG4CXX_TRACE(logger, "OperatorDist::getSynthesizedDistType: operator " << getOperatorName()
                           << " _synthesizedDistType " << distTypeToStr(_synthesizedDistType));
    return _synthesizedDistType;
}


//
// inheritance
//
std::vector<DistType> OperatorDist::inferChildInheritances(DistType inherited, size_t numChildren) const
{
    LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: operator " << getOperatorName()
                          << " inherited " << distTypeToStr(inherited)
                          << " numChildren " << numChildren);
    std::vector<DistType> result(numChildren);
    if(numChildren == 0) {
        LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: no children, returning");
        return result; // this early return is to silence immaterial TRACE messages below
    }

    DistType parameterlessDist = inherited;
    if (!isParameterless(parameterlessDist)) {
        // optimizer will not be able to insert conversions if required
        parameterlessDist = defaultDistType();
        LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: operator " << getOperatorName()
                              << " parameterized dist, changed to defaultDistType "
                              << distTypeToStr(parameterlessDist));
    }
    SCIDB_ASSERT(isParameterless(parameterlessDist));


    // default version passes through the input inheritance.
    // operators like store() need to override this
    // by default we do not pass through dtReplication, as replication should only happen the point where
    // requested and not transitively into the entire expression sub-tree.
    // Otherwise, entire subtrees of the query will become replicated rather than partitioned.
    // When that happens, any SG that must be inserted into the tree will encounter a duplicate
    // and throw an error.  That error shows that the duplication was pointless / costly.
    // so where the input is psReplicated, we use "rootDefaultPartitioning" instead
    // hoping that it will be the most "fair" choice
    // NOTE: doing this will require that we override this method for SG to allow it
    //       if it matches its creation arguments

    for(size_t i=0; i < result.size(); i++) {

        DistType replicationRestrictedDist = parameterlessDist;
        // careful, not the same as !isPartition()
        if (isReplicated(replicationRestrictedDist) && !isReplicatedInputOk(numChildren)[i]) {
            replicationRestrictedDist = defaultDistType(); // never replicated
            LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: operator " << getOperatorName() << " "
                                  << distTypeToStr(parameterlessDist) << " not permitted on input " << i
                                  << " changed to defaultDistType() " << distTypeToStr(replicationRestrictedDist));
        }

        // check: not replication except when repl allowed
        SCIDB_ASSERT(not isReplicated(replicationRestrictedDist) || isReplicatedInputOk(numChildren)[i]);

        result[i] = replicationRestrictedDist;
        LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: operator " << getOperatorName()
                              << " result[" << i<< "] = " << distTypeToStr(result[i]));
    }
    LOG4CXX_TRACE(logger, "OperatorDist::inferChildInheritances: operator " << getOperatorName() << ", returning");
    return result;
}


//
// synthesis
//

/// two-arg standard method that
/// 1. will work for most leaf and unary operators
/// 2. uses the signature supported for overloading by operators that require different behavior
///
/// @note the most common override of this in PhysicalOperators is _schema.getDistribution.getDistType
///
DistType OperatorDist::inferSynthesizedDistType(std::vector<DistType> const& inputDistrib, size_t depth) const
{
    LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesizedDistType(depth="<<depth<<"):"
                          << " operator " << getOperatorName()
                          << " inherited ps" << distTypeToStr(getInheritedDistType()));

    DistType result;
    if (inputDistrib.empty()) {
        result = getInheritedDistType(); // leaf operators cater to their parents' influence
    } else {
        result = inputDistrib[INPUT_INDEX_LEFT];   // the typical answer
        // non-leaf operators try to accept what they are given
        // TODO: if OperatorDist is merged into PhysicalOperator, then
        //  this method could access _schema here.  Using the DistType in _schema is
        //  currently required in about 30 operators which must override this method,
        //  so a base case implementation that works for more derived operators is desirable
        // TODO: is this really the best answer if inputDistrib.size() > 1 ?
        //       Consider alternate rules such as if they all match, return that, otherwise exception
        // TODO: write up a rationale for the rules used by the base class implementations

        // ensure replicated is not synthesized when not accepted on the leftmost input
        if (isReplicated(result)) {  // not the same as !isPartition()
            // must check whether we can handle replication
            bool isReplicatedOk = isReplicatedInputOk(inputDistrib.size())[INPUT_INDEX_LEFT];
            if (not isReplicatedOk) {
                LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesized dist type: operator " << getOperatorName()
                                      << " " << distTypeToStr(result) << " not acceptable ");
                result = defaultDistType(); // never replicated
                LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesized dist type: operator " << getOperatorName()
                                      << " changed to defaultDistType() " << distTypeToStr(result));
            }
        }
        // check: not replication except when allowed as LHS input
        SCIDB_ASSERT(not isReplicated(result) || isReplicatedInputOk(inputDistrib.size())[INPUT_INDEX_LEFT]);
    }

    LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesizedDistType(depth="<<depth<<"):"
                          << " returns " << distTypeToStr(result));
    return result;
}

// three-arg "wrapping" method that is called by the framework
// 1. so adds sanity testing to any 2-arg implementation
// 2. provides an extended signature with the inSchema argument.
// 3. is to be overridden by PhysicalRedistribute and PhysicalRepart only, which is the only
//    motiviation for providing the signature that we would rather not be supporting.
// TODO: consider hiding this by factoring to a "PhysicalSpecialSystemOperator" base class so it is easy
//    to detect which operators use this interface, which can be made inaccessible in
//    PhysicalOperator, which normal operators would be based on.

DistType OperatorDist::inferSynthesizedDistType(std::vector<DistType> const& inDist,
                                                std::vector<ArrayDesc> const& inSchema, size_t depth) const
{
    // TODO(JHM): these preliminatry checks are to be re-factored after all the changes required
    //            to support them have already been checked in.
    ASSERT_EXCEPTION(inDist.size() == inSchema.size(),"inDist and inSchema size mismatch");
    LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesizedDistType(3arg,depth="<<depth<<"):"
                          << " operator " << getOperatorName());

    for(size_t i=0; i<inDist.size(); i++) {
        if(inDist[i] != inSchema[i].getDistribution()->getDistType()) {
            LOG4CXX_TRACE(logger, "inferSynthesizedDistType(3arg) "
                                  << " following 3 traces normal during PhysicalScan failover of psReplicated");
            LOG4CXX_TRACE(logger, "inferSynthesizedDistType(3arg) mismatch operator " << getOperatorName());
            LOG4CXX_TRACE(logger, "inferSynthesizedDistType(3arg)" << " inSchema["<<i<<"]"
                                  << " dt " << distTypeToStr(inSchema[i].getDistribution()->getDistType()));
            LOG4CXX_TRACE(logger, "inferSynthesizedDistType(3arg) inDist["<<i<<"]"
                                  << " dt " << distTypeToStr(inDist[i]));
        }
        // NOTE: we might think _schema (inSchema)'s DistType would agree with inDist
        //       but that would be incorrect.
        //       During failover, PhysicalScan changes the latter to to dtUndefined
        ASSERT_EXCEPTION(inSchema[i].getDistribution()->getDistType() == inDist[i] ||
                         isUndefined(inDist[i]), "inDist and inSchema have inconsistent DistType");
    }

    LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesizedDistType(3arg,depth="<<depth<<"): delegating to 2arg");
    auto result = inferSynthesizedDistType(inDist, depth);   // the common implementation with only 2 args provided

    if(isReplicated(result)) {   // not the same as !isPartition()
        // check that isReplicatedInputOk() was honored by the delegation to the 2 arg
        // (e.g. when the 2 arg is not the base version, this should still be honored)
        bool isReplicatedOk = isReplicatedInputOk(inDist.size())[INPUT_INDEX_LEFT];
        if (not isReplicatedOk) {
            // TODO: would like this to be an ASSERT_EXCEPTION,
            // but can't tighten up the code that tight at this point or some operators will trigger it.
            // But originally it was at WARN level to make it clear its not good that this is happening.
            // However this is cluttering logs in the field (SDB-6868) so it must be demoted to DEBUG.
            // And to avoid letting this demotion make us lose track of this incorrect thing remaining,
            // added SDB-6869 to eliminate the remaining cases and convert this to ASSERT_EXEPTION.
            LOG4CXX_DEBUG(logger, "OperatorDist::inferSynthesizedDistType(3arg):"
                                  " inferSynthesizedDistType replicated, but isReplicatedInputOk() was false,"
                                 " operator: " << getOperatorName());
        }
    }

    LOG4CXX_TRACE(logger, "OperatorDist::inferSynthesizedDistType(3arg,depth="<<depth<<"):"
                          << " returning " << distTypeToStr(result));
    return result;
}

void OperatorDist::checkInputDistAgreement(std::vector<DistType> const& inputDistrib, size_t depth) const
{
    // check that the first matches the rest.
    for(size_t i=1; i<inputDistrib.size(); i++) {
        // the following is not an error UNLESS we are post-converter insertion.
        // otherwise they will be different sometimes because they have not been "fixed" yet
        // the method must still choose what the output should be, though.
        // for now we will capture that issue with an argument
        if(inputDistrib[INPUT_INDEX_LEFT] != inputDistrib[i]) {
            LOG4CXX_WARN(logger, "OperatorDist::checkInputDistAgreement("<<depth<<")"
                                 << " operator " << getOperatorName());
            LOG4CXX_WARN(logger, "OperatorDist::checkInputDistAgreement("<<depth<<") type, in[LEFT] "
                                  << distTypeToStr(inputDistrib[INPUT_INDEX_LEFT])
                                  << " != in[" << i << "] " << distTypeToStr(inputDistrib[i]));
        }
        ASSERT_EXCEPTION(inputDistrib[INPUT_INDEX_LEFT] == inputDistrib[i],"");
    }
}

std::vector<uint8_t> OperatorDist::isReplicatedInputOk(size_t numChildren) const
{
    if (numChildren == 0) {
        // SDB-1929.  Calling code assumes non-empty even if numChildren == 0.
        // XXX Consider fixing calling code to handle empty vectors?
        numChildren = 1;
    }
    return std::vector<uint8_t>(numChildren, false) ;
}

} //namespace
