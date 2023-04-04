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

/// @file PhysicalQueryPlanNode.cpp
// taken from QueryPlan.cpp


#include <query/PhysicalQueryPlan.h>

#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <util/Indent.h>

#include <log4cxx/logger.h>

#include <list>
#include <memory>

using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("PhysicalNode"));

PhysicalQueryPlanNode::PhysicalQueryPlanNode()
{
     LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(), no-arg ctor!!!");
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
                                             bool ddl, bool tile)
: _physicalOperator(physicalOperator),
  _parent(), _ddl(ddl), _tile(tile), _distribution()
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(3arg) opName: " << physicalOperator->getPhysicalName());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(3arg) ps: " << physicalOperator->getSchema());
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
		const std::vector<std::shared_ptr<PhysicalQueryPlanNode> > &childNodes,
                                             bool ddl, bool tile):
	_physicalOperator(physicalOperator),
	_childNodes(childNodes),
    _parent(), _ddl(ddl), _tile(tile), _distribution()
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(4arg) opName: " << physicalOperator->getPhysicalName());
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::PhysicalQueryPlanNode(4arg) ps: " << physicalOperator->getSchema());
}

PhysicalQueryPlanNode::~PhysicalQueryPlanNode()
{}

void PhysicalQueryPlanNode::addChild(const std::shared_ptr<PhysicalQueryPlanNode>& child)
{
    child->_parent = shared_from_this();
    _childNodes.push_back(child);
}

void PhysicalQueryPlanNode::removeChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild)
{
    std::vector<std::shared_ptr<PhysicalQueryPlanNode>> newChildren;
    for(size_t i = 0; i < _childNodes.size(); i++)
    {
        if (_childNodes[i] != targetChild)
        {
            newChildren.push_back(_childNodes[i]);
        }
        else
        {
            targetChild->_parent.reset();
        }
    }
    assert(_childNodes.size() > newChildren.size());
    _childNodes = newChildren;
}

void PhysicalQueryPlanNode::replaceChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild,
                  const std::shared_ptr<PhysicalQueryPlanNode>& newChild)
{
    bool removed = false;
    std::vector<std::shared_ptr<PhysicalQueryPlanNode>> newChildren;
    for(size_t i = 0; i < _childNodes.size(); i++)
    {
        if (_childNodes[i] != targetChild)
        {
            newChildren.push_back(_childNodes[i]);
        }
        else
        {
            newChild->_parent = shared_from_this();
            newChildren.push_back(newChild);
            removed = true;
        }
    }
    _childNodes = newChildren;
    SCIDB_ASSERT(removed==1);
}

std::vector<ArrayDesc> PhysicalQueryPlanNode::getChildSchemas() const
{
    std::vector<ArrayDesc> result;
    for (size_t i = 0, count = _childNodes.size(); i < count; ++i)
    {
        std::shared_ptr<PhysicalQueryPlanNode> const& child = _childNodes[i];
        result.push_back(child->getPhysicalOperator()->getSchema());
    }
    return result;
}

bool PhysicalQueryPlanNode::subTreeOutputFullChunks() const
{
    if (isSgNode())
    {
        return true;
    }
    for (size_t i = 0, count = _childNodes.size(); i< count; ++i)
    {
        if (!_childNodes[i]->subTreeOutputFullChunks())
        {
            return false;
        }
    }
    return _physicalOperator->outputFullChunks(getChildSchemas());
}

const PhysicalBoundaries& PhysicalQueryPlanNode::inferBoundaries()
{
    std::vector<PhysicalBoundaries> childBoundaries;
    for (size_t i =0; i<_childNodes.size(); i++)
    {
        childBoundaries.push_back(_childNodes[i]->getBoundaries());
    }
    _boundaries = _physicalOperator->getOutputBoundaries(childBoundaries, getChildSchemas());
    return _boundaries;
}

void PhysicalQueryPlanNode::setQuery(const std::shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(logger, "PhysicalQueryPlanNode::setQuery()  opName: " << _physicalOperator->getPhysicalName());
    for (size_t i =0; i<_childNodes.size(); i++)
    {
        _childNodes[i]->setQuery(query);
    }
    _physicalOperator->setQuery(query);
}

//
// new distribution inference
//

void PhysicalQueryPlanNode::visitDepthFirstPostOrder(PhysicalQueryPlanNode::Visitor& v, size_t depth)
{
    for (auto& child : _childNodes) {
        child->visitDepthFirstPostOrder(v, depth+1);
    }
    v(*this, nullptr, depth);
}

void PhysicalQueryPlanNode::visitDepthFirstPreOrder(PhysicalQueryPlanNode::Visitor& v, size_t depth)
{
    v(*this, nullptr, depth);
    for (auto& child : _childNodes) {
        child->visitDepthFirstPreOrder(v, depth+1);
    }
}


// indexes directly using decPath instead of child iteration
void PhysicalQueryPlanNode::visitPathPreOrder(PhysicalQueryPlanNode::Visitor& v, const PhysicalQueryPlanPath* descPath, size_t depth)
{
    v(*this, descPath, depth);

    auto child = descPath->front();  // the child on the path
    auto childOp = child->getPhysicalOperator();

    if(descPath->size() > 1) {   // need to recurse on elements after the first one
        std::list<std::shared_ptr<PhysicalQueryPlanNode>> recursePath(++(descPath->begin()),descPath->end());
        LOG4CXX_TRACE(logger, "PQPN::visitPathPreOrder("<<depth<<"): recursing path.size " << recursePath.size());
        child->visitPathPreOrder(v, &recursePath, depth+1);
    }
}

void PhysicalQueryPlanNode::visitPathPostOrder(PhysicalQueryPlanNode::Visitor& v, const PhysicalQueryPlanPath* descPath, size_t depth)
{
    auto child = descPath->front(); // the child on the path 
    auto childOp = child->getPhysicalOperator();

    if(descPath->size() > 1) {   // need to recurse on elements after the first one
        std::list<std::shared_ptr<PhysicalQueryPlanNode>> recursePath(++(descPath->begin()),descPath->end());
        LOG4CXX_TRACE(logger, "PQPN::visitPathPostOrder("<<depth<<"): recursing path.size " << recursePath.size());
        child->visitPathPostOrder(v, &recursePath, depth+1);
    }

    v(*this, descPath, depth);
}

struct SetChildInheritancesVisitor : PhysicalQueryPlanNode::Visitor
{
    void operator()(PhysicalQueryPlanNode& node, const PhysicalQueryPlanPath* descPath, size_t depth)
    {
        SCIDB_ASSERT(!descPath);

        auto op = node.getPhysicalOperator();
        SCIDB_ASSERT(op);

        auto myInheritedDistType = op->getInheritedDistType();
        LOG4CXX_TRACE(logger, "PQPN::setAllChildInheritances("<<depth<<"): begin: " << op->getPhysicalName()
                              << " getInheritedDistType " << myInheritedDistType);
        SCIDB_ASSERT(not isUninitialized(myInheritedDistType));

        //
        // determine my childrens' inheritances
        //
        auto childInheritance = op->inferChildInheritances(myInheritedDistType, node.getChildren().size());
        for (size_t i = 0; i < node.getChildren().size(); i++) {
            auto child = node.getChildren()[i];
            auto childOp = child->getPhysicalOperator();
            LOG4CXX_TRACE(logger, "PQPN::setAllChildInheritances("<<depth<<"):"
                                  << " child["<<i<<"] op =" << childOp->getPhysicalName()
                                  << " child inheritance will be: " << childInheritance[i]);

            // change state of child
            childOp->setInheritedDistType(childInheritance[i]);
        }
    }
};

struct SetPathChildInheritanceVisitor : PhysicalQueryPlanNode::Visitor
{
    void operator()(PhysicalQueryPlanNode& node, const PhysicalQueryPlanPath* descPath, size_t depth)
    {
        auto op = node.getPhysicalOperator();
        SCIDB_ASSERT(op);

        auto myInheritedDistType = op->getInheritedDistType();
        LOG4CXX_TRACE(logger, "PQPN::setAllChildInheritances("<<depth<<"): begin: " << op->getPhysicalName()
                              << " getInheritedDistType " << myInheritedDistType);
        SCIDB_ASSERT(not isUninitialized(myInheritedDistType));

        //
        // determine my childrens' inheritances
        //
        std::vector<DistType> childInheritance = op->inferChildInheritances(myInheritedDistType, node.getChildren().size());
        for (size_t i = 0; i < node.getChildren().size(); i++) {
            auto child = node.getChildren()[i];
            auto childOp = child->getPhysicalOperator();
            // TODO: index directly using decPathFront instead of iteration
            if(child == descPath->front()) { // this child is on the path 
                LOG4CXX_TRACE(logger, "PQPN::setAllChildInheritances("<<depth<<"):"
                                      << " child["<<i<<"] op =" << childOp->getPhysicalName()
                                      << " child inheritance will be: " << childInheritance[i]);

                // change state of child
                childOp->setInheritedDistType(childInheritance[i]);
            }
        }
    }
};

struct SetDistVisitor : PhysicalQueryPlanNode::Visitor
{
public:
    void operator()(PhysicalQueryPlanNode& node, const PhysicalQueryPlanPath* descPath, size_t depth)
    {
        auto op = node.getPhysicalOperator();
        SCIDB_ASSERT(op);
        LOG4CXX_TRACE(logger, "setDistribution::operator()  opName: " << op->getPhysicalName() << " #############");

        auto childNodes = node.getChildren();

        // get all the children's DistTypes
        std::vector<DistType> childOutputTypes;
        for (size_t i = 0; i < childNodes.size(); i++) {
            auto child = childNodes[i];
            auto childOp = child->getPhysicalOperator();

            // get the existing or just-resynthesized type
            childOutputTypes.push_back(childOp->getSynthesizedDistType());
            LOG4CXX_TRACE(logger, "SetDistVisitor::operator("<<depth<<"):"
                                  << " childOp["<<i<<"]=" << childOp->getPhysicalName() << " synthesized " << childOutputTypes[i]);
        }

        // synthesize node's output DistType from the childrens' DistType
        auto myType = op->inferSynthesizedDistType(childOutputTypes, node.getChildSchemas(), depth);
        LOG4CXX_TRACE(logger, "SetDistVisitor::operator("<<depth<<"): synthesized output type: >>> " << myType << " <<<");
        SCIDB_ASSERT(not isUninitialized(myType));
        op->setSynthesizedDistType(myType);

        // synthesis of node's output Distribution, step 1
        // get the already-synthesized output of the children.
        std::vector<RedistributeContext> childRedistContexts;
        for (size_t i=0; i<childNodes.size(); i++) {
            auto child = childNodes[i];

            LOG4CXX_TRACE(logger, "setDistVisitor::operator("<<depth<<"),  calling child["<<i<<"]->getDistribution().getArrayDistribution() ");
            auto distrib = child->getDistribution().getArrayDistribution();
            SCIDB_ASSERT(distrib);
            LOG4CXX_TRACE(logger, "setDistVisitor::operator("<<depth<<"),  child["<<i<<"]->getDistribution().getArrayDistribution " << *distrib);

            childRedistContexts.push_back(child->getDistribution());
            LOG4CXX_TRACE(logger, "setDistVisitor::operator("<<depth<<") childRedistContexts["<<i<<"] = " <<  childRedistContexts.back());
        }

        // synthesis of node's output Distribution, step 2
        // determine our own output distribution
        LOG4CXX_TRACE(logger, "setDistVisitor::operator("<<depth<<"), calling getOutputDistribution() opName: " << op->getPhysicalName());
        auto myDistribution = op->getOutputDistribution(childRedistContexts, node.getChildSchemas());
        node.setDistribution(myDistribution);
        LOG4CXX_TRACE(logger, "setDistVisitor::operator("<<depth<<") distribution set to " << node.getDistribution());

        //
        // TODO: eliminate the need for separate synthesis of DistType from OutputDistribution.
        //       step 1: have the code skip the DistType synthesis, but call
        //       op->setSynthesizedDistType(node.getDistribution().getDistType()),
        //       and attmpet to resolve issues resulting from that.  If successful,
        //       inferSynthesizedDistType will not have to be called here,
        //       and can then be eliminated from PhysicalOperator
        //       step 2: determine if setSynthesiziedDistType() will be needed or whether
        //       getSynthesizedDistType() can be implemented as
        //       getDistribution().getDistType()

        // This the following check duplicates code in CheckDistAgreementVisitor
        // but checking here finds problems closer to their source, in practice.
        // TODO: resolve this, e.g. by factoring the check and calling twice.
        if (node.getDistribution().getDistType() != myType) {
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"): " << op->getPhysicalName()
                                  << " mismatch of synthesized and _distribution");
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"):"
                                  << " inferSynthesizedType " << distTypeToStr(myType));
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"):"
                                  << " distribution.ps: " << distTypeToStr(node.getDistribution().getDistType()));
            ASSERT_EXCEPTION(node.getDistribution().getDistType() == myType,"");
        }
    }
};



// TBD: add Pass suffix to name
void PhysicalQueryPlanNode::inferDistType(const std::shared_ptr<Query>& query, size_t depth,
                                          const PhysicalQueryPlanPath* descPath)
{
    // inheritance pass
    if (descPath) {
       // path version keeps some algorithms from becoming O(n^2)
        SetPathChildInheritanceVisitor setPathChildInheritanceVisitor;
        visitPathPreOrder(setPathChildInheritanceVisitor, descPath, depth);
    } else {
        SetChildInheritancesVisitor setChildInheritancesVisitor;
        visitDepthFirstPreOrder(setChildInheritancesVisitor, depth);
    }

    // synthesis pass
    if (descPath) {
        SetDistVisitor setDistVisitor;
        visitPathPostOrder(setDistVisitor, descPath, depth);
    } else {
        SetDistVisitor setDistVisitor;
        visitDepthFirstPostOrder(setDistVisitor, depth);
    }
}

//
// refactored to use the vistor model
//

struct CheckDistAgreementVisitor : PhysicalQueryPlanNode::Visitor
{
    void operator()(PhysicalQueryPlanNode& node, const PhysicalQueryPlanPath* descPath, size_t depth)
    {
        SCIDB_ASSERT(!descPath);  // not applicable to this Visitor
        auto op = node.getPhysicalOperator();
        SCIDB_ASSERT(op);
        auto myInheritedDistType = op->getInheritedDistType();
        LOG4CXX_TRACE(logger, "CheckDistAgreementVisitor::operator("<<depth<<") opName: " << op->getPhysicalName()
                               << " getInheritedDistType " << myInheritedDistType);
        // NOTE: use ASSERT_EXCEPTION for any behavior a user-supplied
        // plugin could affect, so they will get checked in the running product
        ASSERT_EXCEPTION(not isUninitialized(myInheritedDistType), "operator fails checkDistAgreement 1");

        // determine my childrens' inheritances
        auto childNodes = node.getChildren();
        std::vector<DistType> childInheritance = op->inferChildInheritances(myInheritedDistType, childNodes.size());


        // obtain my children's synthesized DistTypes (as did setDistVisitor)
        std::vector<DistType> childOutDistTypes;
        for (auto& child : childNodes) {
            auto childOp = child->getPhysicalOperator();
            childOutDistTypes.push_back(childOp->getSynthesizedDistType());      // synthesized ... not actuals
        }
        if(logger->isTraceEnabled()) {
            for (size_t i = 0; i < childNodes.size(); i++) {
                LOG4CXX_TRACE(logger, "PQPN::checkDistAgreement("<<depth<<"), childOutDistTypes["<<i<<"] = " << childOutDistTypes[i]);
            }
        }
        LOG4CXX_TRACE(logger, "CheckDistAgreementVisitor::operator("<<depth<<"): children done @ depth " << depth
                              << " operator " << op->getPhysicalName());

        auto myType = op->inferSynthesizedDistType(childOutDistTypes, node.getChildSchemas(), depth);

        LOG4CXX_TRACE(logger, "PQPN::checkDistAgreement("<<depth<<"): operator " << op->getPhysicalName()
                                   << " op->inferSynthesized() " << myType
                                   << " op->getSynthesizedDistType() " << op->getSynthesizedDistType());

        ASSERT_EXCEPTION(not isUninitialized(myType), "operator fails checkDistAgreement 2");

        ASSERT_EXCEPTION(op->getSynthesizedDistType() == myType, "operator fails checkDistAgreement 3");

        if (node.getDistribution().getDistType() != myType) {
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"): " << op->getPhysicalName() << " mismatch of synthesized and _distribution");
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"): inferSynthesizedType " << myType);
            LOG4CXX_ERROR(logger, "setDistVisitor::operator("<<depth<<"): distribution.ps: " << node.getDistribution().getDistType());
            ASSERT_EXCEPTION(node.getDistribution().getDistType() == myType,"operator fails checkDistAgreement 4");
        }
        op->checkInputDistAgreement(childOutDistTypes, depth);  // overrideable by op, default is colocation
    }
};

void PhysicalQueryPlanNode::checkDistAgreement(const std::shared_ptr<Query>& query, size_t depth)
{
    CheckDistAgreementVisitor checkDistAgreementVisitor;
    visitDepthFirstPostOrder(checkDistAgreementVisitor, depth);
}

// NOTE: could have a form where childNode is given instead of childNode's index in parent
//       applyInheritanceToChild(higher, sgNode)
void PhysicalQueryPlanNode::applyInheritanceToChild(size_t childIdx)
{
    auto parentOp = getPhysicalOperator();
    SCIDB_ASSERT(parentOp);

    auto inheritedDistType = parentOp->getInheritedDistType();
    auto children = getChildren();
    auto childInheritances = parentOp->inferChildInheritances(inheritedDistType, children.size());

    SCIDB_ASSERT(childIdx < children.size());
    children[childIdx]->getPhysicalOperator()->setInheritedDistType(childInheritances[childIdx]);
}

// TODO: haven't guaranteed that setDistribution() was already properly called on the node.
//    This will be addressed after the plan generation uses more distinct passes,
//    as it will be less expensive to do so then. See NOTE4 in SDB-5900
// TODO: potential bug?
//    Doxygen in the header states that changing coordinates as done by e.g.
//    redimension causes this to return false, but this does not seem to be the case
//    according to the current impelmentation, and it is not clear why changing
//    of the coordinates themselves should matter at all.
//    It would matter to commuting e.g. filter(), but that is not what this method concerns
bool PhysicalQueryPlanNode::converterCommutesBefore(DistType dstDistType) const
{
    if (_childNodes.size() != 1) return false;  // unary operators only

    // 1. Currently an SG will not commute across a change in distribution or residency.
    //    However we might decide to do that.  Where an operator's residency is small due to a scan from a small
    //    residency, and feeds a store into a larger residency, pushing the sg down may be advantagous.
    //
    //    So while this currenly does not commute if the residency changes between input and output,
    //    the following is subject to revision.
    bool isSame = getDistribution().sameDistribAndResidency(_childNodes[INPUT_INDEX_LEFT]->getDistribution());

    // 2. Does the operator accept dstDistType for input?
    //    (NOTE: this has never been checked.  TODO: check for it or prove it always accepts.)

    // 3. Specifically check replicated input (which many operators do not accept)
    //    if this converter outputs replicated, can't commute before
    //    an operator that does not accept replicated input
    auto op = getPhysicalOperator();
    bool isReplicatedInputOk = op->isReplicatedInputOk(_childNodes.size())[INPUT_INDEX_LEFT];
    bool isReplicatedProhibitedCase = (isReplicated(dstDistType) && not isReplicatedInputOk);

    bool result = isSame && not isReplicatedProhibitedCase;
    return result;
}

void PhysicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);

    out<<"[pNode] "<<_physicalOperator->getPhysicalName()<<" ID "<< _physicalOperator->getOperatorID().getValue()
       <<" ddl "<<isDdl()<<" tile "<<supportsTileMode()<<" children "<<_childNodes.size()<<"\n";
    _physicalOperator->toString(out,indent+1);

    if (children) {
        out << prefix(' ');
        out << "output full chunks: ";
        out << (outputFullChunks() ? "yes" : "no");
        out << "\n";
        out << prefix(' ');
        out << "\n";
    }

    out << prefix(' ');
    out<<"diout "<<_distribution<<"\n";
    const ArrayDesc& schema = _physicalOperator->getSchema();
    out << prefix(' ');
    out<<"bound "<<_boundaries
      <<" cells "<<_boundaries.getNumCells();

    if (_boundaries.getStartCoords().size() == schema.getDimensions().size()) {
        out  << " chunks ";
        try {
            uint64_t n = _boundaries.getNumChunks(schema.getDimensions());
            out << n;
        } catch (PhysicalBoundaries::UnknownChunkIntervalException&) {
            out << '?';
        }
        out << " est_bytes " << _boundaries.getSizeEstimateBytes(schema)
            << '\n';
    }
    else {
        out <<" [improperly initialized]\n";
    }

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++) {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

string PhysicalQueryPlanNode::getSgArrayName(const Parameters& sgParameters)
{
    std::string arrayName;
    if (sgParameters.size() >= 3) {
        arrayName = static_cast<OperatorParamReference*>(sgParameters[2].get())->getObjectName();
    }
    return arrayName;
}

size_t PhysicalQueryPlanNode::findChild(const std::shared_ptr<PhysicalQueryPlanNode> & targetChild)
{
    for(size_t i = 0; i < _childNodes.size(); i++) {
        if (_childNodes[i] == targetChild) {
            return i;
        }
    }
    ASSERT_EXCEPTION_FALSE("targetChild is not a child of the node");
}

void
PhysicalQueryPlanNode::supplantChild(const shared_ptr<PhysicalQueryPlanNode>& targetChild,
                                     const shared_ptr<PhysicalQueryPlanNode>& newChild)
{
    assert(newChild);
    assert(targetChild);
    assert(newChild.get() != this);
    int removed = 0;
    vector<shared_ptr<PhysicalQueryPlanNode>> newChildren;

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        os << "Supplanting targetChild Node:\n";
        targetChild->toString(os, 0 /*indent*/,false /*children*/);
        os << "\nwith\n";
        newChild->toString(os, 0 /*indent*/,false /*children*/);
        LOG4CXX_TRACE(logger, os.str());
    }

    for(auto &child : _childNodes) {
        if (child != targetChild) {
            newChildren.push_back(child);
        }
        else {
            // Set the parent of the newChild to this node.
            newChild->_parent = shared_from_this();

            // NOTE: Any existing children of the newChild are removed from the
            // Query Plan.
            if ((newChild->_childNodes).size() > 0) {
                LOG4CXX_INFO(logger,
                             "Child nodes of supplanting node are being removed from the tree.");
            }

            // Re-parent the children of the targetChild to the newChild
            newChild->_childNodes.swap(targetChild->_childNodes);
            for (auto grandchild : newChild -> _childNodes) {
                assert(grandchild != newChild);
                grandchild->_parent = newChild;
            }

            // Remove any references to the children from the targetChild
            targetChild->_childNodes.clear();
            targetChild->resetParent();

            // Add the newChild to this node
            newChildren.push_back(newChild);
            ++removed;
        }
    }
    _childNodes.swap(newChildren);

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        newChild->toString(os);
        LOG4CXX_TRACE(logger, "New Node subplan:\n"
                      << os.str());
    }

    SCIDB_ASSERT(removed==1);
}


size_t PhysicalQueryPlanNode::estimatedMaxDataVolume(size_t maxStringLength) const
{
    // estimate meaning it does not have to be perfect, it need
    // only be good enough for estimating the cost/benefit of inserting
    // _SG()'s when not required for correctness

    const auto& schema = _physicalOperator->getSchema();

    size_t maxBytesPerCell = 0;
    for (const auto& attr : schema.getAttributes()) {
        auto bytes = attr.getSize();
        if (bytes == 0) {             // variable length
            bytes = maxStringLength;  // caller determines estimate
        }
        maxBytesPerCell += bytes;
    }

    LOG4CXX_TRACE(logger, "estimatedMaxDataVolume: maxBytesPerCell " << maxBytesPerCell);
    LOG4CXX_TRACE(logger, "estimatedMaxDataVolume: numCells " << _boundaries.getNumCells());
    size_t maxBytes = maxBytesPerCell * _boundaries.getNumCells() ;

    return maxBytes;
}

} // namespace
