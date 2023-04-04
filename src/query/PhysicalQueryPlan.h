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
 * PhysicalQueryPlan.h
 * taken from QueryPlan.h
 */

#ifndef PHYSICALQUERYPLAN_H_
#define PHYSICALQUERYPLAN_H_

#include <query/LogicalOperator.h>
#include <query/OperatorLibrary.h>
#include <util/SerializedPtrConverter.h>

#include <boost/archive/text_iarchive.hpp>

namespace scidb
{

typedef std::shared_ptr<PhysicalOperator>      PhysOpPtr;

/*
 *  Currently LogicalQueryPlanNode and PhysicalQueryPlanNode have similar structure.
 *  It may change in future as it needed
 */
class PhysicalQueryPlanNode;
typedef std::list<std::shared_ptr<PhysicalQueryPlanNode>> PhysicalQueryPlanPath;

class PhysicalQueryPlanNode : boost::noncopyable,
                              public std::enable_shared_from_this<PhysicalQueryPlanNode>
{
public:
    PhysicalQueryPlanNode();

    PhysicalQueryPlanNode(PhysOpPtr const& physicalOperator,
                          bool ddl, bool tile);

    PhysicalQueryPlanNode(PhysOpPtr const& PhysicalOperator,
                          std::vector<std::shared_ptr<PhysicalQueryPlanNode>> const& childNodes,
                          bool ddl, bool tile);

    virtual ~PhysicalQueryPlanNode();

    /// @param query the query
    /// @param dbgDepth depth in the physical plan tree
    /// @param descPath when present, recursion is restriction to the specified
    ///                 contiguous path of nodes from the root
    void inferDistType(const std::shared_ptr<Query>& query, size_t dbgDepth,
                       const PhysicalQueryPlanPath* descPath=nullptr);

    /// applies the result of _physicalOperator->inferChildInheritance() to a particular child
    /// @param childIndex The child that should have its inheritance updated
    void applyInheritanceToChild(size_t childIdx);

    /// calls checkDistType on operators, so they can check that DistType and Distribution
    /// are in agreement
    /// @param query the query
    /// @param dbgLevel depth in the physical plan tree
    void checkDistAgreement(const std::shared_ptr<Query>& query, size_t depth);

    void addChild(const std::shared_ptr<PhysicalQueryPlanNode>& child);

    ///
    /// finds the index of the child in the parent
    /// @param targetChild a child of the  node
    /// @return the index of that child
    /// throws if the child is not a child of the node
    ///
    size_t findChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild);

    /**
     * Removes node pointed to by targetChild from children.
     * @param targetChild node to remove. Must be in children.
     */
    void removeChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild);

    /**
     * Replace @c targetChild with @c newChild in QueryPlan Tree.
     *
     * The @c newChild (and its children) completely replaces the @c targetChild
     * (and any of its children) in the QueryPlan.
     *
     * NOTE: The @c targetChild and any of its children nodes are removed from
     * the query Plan tree, although the references between the @c targetChild
     * and its children remain.
     *
     * @verbatim
     *    a                                  a
     *   / \                                / \
     *  b  targetChild    newChild  ==>    b   newChild    targetChild
     *     /  \           /  \                 /  \        / \
     *    c    d         e    f               e    f      c   d
     * @endverbatim
     *
     * @param targetChild node to remove. Must be in children.
     * @param newChild node to insert. Must be in children.
     */
    void replaceChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild,
                      const std::shared_ptr<PhysicalQueryPlanNode>& newChild);

    /**
     * Supplant the @c targetChild with the @c newChild in the QueryPlan tree.
     *
     * The children of the original child are assigned to the newChild, and
     * the newChild is assigned as the child of original child's parent.
     *
     * @note Any children nodes of the @c newChild Node are removed from the
     * query Plan tree, and the references are removed between @c newChild and
     * any of its original children. Each node is a @c std::shared_ptr so any
     * object (@c targetChild, or original children of @c newChild) where the
     * refcount drops to 0 will be deleted.
     *
     * @verbatim
     *    a                                   a
     *   / \                                 / \
     *  b   targetChild    newChild  ==>    b   newChild       targetChild
     *        /  \          /   \                /  \
     *       c    d        e     f              c    d           e     f
     * @endverbatim
     *
     * @note Make certain to avoid creating cyclic graphs. @c newChild must @b
     * not be a child or descendant of @c targetChild. The main purpose of this
     * function is to insert a newChild (with no children) in the place of
     * targetChild.
     *
     *
     * @param targetChild node to remove. Must be in _childNodes.
     * @param newChild node to supplant the original.
     */
    void supplantChild(const std::shared_ptr<PhysicalQueryPlanNode>& targetChild,
                       const std::shared_ptr<PhysicalQueryPlanNode>& newChild);

    PhysOpPtr getPhysicalOperator() const { return _physicalOperator; }

    const std::vector<std::shared_ptr<PhysicalQueryPlanNode>>& getChildren() const { return _childNodes; }

    std::vector<std::shared_ptr<PhysicalQueryPlanNode>>&       getChildren()       { return _childNodes; }

    bool hasParent() const
    {
        return _parent.lock().get() != nullptr;
    }

    void resetParent()
    {
        _parent.reset();
    }

    const std::shared_ptr<PhysicalQueryPlanNode> getParent()
    {
        return _parent.lock();
    }

    struct Visitor {
        virtual ~Visitor() {}
        virtual void operator()(PhysicalQueryPlanNode&, const PhysicalQueryPlanPath*, size_t depth) = 0;
    };

    void visitDepthFirstPostOrder(Visitor& v, size_t depth=0);
    void visitDepthFirstPreOrder(Visitor& v, size_t depth=0);
    void visitPathPreOrder(Visitor& v, const PhysicalQueryPlanPath* descPath, size_t depth=0);
    void visitPathPostOrder(Visitor& v, const PhysicalQueryPlanPath* descPath, size_t depth=0);

    bool isDdl() const
    {
        return _ddl;
    }

    bool supportsTileMode() const
    {
        return _tile;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

    /**
     * Retrieve an ordered list of the shapes of the arrays to be input to this
     * node.
     */
    std::vector<ArrayDesc> getChildSchemas() const;

    /**
     * Determine if this node is for the PhysicalRepart operator.
     * @return true if physicalOperator is PhysicalRepart. False otherwise.
     */
    bool isRepartNode() const
    {
        return _physicalOperator.get() != nullptr &&
               _physicalOperator->getPhysicalName() == "physicalRepart";
    }

    /**
     * Determine if this node is for the PhysicalRedimension operator.
     * @return true if physicalOperator is PhysicalRedimension. False otherwise.
     */
    bool isRedimensionNode() const
    {
        return _physicalOperator.get() != nullptr &&
               _physicalOperator->getPhysicalName() == "PhysicalRedimension";
    }

    /**
     * Determine if this node is for the PhysicalInput operator.
     * @return true if physicalOperator is PhysicalInput. False otherwise.
     */
    bool isInputNode() const
    {
        return _physicalOperator.get() != nullptr &&
               _physicalOperator->getPhysicalName() == "impl_input";
    }

    /**
     * Determine if this node is for the PhysicalSG operator.
     * @return true if physicalOperator is PhysicalSG. False otherwise.
     */
    bool isSgNode() const
    {
        return _physicalOperator.get() != nullptr &&
                _physicalOperator->getPhysicalName() == "impl_sg";
    }

    /**
     * @return true if physicalOperator is summarize. False otherwise.
     * @note  a hack so Habilis can check that no converter was inserted
     *        before summarize.  TODO: Needs generalization
     */
    bool isSummarizeNode() const
    {
        return _physicalOperator.get() != nullptr &&
               _physicalOperator->getPhysicalName() == "PhysicalSummarize";
    }

    /**
     * Extract the array name paramenter from SG operator parameters
     * @return array name, empty if the paramenter is not present or empty
     */
    static std::string getSgArrayName(const Parameters& sgParameters);

    /**
     * Delegator to physicalOperator.
     */
    bool outputFullChunks() const
    {
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    /**
      * [Optimizer API] Determine if the output chunks
      * of this subtree will be completely filled.
      * Optimizer may insert SG operations for subtrees
      * that do not provide full chunks.
      * @return true if output chunking is guraranteed full, false otherwise.
      */
    bool subTreeOutputFullChunks() const;

    DistributionRequirement getDistributionRequirement() const
    {
        return _physicalOperator->getDistributionRequirement(getChildSchemas());
    }

    /**
     * TBD: needsSpecificInputDistributions() ?
     */
    bool needsSpecificDistribution() const
    {
        return getDistributionRequirement().getReqType()== DistributionRequirement::SpecificAnyOrder;
    }

    /**
     * @return estimate of total bytes produced by this operator
     */
    double getDataWidth()
    {
        return _boundaries.getSizeEstimateBytes(getPhysicalOperator()->getSchema());
    }

    /**
     * @return cached RedistributeContext saved by _setDistribution()
     */
    const RedistributeContext& getDistribution() const
    {
        return _distribution;
    }

    /// @return overwrite the distribution
    /// @note needed for certain Vistors
    void setDistribution(const RedistributeContext& distrib) { _distribution = distrib; }

    /**
     * set query on every physical operator in the plan
     * @param query the query to use
     */
    void setQuery(const std::shared_ptr<scidb::Query>&);

    /**
     * Using getDistribution() above, determine whether a
     * distribution converter (sg, reducer)
     * may commute before a (unary) operator.  This is true
     * when a unary operator does not change the distribution
     * of the data and the converter's destination DistType
     * is compatible with the input of the operator.
     * (e.g. not prohibited by isReplicatedInputOk())
     * (For example, false for leaf operators (not unary), redimension
     * (coordinate change).)
     * @return if a distribution converter node may commute before this node
     */
    bool converterCommutesBefore(DistType dstDistType) const;


    //I see an STL pattern coming soon...
    const PhysicalBoundaries& getBoundaries() const
    {
        return _boundaries;
    }

    const PhysicalBoundaries& inferBoundaries();

    /**
     * Combine information from _boundaries and _distribution
     * to produce a maximal result size in bytes.  This does
     * not have to be a tight bound, but it should attempt to be
     * a bound, within reason. (The remaining problem being that
     * we don't have a * bound on the size of string values without
     * accessing them.)  So we allow the upper bound of a string
     * value to be passed in.
     * @param maxStringLength a reasonable value for most purposes
     * @return volume of data in bytes
     * @note XXX TODO: Use ssize_t; if < 0 use CONFIG_STRING_SIZE_ESTIMATION
     */
    size_t estimatedMaxDataVolume(size_t maxStringLength=256) const ;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version);

private:
    PhysOpPtr _physicalOperator;

    std::vector< std::shared_ptr<PhysicalQueryPlanNode> > _childNodes;
    std::weak_ptr <PhysicalQueryPlanNode> _parent;

    bool _ddl;
    bool _tile;

    RedistributeContext _distribution;
    PhysicalBoundaries _boundaries;
};

/**
 * A text_iarchive class that helps de-serializing shared_ptr objects,
 * when a worker instance calls QueryProcessorImpl::parsePhysical().
 * The idea is that multiple deserialized raw pointers are equal will produce the same shared_ptr.
*/
class TextIArchiveQueryPlan: public boost::archive::text_iarchive
{
public:
    TextIArchiveQueryPlan(std::istream & is_, unsigned int flags = 0)
    : boost::archive::text_iarchive(is_, flags)
    {}

    struct SerializationHelper
    {
        SerializedPtrConverter<PhysicalQueryPlanNode> _nodes;
        SerializedPtrConverter<OperatorParam> _params;

        void clear()
        {
            _nodes.clear();
            _params.clear();
        }
    };

    SerializationHelper _helper;
};

template<class Archive>
void PhysicalQueryPlanNode::serialize(Archive& ar, const unsigned int version)
{
    // NOTE: _physicalOperator: not serialized here

    // ar & _childNodes;
    if (Archive::is_loading::value) {
        TextIArchiveQueryPlan::SerializationHelper& helper = dynamic_cast<TextIArchiveQueryPlan&>(ar)._helper;
        size_t size = 0;
        ar & size;
        _childNodes.resize(size);
        for (size_t i=0; i<size; ++i) {
            PhysicalQueryPlanNode* n;
            ar & n;
            _childNodes[i] = helper._nodes.getSharedPtr(n);
        }
    }
    else {
        size_t size = _childNodes.size();
        ar & size;
        for (size_t i=0; i<size; ++i) {
            PhysicalQueryPlanNode* n = _childNodes[i].get();
            ar & n;
        }
    }

    // NOTE: _childNodes not serialzed here

    ar & _ddl;
    ar & _tile;

    // NOTE: _distribution not serialized
    // NOTE: _boundaries not searialized

    /*
     * We not serializing whole operator object, to simplify user's life and get rid work serialization
     * user classes and inherited SciDB classes. Instead this we serializing operator name and
     * its parameters, and later construct operator by hand
     */
    if (Archive::is_loading::value)
    { // de-serializing from archive
        TextIArchiveQueryPlan::SerializationHelper& helper = dynamic_cast<TextIArchiveQueryPlan&>(ar)._helper;
        std::string logicalName;
        std::string physicalName;
        std::string controlCookie;
        Parameters parameters;
        KeywordParameters kwParameters;
        ArrayDesc schema;
        DistType inherited;
        DistType synthesized;

        ar & logicalName;
        ar & physicalName;
        ar & controlCookie;

        // ar & parameters;
        size_t size = 0;
        ar & size;
        parameters.resize(size);
        for (size_t i=0; i<size; ++i) {
            OperatorParam* op;
            ar & op;
            parameters[i] = helper._params.getSharedPtr(op);
        }

        ar & schema;
        ar & inherited;
        ar & synthesized;

        // ar & kwParameters;
        size = 0;
        ar & size;
        for (size_t i=0; i<size; ++i) {
            std::string kw;
            OperatorParam* op;
            ar & kw;
            ar & op;
            kwParameters.insert(make_pair(kw, helper._params.getSharedPtr(op)));
        }

        // ar & operatorID
        OperatorID opID;
        ar & opID;

        _physicalOperator = OperatorLibrary::getInstance()->createPhysicalOperator(
                    logicalName, physicalName, parameters, schema);
        _physicalOperator->setInheritedDistType(inherited);
        _physicalOperator->setSynthesizedDistType(synthesized);
        _physicalOperator->setKeywordParameters(kwParameters);
        _physicalOperator->setTileMode(_tile);
        _physicalOperator->setControlCookie(controlCookie);
        _physicalOperator->setOperatorID(opID);
    }
    else
    { // serializing to archive
        std::string logicalName = _physicalOperator->getLogicalName();
        std::string physicalName = _physicalOperator->getPhysicalName();
        std::string controlCookie = _physicalOperator->getControlCookie();
        Parameters parameters = _physicalOperator->getParameters();
        KeywordParameters kwParameters = _physicalOperator->getKeywordParameters();
        DistType inherited = _physicalOperator->getInheritedDistType();
        DistType synthesized = _physicalOperator->getSynthesizedDistType();
        ArrayDesc schema = _physicalOperator->getSchema();

        ar & logicalName;
        ar & physicalName;
        ar & controlCookie;

        // ar & parameters;
        size_t size = parameters.size();
        ar & size;
        for (size_t i=0; i<size; ++i) {
            OperatorParam* op = parameters[i].get();
            ar & op;
        }

        ar & schema;
        ar & inherited;
        ar & synthesized;

        // ar & kwParameters;
        size = kwParameters.size();
        ar & size;
        std::string keyword;
        for (auto& kwPair : kwParameters) {
            keyword = kwPair.first;
            OperatorParam* op = kwPair.second.get();
            ar & keyword;
            ar & op;
        }

        // ar & operatorID
        OperatorID opID =  _physicalOperator->getOperatorID();
        ar & opID;
    }
}

/**
 * The PhysicalPlan is produced by Optimizer or in simple cases directly by query processor (DDL).
 * It has ready to execution operator nodes and will be passed to an executor.
 */
class PhysicalPlan
{
public:
    PhysicalPlan(const std::shared_ptr<PhysicalQueryPlanNode>& root);

    std::shared_ptr<PhysicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    bool empty() const
    {
        return _root == std::shared_ptr<PhysicalQueryPlanNode>();
    }

    bool isDdl() const
    {
        assert(!empty());
        return _root->isDdl();
    }

    // see PhysicalQueryPlanNode::inferDistType() and PhysicalOperator::inferDistType
    void inferDistType(const std::shared_ptr<Query>& query);


    /// check that above was called correctly
    void checkRootDistType();

    // set _query
    void setQuery(const std::shared_ptr<Query>& query);

    bool supportsTileMode() const
    {
        SCIDB_ASSERT(!empty());
        return _root->supportsTileMode();
    }

    void setRoot(const std::shared_ptr<PhysicalQueryPlanNode>& root)
    {
        _root = root;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &out, int indent = 0, bool children = true) const;

    /// @return count of nodes in plan where node::isSGNode() is true
    size_t countSgNodes() const;

private:
    std::shared_ptr<PhysicalQueryPlanNode> _root;
};

} // namespace


#endif /* PHYSICALQUERYPLAN_H_ */
