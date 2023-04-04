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
 * @file HabilisOptimizer.h
 * @brief Our first attempt at a halfway intelligent optimizer.
 * @author poliocough@gmail.com
 */

// habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,.. (Google translate)
// Homo habilis: the earliest known species in the genus Homo. Perhaps the earliest primate to use stone tools (Wikipedia)

#ifndef HABILISOPTIMIZER_H_
#define HABILISOPTIMIZER_H_

#include <query/optimizer/Optimizer.h>
#include <query/OperatorParam.h>
#include <array/ArrayDistributionInterface.h>

#include <memory>
#include <vector>

namespace scidb
{
class Query;
class OperatorParam;
class ArrayDesc;
class RedistributeContext;
class LogicalQueryPlanNode;
class PhysicalQueryPlanNode;
class PhysicalPlan;
class LogicalOperator;
class PhysicalOperator;
typedef std::shared_ptr<PhysicalPlan> PhysPlanPtr;
typedef std::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;

class HabilisOptimizer : public Optimizer
{
public:
   friend class OptimizerTests;
   friend std::shared_ptr<Optimizer> Optimizer::create();

   ~HabilisOptimizer()
   {}

    /**
     * Create a physical plan from the logical plan.
     * Does not add all the necessary sg nodes - output may not be runnable.
     * @param logicalPlan plan o translate.
     */
     PhysPlanPtr
     createPhysicalPlan(std::shared_ptr<LogicalPlan> logicalPlan,
                        const std::shared_ptr<Query>& query);

   /**
    * Create an optimized physical plan from the given logical plan.
    * @return runnable physical plan.
    */
   PhysPlanPtr
   optimize(const std::shared_ptr<Query>& query,
            std::shared_ptr<PhysicalPlan> physicalPlan,
            bool isDdl);

   enum FeatureMask
   {
        //unused                               0x01
        INSERT_REDIM_OR_REPART               = 0x02,
        INSERT_MATERIALIZATION               = 0x04,
   };

private:
    /**
     * Create a new instance
     */
    HabilisOptimizer();

    class Eraser
    {
    public:
       Eraser(HabilisOptimizer& instance)
       : _instance(instance){}
       ~Eraser()
       {
          _instance._root.reset();
          _instance._query.reset();
          _instance._defaultArrRes.reset();
       }
    private:
       Eraser();                            // no default ctor
       Eraser(const Eraser&);               // no copy ctor
       Eraser& operator=(const Eraser&);    // no assignment
       void* operator new(size_t size);     // no making on the heap

       HabilisOptimizer& _instance;
    };
    friend class Eraser;

    bool isFeatureEnabled (FeatureMask m) const
    {
        return _featureMask & m;
    }

    //////Data:

    /**
     * constant. when an SG is not required (for correctnes)
     * this is the minimum number of bytes of data the estimated
     * input data should be, before we insert and SG for
     * the purpose of load-levelling (e.g. just processing
     * count(list('instances')) on coordinator is typically
     * fastest.  used by tw_insertSgNodes()
     *
     * 50 million bytes can be read and written from memory
     * of typical computers in less than 1ms, which is of
     * rougly the same order as gigabit ethernet latency,
     * and its plenty large enough to cover list('instances'),
     * show(array), and other "small" queries that tend to
     * be made repeatedly by the R and Python clients,
     * for example.
     */
    const size_t EFFICIENT_SG_MIN_DATA_BYTES = 50*1000*1000 ;


    /**
     * Current root of the plan. Initially empty.
     */
     PhysNodePtr    _root;

     /**
     * Current query of the plan. Initially empty.
     */
     std::shared_ptr<Query> _query;

    /**
     * Mask of features that are enabled
     */
    uint64_t        _featureMask;

    /// Temp array residency acquired from the query
    ArrayResPtr _defaultArrRes;

    //////Helper functions - misc:

    /**
     * Ensure that the resulting array is pulled, guaranteeing
     * that expected side-effects occur in all cases.
     *
     * @param physicalPlanRoot The root node of the physical plan.
     * @param logicalPlan The logical plan.
     * @param query The query context corresponding to the physical and
     *     logical plans.
     * @return An updated root node for the physical plan that is
     *     consume(oldroot(...)) if the oldroot(...) is selectible
     *     but the user has opted not to pull on it.
     */
    PhysNodePtr insertResultConsumer(PhysNodePtr const& physicalPlanRoot,
                                     std::shared_ptr<LogicalPlan> const& logicalPlan,
                                     std::shared_ptr<Query> const& query);

    /**
     * Print current WIP plan to stdout.
     */
    void printPlan(PhysNodePtr node = PhysNodePtr(), bool children=true) const;

    /**
     * Print current WIP plan to log with DEBUG level.
     */
    void logPlanDebug(PhysNodePtr node = PhysNodePtr(), bool children=true) const;

    /**
     * Print current WIP plan to log with TRACE level.
     */
    void logPlanTrace(PhysNodePtr node = PhysNodePtr(), bool children=true) const;

    //////Helper functions - node-level manipulators:

    /**
     * Insert a node into the plan tree. Add a nodeToInsert on top of target such that
     * target becomes child of nodeToInsert and nodeToInsert's parent becomes target's old
     * parent.
     * @param target node used to specify the location
     * @param nodeToInsert new node to insert.
     */
    void
    n_addParentNode                         ( PhysNodePtr target,  PhysNodePtr nodeToInsert);

    /**
     * Remove a node from the plan tree.
     * nodeToRemove's child becomes child of nodeToRemove's parent.
     * @param nodeToRemove. Must have only one child.
     */
    void
    n_cutOutNode                            ( PhysNodePtr nodeToRemove);

    /**
     * Build a new PhysicalParameter from a LogicalParameter.
     * Logic replicated from old optimizer.
     * @param[in] logicalParameter the parameter from the logical node
     * @param[in] logicalInputSchemas all inputs to the logical node
     * @param[in] logicalOutputSchema output schema inferred by the logical node
     */
    std::shared_ptr < OperatorParam>
    n_createPhysicalParameter (const std::shared_ptr< OperatorParam> & logicalParameter,
                                    const std::vector< ArrayDesc>& logicalInputSchemas,
                                    const ArrayDesc& logicalOutputSchema,
                                    bool tile);

    /**
     * Build a new PhysicalQueryPlanNode from a LogicalQueryPlanNode.
     * Does not recurse.
     * @param[in] logicalNode the node to translate.
     */
     PhysNodePtr
    n_createPhysicalNode            (std::shared_ptr < LogicalQueryPlanNode> logicalNode, bool tileMode);


    /**
     * Build a new SGNode from given attributes. Persist the result if storeArray is true.
     * If array is persisted, the name and id of are taken from outputSchema.
     * @param[in] outputSchema the output of the SG node
     * @param[in] instanceId the argument to the SG operator
     */
     PhysNodePtr
     n_buildSgNode(const ArrayDesc & outputSchema,
                   RedistributeContext const& dist) const;

     /**
      * Build a new ReduceDistro node based on a given child attributes.
      * @param[in] child the op whose distribution shall be reduced
      * @param[in] arrDist the requested distribution to reduce to
      */
     PhysNodePtr
     n_buildReducerNode(PhysNodePtr child,
                        const ArrayDistPtr& arrDist) const ;

     /// insert a reducer node above child, repair inheritance and propagate distribtion
     /// @param[in] root must equal child->getParent(), as a check
     /// @param[in] child must be a child of root
     /// @param[in] newContext, the target of the reduction inserted
     /// @param[in] depth, for debugging
     void insertReducerAtThinPoint(PhysNodePtr root, PhysNodePtr child, const RedistributeContext& context, size_t depth);

     /// insert an SG node above child, repair inheritance and propagate distribtion
     /// @param[in] root must equal child->getParent(), as a check
     /// @param[in] child must be a child of root
     /// @param[in] newContext, the target of the SG inserted
     /// @param[in] depth, for debugging
     void insertSgAtThinPoint(PhysNodePtr root, PhysNodePtr child, const RedistributeContext& context, size_t depth);

     /// insert distribution conversion
     /// insert a reducer and/or SG node to achive the specified distribution at input to the parent
     /// since dtReplication is and add an SG node if necessary.
     /// @param[in] target the op whose distribution shall be reduced
     /// @param[in] parent target's parent
     /// @param[in] dist the requested distribution+residency to reduce to
    void insertDistConversion(PhysNodePtr root, PhysNodePtr childNode,
                              const RedistributeContext& newContext, size_t depth);

    ///
    /// Helper functions - chain walkers:
    ///

    void
    cw_rectifyChainDistro( PhysNodePtr root,  PhysNodePtr sgCandidate, const  RedistributeContext & requiredDistribution);

    //////Helper functions - tree walkers:

    /**
     * Create an entire physical tree from the logicalTree recursively.
     * Does not add all the necessary sg nodes - output may not be runnable.
     * @param logicalRoot the root of the tree to translate.
     */
     PhysNodePtr
     tw_createPhysicalTree(std::shared_ptr < LogicalQueryPlanNode> logicalRoot, bool tileMode);

    /**
     * Add all necessary scatter-gather nodes to the tree.
     * @param root the root of the physical plan.
     */
    void
    tw_insertSgNodes( PhysNodePtr root, size_t depth);

    /// factored generic multi-child case
    /// handles all >=3 child cases
    /// handles some 2 child cases
    /// @param root the root of the physical plan.
    /// @param depth the depth of descent in the tree prior to modification
    void tw_insertSgNodes_genericMultiChild_oneLevel(PhysNodePtr root, size_t depth,
                                                     std::vector<DistType> childInheritances);

    /**
     * Update SG's isStrict flag if the upstream operator
     * (i.e. input() or redimension()) overrides the default
     * @param root root of physical plan.
     */
    void
    tw_updateSgStrictness(PhysNodePtr root);

    /**
     * Insert redimensions or repartition operators where needed
     * @param root root of physical plan.
     * @return true IFF a transformation was performed
     */
    void tw_insertRedimensionOrRepartitionNodes(PhysNodePtr root, size_t depth);


    /**
     * Determine if an inserted repart/redimension (child input) for a
     * PhysicalOperator may change the dimension range.
     *
     * @param physOperator the physicalOperator
     * @return true IFF physicalOperator can alter the dimension range.
     */
    bool allowEndMaxChange(std::shared_ptr<PhysicalOperator> const & physOperator) const;

    /**
     * Can the @c desired and @c given input schemas be collapsed into a schema
     * for a singular repart/redimension operator for a particular @parent
     * operator?
     *
     * @description
     * Schemas are collapsible if their corresponding dimensions all have:
     * - same low bound
     * - same high bound (unless parent node is merge(), then it's OK
     *   to so long as the desired dimension fits inside the
     *   explicitly given one)
     * - same overlap
     * - given interval is '*' but desired one is specified
     *
     * If collapsible, it's OK to modify the given-dimension intervals
     * with specific ones from the desired-dimensions.  And that
     * schema is returned as the result.
     *
     * @param parent[in]
     * @parblock
     * the PhysicalQueryPlanNode with a redimension/repart child input.
     * The parent's physical operator provides rules about what can and cannot be collapsed.
     *
     * (e.g. merge allows for the endMax to be modified)
     * @endparblock
     *
     * @param[in] desired
     * @parblock
     * the schema of the implicit redimension/repart schema wanted by requiresRedimensionOrRepartition
     * @endparblock
     *
     * @param[in] given
     * @parblock
     * the schema of the child redimension/repart schema in the query
     * @endparblock
     *
     * @param[out] result
     * @parblock
     * the resulting schema iff a collapse of the two redimension/repart schemas is possible
     * @endparblock
     *
     * @return @c true IFF desired schema can be "collapsed" with the current
     */
    bool
    isRedimCollapsible(PhysNodePtr const& parent,
                       ArrayDesc const& desired,
                       ArrayDesc const& given,
                       ArrayDesc & result) const;

    //
    // TODO: Tile-mode arrays (emanating from perhaps, filter, between, apply)
    //            are not generally processable: most operators do not support it
    //            Therefore materializations must be inserted between tile-mode operators
    //            and inputs to non-tile-mode operators
    //            TODO: do we need a full materialization?  seems like overkill
    //            TODO: mismatches should be named by what mismatch they fix, not
    //               what they do to fix the mismatch, unless there are multiple approaches
    //               to fixing a mimatch, they can be distinguished by the approach
    //            TODO: is this a logical or physical mismatch?
    void
    tw_insertChunkMaterializers(PhysNodePtr root, size_t depth);

    //////Helper functions - nested parameter list handling:

    struct TileModeException {};

    class PhysParamsBuilder
    {
    public:
        PhysParamsBuilder(HabilisOptimizer& optimizer,
                          std::shared_ptr<LogicalOperator> const& lop,
                          std::vector<ArrayDesc> const& inSchemas,
                          ArrayDesc const& outSchema,
                          bool wantTileMode);

        // Get ready to try again with new tile mode setting.
        void reset(bool wantTileMode);

        // Access to built physical parameters.
        Parameters& getParameters();
        KeywordParameters& getKeywordParameters();

        // PlistVisitor to walk the logical parameters and build the
        // physical ones.  Throws TileModeException if tile mode
        // wanted but logical expression parameter doesn't compile in
        // tile mode.
        void operator()(Parameter& logical,
                        PlistWhere const& where,
                        std::string const& kw);

    private:
        // References or copies of inputs.
        HabilisOptimizer& _opt;
        std::shared_ptr<LogicalOperator> const& _lop;
        std::vector<ArrayDesc> const& _inSchemas;
        ArrayDesc const& _outSchema;
        bool _tileMode;

        // State for tracking progress in depth-first traversal.
        using Cursor = std::pair<Parameters*, size_t>;
        std::vector<Cursor> _stack;
        bool _firstCall;
        std::string _currKw;

        // Builds the physical parameters here!
        Parameters _physParams;
        KeywordParameters _physKwParams;

        // Runs appropriate overload of compileParamInTileMode().
        bool _opWantsTileMode(PlistWhere const& where, std::string const& kw);
    };

};

}

#endif /* HABILISOPTIMIZER_H_ */
