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
 * @file HabilisOptimizer.cpp
 *
 * @brief Our first attempt at a halfway intelligent optimizer.
 * habilis (adj.) Latin: fit, easy, adaptable, apt, handy, well-adapted, inventive,..
 *
 * @author poliocough@gmail.com
 */

#include "query/optimizer/HabilisOptimizer.h"

#include <array/ArrayDistribution.h>
#include <array/DelegateArray.h>
#include <query/Expression.h>
#include <query/LogicalQueryPlan.h>
#include <query/ParsingContext.h>
#include <query/PhysicalQueryPlan.h>
#include <query/PhysicalQueryPlanUtilities.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <query/ops/sg/SGParams.h>

#include <log4cxx/logger.h>

#include <fstream>
#include <memory>

using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("HabilisOptimizer"));

namespace {

// Adds semantics to code in this file, to avoid calling some_func(..., false, ...).
static constexpr bool TILE_MODE = true;
static constexpr bool CHILDREN = true;
static constexpr bool CONSTANT = true;

bool producesDataframe(PhysicalQueryPlanNode const& node)
{
    auto op = node.getPhysicalOperator();
    return op->getSchema().isDataframe();
}

} // anonymous namespace

HabilisOptimizer::HabilisOptimizer()
:
    _root(),
    _featureMask(INSERT_REDIM_OR_REPART
                 | INSERT_MATERIALIZATION
                )
{
    const char* path = "/tmp/scidb_optimizer_override";
    std::ifstream inFile (path);
    if (inFile && !inFile.eof())
    {
        inFile >> _featureMask;
        LOG4CXX_TRACE(logger, "Feature mask overridden to "<<_featureMask);
    }
    inFile.close();
}


PhysPlanPtr HabilisOptimizer::optimize(const std::shared_ptr<Query>& query,
                                       std::shared_ptr<PhysicalPlan> physicalPlan,
                                       bool isDdl)
{
    // preconditions
    SCIDB_ASSERT(!_root);        // temporary place to build the physical plan
    SCIDB_ASSERT(!_query);
    SCIDB_ASSERT(!_defaultArrRes);
    SCIDB_ASSERT(physicalPlan);

    Eraser onStack(*this);              // cleans up _root, _query, and _defaultArrRes

    _root = physicalPlan->getRoot();
    _query = query;
    _defaultArrRes = _query->getDefaultArrayResidency();

    assert(_root);
    assert(_query);
    assert(_defaultArrRes);

    LOG4CXX_TRACE(logger, "HabilisOptimizer::optimize() inferDistType @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
    physicalPlan->inferDistType(_query);
    physicalPlan->checkRootDistType();  // check it is legitimate

    if (!isDdl)
    {

        //
        // fix type mismatches (logical)
        //
        if (isFeatureEnabled(INSERT_REDIM_OR_REPART))
        {
            tw_insertRedimensionOrRepartitionNodes(_root, 0);
        }

        LOG4CXX_TRACE(logger, "HabilisOptimizer::optimize() @@@ tw_insertSgNodes @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        tw_insertSgNodes(_root, 0);
        LOG4CXX_TRACE(logger, "HabilisOptimizer::optimize() @@@ checkDistAgreement @@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        _root->checkDistAgreement(_query, 0);

        // TODO: SDB-3563 consider re-enabling a single pass of
        // tw_collapseSgNodes(_root);

        //
        // can be treated as logical type mismatch?
        //
        if (isFeatureEnabled(INSERT_MATERIALIZATION))
        {
            tw_insertChunkMaterializers(_root, 0);
            _root->checkDistAgreement(_query, 0); // check that requirements are still met
        }

        //
        // TODO:  ad-hoc adjustent? can this be
        //  incorporated into either insertion or
        //  optimization passes in a more traditional
        //  way?
        //
        tw_updateSgStrictness(_root);
    }

    PhysPlanPtr result(new PhysicalPlan(_root));

    return result;
}

PhysNodePtr HabilisOptimizer::insertResultConsumer(PhysNodePtr const& physicalPlanRoot,
                                                   std::shared_ptr<LogicalPlan> const& logicalPlan,
                                                   std::shared_ptr<Query> const& query)
{
    SCIDB_ASSERT(physicalPlanRoot);
    SCIDB_ASSERT(logicalPlan);
    SCIDB_ASSERT(query);

    if (logicalPlan->isDdl() || query->fetch) {
        // If the query is DDL or the client will pull the results of the executed
        // query locally, then don't insert a consume() at the root.
        return physicalPlanRoot;
    }

    auto logicalPlanRoot = logicalPlan->getRoot();
    SCIDB_ASSERT(logicalPlanRoot);

    if (!logicalPlanRoot->isSelective() ||
        logicalPlanRoot->getLogicalOperator()->getLogicalName() == "consume") {
        // If the query doesn't produce a result array that can be pulled or
        // the outer-most operator in the query is consume() already, then
        // don't insert a consume() at the root.
        return physicalPlanRoot;
    }

    Parameters consumeParams;  // Intentionally empty.
    OperatorLibrary* oplib = OperatorLibrary::getInstance();
    SCIDB_ASSERT(oplib);
    auto rootPhysicalOp = physicalPlanRoot->getPhysicalOperator();
    SCIDB_ASSERT(rootPhysicalOp);

    PhysOpPtr consumeOp =
        oplib->createPhysicalOperator("consume",
                                      "PhysicalConsume",
                                      consumeParams,
                                      rootPhysicalOp->getSchema());
    SCIDB_ASSERT(consumeOp);
    consumeOp->setQuery(query);
    bool useTileMode = rootPhysicalOp->getTileMode();
    consumeOp->setTileMode(useTileMode);
    auto consumePhysNode =
        std::make_shared<PhysicalQueryPlanNode>(consumeOp, /*ddl:*/false, useTileMode);
    SCIDB_ASSERT(consumePhysNode);
    consumePhysNode->addChild(physicalPlanRoot);
    consumePhysNode->inferBoundaries();

    return consumePhysNode;
}

PhysPlanPtr HabilisOptimizer::createPhysicalPlan(std::shared_ptr< LogicalPlan> logicalPlan,
                                                 const std::shared_ptr<Query>& query)
{
    // most of this _query mgmt nonsense is because habilis didn't bother to pass query
    assert(!_query);
    _query = query;
    assert(_query);

    std::shared_ptr<LogicalQueryPlanNode> logicalRoot = logicalPlan->getRoot();
    if (!logicalRoot)
    {
        SCIDB_ASSERT(!_root);
        return std::make_shared<PhysicalPlan>(_root);
    }

    bool tileMode = Config::getInstance()->getOption<int>(CONFIG_TILE_SIZE) > 1;
    PhysNodePtr root = tw_createPhysicalTree(logicalRoot, tileMode); // requires _query
    root = insertResultConsumer(root, logicalPlan, query);

    // restore _query
    _query.reset();  // restore _query to original state
    SCIDB_ASSERT(!_query);

    return std::make_shared<PhysicalPlan>(root);
}


// TODO: consider elimination? unused locally
void HabilisOptimizer::printPlan(PhysNodePtr node, bool children) const
{
    if (!node) {
        node = _root;
    }
    scidb::printPlan(node, 0, children);
}

void HabilisOptimizer::logPlanDebug(PhysNodePtr node, bool children) const
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanDebug(logger, node, 0, children);
}

void HabilisOptimizer::logPlanTrace(PhysNodePtr node, bool children) const
{
    if (!node) {
        node = _root;
    }
    scidb::logPlanTrace(logger, node, 0, children);
}

// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
void HabilisOptimizer::n_addParentNode(PhysNodePtr target, PhysNodePtr nodeToInsert)
{
    auto insertOp = nodeToInsert->getPhysicalOperator();
    SCIDB_ASSERT(insertOp);
    LOG4CXX_TRACE(logger, "[n_addParentNode] node to insert:"<< insertOp->getPhysicalName());
    logPlanTrace(nodeToInsert, false);
    LOG4CXX_TRACE(logger, "[n_addParentNode] target tree:");
    logPlanTrace(target);

    if (target->hasParent())
    {
        PhysNodePtr oldParent = target->getParent();
        oldParent->replaceChild(target, nodeToInsert);
        SCIDB_ASSERT(nodeToInsert->getParent()==oldParent);
        oldParent->findChild(nodeToInsert);    // throws if nodeToInsert is not child of oldParent
    }
    else
    {
        assert(_root == target);
        _root = nodeToInsert;
        _root->resetParent();   // paranoid
    }

    nodeToInsert->addChild(target);
    SCIDB_ASSERT(target->getParent()==nodeToInsert);
    SCIDB_ASSERT(nodeToInsert->findChild(target) == 0);

    LOG4CXX_TRACE(logger, "[n_addParentNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_addParentNode] end");
}

// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
void HabilisOptimizer::n_cutOutNode(PhysNodePtr nodeToRemove)
{
    LOG4CXX_TRACE(logger, "[n_cutOutNode] begin");
    logPlanTrace(nodeToRemove, false);
    vector<PhysNodePtr> children = nodeToRemove->getChildren();
    assert(children.size()<=1);

    if (nodeToRemove->hasParent())
    {
        PhysNodePtr parent = nodeToRemove->getParent();
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            parent->replaceChild(nodeToRemove, child);
        }
        else
        {
            parent->removeChild(nodeToRemove);
        }
    }

    else
    {
        assert(_root == nodeToRemove);
        if (children.size() == 1)
        {
            PhysNodePtr child = children[0];
            _root = child;
            _root->resetParent();
        }
        else
        {
            _root.reset();
        }
    }
    LOG4CXX_TRACE(logger, "[n_cutOutNode] done");
    logPlanTrace();
    LOG4CXX_TRACE(logger, "[n_cutOutNode] end");
}

// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
std::shared_ptr<OperatorParam>
HabilisOptimizer::n_createPhysicalParameter(const std::shared_ptr<OperatorParam> & logicalParameter,
                                            const vector<ArrayDesc>& logicalInputSchemas,
                                            const ArrayDesc& logicalOutputSchema,
                                            bool tile)
{
    if (logicalParameter->getParamType() != PARAM_LOGICAL_EXPRESSION) {
        return logicalParameter;
    }

    auto physicalExpression = std::make_shared<Expression> ();
    std::shared_ptr<OperatorParamLogicalExpression>& logicalExpression =
        (std::shared_ptr<OperatorParamLogicalExpression>&) logicalParameter;

    try
    {
        if (logicalExpression->isConstant()) {
           physicalExpression->compile(logicalExpression->getExpression(),
                                       tile,
                                       logicalExpression->getExpectedType().typeId());
        } else {
           physicalExpression->compile(logicalExpression->getExpression(),
                                       tile,
                                       logicalExpression->getExpectedType().typeId(),
                                       logicalInputSchemas,
                                       logicalOutputSchema);
        }

        if (tile && !physicalExpression->supportsTileMode()) {
            return std::shared_ptr<OperatorParam>();
        }

        return std::shared_ptr<OperatorParam> (
            new OperatorParamPhysicalExpression(logicalParameter->getParsingContext(),
                                                physicalExpression,
                                                logicalExpression->isConstant()));
    }
    catch (Exception &e)
    {
        if (e.getLongErrorCode() != SCIDB_LE_TYPE_CONVERSION_ERROR &&
            e.getLongErrorCode() != SCIDB_LE_TYPE_CONVERSION_ERROR2)
        {
            e.raise();
        }

        throw USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,
                                   SCIDB_LE_PARAMETER_TYPE_ERROR,
                                   logicalExpression->getParsingContext())
            << logicalExpression->getExpectedType().name()
            << TypeLibrary::getType(physicalExpression->getType()).name();
    }
}


bool HabilisOptimizer::PhysParamsBuilder::_opWantsTileMode(PlistWhere const& where, string const& kw)
{
    OperatorLibrary& opLib = *OperatorLibrary::getInstance();
    auto recognizer = opLib.getPlistRecognizer(_lop->getLogicalName());
    if (recognizer) {
        // New-style operators that support plist regular expressions
        // MUST also support this call, which allows for nested
        // parameter lists.  In for a penny, in for a pound.
        return _lop->compileParamInTileMode(where, kw);
    }

    ASSERT_EXCEPTION(where.size() < 2 &&
                     (where.empty() ^ kw.empty()), // xor
                     "Nested parameter list, but operator '"
                     << _lop->getLogicalName()
                     << "' does not use plist regexes?!");

    return kw.empty()
        ? _lop->compileParamInTileMode(where[0])
        : _lop->compileParamInTileMode(kw);
}

HabilisOptimizer::PhysParamsBuilder::PhysParamsBuilder(HabilisOptimizer& optimizer,
                  std::shared_ptr<LogicalOperator> const& op,
                  vector<ArrayDesc> const& ins,
                  ArrayDesc const& outs,
                  bool wantTileMode)
    : _opt(optimizer)
    , _lop(op)
    , _inSchemas(ins)
    , _outSchema(outs)
    , _tileMode(wantTileMode)
    , _firstCall(true)
{ }

void HabilisOptimizer::PhysParamsBuilder::reset(bool wantTileMode)
{
    _tileMode = wantTileMode;
    _physParams.clear();
    _physKwParams.clear();
    _stack.clear();
    _firstCall = true;
    _currKw.clear();
}

Parameters& HabilisOptimizer::PhysParamsBuilder::getParameters()
{
    assert(_stack.empty());     // Proves traversal is done.
    return _physParams;
}

KeywordParameters& HabilisOptimizer::PhysParamsBuilder::getKeywordParameters()
{
    assert(_stack.empty());     // Proves traversal is done.
    return _physKwParams;
}

void
HabilisOptimizer::PhysParamsBuilder::operator()(Parameter& logical,
                                                PlistWhere const& where,
                                                string const& kw)
{
    bool tiling = _tileMode && _opWantsTileMode(where, kw);
    Parameter physical =
        _opt.n_createPhysicalParameter(logical, _inSchemas, _outSchema, tiling);
    if (!physical) {
        assert(tiling);
        throw TileModeException();
    }

    // Special handling for OperatorParamNested.  We need to know how
    // many children, but we also need an empty subparameter list so
    // that we can append physical children.

    size_t nestSize = 0;
    auto nested = dynamic_cast<OperatorParamNested *>(logical.get());
    if (nested) {
        nestSize = nested->getParameters().size();
        physical = Parameter(new OperatorParamNested());
        nested = dynamic_cast<OperatorParamNested *>(physical.get());
        nested->getParameters().reserve(nestSize);
    }

    // Now the tricky part: placing the new physical parameter in its
    // correct position.  We rely on doing a depth-first traversal.
    // Whenever we see a new keyword we know we're starting on a new
    // parameter list, so push a pointer to it and its length.  When
    // we've counted down the length, we know we're done with the
    // list.  When we see an OperatorParamNested instance, its sublist
    // becomes the active top-of-stack list.

    // First parameter for a top-level list?  (That is, first time we
    // see a new keyword?)  Consume it, and maybe set up the stack for
    // the next args.
    if (_firstCall || kw != _currKw) {
        // Starting a new top-level list, perhaps.
        assert(_stack.empty());
        _firstCall = false;
        _currKw = kw;
        if (kw.empty()) {
            assert(!_lop->getParameters().empty());
            _stack.push_back(make_pair(&_physParams, _lop->getParameters().size()));
            _stack.back().first->push_back(physical);
            if (0 == --_stack.back().second) {
                _stack.pop_back();
            }
            if (nested) {
                _stack.push_back(make_pair(&nested->getParameters(), nestSize));
            }
        } else if (nested) {
            _physKwParams[kw] = physical;
            _stack.push_back(make_pair(&nested->getParameters(), nestSize));
        } else {
            // Simple keyword parameter, no stack maintenance to do.
            _physKwParams[kw] = physical;
        }
        return;
    }

    // We're working on the current top-of-stack parameter list,
    // wherever it may be.  Keyword 'kw' may or may not be empty, but
    // the parameter list we care about is on the stack.

    assert(!_stack.empty());
    assert(_stack.back().second > 0);
    _stack.back().first->push_back(physical);
    if (0 == --_stack.back().second) {
        _stack.pop_back();
    }
    if (nested) {
        _stack.push_back(make_pair(&nested->getParameters(), nestSize));
    }
}


// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
PhysNodePtr HabilisOptimizer::n_createPhysicalNode(std::shared_ptr<LogicalQueryPlanNode> logicalNode,
                                                   bool tileMode)
{
    const std::shared_ptr<LogicalOperator>& logicalOp = logicalNode->getLogicalOperator();
    const string& logicalName = logicalOp->getLogicalName();

    OperatorLibrary* opLibrary = OperatorLibrary::getInstance();
    vector<string> physicalOperatorsNames;
    opLibrary->getPhysicalNames(logicalName, physicalOperatorsNames);
    const string &physicalName = physicalOperatorsNames[0];
    const vector<std::shared_ptr<LogicalQueryPlanNode> >& children = logicalNode->getChildren();

    // Collection of input schemas of operator for resolving references
    vector<ArrayDesc> inputSchemas;
    tileMode &= logicalOp->getProperties().tile;
    for (size_t ch = 0; ch < children.size(); ch++)
    {
        inputSchemas.push_back(children[ch]->getLogicalOperator()->getSchema());
    }

    const ArrayDesc& outputSchema = logicalOp->getSchema();

    // For all logical parameters, can I create corresponding physical
    // param in tile mode?  If any param *not* tile mode compatible,
    // then restart, and *nobody* gets tile mode.

    PhysParamsBuilder builder(*this, logicalOp, inputSchemas, outputSchema, tileMode);
    bool done = false;
    for (int retries = 2; !done && retries > 0; --retries) {
        try {
            PlistVisitor v = std::ref(builder);
            logicalOp->visitParameters(v);
            done = true;
        }
        catch (TileModeException&) {
            assert(tileMode);
            tileMode = false;
            builder.reset(tileMode);
        }
    }
    assert(done);

    LOG4CXX_TRACE(logger, "HabilisOptimizer::n_createPhysicalNode: logical "
                  << logicalOp->getLogicalName() << "() outputSchema.getDistribution()->getDistType() "
                  << outputSchema.getDistribution()->getDistType());

    PhysOpPtr physicalOp =
        opLibrary->createPhysicalOperator(logicalName,
                                          physicalName,
                                          builder.getParameters(),
                                          outputSchema);
    physicalOp->setQuery(_query);
    physicalOp->setKeywordParameters(builder.getKeywordParameters());
    physicalOp->setTileMode(tileMode);
    physicalOp->inspectLogicalOp(*logicalOp);

    // where [all?] PhysicalQueryPlanNodes are created
    return std::make_shared<PhysicalQueryPlanNode>(physicalOp, logicalNode->isDdl(), tileMode);
}

// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
PhysNodePtr HabilisOptimizer::n_buildSgNode(const ArrayDesc & outputSchema,
                                            RedistributeContext const& dist) const
{
    if (dist.isUndefined()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER,
                               SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }

    ArrayDistPtr arrDist = dist.getArrayDistribution();
    ArrayResPtr   arrRes = dist.getArrayResidency();

    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(arrDist);

    Parameters sgParams;

    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));

    ps.setInt32(arrDist->getDistType());

    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> psParam(new OperatorParamPhysicalExpression(
                                            std::make_shared<ParsingContext>(),
                                            psConst,
                                            CONSTANT));
    sgParams.push_back(psParam);

    ArrayDesc outputDesc(outputSchema);
    outputDesc.setDistribution(arrDist);
    outputDesc.setResidency(arrRes);

    PhysOpPtr sgOp = OperatorLibrary::getInstance()->createPhysicalOperator("_sg", "impl_sg", sgParams, outputDesc);
    sgOp->setQuery(_query);

    PhysNodePtr sgNode(new PhysicalQueryPlanNode(sgOp, false, false));
    LOG4CXX_TRACE(logger, "n_buildSgNode: built sg ID:" << sgOp->getOperatorID().getValue());
    return sgNode;
}

// TODO: should be refactored/moved to be a PhysicalQueryPlanNode method?
PhysNodePtr HabilisOptimizer::n_buildReducerNode(PhysNodePtr child,
                                                 const ArrayDistPtr& arrDist) const
{
    SCIDB_ASSERT(arrDist);

    //insert a distro reducer node. In this branch sgNeeded is always false.
    Parameters reducerParams;
    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(arrDist->getDistType());
    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> reducer =
        std::make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                          psConst,
                                                          CONSTANT);
    reducerParams.push_back(reducer);

    // reducer always changes the distribution
    ArrayDesc outputDesc(child->getPhysicalOperator()->getSchema());
    outputDesc.setDistribution(arrDist);

    PhysOpPtr reducerOp = OperatorLibrary::getInstance()->createPhysicalOperator("_reduce_distro",
                                                                                 "physicalReduceDistro",
                                                                                 reducerParams,
                                                                                 outputDesc);
    reducerOp->setQuery(_query);
    bool useTileMode = child->getPhysicalOperator()->getTileMode();
    PhysNodePtr reducerNode(new PhysicalQueryPlanNode(reducerOp, false, useTileMode));
    reducerNode->getPhysicalOperator()->setTileMode(useTileMode );
    return reducerNode;
}

// TODO: s_setSGDistribution() once had the default
//       isStrict = true
//       That was converted to an explicit true,
//       and the isPull argument added, which now
//       has an explicit fixed argument in many cases
//       The code is passing all tests at present
//       but only a limited number of operators support
//       isPull
//       After the initial checkin of this code
//       but before closing ticket SDB-5720
//       all calls to this method with fixed arguments
//       should be reviewed for whether those arguments
//       will be sufficinet as more operators support
//       reading from an isPull SG.
static void s_setSgDistribution(PhysNodePtr sgNode,
                                RedistributeContext const& dist,
                                bool isStrict, bool isPull)
{
    if (dist.isUndefined()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER,
                               SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }
    LOG4CXX_TRACE(logger, "s_setSgDistribution: isStrict: " << isStrict << " isPull: " << isPull);

    Parameters parameters = sgNode->getPhysicalOperator()->getParameters();
    Parameters newParameters;

    std::shared_ptr<Expression> psConst = std::make_shared<Expression> ();
    Value ps(TypeLibrary::getType(TID_INT32));
    ps.setInt32(dist.getDistType());
    psConst->compileConstant(false, TID_INT32, ps);
    std::shared_ptr<OperatorParam> psParam(new OperatorParamPhysicalExpression(
                                            std::make_shared<ParsingContext>(),
                                            psConst,
                                            CONSTANT));
    newParameters.push_back(psParam);
    LOG4CXX_TRACE(logger, "s_setSGDistribution: Added newParameters[0] to SG node, ps="<<ps.get<int32_t>());
    SCIDB_ASSERT(newParameters.size() == SGPARAM_DISTRIBUTION+1);

    if( dist.getDistType() == dtLocalInstance)
    {   //add instance number for local node distribution

        const LocalArrayDistribution* localDist =
           safe_dynamic_cast<const LocalArrayDistribution*>(dist.getArrayDistribution().get());

        Value instanceId(TypeLibrary::getType(TID_INT64));
        instanceId.setInt64(localDist->getLogicalInstanceId());

        std::shared_ptr<Expression> instanceIdExpr = std::make_shared<Expression> ();
        instanceIdExpr->compileConstant(false, TID_INT64, instanceId);
        std::shared_ptr<OperatorParam> instParam(new OperatorParamPhysicalExpression(
                                                  std::make_shared<ParsingContext>(),
                                                  instanceIdExpr,
                                                  CONSTANT));
        newParameters.push_back(instParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[1] to SG node, instanceId="<<instanceId.get<int64_t>());
        SCIDB_ASSERT(newParameters.size() == SGPARAM_INSTANCE_ID+1);
    }

    // transfer the rest of the existing params
    for (size_t i = newParameters.size(); i< parameters.size() && i< NUM_SGPARAM; i++) {
        newParameters.push_back(parameters[i]);
        LOG4CXX_TRACE(logger, "s_setSGDistribution: Copying existing param to SG node, from oldParam["<<i<<"]");
    }

    // JHM ?copy SG operator's distribution and residency onto the node?
    ArrayDesc const& opSchema = sgNode->getPhysicalOperator()->getSchema();
    if (!opSchema.getDistribution()->isCompatibleWith(dist.getArrayDistribution()) ||
        !opSchema.getResidency()->isEqual(dist.getArrayResidency())) {

        ArrayDesc sgSchema(opSchema);

        SCIDB_ASSERT(dist.getArrayDistribution());
        sgSchema.setDistribution(dist.getArrayDistribution());

        SCIDB_ASSERT(dist.getArrayResidency());
        sgSchema.setResidency(dist.getArrayResidency());

        sgNode->getPhysicalOperator()->setSchema(sgSchema);
    }

    if (newParameters.size() <= SGPARAM_INSTANCE_ID)
    {   //if we don't already have an instance parameter - add a fake instance
        std::shared_ptr<Expression> instanceConst(new Expression());;
        Value instance(TypeLibrary::getType(TID_INT64));
        instance.setInt64(-1);
        instanceConst->compileConstant(false, TID_INT64, instance);
        std::shared_ptr<OperatorParam> instParam(new OperatorParamPhysicalExpression(
                                                  std::make_shared<ParsingContext>(),
                                                  instanceConst,
                                                  CONSTANT));
        newParameters.push_back(instParam);
        LOG4CXX_TRACE(logger, "s_setSGDistribution: Added newParameters[1] to SG node, instanceId="<<instance.get<int64_t>());
        SCIDB_ASSERT(newParameters.size() == SGPARAM_INSTANCE_ID+1);
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_INSTANCE_ID);

    // if not already there, add the isStrict flag
    std::shared_ptr<Expression> strictFlagExpr(new Expression());
    Value strictFlag(TypeLibrary::getType(TID_BOOL));
    strictFlag.setBool(isStrict);
    strictFlagExpr->compileConstant(false, TID_BOOL, strictFlag);
    std::shared_ptr<OperatorParam> strictParam =
        std::make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                          strictFlagExpr,
                                                          CONSTANT);

    // TODO: cleaner to just resize() and then set IS_STRICT and IS_PULL between here and line 559?
    //       or to factor the if() { push_back() } else { = } to a helper
    if (newParameters.size() <= SGPARAM_IS_STRICT ) {
        SCIDB_ASSERT(newParameters.size() == SGPARAM_IS_STRICT);
        newParameters.push_back(strictParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[" << SGPARAM_IS_STRICT<<"] to SG node, isStrict="<<isStrict);
    } else {
        newParameters[SGPARAM_IS_STRICT] = strictParam;
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_IS_STRICT);
    LOG4CXX_TRACE(logger, "Adding new param to SG node, [" << SGPARAM_IS_STRICT<<"] isStrict="<<isStrict);

    // if not already there, add the isPull flag - mimicking the isStrict code above
    // TODO: decide if this should never be passed in
    std::shared_ptr<Expression> pullFlagExpr(new Expression());
    Value pullFlag(TypeLibrary::getType(TID_BOOL));
    pullFlag.setBool(isPull);
    pullFlagExpr->compileConstant(false, TID_BOOL, pullFlag);
    std::shared_ptr<OperatorParam> pullParam =
        std::make_shared<OperatorParamPhysicalExpression>(std::make_shared<ParsingContext>(),
                                                          pullFlagExpr,
                                                          CONSTANT);
    if (newParameters.size() <= SGPARAM_IS_PULL) {
        SCIDB_ASSERT(newParameters.size() == SGPARAM_IS_PULL);
        newParameters.push_back(pullParam);
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Added newParameters[" << SGPARAM_IS_PULL<<"] to SG node, isPull="<<isPull);
    } else {
        newParameters[SGPARAM_IS_PULL] = pullParam;
        LOG4CXX_TRACE(logger, "is_setSGDistribution: Set newParameters[" << SGPARAM_IS_PULL<<"] on SG node, isPull="<<isPull);
    }
    SCIDB_ASSERT(newParameters.size() > SGPARAM_IS_PULL);
    LOG4CXX_TRACE(logger, "Adding new param to SG node, [" << SGPARAM_IS_PULL <<"] isPull="<<isPull);

    InstanceID instanceShift;
    ArrayDistributionFactory::getTranslationInfo(dist.getArrayDistribution().get(),
                                                 instanceShift);
    SCIDB_ASSERT(instanceShift == 0);

    LOG4CXX_TRACE(logger, "s_setSGDistribution: sgNode->setParamters(), size = "<<newParameters.size());

    sgNode->getPhysicalOperator()->setParameters(newParameters);
}

PhysNodePtr HabilisOptimizer::tw_createPhysicalTree(std::shared_ptr<LogicalQueryPlanNode> logicalRoot, bool tileMode)
{
   logicalRoot = logicalRewriteIfNeeded(_query, logicalRoot);

    vector<std::shared_ptr<LogicalQueryPlanNode> > logicalChildren = logicalRoot->getChildren();
    vector<PhysNodePtr> physicalChildren(logicalChildren.size());
    bool rootTileMode = tileMode;
    for (size_t i = 0; i < logicalChildren.size(); i++)
    {
        std::shared_ptr<LogicalQueryPlanNode> logicalChild = logicalChildren[i];
        PhysNodePtr physicalChild = tw_createPhysicalTree(logicalChild, tileMode);  // recurse
        rootTileMode &= physicalChild->getPhysicalOperator()->getTileMode();
        physicalChildren[i] = physicalChild;
    }
    PhysNodePtr physicalRoot = n_createPhysicalNode(logicalRoot, rootTileMode);

    for (size_t i = 0; i < physicalChildren.size(); i++)
    {
        PhysNodePtr physicalChild = physicalChildren[i];
        physicalRoot->addChild(physicalChild);
    }

    physicalRoot->inferBoundaries();
    return physicalRoot;
}

/// NOTE: as written, descends only on the first child
///
/// in general converters should be inserted into the
/// tree in an earlier pass (compatibility)
/// and moved to their optimum location in a purely
/// optimizing pass.  Attempting to find the optimum
/// point at insertion time is not possible in more
/// general cases than are supported now.
///
static PhysNodePtr s_converterThinPoint(PhysNodePtr root, DistType dstDistType)
{
    double dataWidth = root->getDataWidth();
    PhysNodePtr candidate = root;

    while (root->outputFullChunks() &&         // TBD: document why this matters
           root->converterCommutesBefore(dstDistType) &&
           !root->needsSpecificDistribution()) // TBD: possible merge into converterCommutesBefore
                                               // TODO: eliminate neesSpecificDistribution()
                                               //       getReqType()== DistributionRequirement::SpecificAnyOrder
                                               //       is more useful in this context
    {
        SCIDB_ASSERT(root->getChildren().size()==1); // because of next line
        root = root->getChildren()[0];          // TODO: descends only on first child!
                                                //       review all callers, what guarantees this is only
                                                //       used on single chains?
        if (root->getDataWidth() < dataWidth)
        {
            dataWidth = root->getDataWidth();
            candidate = root;
        }
    }
    return candidate;
}

static void  s_propagateDistribution(PhysNodePtr cvtNode, PhysNodePtr root,
                                     const std::shared_ptr<Query>& query, const size_t rootDepth)
{
    SCIDB_ASSERT(cvtNode);
    SCIDB_ASSERT(root);

    auto rootOp = root->getPhysicalOperator();
    SCIDB_ASSERT(rootOp);
    auto cvtNodeOp = cvtNode->getPhysicalOperator();
    SCIDB_ASSERT(cvtNodeOp);
    LOG4CXX_TRACE(logger, "s_propagateDistribution("<<rootDepth<<"): begin, rootOp name is:" << rootOp->getPhysicalName());
    LOG4CXX_TRACE(logger, "s_propagateDistribution("<<rootDepth<<") begin, cvtNodeOp name is:" << cvtNodeOp->getPhysicalName());
    auto cvtNodeParent = cvtNode->getParent();
    SCIDB_ASSERT(cvtNodeParent);
    auto cvtNodeParentOp = cvtNodeParent->getPhysicalOperator();
    SCIDB_ASSERT(cvtNodeParentOp);
    LOG4CXX_TRACE(logger, "s_propagateDistribution("<<rootDepth<<") begin, cvtNodeParentOp name is:" << cvtNodeParentOp->getPhysicalName());
    logPlanTrace(logger, cvtNode, 0 , false);

    PhysicalQueryPlanPath descNodes;   // descendants of root on the path to cvtNode

    //
    // make a list of descendant nodes in the path that needs updating
    // this is necessary because a top down pass is needed prior
    // to traversing in this natural direction
    //
    for(PhysNodePtr node=cvtNode ; node != root; node = node->getParent()){

        auto op = node->getPhysicalOperator();
        SCIDB_ASSERT(op);

        LOG4CXX_TRACE(logger, "s_propagateDistribution("<<rootDepth<<"): pushing " << op->getPhysicalName() << " inheritedDistType " << op->getInheritedDistType()<<" to descNodes");
        descNodes.push_front(node);     // build list to have top-to-bottom order
    }

    // top-down, inherit DistType and bottom-up synthesize the result DistType
    LOG4CXX_TRACE(logger, "s_propagateDistribution("<<rootDepth<<"): calling inferDistType(descNodes.size()="<< descNodes.size());
    root->inferDistType(query, rootDepth, &descNodes);  // third arg restricts to this path
}

void HabilisOptimizer::insertReducerAtThinPoint(PhysNodePtr root, PhysNodePtr childNode,
                                                const RedistributeContext& newContext, size_t depth)
{
    SCIDB_ASSERT(root);
    SCIDB_ASSERT(childNode);
    SCIDB_ASSERT(childNode->getParent() == root);
    SCIDB_ASSERT(isReplicated(childNode->getDistribution().getDistType()));
    SCIDB_ASSERT(root->getPhysicalOperator());
    LOG4CXX_TRACE(logger, "[insertReducer] enter, root PhysicalName:" << root->getPhysicalOperator()->getPhysicalName());


    // find the insertion point
    DistType newDistType = newContext.getArrayDistribution()->getDistType();
    SCIDB_ASSERT(not isReplicated(newDistType)); // because we are reducing
    ASSERT_EXCEPTION(not childNode->getPhysicalOperator()->getSchema().isDataframe()
                     || isDataframe(newDistType),
                     "Reduced replicated dataframe must have dtDataframe distribution");
    PhysNodePtr descendant = s_converterThinPoint(childNode, newDistType);
    PhysNodePtr cvtParent = descendant->getParent();
    SCIDB_ASSERT(cvtParent);

    // make converter and insert it
    PhysNodePtr cvtNode = n_buildReducerNode(descendant, newContext.getArrayDistribution());
    n_addParentNode(descendant, cvtNode);
    SCIDB_ASSERT(cvtNode->getParent() == cvtParent);

    cvtParent->applyInheritanceToChild(cvtParent->findChild(cvtNode));    // give computed inheritance to cvtNode

    cvtNode->inferBoundaries();
    cvtNode->inferDistType(_query,depth+1);

    s_propagateDistribution(cvtNode, root, _query, depth);   // all the way back to input root, not just cvtParent
    LOG4CXX_TRACE(logger, "[insertReducer] inserted reducer ID:" << cvtNode->getPhysicalOperator()->getOperatorID().getValue());
}

void HabilisOptimizer::insertSgAtThinPoint(PhysNodePtr root, PhysNodePtr childNode,
                                           const RedistributeContext& newContext, size_t depth)
{
    SCIDB_ASSERT(root);
    SCIDB_ASSERT(childNode);
    SCIDB_ASSERT(childNode->getParent() == root);
    SCIDB_ASSERT(root->getPhysicalOperator());
    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") enter, root PhysicalName:" << root->getPhysicalOperator()->getPhysicalName());

    // find the insertion point
    DistType newDistType = newContext.getArrayDistribution()->getDistType();
    PhysNodePtr lower = s_converterThinPoint(childNode, newDistType);
    // lower is childNode or a place below it with less data (e.g. below an apply)
    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") enter, lower PhysicalName:" << lower->getPhysicalOperator()->getPhysicalName());

    PhysNodePtr higher = lower->getParent();
    SCIDB_ASSERT(higher);
    auto higherChildIndex = higher->findChild(lower);
    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") enter, higher PhysicalName:" << higher->getPhysicalOperator()->getPhysicalName());

    // make converter and insert it above lower
    PhysNodePtr sgNode = n_buildSgNode(lower->getPhysicalOperator()->getSchema(), newContext);
    n_addParentNode(lower, sgNode);  // above lower
    SCIDB_ASSERT(lower->getParent() == sgNode);
    SCIDB_ASSERT(sgNode->getParent() == higher);
    SCIDB_ASSERT(higher->findChild(sgNode) == higherChildIndex);
    SCIDB_ASSERT(sgNode->findChild(lower) == 0);

    bool isPull = higher->getPhysicalOperator()->acceptsPullSG(higher->findChild(sgNode));
    s_setSgDistribution(sgNode, newContext, /*strict*/true, isPull); // TODO: merge with n_buildSgNode

    higher->applyInheritanceToChild(higher->findChild(sgNode));

    sgNode->inferBoundaries();
    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") calling inferDistType on the sg node -- historical?");
    sgNode->inferDistType(_query,depth+1001); // TODO: can this be done as part of s_propagate instead?
                                               // TODO: this depth assumes the thin point is at the top,
                                               //       but that is not true in general, so adding 1000

    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") propagating from SG up to " << root->getPhysicalOperator()->getPhysicalName());
    s_propagateDistribution(sgNode, root, _query, depth);   // all the way back to local root, not just higher
                                               // here depth is root depth, so this is correct

    LOG4CXX_TRACE(logger, "insertSGAtThinPoint("<<depth<<") inserted sg done: isPull" << isPull << " ID:" << sgNode->getPhysicalOperator()->getOperatorID().getValue());
}

void HabilisOptimizer::insertDistConversion(PhysNodePtr root, PhysNodePtr childNode,
                                            const RedistributeContext& newContext, size_t depth)
{
    SCIDB_ASSERT(root);
    SCIDB_ASSERT(childNode);
    SCIDB_ASSERT(childNode->getParent() == root);
    SCIDB_ASSERT(root->getPhysicalOperator());

    LOG4CXX_TRACE(logger, __func__ << '(' << depth
                  << "), parentOp=" << root->getPhysicalOperator()->getLogicalName()
                  << ", targetDist=" << newContext.getDistType());

    // having trouble with summarize getting converters inserted in front of it, which makes it lie
    // stopping that right here, so we can get summarize to behave itself
    ASSERT_EXCEPTION(!root->isSummarizeNode(), "error: conversion introduced before summarize");

    if (newContext.isUndefined()) {
        // TODO: consder whether ASSERT_EXCEPTION() might be appropriate
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_NOT_IMPLEMENTED) << "not a meaningful distribution";
    }
    // does this duplicate the above?
    ArrayDistPtr arrDist = newContext.getArrayDistribution();
    if (isUndefined(arrDist->getDistType())) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_CANT_CREATE_SG_WITH_UNDEFINED_DISTRIBUTION);
    }

    RedistributeContext childDistContext = childNode->getDistribution();
    LOG4CXX_TRACE(logger, "insertDistConversion("<<depth<<") and child Op "
                  << childNode->getPhysicalOperator()->getLogicalName()
                  << " childPS " <<  childDistContext.getDistType());

    if (newContext.isSatisfiedBy(childDistContext) && childNode->outputFullChunks()) {
        LOG4CXX_TRACE(logger, "insertDistConversion("<<depth<<") nothing to do, returning. ");
        return ;        // nothing to do
    }

    // Incoming replicated dataframe can only be converted to dtDataframe.
    // Overwrite the newContext if necessary.
    ArrayDistPtr distp = newContext.getArrayDistribution();
    if (producesDataframe(*childNode)
        && isReplicated(childDistContext.getDistType())
        && not isDataframe(newContext.getDistType()))
    {
        LOG4CXX_DEBUG(logger, __func__ << ": Reducing replicated dataframe: override requested distribution '"
                      << distTypeToStr(newContext.getDistType()) << '\'');
        distp = createDistribution(dtDataframe);
    }
    RedistributeContext finalContext(distp, newContext.getArrayResidency());
    // Use only finalContext (and not newContext) beyond this point!

    bool needsSG = true;
    size_t childPosition = root->findChild(childNode);

    if(isReplicated(childDistContext.getDistType())) {  // pre-reduction required
        LOG4CXX_TRACE(logger, "insertDistConversion("<<depth<<") inserting reducer at thin point");

        // childNode has dtReplication distribution, have to insert a reducer
        // operator that will filter out chunks that do not belong to the target distribution.
        insertReducerAtThinPoint(root, childNode, finalContext, depth);
        // reducer is in

        // sg needed only when the prior residency was onto a subset of the current residency so that it will
        // need to be communicated to the full current residency
        if (childNode->getDistribution().getArrayResidency()->isEqual(finalContext.getArrayResidency())) {
            needsSG=false; // no need to SG to broaden the residency
        }
    }

    if (needsSG) {
        LOG4CXX_TRACE(logger, "insertDistConversion("<<depth<<") inserting sg at thin point");
        // if reducer was inserted and needsSG still true, need to use the reducer,
        // not the child node
        // we can do this if we saved the child number
        PhysNodePtr childNode = root->getChildren()[childPosition];
        insertSgAtThinPoint(root, childNode, finalContext, depth); // will not go beyond the reducer
    }
}

void HabilisOptimizer::tw_insertSgNodes(PhysNodePtr root, size_t depth)
{
    // root is parent of some children, not necessarily root of tree
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes] enter, root PhysicalName:" << root->getPhysicalOperator()->getPhysicalName());
    SCIDB_ASSERT(_root);

    auto rootOp=root->getPhysicalOperator();

    // recursion
    root->inferDistType(_query, depth);
    for (size_t i = 0; i < root->getChildren().size(); i ++) {
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes] recursing from " << rootOp->getPhysicalName() << "["<<i<<"]");
        tw_insertSgNodes(root->getChildren()[i], depth+1);
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes] recursion returning to " << rootOp->getPhysicalName() << "["<<i<<"]");
    }


    auto inheritedDistType = rootOp->getInheritedDistType();
    std::vector<DistType> childInheritances = rootOp->inferChildInheritances(inheritedDistType, root->getChildren().size());
    ASSERT_EXCEPTION(childInheritances.size() == root->getChildren().size(),
                     "error by " << rootOp->getPhysicalName() << ".inferChildInheritances");

    // post-recursion insertion
    if (root->isSgNode() == false) {
        auto rootOp = root->getPhysicalOperator();
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 0] candidate root PhysicalName:" << rootOp->getPhysicalName());
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 0] candidate numChildren: " << root->getChildren().size());
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 0] parent needSpecificDistribution: "
                              << root->needsSpecificDistribution());

        // if size == 0 , shoud not be set to need specific distribution
        size_t nChildren = root->getChildren().size();
        std::vector<uint8_t> isReplicatedInputOk = rootOp->isReplicatedInputOk(nChildren);
        if (nChildren == 1) {

            PhysNodePtr child = root->getChildren()[0];
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c] one-input candidate root PhysicalName: "
                                  << rootOp->getPhysicalName());
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c] its only child PhysicalName: " << child->getPhysicalOperator()->getPhysicalName());
            RedistributeContext cDist = child->getDistribution();
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c] cDist->getDistType(): " << cDist.getDistType());


            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c] default Distro ps is:" << childInheritances[0]);
            SCIDB_ASSERT(not isUninitialized(childInheritances[0]));

            PhysNodePtr sgCandidate = child;
            RedistributeContext newDist; // TODO: really necessary? vs Pair<ArrdistPtr,ArrRedisency>?

            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]    outputFullChunks() " << child->outputFullChunks() << " or dtLocalInstance");
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]       cDist.getDistType() == dtLocalInstance: " << (cDist.getDistType() == dtLocalInstance));
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]       child->estimatedMaxDataVolume(): " << child->estimatedMaxDataVolume());

            // TODO: historical special case exception, can it be eliminated?
            bool noInsertDueToSgNode = child->outputFullChunks() && child->isSgNode();
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]  SG child exception : "<< noInsertDueToSgNode);

            // small-data dtLocalInstance optimization
            // TODO: could the excess SG be removed in a following optimization pass instead?
            bool noInsertDueToPsLocalOptimization = child->outputFullChunks()
                && cDist.getDistType() == dtLocalInstance
                && child->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES;
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c] psLocal Optimization : "<< noInsertDueToPsLocalOptimization);

            if(root->needsSpecificDistribution()) { // getReqType()== DistributionRequirement::SpecificAnyOrder
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]       needs specific distribution,"
                                      << " root: "<<rootOp->getPhysicalName());

                RedistributeContext reqDistro = root->getDistributionRequirement().getSpecificRequirements()[0];
                if (not reqDistro.isSatisfiedBy(cDist) || not child->outputFullChunks()) {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]     sg required for specific match");
                    insertDistConversion(root, child, reqDistro, depth);
                } // else no insertion (could have insertDistConversion() optimize by skipping this case
            } else if (noInsertDueToSgNode ||
                       noInsertDueToPsLocalOptimization)  {
                // no insertion
            } else if (child->outputFullChunks() == false ||
                       (isReplicated(cDist.getDistType()) && !isReplicatedInputOk[0])) {
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes 1c]    add cvt from !fullChunks or !isReplicatedInputOk");
                auto targetDistType = childInheritances[0];
                if(!isParameterless(targetDistType)) {
                    targetDistType = defaultDistType();
                }
                auto targetDistContext = RedistributeContext(createDistribution(targetDistType), _defaultArrRes);
                SCIDB_ASSERT(!root->needsSpecificDistribution()); // this is handled as the first case
                insertDistConversion(root, child, targetDistContext, depth);
            }
            // no insertion
        } else if (nChildren == 2) {
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2] 2-input candidate " << rootOp->getOperatorName());
            PhysNodePtr lhs = root->getChildren()[0];
            PhysNodePtr rhs = root->getChildren()[1];
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2] LHS: " << lhs->getPhysicalOperator()->getOperatorName()
                                                 << " ps: " << lhs->getDistribution().getDistType());
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2] RHS: " << rhs->getPhysicalOperator()->getOperatorName()
                                                 << " ps: " << rhs->getDistribution().getDistType());

            if (root->needsSpecificDistribution()) { // getReqType()== DistributionRequirement::SpecificAnyOrder
                // this must take precedence over requiresInputSG(), else cross_join
                // does not get an SG inserted to replicate its right-hand side
                // TODO: enhance tw_insertSgNodes_genericMultiChild_oneLevel() to support EFFICIENT_SG optimization
                //       on its left side.
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2a] candidate " << rootOp->getOperatorName()
                                      << " needsSpecificDistribution");
                tw_insertSgNodes_genericMultiChild_oneLevel(root, depth, childInheritances);

            } else if(lhs->getDistribution().getDistType() == dtLocalInstance &&
               rhs->getDistribution().getDistType() == dtLocalInstance &&
               lhs->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES &&
               rhs->estimatedMaxDataVolume() < EFFICIENT_SG_MIN_DATA_BYTES ) {
               // redistribution would take longer than just doing it where-it-lies, so just do it where-it-lies
               LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2b]       not-required SGs omitted due to low maxDataVolume: "
                                     << lhs->estimatedMaxDataVolume()
                                     << " and "
                                     << rhs->estimatedMaxDataVolume());

            } else {
                // more special casing for the 2-child case
                bool isColocRequired = root->getDistributionRequirement().getReqType() ==
                                       DistributionRequirement::Collocated;
                // NOTE: it would be simpler if each 2-child that needed something other than co-location
                // specifically said what it needed.
                // In fact, let it be up to the parent operator to say what it needs if it cannot take
                // arbitrary co-located inputs.
                //
                auto defaultDist = createDistribution(defaultDistType()); // TODO: eliminate default partitionings
                RedistributeContext defaultDistro(defaultDist, _defaultArrRes);
                RedistributeContext lhsDistContext = lhs->getDistribution();
                RedistributeContext rhsDistContext = rhs->getDistribution();

                if (lhs->outputFullChunks() == false ||                // example: lhs is input() [e.g. load()]
                    lhsDistContext.getDistType() == dtLocalInstance || // example: lhs is list()
                    (isReplicated(lhsDistContext.getDistType()) && !isReplicatedInputOk[0])) {
                    // a converter on lhs is required (regardless of colocation consideration)
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c1] root opName " << rootOp->getPhysicalName());

                    RedistributeContext destDistContext = defaultDistro; // non-optimal fallback
                    if (isColocRequired &&                               // prefer rhs
                        not rhsDistContext.isUndefined() &&                                          // rhs allowed
                        (isReplicatedInputOk[0] || not isReplicated(rhsDistContext.getDistType()))) {// rhs allowed
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c1a] sending lhs to rhs (optimization)");
                        destDistContext = rhsDistContext;
                    } else {
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c1b] sending lhs to default");
                    }
                    insertDistConversion(root, lhs, destDistContext, depth);

                    lhs = root->getChildren()[0];            // update
                    lhsDistContext = lhs->getDistribution(); // update
                }

                if (rhs->outputFullChunks() == false ||                // example: rhs is input() [e.g. load()]
                    rhsDistContext.getDistType() == dtLocalInstance || // example: rhs is list()
                    (isReplicated(rhsDistContext.getDistType()) && !isReplicatedInputOk[1])) {
                    // a converter on rhs is required (regardless of colocation consideration)
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c2] root opName " << rootOp->getPhysicalName());

                    RedistributeContext destDistContext = defaultDistro; // non-optimal fallback
                    if (isColocRequired &&                               // prefer lhs
                        not lhsDistContext.isUndefined() &&                                          // lhs allowed
                        (isReplicatedInputOk[1] || not isReplicated(lhsDistContext.getDistType()))) {// rhs allowed
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c2a] sending rhs to lhs (optimization)");
                        destDistContext = lhsDistContext;
                    } else {
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c2b] sending rhs to default");
                    }
                    insertDistConversion(root, rhs, destDistContext, depth);

                    rhs = root->getChildren()[1];            // update
                    rhsDistContext = rhs->getDistribution(); // update
                }

                //FYI: the logic below can be short-circuited by forcing defaultDistro on both legs
                if(root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated) {
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3] collocated lhsDistContext=" << lhsDistContext);
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3] collocated rhsDistContext=" << rhsDistContext);

                    if (not lhsDistContext.isColocated(rhsDistContext)) {

                        auto lhsDims = lhs->getPhysicalOperator()->getSchema().getDimensions();
                        auto rhsDims = rhs->getPhysicalOperator()->getSchema().getDimensions();
                        bool canMoveLeftToRight = (not rhsDistContext.isUndefined() &&
                                                   rhsDistContext.getArrayDistribution()->valid(lhsDims));
                        bool canMoveRightToLeft = (not lhsDistContext.isUndefined() &&
                                                   lhsDistContext.getArrayDistribution()->valid(rhsDims));
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a] canMoveLeft=" << canMoveLeftToRight);
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a] canMoveRigth=" << canMoveRightToLeft);

                        // we find the thin points here to decide whether better to move
                        // right to left or left to right
                        PhysNodePtr leftCandidate  = s_converterThinPoint(lhs, rhsDistContext.getDistType());
                        PhysNodePtr rightCandidate = s_converterThinPoint(rhs, lhsDistContext.getDistType());

                        double leftDataWidth = leftCandidate->getDataWidth();
                        double rightDataWidth = rightCandidate->getDataWidth();

                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a] leftWidth=" << leftDataWidth);
                        LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a] rigthWidth=" << rightDataWidth);

                        if (leftDataWidth < rightDataWidth && canMoveLeftToRight) {   //move left to right
                            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a1] colocation, move left to right");
                            insertDistConversion(root, lhs, rhsDistContext, depth);
                        } else if (canMoveRightToLeft) {   //move right to left
                            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a2] colocation, mover right to left");
                            insertDistConversion(root, rhs, lhsDistContext, depth);
                        } else {   //move both left and right to a default distribution
                            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 2c3a3] colocation, move both to default");
                            insertDistConversion(root, lhs, defaultDistro, depth);
                            insertDistConversion(root, rhs, defaultDistro, depth);
                        }
                    }
                }
            }
        } else if (root->getChildren().size() > 2) {
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes 3>] candidate " << rootOp->getOperatorName()
                                  << " has 3 or more inputs");
            if(root->getDistributionRequirement().getReqType() != DistributionRequirement::Any &&
               root->getDistributionRequirement().getReqType() != DistributionRequirement::Collocated) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPTIMIZER, SCIDB_LE_DISTRIBUTION_SPECIFICATION_ERROR2);
            }
            tw_insertSgNodes_genericMultiChild_oneLevel(root, depth, childInheritances);
        }
    }

    root->inferDistType(_query, 0); // TODO: might not need to be redone if !sgInserted (no change to tree)
}

void HabilisOptimizer::tw_insertSgNodes_genericMultiChild_oneLevel(PhysNodePtr root, size_t depth,
                                                                   std::vector<DistType> childInheritances)
{
    bool needSpecific = root->getDistributionRequirement().getReqType() == DistributionRequirement::SpecificAnyOrder;
    bool needCollocation = root->getDistributionRequirement().getReqType() == DistributionRequirement::Collocated;

    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] depth: " << depth
                          << " rootName: " << root->getPhysicalOperator()->getOperatorName());
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] needColocation: " << needCollocation);
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] needSpecific: " << needSpecific);


    bool allChildrenSameInheritance=true;
    for(size_t i=0; i<childInheritances.size(); i++) {
        if (childInheritances[0] != childInheritances[i]) {
            allChildrenSameInheritance=false;
            break;
        }
    }

    auto rootOp=root->getPhysicalOperator();
    if (needCollocation) {
        LOG4CXX_ERROR(logger, "[tw_insertSgNodes_generic] rootName: " << rootOp->getOperatorName());
        LOG4CXX_ERROR(logger, "[tw_insertSgNodes_generic] requires collocated input, but inheritance differs")
        ASSERT_EXCEPTION( allChildrenSameInheritance, "when Colocation specified, any childInheritances given must be identical");
    }

    bool allChildOutputsMatchInheritance0=true;
    for(size_t c=0; c<root->getChildren().size(); c++) {
        PhysNodePtr childNode = root->getChildren()[c];
        RedistributeContext childDistContext = childNode->getDistribution();
        if (childDistContext.getArrayDistribution()->getDistType() != childInheritances[0]) {
            allChildOutputsMatchInheritance0=false;
            break;
        }
    }

    size_t nChildren = root->getChildren().size();
    std::vector<uint8_t> isReplicatedInputOk = rootOp->isReplicatedInputOk(nChildren);
    for (size_t c=0; c < nChildren; c++) {
        PhysNodePtr childNode = root->getChildren()[c];
        RedistributeContext childDistContext = childNode->getDistribution();
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] begin child["<<c<<"]");

        bool childHandled = false;
        if(needSpecific) { // must use getSpecificRequirements()[c]
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase1 needSpecific");
            RedistributeContext specDistContext = root->getDistributionRequirement().getSpecificRequirements()[c];
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] childDistContext: " << childDistContext);
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] specDistContext: " << specDistContext);
            if(not specDistContext.isSatisfiedBy(childDistContext)) {
                insertDistConversion(root, childNode, specDistContext, depth);
            } // else no insertion
            childHandled= true;  // if a conversion was possible, it was done (supposedly).
                                 // TODO: we have no way currently to validate that there was no misunderstanding
                                 // between a caller of insertDistConversion() that thought a conversion
                                 // "should" be done, but insertDistConversion() disagrees and returns.
                                 // The division of responsibility needs to be thought out and documented
                                 // and childHandled adjusted if necessary

        } else if (needCollocation) {
            // If needCollocation is true, then we have more than two children who must be collocated.
            // We require the all have the same inheritance when colocations is set
            // That seems a reasonable requirement on an operator that sets needColocation
            // (If not, we can cause this to go to the defaultDistType() case)
            LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase2 needColocation");
            ASSERT_EXCEPTION( allChildrenSameInheritance, "when Colocation specified, any childInheritances given must be identical");

            // do all children already have have that?
            if(allChildOutputsMatchInheritance0){
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase2 needColocation, childHandled true, early");
                childHandled= true;
            } else {
                // at least one of the inputs does not match the target, so conversion required
                // recall that in this block childInheritance[*] == childInheritance[0]
                // they all must match the "cannonical" [0] entry and the use of [0]
                // below is to be a reminder that we make the decision for all of them based on
                // a *single* value
                if(!isParameterless(childInheritances[0])) {
                    // if inheritance[0] requiresParameters(), we won't be able to do that
                    // and will have to fall through to defaultDistType() later below
                    SCIDB_ASSERT(!childHandled);
                } else {
                    // Move everyone to childInherited[0]
                    auto targetDist = createDistribution(childInheritances[0]);
                    RedistributeContext targetDistContext(targetDist, _defaultArrRes);
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase2 child PS: " << childDistContext.getDistType());
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase2 target PS: " << targetDistContext.getDistType());

                    if (not targetDistContext.isSatisfiedBy(childDistContext)) {
                        insertDistConversion(root, childNode, targetDistContext, depth);
                    }
                    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase2 needColocation, childHandled true, end");
                    childHandled= true;
                }
            }
        }

        LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] pre3 childHandled: " << childHandled);
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] pre3 outputFullChunks: " << childNode->outputFullChunks());
        LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] pre3 needColocation: " << needCollocation);

        // in the one-node case this is handled with needsSpecificDistribution
        if (!childHandled && (childNode->outputFullChunks()==false ||
                              needCollocation ||
                              (isReplicated(childDistContext.getDistType()) && !isReplicatedInputOk[c]))) {

            auto defaultDist = createDistribution(defaultDistType());
            RedistributeContext defaultDistContext(defaultDist, _defaultArrRes);
            if (not defaultDistContext.isSatisfiedBy(childDistContext)) {
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase3 child PS: " << childDistContext.getDistType());
                LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] subcase3 target PS: " << defaultDistContext.getDistType());
                logPlanTrace(childNode);

                // sgNode output is defaultDist
                insertDistConversion(root, childNode, defaultDistContext, depth);
            }
            childHandled= true;
        }

        LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic] end child["<<c<<"] loop");
    }
    LOG4CXX_TRACE(logger, "[tw_insertSgNodes_generic]END depth: " << depth
                          << " rootName: " << rootOp->getOperatorName());
}

void HabilisOptimizer::tw_updateSgStrictness(PhysNodePtr root)
{
if (root->isSgNode())
{
    LOG4CXX_TRACE(logger, "[tw_updateSgStrictness] this SG node's Op " << (void*)(root->getPhysicalOperator().get()));

    // NOTE: user-inserted SG will still have isStrict overriden
    // (sg() should not be inserted by hand, so that is ok)
    bool isStrict = true; //default
    PhysNodePtr child = root->getChildren()[0];
    PhysOpPtr inputOp = child->getPhysicalOperator();
    if (inputOp->canToggleStrict())
    {
        isStrict = inputOp->getIsStrict();
    }

// NOTE: user-inserted SG will still have isPull overriden
        // (sg() should not be inserted by hand, so that is ok)

        bool isPull = false;
        PhysNodePtr parent = root->getParent();
        if (parent) {
            PhysOpPtr outputOp = parent->getPhysicalOperator();
            // TODO: SDB-5720
            // while assuming the input would be input 0 works
            // at the moment (because all operators accepting PullSG
            // are unary ops like Input and Store),
            // SDB-5720 can't be closed until we implement a way to know
            // which input of the output is the one we are feeding.
            // tw_updateSgStrictness is not given that information at
            // this time, but it can be maintained and passed
            // in a follow-on checkin
            isPull = outputOp->acceptsPullSG(0);   // assuming input 0 is not generally correct
                                                   // though it is correct at the moment
        }

        const RedistributeContext distro = root->getDistribution();

        LOG4CXX_DEBUG(logger, "[tw_updateSgStrictness]: distrib ="<<distro.getDistType());
        LOG4CXX_DEBUG(logger, "[tw_updateSgStrictness], isStrict: "<<isStrict<<", isPull: " << isPull);
        s_setSgDistribution(root, distro, isStrict, /*pull*/isPull);

        const PhysOpPtr sgOp = root->getPhysicalOperator();
        SCIDB_ASSERT(sgOp->getParameters().size() >= 4);
        SCIDB_ASSERT(sgOp->getParameters()[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
    }
    for (size_t i =0; i<root->getChildren().size(); i++)
    {
        tw_updateSgStrictness(root->getChildren()[i]);
    }
}

bool
HabilisOptimizer::allowEndMaxChange(std::shared_ptr<PhysicalOperator> const & physOperator) const
{
    return physOperator && physOperator->getPhysicalName() == "physicalMerge";
}


bool
HabilisOptimizer::isRedimCollapsible(PhysNodePtr const& parent,
                                     ArrayDesc const& desired,
                                     ArrayDesc const& given,
                                     ArrayDesc& result) const
{
    ASSERT_EXCEPTION(!desired.isAutochunked(),
                     "Unexpected Autochunk schema specified as exemplar schema");

    LOG4CXX_TRACE(logger,  "HabilisOptimizer::isRedimCollapsible(): entry"
                  << " parent " << parent->getPhysicalOperator()->getPhysicalName()
                  << " has redimension/repart ("
                  << (given.isAutochunked() ? "" : "NOT")
                  << " autochunked) child");
    LOG4CXX_TRACE(logger,  "HabilisOptimizer::isRedimCollapsible(): desired schema: " << desired);
    LOG4CXX_TRACE(logger,  "HabilisOptimizer::isRedimCollapsible():   given schema: " << given);

    if (!given.isAutochunked()) {
        LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): GIVEN IS NOT AUTOCHUNKED, returning FALSE");
        return false;
    }

    Dimensions const& desiredDims = desired.getDimensions();
    Dimensions const& givenDims = given.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    ASSERT_EXCEPTION(desiredDims.size() == givenDims.size(),
                     "Desired redimension schema cannot alter the number of dimensions");
    // Start with the "final" resulting schema identical to the given
    // child schema
    result = given;
    Dimensions resultDimensions;
    resultDimensions.reserve(desiredDims.size());

    Dimensions::const_iterator itDesiredDim, itGivenDim;
    for(itDesiredDim = desiredDims.begin(), itGivenDim = givenDims.begin();
        itDesiredDim != desiredDims.end();
        ++itDesiredDim, ++itGivenDim)
    {
        DimensionDesc nextDim = *itGivenDim;
        //
        // startMin
        if (itDesiredDim->getStartMin() != itGivenDim->getStartMin()) {
            LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): NOT collapsible, startMin differs, return FALSE");
            return false;
        }
        //
        // endMax
        if (itDesiredDim->getEndMax() != itGivenDim->getEndMax()) {
            if (allowEndMaxChange(parent->getPhysicalOperator()) &&
                (itDesiredDim->getEndMax() < itGivenDim->getEndMax())) {
                LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): changing endMax from "
                              << itGivenDim->getEndMax() << " to " << itDesiredDim->getEndMax()
                              << " for " << itGivenDim->getBaseName());
                nextDim.setEndMax(itDesiredDim->getEndMax());
            }
            else {
                LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): NOT collapsible endMax differs, return FALSE");
                return false;
            }
        }
        //
        // chunkOverlap
        if (itDesiredDim->getChunkOverlap() != itGivenDim->getChunkOverlap()) {
            LOG4CXX_TRACE(logger,"HabilisOptimizer::isRedimCollapsible(): NOT collapsible chunkOverlap differs, return FALSE");
            return false;
        }
        //
        // chunkInterval
        if (itGivenDim->isAutochunked() && !itDesiredDim->isAutochunked()) {
            // Use (get|set)rawChunkInterval since the Interval could be
            // DimensionDesc::PASSTHRU.
            nextDim.setRawChunkInterval(itDesiredDim->getRawChunkInterval());
        }
        else if (itDesiredDim->getRawChunkInterval() != itGivenDim->getRawChunkInterval())
        {
            LOG4CXX_TRACE(logger,"HabilisOptimizer::isRedimCollapsible(): NOT collapsible Chunk Interval differs AND not autochunked, return FALSE");
            return false;
        }
        //
        // add this dimension to the result
        resultDimensions.push_back(nextDim);
    }

    result.setDimensions(resultDimensions);
    LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): IS collapsible, returning true");
    LOG4CXX_TRACE(logger, "HabilisOptimizer::isRedimCollapsible(): result " << result);
    return true;
}

/**
 *  Insert any needed redimension()/repart() operators into the physical plan.
 */
void HabilisOptimizer::tw_insertRedimensionOrRepartitionNodes(PhysNodePtr nodep, size_t depth)
{
    auto rootOp = nodep->getPhysicalOperator();
    const size_t N_CHILDREN = nodep->getChildren().size();
    LOG4CXX_TRACE(logger, "[tw_insertRedimensionOrRepartitionNodes] enter, depth " << depth
                         << " root PhysicalName:" << rootOp->getPhysicalName());

    // set up the initial inheritance prior to starting recursion

    nodep->inferDistType(_query, depth);  // TODO: rename last arg to preRecurseInferDistType or similar

    // Build input vectors for making the repart/no-repart decision.
    vector<ArrayDesc> schemas(nodep->getChildSchemas());
    SCIDB_ASSERT(schemas.size() == N_CHILDREN);
    vector<ArrayDesc const*> modifiedPtrs(N_CHILDREN, nullptr);

    // Work bottom-up: recurse on the children before doing the current node
    for (size_t nChild = 0; nChild < N_CHILDREN; ++nChild)
    {
        PhysNodePtr& child = nodep->getChildren()[nChild];

        // Handle children first.  Change the tree from bottom to top,
        // so that any inferences about boundaries and distributions
        // can percolate up.
        //
        LOG4CXX_TRACE(logger, "[tw_insertRedimensionOrRepartitionNodes] recursing from depth " << depth
                             << " root PhysicalName:" << rootOp->getPhysicalName());
        tw_insertRedimensionOrRepartitionNodes(child, depth+1);
        LOG4CXX_TRACE(logger, "[tw_insertRedimensionOrRepartitionNodes] recursion returned to depth " << depth
                             << " root PhysicalName:" << rootOp->getPhysicalName());

        // If any child is itself a repartition or redimension operator, it must
        // have been manually inserted, since we haven't altered our immediate
        // children yet.  Set the corresponding repartPtrs entry to be non-NULL.
        //
        if (child->isRepartNode() || child->isRedimensionNode())
        {
            modifiedPtrs[nChild] = &schemas[nChild];
            LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes(): depth: " << depth
                                  << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                                  << " child: " << nChild
                                  << " setting modifiedPtrs["<<nChild<<"]"
                                  << " = schemas["<<nChild<<"]");
        }
    }

    // Now for the current node.  Ask it: want to repartition any input schema?
    // [This handles the N_CHILDREN == 0 case that used to be an early exit path]
    auto nodeOp = nodep->getPhysicalOperator();
    nodeOp->requiresRedimensionOrRepartition(schemas, modifiedPtrs);

    if (modifiedPtrs.empty())  // requiresRedimOrRepart() can clear the list to indicate none
    {
        // Nothing to do here, but keep the inference chain going.
        nodep->inferBoundaries();
        nodep->inferDistType(_query,depth);
        LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes(): depth: " << depth
                             << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                             << " early return"
                             << " because modPtrs.empty(): " << modifiedPtrs.empty()
                             << " note: N_CHILDREN: " << N_CHILDREN);
        return;
    }

    // The modifiedPtrs vector describes how the nodep operator wants
    // each of its children repartitioned.
    //
    OperatorLibrary* oplib = OperatorLibrary::getInstance();
    Parameters params(1);
    size_t numSchemaChanges = 0;
    for (size_t nChild = 0; nChild < N_CHILDREN; ++nChild)
    {
        PhysNodePtr& child = nodep->getChildren()[nChild];
        LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes():"
                             << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                             << " CONSIDERING child nChild: " << nChild
                             << " name: " << child->getPhysicalOperator()->getPhysicalName()
                             << " <<<<<<");

        if (modifiedPtrs[nChild] == nullptr) {
            // This child's schema is fine, no change.
            LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes():"
                                 << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                                 << " nChild nullptr -- continue"
                                 << " <<<<<" );
            continue;
        }
        numSchemaChanges += 1;
        ArrayDesc const& childSchema = schemas[nChild];
        ArrayDesc const& desiredSchema = *modifiedPtrs[nChild];

        if (child->isRepartNode() || child->isRedimensionNode()) {
            LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes(): nChild: " << nChild
                                 << " REPART OR REDIM"
                                 << " <<<<<<");
            if (childSchema.sameShape(desiredSchema)) {
                // The desired schema and the child's schema are the same.
                //  -- no need to insert redimension
                LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes():"
                                     << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                                     << " nChild: " << nChild
                                     << " child.sameShape(desired) -- continue loop"
                                     << " <<<<<" );
                continue;
            }
            // Check if the given child and the desired repart/redimension from
            // requiresRedimensionOrRepartition can be collapsed into a single
            // operation.
            ArrayDesc resultSchema;
            if (isRedimCollapsible(nodep,
                                   desiredSchema,
                                   childSchema,
                                   resultSchema)) {
                LOG4CXX_ERROR(logger, "tw_insertRedimensionOrRepartitionNodes(): "
                                     << " op: " << nodep->getPhysicalOperator()->getPhysicalName()
                                     << " nChild: " << nChild
                                     << " cName: " << child->getPhysicalOperator()->getPhysicalName()
                                     << " desiredSchema: " << desiredSchema
                                     << " childSchema: " << childSchema
                                     << " isRedimCollapsible == okay ");

                // Create a new Physical Operator based upon the original child
                Parameters childParams =
                     child->getPhysicalOperator()->getParameters();

                ArrayDistPtr undefDist =
                    ArrayDistributionFactory::getInstance()->construct(dtUndefined,
                                                                       DEFAULT_REDUNDANCY);
                resultSchema.setDistribution(undefDist);
                // Wrap desired schema in Parameters object.
                childParams[0] = std::make_shared<OperatorParamSchema>(std::make_shared<ParsingContext>(),
                                                                       resultSchema);

                // Create phys. plan node for "op" based upon child
                PhysOpPtr op;
                if (child->isRedimensionNode()) {
                    op = oplib->createPhysicalOperator("redimension","PhysicalRedimension",
                                                       childParams, resultSchema);
                }
                else if (!childSchema.sameDimensionRanges(resultSchema)) {
                    assert(child->isRepartNode());
                    // Child is specified as a repart, but the operator requires
                    // a redimension because the dimension range needs to
                    // change.
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_BAD_EXPLICIT_REPART2)
                        << nodep->getPhysicalOperator()->getLogicalName();
                }
                else {
                    assert(child->isRepartNode());
                    op = oplib->createPhysicalOperator("repart", "physicalRepart",
                                                       childParams, resultSchema);
                }
                op->setQuery(_query);
                PhysNodePtr newNode(new PhysicalQueryPlanNode(op, false/*ddl*/, false/*tile*/));

                // and supplant child node with the new node.
                LOG4CXX_TRACE(logger, "tw_insertRedimensionOrRepartitionNodes():"
                                     << " supplanting child: " << child->getPhysicalOperator()->getPhysicalName()
                                     << " with op " << op->getPhysicalName());
                nodep->supplantChild(child,newNode);

                nodep->applyInheritanceToChild(nChild); // nChild is newNode

                // TODO: s_propagateDistribution needed?

                // Re-run inferences for the new child.
                newNode->inferBoundaries();
                newNode->inferDistType(_query,depth+1);
                continue;

            } // if (isRedimCollapsible(...))
            else {
                // The desired schema does not match the explicitly provided
                // redimension/repart AND the desired schema cannot be
                // "collapsed" with the given repart/redimension, so the query
                // is invalid.
                LOG4CXX_ERROR(logger, "tw_insertRedimensionOrRepartitionNodes(): "
                    << " repart or redim NOT COLLAPSIBLE "
                    << " parent: " << nodep->getPhysicalOperator()->getPhysicalName()
                    << " depth: " << depth
                    << " pdims: " << desiredSchema.getDimensions()
                    << " child[" << nChild << "]: " << child->getPhysicalOperator()->getPhysicalName()
                    << " cdims: " << childSchema.getDimensions()
                    << " <<<<<" );

                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_BAD_EXPLICIT_REPART)
                    << nodep->getPhysicalOperator()->getLogicalName()
                    << childSchema.getDimensions()
                    << desiredSchema.getDimensions();
            }
        } // if (child->isRepartNode() || child->isRedimensionNode())

        // Child is NOT a collapsible repart/redimension.
        // so we will insert a repart/redimension (newOp, newNode)
        ArrayDesc modifiedSchema = *modifiedPtrs[nChild];
        ArrayDistPtr undefDist =
            ArrayDistributionFactory::getInstance()->construct(dtUndefined,
                                                               DEFAULT_REDUNDANCY);
        modifiedSchema.setDistribution(undefDist);
        //
        // Wrap desired schema in Parameters object.
        params[0] = std::make_shared<OperatorParamSchema>(std::make_shared<ParsingContext>(),
                                                          modifiedSchema);
        //
        // If the ranges changed, we must redimension() rather than repart().
        PhysOpPtr newOp =
            schemas[nChild].sameDimensionRanges(*modifiedPtrs[nChild])
            ? oplib->createPhysicalOperator("repart", "physicalRepart", params, modifiedSchema)
            : oplib->createPhysicalOperator("redimension", "PhysicalRedimension", params, modifiedSchema);
        newOp->setQuery(_query);
        newOp->setControlCookie("repart"); // Puts redimension() in "repart mode".

        // Create phys. plan node for newOp [repart or redimension] and splice it in
        // between nodep and child[nChild].
        PhysNodePtr newNode(new PhysicalQueryPlanNode(newOp, false/*ddl*/, false/*tile*/));
        n_addParentNode(nodep->getChildren()[nChild], newNode);
        // nodep[nChild]->newNode->orginalChild

        nodep->applyInheritanceToChild(nChild); // nChild is newNode

        // TODO: s_propagateDistribution needed?

        // Re-run inferences for the new child.
        newNode->inferBoundaries();
        newNode->inferDistType(_query,depth+1);
    }

    // If requiresRedimensionOrRepartition() gave us a non-empty vector, it better have at
    // least one repartSchema/redimensionSchema for us.
    if (isDebug())
    {
        assert(numSchemaChanges > 0);
    }

    // Re-run inferences for this node and we are done.
    nodep->inferBoundaries();
    nodep->inferDistType(_query,depth);
}

void HabilisOptimizer::tw_insertChunkMaterializers(PhysNodePtr root, size_t depth)
{
    if ( root->hasParent() && root->getChildren().size() != 0)
    {
        PhysNodePtr parent = root->getParent();
        if (root->isSgNode() == false && root->getPhysicalOperator()->getTileMode() != parent->getPhysicalOperator()->getTileMode())
        {
            auto rootOp = root->getPhysicalOperator();
            LOG4CXX_TRACE(logger, "[tw_insertChunkMaterializers],"
                                 << " root PhysicalName: " << rootOp->getPhysicalName()
                                 << " tileMode " << root->getPhysicalOperator()->getTileMode());
            LOG4CXX_TRACE(logger, "[tw_insertChunkMaterializers],"
                                 << " parent PhysicalName: " << parent->getPhysicalOperator()->getPhysicalName()
                                 << " tileMode " << parent->getPhysicalOperator()->getTileMode());
            ArrayDesc const& schema = rootOp->getSchema();

            Value formatParameterValue;
            formatParameterValue.setInt64(MaterializedArray::RLEFormat);

            std::shared_ptr<Expression> formatParameterExpr = std::make_shared<Expression> ();
            formatParameterExpr->compileConstant(false, TID_INT64, formatParameterValue);

            Parameters params;
            params.push_back(std::shared_ptr<OperatorParam> (new OperatorParamPhysicalExpression(
                std::make_shared<ParsingContext>(), formatParameterExpr, CONSTANT)));

            PhysOpPtr materializeOp = OperatorLibrary::getInstance()->createPhysicalOperator(
                "_materialize", "impl_materialize", params, schema);
            materializeOp->setQuery(_query);
            PhysNodePtr materializeNode(new PhysicalQueryPlanNode(materializeOp, false, false));
            n_addParentNode(root, materializeNode);

            auto parentOp = parent->getPhysicalOperator();
            // regenerate inheritance on materializeOp
            parent->applyInheritanceToChild(parent->findChild(materializeNode));

            // TODO: s_propagateDistribution needed?

            materializeNode->inferBoundaries();
            materializeNode->inferDistType(_query,depth);
        }
    }

    for (size_t i =0; i < root->getChildren().size(); i++)
    {
        tw_insertChunkMaterializers(root->getChildren()[i], depth+1);
    }
}

std::shared_ptr<Optimizer> Optimizer::create()
{
    LOG4CXX_TRACE(logger, "Creating Habilis optimizer instance");
    return std::shared_ptr<Optimizer> (new HabilisOptimizer());
}

} // namespace
