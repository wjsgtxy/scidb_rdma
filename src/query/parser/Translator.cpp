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

/****************************************************************************/

#include "AST.h"

#include <array/ArrayName.h>
#include <array/Dense1MChunkEstimator.h>
#include <query/Aggregate.h>
#include <query/Expression.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/LogicalExpression.h>
#include <query/LogicalQueryPlan.h>
#include <query/OperatorLibrary.h>
#include <query/ParsingContext.h>
#include <query/Query.h>
#include <query/Serialize.h>
#include <query/UserQueryException.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Session.h>
#include <util/compression/Compressor.h>

#include <fstream>

/****************************************************************************/

#define QPROC(id,x)    USER_QUERY_EXCEPTION(SCIDB_SE_QPROC,   id,this->newParsingContext(x))
#define SYNTAX(id,x)   USER_QUERY_EXCEPTION(SCIDB_SE_SYNTAX,  id,this->newParsingContext(x))
#define INTERNAL(id,x) USER_QUERY_EXCEPTION(SCIDB_SE_INTERNAL,id,this->newParsingContext(x))

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.altranslator"));

/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

using std::map;
using std::numeric_limits;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

typedef std::shared_ptr<ParsingContext>                  ContextPtr;
typedef std::shared_ptr<LogicalExpression>               LEPtr;
typedef std::shared_ptr<LogicalOperator>                 LOPtr;
typedef std::shared_ptr<LogicalQueryPlanNode>            LQPNPtr;
typedef std::shared_ptr<OperatorParamArrayReference>     OPARPtr;
typedef std::shared_ptr<OperatorParamNamespaceReference> OPNSPtr;
typedef std::shared_ptr<OperatorParam>                   OPPtr;

namespace {

    using Sym = dfa::Symbol<OperatorParamPlaceholder>;
    Sym const SYM_EPSILON = Sym::makeEpsilon();
    Sym const SYM_PUSH = Sym::makePush();
    Sym const SYM_POP = Sym::makePop();

    // Dummies to make error reporting easier.
    OperatorParamPlaceholder const pushPlaceholder(PLACEHOLDER_PUSH);
    OperatorParamPlaceholder const popPlaceholder(PLACEHOLDER_POP);
    OperatorParamPlaceholder const epsilonPlaceholder(PLACEHOLDER_END_OF_VARIES);

    // This struct acts as glue to allow both ...ByCallback and
    // ...ByRecognizer code paths to call matchPositionalParam().
    struct RawPlaceholders
        : public std::vector<OperatorParamPlaceholder const *>
    {
        RawPlaceholders() = default;

        explicit RawPlaceholders(Placeholders const& placeholders)
        {
            reserve(placeholders.size());
            for (auto const& ph : placeholders) {
                push_back(ph.get());
            }
        }

        explicit RawPlaceholders(vector<Sym> const& symbols)
        {
            reserve(symbols.size());
            for (auto const& sym : symbols) {
                if (sym.isPure()) {
                    push_back(&sym.symbol());
                } else if (sym == SYM_PUSH) {
                    push_back(&pushPlaceholder);
                } else if (sym == SYM_POP) {
                    push_back(&popPlaceholder);
                } else if (sym == SYM_EPSILON) {
                    push_back(&epsilonPlaceholder);
                } else {
                    SCIDB_UNREACHABLE();
                }
            }
        }
    };

    /**
     * Stack of parameter lists.
     *
     * @details Used for constructing nested parameter lists.
     */
    class PlistStack
    {
        std::vector<Parameters> _stack { Parameters() };
        enum { MAX_SUBLIST_DEPTH = 4 };

    public:
        PlistStack() = default;

        /// Reference to top parameter list on the stack.
        Parameters& top() { return _stack.back(); }

        /// Depth of nested sublists.
        size_t size() const { return _stack.size(); }

        /// Append a parameter to the list on top of the stack.
        void push(Parameter& p) { _stack.back().push_back(p); }

        /// Push a new level of nesting, return true iff it worked.
        bool pushList()
        {
            if (size() < MAX_SUBLIST_DEPTH) {
                _stack.push_back(Parameters());
                return true;
            }
            return false;           // Caller should raise the error.
        }

        /// Pop the top list, turn it into a nested parameter of the new top.
        void popList(const std::shared_ptr<ParsingContext>& parsingCtx)
        {
            // PlistStack can never be empty, always at least one list.
            ASSERT_EXCEPTION(_stack.size() > 1,
                             "Internal error, too many popList() calls!");
            Parameters tmp;
            tmp.swap(_stack.back());
            _stack.pop_back();
            auto group = make_shared<OperatorParamNested>(parsingCtx);
            group->getParameters().swap(tmp);
            _stack.back().push_back(group);
        }
    };

} // anon namespace

/****************************************************************************/

class Translator
{
 public:
                              Translator(Factory& f,Log& l,const StringPtr& s,const QueryPtr& q)
                                  : _fac(f),_log(l),_txt(s),_qry(q),_isMulti(false)
                              {}

 public:
            /**
             * @param depthOperator depth of this node in the query plan tree.
             * @note SciDB allows up to Translator::MAX_DEPTH_OPERATOR nested levels.
             */
            LQPNPtr           AstToLogicalPlan          (const Node*, size_t depthOperator, bool = false);

            /**
             * @param depthExpression  depth of this expression. E.g. when parsing 1+2+3, the depth is 3.
             * @note SciDB allows up to Translator::MAX_DEPTH_EXPRESSION operands.
             */
            LEPtr             AstToLogicalExpression    (const Node* node, size_t depthExpression=0);

 private:   // @see AstToLogicalPlan() for depthOperator.
            LQPNPtr           passAFLOperator           (const Node*, size_t depthOperator);
            LQPNPtr           passSelectStatement       (const Node*, size_t depthOperator);
            LQPNPtr           passJoins                 (const Node*, size_t depthOperator);
            LQPNPtr           passIntoClause            (const Node*,LQPNPtr&, size_t depthOperator);
            LQPNPtr           passGeneralizedJoin       (const Node*, size_t depthOperator);
            LQPNPtr           passCrossJoin             (const Node*, size_t depthOperator);
            LQPNPtr           passJoinItem              (const Node*, size_t depthOperator);
            LQPNPtr           passImplicitScan          (const Node*, size_t depthOperator);
            LQPNPtr           passFilterClause          (const Node*,const LQPNPtr&, size_t depthOperator);
            LQPNPtr           passOrderByClause         (const Node*,const LQPNPtr&, size_t depthOperator);
            LQPNPtr           passThinClause            (const Node*, size_t depthOperator);
            LQPNPtr           passSelectList            (LQPNPtr&,const Node*,const Node*, size_t depthOperator);
            LQPNPtr           passUpdateStatement       (const Node*, size_t depthOperator);
            LQPNPtr           passInsertIntoStatement   (const Node*, size_t depthOperator);

 private:
    std::shared_ptr<OperatorParamAggregateCall>
                              passAggregateCall               (const Node*,const vector<ArrayDesc>&);
            Value             passConstantExpression          (const Node*,const TypeId&);
            int64_t           passIntegralExpression          (const Node*);
            void              passSchema                      (const Node*,ArrayDesc &,const string&);
            void              passReference                   (const Node*,chars&,chars&);
            bool              passGeneralizedJoinOnClause     (
                                                                vector<std::shared_ptr<OperatorParamReference> > &params,
                                                                const Node*,
                                                                size_t depthExpression);
            void              passDimensions                  (const Node*,Dimensions&,const string&,set<string>&);

 private:
            LQPNPtr           fitInput                        (LQPNPtr&,const ArrayDesc&);
            LQPNPtr           canonicalizeTypes               (const LQPNPtr&);
            LQPNPtr           appendOperator                  (const LQPNPtr&,const string&,const Parameters&,const ContextPtr&);
            string            placeholdersToString            (RawPlaceholders const&) const;
            string            astParamToString                (const Node*) const;
            bool              resolveParamAttributeReference  (const vector<ArrayDesc>&,std::shared_ptr<OperatorParamReference> &,bool = true);
            bool              resolveParamDimensionReference  (const vector<ArrayDesc>&,std::shared_ptr<OperatorParamReference> &,bool = true);
            bool              placeholdersVectorContainType   (const vector<std::shared_ptr<OperatorParamPlaceholder> > &,OperatorParamPlaceholderType );
            bool              checkAttribute                  (const vector<ArrayDesc>&,const string& aliasName,const string& attributeName,const ContextPtr&);
            bool              checkDimension                  (const vector<ArrayDesc>&,const string& aliasName,const string& dimensionName,const ContextPtr&);
            void              checkLogicalExpression          (const vector<ArrayDesc>&,const ArrayDesc &,const LEPtr &);
            void              checkParamExpression            (Parameter const& param,
                                                               LQPNPtr const& lqpn,
                                                               vector<ArrayDesc> const& inputSchemas);
            string            genUniqueObjectName             (const string&,unsigned int &,const vector<ArrayDesc> &,bool,cnodes  = cnodes());
            void              prohibitNesting                 (const LQPNPtr&);
            Node*             decomposeExpression             (const Node*,vector<Node*>&,vector<Node*>&,unsigned&,bool,const ArrayDesc&,const set<string>&,bool,bool&);
            int64_t           estimateChunkInterval           (cnodes);
            OPPtr             createBoolConstantParam         (bool b, ContextPtr const&);
            OPARPtr           createArrayReferenceParam       (const Node*,bool);
            OPNSPtr           createNamespaceReferenceParam   (const Node*,bool);
            OPPtr             createDistributionParam         (const Node*);
            OPPtr             createObjectNameParam           (const Node*,
                                                               const OperatorParamObjectName::QualifiedName&);
            bool              resolveDimension                (const vector<ArrayDesc> &inputSchemas,const string&,const string&,size_t&,size_t&,const ContextPtr&,bool);
            bool              astHasUngroupedReferences       (const Node*,const set<string>&)const;
            bool              astHasAggregates                (const Node*)                   const;
            void              matchParamsByRecognizer         (LOPtr op,
                                                               Node const* opAst,
                                                               PlistRecognizerCPtr& prp,
                                                               cnodes astParameters,
                                                               vector<ArrayDesc> &inputSchemas,
                                                               vector<LQPNPtr> &inputs,
                                                               size_t depthOperator);
            cnodes::iterator  matchListByRegex                (LOPtr op,
                                                               Node const* opAst,
                                                               PlistRecognizer::const_iterator& prit,
                                                               cnodes astParameters,
                                                               PlistStack& plistStack,
                                                               vector<ArrayDesc> &inputSchemas,
                                                               vector<LQPNPtr> &inputs,
                                                               size_t depthOperator);
            void              matchParamsByCallback           (LOPtr op,
                                                               Node const* opAst,
                                                               cnodes astParameters,
                                                               vector<ArrayDesc> &inputSchemas,
                                                               vector<LQPNPtr> &inputs,
                                                               size_t depthOperator);
            bool              matchPositionalParam            (LOPtr op,
                                                               const Node*,
                                                               const RawPlaceholders &,
                                                               vector<ArrayDesc> &inputSchemas,
                                                               vector<LQPNPtr> &inputs,
                                                               std::shared_ptr<OperatorParam> &param,
                                                               size_t depthOperator,
                                                               ssize_t* which = nullptr);

 private:                  // Expressions
            LEPtr             onNull              (const Node*);
            LEPtr             onReal              (const Node*);
            LEPtr             onInteger           (const Node*);
            LEPtr             onBoolean           (const Node*);
            LEPtr             onString            (const Node*);
            LEPtr             onQuestionMark      (const Node*);

            // @see AstToLogicalExpression().
            LEPtr             onScalarFunction    (const Node*, size_t depthExpression);

            LEPtr             onAttributeReference(const Node*);

 private:
            ContextPtr        newParsingContext(const Node*  n) const
                                  {return make_shared<ParsingContext>(_txt,n->getWhere());}
            ContextPtr        newParsingContext(const std::shared_ptr<OperatorParam>&  n) const
                                  {return n->getParsingContext();}
            ContextPtr        newParsingContext(const std::shared_ptr<AttributeReference>& n) const
                                  {return n->getParsingContext();}
            ContextPtr        newParsingContext(const ContextPtr& n) const
                                  {return n;}
            void              fail(const UserException& x) const
                                  {_log.fail(x);}

 private:
            void              splitReferenceArgNames(const Node* pn, Node*& ns, Node*& array) const;
            void              splitReferenceArgNames(const Node* pn, Node*& ns, Node*& array, Node*& var) const;
            void              splitReferenceArgNames(const Node* pn,
                                                     OperatorParamObjectName::QualifiedName& name) const;

            void              getAliasFromReference(const Node* pn, chars& name, chars& alias);
            void              checkDepthExpression(size_t depthExpression);
            void              checkDepthOperator(size_t depthOperator);
      const Node*             currentOperator() const   {return _opStack.empty() ? nullptr : _opStack.back();}

            /// Used to maintain _opStack, a stack of operator AST nodes.
            class OpTracker
            {
                Translator& _trans;
            public:
                OpTracker(Translator& t, const Node* ast)
                    : _trans(t)
                {
                    _trans._opStack.push_back(ast);
                }

                ~OpTracker()
                {
                    assert(!_trans._opStack.empty());
                    _trans._opStack.resize(_trans._opStack.size() - 1);
                }
            };

 private:                  // Representation
            Factory&          _fac;                      // Abstract factory
      const Log&              _log;                      // Abstract error log
      const StringPtr&        _txt;                      // The source text: yuk!
            QueryPtr    const _qry;                      // The query
          vector<const Node*> _opStack;                  // Operator stack
            bool              _isMulti;                  // Exp tree is mquery()
};

/****************************************************************************/

/**
 *  If the node 'ast' has child 'c' return the value of this string. Otherwise
 *  return the given default string.
 */
static chars getString(const Node* ast,child c,chars otherwise = "")
{
    assert(ast!=0 && otherwise!=0);                      // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getString();                           // ...return value
    }

    return otherwise;                                    // Return the default
}

/**
 *  If the node 'ast' has child 'c' return the value of this boolean. Otherwise
 *  return the given default value.
 */
static boolean getBoolean(const Node* ast,child c,boolean otherwise = false)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getBoolean();                          // ...return value
    }

    return otherwise;                                    // Return the default
}

/**
 *  If the node 'ast' has child 'c' return the value of this integer. Otherwise
 *  return the given default value.
 */
static integer getInteger(const Node* ast,child c,integer otherwise = 0)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p->getInteger();                          // ...return value
    }

    return otherwise;                                    // Return the default
}

const Node* getChildSafely(const Node* ast,child c)
{
    assert(ast != 0);                                    // Validate arguments

    if (const Node* p = ast->get(c))                     // Has child 'c'?
    {
        return p;                                        // ...so return it
    }

    return ast;                                          // Go with the parent
}

/**
 * Return true iff named operator takes schema parameters that may
 * have unspecified chunk intervals.
 */
static bool canAutochunk(const chars opName)
{
    // Keep this in alpha order.  These are registered logical operator names.
    static const char* permittedOps[] = {
        "Create_Array",         // AQL
        "build",
        "create_array",         // AFL
        "input",
        "redimension",
        "repart",
        NULL
    };

    // There is no need for bsearch(3) just yet, but still: alpha order!
    for (const char** cpp = &permittedOps[0]; *cpp; ++cpp) {
        if (!::strcmp(opName, *cpp)) {
            return true;
        }
    }
    return false;
}

/** Return true iff named operator is remove. */
static bool isRemoveOp(const chars opName)
{
    return !::strcmp(opName, "remove");
}

/// Return true when the named operator is mquery.
static bool isMultiQueryOp(const chars opName)
{
    return !::strcmp(opName, "mquery");
}

/****************************************************************************/
// temporary glue while introducing type 'variable' into the AST hiererchy
static Name* getApplicationArgName(const Node* n)
{
    assert(n!=0 && n->is(application));
    return n->get(applicationArgOperator)->get(variableArgName);
}
static chars getStringApplicationArgName(const Node* n)
{
    if (const Node* s=getApplicationArgName(n)) {
        return s->getString();
    }
    return "";
}
static chars getStringVariableName(const Node* n)
{
    if (n) {
        assert(n->is(variable));
        assert(n->get(variableArgName));
        return n->get(variableArgName)->getString();
    }
    return "";
}
/****************************************************************************/

/**
 * Get namespace and array name from an AST array reference node.
 *
 * The meaning of a qualified identifier "x.y.z" is context-sensitive.
 * Here caller vouches that we're looking at an array name (rather
 * than a dimension or attribute), so there should be at most one
 * qualifier (the namespace).
 */
void Translator::splitReferenceArgNames(const Node* pn, Node*& ns, Node*& array) const
{
    assert(pn && pn->is(reference) && pn->has(referenceArgNames));
    nodes names = pn->get(referenceArgNames)->getList();
    assert(!names.empty());

    ns = array = nullptr;

    switch (names.size()) {
    case 1:
        array = names[0];
        break;
    case 2:
        ns = names[0];
        array = names[1];
        break;
    default:
        fail(SYNTAX(SCIDB_LE_NAMESPACE_NESTING, pn));
        SCIDB_UNREACHABLE();
    }
}

/**
 * Get namespace, array, and variable name from an AST variable reference node.
 *
 * The meaning of a qualified identifier "x.y.z" is context-sensitive.
 * Here caller vouches that we're looking at an attribute or dimension
 * (rather than an array), so there should be at most two qualifiers
 * (namespace, array) and the var name.
 */
void Translator::splitReferenceArgNames(const Node* pn, Node*& ns, Node*& array, Node*& var) const
{
    assert(pn && pn->is(reference) && pn->has(referenceArgNames));
    nodes names = pn->get(referenceArgNames)->getList();
    assert(!names.empty());

    ns = array = var = nullptr;

    switch (names.size()) {
    case 1:
        var = names[0];
        break;
    case 2:
        array = names[0];
        var = names[1];
        break;
    case 3:
        ns = names[0];
        array = names[1];
        var = names[2];
        break;
    default:
        fail(SYNTAX(SCIDB_LE_NAMESPACE_NESTING, pn));
        SCIDB_UNREACHABLE();
    }
}

/**
 * Get the components of a qualified name from an AST variable reference node.
 *
 * The meaning of a qualified identifier "x.y.z" is context-sensitive.
 */
void Translator::splitReferenceArgNames(const Node* pn,
                                        OperatorParamObjectName::QualifiedName& name) const
{
    assert(pn && pn->is(reference) && pn->has(referenceArgNames));
    nodes qualifiedNameNodes = pn->get(referenceArgNames)->getList();
    for (size_t i = 0; i < qualifiedNameNodes.size(); ++i) {
        auto* nameNode = qualifiedNameNodes[i];
        auto nameStr = getStringVariableName(nameNode);
        name.push_back(nameStr);
    }
}

/**
 * In "foo(...) as X ... X.Y", get alias X and name Y from X.Y reference node.
 */
void Translator::getAliasFromReference(const Node* n, chars& alias, chars& name)
{
    Node *al = nullptr;
    Node *nm = nullptr;
    splitReferenceArgNames(n, al, nm);
    assert(nm);
    alias = getStringVariableName(al);
    name = getStringVariableName(nm);
}

LQPNPtr Translator::AstToLogicalPlan(const Node* ast, size_t depthOperator, bool canonicalize)
{
    checkDepthOperator(depthOperator);

    LQPNPtr r;

    switch (ast->getType())
    {
        default:          SCIDB_UNREACHABLE();  // jab:
        case application: r = passAFLOperator(ast, depthOperator);          break;
        case reference:   r = passImplicitScan(ast, depthOperator);         break;
        case insertArray: r = passInsertIntoStatement(ast, depthOperator);  break;
        case selectArray: r = passSelectStatement(ast, depthOperator);      break;
        case updateArray: r = passUpdateStatement(ast, depthOperator);      break;
    }

    if (canonicalize && r->isSelective())
    {
        r = canonicalizeTypes(r);
    }

    return r;
}

int64_t Translator::estimateChunkInterval(cnodes nodes)
{
    Dense1MChunkEstimator estimator;
    for (auto d : nodes)
    {
        assert(d->is(dimension));

        Node const *n = d->get(dimensionArgChunkInterval);
        if (n && !n->is(questionMark))
        {
            estimator.add(passIntegralExpression(n));
        }
        else
        {
            // In this context, "autochunked" means "unspecified."
            estimator.add(DimensionDesc::AUTOCHUNKED);
        }
    }

    int64_t r = estimator.estimate();

    // If all the dimensions were specified (r == 0), there is no need
    // to guess: the "known size" is the number we want.
    return r ? r : estimator.getKnownSize();
}

Value Translator::passConstantExpression(const Node* ast,const TypeId& targetType)
{
    Expression pExpr;
    try
    {
        pExpr.compile(AstToLogicalExpression(ast),false,targetType);
    }
    catch (const Exception& e)
    {
        //Rewrite possible type exceptions to get error location in query
        if (SCIDB_SE_TYPE == e.getShortErrorCode())
        {
            fail(SYNTAX(SCIDB_LE_TYPE_EXPECTED,ast) << targetType);
        }
        e.raise();
    }

    if (!pExpr.isConstant())
    {
        fail(SYNTAX(SCIDB_LE_CONSTANT_EXPRESSION_EXPECTED,ast));
    }

    return pExpr.evaluate();
}

int64_t Translator::passIntegralExpression(const Node* ast)
{
    return passConstantExpression(ast,TID_INT64).getInt64();
}

void Translator::passDimensions(const Node* ast,Dimensions& dimensions,const string& arrayName,set<string> &usedNames)
{
    dimensions.reserve(ast->getSize());                  // Reserve memory

    for (const Node* d : ast->getList())                 // For each dimension
    {
        assert(d->is(dimension));                        // ...is a dimension

        string  const nm = d->get(dimensionArgName)->getString();
        int64_t       lo = 0;                            // ...lower bound
        int64_t       hi = CoordinateBounds::getMax();   // ...upper bound
        int64_t       ci = 0;                            // ...chunk interval
        int64_t       co = 0;                            // ...chunk overlap

        if (!usedNames.insert(nm).second)                // ...already used?
        {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_DIMENSION_NAME,d->get(dimensionArgName)) << nm);
        }

        bool autochunkAllowed = false;
        const Node* nOp = currentOperator();
        chars opName = "(no operator context)";
        if (nOp)
        {
            opName = getStringApplicationArgName(nOp);
            autochunkAllowed = canAutochunk(opName);
        }

        const Node* nLo = d->get(dimensionArgLoBound);
        if (nLo && !nLo->is(questionMark))
        {
            assert(!nLo->is(asterisk));                  // BNF forbids it
            lo = passIntegralExpression(nLo);
        }

        const Node* nHi = d->get(dimensionArgHiBound);
        if (nHi)
        {
            if (!nHi->is(asterisk) && !nHi->is(questionMark))
            {
                hi = passIntegralExpression(nHi);
            }
        }

        const Node* nCi = d->get(dimensionArgChunkInterval);
        if (nCi)
        {
            if (nCi->is(questionMark))
            {
                ci = estimateChunkInterval(ast->getList());
            }
            else if (nCi->is(asterisk))
            {
                if (autochunkAllowed)
                {
                    ci = DimensionDesc::AUTOCHUNKED;
                }
                else
                {
                    fail(SYNTAX(SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED, nCi) << opName);
                }
            }
            else
            {
                ci = passIntegralExpression(nCi);
            }
        }

        const Node* nCo = d->get(dimensionArgChunkOverlap);
        if (nCo)
        {
            co = passIntegralExpression(nCo);
        }

        if (!nCi)
        {
            ci = autochunkAllowed
                ? static_cast<int64_t>(DimensionDesc::AUTOCHUNKED)
                : estimateChunkInterval(ast->getList());
        }

        /*
         *  Validate the dimension specification.
         */

        if (lo == CoordinateBounds::getMax() && hi != CoordinateBounds::getMax())
        {
            fail(SYNTAX(SCIDB_LE_DIMENSION_START_CANT_BE_UNBOUNDED,getChildSafely(d,dimensionArgLoBound)));
        }

        if (lo<CoordinateBounds::getMin() || CoordinateBounds::getMax()<lo)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY,getChildSafely(d,dimensionArgLoBound))
                 << CoordinateBounds::getMin() << CoordinateBounds::getMax());
        }

        if (hi<CoordinateBounds::getMin() || CoordinateBounds::getMax()<hi)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY,getChildSafely(d,dimensionArgHiBound))
                 << CoordinateBounds::getMin() << CoordinateBounds::getMax());
        }

        if (hi<lo && hi+1!=lo)
        {
            fail(SYNTAX(SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW,getChildSafely(d,dimensionArgHiBound)));
        }

        if (ci <= 0 && ci != DimensionDesc::AUTOCHUNKED)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_CHUNK_SIZE,getChildSafely(d,dimensionArgChunkInterval))
                 << numeric_limits<int64_t>::max());
        }

        if (co < 0)
        {
            fail(SYNTAX(SCIDB_LE_INCORRECT_OVERLAP_SIZE,getChildSafely(d,dimensionArgChunkOverlap))
                 << numeric_limits<int64_t>::max());
        }

        if (co > ci && ci != DimensionDesc::AUTOCHUNKED)
        {
            fail(SYNTAX(SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK,getChildSafely(d,dimensionArgChunkOverlap)));
        }

        dimensions.push_back(DimensionDesc(nm,lo,hi,ci,co));
        LOG4CXX_DEBUG(logger, "passDimensions: " << dimensions);
    }
}

void Translator::passSchema(const Node* ast,ArrayDesc& schema,const string& arrayName)
{
    const Node* list    = ast->get(schemaArgAttributes);
    Attributes attributes;
    set<string> usedNames;

    for (const Node* attNode : list->getList())
    {
        assert(attNode->is(attribute));

        const string attName            = getString (attNode,attributeArgName);
        const string attTypeName        = getString (attNode,attributeArgTypeName);
        const bool   attTypeNullable    = getBoolean(attNode,attributeArgIsNullable);
        const string attCompressorName  = getString (attNode,attributeArgCompressorName,"no compression");

        Value defaultValue;

        if (usedNames.find(attName) != usedNames.end())
        {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,attNode->get(attributeArgName)) << attName);
        }

        usedNames.insert(attName);

        int16_t const attFlags = attTypeNullable ? AttributeDesc::IS_NULLABLE : 0;

        try
        {
            const Type attType(TypeLibrary::getType(attTypeName));

            if (attType == TypeLibrary::getType(TID_INDICATOR))
            {
                fail(SYNTAX(SCIDB_LE_EXPLICIT_EMPTY_FLAG_NOT_ALLOWED,attNode->get(attributeArgTypeName)));
            }

            string serializedDefaultValueExpr;

            if (const Node* defaultValueNode = attNode->get(attributeArgDefaultValue))
            {
                if (astHasUngroupedReferences(defaultValueNode, set<string>()))
                {
                    fail(SYNTAX(SCIDB_LE_REFERENCE_NOT_ALLOWED_IN_DEFAULT,defaultValueNode));
                }

                Expression e;
                e.compile(AstToLogicalExpression(defaultValueNode), false, attTypeName);
                serializedDefaultValueExpr = serializePhysicalExpression(e);
                defaultValue = e.evaluate();

                if (defaultValue.isNull() && !attTypeNullable)
                {
                    fail(SYNTAX(SCIDB_LE_NULL_IN_NON_NULLABLE,attNode->get(attributeArgName)) << attName);
                }
            }
            else
            {
                defaultValue = Value(attType);

                if (attTypeNullable)
                {
                    defaultValue.setNull();
                }
                else
                {
                    defaultValue = TypeLibrary::getDefaultValue(attType.typeId());
                }
            }

            const Compressor *attCompressor = NULL;
            for (Compressor* c : CompressorFactory::getInstance().getCompressors())
            {
                if (c && c->getName() == attCompressorName)
                {
                    attCompressor = c;
                    break;
                }
            }

            if (attCompressor == NULL)
            {
                fail(SYNTAX(SCIDB_LE_COMPRESSOR_DOESNT_EXIST,attNode->get(attributeArgCompressorName))
                     << attCompressorName);
            }

            attributes.push_back(
                AttributeDesc(
                    attName,
                    attType.typeId(),
                    attFlags,
                    attCompressor->getType(),
                    set<string>(),
                    safe_static_cast<int16_t>(
                        getInteger(attNode,attributeArgReserve,
                                   Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE))),
                    &defaultValue,
                    serializedDefaultValueExpr));
        }
        catch (SystemException& e)
        {
            if (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED)
            {
                fail(CONV_TO_USER_QUERY_EXCEPTION(e, newParsingContext(attNode->get(attributeArgTypeName))));
            }

            e.raise();
        }
    }

    // In 14.3, all arrays became emptyable
    //FIXME: Which compressor for empty indicator attribute?
    attributes.addEmptyTagAttribute();

    SCIDB_ASSERT(_qry);

    ArrayDistPtr dist;
    Dimensions dimensions;
    Node const* dimsAst = ast->get(schemaArgDimensions);
    if (dimsAst) {
        passDimensions(ast->get(schemaArgDimensions),dimensions,arrayName,usedNames);
        dist = createDistribution(defaultDistType());
    } else {
        // No dimension list, so it must be a dataframe!
        if (usedNames.find(DFD_INST) != usedNames.end()) {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_DIMENSION_NAME, ast) << DFD_INST);
        }
        if (usedNames.find(DFD_SEQ) != usedNames.end()) {
            fail(SYNTAX(SCIDB_LE_DUPLICATE_DIMENSION_NAME, ast) << DFD_SEQ);
        }
        dimensions = makeDataframeDimensions();
        dist = createDistribution(dtDataframe);
    }

    std::string namespaceName = _qry->getNamespaceName();
    schema = ArrayDesc(0,0,0,namespaceName,arrayName,attributes,dimensions,dist,
                       _qry->getDefaultArrayResidency());
}

LQPNPtr Translator::passAFLOperator(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);
    OpTracker ot(*this, ast);

    const chars opName   = getStringApplicationArgName(ast);
    cnodes astParameters = ast->getList(applicationArgOperands);
    const string opAlias = getString(ast,applicationArgAlias);

    LOPtr               op;
    OperatorLibrary&    opLib = *OperatorLibrary::getInstance();
    try
    {
        op = opLib.createLogicalOperator(opName,opAlias);
    }
    catch (Exception &e)
    {
        if (e.getLongErrorCode() == SCIDB_LE_LOGICAL_OP_DOESNT_EXIST)
        {
            fail(CONV_TO_USER_QUERY_EXCEPTION(e,newParsingContext(ast)));
        }

        e.raise();
    }

    bool multi = isMultiQueryOp(opName);
    if (multi) {
        if (depthOperator == 0) {
            _isMulti = multi;
        }
        else {
            fail(QPROC(SCIDB_LE_DDL_CANT_BE_NESTED,newParsingContext(ast)));
        }
    }

    vector<LQPNPtr >    opInputs;
    vector<ArrayDesc>   inputSchemas;

    // Match positional parameters.
    auto pr = opLib.getPlistRecognizer(opName);
    if (pr) {
        // Regular expression based matching of parameter types.
        matchParamsByRecognizer(op, ast, pr,
                                astParameters,
                                inputSchemas,
                                opInputs,
                                depthOperator);
    } else if (!astParameters.empty()) {
        fail(SYNTAX(SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT, ast->get(applicationArgOperands))
             << op->getLogicalName() << astParameters.size());
    }

    if (opInputs.size() &&
        op->getProperties().ddl &&
        !_isMulti)
    {
        fail(INTERNAL(SCIDB_LE_DDL_SHOULDNT_HAVE_INPUTS,ast));
    }

    LQPNPtr result = make_shared<LogicalQueryPlanNode>(newParsingContext(ast), op, opInputs);

    // We can't check expression before getting all operator parameters. So here we already have
    // all params and can get operator output schema. On each iteration we checking references in
    // all non-constant expressions. If ok, we trying to compile expression to check type compatibility.

    PlistVisitor checkExpr =
        [this, &result, &inputSchemas] (Parameter& p, PlistWhere const&, string const&)
        {
            if (p->getParamType() == PARAM_LOGICAL_EXPRESSION) {
                this->checkParamExpression(p, result, inputSchemas);
            }
        };
    op->visitParameters(checkExpr);

    return result;
}

namespace {

// The AST parameter list might include nested parameter lists.
// This helper class lets us walk all the parameter ASTs in
// depth-first order.

class AstParamIterator {
    struct AstContext {
        cnodes  astList;
        cnodes::const_iterator  currAst;
    };
    vector<AstContext> _astStack;
public:
    explicit AstParamIterator(cnodes params);
    Node const* curr();
    Node const* next();
    cnodes::const_iterator position() const;
    size_t depth() const { return _astStack.size(); }
};

AstParamIterator::AstParamIterator(cnodes params)
{
    _astStack.push_back({params, params.begin()});
}

Node const* AstParamIterator::curr()
{
    assert(!_astStack.empty()); // We never pop the last one.

    if (_astStack.size() == 1 &&
        _astStack.back().currAst == _astStack.back().astList.end())
    {
        return nullptr;
    }

    return *_astStack.back().currAst;
}

Node const* AstParamIterator::next()
{
    assert(!_astStack.empty()); // We never pop the last one.

    if (_astStack.size() == 1 &&
        _astStack.back().currAst == _astStack.back().astList.end())
    {
        return nullptr;
    }

    Node const* result = *_astStack.back().currAst;
    ++_astStack.back().currAst;

    // All nested parameter lists end with a popNestedArgs node.
    assert((_astStack.back().currAst != _astStack.back().astList.end())
           || result->is(popNestedArgs) // was a nested list
           || _astStack.size() == 1);   // was top-level list (so end() is OK)

    if (result->is(nestedArgs)) {
        _astStack.push_back({result->getList(), result->getList().begin()});
    } else if (result->is(popNestedArgs)) {
        assert(_astStack.size() > 1);
        _astStack.pop_back();
    }

    return result;
}

cnodes::const_iterator AstParamIterator::position() const
{
    assert(_astStack.size() == 1); // Not too useful otherwise.
    return _astStack.back().currAst;
}

string expectedToString(vector<Sym> const& expected)
{
    stringstream ss;
    ss << "one of: ";
    bool first = true;
    for (auto const& sym : expected) {
        if (first) {
            first = false;
        } else {
            ss << ", ";
        }
        if (sym == SYM_EPSILON) {
            ss << "end of parameter list";
        } else if (sym == SYM_PUSH) {
            ss << "nested parameter list";
        } else if (sym == SYM_POP) {
            ss << "end of nested parameters";
        } else {
            ss << sym.symbol().toDescription();
        }
    }
    return ss.str();
}

} // anon namespace


void Translator::matchParamsByRecognizer(LOPtr op,
                                         Node const* opAst,
                                         PlistRecognizerCPtr& prp,
                                         cnodes astParameters,
                                         vector<ArrayDesc> &inputSchemas,
                                         vector<LQPNPtr> &opInputs,
                                         size_t opDepth)
{
    assert(op->getParameters().empty());

    // The AFL grammar assures us that positonal arguments, if any,
    // will come before keyword arguments.  There may not be any
    // positionals... but if the operator specified a regex for
    // positional arguments, we have to make sure that the regex
    // accepts "no positional arguments" as valid.

    auto astIter = astParameters.begin();
    Node const* ast = *astIter;
    PlistRecognizer::const_iterator prit = prp->find("");
    if (prit != prp->end()) {
        // This operator has a regex for its positional parameters.
        if (astIter == astParameters.end() || ast->is(kwarg)) {
            // No positionals supplied.  Is that OK according to the regex?
            auto cursor = prit->second.dfa->getCursor();
            if (!cursor.isAccepting()) {
                auto err = (astIter == astParameters.end()) ? opAst : ast;
                fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2, err)
                     << op->getLogicalName());
            }
        }
    }
    else if (astIter != astParameters.end()) {
        if (!ast->is(kwarg)) {
            // Positionals supplied, but no regex for them!
            fail(SYNTAX(SCIDB_LE_UNEXPECTED_OPERATOR_ARGUMENT, ast->get(applicationArgOperands))
                 << op->getLogicalName() << astParameters.size());
        }
    }
    else {
        // No parameters and none required.  That was easy.
        return;
    }

    assert(astIter == astParameters.begin());
    while (astIter != astParameters.end()) {
        ast = *astIter;
        PlistStack plistStack;

        if (!ast->is(kwarg)) {
            // Positionals.  Work on the top-level astParameters list.

            assert(astIter == astParameters.begin());
            assert(prit != prp->end()); // Already looking at positionals regex.

            astIter = matchListByRegex(op, opAst,
                                       prit,
                                       astParameters,
                                       plistStack,
                                       inputSchemas,
                                       opInputs,
                                       opDepth);

            assert(astIter != astParameters.begin());
            assert(plistStack.size() == 1);
            op->getParameters().swap(plistStack.top());
        } else {
            // Keyword!  Single argument is the "list" to work on.

            prit = prp->find(ast->get(kwargArgKeyword)->getString());
            if (prit == prp->end()) {
                fail(SYNTAX(SCIDB_LE_UNRECOGNIZED_KEYWORD_PARAM, ast->get(kwargArgKeyword))
                     << ast->get(kwargArgKeyword)->getString());
            }

            Node const* arg = ast->get(kwargArgArgument);
            cnodes kwargs(1, (Node**)&arg); // Ugh.
            (void) matchListByRegex(op, opAst,
                                    prit,
                                    kwargs,
                                    plistStack,
                                    inputSchemas,
                                    opInputs,
                                    opDepth);
            ++astIter;

            assert(plistStack.size() == 1);
            if (plistStack.top().size() != 1) {
                // The operator's author messed up, keyword regexes
                // must match either a single placeholder or a nested
                // one, never a sequence.  (In fact, this could only
                // happen if the AFL grammar changed, since as of this
                // writing the grammar simply cannot produce a
                // sequence for a keyword argument.)
                fail(INTERNAL(SCIDB_LE_BAD_KEYWORD_REGEX, ast)
                     << op->getLogicalName()
                     << prit->first
                     << prit->second.regex->asRegex());
            }
            op->addParameter(prit->first.c_str(), plistStack.top().front());
        }
    }
}


cnodes::iterator
Translator::matchListByRegex(LOPtr op,
                             Node const* opAst,
                             PlistRecognizer::const_iterator& prit,
                             cnodes astParams,
                             PlistStack& plistStack,
                             vector<ArrayDesc> &inputSchemas,
                             vector<LQPNPtr> &opInputs,
                             size_t opDepth)
{
    // We have a deterministic finite-state automaton (DFA) that gives
    // a complete description of what a valid parameter list looks
    // like.  Walk the DFA to get the next set of expected parameter
    // placeholders at each step.

    auto cursor = prit->second.dfa->getCursor();
    AstParamIterator astCursor(astParams);
    Node const* ast;

    for (; (ast = astCursor.curr()) != nullptr; astCursor.next()) {

        if (ast->is(kwarg)) {
            // We were working on matching the positionals, so first
            // kwarg found means we're done with that.
            assert(prit->first.empty());    // AFL grammar says no nested kwargs
            assert(astCursor.depth() == 1); // Popped all AST sublists
            break;
        }

        vector<Sym> expected = cursor.getExpected();
        if (expected.empty()) {
            size_t n = plistStack.top().size();
            fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT, ast)
                 << op->getLogicalName() << n << n + 1);
        }

        if (ast->is(nestedArgs)) {
            auto p = find(expected.begin(), expected.end(), SYM_PUSH);
            if (p == expected.end()) {
                fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENT2, ast) << expectedToString(expected));
            }
            if (!plistStack.pushList()) {
                fail(SYNTAX(SCIDB_LE_ARGUMENT_NESTING, ast));
            }
            bool moved = cursor.move(SYM_PUSH);
            SCIDB_ASSERT(moved);
        }
        else if (ast->is(popNestedArgs)) {
            auto p = find(expected.begin(), expected.end(), SYM_POP);
            if (p == expected.end()) {
                fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2, ast) << op->getLogicalName());
            }
            plistStack.popList(newParsingContext(ast));
            bool moved = cursor.move(SYM_POP);
            SCIDB_ASSERT(moved);
        }
        else {
            std::shared_ptr<OperatorParam> opParam;
            ssize_t which = -1;
            if (matchPositionalParam(op, ast, RawPlaceholders(expected),
                                     inputSchemas, opInputs, opParam, opDepth, &which))
            {
                plistStack.top().push_back(opParam);
            }
            else if (astCursor.depth() > 1) {
                fail(SYNTAX(SCIDB_LE_NESTED_INPUT_ARRAY, ast));
            }
            assert(which > -1 && static_cast<size_t>(which) < expected.size());
            if (expected[which].symbol().getPlaceholderType() == PLACEHOLDER_INPUT) {
                // Input arrays can only appear in top-level parameter
                // lists (because the logical op uses separate vectors
                // for inputs and parameters).
                if (plistStack.size() > 1) {
                    fail(SYNTAX(SCIDB_LE_INPUTS_MUST_BE_BEFORE_PARAMS, ast)
                         << op->getLogicalName());
                }
            }
            bool moved = cursor.move(expected[which]);
            SCIDB_ASSERT(moved);
        }
    }

    // Should be done, not buried in the middle of a nested list!
    ASSERT_EXCEPTION(plistStack.size() == 1 && astCursor.depth() == 1,
                     "Incomplete parameter translation for operator '"
                     << op->getLogicalName()
                     << "', keyword '" << prit->first
                     << "', regex: " << prit->second.regex->asRegex());

    assert(!ast || ast->is(kwarg));
    assert(!cursor.isError());

    if (!cursor.isAccepting()) {
        if (prit->first.empty()) {
            fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_REGEX, opAst)
                 << op->getLogicalName()
                 << prit->second.regex->asRegex());
        } else {
            fail(SYNTAX(SCIDB_LE_WRONG_OPERATOR_KWARGS_REGEX, opAst)
                 << op->getLogicalName()
                 << prit->first
                 << prit->second.regex->asRegex());
        }
    }

    return ast ? astCursor.position() : astParams.end();
}


void Translator::checkParamExpression(Parameter const& param,
                                      LQPNPtr const& lqpn,
                                      vector<ArrayDesc> const& inputSchemas)
{
    assert(PARAM_LOGICAL_EXPRESSION == param->getParamType());

    const std::shared_ptr<OperatorParamLogicalExpression>& paramLE =
        (const std::shared_ptr<OperatorParamLogicalExpression>&) param;

    if (paramLE->isConstant())
        return;

    const ArrayDesc& outputSchema = lqpn->inferTypes(_qry); // NODE

    const LEPtr& lExpr = paramLE->getExpression();
    checkLogicalExpression(inputSchemas, outputSchema, lExpr);

    std::shared_ptr<Expression> pExpr = make_shared<Expression>();

    try
    {
        pExpr->compile(lExpr, false, paramLE->getExpectedType().typeId(), inputSchemas, outputSchema);
    }
    catch (const Exception &e)
    {
        if (e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR)
        {
            fail(SYNTAX(SCIDB_LE_PARAMETER_TYPE_ERROR,param)
                 << paramLE->getExpectedType().name() << pExpr->getType());
        }
        e.raise();
    }
}


OPPtr Translator::createBoolConstantParam(bool b, ContextPtr const& ctx)
{
    Type const& type = TypeLibrary::getType(TID_BOOL);
    Value v(type);
    v.setBool(b);
    auto lexp = std::make_shared<Constant>(ctx, v, TID_BOOL);
    return std::make_shared<OperatorParamLogicalExpression>(ctx, lexp, type, /*isConstant:*/ true);
}


std::shared_ptr<OperatorParamArrayReference>
Translator::createArrayReferenceParam(const Node *arrayReferenceAST, bool inputSchema)
{
    ArrayDesc schema;
    string dimName;

    string arrayName, namespaceName;
    Node *ns, *ary;
    splitReferenceArgNames(arrayReferenceAST, ns, ary);
    assert(ary);
    arrayName = getStringVariableName(ary);
    namespaceName = getStringVariableName(ns);
    assert(arrayName != "");
    assert(isNameUnversioned(arrayName));

    assert(_qry);
    SystemCatalog& sysCat = *SystemCatalog::getInstance();

    // An X.Y name in the AST will yield Y as arrayName above, and X as namespaceName here.
    if (namespaceName.empty())
    {
        namespaceName = _qry->getNamespaceName();
    }

    NamespaceDesc nsDesc(namespaceName);
    if(!sysCat.findNamespaceByName(nsDesc))
    {
        fail(QPROC(SCIDB_LE_CANNOT_RESOLVE_NAMESPACE,arrayReferenceAST) << namespaceName);
    }

    std::string qualifiedArrayName = makeQualifiedArrayName(namespaceName, arrayName);

    if (!inputSchema)
    {
        assert(!arrayReferenceAST->get(referenceArgVersion));
        return std::make_shared<OperatorParamArrayReference>(
            newParsingContext(arrayReferenceAST),"",qualifiedArrayName,inputSchema,0);
    }

    // When removing an array, we don't care if UDTs it uses are missing.
    bool removing = false;
    if (const Node* nOp = currentOperator())
    {
        removing = isRemoveOp(getStringApplicationArgName(nOp));
    }

    SystemCatalog::GetArrayDescArgs args;
    args.result = &schema;
    args.nsName = namespaceName;
    args.arrayName = arrayName;
    args.catalogVersion = _qry->getCatalogVersion(namespaceName, arrayName);
    args.throwIfNotFound = false;
    args.ignoreOrphanAttrs = removing;
    bool found = sysCat.getArrayDesc(args);
    if (!found)
    {
        fail(QPROC(SCIDB_LE_ARRAY_DOESNT_EXIST, ary) << qualifiedArrayName);
    }

    VersionID version = LAST_VERSION;

    if (arrayReferenceAST->get(referenceArgVersion))
    {
        if (arrayReferenceAST->get(referenceArgVersion)->is(asterisk))
        {
            return std::make_shared<OperatorParamArrayReference>(
                newParsingContext(arrayReferenceAST), "",
                qualifiedArrayName, inputSchema, ALL_VERSIONS);
        }
        else
        {
            LEPtr lExpr = AstToLogicalExpression(arrayReferenceAST->get(referenceArgVersion));
            Expression pExpr;
            pExpr.compile(lExpr, false);
            const Value &value = pExpr.evaluate();

            if (pExpr.getType() == TID_INT64)
            {
                version = std::max(int64_t(0),value.get<int64_t>());
            }
            else if (pExpr.getType() == TID_UINT64)
            {
                version = value.get<uint64_t>();
            }
            else if (pExpr.getType() == TID_DATETIME)
            {
                version = sysCat.lookupVersionByTimestamp(schema.getId(), value.getDateTime());
            }
            else
            {
                fail(SYNTAX(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,arrayReferenceAST->get(referenceArgVersion))
                    << qualifiedArrayName);
            }
        }
    }

    SystemCatalog::GetArrayDescArgs args2;
    args2.result = &schema;
    args2.nsName = namespaceName;
    args2.arrayName = arrayName;
    args2.catalogVersion = _qry->getCatalogVersion(namespaceName, arrayName);
    args2.versionId = version;
    args2.throwIfNotFound = false;
    args2.ignoreOrphanAttrs = removing;
    found = sysCat.getArrayDesc(args2);
    if (version == 0 || (!found && version != LAST_VERSION) )
    {
        fail(QPROC(SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST,arrayReferenceAST->get(referenceArgVersion))
            <<  qualifiedArrayName);
    }
    SCIDB_ASSERT(!isNameVersioned(arrayName));

    return  std::make_shared<OperatorParamArrayReference>(
        newParsingContext(arrayReferenceAST), "",qualifiedArrayName, inputSchema, version);
}

OPNSPtr
Translator::createNamespaceReferenceParam(const Node *nsRefAst, bool inputSchema)
{
    // To the grammar (and in the AST) it's an array reference, but it
    // must be an unqualified name.
    string nsName;
    Node *left, *right;
    splitReferenceArgNames(nsRefAst, left, right);
    assert(right);
    nsName = getStringVariableName(right);
    if (left) {
        fail(SYNTAX(SCIDB_LE_NAMESPACE_NESTING, nsRefAst));
    }
    assert(isNameUnversioned(nsName));

    assert(_qry);
    SystemCatalog& sysCat = *SystemCatalog::getInstance();

    assert(!nsName.empty());
    NamespaceDesc nsDesc(nsName);
    bool found = sysCat.findNamespaceByName(nsDesc);
    if (!found && inputSchema)
    {
        // Input namespaces have to exist.
        fail(QPROC(SCIDB_LE_CANNOT_RESOLVE_NAMESPACE, nsRefAst) << nsName);
    }

    return std::make_shared<OperatorParamNamespaceReference>(newParsingContext(nsRefAst), nsDesc);
}

OPPtr
Translator::createDistributionParam(const Node* distributionNode)
{
    SCIDB_ASSERT(distributionNode);
    std::string what = distributionNode->getString();
    SCIDB_ASSERT(!what.empty());

    DistType distType {dtUninitialized};
    try {
        if (what == "default") {
            distType = defaultDistType();
        }
        else {
            distType = toDistType(what);
        }
    }
    catch(...) {
        fail(SYNTAX(SCIDB_LE_INVALID_DISTRIBUTION, distributionNode) << what);
    }

    if(!isUserSpecifiable(distType)) {
        fail(SYNTAX(SCIDB_LE_INVALID_DISTRIBUTION, distributionNode) << what);
    }

    return std::make_shared<OperatorParamDistribution>(
        newParsingContext(distributionNode), createDistribution(distType));
}

OPPtr
Translator::createObjectNameParam(const Node* nameNode,
                                  const OperatorParamObjectName::QualifiedName& objectName)
{
    // Admittedly this method doesn't do much beyond creating the parameter
    // object, but it keeps with the pattern of the Translator.
    SCIDB_ASSERT(nameNode);
    return std::make_shared<OperatorParamObjectName>(
        newParsingContext(nameNode), objectName);
}

bool Translator::matchPositionalParam(LOPtr op,
                                      const Node* ast,
                                      const RawPlaceholders &placeholders,
                                      vector<ArrayDesc> &inputSchemas,
                                      vector<LQPNPtr > &inputs,
                                      std::shared_ptr<OperatorParam> &param,
                                      size_t depthOperator,
                                      ssize_t* which)
{
    LOG4CXX_TRACE(logger, "matchPositionalParam(): start");

    checkDepthOperator(depthOperator);
    SystemCatalog& sysCat = *SystemCatalog::getInstance();

    int matched = 0;

    // Some callers want to know which placeholder was matched.
    if (which) {
        *which = -1;
    }

    //Each operator parameter from AST can match several placeholders. We trying to catch best one.
    ssize_t phNumber = 0;
    for (auto const& placeholder : placeholders)
    {
        switch (placeholder->getPlaceholderType())
        {
            case PLACEHOLDER_INPUT:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): INPUT");
                LQPNPtr input;
                //This input is implicit scan.
                if (ast->is(reference))
                {
                    if (ast->has(referenceArgOrder))
                    {
                        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
                    }

                    input = passImplicitScan(ast, depthOperator+1);
                }
                //This input is result of other operator, so go deeper in tree and translate this operator.
                else if (ast->is(application) || ast->is(selectArray))
                {
                    input = AstToLogicalPlan(ast, depthOperator+1);
                    prohibitNesting(input);
                }
                else
                {
                    break;
                }

                inputSchemas.push_back(input->inferTypes(_qry));  // NODE
                inputs.push_back(input);

                //Inputs can not be mixed in vary parameters. Return and go to next parameter.
                if (which) {
                    *which = phNumber;
                }
                return false;
            }

            case PLACEHOLDER_ARRAY_NAME:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): ARRAY_NAME");
                if (ast->is(reference))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (ast->has(referenceArgVersion))
                    {
                        if (!placeholder->allowVersions())
                            fail(SYNTAX(SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,ast->get(referenceArgVersion)));
                    }

                    if (ast->has(referenceArgOrder))
                    {
                        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
                    }

                    param = createArrayReferenceParam(ast, placeholder->isInputSchema());

                    matched |= PLACEHOLDER_ARRAY_NAME;
                    if (which) {
                        *which = phNumber;
                    }
                }

                break;
            }

            case PLACEHOLDER_ATTRIBUTE_NAME:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): ATTRIBUTE_NAME");
                if (ast->is(reference) && !ast->has(referenceArgVersion))
                {
                    Node *ns, *ary, *var;
                    splitReferenceArgNames(ast, ns, ary, var);
                    if (ns) {
                        // XXX TODO Support it.
                        fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
                           "Namespace qualifier not supported in this context");
                    }

                    std::shared_ptr<OperatorParamAttributeReference> opParam =
                        std::make_shared<OperatorParamAttributeReference>(
                            newParsingContext(ast),
                            getStringVariableName(ary),
                            getStringVariableName(var),
                            placeholder->isInputSchema());

                    opParam->setSortAscent(getInteger(ast,referenceArgOrder,ascending) == ascending);

                    //Trying resolve attribute in input schema
                    if (placeholder->isInputSchema())
                    {
                        if (!resolveParamAttributeReference(
                            inputSchemas,
                            (std::shared_ptr<OperatorParamReference>&)opParam, false))
                        {
                            break;
                        }
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                        || (!placeholder->isInputSchema() &&  (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                            if (which) {
                                *which = phNumber;
                            }
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_ATTRIBUTE_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                    if (which) {
                        *which = phNumber;
                    }
                }
                break;
            }

            case PLACEHOLDER_DIMENSION_NAME:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): DIMENSION_NAME");
                if (ast->is(reference) && !ast->has(referenceArgVersion))
                {
                    Node *ns, *ary, *var;
                    splitReferenceArgNames(ast, ns, ary, var);
                    if (ns) {
                        // XXX TODO Support it.
                        fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
                           "Namespace qualifier not supported in this context");
                    }

                    std::shared_ptr<OperatorParamReference> opParam =
                        std::make_shared<OperatorParamDimensionReference>(
                            newParsingContext(ast),
                            getStringVariableName(ary),
                            getStringVariableName(var),
                            placeholder->isInputSchema());

                    //Trying resolve dimension in input schema
                    if (placeholder->isInputSchema())
                    {
                        if (!resolveParamDimensionReference(inputSchemas, opParam, false))
                            break;
                    }

                    //Check if something already matched in overloaded parameter
                    if (matched)
                    {
                        //If current parameter from input schema and some previous matched was from
                        //input schema, or current parameter from output schema and some previous
                        //matched was from output schema, so we can't resolve such ambigouty
                        if ((placeholder->isInputSchema() && !(matched & PLACEHOLDER_OUTPUT_FLAG))
                             || (!placeholder->isInputSchema() && (matched & PLACEHOLDER_OUTPUT_FLAG)))
                        {
                            fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                        }

                        //If some matched, but in different schema, prefer input schema parameter over
                        //output schema
                        if (placeholder->isInputSchema())
                        {
                            param = opParam;
                        }
                    }
                    else
                    {
                        param = opParam;
                    }

                    //Raise flags in any case, even parameter was not catched
                    matched |= PLACEHOLDER_DIMENSION_NAME;
                    matched |= placeholder->isInputSchema() ? 0 : PLACEHOLDER_OUTPUT_FLAG;
                    if (which) {
                        *which = phNumber;
                    }
                }
                break;
            }

            case PLACEHOLDER_CONSTANT:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): PLACEHOLDER_CONSTANT");
                if (ast->is(application)
                 || ast->is(cnull)
                 || ast->is(creal)
                 || ast->is(cstring)
                 || ast->is(cboolean)
                 || ast->is(cinteger))
                {
                    LEPtr lExpr;
                    std::shared_ptr<Expression> pExpr = std::make_shared<Expression>();

                    try
                    {
                       lExpr = AstToLogicalExpression(ast);
                       pExpr->compile(lExpr, false, placeholder->getRequiredType().typeId());
                    }
                    catch (const Exception &e)
                    {
                        if (e.getLongErrorCode() == SCIDB_LE_REF_NOT_FOUND
                            || e.getLongErrorCode() == SCIDB_LE_TYPE_CONVERSION_ERROR
                            || e.getLongErrorCode() == SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION)
                        {
                            break;
                        }
                    }

                    //Ignore non-constant expressions to avoid conflicts with
                    //PLACEHOLDER_EXPRESSION and PLACEHOLDER_AGGREGATE_CALL
                    if (!pExpr->isConstant())
                    {
                        break;
                    }

                    if (matched && !(matched & PLACEHOLDER_CONSTANT))
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (!(matched & PLACEHOLDER_CONSTANT))
                    {
                        param = std::make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                                    lExpr, placeholder->getRequiredType(), true);
                    }
                    else
                    {
                        pExpr->compile(lExpr, false);

                        if (pExpr->getType() == placeholder->getRequiredType().typeId())
                        {
                            param = std::make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                                        lExpr, placeholder->getRequiredType(), true);
                        }
                    }

                    matched |= PLACEHOLDER_CONSTANT;
                    if (which) {
                        *which = phNumber;
                    }
                }
                break;
            }

            case PLACEHOLDER_EXPRESSION:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): PLACEHOLDER_EXPRESSION");
                if (ast->is(application)
                 || ast->is(reference)
                 || ast->is(cnull)
                 || ast->is(creal)
                 || ast->is(cstring)
                 || ast->is(cboolean)
                 || ast->is(cinteger))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    LEPtr lExpr = AstToLogicalExpression(ast);

                    // We are not checking expression now, because we can't get output schema. Checking
                    // will be done after getting all operator parameters.
                    param = std::make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
                            lExpr, placeholder->getRequiredType(), false);

                    matched |= PLACEHOLDER_EXPRESSION;
                    if (which) {
                        *which = phNumber;
                    }
                }

                break;
            }

            case PLACEHOLDER_SCHEMA:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): PLACEHOLDER_SCHEMA");
                if (ast->is(schema))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    ArrayDesc schema;

                    passSchema(ast, schema, "");

                    // If the input schema parameter for this operator contains
                    // reserved names but this operator doesn't allow reserved names
                    // in that parameter, then throw an error.
                    if (!op->getProperties().schemaReservedNames &&
                        schema.hasReservedNames()) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                               SCIDB_LE_RESERVED_NAMES_IN_SCHEMA)
                            << op->getLogicalName();
                    }

                    param = std::make_shared<OperatorParamSchema>(newParsingContext(ast), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                    if (which) {
                        *which = phNumber;
                    }
                }
                else
                if (ast->is(reference))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    assert(_qry);

                    // An X.Y name in the AST will yield Y as arrayName and X as namespaceName.
                    Node *ns, *ary;
                    splitReferenceArgNames(ast, ns, ary);
                    string arrayName = getStringVariableName(ary);
                    string namespaceName = getStringVariableName(ns);
                    if (namespaceName.empty())
                    {
                        namespaceName = _qry->getNamespaceName();
                    }

                    NamespaceDesc nsDesc(namespaceName);
                    if (!sysCat.findNamespaceByName(nsDesc))
                    {
                        fail(QPROC(SCIDB_LE_CANNOT_RESOLVE_NAMESPACE,ast) << namespaceName);
                    }

                    ArrayDesc schema;
                    SystemCatalog::GetArrayDescArgs args;
                    args.result = &schema;
                    args.nsName = namespaceName;
                    args.arrayName = arrayName;
                    args.catalogVersion = _qry->getCatalogVersion(namespaceName, arrayName);
                    args.throwIfNotFound = false;
                    const bool nameUnversioned = isNameUnversioned(arrayName);
                    if (nameUnversioned) {
                        args.versionId = LAST_VERSION;
                    }
                    bool found = sysCat.getArrayDesc(args);
                    if (!found)
                    {
                        fail(SYNTAX(SCIDB_LE_ARRAY_DOESNT_EXIST, ast)
                            << makeQualifiedArrayName(namespaceName, arrayName));
                    }

                    if (nameUnversioned) {
                        // This case produces an array name with the version
                        // baked-in, so remove the version.
                        schema.setName(arrayName);
                    }

                    param = std::make_shared<OperatorParamSchema>(newParsingContext(ast), schema);

                    matched |= PLACEHOLDER_SCHEMA;
                    if (which) {
                        *which = phNumber;
                    }
                }
                break;
            }

            case PLACEHOLDER_AGGREGATE_CALL:
            {
                LOG4CXX_TRACE(logger, "matchPositionalParam(): PLACEHOLDER_AGGREGATE_CALL");
                if (ast->is(application))
                {
                    if (matched)
                    {
                        if ((matched & ~PLACEHOLDER_CONSTANT) == 0)
                        {
                            // Already matched as a constant (for example, the "application" was
                            // unary minus), so it cannot possibly be an aggregate call.
                            break;
                        }

                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    param = passAggregateCall(ast, inputSchemas);
                    matched |= PLACEHOLDER_AGGREGATE_CALL;
                    if (which) {
                        *which = phNumber;
                    }
                }
                break;
            }

            case PLACEHOLDER_NS_NAME:
            {
                if (ast->is(reference))
                {
                    if (matched)
                    {
                        fail(INTERNAL( SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER,ast));
                    }

                    if (ast->has(referenceArgVersion))
                    {
                        fail(SYNTAX(SCIDB_LE_CANT_ACCESS_ARRAY_VERSION,ast->get(referenceArgVersion)));
                    }

                    if (ast->has(referenceArgOrder))
                    {
                        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
                    }

                    param = createNamespaceReferenceParam(ast, placeholder->isInputSchema());

                    matched |= PLACEHOLDER_NS_NAME;
                    if (which) {
                        *which = phNumber;
                    }
                }

                break;
            }

            case PLACEHOLDER_PUSH:
            case PLACEHOLDER_POP:
            case PLACEHOLDER_END_OF_VARIES:
                LOG4CXX_TRACE(logger, "matchPositionalParam(): PLACEHOLDER_END_OF_VARIES");
                break;

            case PLACEHOLDER_DISTRIBUTION:
            {
                Node* distAstNode = nullptr;
                if (ast->is(distribution)) {
                    distAstNode = ast->get(distributionType);
                }
                else if (ast->is(reference)) {
                    distAstNode = ast->getList(listArg0)[0]->get(distributionType);
                }
                else {
                    SCIDB_UNREACHABLE();
                }

                if (distAstNode) {
                    if (matched) {
                        fail(INTERNAL(SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER, ast));
                    }

                    param = createDistributionParam(distAstNode);
                    matched |= PLACEHOLDER_DISTRIBUTION;
                    if (which) {
                        *which = phNumber;
                    }
                }

                break;
            }

            case PLACEHOLDER_OBJECT_NAME:
            {
                OperatorParamObjectName::QualifiedName objectName;

                if (ast->is(reference)) {
                    splitReferenceArgNames(ast, objectName);
                }
                // else ast is some other type that requires repackaging
                // into the 'objectName' variable.

                if (!objectName.empty()) {
                    if (matched) {
                        fail(INTERNAL(SCIDB_LE_AMBIGUOUS_OPERATOR_PARAMETER, ast));
                    }

                    matched |= PLACEHOLDER_OBJECT_NAME;

                    if (which) {
                        *which = phNumber;
                    }

                    // Defer to the operator any work related to parsing the name.
                    param = createObjectNameParam(ast, objectName);
                }

                break;
            }

            default:    SCIDB_UNREACHABLE();
        }

        ++phNumber;
    }

    assert(bool(matched) == bool(param));
    if (!matched)
    {
        fail(QPROC(SCIDB_LE_WRONG_OPERATOR_ARGUMENT2,ast) << placeholdersToString(placeholders));
    }

    return true;
}

string Translator::placeholdersToString(RawPlaceholders const& placeholders) const
{
    bool first = true;
    ostringstream ss;
    for (auto const& placeholder : placeholders)
    {
        if (!first)
            ss << " or ";
        first = false;
        ss << placeholder->toDescription();
    }

    return ss.str();
}

string Translator::astParamToString(const Node* ast) const
{
    switch (ast->getType())
    {
        default: SCIDB_UNREACHABLE();
        case application:       return "operator (or function)";
        case reference:         return ast->has(referenceArgVersion)
                                           ? "array name"
                                           : "reference (array, attribute or dimension name)";
        case schema:            return "schema";
        case cnull:             return "constant with unknown type";
        case creal:             return string("constant with type '") + TID_DOUBLE + "'";
        case cstring:           return string("constant with type '") + TID_STRING + "'";
        case cboolean:          return string("constant with type '") + TID_BOOL   + "'";
        case cinteger:          return string("constant with type '") + TID_INT64  + "'";
    }

    return string();
}

bool Translator::resolveParamAttributeReference(const vector<ArrayDesc> &inputSchemas,
                                                std::shared_ptr<OperatorParamReference> &attRef,
                                                bool throwException)
{
    bool found = false;

    size_t inputNo = 0;
    const std::string &arrayName = attRef->getArrayName();
    for (ArrayDesc const& schema : inputSchemas)
    {
        size_t attributeNo = 0;
        for (size_t i = 0; i < schema.getAttributes().size(); ++i)
        {
            AttributeDesc const& attribute = schema.getAttributes().findattr(i);
            const std::string &namespaceName = schema.getNamespaceName();
            std::string qualifiedArrayName =
                makeQualifiedArrayName(
                    namespaceName, arrayName);

            if (attribute.getName() == attRef->getObjectName()
             && (   attribute.hasAlias(qualifiedArrayName) ||   // NS1.A1 or A1.attribute format
                    attribute.hasAlias(arrayName)))             // ALIAS1.attribute format
            {
                if (found)
                {
                    string fullName;
                    if(attribute.hasAlias(arrayName)) {
                        fullName = str(
                            boost::format("%s%s")
                                % (arrayName != ""
                                    ? arrayName + "."
                                    : "")
                                % attRef->getObjectName() );
                    } else {
                        fullName = str(
                            boost::format("%s%s")
                                % (arrayName != ""
                                    ? qualifiedArrayName + "."
                                    : "")
                                % attRef->getObjectName() );
                    }
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE,attRef) << fullName);
                }
                found = true;

                attRef->setInputNo(safe_static_cast<int32_t>(inputNo));
                attRef->setObjectNo(safe_static_cast<int32_t>(attributeNo));
            }
            ++attributeNo;
        }
        ++inputNo;
    }

    if (!found && throwException)
    {
        const string fullName = str(
			boost::format("%s%s")
				% (arrayName != ""
					? arrayName + "." : "")
				% attRef->getObjectName() );
        fail(SYNTAX(SCIDB_LE_ATTRIBUTE_NOT_EXIST, attRef) << fullName);
    }

    return found;
}

bool Translator::resolveDimension(const vector<ArrayDesc> &inputSchemas, const string& name, const string& alias,
    size_t &inputNo, size_t &dimensionNo, const ContextPtr &parsingContext, bool throwException)
{
    bool found = false;

    size_t _inputNo = 0;
    for (ArrayDesc const& schema : inputSchemas)
    {
        ssize_t _dimensionNo = schema.findDimension(name, alias);
        if (_dimensionNo >= 0)
        {
            if (found)
            {
                const string fullName = str(boost::format("%s%s") % (alias != "" ? alias + "." : "") % name );
                fail(SYNTAX(SCIDB_LE_AMBIGUOUS_DIMENSION, parsingContext) << fullName);
            }
            found = true;

            inputNo = _inputNo;
            dimensionNo = _dimensionNo;
        }

        ++_inputNo;
    }

    if (!found && throwException)
    {
        const string fullName = str(boost::format("%s%s") % (alias != "" ? alias + "." : "") % name );
        fail(SYNTAX(SCIDB_LE_DIMENSION_NOT_EXIST, parsingContext) << fullName << "input" << "?");
    }

    return found;
}

bool Translator::resolveParamDimensionReference(const vector<ArrayDesc> &inputSchemas, std::shared_ptr<OperatorParamReference>& dimRef, bool throwException)
{
    size_t inputNo = 0;
    size_t dimensionNo = 0;

    if (resolveDimension(inputSchemas, dimRef->getObjectName(), dimRef->getArrayName(), inputNo, dimensionNo, newParsingContext(dimRef), throwException))
    {
        dimRef->setInputNo(safe_static_cast<int32_t>(inputNo));
        dimRef->setObjectNo(safe_static_cast<int32_t>(dimensionNo));
        return true;
    }

    return false;
}

std::shared_ptr<OperatorParamAggregateCall> Translator::passAggregateCall(const Node* ast, const vector<ArrayDesc> &inputSchemas)
{
    if (ast->get(applicationArgOperands)->getSize() != 1)
    {
        fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,ast));
    }

    const Node* const arg = ast->get(applicationArgOperands)->get(listArg0);

    std::shared_ptr<OperatorParam> opParam;

    if (arg->is(reference))
    {
        std::shared_ptr <AttributeReference> argument = std::static_pointer_cast<AttributeReference>(onAttributeReference(arg));

        opParam = std::make_shared<OperatorParamAttributeReference>( newParsingContext(arg),
                                                                argument->getArrayName(),
                                                                argument->getAttributeName(),
                                                                true );

        resolveParamAttributeReference(inputSchemas, (std::shared_ptr<OperatorParamReference>&) opParam, true);
    }
    else
    if (arg->is(asterisk))
    {
        opParam = std::make_shared<OperatorParamAsterisk>(newParsingContext(arg));
    }
    else
    {
        fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT, ast));
    }

    return std::make_shared<OperatorParamAggregateCall> (
            newParsingContext(ast),
            getStringApplicationArgName(ast),
            opParam,
            getString(ast,applicationArgAlias));
}

bool Translator::placeholdersVectorContainType(const vector<std::shared_ptr<OperatorParamPlaceholder> > &placeholders,
    OperatorParamPlaceholderType placeholderType)
{
    for (auto const& placeholder : placeholders)
    {
        if (placeholder->getPlaceholderType() == placeholderType)
            return true;
    }
    return false;
}

LQPNPtr Translator::passSelectStatement(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LQPNPtr result;

    const Node* const fromClause = ast->get(selectArrayArgFromClause);
    const Node* const selectList = ast->get(selectArrayArgSelectList);
    const Node* const grwClause  = ast->get(selectArrayArgGRWClause);

    if (fromClause)
    {
        //First of all joins,scan or nested query will be translated and used
        result = passJoins(fromClause, depthOperator+1);

        //Next WHERE clause
        const Node *filterClause = ast->get(selectArrayArgFilterClause);
        if (filterClause)
        {
            result = passFilterClause(filterClause, result, depthOperator+1);
        }

        const Node *orderByClause = ast->get(selectArrayArgOrderByClause);
        if (orderByClause)
        {
            result = passOrderByClause(orderByClause, result, depthOperator+1);
        }

        result = passSelectList(result, selectList, grwClause, depthOperator+1);
    }
    else
    {
        if (selectList->getSize() > 1
            ||  selectList->get(listArg0)->is(asterisk)
            || !selectList->get(listArg0)->get(namedExprArgExpr)->is(application)
            || !AggregateLibrary::getInstance()->hasAggregate(getStringApplicationArgName(selectList->get(listArg0)->get(namedExprArgExpr))))
        {
             fail(SYNTAX(SCIDB_LE_AGGREGATE_EXPECTED,selectList));
        }

        const Node* aggregate = selectList->get(listArg0)->get(namedExprArgExpr);
        const chars funcName  = getStringApplicationArgName(aggregate);
        const Node* funcParams = aggregate->get(applicationArgOperands);

        if (funcParams->getSize() != 1)
        {
            fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,funcParams));
        }

        LQPNPtr aggInput;

        switch (funcParams->get(listArg0)->getType())
        {
            case reference:
                aggInput = passImplicitScan(funcParams->get(listArg0), depthOperator+1);
                break;
            case selectArray:
                aggInput = passSelectStatement(funcParams->get(listArg0), depthOperator+1);
                break;
            default:
                fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENT2,funcParams->get(listArg0)));
        }

        // First of all try to convert it as select agg(*) from A group by x as G
        // Let's check if asterisk supported
        bool asteriskSupported = true;
        try
        {
            AggregateLibrary::getInstance()->createAggregate(funcName, TypeLibrary::getType(TID_VOID));
        }
        catch (const UserException &e)
        {
            if (SCIDB_LE_AGGREGATE_DOESNT_SUPPORT_ASTERISK == e.getLongErrorCode())
            {
                asteriskSupported = false;
            }
            else
            {
                e.raise();
            }
        }

        const ArrayDesc &aggInputSchema = aggInput->inferTypes(_qry); // NODE
        std::shared_ptr<OperatorParamAggregateCall> aggCallParam;

        if (asteriskSupported)
        {
            Node *aggregateCallAst = _fac.newApp(aggregate->getWhere(),
                        funcName,
                        _fac.newNode(asterisk,funcParams->get(listArg0)->getWhere()));
            aggCallParam = passAggregateCall(aggregateCallAst, vector<ArrayDesc>(1, aggInputSchema));
        }
        else
        {
            if (aggInputSchema.getAttributes(true).size() == 1)
            {
                size_t attNo = aggInputSchema.getEmptyBitmapAttribute() && aggInputSchema.getEmptyBitmapAttribute()->getId() == 0 ? 1 : 0;

                Node *aggregateCallAst = _fac.newApp(aggregate->getWhere(),funcName,
                            _fac.newRef(funcParams->get(listArg0)->getWhere(),
                                _fac.newString(funcParams->get(listArg0)->getWhere(),aggInputSchema.getAttributes().findattr(attNo).getName())));
                aggCallParam = passAggregateCall(
                            aggregateCallAst,
                            vector<ArrayDesc>(1, aggInputSchema));
            }
            else
            {
                fail(SYNTAX(SCIDB_LE_SINGLE_ATTRIBUTE_IN_INPUT_EXPECTED,funcParams->get(listArg0)));
            }
        }
        aggCallParam->setAlias( getString(selectList->get(listArg0),namedExprArgName));
        Parameters aggParams;
        aggParams.push_back(aggCallParam);
        result = appendOperator(aggInput, "aggregate", aggParams, newParsingContext(aggregate));
    }

    if (const Node* intoClause = ast->get(selectArrayArgIntoClause))
    {
        result = passIntoClause(intoClause,result, depthOperator+1);
    }

    return result;
}

LQPNPtr Translator::passJoins(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    // Left part holding result of join constantly but initially it empty
    LQPNPtr left;

    // Loop for joining all inputs consequentially. Left joining part can be joined previously or
    // empty nodes. Right part will be joined to left on every iteration.
    for (Node* joinItem : ast->getList())
    {
        LQPNPtr right = passJoinItem(joinItem, depthOperator+1);

        // If we on first iteration - right part turning into left, otherwise left and right parts
        // joining and left part turning into join result.
        if (!left)
        {
            left = right;
        }
        else
        {
            LQPNPtr node = std::make_shared<LogicalQueryPlanNode>(newParsingContext(joinItem),
                    OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(left);
            node->addChild(right);
            left = node;
        }
    }

    //Check JOIN with its inferring
    try
    {
        left->inferTypes(_qry);     // NODE
    }
    catch(const Exception &e)
    {
        fail(CONV_TO_USER_QUERY_EXCEPTION(e, newParsingContext(ast)));
    }

    // Ok, return join result
    return left;
}

LQPNPtr Translator::passGeneralizedJoin(const Node* ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LOG4CXX_TRACE(logger, "Translating JOIN-ON clause...");

    LQPNPtr left = passJoinItem(ast->get(joinClauseArgLeft), depthOperator+1);
    LQPNPtr right = passJoinItem(ast->get(joinClauseArgRight), depthOperator+1);

    vector<ArrayDesc> inputSchemas;
    inputSchemas.push_back(left->inferTypes(_qry));     // NODE
    inputSchemas.push_back(right->inferTypes(_qry));    // NODE

    vector<std::shared_ptr<OperatorParamReference> > opParams;
    // Checking JOIN-ON clause for pure DD join
    Node* joinOnAst = ast->get(joinClauseArgExpr);
    const size_t depthExpression = 0;
    bool pureDDJoin = passGeneralizedJoinOnClause(opParams, joinOnAst, depthExpression);

    // Well it looks like DD-join but there is a probability that we have attributes or
    // duplicates in expression. Let's check it.
    for (size_t i = 0; pureDDJoin && (i < opParams.size()); i += 2)
    {
        LOG4CXX_TRACE(logger, "Probably pure DD join");

        bool isLeftDimension = resolveParamDimensionReference(inputSchemas, opParams[i], false);
        bool isLeftAttribute = resolveParamAttributeReference(inputSchemas, opParams[i], false);

        bool isRightDimension = resolveParamDimensionReference(inputSchemas, opParams[i + 1], false);
        bool isRightAttribute = resolveParamAttributeReference(inputSchemas, opParams[i + 1], false);

        const string leftFullName = str(boost::format("%s%s") % (opParams[i]->getArrayName() != "" ?
                opParams[i]->getArrayName() + "." : "") % opParams[i]->getObjectName() );

        const string rightFullName = str(boost::format("%s%s") % (opParams[i + 1]->getArrayName() != "" ?
                opParams[i + 1]->getArrayName() + "." : "") % opParams[i + 1]->getObjectName() );

        // Generic checks on existing and ambiguity first of all
        if (!isLeftDimension && !isLeftAttribute)
        {
            fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,opParams[i]) << leftFullName);
        }
        else if (isLeftDimension && isLeftAttribute)
        {
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,opParams[i]) << leftFullName);
        }

        if (!isRightDimension && !isRightAttribute)
        {
            fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,opParams[i + 1]) << rightFullName);
        }
        else if (isRightDimension && isRightAttribute)
        {
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,opParams[i + 1]) << rightFullName);
        }

        // No chance. There are attributes and we can not do 'SELECT * FROM A JOIN B ON A.x = A.x' with CROSS_JOIN
        if (isRightAttribute || isLeftAttribute || (opParams[i]->getInputNo() == opParams[i + 1]->getInputNo()))
        {
            LOG4CXX_TRACE(logger, "Nope. This is generalized JOIN");
            pureDDJoin = false;
            break;
        }

        //Ensure dimensions ordered by input number
        if (opParams[i]->getInputNo() == 1)
        {
            LOG4CXX_TRACE(logger, "Swapping couple of dimensions");

            std::shared_ptr<OperatorParamReference> newRight = opParams[i];
            opParams[i] = opParams[i+1];
            opParams[i+1] = newRight;

            isLeftAttribute = isRightAttribute;
            isRightAttribute = isLeftAttribute;

            isLeftDimension = isRightDimension;
            isRightDimension = isLeftDimension;
        }
    }

    if (pureDDJoin)
    {
        LOG4CXX_TRACE(logger, "Yep. This is really DD join. Inserting CROSS_JOIN");
        // This is DD join! We can do it fast with CROSS_JOIN
        LQPNPtr crossJoinNode = std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast),
                OperatorLibrary::getInstance()->createLogicalOperator("cross_join"));

        crossJoinNode->addChild(left);
        crossJoinNode->addChild(right);
        crossJoinNode->getLogicalOperator()->setParameters(vector<std::shared_ptr<OperatorParam> >(opParams.begin(), opParams.end()));

        return crossJoinNode;
    }
    else
    {
        LOG4CXX_TRACE(logger, "Inserting CROSS");

        // This is generalized join. Emulating it with CROSS_JOIN+FILTER
        LQPNPtr crossNode = std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast),
                OperatorLibrary::getInstance()->createLogicalOperator("Cross_Join"));

        crossNode->addChild(left);
        crossNode->addChild(right);

        LOG4CXX_TRACE(logger, "Inserting FILTER");
        vector<std::shared_ptr<OperatorParam> > filterParams(1);
        filterParams[0] = std::make_shared<OperatorParamLogicalExpression>(
                            newParsingContext(joinOnAst),
                            AstToLogicalExpression(joinOnAst),
                            TypeLibrary::getType(TID_BOOL));

        return appendOperator(crossNode, "filter", filterParams, newParsingContext(joinOnAst));
    }
}

bool Translator::passGeneralizedJoinOnClause(
        vector<std::shared_ptr<OperatorParamReference> > &params,
        const Node *ast,
        size_t depthExpression)
{
    checkDepthExpression(depthExpression);

    if (ast->is(application))
    {
        const string funcName  = getStringApplicationArgName(ast);
        const Node* funcParams = ast->get(applicationArgOperands);

        if (funcName == "and")
        {
            return passGeneralizedJoinOnClause(params,funcParams->get(listArg0), depthExpression+1)
                && passGeneralizedJoinOnClause(params,funcParams->get(listArg1), depthExpression+1);
        }
        else if (funcName == "=")
        {
            for (const Node* ref : funcParams->getList())
            {
                if (!ref->is(reference))
                {
                    return false;
                }
            }

            const Node *leftDim  = funcParams->get(listArg0);
            const Node *rightDim = funcParams->get(listArg1);

            Node *ns, *ary, *var;
            splitReferenceArgNames(leftDim, ns, ary, var);
            if (ns) {
                // XXX TODO Support it.
                fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
                     "Namespace qualifier not supported in this context");
            }
            const string leftObjectName = getStringVariableName(var);
            const string leftArrayName  = getStringVariableName(ary);

            params.push_back(std::make_shared<OperatorParamDimensionReference>(
                    newParsingContext(leftDim),
                    leftArrayName,
                    leftObjectName,
                    true));

            splitReferenceArgNames(rightDim, ns, ary, var);
            if (ns) {
                // XXX TODO Support it.
                fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
                     "Namespace qualifier not supported in this context");
            }
            const string rightObjectName = getStringVariableName(var);
            const string rightArrayName  = getStringVariableName(ary);

            params.push_back(std::make_shared<OperatorParamDimensionReference>(
                    newParsingContext(rightDim),
                    rightArrayName,
                    rightObjectName,
                    true));

            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

LQPNPtr Translator::passCrossJoin(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LQPNPtr left  = passJoinItem(ast->get(joinClauseArgLeft), depthOperator+1);
    LQPNPtr right = passJoinItem(ast->get(joinClauseArgRight), depthOperator+1);
    LQPNPtr node  = std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast),OperatorLibrary::getInstance()->createLogicalOperator("Cross_Join"));
    node->addChild(left);
    node->addChild(right);

    return node;
}

LQPNPtr Translator::passJoinItem(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    switch (ast->getType())
    {
        case namedExpr:
        {
            Node* expr = ast->get(namedExprArgExpr);

            if (   !expr->is(application)
                && !expr->is(reference)
                && !expr->is(selectArray))
            {
                fail(SYNTAX(SCIDB_LE_INPUT_EXPECTED,expr));
            }

            LQPNPtr result(AstToLogicalPlan(expr, depthOperator+1));
            prohibitNesting(result);

            if (expr->is(reference) && expr->has(referenceArgAlias))
            {
                result->getLogicalOperator()->setAliasName(getString(expr,referenceArgAlias));
            }

            result->getLogicalOperator()->setAliasName(getString(ast,namedExprArgName));
            result->inferTypes(_qry, true); // force update flag required after modifying the AliasName // NODE
                                            // TBD: perhaps we could have setAliasName clear the cached flag?
            return result;
        }

        case joinClause:
            if (ast->has(joinClauseArgExpr))
            {
                return passGeneralizedJoin(ast, depthOperator+1);
            }
            else
            {
                return passCrossJoin(ast, depthOperator+1);
            }

        case thinClause:    return passThinClause(ast, depthOperator+1);
        default:            SCIDB_UNREACHABLE();
                            return LQPNPtr();
    }
}

LQPNPtr Translator::passImplicitScan(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    assert(ast->is(reference));
    Parameters scanParams;
    std::shared_ptr<OperatorParamArrayReference> ref = createArrayReferenceParam(ast, true);
    scanParams.push_back(ref);
    LOPtr scanOp = OperatorLibrary::getInstance()->createLogicalOperator(
        (ref->getVersion() == ALL_VERSIONS) ? "allversions" : "scan" , getString(ast,referenceArgAlias));
    scanOp->setParameters(scanParams);
    scanOp->setInserter(LogicalOperator::Inserter::TRANSLATOR);
    return std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast), scanOp);
}

LQPNPtr Translator::passFilterClause(const Node* ast, const LQPNPtr &input, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    Parameters filterParams;
    const ArrayDesc &inputSchema = input->inferTypes(_qry); // NODE

    LEPtr lExpr = AstToLogicalExpression(ast);

    LOG4CXX_WARN(logger, "passFilterClause calling checkLogicalExprssion");
    checkLogicalExpression(vector<ArrayDesc>(1, inputSchema), ArrayDesc(), lExpr);

    filterParams.push_back(std::make_shared<OperatorParamLogicalExpression>(newParsingContext(ast),
        lExpr, TypeLibrary::getType(TID_BOOL)));

    LOPtr filterOp = OperatorLibrary::getInstance()->createLogicalOperator("filter");
    filterOp->setParameters(filterParams);

    LQPNPtr result = std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast), filterOp);
    result->addChild(input);
    return result;
}

LQPNPtr Translator::passOrderByClause(const Node* ast, const LQPNPtr &input, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    Parameters sortParams;
    sortParams.reserve(ast->getSize());
    const ArrayDesc &inputSchema = input->inferTypes(_qry); // NODE
    Node *ns, *ary, *var;

    for (const Node* sortAttributeAst : ast->getList())
    {
        splitReferenceArgNames(sortAttributeAst, ns, ary, var);
        if (ns) {
            // XXX TODO Support it.
            fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
                 "Namespace qualifier not supported in this context");
        }

        std::shared_ptr<OperatorParamAttributeReference> sortParam = std::make_shared<OperatorParamAttributeReference>(
            newParsingContext(sortAttributeAst),
            getStringVariableName(ary),
            getStringVariableName(var),
            true);

        sortParam->setSortAscent(getInteger(sortAttributeAst,referenceArgOrder,ascending) == ascending);

        resolveParamAttributeReference(vector<ArrayDesc>(1, inputSchema),
                                       (std::shared_ptr<OperatorParamReference>&) sortParam,
                                       true);

        sortParams.push_back(sortParam);
    }

    LQPNPtr result = appendOperator(input, "sort", sortParams, newParsingContext(ast));
    auto lop = result->getLogicalOperator();
    lop->addParameter("_coords", // Drop prefixed coordinates
                      createBoolConstantParam(false, newParsingContext(ast)));
    result->inferTypes(_qry);   // NODE
    return result;
}

LQPNPtr Translator::passIntoClause(const Node* ast, LQPNPtr &input, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LOG4CXX_TRACE(logger, "Translating INTO clause...");

    const ArrayDesc   inputSchema    = input->inferTypes(_qry); // NODE
    const string      targetName     = ast->getString();
    const ContextPtr& parsingContext = newParsingContext(ast);

    LQPNPtr result;

    Parameters targetParams(1,std::make_shared<OperatorParamArrayReference>(parsingContext,"",targetName,true));
    LOPtr storeOp;

    assert(_qry);
    assert(isNameUnversioned(targetName));

    std::string nsName(_qry->getNamespaceName());
    std::string qualName(makeQualifiedArrayName(nsName, targetName));

    ArrayDesc destinationSchema;
    SystemCatalog::GetArrayDescArgs args;
    args.result = &destinationSchema;
    args.nsName = nsName;
    args.arrayName = targetName;
    args.catalogVersion = _qry->getCatalogVersion(nsName, targetName);
    args.throwIfNotFound = false;
    bool found = SystemCatalog::getInstance()->getArrayDesc(args);
    if (!found)
    {
        LOG4CXX_TRACE(logger, str(boost::format("Target array '%s' not existing so inserting STORE") %
                                  qualName));
        storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
        storeOp->setParameters(targetParams);
        result = std::make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
        result->addChild(input);
    }
    else
    {
        LOG4CXX_TRACE(logger, str(boost::format("Target array '%s' existing.") % targetName));

        /*
         * Let's check if input can fit somehow into destination array. If names differ we can insert
         * CAST. If array partitioning differ we can insert REPART. Also we can force input to be empty
         * array to fit empty destination. We can't change array size or dimensions/attributes types,
         * so if such difference found - we skipping casting/repartioning.
         */
        LQPNPtr fittedInput = fitInput(input, destinationSchema);
        bool tryFlip = false;
        try
        {
            LOG4CXX_TRACE(logger, "Trying to insert STORE");
            storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
            storeOp->setParameters(targetParams);
            result = std::make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
            result->addChild(fittedInput);
            result->inferTypes(_qry);   // NODE
        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REPART and/or CAST and/or STORE");
                tryFlip = true;
            }
            else
            {
                LOG4CXX_TRACE(logger, "Something going wrong");
                e.raise();
            }
        }

        if (!tryFlip)
        {
            LOG4CXX_TRACE(logger, "OK. We managed to fit input into destination. STORE will be used.");
            return result;
        }

        try
        {
            LOG4CXX_TRACE(logger, "Trying to wrap with STORE(REDIMENSION(...))");

            {
            LOPtr redimOp = OperatorLibrary::getInstance()->createLogicalOperator("redimension");
            redimOp->setParameters(Parameters(1,std::make_shared<OperatorParamSchema>(parsingContext, destinationSchema)));
            result = std::make_shared<LogicalQueryPlanNode>(parsingContext, redimOp);
            result->addChild(input);
            result->inferTypes(_qry); // NODE
            }

            {
            LOPtr storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
            storeOp->setParameters(targetParams);
            LQPNPtr storeNode = std::make_shared<LogicalQueryPlanNode>(parsingContext, storeOp);
            storeNode->addChild(result);
            return storeNode;
            }
        }
        catch (const UserException& e)
        {
            if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
            {
                LOG4CXX_TRACE(logger, "Can not infer schema from REDIMENSION_STORE");
                fail(SYNTAX(SCIDB_LE_CAN_NOT_STORE,ast) << targetName);
            }

            LOG4CXX_TRACE(logger, "Something going wrong");
            e.raise();
        }
        LOG4CXX_TRACE(logger, "OK. REDIMENSION_STORE matched.");
    }

    return result;
}

LQPNPtr Translator::passUpdateStatement(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    Node *arrayRef = ast->get(updateArrayArgArrayRef);
    LQPNPtr result = passImplicitScan(arrayRef, depthOperator+1);

    assert(_qry);

    Node *ns, *ary;
    splitReferenceArgNames(arrayRef, ns, ary);

    ArrayDesc arrayDesc;
    SystemCatalog::GetArrayDescArgs args;
    args.result = &arrayDesc;
    args.nsName = ns ? getStringVariableName(ns) : _qry->getNamespaceName();
    args.arrayName = getStringVariableName(ary);
    args.catalogVersion = _qry->getCatalogVersion(args.nsName, args.arrayName);
    args.throwIfNotFound = true;
    SystemCatalog::getInstance()->getArrayDesc(args);

    const Node *updateList = ast->get(updateArrayArgUpdateList);

    map<string,string> substMap;

    Parameters applyParams;
    unsigned int counter = 0;
    for (const Node* updateItem : updateList->getList())
    {
        const string attName = getString(updateItem,updateArgName);

        bool found = false;
        for (size_t i = 0; i < arrayDesc.getAttributes(/*excludeEbm:*/true).size(); ++i)
        {
            AttributeDesc const& att =
                arrayDesc.getAttributes(/*excludeEbm:*/true).findattr(i);
            if (att.getName() == attName)
            {
                const string newAttName = genUniqueObjectName("updated_" + attName, counter, vector<ArrayDesc>(1, arrayDesc), true);
                substMap[att.getName()] = newAttName;
                found = true;

                //placeholder
                Node* attExpr = updateItem->get(updateArgExpr);

                vector<ArrayDesc> schemas;
                schemas.push_back(arrayDesc);

                if (expressionType(AstToLogicalExpression(attExpr), schemas) != TypeLibrary::getType(att.getType()).typeId())
                {
                    //Wrap expression with type converter appropriate attribute type
                    attExpr = _fac.newApp(
                                attExpr->getWhere(),
                               _fac.newString(attExpr->getWhere(),TypeLibrary::getType(att.getType()).name()),
                               _fac.newCopy(attExpr));
                }

                /* Converting WHERE predicate into iif application in aply operator parameter:
                 * apply(A, a_updated, iif(iif(is_null(whereExpr), false, whereExpr), updateExpr, a))
                 * If we have where clause, we must check if value of where predicate is null or not.
                 * Value of attribute changed only when result of predicate is TRUE. If result of
                 * predicate is NULL or FALSE, value of attribute will not be changed.
                 */

                if (const Node* const whereExpr = ast->get(updateArrayArgWhereClause))
                {
                    location w = attExpr->getWhere();
                    attExpr    = _fac.newApp(w,"iif",
                                 _fac.newApp(w,"iif",
                                 _fac.newApp(w,"is_null",_fac.newCopy(whereExpr)),
                                 _fac.newBoolean(w,false),
                                 _fac.newCopy(whereExpr)),
                                 _fac.newCopy(attExpr),
                                 _fac.newRef(w,_fac.newString(w,attName)));
                }

                applyParams.push_back(std::make_shared<OperatorParamAttributeReference>(newParsingContext(updateItem),
                        "", newAttName, false));

                applyParams.push_back(std::make_shared<OperatorParamLogicalExpression>(newParsingContext(updateItem),
                        AstToLogicalExpression(attExpr), TypeLibrary::getType(att.getType()), false));

                break;
            }
        }

        if (!found)
        {
            fail(SYNTAX(SCIDB_LE_ATTRIBUTE_NOT_EXIST,updateItem->get(updateArgName)) << attName);
        }
    }

    //Wrap child nodes with apply operator created new attribute
    result = appendOperator(result, "apply", applyParams, newParsingContext(updateList));

    //Projecting changed attributes along with unchanged to simulate real update
    vector<ArrayDesc> schemas;
    schemas.push_back(result->inferTypes(_qry)); // NODE
    Parameters projectParams;
    for (size_t i = 0; i < arrayDesc.getAttributes(/*excludeEbm:*/true).size(); ++i)
    {
        AttributeDesc const& att = arrayDesc.getAttributes().findattr(i);
        std::shared_ptr<OperatorParamReference> newAtt;
        if (substMap[att.getName()] != "")
        {
            newAtt = std::make_shared<OperatorParamAttributeReference>(newParsingContext(updateList),
                    "", substMap[att.getName()], true);
        }
        else
        {
            newAtt = std::make_shared<OperatorParamAttributeReference>(newParsingContext(updateList),
                    "", att.getName(), true);
        }
        resolveParamAttributeReference(schemas, newAtt);
        projectParams.push_back(newAtt);
    }

    LOPtr projectOp = OperatorLibrary::getInstance()->createLogicalOperator("project");
    projectOp->setParameters(projectParams);

    LQPNPtr projectNode = std::make_shared<LogicalQueryPlanNode>(newParsingContext(updateList), projectOp);
    projectNode->addChild(result);
    result = projectNode;

    //Finally wrap input with STORE operator
    Parameters storeParams;

    storeParams.push_back(std::make_shared<OperatorParamArrayReference>(
                              newParsingContext(ary), "", args.arrayName, true));

    LOPtr storeOp = OperatorLibrary::getInstance()->createLogicalOperator("store");
    storeOp->setParameters(storeParams);

    LQPNPtr storeNode = std::make_shared<LogicalQueryPlanNode>(newParsingContext(ast), storeOp);
    storeNode->addChild(result);
    result = storeNode;

    return result;
}

LQPNPtr Translator::passInsertIntoStatement(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    assert(ast->is(insertArray));
    LOG4CXX_TRACE(logger, "Translating INSERT INTO");

    const Node* srcAst = ast->get(insertArrayArgSource);
    const Node* dstAst = ast->get(insertArrayArgDestination);

    string dstName = dstAst->getString();
    Parameters dstOpParams;
    dstOpParams.push_back(std::make_shared<OperatorParamArrayReference>(newParsingContext(dstAst), "", dstName, true));

    assert(_qry);
    ArrayDesc dstSchema;
    SystemCatalog::GetArrayDescArgs args;
    args.result = &dstSchema;
    args.nsName = _qry->getNamespaceName();
    args.arrayName = dstName;
    args.catalogVersion = _qry->getCatalogVersion(args.nsName, dstName);
    args.throwIfNotFound = false;
    bool found = SystemCatalog::getInstance()->getArrayDesc(args);
    if (!found) {
        fail(QPROC(SCIDB_LE_ARRAY_DOESNT_EXIST, dstAst)
             << makeQualifiedArrayName(args.nsName, dstName));
    }

    LQPNPtr srcNode;
    if (srcAst->is(selectArray))
    {
        LOG4CXX_TRACE(logger, "Source of INSERT INTO is SELECT");
        srcNode = passSelectStatement(srcAst, depthOperator+1);
    }
    else if (srcAst->is(cstring))
    {
        LOG4CXX_TRACE(logger, "Source of INSERT INTO is array literal");
        Parameters buildParams;
        buildParams.push_back(std::make_shared<OperatorParamSchema>(
            newParsingContext(dstAst),
            dstSchema));

        const string arrayLiteral = srcAst->getString();
        Value sval(TypeLibrary::getType(TID_STRING));
        sval.setData(arrayLiteral.c_str(), arrayLiteral.length() + 1);
        LEPtr expr = std::make_shared<Constant>(newParsingContext(ast), sval, TID_STRING);
        buildParams.push_back(std::make_shared<OperatorParamLogicalExpression>(
            newParsingContext(srcAst),
            expr, TypeLibrary::getType(TID_STRING), true));

        Value bval(TypeLibrary::getType(TID_BOOL));
        bval.setBool(true);
        expr = std::make_shared<Constant>(newParsingContext(ast), bval, TID_BOOL);
        buildParams.push_back(std::make_shared<OperatorParamLogicalExpression>(
            newParsingContext(srcAst),
            expr, TypeLibrary::getType(TID_BOOL), true));

        srcNode = std::make_shared<LogicalQueryPlanNode>(
                newParsingContext(srcAst),
                OperatorLibrary::getInstance()->createLogicalOperator("build"));
        srcNode->getLogicalOperator()->setParameters(buildParams);
    }
    else
    {
        SCIDB_UNREACHABLE();
    }

    LOG4CXX_TRACE(logger, "Checking source schema and trying to fit it to destination for inserting");
    srcNode = fitInput(srcNode, dstSchema);

    LOG4CXX_TRACE(logger, "Inserting INSERT operator");
    return appendOperator(srcNode, "insert", dstOpParams, newParsingContext(ast));
}

void Translator::checkLogicalExpression(const vector<ArrayDesc> &inputSchemas, const ArrayDesc &outputSchema,const LEPtr &expr)
{
    if (typeid(*expr) == typeid(AttributeReference))
    {
        const std::shared_ptr<AttributeReference> &ref = std::static_pointer_cast<AttributeReference>(expr);

        //We don't know exactly what type this reference, so check both attribute and dimension,
        //and if we eventually found both, so throw ambiguous exception

        const bool foundAttrIn  = checkAttribute(inputSchemas,    ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundAttrOut = checkAttribute(vector<ArrayDesc>(1, outputSchema),   ref->getArrayName(),     ref->getAttributeName(), ref->getParsingContext());
        const bool foundDimIn   = checkDimension(inputSchemas,    ref->getArrayName(), ref->getAttributeName(), ref->getParsingContext());
        const bool foundDimOut  = checkDimension(vector<ArrayDesc>(1, outputSchema),    ref->getArrayName(),     ref->getAttributeName(), ref->getParsingContext());

        // Checking ambiguous situation in input schema. If no ambiguity we found dimension/attribute
        // or not.
        if (foundAttrIn && foundDimIn)
        {
            const string fullName = str(boost::format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );
            fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
        }
        // If we can't find references in input schema, checking output schema.
        else if (!(foundAttrIn || foundDimIn))
        {
            // Same as for input: checking ambiguity in output schema.
            if (foundAttrOut && foundDimOut)
            {
                const string fullName = str(boost::format("%s%s") % (ref->getArrayName() != "" ? ref->getArrayName() + "." : "") % ref->getAttributeName() );
                fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
            }
            // If we can't find reference even in output schema, finally throw error
            else if (!(foundAttrOut || foundDimOut))
            {
                // we used to try to resolve the name as if it was a reference to a single cell array
                // that does not work with our locking/transaction strategy and had been broken for some time
                // so, we just throw
                const string fullName = str(boost::format("%s%s") % (ref->getArrayName() != "" ?
                                                                     ref->getArrayName() + "." :
                                                                     "") % ref->getAttributeName() );
                fail(SYNTAX(SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,ref) << fullName);
            }
        }
                // If no ambiguity, and found some reference, we ignoring all from output schema.
    }
    else if (typeid(*expr) == typeid(Function))
    {
        const std::shared_ptr<Function> &func = std::static_pointer_cast<Function>(expr);
        for (LEPtr const& funcArg : func->getArgs())
        {
            LOG4CXX_DEBUG(logger, "checkLogicalExpression recursing on function argument");
            checkLogicalExpression(inputSchemas, outputSchema, funcArg);
        }
    }
}

bool Translator::checkAttribute(const vector<ArrayDesc> &inputSchemas,const string& aliasName,const string& attributeName,const ContextPtr &ctxt)
{
    bool found = false;
    size_t schemaNo = 0;
    size_t attNo = 0;

    for (ArrayDesc const& schema : inputSchemas)
    {
        for (const auto& attribute : schema.getAttributes())
        {
            if (attribute.getName() == attributeName && attribute.hasAlias(aliasName))
            {
                if (found)
                {
                    const string fullName = str(boost::format("%s%s") % (aliasName != "" ? aliasName + "." : "") % attributeName);
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_ATTRIBUTE, ctxt) << fullName);
                }
                found = true;
            }
            ++attNo;
        }
        attNo = 0;
        ++schemaNo;
    }

    return found;
}

bool Translator::checkDimension(const vector<ArrayDesc> &inputSchemas, const string& aliasName, const string& dimensionName,const ContextPtr &ctxt)
{
    bool found = false;
    for (ArrayDesc const& schema : inputSchemas)
    {
        LOG4CXX_TRACE(logger, "Translator::checkDimension("
            << "namespaceName=" << schema.getNamespaceName()
            << ", arrayName=" << schema.getName()
            << ")");

        for (DimensionDesc const& dim : schema.getDimensions())
        {
            std::string qualifiedAliasName = makeQualifiedArrayName(
                schema.getNamespaceName(), aliasName);

            if (dim.hasNameAndAlias(dimensionName, qualifiedAliasName) ||
                dim.hasNameAndAlias(dimensionName, aliasName))
            {
                if (found)
                {
                    const string fullName = str(boost::format("%s%s") % (aliasName != "" ? aliasName + "." : "") % dimensionName);
                    fail(SYNTAX(SCIDB_LE_AMBIGUOUS_DIMENSION, ctxt) << fullName);
                }
                found = true;
            }
        }
    }

    return found;
}

LQPNPtr Translator::appendOperator(const LQPNPtr &node,const string& opName,const Parameters &opParams,const ContextPtr &opParsingContext)
{
    LQPNPtr newNode = std::make_shared<LogicalQueryPlanNode>(
            opParsingContext,
            OperatorLibrary::getInstance()->createLogicalOperator(opName));
    auto logicalOperator = newNode->getLogicalOperator();
    logicalOperator->setParameters(opParams);
    logicalOperator->setInserter(LogicalOperator::Inserter::TRANSLATOR);
    newNode->addChild(node);
    return newNode;
}

bool Translator::astHasUngroupedReferences(const Node* ast,const set<string>& grouped) const
{
    switch (ast->getType())
    {
        case application:
        {
            for (const Node* a : ast->getList(applicationArgOperands))
            {
                if (astHasUngroupedReferences(a,grouped))
                    return true;
            }

            return false;
        }

        case reference:
        {
            Node *ns, *ary, *var;
            splitReferenceArgNames(ast, ns, ary, var);
            ASSERT_EXCEPTION(ns == nullptr, // fail() is non-const, mumble
                         "Namespace qualifier not supported in this context");
            return grouped.find(getStringVariableName(var)) == grouped.end();
        }
        case asterisk:  return true;
        default:        return false;
    }
}

bool Translator::astHasAggregates(const Node* ast) const
{
    switch (ast->getType())
    {
        case olapAggregate: ast = ast->get(olapAggregateArgApplication); // and fall through
        case application:
        {
            if (AggregateLibrary::getInstance()->hasAggregate(getStringApplicationArgName(ast)))
                return true;

            for (const Node* a : ast->getList(applicationArgOperands))
            {
                if (astHasAggregates(a))
                    return true;
            }

            return false;
        }

        default:  return false;
    }
}

Node* Translator::decomposeExpression(
    const Node*        ast,
    vector<Node*>&     preAggregationEvals,
    vector<Node*>&     aggregateFunctions,
    unsigned int&      internalNameCounter,
    bool               hasAggregates,
    const ArrayDesc&   inputSchema,
    const set<string>& groupedDimensions,
    bool               window,
    bool&              joinOrigin)
{
    LOG4CXX_TRACE(logger, "Decomposing expression");
    vector<ArrayDesc> inputSchemas(1, inputSchema);

    switch (ast->getType())
    {
        case application:
        case olapAggregate:
        {
            LOG4CXX_TRACE(logger, "This is function");

            const Node* funcNode = ast->is(application)
                ? ast
                : ast->get(olapAggregateArgApplication);

            const chars funcName = getStringApplicationArgName(funcNode);
            const Node* funcArgs = funcNode->get(applicationArgOperands);

            bool isAggregate = AggregateLibrary::getInstance()->hasAggregate(funcName);
            bool isScalar    = FunctionLibrary::getInstance()->hasFunction(funcName, false);

            if (isAggregate && isScalar)
            {
                std::shared_ptr<Expression> pExpr = std::make_shared<Expression>();
                try
                {
                    ArrayDesc outputSchema;
                    pExpr->compile(AstToLogicalExpression(ast), false, TID_VOID, inputSchemas, outputSchema);
                    isAggregate = false;
                }
                catch (const Exception &e)
                {}
            }

            // We found aggregate and must care of it
            if (isAggregate)
            {
                LOG4CXX_TRACE(logger, "This is aggregate call");
                // Currently framework supports only one argument aggregates so drop any other cases
                if (funcArgs->getSize() != 1)
                {
                    LOG4CXX_TRACE(logger, "Passed too many arguments to aggregate call");
                    fail(SYNTAX(SCIDB_LE_WRONG_AGGREGATE_ARGUMENTS_COUNT,funcNode));
                }

                const Node* aggArg = funcArgs->get(listArg0);

                // Check if this sole expression has aggregate calls itself and drop if yes
                if (astHasAggregates(aggArg))
                {
                    LOG4CXX_TRACE(logger, "Nested aggregate");
                    fail(SYNTAX(SCIDB_LE_AGGREGATE_CANT_BE_NESTED,funcNode));
                }

                bool isDimension = false;
                if (aggArg->is(reference))
                {
                    chars dimName;
                    chars dimAlias;
                    passReference(aggArg, dimAlias, dimName);

                    //If we found dimension inside aggregate we must convert into attribute value before aggregating
                    for (DimensionDesc const& dim : inputSchema.getDimensions())
                    {
                        if (dim.hasNameAndAlias(dimName, dimAlias))
                        {
                            isDimension = true;
                            break;
                        }
                    }
                }

                // If function argument is reference or asterisk, we can translate to aggregate call
                // it as is but must assign alias to reference in post-eval expression
                if ((aggArg->is(reference) && !isDimension) ||  aggArg->is(asterisk))
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is reference or asterisk");
                    Node *alias = _fac.newString(
                        funcNode->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    if (ast->is(application))
                    {
                        Node* aggFunc = _fac.newCopy(funcNode);
                        aggFunc->set(applicationArgAlias, alias);
                        aggregateFunctions.push_back(aggFunc);
                    }
                    else
                    if (ast->is(olapAggregate))
                    {
                        Node* aggFunc = _fac.newCopy(ast);
                        aggFunc->get(olapAggregateArgApplication)->set(applicationArgAlias, alias);
                        aggregateFunctions.push_back(aggFunc);
                    }
                    else
                    {
                        SCIDB_UNREACHABLE();
                    }

                    // Finally returning reference to aggregate result in overall expression
                    return _fac.newRef(funcNode->get(applicationArgOperands)->getWhere(),_fac.newCopy(alias));
                }
                // Handle select statement
                else
                if (aggArg->is(selectArray))
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is SELECT");
                    fail(SYNTAX(SCIDB_LE_UNEXPECTED_SELECT_INSIDE_AGGREGATE,ast));
                }
                // We found expression or constant. We can't use it in aggregate/regrid/window and
                // must create pre-evaluate
                else
                {
                    LOG4CXX_TRACE(logger, "Aggregate's argument is expression");
                    // Let's we have aggregate(expression)

                    // Prepare result attribute name for expression which must be evaluated
                    Node *preEvalAttName = _fac.newString(ast->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));
                    // Named expression 'expression as resname1' will be later translated into
                    // operator APPLY(input, expression, preEvalAttName)
                    Node *applyExpression = _fac.newNode(namedExpr, ast->getWhere(),_fac.newCopy(ast->get(applicationArgOperands)->get(listArg0)), preEvalAttName);
                    // This is must be evaluated before aggregates
                    preAggregationEvals.push_back(applyExpression);

                    // Prepare result attribute name for aggregate call which must be evaluate before
                    // evaluating whole expression
                    Node *postEvalAttName = _fac.newString(ast->getWhere(),
                        genUniqueObjectName("expr", internalNameCounter, inputSchemas, true));

                    // Aggregate call will be translated later into AGGREGATE(input, aggregate(preEvalAttName) as postEvalName)
                    Node *aggregateExpression =
                        _fac.newApp(ast->getWhere(),
                        _fac.newCopy(getApplicationArgName(ast)),
                        _fac.newRef(ast->get(applicationArgOperands)->getWhere(),_fac.newCopy(preEvalAttName)));
                    aggregateExpression->set(applicationArgAlias,postEvalAttName);
                    aggregateFunctions.push_back(aggregateExpression);

                    // Finally returning reference to aggregate result in overall expression
                    return _fac.newRef(funcNode->get(applicationArgOperands)->getWhere(),_fac.newCopy(postEvalAttName));
                }
            }
            // This is scalar function. We must pass each argument and construct new function call
            // AST node for post-eval expression
            else
            {
                if (ast->is(olapAggregate))
                {
                    fail(SYNTAX(SCIDB_LE_WRONG_OVER_USAGE,ast));
                }

                LOG4CXX_TRACE(logger, "This is scalar function");
                vector<Node*> newArgs;
                for (const Node* funcArg : funcArgs->getList())
                {
                    LOG4CXX_TRACE(logger, "Passing function argument");
                      newArgs.push_back(
                         decomposeExpression(
                            funcArg,
                            preAggregationEvals,
                            aggregateFunctions,
                            internalNameCounter,
                            hasAggregates,
                            inputSchema,
                            groupedDimensions,
                            window,
                            joinOrigin));
                }

                return _fac.newApp(ast->getWhere(),
                       _fac.newCopy(getApplicationArgName(funcNode)),
                       newArgs);
            }

            break;
        }

        default:
        {
            LOG4CXX_TRACE(logger, "This is reference or constant");
            if (ast->is(reference))
            {
                if (astHasUngroupedReferences(ast, groupedDimensions) && hasAggregates && !window)
                {
                    LOG4CXX_TRACE(logger, "We can not use references in expression with aggregate");
                    fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,ast));
                }

                bool isDimension = false;
                chars dimName;
                chars dimAlias;
                passReference(ast, dimAlias, dimName);

                for (DimensionDesc const& dim : inputSchema.getDimensions())
                {
                    if (dim.hasNameAndAlias(dimName, dimAlias))
                    {
                        isDimension = true;
                        break;
                    }
                }
                if (window && !isDimension)
                    joinOrigin = true;
            }

            LOG4CXX_TRACE(logger, "Cloning node to post-evaluation expression");
            return _fac.newCopy(ast);

            break;
        }
    }

    SCIDB_UNREACHABLE();
    return NULL;
}

LQPNPtr Translator::passSelectList(
    LQPNPtr &input,
    const Node *const selectList,
    const Node *const grwAsClause,
    size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LOG4CXX_TRACE(logger, "Translating SELECT list");
    const ArrayDesc& inputSchema = input->inferTypes(_qry); // NODE
    const vector<ArrayDesc> inputSchemas(1, inputSchema);
    Parameters projectParams;
    bool joinOrigin = false;
    const bool isWindowClauses = grwAsClause && grwAsClause->is(list);

    bool selectListHasAggregates = false;
    for (const Node *selItem : selectList->getList())
    {
        if (selItem->is(namedExpr))
        {
            if (astHasAggregates(selItem->get(namedExprArgExpr)))
            {
                selectListHasAggregates = true;
                break;
            }
        }
    }

    if (grwAsClause && !selectListHasAggregates)
    {
        LOG4CXX_TRACE(logger,"GROUP BY, WINDOW, REGRID or REDIMENSION present, but SELECT list does"
                " not contain aggregates");
        fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE,selectList->get(listArg0)));
    }

    //List of objects in GROUP BY or REDIMENSION list. We don't care about ambiguity now it will be done later.
    //In case REGRID or WINDOW we just enumerate all dimensions from input schema
    set<string> groupedDimensions;
    if (grwAsClause)
    {
        switch (grwAsClause->getType())
        {
            case groupByClause:
                for (const Node *dimensionAST : grwAsClause->getList(groupByClauseArgList))
                {
                    Node *ns, *ary, *var;
                    splitReferenceArgNames(dimensionAST, ns, ary, var);
                    if (ns) {
                        // XXX TODO Support it.  (Hmmm, code here
                        // doesn't seem to care about ary either,
                        // i.e. Y but not X.Y .  Maybe we don't care?)
                        fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, dimensionAST) <<
                             "Namespace qualifier not supported in this context");
                    }
                    groupedDimensions.insert(getStringVariableName(var));
                }
                break;
            case redimensionClause:
                for (const Node *dimensionAST : grwAsClause->getList(listArg0))
                {
                    assert(dimensionAST->is(dimension));
                    groupedDimensions.insert(dimensionAST->get(dimensionArgName)->getString());
                }
                break;
            case list:
                assert(isWindowClauses);
            case regridClause:
                for (DimensionDesc const& dim : inputSchema.getDimensions())
                {
                    groupedDimensions.insert(dim.getBaseName());
                    for (DimensionDesc::NamesPairType const& name : dim.getNamesAndAliases())
                    {
                        groupedDimensions.insert(name.first);
                    }
                }
                break;
            default:
                SCIDB_UNREACHABLE();
                break;
        }
    }

    vector<Node*> preAggregationEvals;
    vector<Node*> aggregateFunctions;
    vector<Node*> postAggregationEvals;

    LQPNPtr result = input;

    unsigned int internalNameCounter = 0;
    unsigned int externalExprCounter = 0;
    unsigned int externalAggregateCounter = 0;
    for (Node *selItem : selectList->getList())
    {
        LOG4CXX_TRACE(logger, "Translating SELECT list item");

        switch (selItem->getType())
        {
            case namedExpr:
            {
                LOG4CXX_TRACE(logger, "Item is named expression");

                // If reference is attribute, we must do PROJECT
                bool doProject = false;
                if ( selItem->get(namedExprArgExpr)->is(reference)
                 && !selItem->get(namedExprArgName)
                 && !(grwAsClause && grwAsClause->is(redimensionClause)))
                {
                    const Node* refNode = selItem->get(namedExprArgExpr);
                    chars alias, name;
                    getAliasFromReference(refNode, alias, name);
                    for (size_t i = 0; i < inputSchema.getAttributes().size(); ++i)
                    {
                        AttributeDesc const& aDesc = inputSchema.getAttributes().findattr(i);
                        LOG4CXX_TRACE(logger, "Item is named expression");
                        if (aDesc.getName() == name && aDesc.hasAlias(alias))
                        {
                            doProject = true;
                            break;
                        }
                    }
                }

                if (doProject)
                {
                    LOG4CXX_TRACE(logger, "Item is has no name so this is projection");
                    const Node *refNode = selItem->get(namedExprArgExpr);
                    if (selectListHasAggregates && !isWindowClauses)
                    {
                        LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't do projection");
                        fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,refNode));
                    }
                    else if (isWindowClauses)
                    {
                        joinOrigin = true;
                    }

                    Node *ns, *ary, *var;
                    splitReferenceArgNames(refNode, ns, ary, var);
                    if (ns) {
                        // XXX TODO Support it.
                        fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, refNode) <<
                           "Namespace qualifier not supported in this context");
                    }

                    std::shared_ptr<OperatorParamReference> param = std::make_shared<OperatorParamAttributeReference>(
                        newParsingContext(selItem),
                        getStringVariableName(ary),
                        getStringVariableName(var),
                        true);

                    resolveParamAttributeReference(inputSchemas, param);
                    projectParams.push_back(param);
                }
                else
                {
                    LOG4CXX_TRACE(logger, "This is will be expression evaluation");

                    if (astHasAggregates(selItem->get(namedExprArgExpr)))
                    {
                        LOG4CXX_TRACE(logger, "This is will be expression with aggregate evaluation");
                    }
                    else
                    {
                        LOG4CXX_TRACE(logger, "This is will be expression evaluation");
                        if (astHasUngroupedReferences(selItem->get(namedExprArgExpr), groupedDimensions)
                            && selectListHasAggregates && !isWindowClauses)
                        {
                            LOG4CXX_TRACE(logger, "This expression has references we can't evaluate it because we has aggregates");
                            fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,selItem));
                        }
                        else if (isWindowClauses)
                        {
                            joinOrigin = true;
                        }
                    }

                    Node *postEvalExpr = decomposeExpression(
                        selItem->get(namedExprArgExpr),
                        preAggregationEvals,
                        aggregateFunctions,
                        internalNameCounter,
                        selectListHasAggregates,
                        inputSchema,
                        groupedDimensions,
                        isWindowClauses,
                        joinOrigin);

                    // Prepare name for SELECT item result. If AS was used by user, we just copy it
                    // else we generate new name
                    Node* outputNameNode = NULL;
                    if (selItem->get(namedExprArgName))
                    {
                        outputNameNode = _fac.newCopy(selItem->get(namedExprArgName));
                    }
                    else
                    {
                        // If SELECT item is single aggregate we will use function name as prefix
                        // else we will use 'expr' prefix
                        string prefix;
                        if (selItem->get(namedExprArgExpr)->is(application)
                            && AggregateLibrary::getInstance()->hasAggregate(
                                getStringApplicationArgName(selItem->get(namedExprArgExpr))))
                        {
                            outputNameNode = _fac.newString(selItem->getWhere(),
                                genUniqueObjectName(
                                    getStringApplicationArgName(selItem->get(namedExprArgExpr)),
                                    externalAggregateCounter, inputSchemas, false, selectList->getList()));
                        }
                        else if (olapAggregate == selItem->get(namedExprArgExpr)->getType()
                            && AggregateLibrary::getInstance()->hasAggregate(
                                getStringApplicationArgName(selItem->get(namedExprArgExpr)->get(olapAggregateArgApplication))))
                        {
                            Node* funcNode = selItem->get(namedExprArgExpr)->get(olapAggregateArgApplication);
                            outputNameNode = _fac.newString(funcNode->getWhere(),
                                genUniqueObjectName(
                                    getStringApplicationArgName(funcNode),
                                    externalAggregateCounter, inputSchemas, false, selectList->getList()));
                        }
                        else
                        {
                            outputNameNode = _fac.newString(selItem->getWhere(),
                                genUniqueObjectName("expr", externalExprCounter, inputSchemas, false, selectList->getList()));
                        }
                    }

                    Node *postEvalNamedExpr = _fac.newNode(namedExpr, selItem->get(namedExprArgExpr)->getWhere(),postEvalExpr,outputNameNode);

                    postAggregationEvals.push_back(postEvalNamedExpr);

                    projectParams.push_back(std::make_shared<OperatorParamAttributeReference>(
                        newParsingContext(postEvalNamedExpr->get(namedExprArgName)),
                        "", postEvalNamedExpr->get(namedExprArgName)->getString(), true));
                }

                break;
            }

            case asterisk:
            {
                LOG4CXX_TRACE(logger, "Item is asterisk. It will be expanded to attributes.");

                if (selectListHasAggregates)
                {
                    LOG4CXX_TRACE(logger, "SELECT list contains aggregates so we can't expand asterisk");
                    fail(SYNTAX(SCIDB_LE_ITEM_MUST_BE_INSIDE_AGGREGATE2,selItem));
                }

                // We can freely omit project, if asterisk is sole item in selection list
                if (selectList->getSize() == 1)
                {
                    break;
                }

                // Otherwise prepare parameters for project
                for (size_t i = 0; i < inputSchema.getAttributes(true).size(); ++i)
                {
                    AttributeDesc const& att = inputSchema.getAttributes(true).findattr(i);
                    std::shared_ptr<OperatorParamReference> param = std::make_shared<OperatorParamAttributeReference>(
                        newParsingContext(selItem),
                        "",
                        att.getName(),
                        true);

                    resolveParamAttributeReference(inputSchemas, param);
                    projectParams.push_back(param);
                }
                break;
            }

            default:
            {
                LOG4CXX_TRACE(logger, "Unknown item. Asserting.");
                SCIDB_UNREACHABLE();
            }
        }
    }

    if (preAggregationEvals.size())
    {
        Parameters applyParams;
        // Pass list of pre-aggregate evaluations and translate it into APPLY operators
        LOG4CXX_TRACE(logger, "Translating preAggregateEval into logical operator APPLY");
        for (const Node *namedExprNode : preAggregationEvals)
        {
            assert(namedExprNode->is(namedExpr));

            // This is internal output reference which will be used for aggregation
            std::shared_ptr<OperatorParamReference> refParam = std::make_shared<OperatorParamAttributeReference>(
                newParsingContext(namedExprNode->get(namedExprArgName)),
                "", namedExprNode->get(namedExprArgName)->getString(), false);

            // This is expression which will be used as APPLY expression
            LEPtr lExpr = AstToLogicalExpression(namedExprNode->get(namedExprArgExpr));
            checkLogicalExpression(inputSchemas, ArrayDesc(), lExpr);
            std::shared_ptr<OperatorParam> exprParam = std::make_shared<OperatorParamLogicalExpression>(
                newParsingContext(namedExprNode->get(namedExprArgExpr)),
                lExpr, TypeLibrary::getType(TID_VOID));

            applyParams.push_back(refParam);
            applyParams.push_back(exprParam);
        }
        LOG4CXX_TRACE(logger, "APPLY node appended");
        result = appendOperator(result, "apply", applyParams, newParsingContext(selectList));
    }

    const vector<ArrayDesc> preEvalInputSchemas(1, result->inferTypes(_qry));   // NODE

    // Pass list of aggregates and create single AGGREGATE/REGRID/WINDOW operator
    if (aggregateFunctions.size() > 0)
    {
        LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
        //WINDOW can be used multiple times so we have array of parameters for each WINDOW
        map<string, std::pair<string, Parameters> > aggregateParams;

        if (grwAsClause)
        {
            switch (grwAsClause->getType())
            {
                case list:
                {
                    assert(isWindowClauses);
                    LOG4CXX_TRACE(logger, "Translating windows list");
                    for (const Node* windowClause : grwAsClause->getList())
                    {
                        LOG4CXX_TRACE(logger, "Translating window");
                        const Node* ranges = windowClause->get(windowClauseArgRangesList);
                        typedef std::pair<Node*, Node*> pairOfNodes;
                        vector<pairOfNodes> windowSizes(inputSchema.getDimensions().size(),
                                                        std::make_pair<Node*, Node*>(NULL, NULL));

                        size_t inputNo;
                        size_t dimNo;

                        Parameters windowParams;
                        LOG4CXX_TRACE(logger, "Translating dimensions of window");
                        bool variableWindow = false;
                        for (const Node* dimensionRange : ranges->getList())
                        {
                            variableWindow = windowClause->get(windowClauseArgVariableWindowFlag)->getBoolean();
                            Node* dimNameClause  = dimensionRange->get(windowDimensionRangeArgName);
                            chars dimAlias, dimName;
                            getAliasFromReference(dimNameClause, dimAlias, dimName);
                            resolveDimension(inputSchemas, dimName, dimAlias, inputNo, dimNo,
                                             newParsingContext(dimNameClause), true);

                            if (variableWindow)
                            {
                                LOG4CXX_TRACE(logger, "This is variable_window so append dimension name");
                                std::shared_ptr<OperatorParamReference> refParam =
                                    std::make_shared<OperatorParamDimensionReference>(
                                                        newParsingContext(dimNameClause),
                                                        dimAlias,
                                                        dimName,
                                                        true);
                                resolveParamDimensionReference(preEvalInputSchemas, refParam);
                                windowParams.push_back(refParam);
                            }


                            if (windowSizes[dimNo].first != NULL)
                            {
                                fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,dimNameClause));
                            }
                            else
                            {
                                LOG4CXX_TRACE(logger, "Append window sizes");
                                windowSizes[dimNo].first = dimensionRange->get(windowDimensionRangeArgPreceding);
                                windowSizes[dimNo].second = dimensionRange->get(windowDimensionRangeArgFollowing);
                            }
                        }

                        if (!variableWindow && ranges->getSize() < inputSchema.getDimensions().size())
                        {
                            fail(QPROC(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION,windowClause));
                        }

                        dimNo = 0;
                        Node* unboundSizeAst = NULL;
                        for (auto const& wsize : windowSizes)
                        {
                            //For variable_window we use single dimension so skip other
                            if (!wsize.first)
                                continue;
                            if (wsize.first->getInteger() < 0)
                            {
                                unboundSizeAst = _fac.newInteger(wsize.first->getWhere(),inputSchema.getDimensions()[dimNo].getLength());
                            }

                            windowParams.push_back(
                                    std::make_shared<OperatorParamLogicalExpression>(
                                        newParsingContext(wsize.first),
                                        AstToLogicalExpression(unboundSizeAst ? unboundSizeAst : wsize.first),
                                        TypeLibrary::getType(TID_VOID)));

                            if (unboundSizeAst)
                            {
                                unboundSizeAst = NULL;
                            }

                            if (wsize.second->getInteger() < 0)
                            {
                                unboundSizeAst = _fac.newInteger(wsize.second->getWhere(),
                                    inputSchema.getDimensions()[dimNo].getLength());
                            }

                            windowParams.push_back(
                                    std::make_shared<OperatorParamLogicalExpression>(
                                        newParsingContext(wsize.second),
                                        AstToLogicalExpression(unboundSizeAst ? unboundSizeAst : wsize.second),
                                        TypeLibrary::getType(TID_VOID)));

                            if (unboundSizeAst)
                            {
                                unboundSizeAst = NULL;
                            }
                        }

                        const string windowName = getString(windowClause,windowClauseArgName);

                        LOG4CXX_TRACE(logger, "Window name is: " << windowName);
                        if (aggregateParams.find(windowName) != aggregateParams.end())
                        {
                            LOG4CXX_TRACE(logger, "Such name already used. Halt.");
                            fail(QPROC(SCIDB_LE_PARTITION_NAME_NOT_UNIQUE,windowClause->get(windowClauseArgName)));
                        }

                        aggregateParams[windowName] = make_pair(variableWindow ? "variable_window" : "window", windowParams);
                    }

                    LOG4CXX_TRACE(logger, "Done with windows list");
                    break;
                }

                case regridClause:
                {
                    LOG4CXX_TRACE(logger, "Translating regrid");
                    const Node* regridDimensionsAST = grwAsClause->get(regridClauseArgDimensionsList);
                    vector<Node*> regridSizes(inputSchema.getDimensions().size(), NULL);

                    size_t inputNo;
                    size_t dimNo;

                    LOG4CXX_TRACE(logger, "Translating dimensions of window");
                    for (const Node* regridDimension : regridDimensionsAST->getList())
                    {
                        Node* dimNameClause = regridDimension->get(regridDimensionArgName);
                        chars alias, name;
                        getAliasFromReference(dimNameClause, alias, name);
                        resolveDimension(inputSchemas,
                                         name,
                                         alias,
                                         inputNo,
                                         dimNo,
                                         newParsingContext(dimNameClause),
                                         true);

                        if (regridSizes[dimNo] != NULL)
                        {
                            fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,regridDimension));
                        }
                        else
                        {
                            regridSizes[dimNo] = regridDimension->get(regridDimensionArgStep);
                        }
                    }

                    if (regridDimensionsAST->getSize() != preEvalInputSchemas[0].getDimensions().size())
                    {
                        fail(SYNTAX(SCIDB_LE_WRONG_REGRID_REDIMENSION_SIZES_COUNT,regridDimensionsAST));
                    }

                    Parameters regridParams;

                    for (Node *size : regridSizes)
                    {
                        regridParams.push_back(
                                std::make_shared<OperatorParamLogicalExpression>(
                                    newParsingContext(size),
                                    AstToLogicalExpression(size),
                                    TypeLibrary::getType(TID_VOID)));
                    }

                    aggregateParams[""] = make_pair("regrid", regridParams);
                    break;
                }
                case groupByClause:
                {
                    aggregateParams[""] = make_pair("aggregate", Parameters());
                    break;
                }
                case redimensionClause:
                {
                    LOG4CXX_TRACE(logger, "Adding schema to REDIMENSION parameters");

                    //First we iterate over all aggregates and prepare attributes for schema
                    //which will be inserted to redimension. We need to extract type of attribute
                    //from previous schema, which used in aggregate to get output type
                    set<string> usedNames;
                    Attributes redimensionAttrs;
                    for (Node *aggCallNode : aggregateFunctions)
                    {
                        const chars aggName  = getStringApplicationArgName(aggCallNode);
                        const chars aggAlias = getString(aggCallNode,applicationArgAlias);

                        Type aggParamType;
                        if (aggCallNode->get(applicationArgOperands)->get(listArg0)->is(asterisk))
                        {
                            LOG4CXX_TRACE(logger, "Getting type of " << aggName << "(*) as " << aggAlias);
                            aggParamType = TypeLibrary::getType(TID_VOID);
                        }
                        else
                        if (aggCallNode->get(applicationArgOperands)->get(listArg0)->is(reference))
                        {
                            Node* refNode = aggCallNode->get(applicationArgOperands)->get(listArg0);
                            Node *ns, *ary, *var;
                            splitReferenceArgNames(refNode, ns, ary, var);
                            if (ns) {
                                // XXX should we fail if ary != nullptr too???  See prev calls too?
                                // XXX TODO Support it.
                                fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, refNode) <<
                                     "Namespace qualifier not supported in this context");
                            }
                            const chars aggAttrName = getStringVariableName(var);

                            LOG4CXX_TRACE(logger, "Getting type of " << aggName
                                          << "(" << aggAttrName << ") as " << aggAlias);

                            for (size_t i = 0; i < preEvalInputSchemas[0].getAttributes().size(); ++i)
                            {
                                AttributeDesc const& attr = preEvalInputSchemas[0].getAttributes().findattr(i);
                                if (attr.getName() == aggAttrName)
                                {
                                    aggParamType = TypeLibrary::getType(attr.getType());
                                    break;
                                }
                            }
                        }
                        else
                        {
                            SCIDB_UNREACHABLE();
                        }

                        redimensionAttrs.push_back(
                            AttributeDesc(aggAlias,
                                          AggregateLibrary::getInstance()->createAggregate(
                                              aggName, aggParamType)->getResultType().typeId(),
                                          AttributeDesc::IS_NULLABLE,
                                          CompressorType::NONE));
                        usedNames.insert(aggAlias);
                    }

                    //Now prepare dimensions
                    Dimensions redimensionDims;
                    passDimensions(grwAsClause->get(listArg0), redimensionDims, "", usedNames);

                    //Ok. Adding schema parameter
                    ArrayDesc redimensionSchema = ArrayDesc("", redimensionAttrs.addEmptyTagAttribute(),
                                                            redimensionDims,
                                                            createDistribution(defaultDistType()),
                                                            _qry->getDefaultArrayResidency(),
                                                            0);
                    LOG4CXX_TRACE(logger, "Schema for redimension " <<  redimensionSchema);
                    aggregateParams[""] = make_pair("redimension",
                        Parameters(1,
                            std::make_shared<OperatorParamSchema>(newParsingContext(grwAsClause),redimensionSchema)));

                    break;
                }
                default: SCIDB_UNREACHABLE();
            }
        }
        else
        {
            //No additional parameters to aggregating operators
            aggregateParams[""] = make_pair("aggregate", Parameters());
        }

        for (Node *aggCallNode : aggregateFunctions)
        {
            LOG4CXX_TRACE(logger, "Translating aggregate into logical aggregate call");
            if (aggCallNode->is(application))
            {
                if (isWindowClauses && grwAsClause->getSize() > 1)
                {
                    fail(QPROC(SCIDB_LE_PARTITION_NAME_NOT_SPECIFIED,aggCallNode));
                }

                aggregateParams.begin()->second.second.push_back(passAggregateCall(aggCallNode, preEvalInputSchemas));
            }
            else
            if (aggCallNode->is(olapAggregate))
            {
                const string partitionName = getString(aggCallNode,olapAggregateArgPartitionName);
                if (aggregateParams.end() == aggregateParams.find(partitionName))
                {
                    fail(QPROC(SCIDB_LE_UNKNOWN_PARTITION_NAME,aggCallNode->get(olapAggregateArgPartitionName)));
                }

                aggregateParams[partitionName].second.push_back(passAggregateCall(aggCallNode->get(olapAggregateArgApplication), preEvalInputSchemas));
            }
            else
            {
                SCIDB_UNREACHABLE();
            }
        }

        if (grwAsClause && grwAsClause->is(groupByClause))
        {
            Node *ns, *ary, *var;
            for (const Node *groupByItem : grwAsClause->getList(groupByClauseArgList))
            {
                assert(groupByItem->is(reference));

                if (groupByItem->has(referenceArgVersion))
                {
                    fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,groupByItem));
                }

                splitReferenceArgNames(groupByItem, ns, ary, var);
                if (ns) {
                    fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, groupByItem) <<
                         "Namespace qualifier not supported in this context");
                }

                std::shared_ptr<OperatorParamReference> refParam =
                    std::make_shared<OperatorParamDimensionReference>(
                        newParsingContext(var),
                        getStringVariableName(ary),
                        getStringVariableName(var),
                        true);
                resolveParamDimensionReference(preEvalInputSchemas, refParam);
                aggregateParams[""].second.push_back(refParam);
                break;
            }
        }

        LOG4CXX_TRACE(logger, "AGGREGATE/REGRID/WINDOW node appended");

        auto it = aggregateParams.begin();

        LQPNPtr left = appendOperator(result, it->second.first,it->second.second, newParsingContext(selectList));

        ++it;

        for (;it != aggregateParams.end(); ++it)
        {
            LQPNPtr right = appendOperator(result, it->second.first,it->second.second, newParsingContext(selectList));

            LQPNPtr node = std::make_shared<LogicalQueryPlanNode>(
                newParsingContext(selectList), OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(left);
            node->addChild(right);
            left = node;
        }

        result = left;
    }

    if (joinOrigin)
    {
        LQPNPtr node = std::make_shared<LogicalQueryPlanNode>(
            newParsingContext(selectList),
            OperatorLibrary::getInstance()->createLogicalOperator("join"));
            node->addChild(result);
            node->addChild(input);
        result = node;
    }

    const vector<ArrayDesc> aggInputSchemas(1, result->inferTypes(_qry)); // NODE

    if (postAggregationEvals.size())
    {
        Parameters applyParams;
        // Finally pass all post-aggregate evaluations and translate it into APPLY operators
        LOG4CXX_TRACE(logger, "Translating postAggregateEval into logical operator APPLY");
        for (const Node *namedExprNode : postAggregationEvals)
        {
            assert(namedExprNode->is(namedExpr));

            // This is user output. Final attribute name will be used from AS clause (it can be defined
            // by user in _qry or generated by us above)
            applyParams.push_back(std::make_shared<OperatorParamAttributeReference>(
                newParsingContext(namedExprNode->get(namedExprArgName)),
                "",
                getString(namedExprNode,namedExprArgName),
                false));

            // This is expression which will be used as APPLY expression
            LEPtr lExpr = AstToLogicalExpression(namedExprNode->get(namedExprArgExpr));
            checkLogicalExpression(aggInputSchemas, ArrayDesc(), lExpr);
            std::shared_ptr<OperatorParam> exprParam = std::make_shared<OperatorParamLogicalExpression>(
                newParsingContext(namedExprNode->get(namedExprArgExpr)),
                lExpr, TypeLibrary::getType(TID_VOID));

            applyParams.push_back(exprParam);
        }

        result = appendOperator(result, "apply", applyParams, newParsingContext(selectList));
    }

    const vector<ArrayDesc> postEvalInputSchemas(1, result->inferTypes(_qry)); // NODE

    if (projectParams.size() > 0)
    {
        for (auto& param : projectParams)
        {
            resolveParamAttributeReference(postEvalInputSchemas, (std::shared_ptr<OperatorParamReference>&) param);
        }

        result = appendOperator(result, "project", projectParams, newParsingContext(selectList));
    }

    return result;
}

string Translator::genUniqueObjectName(const string& prefix, unsigned int &initialCounter,const vector<ArrayDesc> &inputSchemas, bool internal, cnodes namedExpressions)
{
    string name;

    while(true)
    {
        nextName:

        if (initialCounter == 0)
        {
            name = str(boost::format("%s%s%s")
                % (internal ? "$" : "")
                % prefix
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }
        else
        {
            name = str(boost::format("%s%s_%d%s")
                % (internal ? "$" : "")
                % prefix
                % initialCounter
                % (internal ? "$" : "")
                );
            ++initialCounter;
        }

        for (ArrayDesc const& schema : inputSchemas)
        {
            for (size_t i = 0; i < schema.getAttributes().size(); ++i)
            {
                AttributeDesc const& att = schema.getAttributes().findattr(i);
                if (att.getName() == name)
                    goto nextName;
            }

            for (DimensionDesc const& dim : schema.getDimensions())
            {
                if (dim.hasNameAndAlias(name, ""))
                    goto nextName;
            }

            for (const Node* ast : namedExpressions)
            {
                if (ast->is(namedExpr) && getString(ast,namedExprArgName) == name)
                    goto nextName;
            }
        }

        break;
    }

    return name;
}

LQPNPtr Translator::passThinClause(const Node *ast, size_t depthOperator)
{
    checkDepthOperator(depthOperator);

    LOG4CXX_TRACE(logger, "Translating THIN clause");
    typedef std::pair<Node*,Node*>   PairOfNodes;

    const Node* arrayRef = ast->get(thinClauseArgArrayReference);

    LQPNPtr result = AstToLogicalPlan(arrayRef, depthOperator+1);

    prohibitNesting(result);

    const ArrayDesc& thinInputSchema = result->inferTypes(_qry); // NODE

    vector<PairOfNodes> thinStartStepList(thinInputSchema.getDimensions().size());

    size_t inputNo;
    size_t dimNo;

    for (PairOfNodes& startStep : thinStartStepList)
    {
        startStep.first = startStep.second = NULL;
    }

    LOG4CXX_TRACE(logger, "Translating THIN start-step pairs");
    for (const Node* thinDimension : ast->getList(thinClauseArgDimensionsList))
    {
        const Node* dimNameClause = thinDimension->get(thinDimensionClauseArgName);
        chars alias, name;
        getAliasFromReference(dimNameClause, alias, name);

        resolveDimension(
                vector<ArrayDesc>(1, thinInputSchema),
                name,
                alias,
                inputNo,
                dimNo,
                newParsingContext(dimNameClause),
                true);

        if (thinStartStepList[dimNo].first != NULL)
        {
            fail(QPROC(SCIDB_LE_MULTIPLE_DIMENSION_SPECIFICATION,dimNameClause));
        }
        else
        {
            thinStartStepList[dimNo].first = thinDimension->get(thinDimensionClauseArgStart);
            thinStartStepList[dimNo].second = thinDimension->get(thinDimensionClauseArgStep);
        }
    }

    if (ast->get(thinClauseArgDimensionsList)->getSize() < thinInputSchema.getDimensions().size())
    {
        fail(QPROC(SCIDB_LE_NOT_ENOUGH_DIMENSIONS_IN_SPECIFICATION,ast->get(thinClauseArgDimensionsList)));
    }

    Parameters thinParams;
    for (PairOfNodes& startStep : thinStartStepList)
    {
        thinParams.push_back(
            std::make_shared<OperatorParamLogicalExpression>(
                newParsingContext(startStep.first),
                AstToLogicalExpression(startStep.first),
                TypeLibrary::getType(TID_VOID)));
        thinParams.push_back(
            std::make_shared<OperatorParamLogicalExpression>(
                newParsingContext(startStep.second),
                AstToLogicalExpression(startStep.second),
                TypeLibrary::getType(TID_VOID)));
    }

    result = appendOperator(result, "thin", thinParams, newParsingContext(ast));

    return result;
}

void Translator::prohibitNesting(const LQPNPtr& planNode)
{
    // Nesting DDL makes no sense, since there's no output.
    if (planNode->isDdl())
    {
        fail(QPROC(SCIDB_LE_DDL_CANT_BE_NESTED,planNode->getParsingContext()));
    }

    // To avoid update anomalies, we prevent clients from nesting
    // operators that update persistent storage.  SDB-5447.
    if (planNode->getLogicalOperator()->getProperties().updater && !_isMulti)
    {
        fail(QPROC(SCIDB_LE_NESTING_PROHIBITED, planNode->getParsingContext())
             << planNode->getLogicalOperator()->getLogicalName());
    }
}

void Translator::passReference(const Node* ast,chars& alias,chars& name)
{
    if (ast->has(referenceArgVersion))
    {
        fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,ast->get(referenceArgVersion)));
    }

    if (ast->has(referenceArgOrder))
    {
        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
    }

    getAliasFromReference(ast, alias, name);
}

LQPNPtr Translator::fitInput(LQPNPtr &input,const ArrayDesc& destinationSchema)
{
    ArrayDesc inputSchema = input->inferTypes(_qry); // NODE
    LQPNPtr fittedInput = input;

    if (!inputSchema.getEmptyBitmapAttribute()
        && destinationSchema.getEmptyBitmapAttribute())
    {
        vector<std::shared_ptr<OperatorParam> > betweenParams;
        for (size_t i=0, n=destinationSchema.getDimensions().size(); i<n; ++i)
        {
            Value bval(TypeLibrary::getType(TID_INT64));
            bval.setNull();
            std::shared_ptr<OperatorParamLogicalExpression> param = std::make_shared<OperatorParamLogicalExpression>(
                input->getParsingContext(),
                std::make_shared<Constant>(input->getParsingContext(), bval, TID_INT64),
                TypeLibrary::getType(TID_INT64), true);
            betweenParams.push_back(param);
            betweenParams.push_back(param);
        }

        fittedInput = appendOperator(input, "between", betweenParams, input->getParsingContext());
        inputSchema = fittedInput->inferTypes(_qry); // NODE
    }

    bool needCast = false;
    bool needRepart = false;

    //Give up on casting if schema objects count differ. Nothing to do here.
    if (destinationSchema.getAttributes().size()
        == inputSchema.getAttributes().size()
        && destinationSchema.getDimensions().size()
            == inputSchema.getDimensions().size())
    {
        for (size_t attrNo = 0; attrNo < inputSchema.getAttributes().size();
            ++attrNo)
        {
            const AttributeDesc &inAttr =
                destinationSchema.getAttributes().findattr(attrNo);
            const AttributeDesc &destAttr = inputSchema.getAttributes().findattr(attrNo);

            //If attributes has differ names we need casting...
            if (inAttr.getName() != destAttr.getName())
                needCast = true;

            //... but if type and flags differ we can't cast
            if (inAttr.getType() != destAttr.getType()
                || inAttr.getFlags() != destAttr.getFlags())
            {
                needCast = false;
                goto noCastAndRepart;
            }
        }

        for (size_t dimNo = 0; dimNo < inputSchema.getDimensions().size();
            ++dimNo)
        {
            const DimensionDesc &destDim =destinationSchema.getDimensions()[dimNo];
            const DimensionDesc &inDim = inputSchema.getDimensions()[dimNo];

            //If dimension has differ names we need casting...
            if (inDim.getBaseName() != destDim.getBaseName())
                needCast = true;

            //If dimension has different chunk size, we need repart.
            //("Autochunked" matches anything, so no repart needed there.)
            if (inDim.getChunkOverlap() != destDim.getChunkOverlap()
             || (!destDim.isAutochunked()
                 && inDim.getChunkInterval() != destDim.getChunkInterval()))
                needRepart = true;

            //... but if length or type of dimension differ we cant cast and repart
            if (inDim.getStartMin() != destDim.getStartMin()
                || !(inDim.getEndMax() == destDim.getEndMax()
                    || (inDim.getEndMax() < destDim.getEndMax()
                        && ((inDim.getLength() % inDim.getChunkInterval()) == 0
                            || inputSchema.getEmptyBitmapAttribute() != NULL))))
            {
                needCast = false;
                needRepart = false;
                goto noCastAndRepart;
            }
        }

    }

    noCastAndRepart:

    try
    {
        if (needRepart)
        {
            LOPtr repartOp;
            LOG4CXX_TRACE(logger, "Inserting REPART operator");
            repartOp = OperatorLibrary::getInstance()->createLogicalOperator("repart");

            Parameters repartParams(1,
                std::make_shared<OperatorParamSchema>(input->getParsingContext(),destinationSchema));
            repartOp->setParameters(repartParams);

            LQPNPtr tmpNode = std::make_shared<LogicalQueryPlanNode>(input->getParsingContext(), repartOp);
            tmpNode->addChild(fittedInput);
            tmpNode->inferTypes(_qry); // NODE
            fittedInput = tmpNode;
        }

        if (needCast)
        {
            LOPtr castOp;
            LOG4CXX_TRACE(logger, "Inserting CAST operator");
            castOp = OperatorLibrary::getInstance()->createLogicalOperator("cast");

            Parameters castParams(1,
                std::make_shared<OperatorParamSchema>(input->getParsingContext(),destinationSchema));
            castOp->setParameters(castParams);

            LQPNPtr tmpNode = std::make_shared<LogicalQueryPlanNode>(input->getParsingContext(), castOp);
            tmpNode->addChild(fittedInput);
            tmpNode->inferTypes(_qry); // NODE
            fittedInput = tmpNode;
        }
    }
    catch (const UserException& e)
    {
        if (SCIDB_SE_INFER_SCHEMA == e.getShortErrorCode())
        {
            LOG4CXX_TRACE(logger, "Can not infer schema from REPART and/or CAST. Give up.");
        }
        else
        {
            LOG4CXX_TRACE(logger, "Something going wrong");
            e.raise();
        }
    }

    return fittedInput;
}

LQPNPtr Translator::canonicalizeTypes(const LQPNPtr &input)
{
    LOG4CXX_TRACE(logger, "Types canonicalization");
    const ArrayDesc& inputSchema = input->inferTypes(_qry); // NODE
    bool skip = true;
    for (const auto& attr : inputSchema.getAttributes())
    {
        if(!isBuiltinType(attr.getType()))
        {
            skip = false;
            break;
        }
    }

    if (skip)
    {
        return input;
    }

    ContextPtr pc = input->getParsingContext();
    vector<std::shared_ptr<OperatorParam> > castParams(1);

    Attributes attrs;
    for (const auto& attr : inputSchema.getAttributes())
    {
        TypeId attType;
        if(isBuiltinType(attr.getType()))
        {
            attType = attr.getType();
        }
        else
        {
            attType = TID_STRING;
        }
        AttributeDesc newAtt(
                attr.getName(),
                attType,
                attr.getFlags(),
                attr.getDefaultCompressionMethod(),
                attr.getAliases(),
                attr.getReserve());
        attrs.push_back(newAtt);
    }

    ArrayDesc castSchema(
            inputSchema.getId(),
            inputSchema.getUAId(),
            inputSchema.getVersionId(),
            inputSchema.getName(),
            attrs,
            inputSchema.getDimensions(),
            inputSchema.getDistribution(),
            inputSchema.getResidency(),
            inputSchema.getFlags());

    castParams[0] = std::make_shared<OperatorParamSchema>(pc, castSchema);

    return appendOperator(input, "cast", castParams, pc);
}

/****************************************************************************/
/* Expressions */

LEPtr Translator::AstToLogicalExpression(const Node* ast, size_t depthExpression)
{
    checkDepthExpression(depthExpression);

    switch (ast->getType())
    {
        case cnull:             return onNull(ast);
        case creal:             return onReal(ast);
        case cstring:           return onString(ast);
        case cboolean:          return onBoolean(ast);
        case cinteger:          return onInteger(ast);
        case application:       return onScalarFunction(ast, depthExpression);
        case reference:         return onAttributeReference(ast);
        case questionMark:      return onQuestionMark(ast);
        case olapAggregate:     fail(SYNTAX(SCIDB_LE_WRONG_OVER_USAGE,ast));
        case asterisk:          fail(SYNTAX(SCIDB_LE_WRONG_ASTERISK_USAGE,ast));
        case selectArray:       fail(SYNTAX(SCIDB_LE_SUBQUERIES_NOT_SUPPORTED,ast));
        default:                fail(INTERNAL(SCIDB_LE_UNKNOWN_ERROR,ast) << ast->getType());
    }

    SCIDB_UNREACHABLE();
    return LEPtr();
}

LEPtr Translator::onNull(const Node* ast)
{
    assert(ast->is(cnull));

    Value c; c.setNull();

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_VOID);
}

LEPtr Translator::onReal(const Node* ast)
{
    assert(ast->is(creal));

    Value c(TypeLibrary::getType(TID_DOUBLE));
    c.setDouble(ast->getReal());

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_DOUBLE);
}

LEPtr Translator::onString(const Node* ast)
{
    assert(ast->is(cstring));

    Value c(TypeLibrary::getType(TID_STRING));
    c.setString(ast->getString());

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_STRING);
}

LEPtr Translator::onBoolean(const Node* ast)
{
    assert(ast->is(cboolean));

    Value c(TypeLibrary::getType(TID_BOOL));
    c.setBool(ast->getBoolean());

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_BOOL);
}

LEPtr Translator::onInteger(const Node* ast)
{
    assert(ast->is(cinteger));

    Value c(TypeLibrary::getType(TID_INT64));
    c.setInt64(ast->getInteger());

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_INT64);
}

LEPtr Translator::onScalarFunction(const Node* ast, size_t depthExpression)
{
    checkDepthExpression(depthExpression);

    assert(ast->is(application));

    chars         name(getStringApplicationArgName(ast));
    vector<LEPtr> args;

    if (OperatorLibrary::getInstance()->hasLogicalOperator(name))
    {
        fail(SYNTAX(SCIDB_LE_UNEXPECTED_OPERATOR_IN_EXPRESSION,ast));
    }

    for (const Node* a : ast->getList(applicationArgOperands))
    {
        args.push_back(AstToLogicalExpression(a, depthExpression+1));
    }

    return std::make_shared<Function>(newParsingContext(ast),name,args);
}

LEPtr Translator::onAttributeReference(const Node* ast)
{
    assert(ast->is(reference));

    if (ast->has(referenceArgVersion))
    {
        fail(SYNTAX(SCIDB_LE_REFERENCE_EXPECTED,ast->get(referenceArgVersion)));
    }

    if (ast->has(referenceArgOrder))
    {
        fail(SYNTAX(SCIDB_LE_SORTING_QUIRK_WRONG_USAGE,ast->get(referenceArgOrder)));
    }

    Node *ns, *ary, *var;
    splitReferenceArgNames(ast, ns, ary, var);
    if (ns) {
        // XXX TODO Support it.
        fail(SYNTAX(SCIDB_LE_UNKNOWN_ERROR, ast) <<
             "Namespace qualifier not supported in this context");
    }

    return std::make_shared<AttributeReference>(
            newParsingContext(ast),
            getStringVariableName(ary),
            getStringVariableName(var));
}

LEPtr Translator::onQuestionMark(const Node* ast)
{
    assert(ast->is(questionMark));

    Value c; c.setNull();

    return std::make_shared<Constant>(newParsingContext(ast),c,TID_VOID);
}

void Translator::checkDepthExpression(size_t depthExpression)
{
    static const size_t MAX_DEPTH_EXPRESSION = 400; // ~444 - margin

    if (depthExpression >= MAX_DEPTH_EXPRESSION) {
        throw USER_EXCEPTION(SCIDB_SE_PARSER,
                             SCIDB_LE_EXPRESSION_HAS_TOO_MANY_OPERANDS)
        << MAX_DEPTH_EXPRESSION;
    }
}

void Translator::checkDepthOperator(size_t depthOperator)
{
    static const size_t MAX_DEPTH_OPERATOR = 90; // ~95 - margin

    if (depthOperator >= MAX_DEPTH_OPERATOR) {
        throw USER_EXCEPTION(SCIDB_SE_PARSER,
                             SCIDB_LE_QUERY_HAS_TOO_DEEP_NESTING_LEVELS)
        << MAX_DEPTH_OPERATOR;
    }
}

/****************************************************************************/
LEPtr translateExp(Factory& f,Log& l,const StringPtr& s,Node* n,const QueryPtr& q)
{
    return Translator(f,l,s,q).AstToLogicalExpression(n);
}

LQPNPtr translatePlan(Factory& f,Log& l,const StringPtr& s,Node* n,const QueryPtr& q)
{
    const size_t depthOperator = 0;
    LQPNPtr p = Translator(f,l,s,q).AstToLogicalPlan(n, depthOperator, true);
    return p;
}

/****************************************************************************/
}}
/****************************************************************************/
