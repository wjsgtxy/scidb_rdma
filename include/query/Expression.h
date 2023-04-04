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
 * @file Expression.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Expression evaluator that compiles logical expression and use inline Value to perform
 * fast evaluations.
 */

#ifndef EXPRESSION_H_
#define EXPRESSION_H_

#include <boost/serialization/vector.hpp>
#include <boost/shared_array.hpp>

#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <util/SpatialType.h>  // SpatialRangesPtr

#include <array/ArrayDesc.h>
#include <unordered_map>

namespace scidb
{
class LogicalExpression;

/// @brief A struct that describes the origin of a variable that appears in an expression.
/// @note Assume there is a vector of input schemas, and an output schema.
struct BindInfo
{
    enum {
        BI_ATTRIBUTE,   ///< the variable is an attribute.
        BI_COORDINATE,  ///< the variable is a dimension.
        BI_VALUE        ///< neither an attribute nor a dimension, e.g. a constant.
    } kind;


    /// @brief Which input schema contains the variable.
    /// - If the variable is an attribute or dimension that comes from one input schema out of vector<ArrayDesc>,
    ///   inputNo is the index in the vector representing the input schema.
    ///   resolvedId is the attribute ID in the input schema.
    /// - If the variable is an attribute or dimension that comes from the output schema,
    ///   inputNo is ~0.
    ///   resolvedId is the attribute ID in the output schema.
    /// - If the variable is BI_VALUE,
    ///   inputNo and resolvedId are undefined.
    size_t inputNo;

    size_t resolvedId;  ///< @see inputNo.

    TypeId type;
    Value value;

    bool operator==(const BindInfo& bind) const
    {
        return bind.kind == kind && bind.inputNo == inputNo && bind.resolvedId == resolvedId && bind.type == type;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & (int&)kind;
        ar & inputNo;
        ar & resolvedId;
        ar & type;
        ar & value;
    }
};

/**
 * @brief Use this class to hold context of Expression used for evaluations.
 * It makes Expression evaluations reentrant.
 */
class ExpressionContext
{
friend class Expression;
private:
    /**
     * A vector of Value objects where context variables should go.
     * Pointers to these values can be used multiple times.
     */
    std::vector<Value> _context;
    class Expression& _expression;
    std::vector<Value*> _args; /**< Pointers to Value objects which will be used for evaluations. */
    std::vector<Value> _vargs; /**< Value objects which will be used for evaluations. */
    bool _contextChanged;
    std::vector<boost::shared_array<char>> _state;

public:
    ExpressionContext(Expression& expression);

    const Value& operator[](size_t i) const;
    Value& operator[](size_t i);
};

// Defined in Expression.cpp that implements extractSpatialConstraints().
class FilterHelper;

/**
 * @brief Class Expression evaluates an expression using Value objects.
 * To do this it will use FunctionLibrary to find function with related types.
 * If there is no such functions Expression tries to insert converter which will be searched
 * in the FunctionLibrary too.
 */
class Expression
{
friend class ExpressionContext;
friend class FilterHelper;

public:
    Expression()
            : _compiled(false)
            , _nullable(false)
            , _constant(false)
            , _tileMode(false)
            , _tempValuesNumber(0)
            , _functions()
            , _arguments(1)
            , _isDeterministic(true)
            , _isInstIdentical(true)
    {
    }

    Expression(const Expression&) = default;

    virtual ~Expression() {}

    /**
     * @brief Compile the Expression from a given LogicalExpression.
     * @param expr  a pointer to logical expression tree to be compiled.
     * @param tile  tile mode.
     * @param expectedType  a type of the expression result that is expected by user.
     * @param inputSchemas  the input array schema.
     * @param outputSchema  the output array schema.
     */
    void compile(std::shared_ptr<LogicalExpression> expr,
                 bool tile,
                 TypeId expectedType =  TID_VOID,
                 const std::vector< ArrayDesc >& inputSchemas = std::vector< ArrayDesc >(),
                 const ArrayDesc& outputSchema = ArrayDesc());

    /**
     * @brief Compile the Expression from a string.
     *
     * @param expression   a string with expression that should be compiled.
     * @param tile         tile mode.
     * @param names        strings with names of variables which are used in expression.
     * @param types        a vector of variable types.
     * @param expectedType the expected type of result.
     */
    void compileExpressionString(
        const std::string& expression,
        bool tile,
        const std::vector<std::string>& names,
        const std::vector<TypeId>& types,
        TypeId expectedType = TID_VOID);

    /**
     * @brief Compile the expression from a constant.
     *
     * @param tile  tile mode.
     * @param type  the type id of the expression.
     * @param value an value of constant.
     */
    void compileConstant(bool tile, const TypeId& type, const Value& value);

    /**
     * @brief Evaluate the current Expression under a given ExpressionContext.
     * @param context  the context under which to evaluate the current expression.
     *
     * @return a constant reference to evaluated Value object.
     */
    const Value& evaluate(ExpressionContext& context);

    /**
     * @brief Evaluate the current expression "in place", i.e. not under any extra context.
     * @return the evaluated Value object.
     */
    Value evaluate() {
        ExpressionContext e(*this);
        return evaluate(e);
    }

    /**
     * @return a resulting type of expression
     */
     TypeId getType() const {
        return _resultType;
    }

    bool supportsTileMode() const {
        return _tileMode;
    }

    bool isNullable() const {
        return _nullable;
    }

    bool isConstant() const {
        return _arguments.size() > 0 && (_arguments[0].isConst || _arguments[0].isConstantFunction);
    }

    const std::vector<BindInfo>& getBindings() const {
        return _bindings;
    }

    /**
     * Serialize expression into boost::Archive for transfer to remote instances with
     * physical plan.
     */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        const bool hadFunctions = _functions.size(); // True for loading
	if (Archive::is_loading::value) {
	  _arguments.clear();
	}

        ar & _resultType;
        ar & _bindings;
        ar & _contextNo;
        ar & _arguments;
        ar & _functions;
        ar & _compiled;
        ar & _tileMode;
        ar & _tempValuesNumber;

        if (!hadFunctions && _functions.size()) {
            resolveFunctions();
        }
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    virtual void toString (std::ostream &out, int indent = 0) const;

    /**
     * @brief Extract dimension clauses.
     * @param[out] spatialRangesPtr  every dim_conjunctions leads to a spatial range.
     * @param[out] hasOtherClauses   whether there are additional clauses other than dimension clauses.
     *
     * The general form of expression that we want to see is:
     *     (dim_conjunctions || ...) && other_clauses.
     * Here dim_conjunctions is AND-separated dimensionName OP constant comparisons, where OP is one of <, >, <=, >=, =.
     * For example:
     *     dim1>=5 and dim1<=100 and dim2>5 and dim3==4
     * Under the hood, each dim_conjunctions gets translated into a spatial range.
     */
    void extractSpatialConstraints(
        std::shared_ptr<SpatialRanges>& spatialRangesPtr,
        bool& hasOtherClauses);

    /// @brief Return a new expression derived by applying an internal negate() function over *this.
    /// The negate() function is like the built-in not():
    ///  - negate(true) = false
    ///  - negate(false) = true
    /// The difference is that:
    ///  - negate(null) = true;  note that not(null), which evaluates to null, is considered false.
    std::shared_ptr<Expression> negate() const;

    /**
     * Does this expression result in the same output for some given inputs on
     * repeated invocations?
     * @return true if deterministic, false if not
     */
    bool isDeterministic() const;

    /**
     * @return whether this expression returns the same result on every instance
     *         given the same arguments.
     * TODO: consider whether we need both isDeterministic() and
     *       isInstIdentical() and if so, give them names which
     *       allow easy selection of the correct one to use.
     */
    bool isInstIdentical() const;

private:  // types
    /**
     * @brief A structure to hold a compiled function.
     */
    struct CompiledFunction
    {
        FunctionPointer function;  ///< A pointer to a function.
        size_t argIndex; ///< the index in _arguments storing the first argument for the function.
                         ///< Note that a function's arguments have consecutive indices.
        size_t resultIndex;  ///< the index in _arguments storing the function's result.

        // TODO: next 2 fields should be replaced by FunctionDescriptor later for flexibility
        std::string functionName;  ///< A name of function for debug and serialization purposes
        std::vector< TypeId> functionTypes;  ///< Function type ids for debug and serialization purposes

        /// @brief The value (of one argument) that will trigger skipping evaluation of another argument.
        /// The evaluation of a given argument of functions and(), or(), and iif() may be skipped,
        /// if another argument (at skipIndex in _arguments) possesses a certain value (equal to skipValue).
        /// - and(skipIndexArg, currentArg): Skip evaluating currentArg if the skipIndexArg has skipValue=false.
        /// - or(skipIndexArg, currentArg): Skip evaluating currentArg if the skipIndexArg has skipValue=true.
        /// - iif(skipIndexArg, -, currentArg): Skip evaluating currentArg if the skipIndexArg has skipValue=true.
        /// - iif(skipIndexArg, currentArg, -): Skip evaluating currentArg if the skipIndexArg has skipValue=false.
        bool skipValue;
        size_t skipIndex; ///< @see skipValue.

        size_t stateSize;

        CompiledFunction(): skipValue(false), skipIndex(0), stateSize(0)
        {
        }

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version)
        {
            ar & argIndex;
            ar & resultIndex;
            ar & functionName;
            ar & functionTypes;
            ar & skipValue;
            ar & skipIndex;
        }
    };  // struct CompiledFunction

    /**
     * @brief A structure to hold one argument of a function.
     * @note The structure also serves as the purpose of holding the result of a sub-expression.
     */
    struct Argument
    {
        TypeId type;
        bool isConst;  ///< true if value present and is constant
        bool isNull;   ///< true if isConst value (which may be a tile) is null
        bool isConstantFunction;
        Value value;

        Argument()
            : type(TID_VOID)
            , isConst(false)
            , isNull(false)
            , isConstantFunction(false)
        {}

        template<class Archive>
        void serialize(Archive& ar, const unsigned int version)
        {
            ar & type;
            ar & isConst;
            ar & isNull;
            ar & isConstantFunction;
            ar & value;
        }
    };  // struct Argument

  /// @brief key = variable name; value = an index in _bindings.
  using NameToIndex = std::unordered_map<std::string, size_t>;

private:  // methods
    /**
     * @brief Resolves an attribute or dimension reference to BindInfo.
     * @param ref  a name of attribute or dimension
     * @param nameToIndexInBindings  a map for translating a variable name to an index in _bindings.
     * @return  a BindInfo structure with resolved IDs.
     */
    BindInfo resolveContext(const std::shared_ptr<AttributeReference>& ref,
                            NameToIndex const*const nameToIndexInBindings);

    /**
     * @brief Resolves an attribute or dimension name to BindInfo.
     *
     * @param arrayDesc      an array schema.
     * @param arrayName      the name of an array.
     * @param referenceName  a name of attribute or dimension to resolve.
     * @return  a BindInfo structure with resolved IDs.
     */
    BindInfo resolveContext(const ArrayDesc& arrayDesc, const std::string& arrayName,
                            const std::string& referenceName);

    /**
     * @brief Swap two consecutive arguments in _arguments.
     * @param firstIndex  The index of the first argument.
     * @note The function retrieves the functions that produce results at _arguments[firstIndex]
     *       and _arguments[firstIndex+1] and swaps them as well. Similarly, _contextNos is also
     *       updated.
     */
    void swapArguments(size_t firstIndex);

    /**
     * @brief Compile a sub-expression into _arguments[resultIndex].
     * @param subExpr     the logical expression to compile.
     * @param tile        tile mode.
     * @param resultIndex the index in _arguments at which the result should be stored.
     * @param skipIndex   see CompiledFunction::skipIndex.
     * @param skipValue   see CompiledFunction::skipValue.
     * @param nameToIndexInBindings  a map for translating a variable name to an index in _bindings.
     *                               To be passed to resolveContext() to resolve a name.
     * @pre If nameToIndexInBindings is not nullptr, before calling this function _bindings must
     *      have been expanded to include one entry per variable in nameToIndexInBindings.
     * @note In case the sub-expression is a function, the method expands _arguments by the number
     *       of function arguments, and makes recursive calls for each argument in reserve order.
     * @example
     *   Suppose compile() calls internalCompile() with an expression "a + 30".
     *   @verbatim
     *                            -----------------
     *   - Initially, _arguments = | PH for result |, a vector with a one-element placeholder.
     *                            -----------------
     *   - Since the expression is a function with two arguments, internalCompile() pushes function "+"
     *     to _functions, and expands _arguments to:
     *                 ---------------------------------------------------
     *     _arguments = | PH for result | PH for arg one | PH for arg two |
     *                 ---------------------------------------------------
     *   - A recursive call is made to argument two (i.e. 30),
     *                  -------------------------------------------------
     *     _arguments = | PH for result | PH for arg one | constant, 30 |
     *                  -------------------------------------------------
     *   - A recursive call is made to argument one (i.e. a),
     *                  -----------------------------------------------------
     *     _arguments = | PH for result | attribute ref, "a" | constant, 30 |
     *                  -----------------------------------------------------
     *   - After internalCompile() returns,
     *                  -----------------------------------------------------
     *     _arguments = | function, a+3 | attribute ref, "a" | constant, 30 |
     *                  -----------------------------------------------------
     *   @endverbatim
     */
    void internalCompile(std::shared_ptr<LogicalExpression> subExpr, bool tile,
                         size_t resultIndex, size_t skipIndex, bool skipValue,
                         NameToIndex const*const nameToIndexInBindings);

    /**
     * @brief Internal impl for the version of compile that takes a LogicalExpression as argument.
     *
     * @param nameToIndexInBindings  for internal use only.
     * See compile(std::shared_ptr<LogicalExpression> expr, ...) for arguments except the last.
     *
     * @note The function is introduced to support both public versions of compile().
     * - Algorithm compile(shared_ptr<LogicalExpression>, ...):
     *   - clear()
     *   - compileCommon(..., nullptr)
     * - Algorithm compile(const std::string&, ...):
     *   - Build _bindings and nameToIndexInBindings from the input variable names and types.
     *   - compileCommon(..., &nameToIndexInBindings)
     */
    void compileCommon(std::shared_ptr<LogicalExpression> expr,
                 bool tile,
                 TypeId expectedType,
                 const std::vector<ArrayDesc >& inputSchemas,
                 const ArrayDesc& outputSchema,
                 NameToIndex const*const nameToIndexInBindings);

    /**
     * The function inserts given converter to given position
     *
     * @param functionPointer a pointer to converter to be inserted
     * @param resultIndex an index in _args vector where type converter must save a result
     * @oaram functionIndex an index of function after that converter must be inserted
     */
    void insertConverter(TypeId newType,
                         FunctionPointer converter,
                         size_t resultIndex,
                         int64_t functionIndex,
                         bool tile);

    /**
     * Resolves Function pointers after loading expression from stream.
     * Called from serialize method
     */
    void resolveFunctions();

    /**
     * @brief Clear the content of the current expression.
     */
    void clear();

private:  // data
    TypeId _resultType;
    std::vector< ArrayDesc > _inputSchemas;
    ArrayDesc _outputSchema;

    /// @brief a vector of BindInfo objects, one per variable.
    std::vector<BindInfo> _bindings;

    /// @brief a vector, one element per variable, of a vector of resultIndex.
    ///
    /// There is a one-to-one mapping between a variable in _bindings and an element (inner vector)
    /// in _contextNo. A variable may appear multiple times in _arguments. E.g. in expression
    /// "a*a-3*a", the variable "a" will appear three times in _arguments. Let _bindings[i] be
    /// the BindInfo for "a". Then all the indices in _arguments that correspond to this variable
    /// are stored in a vector at _contextNo[i].
    std::vector<std::vector<size_t>> _contextNo;

    bool _compiled;
    bool _nullable;
    bool _constant;  ///< doesn't depend on input data
    bool _tileMode;
    size_t _tempValuesNumber;

    std::vector<CompiledFunction> _functions;

    /// @brief a vector of argument properties for correct compilation and optimizations.
    std::vector<Argument> _arguments;

    // true if this expression results in the same output for the given inputs,
    // false otherwise (e.g., random())
    bool _isDeterministic;
    bool _isInstIdentical;
};  // class Expression


/**
 * This function compile and evaluate a logical expression without context and
 * cast the result to expectedType.
 * This function is useful in LogicalOperators where expression is not compiled yet.
 *
 * @param expr a logical expression for evaluating
 * @param expectedType a type of result value if it's omitted the type will be inferred from expression
 */
Value evaluate(std::shared_ptr<LogicalExpression> expr,
                TypeId expectedType =  TID_VOID);


/**
 * This function compile and infer type of a logical expression and
 * cast the result to expectedType.
 * This function is useful in LogicalOperators where expression is not compiled yet.
 *
 * @param expr a logical expression for evaluating
 * @param expectedType a type of result value if it's omitted the type will be inferred from expression
 * @param query performing the operation
 */
TypeId expressionType(std::shared_ptr<LogicalExpression> expr,
                       const std::vector< ArrayDesc >& inputSchemas = std::vector< ArrayDesc >());

} // namespace

#endif
