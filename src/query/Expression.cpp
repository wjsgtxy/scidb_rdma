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
 * @file Expression.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <query/Expression.h>

#include <cmath>
#include <cstdarg>
#include <memory>

#include <array/ArrayName.h>
#include <query/LogicalExpression.h>
#include <query/Parser.h>
#include <query/ParsingContext.h>
#include <query/TypeSystem.h>
#include <query/UserQueryException.h>
#include <system/SystemCatalog.h>
#include <util/Indent.h>
#include <util/StringUtil.h>

using namespace std;

namespace scidb
{

namespace {
// The flags defined here may be used by code in this file, as arguments when calling certain functions.
// E.g. it is more readable to call
//   expr.compile(..., TILE_MODE, ..., !SKIP_VALUE, ...)
// instead of
//   expr.compile(..., true, ..., false, ...)
static constexpr bool TILE_MODE = true;
static constexpr bool RAISE_EXCEPTION = true;
static constexpr bool SKIP_VALUE = true;
}  // namespace

class FilterHelper
{
public:
  /**
   * @see Expression::extractSpatialConstraints.
   */
  static void extractSpatialConstraints(
      Expression const*const expr,
      std::shared_ptr<SpatialRanges>& spatialRangesPtr,
      bool& hasOtherClauses)
  {
      _extractSpatialConstraints(expr, spatialRangesPtr, hasOtherClauses);
      spatialRangesPtr->buildIndex();
  }

private:
    /**
     * The type and index of an argument in Expression::_arguments.
     */
    struct ArgInfo
    {
        enum Type
        {
            Unknown,
            Function,
            Constant,
            Variable
        };

        Type type;
        size_t index;  ///< the index in _functions (for Function) or _bindings (for Variable).
    };

    /**
     * A wrapper for some expression data that are passed between functions.
     */
    struct ExpressionInfo
    {
        const std::vector<Expression::CompiledFunction>& _functions;
        const std::vector<Expression::Argument>& _arguments;
        const std::vector<BindInfo>& _bindings;
        const std::vector<ArgInfo>& _argInfos;

        ExpressionInfo(
            const std::vector<Expression::CompiledFunction>& functions,
            const std::vector<Expression::Argument>& arguments,
            const std::vector<BindInfo>& bindings,
            const std::vector<ArgInfo>& argInfos)
          : _functions(functions),
            _arguments(arguments),
            _bindings(bindings),
            _argInfos(argInfos)
        {}
    };

    /**
     * @brief Impl for the public version.
     * @note The reason for this extra level of calling is to enable the public version
     *       to call SpatialRanges::buildIndex().
     */
    static void _extractSpatialConstraints(
        Expression const*const expr,
        std::shared_ptr<SpatialRanges>& spatialRangesPtr,
        bool& hasOtherClauses);

    static bool isAnd(const std::string& name) noexcept
    {
        return compareStringsIgnoreCase(name, "and") == 0;
    }

    static bool isOr(const std::string& name) noexcept
    {
        return compareStringsIgnoreCase(name, "or") == 0;
    }

    /// @brief Read a value from a scidb::Value that stores a constant (could be tiled).
    template <typename ValueType>
    static ValueType getFromConstValue(const Value& value)
    {
        if (value.isTile()) {
            Value v;
            value.getTile()->getValueByIndex(v, 0);
            return v.get<ValueType>();
        }
        return value.get<ValueType>();
    }

    /**
     * @brief Recursively find all operands rooted by a function.
     *
     * Here we are talking about a binary tree of Expression components.
     * The root of the tree is described by rootIndex, which is an index in ei._arguments
     * that marks the result index of a function.
     *
     * @example
     * - Let the root denote an 'and' function where both children are also 'and' functions.
     * - Further assume none of the four grandchildren is an 'and' function.
     * - The findOperands() function will gather the indices of the four grandchildren.
     *
     * @note
     * - The recursion continues on a descendant node which is a function and whose function name is equal
     *   to the root function.
     * - At the root level, only 'and' and 'or' functions are recognized.
     *
     * @param[in]  ei  the expression wrapper.
     * @param[in]  rootIndex  the result index in ei._arguments that corresponds to the root (of a subtree).
     * @param[out] operands  placeholder for the operands, in the form of result indices in _arguments.
     *
     * @pre The root node at argIndex is a binary function with name either "and" or "or".
     */
    static void findOperands(
        const ExpressionInfo& ei,
        const size_t rootIndex,
        vector<size_t>& operands);

    /**
     * @brief Return whether an argument at rootIndex is a DimCompare function.
     *
     * - Definition: A 'DimCompare' is a function of the form "dimName op constant" or "constant op dimName",
     *   where op is one of: < | > | <= | >= | =
     */
    static bool isDimComparison(
        const ExpressionInfo& ei,
        const size_t rootIndex);

    /**
     * @brief Given an 'or' function rooted by rootIndex, are all operands ranges?
     *
     * @param[in]  ei  the expression wrapper.
     * @param[in]  rootIndex  the result index in Expression::_arguments that corresponds to the root (of a subtree).
     *
     * - Definition: A 'range' is the conjunction of at least one DimCompare.
     *
     * @pre An 'or' function is at rootIndex.
     */
    static bool allOperandsOfOrAreRanges(
        const ExpressionInfo& ei,
        const size_t rootIndex);

    /**
     * @brief Given a vector of functions each of which is a dimComparison, build a spatial range.
     * @note The returned SpatialRange may or may not be valid.
     */
    static SpatialRange buildRangeFromDimComparisons(
        const ExpressionInfo& ei,
        const std::vector<size_t>& dimComparisons,
        const size_t numDims);

    /**
     * @brief Given a sub-tree of Expression which is an RangeDisjunction, fill a SpatialRanges object.
     *
     * - Definition: an RangeDisjunction is a n-ary "or" function, where every operand is either a DimCompare,
     *   or an n-ary "and" function of DimCompares.
     */
    static void addFromRangeDisjunction(
        const ExpressionInfo& ei,
        const size_t rootIndex,
        std::shared_ptr<SpatialRanges>& spatialRangesPtr);
};  // class FilterHelper

void FilterHelper::findOperands(
    const ExpressionInfo& ei,
    const size_t rootIndex,
    vector<size_t>& operands)
{
    SCIDB_ASSERT(ei._argInfos[rootIndex].type == ArgInfo::Function);
    auto const& rootFunction = ei._functions[ei._argInfos[rootIndex].index];
    SCIDB_ASSERT(rootFunction.functionTypes.size() == 2);
    SCIDB_ASSERT(isAnd(rootFunction.functionName) || isOr(rootFunction.functionName));

    // Left child.
    const size_t left = rootFunction.argIndex;
    const auto& argInfoLeft = ei._argInfos[left];
    if (argInfoLeft.type == ArgInfo::Function &&
            ei._functions[argInfoLeft.index].functionName == rootFunction.functionName) {
        findOperands(ei, left, operands);
    } else {
        operands.push_back(left);
    }

    // Right child.
    const size_t right = left + 1;
    const auto& argInfoRight = ei._argInfos[right];
    if (argInfoRight.type == ArgInfo::Function &&
            ei._functions[argInfoRight.index].functionName == rootFunction.functionName) {
        findOperands(ei, right, operands);
    } else {
        operands.push_back(right);
    }
}

bool FilterHelper::isDimComparison(
    const ExpressionInfo& ei,
    const size_t rootIndex)
{
    // Has to be a function.
    const auto& argInfoRoot = ei._argInfos[rootIndex];
    if (argInfoRoot.type != ArgInfo::Function) {
        return false;
    }

    // The function name has to be a comparison.
    auto const& name = ei._functions[argInfoRoot.index].functionName;
    static std::vector<std::string> dimComparisons({"<", ">", "<=", ">=", "="});
    if (std::find(dimComparisons.cbegin(), dimComparisons.cend(), name) == dimComparisons.cend()) {
        return false;
    }

    // One arg must be a dimension name, and the other arg must be a constant.
    const auto argIndex = ei._functions[argInfoRoot.index].argIndex;
    const auto& argInfoLeft = ei._argInfos[argIndex];
    const auto& argInfoRight = ei._argInfos[argIndex+1];

    bool dimComparisonConst = false;
    if (argInfoLeft.type == ArgInfo::Function &&
        !ei._argInfos.empty() &&
        !ei._bindings.empty() &&
        !ei._functions.empty()) {
        // Inspect the function on the left-hand side...
        auto& leftHandFunction = ei._functions[argInfoLeft.index];

        // ...is it a converter function?
        auto leftIsConverter = (leftHandFunction.functionName.empty() &&
                                leftHandFunction.functionTypes.size() == 2 &&
                                (leftHandFunction.functionTypes[0] !=
                                 leftHandFunction.functionTypes[1]));

        // Supporting only double->int conversion (for now)
        // TODO SDB-6200 (see ticket and comment with next 'TODO SDB-6200' below)
        auto isSupportedConversion = (leftHandFunction.functionTypes.size() == 2 &&
                                      leftHandFunction.functionTypes[0] == "int64" &&
                                      leftHandFunction.functionTypes[1] == "double");

        auto converterOperatesOnVariable =
            ei._argInfos[leftHandFunction.argIndex].type == ArgInfo::Variable;

        auto hasCoordinateBinding =
            ei._bindings[ei._argInfos[leftHandFunction.argIndex].index].kind == BindInfo::BI_COORDINATE;

        dimComparisonConst = (leftIsConverter &&
                              isSupportedConversion &&
                              converterOperatesOnVariable &&
                              hasCoordinateBinding &&
                              argInfoRight.type == ArgInfo::Constant);
    }
    else {
        dimComparisonConst = (argInfoLeft.type == ArgInfo::Variable &&
                              argInfoRight.type == ArgInfo::Constant &&
                              ei._bindings[argInfoLeft.index].kind == BindInfo::BI_COORDINATE);
    }

    const bool constCompareDim = (
        argInfoLeft.type == ArgInfo::Constant &&
        argInfoRight.type == ArgInfo::Variable &&
        ei._bindings[argInfoRight.index].kind == BindInfo::BI_COORDINATE);
    return dimComparisonConst || constCompareDim;
}

bool FilterHelper::allOperandsOfOrAreRanges(
    const ExpressionInfo& ei,
    const size_t rootIndex)
{
    const auto& argInfoRoot = ei._argInfos[rootIndex];
    SCIDB_ASSERT(argInfoRoot.type == ArgInfo::Function);
    SCIDB_ASSERT(isOr(ei._functions[argInfoRoot.index].functionName));

    std::vector<size_t> operands;
    findOperands(ei, rootIndex, operands);
    for (auto const& index : operands) {
        const auto& argInfo = ei._argInfos[index];
        if (argInfo.type == ArgInfo::Function &&
                isAnd(ei._functions[argInfo.index].functionName)) {
            // Is this an "and"-separated DimCompares?
            std::vector<size_t> operandsOfAnd;
            findOperands(ei, index, operandsOfAnd);
            for (auto const& a : operandsOfAnd) {
                if (!isDimComparison(ei, a)) {
                    return false;
                }
            }
        } else {
            // Is this a single DimCompare?
            if (!isDimComparison(ei, index)) {
                return false;
            }
        }
    }

    return true;
}

SpatialRange FilterHelper::buildRangeFromDimComparisons(
    const ExpressionInfo& ei,
    const std::vector<size_t>& dimComparisons,
    const size_t numDims)
{
    // Initialize a range as large as the whole space.
    SpatialRange range(numDims);
    for (size_t i=0; i < numDims; ++i) {
        range._low[i] = CoordinateBounds::getMin();
        range._high[i] = CoordinateBounds::getMax();
    }

    // For each DimCompare, shrink the range.
    for (auto const& index : dimComparisons) {
        SCIDB_ASSERT(isDimComparison(ei, index));
        const auto& function = ei._functions[ei._argInfos[index].index];
        const auto& argIndex = function.argIndex;
        const auto& name = function.functionName;
        const auto& argInfoLeft = ei._argInfos[argIndex];
        const auto& argInfoRight = ei._argInfos[argIndex+1];

        if (argInfoLeft.type == ArgInfo::Constant) {
            size_t dimNo = ei._bindings[argInfoRight.index].resolvedId;
            if (ei._arguments[argIndex].isNull) {
                // DimCompare with null?!  Close the range, you get nothing.
                range._low[dimNo] = CoordinateBounds::getMax();
                range._high[dimNo] = CoordinateBounds::getMin();
            } else {
                Coordinate c = getFromConstValue<int64_t>(ei._arguments[argIndex].value);
                if (name == "<") {
                    // c+1 <= dim
                    range._low[dimNo] = max(range._low[dimNo], c+1);
                } else if (name == ">") {
                    // c-1 >= dim
                    range._high[dimNo] = min(range._high[dimNo], c-1);
                } else if (name == "<=") {
                    // c <= dim
                    range._low[dimNo] = max(range._low[dimNo], c);
                } else if (name == ">=") {
                    // c >= dim
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else if (name == "=") {
                    // c == dim
                    range._low[dimNo] = max(range._low[dimNo], c);
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else {
                    SCIDB_ASSERT(false);
                }
            }
        } else if (argInfoLeft.type == ArgInfo::Function) {
            auto& leftHandFunction = ei._functions[argInfoLeft.index];
            SCIDB_ASSERT(leftHandFunction.functionName.empty());
            SCIDB_ASSERT(leftHandFunction.functionTypes.size());
            SCIDB_ASSERT(leftHandFunction.functionTypes[0] !=
                         leftHandFunction.functionTypes[1]);
            SCIDB_ASSERT(leftHandFunction.functionTypes[0] == "int64");
            SCIDB_ASSERT(leftHandFunction.functionTypes[1] == "double");

            const auto& converterFunctionEntry = ei._functions[argInfoLeft.index];

            // converterInputArgIndex references the entry that is the input to
            // the converter, rather than the converter itself.
            auto converterInputArgIndex = converterFunctionEntry.argIndex;

            // Fetch the dimension (resolvedId) from the binding of the variable
            // that is the input to the typecast converter instead of using the
            // (nonexistent) binding of the converter itself.
            const auto& converterInputArgument = ei._argInfos[converterInputArgIndex];
            const auto& inputArgumentBinding = ei._bindings[converterInputArgument.index];
            size_t dimNo = inputArgumentBinding.resolvedId;

            if (ei._arguments[converterInputArgIndex-1].isNull) {
                // DimCompare with null?!  Close the range, you get nothing.
                range._low[dimNo] = CoordinateBounds::getMax();
                range._high[dimNo] = CoordinateBounds::getMin();
            } else {
#if 0
                // TODO SDB-6200
                // The optimizer wants to convert floating-point params like
                // 11.0 from int64 to double... but it's already a double type
                // and since dimension comparisons are int64s, the double
                // should be coerced into an int64 (or the parser should reject
                // the query with such a comparison as invalid, which IMHO is a
                // better solution than trying to jump through all of these
                // hoops to support this case).

                // fetch converter function
                auto converter = ei._functions[argInfoLeft.index].function;
                auto* psrc = &(ei._arguments[converterInputArgIndex].value);
                Value dst(TypeLibrary::getType(TID_INT64), Value::asTile);
                converter(&psrc, &dst, nullptr);  // call converter to translate arg to int64
                Coordinate c = getFromConstValue<int64_t>(dst);
#else
                // We know that the value has a double on it because we checked earlier,
                // go ahead and grab it directly.
                auto decodedValue = getFromConstValue<double>(ei._arguments[converterInputArgIndex-1].value);
                auto c = static_cast<Coordinate>(decodedValue);
#endif
                if (name == ">") {
                    // dim >= c+1
                    range._low[dimNo] = max(range._low[dimNo], c+1);
                } else if (name == "<") {
                    // dim <= c-1
                    range._high[dimNo] = min(range._high[dimNo], c-1);
                } else if (name == ">=") {
                    // dim >= c
                    range._low[dimNo] = max(range._low[dimNo], c);
                } else if (name == "<=") {
                    // dim <= c
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else if (name == "=") {
                    // dim == c
                    range._low[dimNo] = max(range._low[dimNo], c);
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else {
                    SCIDB_ASSERT(false);
                }
            }
        } else {
            SCIDB_ASSERT(argInfoRight.type == ArgInfo::Constant);
            size_t dimNo = ei._bindings[argInfoLeft.index].resolvedId;
            if (ei._arguments[argIndex+1].isNull) {
                // DimCompare with null?!  Close the range, you get nothing.
                range._low[dimNo] = CoordinateBounds::getMax();
                range._high[dimNo] = CoordinateBounds::getMin();
            } else {
                Coordinate c = getFromConstValue<int64_t>(ei._arguments[argIndex+1].value);
                if (name == ">") {
                    // dim >= c+1
                    range._low[dimNo] = max(range._low[dimNo], c+1);
                } else if (name == "<") {
                    // dim <= c-1
                    range._high[dimNo] = min(range._high[dimNo], c-1);
                } else if (name == ">=") {
                    // dim >= c
                    range._low[dimNo] = max(range._low[dimNo], c);
                } else if (name == "<=") {
                    // dim <= c
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else if (name == "=") {
                    // dim == c
                    range._low[dimNo] = max(range._low[dimNo], c);
                    range._high[dimNo] = min(range._high[dimNo], c);
                } else {
                    SCIDB_ASSERT(false);
                }
            }
        }
    }

    return range;
}

void FilterHelper::addFromRangeDisjunction(
    const ExpressionInfo& ei,
    const size_t rootIndex,
    std::shared_ptr<SpatialRanges>& spatialRangesPtr)
{
    const auto& argInfoRoot = ei._argInfos[rootIndex];
    SCIDB_ASSERT(argInfoRoot.type == ArgInfo::Function);
    SCIDB_ASSERT(isOr(ei._functions[argInfoRoot.index].functionName));

    std::vector<size_t> operands;
    findOperands(ei, rootIndex, operands);
    auto numDims = spatialRangesPtr->numDims();
    for (auto const& index : operands) {
        const auto& argInfo = ei._argInfos[index];
        if (argInfo.type == ArgInfo::Function &&
                isAnd(ei._functions[argInfo.index].functionName)) {
            // Every operand of the "and" function must be a DimCompare.
            // This is due to the definition of an RangeConjunction (see addFromRangeConjunction()):
            // an n-ary "or" function, where every operand is either a DimComparison, or an n-ary "and"
            // function of DimComparisons.
            std::vector<size_t> operandsOfAnd;
            findOperands(ei, index, operandsOfAnd);
            auto range = buildRangeFromDimComparisons(ei, operandsOfAnd, numDims);
            if (range.valid()) {
                spatialRangesPtr->insert(std::move(range));
            }
        } else {
            // Must be a single dimComparison.
            std::vector<size_t> dimComparisons(1);
            dimComparisons[0] = index;
            auto range = buildRangeFromDimComparisons(ei, dimComparisons, numDims);
            if (range.valid()) {
                spatialRangesPtr->insert(std::move(range));
            }
        }
    }
}

void FilterHelper::_extractSpatialConstraints(
    Expression const*const expr,
    std::shared_ptr<SpatialRanges>& spatialRangesPtr,
    bool& hasOtherClauses)
{
    SCIDB_ASSERT(expr);

    // The special case when the expression is not a function.
    if (expr->_functions.empty()) {
        hasOtherClauses = true;
        return;
    }

    // Understand the type of every element in expr->_arguments.
    const size_t numArgs = expr->_arguments.size();
    std::vector<ArgInfo> argInfos(numArgs);
    for (size_t i=0; i < expr->_functions.size(); ++i) {
        const size_t index = expr->_functions[i].resultIndex;
        SCIDB_ASSERT(argInfos[index].type == ArgInfo::Unknown);
        argInfos[index].index = i;
        argInfos[index].type = ArgInfo::Function;
    }
    for (size_t i=0; i < expr->_contextNo.size(); ++i) {
        for (auto const& index: expr->_contextNo[i]) {
          SCIDB_ASSERT(argInfos[index].type == ArgInfo::Unknown);
          argInfos[index].index = i;
          argInfos[index].type = ArgInfo::Variable;
        }
    }
    for (size_t index=0; index < expr->_arguments.size(); ++index) {
        auto& arg = expr->_arguments[index];
        if (arg.isConst) {
            SCIDB_ASSERT(argInfos[index].type == ArgInfo::Unknown);
            argInfos[index].type = ArgInfo::Constant;
        } else {
            // ...ignore the arg, we already processed it above.
            SCIDB_ASSERT(argInfos[index].type != ArgInfo::Unknown);
        }
    }

    // Construct the ExpressionInfo object.
    ExpressionInfo ei(expr->_functions, expr->_arguments, expr->_bindings, argInfos);

    // Differentiate the function name of the root node.
    SCIDB_ASSERT(!expr->_functions.empty() && expr->_functions[0].resultIndex == 0);
    auto const& rootName = ei._functions[0].functionName;
    const size_t numDims = spatialRangesPtr->numDims();
    if (isAnd(rootName)) {
        // Among the operands of 'and', we look for one of two cases:
        // Case 1: there is at least one DimCompare.
        //   E.g. op[0] = (dim1>5); op[1] = (dim1<100)
        // Case 2: there is one 'or' operand, whose children are all ranges.
        //   E.g. op[0] = (dim1>5 and dim1<100) or (dim2<2)
        std::vector<size_t> operands;
        findOperands(ei, 0, operands);

        // Get the type of each operand.
        enum class OperandType {
            Other,
            DimCompare,
            RangeDisjunction
        };
        std::vector<OperandType> operandTypes(operands.size());

        bool hasDimCompare = false;
        bool hasRangeDisjunction = false;
        bool hasOther = false;
        for (size_t i=0; i < operands.size(); ++i) {
            auto const& argInfo = ei._argInfos[operands[i]];
            if (argInfo.type == ArgInfo::Function &&
                isOr(ei._functions[argInfo.index].functionName)) {
              if (allOperandsOfOrAreRanges(ei, operands[i])) {
                  operandTypes[i] = OperandType::RangeDisjunction;
                  hasRangeDisjunction = true;
              } else {
                  operandTypes[i] = OperandType::Other;
                  hasOther = true;
              }
            } else if (isDimComparison(ei, operands[i])) {
                operandTypes[i] = OperandType::DimCompare;
                hasDimCompare = true;
            } else {
                operandTypes[i] = OperandType::Other;
                hasOther = true;
            }
        }

        // Case 1: at least one dimComparison.
        if (hasDimCompare) {
            std::vector<size_t> dimComparisons;
            for (size_t i=0; i < operands.size(); ++i) {
                if (operandTypes[i] == OperandType::DimCompare) {
                    dimComparisons.push_back(operands[i]);
                }
            }
            auto range = buildRangeFromDimComparisons(ei, dimComparisons, numDims);
            if (range.valid()) {
                spatialRangesPtr->insert(std::move(range));
            }
            hasOtherClauses = hasOther || hasRangeDisjunction;
            return;
        }

        // Case 2: at least one rangeDisjunction (but be sure to process them all).
        if (hasRangeDisjunction) {
            for (size_t i=0; i < operands.size(); ++i) {
                if (operandTypes[i] == OperandType::RangeDisjunction) {
                    addFromRangeDisjunction(ei, operands[i], spatialRangesPtr);
                    SCIDB_ASSERT(hasDimCompare == false);
                    hasOtherClauses = hasOther;
                }
            }
        }
    } else if (isOr(rootName)) {
        // We can take a shortcut under one condition: if all operands are ranges.
        if (allOperandsOfOrAreRanges(ei, 0)) {
            addFromRangeDisjunction(ei, 0, spatialRangesPtr);
            hasOtherClauses = false;
            return;
        }
    } else if (isDimComparison(ei, 0)) {
        std::vector<size_t> dimComparisons(1);
        dimComparisons[0] = 0;
        auto range = buildRangeFromDimComparisons(ei, dimComparisons, numDims);
        if (range.valid()) {
            spatialRangesPtr->insert(std::move(range));
        }
        hasOtherClauses = false;
        return;
    }

    // By default: no ranges; has other clauses.
    hasOtherClauses = true;
}

//-----------------------------------------------------------------------------------

// ExpressionContext implementation
ExpressionContext::ExpressionContext(Expression& expression):
    _expression(expression), _contextChanged(true)
{
    SCIDB_ASSERT(_expression._compiled);

    const size_t nArgs = _expression._arguments.size();
    _args.resize(nArgs);
    _vargs.resize(_expression._tempValuesNumber);
    memset(&_args[0], 0, nArgs * sizeof(Value*));
    _context.resize(_expression._contextNo.size());

    for (size_t i = 0; i< _expression._contextNo.size(); i++) {
        const vector<size_t>& indices = _expression._contextNo[i];
        _context[i] = expression._arguments[indices[0]].value;
        for (size_t j = 0; j < indices.size(); j++) {
            _args[indices[j]] = &_context[i];
        }
    }

    size_t tempIndex = 0;
    for (size_t i = 0; i < nArgs; i++) {
        if (!_args[i]) {
            if (_expression._arguments[i].isConst) {
                _args[i] = &_expression._arguments[i].value;
            } else {
                _vargs[tempIndex] = _expression._arguments[i].value;
                _args[i] = &_vargs[tempIndex++];
            }
        }
    }

    _state.resize(expression._functions.size());
    for (size_t i = 0; i < _state.size(); i++) {
        const size_t size = expression._functions[i].stateSize;
        if (size > 0)
        {
            _state[i] = boost::shared_array<char>(new char[size]);
            memset(_state[i].get(), 0, size);
        }
    }
}

const Value& ExpressionContext::operator[](size_t i) const
{
    return _context[i];
}

Value& ExpressionContext::operator[](size_t i)
{
    _contextChanged = true;
    return _context[i];
}

void Expression::compile(std::shared_ptr<LogicalExpression> expr,
                         bool tile,
                         TypeId expectedType,
                         const vector< ArrayDesc>& inputSchemas,
                         const ArrayDesc& outputSchema)
{
    clear();
    compileCommon(expr, tile, expectedType, inputSchemas, outputSchema, nullptr);
}

void Expression::compileCommon(std::shared_ptr<LogicalExpression> expr,
                         bool tile,
                         TypeId expectedType,
                         const vector< ArrayDesc>& inputSchemas,
                         const ArrayDesc& outputSchema,
                         NameToIndex const*const nameToIndexInBindings)
{
    try
    {
        _tileMode = tile;
        _inputSchemas = inputSchemas;
        _outputSchema = outputSchema;
        _nullable = false;
        _constant = true;
        internalCompile(expr, tile, 0, 0, !SKIP_VALUE, nameToIndexInBindings);
        _resultType = _arguments[0].type;
        if (_resultType != expectedType && expectedType != TID_VOID) {
            ConversionCost cost = EXPLICIT_CONVERSION_COST;
            FunctionPointer converter =
                FunctionLibrary::getInstance()->findConverter(_resultType,
                                                              expectedType,
                                                              tile,
                                                              RAISE_EXCEPTION,
                                                              &cost);
            insertConverter(expectedType, converter, 0, -1, tile);
            _resultType = expectedType;
        }
        for (size_t i = 0; i < _arguments.size(); i++)
        {
            // Create new value with needed type if it's not a constant
            if (!_arguments[i].isConst) {
                Type const& t = TypeLibrary::getType(_arguments[i].type);
                _arguments[i].value = tile ? Value(t,Value::asTile) : Value(t);
                _tempValuesNumber++;
            }
        }
        _tempValuesNumber -= _contextNo.size();
        _compiled = true;
    }
    catch (const scidb::Exception& e)
    {
        if (_tileMode &&
                (e.getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND ||
                 e.getShortErrorCode() == SCIDB_SE_TYPE_CONVERSION)) {
            compileCommon(expr, !TILE_MODE, expectedType, inputSchemas, outputSchema, nameToIndexInBindings);
        } else {
            e.raise();
        }
    }
}

void Expression::compileExpressionString(
    const string& expression,
    bool tile,
    const vector<string>& names,
    const vector<TypeId>& types,
    TypeId expectedType)
{
    SCIDB_ASSERT(names.size() == types.size());
    SCIDB_ASSERT(_bindings.empty());

    // For each <name, type> pair, add an entry to _bindings and record the index in a map.
    NameToIndex nameToIndexInBindings;
    const size_t numVariables = names.size();
    _bindings.resize(numVariables);
    _contextNo.resize(numVariables);

    BindInfo bindInfo;
    bindInfo.kind = BindInfo::BI_VALUE;
    bindInfo.resolvedId = 0;
    for (size_t i = 0; i < numVariables; i++) {
        bindInfo.type = types[i];
        bindInfo.inputNo = i;
        _bindings[i] = bindInfo;
        nameToIndexInBindings.emplace(names[i], i);
    }

    // Call the version of compile that takes a logicalExpression.
    std::shared_ptr<LogicalExpression> logicalExpression(parseExpression(expression));
    compileCommon(logicalExpression, tile, expectedType,
            std::vector<ArrayDesc>(), ArrayDesc(), &nameToIndexInBindings);
}

void Expression::compileConstant(bool tile, const TypeId& type, const Value& value)
{
    _tileMode = tile;
    _arguments[0].value = tile ? makeTileConstant(type,value) : value;
    _arguments[0].type = type;
    _arguments[0].isConst = true;
    _arguments[0].isNull = value.isNull();
    _resultType = type;
    _compiled = true;
    _constant = true;
    _nullable = value.isNull();
}

BindInfo
Expression::resolveContext(const ArrayDesc& arrayDesc, const string& arrayNameOrg,
                           const string& referenceName)
{
    BindInfo bind;
    const Attributes& attrs = arrayDesc.getAttributes();
    for (const auto& attr : attrs)
    {
        if (attr.getName() == referenceName && attr.hasAlias(arrayNameOrg))
        {
            bind.kind = BindInfo::BI_ATTRIBUTE;
            bind.resolvedId = attr.getId(); // DJG
            bind.type = attr.getType();
            _nullable |= attr.isNullable();
            _constant = false;
            return bind;
        }
    }

    const Dimensions& dims = arrayDesc.getDimensions();
    for (size_t i = 0; i < dims.size(); i++) {
        if (dims[i].hasNameAndAlias(referenceName, arrayNameOrg))
        {
            bind.kind = BindInfo::BI_COORDINATE;
            bind.resolvedId = i;
            bind.type = TID_INT64;
            return bind;
        }
    }

    bind.type = TID_VOID;

    if(	(arrayNameOrg != "") && !isQualifiedArrayName(arrayNameOrg))
    {
        // Try to resolve using a qualified array name.
        std::string arrayName = makeQualifiedArrayName(
            arrayDesc.getNamespaceName(), arrayNameOrg);
        bind = resolveContext(arrayDesc, arrayName, referenceName);
    }

    return bind;
}


BindInfo Expression::resolveContext(const std::shared_ptr<AttributeReference>& ref,
                                    NameToIndex const*const nameToIndexInBindings)
{
    BindInfo bind;

    // If array name presents point is used and it's not necessary to check variable usage
    if (ref->getArrayName() == "" && nameToIndexInBindings != nullptr)
    {
        auto it = nameToIndexInBindings->find(ref->getAttributeName());
        if (it != nameToIndexInBindings->cend()) {
            return _bindings[it->second];
        }
    }

    // Look for ref->getArrayName() in each of the input schemas
    std::string arrayName = ref->getArrayName();
    for (size_t a = 0; a < _inputSchemas.size(); a++)
    {
        bind = resolveContext(_inputSchemas[a], arrayName, ref->getAttributeName());
        if (bind.type != TID_VOID) {
            bind.inputNo = a;
            return bind;
        }
    }

    // Look for ref->getArrayName() in the output schema
    bind = resolveContext(_outputSchema, arrayName, ref->getAttributeName());
    if (bind.type != TID_VOID) {
        bind.inputNo = ~0;
        return bind;
    }

    // ref->getArrayName() not found, throw an exception
    throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REF_NOT_FOUND, ref->getParsingContext())
        << (ref->getArrayName() + "." + ref->getAttributeName());
}

void Expression::swapArguments(size_t firstIndex)
{
    // Search index of function that output result into current result index to change its output to input of converter
    size_t producer1 = 0;
    for (; producer1 < _functions.size(); producer1++)
    {
        if (_functions[producer1].resultIndex == firstIndex) {
            break;
        }
    }
    size_t producer2 = 0;
    for (; producer2 < _functions.size(); producer2++)
    {
        if (_functions[producer2].resultIndex == firstIndex + 1) {
            break;
        }
    }

    if (producer1 < _functions.size()) {
        _functions[producer1].resultIndex = firstIndex + 1;
    }
    if (producer2 < _functions.size()) {
        _functions[producer2].resultIndex = firstIndex;
    }

    // Search index of context that assign binding value to current result index to change its index to input of converter
    size_t contextIdx1 = 0;
    bool found1 = false;
    if (producer1 == _functions.size()) {
        for ( ; contextIdx1 < _contextNo.size(); contextIdx1++)
        {
            vector<size_t>& indicies = _contextNo[contextIdx1];
            for (producer1 = 0; producer1 < indicies.size(); producer1++) {
                if (indicies[producer1] == firstIndex) {
                    found1 = true;
                    break;
                }
            }
            if (found1)
                break;
        }
    }
    size_t contextIdx2 = 0;
    bool found2 = false;
    if (producer2 == _functions.size()) {
        for ( ; contextIdx2 < _contextNo.size(); contextIdx2++)
        {
            vector<size_t>& indicies = _contextNo[contextIdx2];
            for (producer2 = 0; producer2 < indicies.size(); producer2++) {
                if (indicies[producer2] == firstIndex + 1) {
                    found2 = true;
                    break;
                }
            }
            if (found2)
                break;
        }
    }

    if (found1) {
        _contextNo[contextIdx1][producer1] = firstIndex + 1;
    }
    if (found2) {
        _contextNo[contextIdx2][producer2] = firstIndex;
    }

    std::swap(_arguments[firstIndex], _arguments[firstIndex + 1]);
}

void Expression::internalCompile(std::shared_ptr<LogicalExpression> expr,
                                 bool tile,
                                 size_t resultIndex, size_t skipIndex, bool skipValue,
                                 NameToIndex const*const nameToIndexInBindings)
{
    if (typeid(*expr) == typeid(AttributeReference))
    {
        std::shared_ptr<AttributeReference> e = dynamic_pointer_cast<AttributeReference>(expr);
        const BindInfo bind = resolveContext(e, nameToIndexInBindings);
        size_t i;
        for (i = 0; i < _bindings.size(); i++) {
            if (_bindings[i] == bind) {
                _contextNo[i].push_back(resultIndex);
                break;
            }
        }
        if (i == _bindings.size()) {
            _bindings.push_back(bind);
            _contextNo.push_back(vector<size_t>(1, resultIndex));
        }
        _arguments[resultIndex].type = bind.type;
    }
    else if (typeid(*expr) == typeid(Constant))
    {
        std::shared_ptr<Constant> e = dynamic_pointer_cast<Constant>(expr);
        _arguments[resultIndex].type = e->getType();
        _arguments[resultIndex].value = tile ? makeTileConstant(e->getType(), e->getValue()) : e->getValue();
        _arguments[resultIndex].isConst = true;
        _arguments[resultIndex].isNull = e->getValue().isNull();
        SCIDB_ASSERT(_arguments[resultIndex].type != TID_VOID || e->getValue().isNull());
        _nullable |= e->getValue().isNull();
    }
    else if (typeid(*expr) == typeid(Function))
    {
        std::shared_ptr<Function> e = dynamic_pointer_cast<Function>(expr);

        // Push a CompiledFunction object to _functions.
        CompiledFunction f;
        f.functionName = e->getFunction();
        f.resultIndex = resultIndex;
        f.argIndex = _arguments.size();
        f.skipIndex = skipIndex;
        f.skipValue = skipValue;
        size_t functionIndex = _functions.size();
        _functions.push_back(f);

        // Reserving space in _arguments for the function's arguments.
        _arguments.resize(_arguments.size() + e->getArgs().size());

        // Recursively call internalCompile() for every argument, in reserse order.
        vector<TypeId> funcArgTypes(e->getArgs().size());
        bool argumentsAreConst = true;
        for (int i = safe_static_cast<int>(e->getArgs().size()) - 1; i >=0 ; i--)
        {
            size_t newSkipIndex = skipIndex;
            bool newSkipValue = skipValue;
            if (!_tileMode) {
                if (i != 0 && strcasecmp(f.functionName.c_str(), "iif") == 0) {
                    if (i == 2) {
                        newSkipIndex = f.argIndex;
                        newSkipValue = true;
                    } else if (i == 1) {
                        newSkipIndex = f.argIndex;
                        newSkipValue = false;
                    }
                } else if (i == 1 && strcasecmp(f.functionName.c_str(), "or") == 0) {
                    newSkipIndex = f.argIndex;
                    newSkipValue = true;
                } else if (i == 1 && strcasecmp(f.functionName.c_str(), "and") == 0) {
                    newSkipIndex = f.argIndex;
                    newSkipValue = false;
                }
            }
            internalCompile(e->getArgs()[i], tile,
                            f.argIndex + i, newSkipIndex, newSkipValue,
                            nameToIndexInBindings);
            funcArgTypes[i] = _arguments[f.argIndex + i].type;
            argumentsAreConst = argumentsAreConst &&
                    (_arguments[f.argIndex + i].isConst || _arguments[f.argIndex + i].isConstantFunction);
        }

        // Searching function pointer for given function name and type of arguments
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        bool swapInputs = false;
        if (FunctionLibrary::getInstance()->findFunction(f.functionName, funcArgTypes,
                                                         functionDesc, converters, tile, swapInputs))
        {
            _functions[functionIndex].function = functionDesc.getFuncPtr();
            _functions[functionIndex].functionTypes = functionDesc.getInputArgs();
            if (functionDesc.getScratchSize() > 0)
                _functions[functionIndex].stateSize = functionDesc.getScratchSize();
            for (size_t i = 0; i < converters.size(); i++) {
                if (converters[i]) {
                    insertConverter(_functions[functionIndex].functionTypes[i],
                                    converters[i], f.argIndex + i, functionIndex, tile);
                }
            }
            _arguments[resultIndex].type = functionDesc.getOutputArg();
            if (swapInputs) {
                // We need to swap functions producing arguments for this call
                swapArguments(f.argIndex);
            }
            _arguments[resultIndex].isConstantFunction = functionDesc.isDeterministic() && argumentsAreConst;

            // We can't rely on checking Expression::isConstant for
            // determinism, even though it inspects _arguments[i].isConst
            // and _arguments[i].isConstantFunction.  The _arguments
            // container default-constructs its Argument instances with
            // isConst and isConstantFunction set to false by default.
            // In the case that there are no arguments for a deterministic
            // function, these values will be incorrect at their defaults
            // and will not be updated by internalCompile (for the very
            // reason that there are no arguments to inspect).  Rather
            // than untangle that ball right now, carry around the
            // determinism from the function descriptor and taint it in
            // the case that any nested function in the expression is
            // not deterministic.  Then operators like filter can ask the
            // expression if it's deterministic or not without having to
            // conflate that idea with constant functions or the presence
            // of arguments, both of which aren't handled correctly with
            // respect to the idea of determinism.
            _isDeterministic = _isDeterministic && functionDesc.isDeterministic();

            _isInstIdentical = _isInstIdentical && functionDesc.isInstIdentical();

            /**
             * TODO: here it would be useful to set isConst and notNull
                         * flags for result arg prop. Also here we can evaluate function
                         * if every its argument is constant and place in result index
                         * constant value with the result without adding CompiledFunction
                     * into _functionMap at all.
             */
        }
        else
        {
            stringstream ss;
            ss << f.functionName << '(';
            for (size_t i = 0; i < funcArgTypes.size(); i++) {
                ss << TypeLibrary::getType(funcArgTypes[i]).name();
                if (i < funcArgTypes.size() - 1)
                    ss << ", ";
            }
            ss << ')';
            throw USER_QUERY_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND, e->getParsingContext())
                << ss.str();
        }
    }
    else {
        SCIDB_ASSERT(false);
    }
}

void Expression::insertConverter(TypeId newType, FunctionPointer converter,
                                 size_t resultIndex, int64_t functionIndex, bool tile)
{
    // Search index of function that output result into current result index to change its output to input of converter
    size_t resultProducer;
    for (resultProducer = functionIndex < 0 ? 0 : functionIndex; resultProducer < _functions.size(); resultProducer++)
    {
        if (_functions[resultProducer].resultIndex == resultIndex) {
            break;
        }
    }
    if (resultProducer < _functions.size()) {
        /*
         * There is needed function and we change its resultIndex
         */
        CompiledFunction f;
        f.resultIndex = resultIndex;
        f.function = converter;
        f.argIndex = _arguments.size();
        f.skipIndex = _functions[resultProducer].skipIndex;
        f.skipValue = _functions[resultProducer].skipValue;
        f.functionTypes.push_back(_arguments[resultIndex].type);
        f.functionTypes.push_back(newType);
        _arguments.push_back(_arguments[resultIndex]);
        _arguments[resultIndex].type = newType;
        _functions[resultProducer].resultIndex = f.argIndex;
        _functions.insert(_functions.begin() + functionIndex + 1, f);
        return;
    }

    // Search index of context that assign binding value to current result index to change its index to input of converter
    size_t contextIdx;
    bool found = false;
    for (contextIdx = 0; contextIdx < _contextNo.size(); contextIdx++)
    {
        vector<size_t>& indicies = _contextNo[contextIdx];
        for (resultProducer = 0; resultProducer < indicies.size(); resultProducer++) {
            if (indicies[resultProducer] == resultIndex) {
                found = true;
                break;
            }
        }
        if (found)
            break;
    }
    if (found) {
        /*
         * There is needed context and we change its resultIndex
         */
        CompiledFunction f;
        f.resultIndex = resultIndex;
        f.function = converter;
        f.argIndex = _arguments.size();
        f.functionTypes.push_back(_arguments[resultIndex].type);
        f.functionTypes.push_back(newType);
        _arguments.push_back(_arguments[resultIndex]);
        _arguments[resultIndex].type = newType;
        _contextNo[contextIdx][resultProducer] = f.argIndex;
        _functions.insert(_functions.begin() + functionIndex + 1, f);
        return;
    }

    /**
     *  There is no function which output result into position to be converter,
     *  so convert in-place and only once now.
     */
    auto& resultValue = _arguments[resultIndex].value;
    SCIDB_ASSERT(resultValue.isNull() || resultValue.isTile() || _arguments[resultIndex].type   != TID_VOID);
    SCIDB_ASSERT(resultValue.isNull() || resultValue.isTile() || resultValue.data() != NULL);
    _arguments[resultIndex].type = newType;
    Type const& resType(TypeLibrary::getType(newType));
    Value val = tile ? Value(resType,Value::asTile) : Value(resType);
    const Value* v = &resultValue;
    converter(&v, &val, NULL);
    resultValue = val;
}

void Expression::toString (std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false) << "[Expression] ";
    out << "resultType " <<  TypeLibrary::getType(_resultType).name();
    out << "\n";

    for (size_t i = 0; i < _inputSchemas.size(); i++ )
    {
        out << prefix(' ') << "[in] "<< _inputSchemas[i] << "\n";
    }
    out << prefix(' ') << "[out] "<< _outputSchema << "\n";
}

const Value& Expression::evaluate(ExpressionContext& e)
{
    SCIDB_ASSERT(e._context.size() == _contextNo.size());
    const CompiledFunction * f = NULL;

    /**
     * Loop for every function to be performed due expression evaluating
     */
    try {
       for (size_t i = _functions.size(); i > 0; i--)
        {
            f = &_functions[i - 1];

         /* Can this evaluation potentially be skipped, perhaps because it
            occurs within argument position of a special form like IIF, AND,
            or OR...? */

            if (f->skipIndex)
            {
             /* Locate the  boolean value that determines whether or not we
                short circuit: for IIF, for example, this is the conditional
                expression in the first argument position...*/

                const Value* v = e._args[f->skipIndex];

            /* Compare 'v's actual value with 'skipValue', the value that the
               expression compiler determined earlier should lead to a short
               circuit. Notice here that when used as a conditonal exprerssion,
               a null value is currently treated as if it were 'false'. PB
               notes we may want to redefine this behaviour, however...*/

                if (f->skipValue == (v->isNull() ? false : v->getBool()))
                    continue;
            }

            f->function((const Value**)&e._args[f->argIndex], e._args[f->resultIndex], e._state[i-1].get());
        }
    } catch (const Exception& ex) {
        ex.raise();
    } catch (const std::exception& ex) {
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_ERROR_IN_UDF)
            << ex.what() << f->functionName;
    } catch ( ... ) {
        throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_UNKNOWN_ERROR_IN_UDF)
            << f->functionName;
    }

    return *e._args[0];
}


void Expression::resolveFunctions()
{
    for (size_t i = 0; i < _functions.size(); i++)
    {
        CompiledFunction& f = _functions[i];
        FunctionDescription functionDesc;
        vector<FunctionPointer> converters;
        if (f.functionName.empty()) {
            // Converter case
            SCIDB_ASSERT(f.functionTypes.size() == 2);
            f.function = FunctionLibrary::getInstance()->findConverter(f.functionTypes[0], f.functionTypes[1], _tileMode);
        }
        else {
            // Function case
            if (FunctionLibrary::getInstance()->findFunction(f.functionName, f.functionTypes,
                                                             functionDesc, converters, _tileMode))
            {
                f.function = functionDesc.getFuncPtr();
                if (functionDesc.getScratchSize() > 0)
                    f.stateSize = functionDesc.getScratchSize();
                SCIDB_ASSERT(functionDesc.getOutputArg() == _arguments[f.resultIndex].type);
                SCIDB_ASSERT(converters.size() == 0);
            }
            else
            {
                stringstream ss;
                ss << f.functionName << '(';
                for (size_t i = 0; i < f.functionTypes.size(); i++) {
                    ss << TypeLibrary::getType(f.functionTypes[i]).name();
                    if (i < f.functionTypes.size() - 1)
                        ss << ", ";
                }
                ss << ')';
                throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_FUNCTION_NOT_FOUND)
                    << ss.str();
            }
        }
    }
}

void Expression::clear()
{
    _bindings.clear();
    _compiled = false;
    _constant = false;
    _contextNo.clear();
    _functions.clear();
    _nullable = false;
    _arguments.resize(1);
}

void Expression::extractSpatialConstraints(
        std::shared_ptr<SpatialRanges>& spatialRangesPtr,
        bool& hasOtherClauses)
{
    return FilterHelper::extractSpatialConstraints(this, spatialRangesPtr, hasOtherClauses);
}

namespace {
/// @brief negate() function is like unary-not that treats null differently.
/// @see Expression::negate()
void negateFunc(const Value** args, Value* res, void*)
{
    if (args[0]->isNull()) {
        res->setBool(true);
    } else {
        res->setBool(!args[0]->getBool());
    }
}
}  // namespace

std::shared_ptr<Expression> Expression::negate() const
{
    auto ret = std::make_shared<Expression>(*this);

    // push_front another copy of _argument[0] as the result of the negate() function.
    // Here setting _arguments[0] can be safely omitted for the following reasons.
    // The original _argument[0] stores the final result of the original, non-negated expression.
    // The same result can be reused after applying negate().
    // e.g. if the original expression's result had isConst=true, after applying negate() the result is still const.
    ret->_arguments.resize(ret->_arguments.size()+1);
    for (size_t i=ret->_arguments.size()-1; i>=1; --i) {
        ret->_arguments[i] = ret->_arguments[i-1];
    }

    // The number of temp values in the new expression includes this new argument and should be incremented.
    ++ret->_tempValuesNumber;

    // push_front a function, and adjust the argIndex & resultIndex of all existing functions.
    ret->_functions.resize(ret->_functions.size()+1);
    for (size_t i=ret->_functions.size()-1; i>=1; --i) {
        ret->_functions[i] = ret->_functions[i-1];
        ++ret->_functions[i].argIndex;
        ++ret->_functions[i].resultIndex;
        // skipIndex==0 means do not skip; so only increase skipIndex when it > 0.
        if (ret->_functions[i].skipIndex > 0) {
            ++ret->_functions[i].skipIndex;
        }
    }
    static vector<TypeId> negateInputArgs{TID_BOOL};
    static FunctionDescription negateFuncDesc("negate", negateInputArgs, TID_BOOL, negateFunc, 0);
    auto& newFunc = ret->_functions[0];
    newFunc.function = negateFuncDesc.getFuncPtr();
    newFunc.argIndex = 1;
    newFunc.resultIndex = 0;
    newFunc.functionName = negateFuncDesc.getName();
    newFunc.functionTypes = negateFuncDesc.getInputArgs();
    newFunc.skipValue = false;
    newFunc.skipIndex = 0;
    newFunc.stateSize = negateFuncDesc.getScratchSize();

    // Adjust the index to arguments stored in contextNo.
    for (auto itOuter = ret->_contextNo.begin(); itOuter != ret->_contextNo.end(); ++itOuter) {
        for (auto itInner = itOuter->begin(); itInner != itOuter->end(); ++itInner) {
            ++(*itInner);
        }
    }

    return ret;
}

bool Expression::isDeterministic() const
{
    return _isDeterministic;
}

bool Expression::isInstIdentical() const
{
    return _isInstIdentical;
}


// Functions
Value evaluate(std::shared_ptr<LogicalExpression> expr, TypeId expectedType)
{
    Expression e;
    e.compile(expr, !TILE_MODE, expectedType);
    return e.evaluate();
}

TypeId expressionType(std::shared_ptr<LogicalExpression> expr,
                      const vector<ArrayDesc>& inputSchemas)
{
    Expression e;
    e.compile(expr, !TILE_MODE, TID_VOID, inputSchemas);
    return e.getType();
}

} // namespace
