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
 * LogicalCrossJoin.cpp
 *
 *  Created on: Mar 09, 2011
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <array/ArrayName.h>
#include <query/AutochunkFixer.h>
#include <query/UserQueryException.h>

#include <bitset>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: cross_join().
 *
 * @par Synopsis:
 *   cross_join( leftArray, rightArray {, attrLeft, attrRight}* )
 *
 * @par Summary:
 *   Calculates the cross product of two arrays, with 0 or more equality conditions on the dimensions.
 *   Assume p pairs of equality conditions exist. The result is an (m+n-p) dimensional array.
 *   From the coordinates of each cell in the result array, a single cell in leftArray and a single cell in rightArray can be located.
 *   The cell in the result array contains the concatenation of the attributes from the two source cells.
 *   If a pair of join dimensions have different lengths, the result array uses the smaller of the two.
 *
 * @par Input:
 *   - leftArray: the left-side source array with leftAttrs and leftDims.
 *   - rightArray: the right-side source array with rightAttrs and rightDims.
 *   - 0 or more pairs of an attribute from leftArray and an attribute from rightArray.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs + rightAttrs
 *   <br> >
 *   <br> [
 *   <br>   leftDims + (rightDims - leftDims)
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *
 *   - Given array B <v:uint64> [k] =
 *     <br> k,  v
 *     <br> 1,  10
 *     <br> 2,  20
 *     <br> 3,  30
 *     <br> 4,  40
 *     <br> 5,  50
 *   - cross_join(A, B, item, k) <quantity: uint64, sales:double, v:uint64> [year, item] =
 *     <br> year, item, quantity, sales,  v
 *     <br> 2011,  2,      7,     31.64,  20
 *     <br> 2011,  3,      6,     19.98,  30
 *     <br> 2012,  1,      5,     41.65,  10
 *     <br> 2012,  2,      9,     40.68,  20
 *     <br> 2012,  3,      8,     26.64,  30
 *
 * @par Errors:
 *   - SCIDB_SE_OPERATOR::SCIDB_LE_OP_CROSSJOIN_ERROR2: if the number of input dimensions is not even.
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_ARRAYS_NOT_CONFORMANT: if any join dimension is not an integer dimension, or if a pair of join dimensions
 *     do not have the same start, chunk interval, or overlap.
 *
 * @par Notes:
 *   - Joining non-integer dimensions does not work.
 *
 */
class LogicalCrossJoin: public LogicalOperator
{
    AutochunkFixer  _fixer;

  public:
    LogicalCrossJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
       _usage = "cross_join(left_array [ as left-alias], right_array [ as right-alias ], [left-alias.]left_dim1, [right-alias.]right_dim1,...)";
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_INPUT)),
             RE(PP(PLACEHOLDER_INPUT)),
             RE(RE::STAR, {
                     RE(PP(PLACEHOLDER_DIMENSION_NAME)),
                     RE(PP(PLACEHOLDER_DIMENSION_NAME))
             })
          })
         }
        };
        return &argSpec;
    }

    std::string getInspectable() const override
    {
        return _fixer.str();
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        // Matched dimensions must come in pairs.
        if ((_parameters.size() & 1) != 0) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_CROSSJOIN_ERROR2);
        }
        assert(schemas.size() == 2);

        // Names for things... leftFoo, rightFoo.
        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Dimensions leftDimensions = leftArrayDesc.getDimensions();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();
        Dimensions const& rightDimensions = rightArrayDesc.getDimensions();
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        AttributeDesc const* leftBitmap = leftArrayDesc.getEmptyBitmapAttribute();
        AttributeDesc const* rightBitmap = rightArrayDesc.getEmptyBitmapAttribute();
        if (leftBitmap && rightBitmap) {
            totalAttributes -= 1;
        }

        // Cannot use join-along dimension pairs if dataframes are involved.
        if (!_parameters.empty() && (isDataframe(leftDimensions) || isDataframe(rightDimensions))) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_BAD_DATAFRAME_USAGE)
                << getLogicalName()
                << "Cannot use join dimensions with dataframes, full Cartesian products only";
        }

        // Accumulate the result attributes.  Prefer the rightBitmap if present; if not then the
        // leftBitmap; if not then oh well.
        Attributes CrossJoinAttributes(totalAttributes);
        AttributeID j = 0;  // DJG
        for (size_t i = 0, n = leftAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = leftAttributes.findattr(i);
            if (!attr.isEmptyIndicator()) {
                AttributeDesc newAttr(
                    attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                newAttr.addAlias(leftArrayDesc.getName());
                CrossJoinAttributes.push_back(newAttr);
            }
        }
        for (size_t i = 0, n = rightAttributes.size(); i < n; i++) {
            AttributeDesc const& attr = rightAttributes.findattr(i);
            AttributeDesc newAttr(
                attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            newAttr.addAlias(rightArrayDesc.getName());
            CrossJoinAttributes.push_back(newAttr);
        }
        if (leftBitmap && !rightBitmap) {
            AttributeDesc const& attr = *leftBitmap;
            AttributeDesc newAttr(
                attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            newAttr.addAlias(leftArrayDesc.getName());
            CrossJoinAttributes.push_back(newAttr);
        }

        size_t nRightDims = rightDimensions.size();
        size_t nLeftDims = leftDimensions.size();
        vector<ssize_t> CrossJoinOnDimensions(nRightDims, -1);
        std::bitset<MAX_NUM_DIMS_SUPPORTED> leftCrossJoinOnMask;
        std::bitset<MAX_NUM_DIMS_SUPPORTED> rightCrossJoinOnMask;
        // For each pair of matched dimensions...
        for (size_t p = 0, np = _parameters.size(); p < np; p += 2) {
            std::shared_ptr<OperatorParamDimensionReference> leftDim = (std::shared_ptr<OperatorParamDimensionReference>&)_parameters[p];
            std::shared_ptr<OperatorParamDimensionReference> rightDim = (std::shared_ptr<OperatorParamDimensionReference>&)_parameters[p+1];

            const string &leftDimName = leftDim->getObjectName();
            const string &rightDimName = rightDim->getObjectName();
            const string &leftDimArray = leftDim->getArrayName();
            const string &rightDimArray = rightDim->getArrayName();

            // Get left dimension index, make sure it's not a repeat.
            ssize_t l = leftArrayDesc.findDimension(leftDimName, leftDimArray);
            if (l < 0) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                           leftDim->getParsingContext())
                    << leftDimName << "lefthand" << leftDimensions;
            }
            if (leftCrossJoinOnMask.test(l)) {
                // Dimension should be specified only once in parameter list.
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           leftDim->getParsingContext());
            }
            leftCrossJoinOnMask.set(l);

            // Get right dimension index, make sure it's not a repeat.
            ssize_t r = rightArrayDesc.findDimension(rightDimName, rightDimArray);
            if (r < 0) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_NOT_EXIST,
                                           rightDim->getParsingContext())
                    << rightDimName << "righthand" << rightDimensions;
            }
            if (rightCrossJoinOnMask.test(r)) {
                // Dimension should be specified only once in parameter list.
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1,
                                           rightDim->getParsingContext());
            }
            rightCrossJoinOnMask.set(r);

            // Differences in chunk size and overlap are now handled
            // via PhysicalCrossJoin::requiresRedimensionOrRepartition().
            if (leftDimensions[l].getStartMin() != rightDimensions[r].getStartMin()) {
                ostringstream ss;
                ss << leftDimensions[l] << " != " << rightDimensions[r];
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
            }

            // Build (r --> l) "joinOn" map of matched dimensions.
            if (CrossJoinOnDimensions[r] >= 0) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_CROSSJOIN_ERROR1);
            }
            CrossJoinOnDimensions[r] = l;
        }

        // Build result dimensions descriptor vector.  Left dimensions are untouched, right
        // dimensions are the unmatched ones---those not part of a matched parameter pair.
        _fixer.clear();
        j = 0;
        Dimensions CrossJoinDimensions(nLeftDims + nRightDims - _parameters.size()/2);
        // All of the left Dimensions are in the CrossJoinDimensions
        for (size_t i = 0; i < nLeftDims; i++) {
            CrossJoinDimensions[j] = leftDimensions[i];
            CrossJoinDimensions[j].addAlias(leftArrayDesc.getName());
            _fixer.takeDimension(j).fromArray(0).fromDimension(i);
            ++j;
        }
        for (size_t i = 0; i < nRightDims; i++) {
            // If the rightDimension is not part of the parameter pairs, add it
            // to the CrossJoinDimensions.
            if (CrossJoinOnDimensions[i] < 0)
            {
                CrossJoinDimensions[j] = rightDimensions[i];
                CrossJoinDimensions[j].addAlias(rightArrayDesc.getName());
                _fixer.takeDimension(j).fromArray(1).fromDimension(i);
                ++j;
            }
            else
            {
                // We are joining on this right-dimension.  Adjust the paired left-dimension
                // descriptor so that it covers its *intersection* with the paired right-dimension.
                // (We use the smaller of the two dimension lengths... and the smallest overlap.)

                DimensionDesc& d = CrossJoinDimensions[CrossJoinOnDimensions[i]];
                DimensionDesc const& right = rightDimensions[i];
                Coordinate newCurrStart = max(d.getCurrStart(), right.getCurrStart());
                Coordinate newCurrEnd = min(d.getCurrEnd(), right.getCurrEnd());
                Coordinate newEndMax = min(d.getEndMax(), right.getEndMax());
                d.setCurrStart(newCurrStart);
                d.setCurrEnd(newCurrEnd);
                d.setEndMax(newEndMax);
                d.setChunkOverlap(min(d.getChunkOverlap(), right.getChunkOverlap()));
            }
        }

        const std::string &leftName = leftArrayDesc.getName();
        const std::string &rightName = rightArrayDesc.getName();

        std::string newName;
        if(isQualifiedArrayName(leftName) ||
           isQualifiedArrayName(rightName))
        {
            newName = leftArrayDesc.getName() + std::string("~") + rightArrayDesc.getName();
        } else {
            newName = leftArrayDesc.getName() + rightArrayDesc.getName();
        }
        return ArrayDesc(newName,
                         CrossJoinAttributes,
                         CrossJoinDimensions,
                         createDistribution(dtUndefined),
                         leftArrayDesc.getResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCrossJoin, "cross_join")


} //namespace
