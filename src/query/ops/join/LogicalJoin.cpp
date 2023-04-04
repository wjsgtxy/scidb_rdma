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
 * LogicalJoin.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>
#include <system/Exceptions.h>


using namespace std;

namespace scidb
{

/**
 * @brief The operator: join().
 *
 * @par Synopsis:
 *   join( leftArray, rightArray )
 *
 * @par Summary:
 *   Combines the attributes of two arrays at matching dimension values.
 *   The two arrays must have the same dimension start coordinates, the same chunk size, and the
 *   same chunk overlap.
 *   The join result has the same dimension names as the first input.
 *   The cell in the result array contains the concatenation of the attributes from the two source cells.
 *   If a pair of join dimensions have different lengths, the result array uses the smaller of the two.
 *
 * @par Input:
 *   - leftArray: the left-side source array with leftAttrs and leftDims.
 *   - rightArray: the right-side source array with rightAttrs and rightDims.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs + rightAttrs: in case an attribute in rightAttrs conflicts with an attribute in leftAttrs, '_2' will be appended.
 *   <br> >
 *   <br> [
 *   <br>   leftDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - join() is a special case of cross_join() with all pairs of dimensions given.
 *
 */
class LogicalJoin: public LogicalOperator
{
  public:
    LogicalJoin(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Makes no sense for dataframe inputs.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT))
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        ArrayDesc const& leftArrayDesc = schemas[0];
        ArrayDesc const& rightArrayDesc = schemas[1];
        Attributes const& leftAttributes = leftArrayDesc.getAttributes();
        Attributes const& rightAttributes = rightArrayDesc.getAttributes();

        // The join attributes are the set of attributes formed from the union of the
        // attributes of the two inputs.
        size_t totalAttributes = leftAttributes.size() + rightAttributes.size();
        int nBitmaps = 0;
        nBitmaps += (leftAttributes.hasEmptyIndicator());
        nBitmaps += (rightAttributes.hasEmptyIndicator());
        if (nBitmaps == 2) {
            // The attributes for the join output only require one empty bitmap
            // attribute.
            totalAttributes -= 1;
        }
        Attributes joinAttributes(totalAttributes);

        // Add all of the attributes from the first (left) input (except any
        // empty bitmap attribute).
        for (const auto& attr : leftAttributes) {
            if (!attr.isEmptyIndicator()) {  // TODO remove this once operator[] doesn't include the EBM
                AttributeDesc newAttr(attr.getName(), attr.getType(), attr.getFlags(),
                    attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                    attr.getDefaultValueExpr());
                newAttr.addAlias(leftArrayDesc.getName());
                joinAttributes.push_back(newAttr);
            }
        }
        // Add all the attributes from the second (right) input (including any
        // empty bitmap attribute).
        // The set of join attributes ONLY needs one empty bitmap attribute.
        // Prefer using the one given in the second (right) input attribute set, otherwise
        // use a completely new empty bitmap attribute.
        for (const auto& attr : rightAttributes) {
            AttributeDesc newAttr(attr.getName(), attr.getType(), attr.getFlags(),
                attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
                attr.getDefaultValueExpr());
            newAttr.addAlias(rightArrayDesc.getName());
            joinAttributes.push_back(newAttr);
        }

        // The exemplar schema is the left-most Non-autochunked schema. It will
        // be used to define the needed chunkInterval.
        size_t exemplarIndex = 0;
        size_t targetIndex = 1;
        if (leftArrayDesc.isAutochunked()) {
            if (rightArrayDesc.isAutochunked()) {
                // Only one input may be autochunked
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
                    << getLogicalName();
            }
            // The left input is autochunked, so the exemplar is the right
            // schema (index 1)
            exemplarIndex = 1;
            targetIndex = 0;
        }

        Dimensions const& exemplarDimensions = schemas[exemplarIndex].getDimensions();
        Dimensions const& targetDimensions = schemas[targetIndex].getDimensions();
        // Check that the two inputs have the same number of dimensions.
        if(exemplarDimensions.size() != targetDimensions.size())
        {
            ostringstream exemplar, target;
            printDimNames(exemplar, exemplarDimensions);
            printDimNames(target, targetDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << getLogicalName() << exemplar.str() << target.str();
        }

        // Check that the corresponding dimensions in the two inputs have the
        // same starting index (startMin). Report any and all mismatches.
        ostringstream ss;
        int mismatches = 0;
        for (size_t i = 0, n = exemplarDimensions.size(); i < n; i++)
        {
            if(exemplarDimensions[i].getStartMin() != targetDimensions[i].getStartMin())
            {
                if (mismatches++) {
                    ss << ", ";
                }
                ss << '[' << exemplarDimensions[i] << "] != [" << targetDimensions[i] << ']';
            }
        }
        if (mismatches)
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
        }

        Dimensions joinDimensions;
        for (size_t i = 0, n = exemplarDimensions.size(); i < n; i++)
        {
            assert(exemplarDimensions[i].getStartMin() == targetDimensions[i].getStartMin());
            DimensionDesc const& exemplarDim = exemplarDimensions[i];
            DimensionDesc const& targetDim = targetDimensions[i];
            // The names of the dimensions in the output are defined by the names
            // of the first (left) schema, which is not necessarily the exemplarDim.
            DimensionDesc const& leftDim = leftArrayDesc.getDimensions()[i];
            joinDimensions.push_back(
                DimensionDesc(
                    leftDim.getBaseName(),
                    leftDim.getNamesAndAliases(),
                    exemplarDim.getStartMin(),
                    max(exemplarDim.getCurrStart(), targetDim.getCurrStart()),
                    min(exemplarDim.getCurrEnd(), targetDim.getCurrEnd()),
                    min(exemplarDim.getEndMax(), targetDim.getEndMax()),
                    exemplarDim.getChunkInterval(),
                    min(exemplarDim.getChunkOverlap(), targetDim.getChunkOverlap())
                    )
                );
            joinDimensions[i].addAlias(leftArrayDesc.getName());

            // We're taking the leftDim names, but fold in the rightDim names 'n aliases in case
            // some other part of the query references them.
            DimensionDesc const& rightDim = rightArrayDesc.getDimensions()[i];
            for (const ObjectNames::NamesPairType& rDimName : rightDim.getNamesAndAliases()) {
                for (const string& alias : rDimName.second) {
                   joinDimensions[i].addAlias(alias, rDimName.first);
                }
            }
        }

        // joinAttributes will have an empty tag if and only if the
        // right-hand array has an empty tag.
        return ArrayDesc(leftArrayDesc.getName() + rightArrayDesc.getName(),
                         joinAttributes,
                         joinDimensions,
                         createDistribution(dtUndefined), // Distribution is unknown until the physical stage.
                         query->getDefaultArrayResidency() );
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalJoin, "join")


} //namespace
