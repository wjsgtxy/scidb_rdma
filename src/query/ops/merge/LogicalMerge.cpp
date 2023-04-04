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
 * LogicalMerge.cpp
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
 * @brief The operator: merge().
 *
 * @par Synopsis:
 *   merge( leftArray, rightArray )
 *
 * @par Summary:
 *   Combines elements from the input arrays the following way:
 *   for each cell in the two inputs, if the cell of leftArray is not empty, the attributes from that cell are selected and placed in the output array;
 *   otherwise, the attributes from the corresponding cell in rightArray are taken.
 *   The two arrays should have the same attribute list, number of dimensions, and dimension start index.
 *   If the dimensions are not the same size, the output array uses the larger of the two.
 *
 * @par Input:
 *   - leftArray: the left-hand-side array.
 *   - rightArray: the right-hand-side array.
 *
 * @par Output array:
 *        <
 *   <br>   leftAttrs: which is equivalent to rightAttrs.
 *   <br> >
 *   <br> [
 *   <br>   max(leftDims, rightDims): for each dim, use the larger of leftDim and rightDim.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalMerge: public LogicalOperator
{
public:
    LogicalMerge(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Dataframe inputs make no sense.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_INPUT))
                 })
              })
            }
        };
        return &argSpec;
    }

    /**
     * @brief Determine what the schema of the array resulting from the merge should look like
     *
     *  @par Input:
     *   - schemas:  a vector of array descriptors describing the arrays to be merged
     *   - query:    the query that generated the merge request
     *
     * @par Output:  The descriptor that describes the merged array
     **/
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() >= 2);
        assert(_parameters.size() == 0);
        // NOTE: Merge allows > 2 input schemas.

        // Check that the attributes in the right schema(s) match the left:
        //   The number of attributes must be the same, and the attribute type
        //   needs to be the same at each position in all schemas.
        Attributes const& leftAttributes = schemas[0].getAttributes();
        Attributes const* mergedAttributes = &leftAttributes;
        for (size_t nSchema = 1; nSchema < schemas.size(); ++nSchema) {
            Attributes const& rightAttributes = schemas[nSchema].getAttributes();
            // Check attribute count.
            if (leftAttributes.size() != rightAttributes.size()
                && (leftAttributes.size() != rightAttributes.size()+1
                    || !leftAttributes.hasEmptyIndicator())
                && (leftAttributes.size()+1 != rightAttributes.size()
                    || !rightAttributes.hasEmptyIndicator()))
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTR_COUNT_MISMATCH)
                    << schemas[0] << schemas[nSchema];
            }
            // Add empty bitmap attribute, if necessary, to mergedAttributes.
            size_t nAttributes = min(leftAttributes.size(), rightAttributes.size());
            if (rightAttributes.size() > mergedAttributes->size()) {
                mergedAttributes = &rightAttributes;
            }
            // Check the types of the Attributes.
            for (size_t nAttribute = 0; nAttribute < nAttributes; nAttribute++) {
                AttributeDesc const& leftAttribute = leftAttributes.findattr(nAttribute);
                AttributeDesc const& rightAttribute = rightAttributes.findattr(nAttribute);
                if (leftAttribute.getType() != rightAttribute.getType()
                    || leftAttribute.getFlags() != rightAttribute.getFlags())
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTR_TYPE_MISMATCH)
                        << leftAttribute.getName() << rightAttribute.getName()
                        << schemas[0] << schemas[nSchema];
                }
            }
        }

        // Find the left most non-autochunked schema as the exemplar for the
        // chunkInterval and chunkOverlap of the dimensions.
        size_t exemplarIndex = 0;
        for (auto const &input : schemas) {
            if (! input.isAutochunked()) {
                break;
            }
            ++exemplarIndex;
        }
        if (exemplarIndex == schemas.size()) {
            // All the schemas are autochunked. There is no way to determine the
            // exemplar schema.
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ALL_INPUTS_AUTOCHUNKED)
                << getLogicalName();
        }

        // Set the required chunkOverlap/chunkInterval based upon the exemplar schema.
        // Set dimension names based upon the left dimension.
        Dimensions const& exemplarDimensions = schemas[exemplarIndex].getDimensions();
        Dimensions const& leftDimensions = schemas[0].getDimensions();
        size_t nDimensions = exemplarDimensions.size();
        Dimensions mergedDimensions(nDimensions);
        for (size_t nSchema = 0; nSchema < schemas.size(); nSchema++) {
            if (nSchema == exemplarIndex) {
                // This schema is the exemplar, so the dimensions already match.
                continue;
            }
           Dimensions const& targetDimensions = schemas[nSchema].getDimensions();

            if (nDimensions != targetDimensions.size()) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                    << getLogicalName() << schemas[exemplarIndex] << schemas[nSchema];
            }

            // Report all startIndex problems at once.
            ostringstream ss;
            int mismatches = 0;
            for (size_t i = 0; i < nDimensions; i++) {
                DimensionDesc const& exemplarDim = exemplarDimensions[i];
                DimensionDesc const& targetDim = targetDimensions[i];
                if(exemplarDim.getStartMin() != targetDim.getStartMin()) {
                    if (mismatches++) {
                        ss << ", ";
                    }
                    ss << '[' << exemplarDim << "] != [" << targetDim << ']';
                }
            }
            if (mismatches) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH) << ss.str();
            }

            for (size_t i = 0; i < nDimensions; i++) {
                DimensionDesc const& exemplarDim = exemplarDimensions[i];
                DimensionDesc const& leftDim     = leftDimensions[i];
                DimensionDesc const& targetDim   = targetDimensions[i];

                mergedDimensions[i] = DimensionDesc(
                    leftDim.getBaseName(),
                    leftDim.getNamesAndAliases(),
                    exemplarDim.getStartMin(),
                    min(exemplarDim.getCurrStart(), targetDim.getCurrStart()),
                    max(exemplarDim.getCurrEnd(),   targetDim.getCurrEnd()),
                    max(exemplarDim.getEndMax(),    targetDim.getEndMax()),
                    exemplarDim.getChunkInterval(),
                    exemplarDim.getChunkOverlap());
            }
        }  // for (size_t nSchema = 1;  ... ) { ... }

        return ArrayDesc(
            schemas[0].getName(),
            *mergedAttributes,
            mergedDimensions,
            createDistribution(dtUndefined), // unknown until the physical stage
            query->getDefaultArrayResidency() );
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalMerge, "merge")

} //namespace
