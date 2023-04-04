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

#include <query/LogicalOperator.h>
#include <query/Query.h>

namespace scidb {

/**
 * @brief The operator: fold().
 *
 * @par Synopsis:
 *   fold( A )
 *
 * @par Summary:
 *   Roughly speaking, converts values along the last
 *   dimension of an array into new attributes.  See
 *   https://paradigm4.atlassian.net/wiki/x/IgDcJw
 *   for more detail.
 *
 */
struct LogicalFold : LogicalOperator
{
    LogicalFold(const std::string& logicalName,
                const std::string& alias);
    virtual ~LogicalFold();

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_INPUT))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override;
};


LogicalFold::LogicalFold(const std::string& logicalName,
                         const std::string& alias)
    : LogicalOperator(logicalName, alias)
{
}

LogicalFold::~LogicalFold()
{
}

ArrayDesc LogicalFold::inferSchema(std::vector<ArrayDesc> schemas,
                                   std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(schemas.size() == 1);
    SCIDB_ASSERT(_parameters.empty());

    // Copy-out the input dimensions and massage them into
    // the output dimensions.  Save the last dimension
    // for later as we'll reference it when constructing the
    // output dimensions.
    const auto& inputSchema = schemas.front();
    auto dims = inputSchema.getDimensions();
    const DimensionDesc lastDim = dims.back();

    // The output array has one fewer dimension than the
    // input array, unless the input array is only one dimension.
    const auto targetDimSize = dims.size() - 1;
    if (targetDimSize > 0) {
        dims.resize(targetDimSize);
    }
    else {
        // The input array is only one dimension, so the
        // output array will have one dimension that has only
        // one position on it.
        const auto startMin = dims.back().getStartMin();
        dims.back().setEndMax(startMin);
        // Continue to use same chunk interval to avoid
        // special cases later.
        const auto length = dims.back().getLength();
        dims.back().setChunkInterval(length);
        dims.back().setChunkOverlap(0);
    }

    // The chunk interval along the last dimension must cover
    // the dimension length to preserve row-major ordering.
    // We compute the expected chunk interval manually to ensure
    // that it is at least as big as the interval specified in
    // the schema.  This is necessary because schemas with
    // dimension intervals less-than the dimension range
    // (e.g., '[i=0:9:0:5]') aren't allowed.
    const auto requiredChunkInterval =
        lastDim.getEndMax() - lastDim.getStartMin() + 1;
    if (lastDim.getChunkInterval() < requiredChunkInterval) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR,
                               SCIDB_LE_CHUNK_INTERVAL_TOO_SMALL)
            << requiredChunkInterval
            << lastDim.getBaseName();
    }

    // A new set of attributes for each position but the first in the
    // last dimension.
    const auto& inAttrs = inputSchema.getAttributes(true);
    auto outAttrs = inputSchema.getAttributes(true);  // copy
    const auto newAttrs = (lastDim.getLength() - 1) * inAttrs.size();

    // Copy the source attributes and rename them to avoid name collisions.
    // Attributes in the input such as
    //   v, w, x
    // with output dimension
    //   [i=0:2]
    // will result in attributes in the output having names
    //   v, w, x, $v_1, $w_1, $x_1, $v_2, $w_2, $x_2
    const std::string internalPrefix = "$";
    size_t attrSuffix = 1;
    for (size_t i = 0; i < newAttrs; ++i) {
        const auto& srcAttr = inAttrs.findattr(i % inAttrs.size());
        auto targetName =
            internalPrefix +
            srcAttr.getName() + "_" +
            std::to_string(attrSuffix);
        AttributeDesc targetAttr(
                 targetName,
                 srcAttr.getType(),
                 srcAttr.getFlags(),
                 srcAttr.getDefaultCompressionMethod());
        outAttrs.push_back(targetAttr);

        // After each pass through all of the input attributes,
        // increment the attribute suffix by one.  We know if
        // we've passed through all of the input attributes when
        // the next value of i modulo the number of new attributes
        // is zero.  Otherwise, don't increment (add zero to the suffix).
        attrSuffix += ((i + 1) % inAttrs.size() == 0 ? 1 : 0);
    }

    if (inputSchema.getAttributes().hasEmptyIndicator()) {
        outAttrs.addEmptyTagAttribute();
    }

    auto resultArrayName =
        internalPrefix +
        getOperatorName() + "_" +
        inputSchema.getName();
    ArrayDesc target(resultArrayName,
                     outAttrs,
                     dims,
                     createDistribution(dtUndefined),
                     query->getDefaultArrayResidency());

    return target;
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalFold, "_fold")

}  // namespace scidb
