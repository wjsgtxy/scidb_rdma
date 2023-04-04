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
 * @file LogicalFlatten.cpp
 * @date Aug 10, 2018
 * @author Mike Leibensperger
 */

#include "FlattenSettings.h"

#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: flatten().
 *
 * @par Synopsis:
 *   flatten( A [, cells_per_chunk: N] )
 *   flatten( DF [, cells_per_chunk: N] )
 *   flatten( A_or_DF, dimName [, cells_per_chunk: N] )
 *
 * @par Summary:
 *   Reshape an array into a data frame (1st form), change the chunk
 *   size of an existing data frame (2nd form), or reshape an array or
 *   dataframe into a 1-D array (3rd form).
 *
 * @par Input:
 *   - A - an input array
 *   - DF - a data frame
 *   - A_or_DF - either an array or a dataframe
 *   - cells_per_chunk:n - cells per result chunk
 *   - dimName - dimension name for output 1-D array dimension
 *
 * @par Output:
 *   Either a dataframe or a 1-D array with the given dimension name.
 *   All input attributes are preserved.  If the input was an array,
 *   its dimensions will become attributes prefixed to the original
 *   input attributes.
 */
class LogicalFlatten : public LogicalOperator
{
public:
    LogicalFlatten(const string& logicalName, const string& alias);
    ArrayDesc inferSchema(vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override;
    static PlistSpec const* makePlistSpec();
    std::string getInspectable() const override { return _settings.toString(); }

private:
    FlattenSettings _settings;
};


LogicalFlatten::LogicalFlatten(const string& logicalName, const string& alias)
    : LogicalOperator(logicalName, alias)
{
    _properties.dataframe = true;
}


PlistSpec const* LogicalFlatten::makePlistSpec()
{
    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_INPUT)),
             RE(RE::QMARK, {
                RE(PP(PLACEHOLDER_DIMENSION_NAME).setMustExist(false))
             })
          })
        },

        // keywords
        { FlattenSettings::KW_CELLS_PER_CHUNK,
          RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
        { FlattenSettings::KW_FAST, RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
    };
    return &argSpec;
}


ArrayDesc LogicalFlatten::inferSchema(vector<ArrayDesc> schemas,
                                      std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(schemas.size() == 1);
    ArrayDesc const& inSchema = schemas[0];
    _settings.init(inSchema, _parameters, _kwParameters, query);

    Dimensions const& inDims = inSchema.getDimensions();
    Attributes attrs;

    // Prefixed attributes, one for each input dimension.  Dataframe
    // inputs have no visible dimensions, so do this only for arrays.
    if (!_settings.inputIsDataframe()) {
        for (auto const& dim : inDims) {
            attrs.push_back(
                AttributeDesc(dim.getBaseName(),
                              TID_INT64,
                              0, // flags
                              CompressorType::NONE));
        }
    }

    // Copy the genuine input attributes.
    Attributes const& inAttrs = inSchema.getAttributes(/*exclude:*/false);
    for (auto const& inAttr : inAttrs) {
        attrs.push_back(
            AttributeDesc(inAttr.getName(),
                          inAttr.getType(),
                          inAttr.getFlags(),
                          inAttr.getDefaultCompressionMethod(),
                          inAttr.getAliases(),
                          inAttr.getReserve(),
                          &inAttr.getDefaultValue(),
                          inAttr.getDefaultValueExpr(),
                          inAttr.getVarSize()));
    }

    ArrayDistPtr dist;
    Dimensions dims;
    if (_settings.unpackMode()) {
        dims.push_back(
            DimensionDesc(_settings.getDimName(),
                          0, CoordinateBounds::getMax(), // start, end
                          _settings.getChunkSize(), 0)); // interval, overlap
        dist = createDistribution(dtUndefined);
    } else {
        dims = makeDataframeDimensions(_settings.getChunkSize());
        dist = createDistribution(dtDataframe);
    }

    return ArrayDesc(inSchema.getName(), attrs, dims, dist,
                     query->getDefaultArrayResidency());
}


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalFlatten, "flatten")

}  // namespace scidb
