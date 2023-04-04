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
 * LogicalRedimension.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include "RedimSettings.h"

#include <array/ArrayDistributionInterface.h>
#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: redimension().
 *
 * @par Synopsis:
 *   redimension( srcArray, schemaArray | schema , isStrict=true | {, AGGREGATE_CALL}* )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Produces a array using some or all of the variables of a source array, potentially changing some or all of those variables from dimensions
 *   to attributes or vice versa, and optionally calculating aggregates to be included in the new array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - schemaArray | schema: an array or schema from which outputAttrs and outputDims can be acquired.
 *     All the dimensions in outputDims must exist either in srcAttrs or in srcDims, with one exception. One new dimension called the synthetic dimension
 *     is allowed. All the attributes in outputAttrs, which is not the result of an aggregate, must exist either in srcAttrs or in srcDims.
 *   - isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false.
 *     In case of aggregates, isStrict requires that the aggreates be specified for all source array attributes which are also attributes in the new array.
 *     In case of synthetic dimension, isStrict has no effect.
 *   - 0 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     The resultNames must already exist in outputAttrs.
 *
 * @par Output array:
 *        <
 *   <br>   outputAttrs
 *   <br> >
 *   <br> [
 *   <br>   outputDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - The synthetic dimension cannot co-exist with aggregates. That is, if there exists at least one aggregate call, the synthetic dimension must not exist.
 *   - When multiple values are "redimensioned" into the same cell in the output array, the collision handling depends on the schema:
 *     (a) If there exists a synthetic dimension, all the values are retained in a vector along the synthetic dimension.
 *     (b) Otherwise, for an aggregate attribute, the aggregate result of the values is stored.
 *     (c) Otherwise, an arbitrary value is picked and the rest are discarded.
 *   - Current redimension() does not support Non-integer dimensions or data larger than memory.
 *
 */
class LogicalRedimension: public  LogicalOperator
{
public:
    LogicalRedimension(const string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
    {
        // When schema parameter is a schema and not a reference, that schema is
        // allowed to refer to reserved attribute or dimension names.
        _properties.schemaReservedNames = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_SCHEMA)),
                 RE(RE::QMARK, { RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) } ),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_AGGREGATE_CALL))
                 })
              })
            },
            { RedimSettings::KW_CELLS_PER_CHUNK, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { RedimSettings::KW_PHYS_CHUNK_SIZE, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { RedimSettings::KW_COLLISION_RATIO, RE(PP(PLACEHOLDER_CONSTANT, TID_FLOAT)) },
            { RedimSettings::KW_OFFSET,
              RE(RE::GROUP, { RE(RE::PLUS, { RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) }) })
            },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        // RedimSettings constructor validates the keyword parameters.
        RedimSettings settings(getLogicalName(), _kwParameters, /*logical:*/true);

        ArrayDesc const& srcDesc = schemas[0];
        ArrayDesc dstDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        // The user can refer to reserved names but not introduce new dimensions or attributes
        // with reserved names.
        if (isUserInserted() &&
            dstDesc.hasReservedNames() &&
            !srcDesc.hasReservedNames()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_RESERVED_NAMES_IN_SCHEMA)
                << getLogicalName();
        }

        if (!settings.getOffset().empty()
                && settings.getOffset().size() != dstDesc.getDimensions().size()) {
            stringstream ss;
            ss << RedimSettings::KW_OFFSET << ':' << CoordsToStr(settings.getOffset());
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                << dstDesc.getDimensions() << ss.str();
        }

        //Compile a desc of all possible attributes (aggregate calls first) and source dimensions
        ArrayDesc aggregationDesc (srcDesc.getName(), Attributes(), srcDesc.getDimensions(),
                                   createDistribution(getSynthesizedDistType()),
                                   srcDesc.getResidency());
        vector<string> aggregatedNames;
        bool isStrictSet = false;
        bool isStrict=true;

        //add aggregate calls first
        for (size_t i = 1; i < _parameters.size(); ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_LOGICAL_EXPRESSION) {
                assert(i==1);
                SCIDB_ASSERT(!isStrictSet);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[i].get());
                assert(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
                isStrictSet = true;
                isStrict = evaluate(lExp->getExpression(), TID_BOOL).get<bool>();
                continue;
            }

            const bool isInOrderAggregation = false; // We can't guarantee any ordering when computing aggregates.
            addAggregatedAttribute( (std::shared_ptr <OperatorParamAggregateCall>&) _parameters[i], srcDesc, aggregationDesc,
                                    isInOrderAggregation);
            string aggName =  aggregationDesc.getAttributes().findattr(aggregationDesc.getAttributes().size()-1).getName();  // DJG
            bool aggFound = false;
            for (size_t i = 0; i < dstDesc.getAttributes().size(); ++i) {
                const AttributeDesc &dstAttr = dstDesc.getAttributes().findattr(i);
                if (dstAttr.getName() == aggName) {
                    aggFound = true;
                    break;
                }
            }
            if (!aggFound) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ATTRIBUTE_DOESNT_EXIST) << aggName << dstDesc.getName();
            }
            aggregatedNames.push_back(aggName);
        }

        //add other attributes
        for (const auto& srcAttr : srcDesc.getAttributes())
        {
            //if there's an attribute with same name as an aggregate call - skip the attribute
            bool found = false;
            for (const auto& aggAttr : aggregationDesc.getAttributes())
            {
                if( aggAttr.getName() == srcAttr.getName())
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                aggregationDesc.addAttribute(
                    AttributeDesc(srcAttr.getName(),
                                  srcAttr.getType(),
                                  srcAttr.getFlags(),
                                  srcAttr.getDefaultCompressionMethod(),
                                  srcAttr.getAliases(),
                                  &srcAttr.getDefaultValue(),
                                  srcAttr.getDefaultValueExpr(),
                                  srcAttr.getVarSize()));
            }
        }

        if (!dstDesc.getEmptyBitmapAttribute()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_ERROR1);
        }

        //Ensure attributes names uniqueness.
        size_t numPreservedAttributes = 0;
        for (const auto& dstAttr : dstDesc.getAttributes())
        {
            // Look for dstAttr among the source and aggregate attributes.
            for (const auto& srcAttr : aggregationDesc.getAttributes())
            {
                if (srcAttr.getName() == dstAttr.getName())
                {
                    if (srcAttr.getType() != dstAttr.getType())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
                        << srcAttr.getName() << srcAttr.getType() << dstAttr.getType();
                    }
                    if (!dstAttr.isNullable() && srcAttr.isNullable())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
                        << srcAttr.getName();
                    }
                    if (!srcAttr.isEmptyIndicator())
                    {
                        ++numPreservedAttributes;
                    }
                    goto NextAttr;
                }
            }

            // Not among the source attributes, look for it among source dimensions (copied to
            // aggregationDesc above).
            for (const DimensionDesc &srcDim : aggregationDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstAttr.getName()))
                {
                    if (dstAttr.getType() != TID_INT64)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_DESTINATION_ATTRIBUTE_TYPE)
                        << dstAttr.getName() << TID_INT64;
                    }

                    goto NextAttr;
                }
            }

            // This dstAttr should now be accounted for (i.e. we know where it's derived from), so
            // if we get here then this one better be the emptyBitmap.
            if (dstAttr.isEmptyIndicator() == false)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_ATTRIBUTE)
                << dstAttr.getName();
            }
        NextAttr:;
        }

        // Similarly, make sure we know how each dstDim is derived.
        Dimensions outputDims;
        size_t nNewDims = 0;
        for (const DimensionDesc &dstDim : dstDesc.getDimensions())
        {
            int64_t interval = dstDim.getChunkIntervalIfAutoUse(std::max(1L, dstDim.getChunkOverlap()));
            if (dstDim.getChunkOverlap() > interval)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OVERLAP_CANT_BE_LARGER_CHUNK);
            }
            for (const auto& srcAttr : aggregationDesc.getAttributes())
            {
                if (dstDim.hasNameAndAlias(srcAttr.getName()))
                {
                    for (size_t i = 0; i< aggregatedNames.size(); i++)
                    {
                        if (srcAttr.getName() == aggregatedNames[i])
                            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_ERROR2);
                    }
                    if ( !IS_INTEGRAL(srcAttr.getType())  || srcAttr.getType() == TID_UINT64 )
                    {
                        // (TID_UINT64 is the only integral type that won't safely convert to TID_INT64.)
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_SOURCE_ATTRIBUTE_TYPE)
                            << srcAttr.getName();
                    }
                    outputDims.push_back(dstDim);
                    goto NextDim;
                }
            }
            for (const DimensionDesc &srcDim : aggregationDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstDim.getBaseName()))
                {
                    DimensionDesc outputDim = dstDim;
                    outputDims.push_back(outputDim);
                    goto NextDim;
                }
            }
            // One synthetic dimension allowed.  Can't have both synthetic dimension *and*
            // aggregates (because both rely on cell collisions in the output schema's
            // coordinate system).
            if (nNewDims++ != 0 || !aggregatedNames.empty() )
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNEXPECTED_DESTINATION_DIMENSION)
                    << dstDim.getBaseName();
            }
            if (DimensionDesc::isReservedName(dstDim.getBaseName())) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_RESERVED_NAMES_IN_SCHEMA)
                    << getLogicalName();
            }
            outputDims.push_back(dstDim);
        NextDim:;
        }

        if (isStrict &&
            !aggregatedNames.empty() &&
            numPreservedAttributes != aggregatedNames.size()) {

            stringstream ss; ss << "zero or exactly "<< numPreservedAttributes << " aggregate";
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3)
                  << "redimension" << ss.str();
        }

        return ArrayDesc(srcDesc.getName(),
                         dstDesc.getAttributes(),
                         outputDims,
                         createDistribution(dtUndefined),
                         query->getDefaultArrayResidency(),
                         dstDesc.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRedimension, "redimension")

}  // namespace scidb
