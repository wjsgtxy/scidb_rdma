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
 * LogicalAggregate.cpp
 *
 *  Created on: Jul 7, 2011
 *      Author: poliocough@gmail.com
 */

#include <query/LogicalOperator.h>

#include <query/Aggregate.h>
#include <query/AutochunkFixer.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/Query.h>
#include <query/TypeSystem.h>
#include <system/Exceptions.h>
#include <util/Arena.h>

#include <set>

namespace scidb {

template<class T>
using Set = std::set<T, std::less<T>, arena::ScopedAllocatorAdaptor<T>>;

using namespace std;

int64_t constexpr UNDEF_CHUNKSIZE = -1L; // (Same as AUTOCHUNKED but usage does not collide, so OK.)

/**
 * @brief The operator: aggregate().
 *
 * @par Synopsis:
 *   aggregate( srcArray {, AGGREGATE_CALL}+ {, groupbyDim}* {, chunkSize}* )
 *   <br> AGGREGATE_CALL := AGGREGATE_FUNC(inputAttr) [as resultName]
 *   <br> AGGREGATE_FUNC := approxdc | avg | count | max | min | sum | stdev | var | some_use_defined_aggregate_function
 *
 * @par Summary:
 *   Calculates aggregates over groups of values in an array, given the aggregate types and attributes to aggregate on. <br>
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more aggregate calls.
 *     Each aggregate call has an AGGREGATE_FUNC, an inputAttr and a resultName.
 *     The default resultName is inputAttr followed by '_' and then AGGREGATE_FUNC.
 *     For instance, the default resultName for sum(sales) is 'sales_sum'.
 *     The count aggregate may take * as the input attribute, meaning to count all the items in the group including null items.
 *     The default resultName for count(*) is 'count'.
 *   - 0 or more dimensions that together determines the grouping criteria.
 *   - 0 or numGroupbyDims chunk sizes.
 *     If no chunk size is given, the groupby dims will inherit chunk sizes from the input array.
 *     If at least one chunk size is given, the number of chunk sizes must be equal to the number of groupby dimensions,
 *     and the groupby dimensions will use the specified chunk sizes.
 *
 * @par Output array:
 *        <
 *   <br>   The aggregate calls' resultNames.
 *   <br> >
 *   <br> [
 *   <br>   The list of groupbyDims if provided (with the specified chunk sizes if provided),
 *   <br>   or
 *   <br>   'i' if no groupbyDim is provided.
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
 *   - aggregate(A, count(*), max(quantity), sum(sales), year) <count: uint64, quantity_max: uint64, sales_sum: double> [year] =
 *     <br> year, count, quantity_max, sales_sum
 *     <br> 2011,   2,      7,           51.62
 *     <br> 2012,   3,      9,          108.97
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - All the aggregate functions ignore null values, except count(*).
 *
 */
class LogicalAggregate: public  LogicalOperator
{
    AutochunkFixer _fixer;

public:
    LogicalAggregate(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.dataframe = true; // Supports dataframes... unless we decide otherwise below.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::PLUS, {
                    RE(PP(PLACEHOLDER_AGGREGATE_CALL))
                 }),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_DIMENSION_NAME))
                 }),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_INT64))
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

    /**
     * @param inputDims  the input dimensions.
     * @param outDims    the output dimensions.
     * @param param      the OperatorParam object.
     * @param chunkSize  the chunkSize for the dimension; UNDEF_CHUNKSIZE means to use the input dimension size.
     */
    void addDimension(Dimensions const& inputDims,
                      Dimensions& outDims,
                      std::shared_ptr<OperatorParam> const& param,
                      int64_t chunkSize)
    {
        std::shared_ptr<OperatorParamReference> const& reference =
            (std::shared_ptr<OperatorParamReference> const&) param;

        string const& dimName = reference->getObjectName();
        string const& dimAlias = reference->getArrayName();

        for (size_t j = 0, n = inputDims.size(); j < n; j++)
        {
            if (inputDims[j].hasNameAndAlias(dimName, dimAlias))
            {
                //no overlap
                outDims.push_back(DimensionDesc( inputDims[j].getBaseName(),
                                                 inputDims[j].getNamesAndAliases(),
                                                 inputDims[j].getStartMin(),
                                                 inputDims[j].getCurrStart(),
                                                 inputDims[j].getCurrEnd(),
                                                 inputDims[j].getEndMax(),
                                                 chunkSize == UNDEF_CHUNKSIZE
                                                     ? inputDims[j].getRawChunkInterval()
                                                     : chunkSize,
                                                 0));
                if (chunkSize == UNDEF_CHUNKSIZE) {
                    _fixer.takeDimension(outDims.size()-1).fromArray(0).fromDimension(j);
                }
                return;
            }
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_DIMENSION_NOT_EXIST)
            << dimName << "aggregate input" << inputDims;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        ArrayDesc const& input = schemas[0];

        Dimensions const& inputDims = input.getDimensions();

        Attributes outAttrs;
        Dimensions outDims;

        if (_parameters.empty())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2)
                << getLogicalName();
        }

        removeDuplicateDimensions();

        // How many parameters are of each type.
        size_t numAggregateCalls = 0;
        size_t numGroupbyDims = 0;
        size_t numChunkSizes = 0;
        for (size_t i = 0, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                ++numAggregateCalls;
            }
            else if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                ++numGroupbyDims;
            }
            else // chunk size
            {
                ++numChunkSizes;
            }
        }
        if (numChunkSizes && numChunkSizes != numGroupbyDims) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_NUM_CHUNKSIZES_NOT_MATCH_NUM_DIMS)
                << getLogicalName();
        }

        if (numGroupbyDims && isDataframe(inputDims)) {
            stringstream ss;
            ss << getLogicalName() << " with group-by dimension(s)";
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DATAFRAMES_NOT_SUPPORTED)
                << ss.str();
        }

        for (size_t i = 0, n = _parameters.size(); i < n; ++i)
        {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF)
            {
                int64_t chunkSize = UNDEF_CHUNKSIZE;
                if (numChunkSizes) {
                    // E.g. here are the parameters:
                    //       0           1      2        3            4
                    // AGGREGATE_CALL,  dim1,  dim2,  chunkSize1,  chunkSize2
                    // i=1 ==> index = 3
                    // i=2 ==> index = 4
                    size_t index = i + numGroupbyDims;
                    assert(index < _parameters.size());
                    chunkSize = evaluate(
                        ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[index])->getExpression(),
                        TID_INT64).getInt64();
                    if (chunkSize<=0) {
                        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
                    }
                }
                addDimension(inputDims, outDims, _parameters[i], chunkSize);
            }
        }

        bool grand = true;
        if (outDims.size() == 0)
        {
            outDims.push_back(DimensionDesc("i", 0, 0, 0, 0, 1, 0));
        }
        else
        {
            _properties.tile = false;
            grand = false;
        }

        ArrayDesc outSchema(input.getName(), Attributes(), outDims,
                            createDistribution(getSynthesizedDistType()),   // gets fed to the physical
                            query->getDefaultArrayResidency());

        for (size_t i =0, n = _parameters.size(); i<n; i++)
        {
            if (_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
            {
                bool isInOrderAggregation = false;
                addAggregatedAttribute( (std::shared_ptr <OperatorParamAggregateCall> &) _parameters[i], input,
                                        outSchema, isInOrderAggregation);
            }
        }

        if(!grand)
        {
            outSchema.addEmptyTagAttribute();
        }
        return outSchema;
    }

private:

    /**
     * Remove duplicate dimension references from the parameter list.
     */
    void removeDuplicateDimensions()
    {
        typedef std::pair<std::string, std::string> NameAndAlias;
        using DimNameSet = Set<NameAndAlias>;

        DimNameSet seen;
        Parameters tmp;
        tmp.reserve(_parameters.size());

        for (size_t i = 0; i < _parameters.size(); ++i) {
            if (_parameters[i]->getParamType() == PARAM_DIMENSION_REF) {
                const OperatorParamDimensionReference* dimRef =
                    safe_dynamic_cast<OperatorParamDimensionReference*>(_parameters[i].get());
                NameAndAlias key(dimRef->getObjectName(), dimRef->getArrayName());
                DimNameSet::iterator pos = seen.find(key);
                if (pos == seen.end()) {
                    tmp.push_back(_parameters[i]);
                    seen.insert(key);
                }
                // ...else a duplicate, ignore it.
            } else {
                tmp.push_back(_parameters[i]);
            }
        }

        _parameters.swap(tmp);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAggregate, "aggregate")

}  // namespace scidb
