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
 * LogicalUnpack.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 *      Author: poliocough@gmail.com
 */

#include <array/ArrayDistributionInterface.h>
#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <system/Config.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/***
 * Helper function to construct array descriptor for the unpacked array
 ***/
ArrayDesc addAttributes(ArrayDesc const& desc,
                        std::string const& dimName,
                        size_t const chunkSize,
                        std::shared_ptr<Query> const& query)
{
    Attributes const& oldAttributes = desc.getAttributes();
    Dimensions const& dims = desc.getDimensions();
    Attributes newAttributes;
    if (!isDataframe(dims)) {
        for (size_t j = 0; j < dims.size(); ++j) {
            newAttributes.push_back(AttributeDesc(dims[j].getBaseName(),
                                                  TID_INT64, 0, CompressorType::NONE));
        }
    }

    for (size_t j = 0; j < oldAttributes.size(); ++j)
    {
        AttributeDesc const& attr = oldAttributes.findattr(j);
        newAttributes.push_back(AttributeDesc(attr.getName(), attr.getType(), attr.getFlags(),
            attr.getDefaultCompressionMethod(), attr.getAliases(), &attr.getDefaultValue(),
            attr.getDefaultValueExpr()));
    }

    Dimensions newDimensions(1);
    newDimensions[0] = DimensionDesc(
        dimName, 0, 0,
        CoordinateBounds::getMax(),
        CoordinateBounds::getMax(),
        chunkSize, 0);

    return ArrayDesc(desc.getName(),
                     newAttributes.addEmptyTagAttribute(),
                     newDimensions,
                     createDistribution(dtUndefined),
                     desc.getResidency() );
}

/**
 * @brief The operator: unpack().
 *
 * @par Synopsis:
 *   unpack( srcArray, newDim {, CHUNK_SIZE} )
 *   unpack( srcArray, newDim {, chunk_size: CHUNK_SIZE} )
 *
 * @par Summary:
 *   Unpacks a multi-dimensional array into a single-dimensional array,
 *   creating new attributes to represent the dimensions in the source array.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - newDim: the name of the dimension in the result 1D array.
 *   - CHUNK_SIZE: force chunk size for output array (optional)
 *
 * @par Output array:
 *        <
 *   <br>   srcDims (as attributes in the output), followed by srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   newDim: start=0, end=#logical cells in srcArray less 1, chunk
 *                  interval=the chunk interval of the last dimension in srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   If the srcArray is a dataframe, new attributes are not added.  Dataframes
 *   have no concept of position, so there is no position to preserve.
 */
class LogicalUnpack: public LogicalOperator
{
public:
    LogicalUnpack(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_DIMENSION_NAME).setMustExist(false)),
                 RE(RE::QMARK, { RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) } )
              })
            },
            { "chunk_size", RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getParamType() == PARAM_DIMENSION_REF);
        const string &dimName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        size_t chunkSize = 0;
        Parameter chunkParam = findKeyword("chunk_size");
        if (_parameters.size() == 2)
        {
            if (chunkParam) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL)
                    << getLogicalName() << "chunk_size" << 2;
            }
            chunkParam = _parameters[1];
        }
        if (chunkParam)
        {
            chunkSize = paramToInt64(chunkParam);
            if(chunkSize <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CHUNK_SIZE_MUST_BE_POSITIVE);
            }
        }

        size_t inputSchemaSize = schemas[0].getSize();
        if (chunkSize == 0) //not set by user
        {
            // Target cells per chunk (default 1M) is a good/recommended chunk size for most
            // one-dimensional arrays -- unless you are using large strings or UDTs.
            chunkSize = Config::getInstance()->getOption<size_t>(CONFIG_TARGET_CELLS_PER_CHUNK);

            //If there's no way that the input has one million elements - reduce the chunk size further.
            //This is ONLY done for aesthetic purposes - don't want to see one million "()" on the screen.
            //In fact, sometimes it can become a liability...
            if(inputSchemaSize<chunkSize)
            {
                //make chunk size at least 1 to avoid errors
                chunkSize = std::max<size_t>(inputSchemaSize,1);
            }
        }

        return addAttributes(schemas[0], dimName, chunkSize, query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalUnpack, "unpack")

} //namespace
