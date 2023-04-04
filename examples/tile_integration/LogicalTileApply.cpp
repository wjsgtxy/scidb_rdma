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

#include <query/Expression.h>
#include <query/TypeSystem.h>
#include <system/Exceptions.h>

namespace scidb {

using namespace std;

/**
 * @brief The operator: tile_apply().
 *
 * @par Synopsis:
 *   tile_apply(srcArray {, newAttr, expression}+)
 *
 * @par Summary:
 *   Produces a result array with new attributes and computes values for them.
 *   It supports the getData() API to produce tiles, and internally operates on tiles
 *   (i.e. pulls tiles from the input and generates tiles as the output).
 *   Unlike apply(), it does not perform vectorized expression evaluation and often runs slower.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more pairs of a new attribute and the expression to compute the values for the attribute.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br>   the list of newAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
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
 *   - tile_apply(A, unitprice, sales/quantity) <quantity: uint64, sales: double, unitprice: double> [year, item] =
 *     <br> year, item, quantity, sales, unitprice
 *     <br> 2011,  2,      7,     31.64,   4.52
 *     <br> 2011,  3,      6,     19.98,   3.33
 *     <br> 2012,  1,      5,     41.65,   8.33
 *     <br> 2012,  2,      9,     40.68,   4.52
 *     <br> 2012,  3,      8,     26.64,   3.33
 *
 * @par Errors:
 *   - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME, if a new attribute has the same name as an existing attribute.
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalTileApply: public  LogicalOperator
{
public:
    LogicalTileApply(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = false;  // we dont run in the old tile mode
    }

    static PlistSpec const* makePlistSpec()
    {
        // Some shorthand definitions.
        PP const PP_EXPR(PLACEHOLDER_EXPRESSION, TID_VOID);
        PP const PP_ATTR_OUT =
            PP(PLACEHOLDER_ATTRIBUTE_NAME).setMustExist(false);

        // The parameter list specification.
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::OR, {
                    RE(RE::PLUS, { RE(PP_ATTR_OUT), RE(PP_EXPR) }),
                    RE(RE::PLUS,
                     { RE(RE::GROUP, { RE(PP_ATTR_OUT), RE(PP_EXPR) }) })
                  })
               })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_ATTRIBUTE_REF);
        assert(_parameters[1]->getParamType() == PARAM_LOGICAL_EXPRESSION);

        if ( _parameters.size() % 2 != 0 )
        {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT2) << "tile_apply";
        }

        Attributes outAttrs;

        const auto& attrs = schemas[0].getAttributes(true);
        for (const auto& attr : attrs) {
            outAttrs.push_back( AttributeDesc(attr.getName(),
                                              attr.getType(),
                                              attr.getFlags(),
                                              attr.getDefaultCompressionMethod(),
                                              attr.getAliases(),
                                              attr.getReserve(),
                                              &attr.getDefaultValue(),
                                              attr.getDefaultValueExpr(),
                                              attr.getVarSize()));
        }

        size_t k;
        for (k=0; k<_parameters.size(); k+=2) {

            const string &attributeName = ((std::shared_ptr<OperatorParamReference>&)_parameters[k])->getObjectName();
            Expression expr;
            expr.compile(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[k+1])->getExpression(),
                         _properties.tile, TID_VOID, schemas);
            assert(!_properties.tile);

            int16_t flags = 0;
            if (expr.isNullable()) {
                flags = AttributeDesc::IS_NULLABLE;
            }

            for (const auto& attr : outAttrs) {
                if (attr.getName() ==  attributeName)
                {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << attributeName;
                }
            }

            outAttrs.push_back(AttributeDesc(attributeName,
                                             expr.getType(),
                                             flags,
                                             CompressorType::NONE));
        }

        if(schemas[0].getEmptyBitmapAttribute()) {
            outAttrs.addEmptyTagAttribute();
        }
        return ArrayDesc(schemas[0].getName(),
                         outAttrs,
                         schemas[0].getDimensions(),
                         schemas[0].getDistribution(),
                         schemas[0].getResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTileApply, "tile_apply");

}  // namespace scidb
