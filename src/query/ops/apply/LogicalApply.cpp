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
 * LogicalApply.cpp
 *
 *  Created on: Apr 11, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>

#include <query/Expression.h>
#include <system/Exceptions.h>

namespace scidb {

using namespace std;

/**
 * @brief The operator: apply().
 *
 * @par Synopsis:
 *   apply(srcArray {, newAttr, expression}+)
 *
 * @par Summary:
 *   Produces a result array with new attributes and computes values for them.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *   - 1 or more pairs of a new attribute and the expression to compute the values for the attribute.
 *   - the (attribute, expression) pairs may be grouped in nested parameter lists.
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
 *   - apply(A, unitprice, sales/quantity) <quantity: uint64, sales: double, unitprice: double> [year, item] =
 *     <br> year, item, quantity, sales, unitprice
 *     <br> 2011,  2,      7,     31.64,   4.52
 *     <br> 2011,  3,      6,     19.98,   3.33
 *     <br> 2012,  1,      5,     41.65,   8.33
 *     <br> 2012,  2,      9,     40.68,   4.52
 *     <br> 2012,  3,      8,     26.64,   3.33
 *
 * @par Errors:
 * - SCIDB_SE_INFER_SCHEMA::SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME, if a new attribute has the same name
 *      as an existing attribute.
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalApply: public  LogicalOperator
{
public:
    LogicalApply(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        _properties.dataframe = true;
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

    bool compileParamInTileMode(PlistWhere const& where, string const&) override
    {
        assert(where.size() < 3);
        if (where.size() == 2) {
            // In a (foo, bar) nested list, compile 'bar' in tile mode if possible.
            return where[1] == 1;
        }

        assert(where.size() == 1);
        if (getParameters()[where[0]]->getParamType() == PARAM_NESTED) {
            return false;
        }

        // Backward compatibility: old-style parameter list without nesting.  In a "foo, bar,
        // baz, mumble" parameter list, compile 'bar' and 'mumble' in tile mode if possible.
        return (where[0] % 2) == 1;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);

        set<string> outAttrNames;
        Attributes outAttrs;

        for (size_t i=0; i<schemas[0].getAttributes(true).size(); i++)
        {
            AttributeDesc const& attr = schemas[0].getAttributes().findattr(i);
            outAttrNames.insert(attr.getName());
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

        // Lambda to capture an attribute name from a PARAM_ATTRIBUTE_REF.
        auto getAttrName = [&](Parameter& p) {
            auto pRef = dynamic_cast<OperatorParamReference*>(p.get());
            SCIDB_ASSERT(pRef);
            string result = pRef->getObjectName();
            if (isUserInserted() && AttributeDesc::isReservedName(result)) {
                // We don't allow the user to create new attributes with internal
                // names, but we allow the user to reference existing internal names.
                throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_RESERVED_NAME)
                    << result;
            }
            bool inserted = outAttrNames.insert(result).second;
            if (!inserted) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME)
                    << result;
            }
            return result;
        };

        // Lambda to construct the next attribute from a PARAM_LOGICAL_EXPRESSION.
        auto makeAttribute = [this, &schemas, &outAttrs](Parameter& p, string const& name) {
            auto lExp = dynamic_cast<OperatorParamLogicalExpression*>(p.get());
            SCIDB_ASSERT(lExp);
            Expression expr;
            expr.compile(lExp->getExpression(), _properties.tile, TID_VOID, schemas);
            if (_properties.tile && expr.isConstant()) {
                // "TODO: it's not good to switch off tiles if we have constant. See #1587
                // (SDB-2995?) for more details."
                _properties.tile = false;
                expr.compile(lExp->getExpression(), _properties.tile, TID_VOID, schemas);
            }
            int16_t flags = expr.isNullable() ? AttributeDesc::IS_NULLABLE : 0;
            outAttrs.push_back(AttributeDesc(name,
                                             expr.getType(),
                                             flags,
                                             CompressorType::NONE));
        };

        bool wantExpr = false;
        string attrName;
        Parameters& plist = getParameters();
        assert(!plist.empty());

        for (Parameter& p: plist) {
            auto pType = p->getParamType();
            if (pType == PARAM_ATTRIBUTE_REF) {
                SCIDB_ASSERT(!wantExpr);
                wantExpr = true;
                attrName = getAttrName(p);
            }
            else if (pType == PARAM_LOGICAL_EXPRESSION) {
                SCIDB_ASSERT(wantExpr);
                wantExpr = false;
                SCIDB_ASSERT(!attrName.empty());
                makeAttribute(p, attrName);
            }
            else if (pType == PARAM_NESTED) {
                SCIDB_ASSERT(!wantExpr);
                auto group = dynamic_cast<OperatorParamNested*>(p.get());
                Parameters& gParams = group->getParameters();
                SCIDB_ASSERT(gParams.size() == 2);
                attrName = getAttrName(gParams[0]);
                makeAttribute(gParams[1], attrName);
            }
            else {
                SCIDB_UNREACHABLE();
            }
        }
        SCIDB_ASSERT(!wantExpr);

        if (schemas[0].getEmptyBitmapAttribute()) {
            outAttrs.addEmptyTagAttribute();
        }

        return ArrayDesc(schemas[0].getName(),
                         outAttrs,
                         schemas[0].getDimensions(),
                         schemas[0].getDistribution(),
                         schemas[0].getResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalApply, "apply")


}  // namespace scidb
