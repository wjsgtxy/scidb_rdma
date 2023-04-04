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
 * @file LogicalProject.cpp
 *
 * @author knizhnik@garret.ru
 */

#include "Projection.h"

#include <query/LogicalOperator.h>

#include <query/Expression.h>
#include <query/TypeSystem.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: project().
 *
 * @par Synopsis:
 *   project( srcArray {, selectedAttr}+ )
 *
 * @par Summary:
 *   Produces a result array that includes some attributes of the source array.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - a list of at least one selectedAttrs from the source array.
 *
 * @par Output array:
 *        <
 *   <br>   selectedAttrs: the selected attributes
 *   <br> >
 *   <br> [
 *   <br>   srcDims
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
class LogicalProject : public LogicalOperator
{
public:
    LogicalProject(const std::string& logicalName, const std::string& alias);
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query) override;
    std::string getInspectable() const override { return _keep.toString(); }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
                 RE(RE::LIST, {
                    RE(PP(PLACEHOLDER_INPUT)),
                    RE(RE::PLUS, {
                       RE(PP(PLACEHOLDER_ATTRIBUTE_NAME))
                    })
                 })
            },
            {"inverse", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
        };
        return &argSpec;
    }

private:
    Projection _keep;       // Attribute indices to keep, in desired order
};

LogicalProject::LogicalProject(const string& logicalName, const string& alias)
    : LogicalOperator(logicalName, alias)
{
    _properties.tile = true;
}

ArrayDesc LogicalProject::inferSchema(vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
{
    assert(schemas.size() == 1);

    // Should we keep the named attributes (normal), or discard all
    // *but* the named attributes (inverse)?
    bool inverse = false;
    Parameter p = findKeyword("inverse");
    if (p) {
        auto lexp = dynamic_cast<OperatorParamLogicalExpression*>(p.get());
        assert(lexp); // paranoid, keyword only allows PARAM_CONSTANT(TID_BOOL)
        inverse = evaluate(lexp->getExpression(), TID_BOOL).getBool();
    }

    _keep.clear();        // Because of multiple calls to inferSchema().
    set<AttributeID> discard;
    for (auto const& p : _parameters) {
        assert(p->getParamType() == PARAM_ATTRIBUTE_REF);
        auto const& ref = dynamic_cast<OperatorParamReference&>(*p);
        if (ref.getObjectName() == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                 SCIDB_LE_EXPLICIT_EMPTY_FLAG_NOT_ALLOWED);
        }
        AttributeID aid = static_cast<AttributeID>(ref.getObjectNo());
        if (inverse) {
            discard.insert(aid);
        } else {
            _keep.push_back(aid);
        }
    }

    Attributes newAttributes;
    const Attributes& oldAttributes = schemas[0].getAttributes(/*excludeEbm:*/true);

    if (inverse) {
        if (discard.size() == oldAttributes.size()) {
            // Wiseguy tried project(A, every, single, attribute, inverse:1)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ILLEGAL_OPERATION)
                << "Cannot project() away all attributes";
        }

        for (const auto& attr : oldAttributes) {
            if (discard.find(attr.getId()) != discard.end()) {
                continue;
            }
            _keep.push_back(attr.getId());

            newAttributes.push_back(AttributeDesc(attr.getName(), attr.getType(),
                                                  attr.getFlags(), attr.getDefaultCompressionMethod(),
                                                  attr.getAliases(), &attr.getDefaultValue(),
                                                  attr.getDefaultValueExpr()));
        }
    } else {
        for (AttributeID oldAid : _keep) {
            const AttributeDesc &attr = oldAttributes.findattr(oldAid);
            newAttributes.push_back(AttributeDesc(attr.getName(), attr.getType(),
                                                  attr.getFlags(), attr.getDefaultCompressionMethod(),
                                                  attr.getAliases(), &attr.getDefaultValue(),
                                                  attr.getDefaultValueExpr()));
        }
    }

    AttributeDesc const* indicator = schemas[0].getEmptyBitmapAttribute();
    if (indicator != NULL) {
        newAttributes.push_back(AttributeDesc(indicator->getName(), indicator->getType(),
                                              indicator->getFlags(),
                                              indicator->getDefaultCompressionMethod(),
                                              indicator->getAliases()));
        _keep.push_back(indicator->getId());
    }

    return ArrayDesc(schemas[0].getName(), newAttributes, schemas[0].getDimensions(),
                     schemas[0].getDistribution(),
                     schemas[0].getResidency());
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalProject, "project")

} //namespace
