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
 * @file PhysicalApply.cpp
 */

#include <query/ops/apply/ApplyArray.h>
#include <query/PhysicalOperator.h>

#include <log4cxx/logger.h>

namespace scidb {

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.apply"));

using namespace std;

class PhysicalApply: public  PhysicalOperator
{
public:
    PhysicalApply(const string& logicalName,
                  const string& physicalName,
                  const Parameters& parameters,
                  const ArrayDesc& schema)
    :   PhysicalOperator(logicalName, physicalName, parameters, schema),
        _attrExprMap(makeAttrExprMap())
    { }

    std::vector<uint8_t> isReplicatedInputOk(size_t numChildren) const override
    {
        // SDB-6479 determine if expression is identical across instances
        // some functions (e.g. random(), instanceid()) would make the data inconsistent
        // across instances which is not permitted for replicated.  However the majority
        // of cases are consistent, and would allow replicated input
        bool isReplicatedSafe = true;
        for(auto it = _attrExprMap.begin(); it != _attrExprMap.end(); ++it) {
            isReplicatedSafe = isReplicatedSafe && it->second->isInstIdentical();
            // Note: we do not need to consider whether there is only one instance.
            // When not inst-identical the apply for a given chunk is run on
            // only one instance.
        }

        LOG4CXX_TRACE(logger, "PhysicalApply::isReplicatedInputOk() returning " << isReplicatedSafe);
        // SDB-1929: Don't return empty vector!  (Formerly relied on bad vector<bool> behavior.)
        return std::vector<uint8_t>(numChildren ? numChildren : 1, isReplicatedSafe);
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const override
    {
        return inputBoundaries[0];
    }

private: // TODO: move to class end as a separate commit
    map<string, std::shared_ptr<Expression>> makeAttrExprMap()
    {
        // We know there are no duplicate attribute names (this was
        // checked in inferSchema()).  Collect the (name, expr) pairs
        // from the parameter list.

        map<string, std::shared_ptr<Expression> > attrExprMap;

        // Lambda to grab an attribute name from a PARAM_ATTRIBUTE_REF.
        auto getAttrName = [](Parameter& p) {
            auto pRef = dynamic_cast<OperatorParamReference*>(p.get());
            SCIDB_ASSERT(pRef);
            return pRef->getObjectName();
        };

        // Lambda to grab a physical expression and update the attrExprMap.
        auto makeAttrExpr = [&attrExprMap](Parameter& p, string const& name) {
            auto pExp = dynamic_cast<OperatorParamPhysicalExpression*>(p.get());
            attrExprMap.insert(make_pair(name, pExp->getExpression()));
        };

        string attrName;
        for (Parameter& p: _parameters) {
            auto pType = p->getParamType();
            if (pType == PARAM_ATTRIBUTE_REF) {
                attrName = getAttrName(p);
            }
            else if (pType == PARAM_PHYSICAL_EXPRESSION) {
                SCIDB_ASSERT(!attrName.empty());
                makeAttrExpr(p, attrName);
            }
            else if (pType == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(p.get());
                SCIDB_ASSERT(group);
                Parameters& gParams = group->getParameters();
                SCIDB_ASSERT(gParams.size() == 2);
                attrName = getAttrName(gParams[0]);
                makeAttrExpr(gParams[1], attrName);
            }
            else {
                SCIDB_UNREACHABLE();
            }
        }
        return attrExprMap;
    }
    map<string, std::shared_ptr<Expression> > _attrExprMap;

public:
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

        auto inputArray = ensureRandomAccess(inputArrays[0], query);
        checkOrUpdateIntervals(_schema, inputArray);

        // Now populate an expression vector, matching the non-empty
        // expressions with their relative positions in the output
        // schema.

        vector<std::shared_ptr<Expression> > expressions(0);
        size_t nAttrs = _schema.getAttributes().size();
        for (size_t i = 0; i < nAttrs; ++i) {
            AttributeDesc const& aDesc = _schema.getAttributes().findattr(i);
            auto pos = _attrExprMap.find(aDesc.getName());
            if (pos == _attrExprMap.end()) {
                expressions.push_back(std::shared_ptr<Expression>());
            } else {
                expressions.push_back(pos->second);
            }
        }
        assert(expressions.size() == _schema.getAttributes().size());

        return std::shared_ptr<Array>(new ApplyArray(_schema, inputArray, expressions, query, _tileMode));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalApply, "apply", "physicalApply")

}  // namespace scidb
