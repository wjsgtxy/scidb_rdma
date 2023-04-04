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
#include <query/LogicalQueryPlan.h>
#include <query/Query.h>
#include <rbac/Rights.h>

namespace scidb {

/**
 * @brief The operator: mquery().
 *
 * @par Synopsis:
 *   mquery( subquery1 [ , subquery2 ]* )
 *
 * @par Summary:
 *   Operator mquery() will take one or more AFL statements and
 *   execute them sequentially, starting with the first, as one query.
 *     mquery(afl_statement_1,
 *                    afl_statement_2,
 *                    ...,
 *                    afl_statement_n);
 *   Only combinations of insert() and delete() are supported as top-level
 *   operators in mquery() for its first incarnation. insert() and
 *   delete() may appear in the same mquery() statement.
 *   The latest version of an array produced by some afl_statement_j
 *   (where j is one of n total AFL statements in a given mquery())
 *   is visible for each successive afl_statement_(j+1) (put another way,
 *   later AFL statements in a mquery() see the latest versions of
 *   arrays updated by earlier AFL statements of the same mquery()
 *   query).
 *   mquery() provides READ COMMITTED isolation with respect to the arrays
 *   that it creates, which in the case of an mquery() failure could lead
 *   to phantom reads.
 */
class LogicalMultiquery : public LogicalOperator
{
public:
    LogicalMultiquery(const std::string& logicalName,
                      const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        // This operator is not DDL, yet it's marked as such to avoid
        // optimization and SG insertion of subqueries until they're
        // spawned.
        _properties.ddl = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::PLUS, { RE(PP(PLACEHOLDER_INPUT)) })
            }
        };
        return &argSpec;
    }

private:
    void _validate(const std::shared_ptr<LogicalOperator>& childOp) const
    {
        // No DDL permitted in mquery().  While the translator prevents
        // DDL from nesting, that may not always be the case; enforce no DDL
        // here as an operator-specific requirement.
        if (childOp->getProperties().ddl) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATOR_FAILED)
                << getOperatorName()
                << "DDL operators are not permitted ";
        }

        // Top-level query statements in mquery() may only be insert() or delete().
        if (childOp->getOperatorName() != "insert" &&
            childOp->getOperatorName() != "delete") {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATOR_FAILED)
                << getOperatorName()
                << "Only insert() or delete() permitted as top-level operators ";
        }

        // _fetch keyword ignored in insert() when invoked from mquery().
        const auto& childOpParams = childOp->getKeywordParameters();
        if (childOpParams.find("_fetch") != childOpParams.end()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OPERATOR_FAILED)
                << getOperatorName()
                << "_fetch keyword in nested insert() or delete() not supported ";
        }
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        // Validate all of the operator input before we try to take any locks.
        // We don't (yet) support nesting of mquery, so it must be at
        // the top level.
        SCIDB_ASSERT(query);
        auto queryLogicalPlan = query->logicalPlan;
        SCIDB_ASSERT(queryLogicalPlan);
        auto mquery_node = queryLogicalPlan->getRoot();

        for (const auto& child : mquery_node->getChildren()) {
            // All children should feel loved and validated.
            SCIDB_ASSERT(child);
            SCIDB_ASSERT(child->getLogicalOperator());
            _validate(child->getLogicalOperator());
        }
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(query);

        // Leverage ddlArrayDesc to create an array descriptor suitable to
        // indicate that this operator does not return an array.
        return ddlArrayDesc(query);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalMultiquery, "mquery")

}  // namespace scidb
