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
 * @file LogicalRemoveVersions.cpp
 * @date Jun 11, 2014
 * @author sfridella, mjl
 */

#include <array/ArrayName.h>
#include <array/VersionDesc.h>
#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: remove_versions().
 *
 * @par Synopsis:
 *   remove_versions( targetArray, oldestVersionToSave )
 *   remove_versions( targetArray, keep: numberOfRecentVersionsToSave )
 *   remove_versions( targetArray )
 *
 * @par Summary:
 *   The first form removes all versions of targetArray that are older than
 *   oldestVersionToSave.  The second form keeps numberOfRecentVersionsToSave
 *   versions, removing all earlier versions.  The third form is the same as
 *   "keep: 1".
 *
 * @par Input:
 *   - targetArray - the array which is targeted.
 *   - oldestVersionToSave - this and all later versions will be kept, all
 *                           earlier versions will be removed.
 *   - keep:n - keep latest n versions
 *
 * @par Output array:
 *   NULL
 *
 * @par Examples:
 *   remove_versions(A, 5);       -- Remove versions 1, 2, 3, and 4
 *   remove_versions(A, keep:5);  -- Remove all but *last* five versions
 *   remove_versions(A, keep:1);  -- Remove all but the latest version
 *   remove_versions(A);          -- Remove all but the latest version
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalRemoveVersions : public LogicalOperator
{
public:
    LogicalRemoveVersions(const string& logicalName, const string& alias);
    ArrayDesc inferSchema(vector<ArrayDesc> schemas, std::shared_ptr<Query> query) override;
    void inferAccess(const std::shared_ptr<Query>& query) override;
    static PlistSpec const* makePlistSpec();


private:
    void _checkAndUpdateParameters(std::shared_ptr<Query> const& qry);
    VersionID _computeTargetFromKeepCount(int64_t keep);
    Parameter _makeConstant(uint64_t x, std::shared_ptr<ParsingContext> const& ctx);

    bool _checked { false };
    VersionID _targetVersion { NO_VERSION };
    string _nsName;
    string _arrayName;
};


PlistSpec const* LogicalRemoveVersions::makePlistSpec()
{
    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_ARRAY_NAME)),
             RE(RE::QMARK, {
                RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64))
             })
          })
        },
        { "keep", RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
    };
    return &argSpec;
}

LogicalRemoveVersions::LogicalRemoveVersions(const string& logicalName, const string& alias)
    : LogicalOperator(logicalName, alias)
{
    _properties.exclusive = true;
    _properties.ddl = true;

}


ArrayDesc LogicalRemoveVersions::inferSchema(vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
{
    assert(schemas.empty());
    _checkAndUpdateParameters(query);
    return ddlArrayDesc(query);
}


void LogicalRemoveVersions::inferAccess(const std::shared_ptr<Query>& query)
{
    LogicalOperator::inferAccess(query);
    _checkAndUpdateParameters(query);

    std::shared_ptr<LockDesc>  lock(
        new LockDesc(
            _nsName,
            _arrayName,
            query->getQueryID(),
            Cluster::getInstance()->getLocalInstanceId(),
            LockDesc::COORD,
            LockDesc::RM));
    lock->setArrayVersion(_targetVersion);
    std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
    assert(resLock);
    assert(resLock->getLockMode() >= LockDesc::RM);

    query->getRights()->upsert(rbac::ET_NAMESPACE, _nsName, rbac::P_NS_DELETE);
}


/**
 * Check for valid parameters, possibly updating them.
 *
 * @details In addition to normal parameter validation, we also reconcile the
 * two forms of the operator so that PhysicalRemoveVersions doesn't have to
 * care either way.  We start out with either of these forms:
 *
 * @code
 * remove_versions(A, x)
 * remove_versions(A)  -- same as "keep: 1"
 * remove_versions(A, keep: y)
 * @endcode
 *
 * If the latter, we rewrite the _parameters[] vector to
 *
 * @code
 * remove_versions(A, f(y), keep: y)
 * @endcode
 *
 * where @c f(y) means "figure out the target VersionID that will let us keep
 * the latest @c y versions".  Now the physical operator doesn't have to look
 * at the "keep:" keyword at all, it works the same for either call form.
 *
 * @note Because the horrific language frontend requires repeated reparsing
 *       and re-creation of the logical plan tree, we can't just compute this
 *       stuff once and store it in member variables.  If we do, the object
 *       can be thrown away and the query reparsed, and the member data
 *       (including the _parameters[] vector modified here) is no longer
 *       available.  That's why this method is called by both inferAccess()
 *       and inferSchema(): to make sure that the changes stick so that the
 *       physical plan will receive our updated parameters.
 */
void LogicalRemoveVersions::_checkAndUpdateParameters(std::shared_ptr<Query> const& query)
{
    // Don't repeat this work unless we absolutely must.  (Multiple
    // inferSchema() calls are possible on the same logical plan tree.)
    if (_checked) {
        return;
    }
    _checked = true;

    // Resolve namespace and array name.
    assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
    auto opr = dynamic_cast<OperatorParamReference*>(_parameters[0].get());
    if (isNameVersioned(opr->getObjectName())) {
        // Translator doesn't allow versioned names for PARAM_IN_ARRAY_NAME(),
        // but a little paranoia is sometimes good.
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                             SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
            << getLogicalName() << "Versioned array name not allowed";
    }
    query->getNamespaceArrayNames(opr->getObjectName(), _nsName, _arrayName);

    Parameter kw = findKeyword("keep");
    if (kw) {
        if (_parameters.size() > 1) {
            // Too may arguments, can't have both "oldestVersionToSave" and
            // "keep: thisMany".
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                 SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3)
                << getLogicalName()
                << "either a target version or a keep count but not both";
        }

        // Compute new _targetVersion based on the "keep" count, and pretend it
        // was in our parameter list all along.  Then PhysicalRemoveVersions
        // need not change.

        auto lexp = dynamic_cast<OperatorParamLogicalExpression*>(kw.get());
        int64_t keep = evaluate(lexp->getExpression(), TID_INT64).getInt64();
        if (keep < 1) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                 SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                << getLogicalName() << keep;
        }
        _targetVersion = _computeTargetFromKeepCount(keep);
        assert(_parameters.size() == 1);
        _parameters.push_back(_makeConstant(_targetVersion,
                                            kw->getParsingContext()));
    }
    else if (_parameters.size() == 1) {
        // remove_versions(A) is same as remove_versions(A, keep:1)
        _targetVersion = _computeTargetFromKeepCount(1);
        _parameters.push_back(_makeConstant(_targetVersion,
                                            _parameters[0]->getParsingContext()));
    }
    else {
        auto lexp = dynamic_cast<OperatorParamLogicalExpression*>(_parameters[1].get());
        _targetVersion = evaluate(lexp->getExpression(), TID_UINT64).getUint64();
    }

    // Although parameter is uint64_t, valid range is 1..(2**63)-1
    if (_targetVersion < 1 || _targetVersion > SystemCatalog::MAX_VERSIONID) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                               SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST)
            << _targetVersion;
    }
}


//  Given count of versions to keep, find version number of oldest to keep.
VersionID LogicalRemoveVersions::_computeTargetFromKeepCount(int64_t keep)
{
    assert(keep > 0);
    assert(!_nsName.empty());
    assert(!_arrayName.empty());

    SystemCatalog& sysCat = *SystemCatalog::getInstance();
    ArrayID uaid = sysCat.getArrayId(_nsName, _arrayName);
    assert(uaid != INVALID_ARRAY_ID); // because PARAM_IN_ARRAY_NAME() (not OUT)
    vector<VersionDesc> versions = sysCat.getArrayVersions(uaid);
    if (safe_static_cast<size_t>(keep) < versions.size()) {
        return versions[versions.size() - keep].getVersionID();
    } else {
        // Keeping them all, so oldest to keep is (or may as well be) version 1.
        return 1;
    }
}


Parameter LogicalRemoveVersions::_makeConstant(uint64_t x, std::shared_ptr<ParsingContext> const& ctx)
{
    Type const& type = TypeLibrary::getType(TID_UINT64);
    Value v(type);
    v.setUint64(x);
    auto lexp = std::make_shared<Constant>(ctx, v, TID_UINT64);
    return std::make_shared<OperatorParamLogicalExpression>(ctx, lexp, type, /*isConstant:*/ true);
}


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRemoveVersions, "remove_versions")

}  // namespace scidb
