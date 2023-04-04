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
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include <query/LogicalOperator.h>

#include <array/ArrayName.h>     // for makeUnversionedName
#include <query/OperatorLibrary.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <util/Indent.h>

#include <log4cxx/logger.h>

using namespace std;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

void LogicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lOperator] "<<_logicalName;
    out << " ddl "<< _properties.ddl << "\n";

    Parameters const& plist = getParameters();
    for (Parameter const& p : plist) {
        p->toString(out, indent + 1);
    }

    auto const& kwParams = getKeywordParameters();
    Indent more(indent + 1);
    for (auto const& kwPair : kwParams) {
        cout << more('>', false) << kwPair.first << ": ";
        kwPair.second->toString(out, indent + 1);
    }

    for (auto const& ph : _paramPlaceholders) {
        ph->toString(out, indent + 1);
    }

    out << prefix('>', false);
    out << "schema: " << _schema << "\n";
}

ArrayDesc LogicalOperator::ddlArrayDesc(const std::shared_ptr<Query>& query) const
{
    ArrayDesc arrDesc;
    arrDesc.setDistribution(createDistribution(getSynthesizedDistType()));
    arrDesc.setResidency(query->getDefaultArrayResidency());
    return arrDesc;
}

ostream& operator<< (ostream& os, PlistRegex const& re)
{
    os << re.asRegex();
    return os;
}

ostream& operator<< (ostream& os, PlistSpec const& spec)
{
    os << '{';
    string sep;
    for (auto const& x : spec) {
        os << sep << '"' << x.first << "\": \"" << x.second << '"';
        if (sep.empty()) {
            sep = ", ";
        }
    }
    os << '}';
    return os;
}

namespace {
    // Internal machinery for visitParameters().

    void visitOnePlist(PlistVisitor func, Parameters& plist, string const& kw)
    {
        if (plist.empty()) {
            return;
        }

        using PlistRange =
            std::pair<Parameters::iterator, Parameters::iterator>;

        PlistWhere where(1, 0);   // Tracks current position in (sub)list
        vector<PlistRange> stack; // Tracks depth-first traversal
        stack.push_back(PlistRange(plist.begin(), plist.end()));

        while (!stack.empty()) {
            auto& cursor = stack.back().first;
            if (cursor == stack.back().second) {
                stack.pop_back();
                where.pop_back();
                if (!where.empty()) {
                    // Delayed increment after processing sublist.
                    ++where.back();
                }
                continue;
            }

            Parameter p = *cursor++;
            func(p, where, kw);
            // ...but delay ++where.back().

            if (p->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(p.get());
                Parameters& g = group->getParameters();
                stack.push_back(PlistRange(g.begin(), g.end()));

                // Pushed a sublist, so don't increment *current*
                // where.back() until we pop.  Instead, push a new
                // index for first item in sublist.
                where.push_back(0);
            } else {
                // Parameter wasn't nested, OK to increment 'where'
                // position.
                ++where.back();
            }
        }
    }
} // anon namespace

void visitParameters(PlistVisitor& func,
                     Parameters& plist,
                     KeywordParameters& kwargs)
{
    visitOnePlist(func, plist, "");

    // Keyword parameters too.  Right now we don't allow them inside
    // of nested parameter lists, but we *do* allow keyword parameter
    // *values* to be nested lists.

    PlistWhere const nowhere;
    for (auto& kwPair : kwargs) {
        Parameter p = kwPair.second;
        func(p, nowhere, kwPair.first);

        if (p->getParamType() == PARAM_NESTED) {
            auto group = dynamic_cast<OperatorParamNested*>(p.get());
            visitOnePlist(func, group->getParameters(), kwPair.first);
        }
    }
}


void BaseLogicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addLogicalOperatorFactory(this);
}

LogicalOperator::LogicalOperator(const std::string& logicalName,
                                 const std::string& aliasName)
    : _logicalName(logicalName)
    , _hasSchema(false)
    , _aliasName(aliasName)
    , _inserter(Inserter::USER)
{}

const std::string& LogicalOperator::getLogicalName() const { return _logicalName; }

const std::string& LogicalOperator::getOperatorName() const { return getLogicalName(); }

DistType LogicalOperator::getSynthesizedDistType() const
{
    // temporary, see notice in header
    DistType result = defaultDistType();
    LOG4CXX_TRACE(logger, "LogicalOperator::getSynthesizedDistType: operator " << getOperatorName()
                           << " returning ps=" << result);
    return result;
}

void LogicalOperator::setParameters(const Parameters& parameters)
{
    _parameters = parameters;
}

void LogicalOperator::addParameter(const Parameter& parameter)
{
    _parameters.push_back(parameter);
}

const KeywordParameters& LogicalOperator::getKeywordParameters() const
{
    return _kwParameters;
}

void LogicalOperator::setSchema(const ArrayDesc& newSchema)
{
    if (hasSchema()) {
        LOG4CXX_DEBUG(logger, "LogicalOperator::setSchema() updating existing _schema: " << getSchema()
                              << " to: "  << newSchema);
    }

    _schema = newSchema;
    _hasSchema = true;

    // historical behavior: a non-empty _aliasName on this overrides the name from newSchema
    if (_aliasName.size() != 0) {
        _schema.setName(_aliasName);
    }
}

bool LogicalOperator::hasSchema() const
{
    return _hasSchema;
}

const ArrayDesc& LogicalOperator::getSchema() const
{
    SCIDB_ASSERT(hasSchema());
    return _schema;
}

void LogicalOperator::setAliasName(const std::string &alias)
{
    // TODO: consider changing callers so we can assert !alias.empty()
    // TODO: consider changing callers so we can assert _aliasName.empty()

    if(!_aliasName.empty() && _aliasName != alias) {
        LOG4CXX_WARN(logger, "LogicalOperator::setAliasName changing from" << _aliasName << " to " << alias);
    }

    _aliasName = alias;
}

void LogicalOperator::inferAccess(const std::shared_ptr<Query>& query)
{
    PlistVisitor v =
        [this, &query](Parameter& p, PlistWhere const&, string const&)
        {
            _requestLeastAccess(p, query);
        };
    visitParameters(v);
}

void LogicalOperator::_requestLeastAccess(Parameter const& param,
                                          const shared_ptr<Query>& query)
{
    string arrayNameOrig;
    bool isSchema = false;
    if (param->getParamType() == PARAM_ARRAY_REF) {
        arrayNameOrig = ((std::shared_ptr<OperatorParamReference>&)param)->getObjectName();
    } else if (param->getParamType() == PARAM_SCHEMA) {
        arrayNameOrig = ((std::shared_ptr<OperatorParamSchema>&)param)->getSchema().getQualifiedArrayName();
        isSchema = true;
    }
    if (arrayNameOrig.empty()) {
        return;
    }

    std::string arrayName;
    std::string namespaceName;
    query->getNamespaceArrayNames(arrayNameOrig, namespaceName, arrayName);
    const string baseName = makeUnversionedName(arrayName);

    std::shared_ptr<LockDesc> lock(
            std::make_shared<LockDesc>(
            namespaceName,
            baseName,
            query->getQueryID(),
            Cluster::getInstance()->getLocalInstanceId(),
            LockDesc::COORD,
            LockDesc::RD));
    query->requestLock(lock);

    // Only grant automatic namespace read rights if this array
    // name is used as a schema parameter.  Here's why:
    //
    // Unlike catalog locks, which are hierarchical (RD < WR <
    // XCL), there is no such ordering for namespace rights.  An
    // operator having namespace write access (store, insert) does
    // @em not automatically imply namespace read access.  Derived
    // classes must explicitly ask for (only) the namespace rights
    // they need.
    //
    // Often, operators call this base class method before
    // upgrading the catalog read lock(s) acquired above, so they
    // don't -really- need read access to the namespace.  That's
    // inaccurate and wasteful, oh well.  But if an array is being
    // used just for its schema, then definitely you need read
    // access to -that- namespace.  So here we request it.
    //
    if (isSchema) {
        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName,
                                   rbac::P_NS_READ);
    }
}

bool LogicalOperator::hasRightsForAllNamespaces(const shared_ptr<Query>& query)
{
    // This check is not foolproof: the current operator may have
    // failed to specify rights for namespace "ns1", but another
    // operator in the tree already did so, masking this operator's
    // failure.  Oh well, we do the best we can.

    int missing = 0;

    PlistVisitor countMissing =
        [this, &missing, &query](Parameter& p, PlistWhere const&, string const&)
        {
            if (!_hasNamespaceRights(p, query)) {
                ++missing;
            }
        };

    visitParameters(countMissing);
    return missing == 0;
}

bool LogicalOperator::_hasNamespaceRights(Parameter const& param,
                                          shared_ptr<Query> const& query)
{
    string arrayNameOrig;
    if (param->getParamType() == PARAM_ARRAY_REF) {
        arrayNameOrig = ((std::shared_ptr<OperatorParamReference>&)param)->getObjectName();
    } else if (param->getParamType() == PARAM_SCHEMA) {
        arrayNameOrig = ((std::shared_ptr<OperatorParamSchema>&)param)->getSchema().getQualifiedArrayName();
    }
    if (arrayNameOrig.empty()) {
        return true;
    }

    string ns, ary;
    query->getNamespaceArrayNames(arrayNameOrig, ns, ary);
    rbac::RightsMap const& rights = *query->getRights();
    auto pos = rights.find(rbac::EntityTag(rbac::ET_NAMESPACE, ns));
    if (pos == rights.end()) {
        LOG4CXX_ERROR(logger, "Operator " << getLogicalName()
                      << " did not acquire namespace rights for array "
                      << arrayNameOrig << " in namespace " << ns);
    } else {
        // Assert that at least one permission bit is set.
        SCIDB_ASSERT(pos->second);
    }
    return pos != rights.end();
}

void LogicalOperator::setInserter(Inserter inserter)
{
    _inserter = inserter;
}

bool LogicalOperator::isUserInserted() const
{
    return _inserter == Inserter::USER;
}

} //namespace
