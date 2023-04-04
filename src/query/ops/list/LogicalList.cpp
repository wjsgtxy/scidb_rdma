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

#include "ListArrayBuilders.h"

#include <query/Expression.h>
#include <query/LogicalOperator.h>
#include <query/OperatorLibrary.h>
#include <query/Parser.h>
#include <query/UserQueryException.h>
#include <rbac/NamespaceDesc.h>
#include <rbac/NamespacesCommunicator.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>
#include <system/SystemCatalog.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace std;
using NsComm = scidb::namespaces::Communicator;

/**
 * @brief The operator: list().
 *
 * @par Synopsis:
 *   list( what='arrays', showSystem=false )
 *
 * @par Summary:
 *   Produces a result array containing information per its argument
 *   The available things to list include:
 *   - aggregates: show all the aggregate operators.
 *   - arrays: show all the arrays.
 *   - chunk descriptors: show all the chunk descriptors.
 *   - chunk map: show the chunk map.
 *   - functions: show all the functions.
 *   - instances: show all SciDB instances.
 *   - libraries: show all the libraries that are loaded in the current SciDB session.
 *   - operators: show all the operators and the libraries in which they reside.
 *   - types: show all the datatypes that SciDB supports.
 *   - queries: show all the active queries.
 *   - datastores: show information about each datastore
 *   - counters: (undocumented) dump info from performance counters
 *
 * @par Input:
 *   - what: what to list.
 *   - showSystem: whether to show systems information.
 *
 * @par Output array:
 *        <
 *   <br>   The list of attributes depends on the input.
 *   <br> >
 *   <br> [
 *   <br>   No: sequence number
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
class LogicalList : public LogicalOperator
{
public:
    LogicalList(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _usage =
            "list(['what' [, flag]])\n"
            "Where the literal string 'what' is one of:\n"
            "    'aggregates', 'arrays', 'functions', 'instances',\n"
            "    'libraries', 'macros', 'namespaces', 'operators', 'queries',\n"
            "    'roles', or 'types'.\n"
            "The meaning of the flag parameter depends on 'what'.\n"
            "If missing, the default 'what' is 'arrays'.\n";

        // Intentionally undocumented, at least for now:
        //    'chunk map', 'disk indexes', 'buffer stats', 'datastores',
        //    'counters'.

    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::QMARK, {
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))
                 })
              })
            },

            // keyword parameters
            {KW_NS, RE(PP(PLACEHOLDER_NS_NAME))},
            {KW_NS_ABBREV, RE(PP(PLACEHOLDER_NS_NAME))},
            {KW_BACKUP, RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL))},
        };
        return &argSpec;
    }

    string getMainParameter() const
    {
        if (_parameters.empty())
        {
            return "arrays";
        }

        return evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                TID_STRING).getString();
    }

    bool getShowSysParameter() const
    {
        if (_parameters.size() < 2)
        {
            return false;
        }

        return evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                TID_BOOL).getBool();
    }

    NamespaceDesc getNsParameter(const std::shared_ptr<Query>& query) const
    {
        Parameter p1 = findKeyword(KW_NS);
        Parameter p2 = findKeyword(KW_NS_ABBREV);

        if (p1 && p2) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_MUTUALLY_EXCLUSIVE_PARAMS,
                                       p1->getParsingContext())
                << getLogicalName() << KW_NS << KW_NS_ABBREV;
        }
        if (p1) {
            auto const& ref = dynamic_cast<OperatorParamNamespaceReference const&>(*p1);
            return ref.getNamespace();
        }
        if (p2) {
            auto const& ref = dynamic_cast<OperatorParamNamespaceReference const&>(*p2);
            return ref.getNamespace();
        }
        return query->getSession()->getNamespace();
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        string const what = getMainParameter();
        if (what == "roles" || what == "users") {
            // Operator privilege required to list roles or users.
            query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
        }
        else if (what == "arrays") {
            // Operator privilege required to list arrays across all namespaces.
            NamespaceDesc nsDesc(getNsParameter(query));
            if (nsDesc.getId() == rbac::ALL_NS_ID) {
                query->getRights()->upsert(rbac::ET_DB, "", rbac::P_DB_OPS);
            } else {
                query->getRights()->upsert(rbac::ET_NAMESPACE,
                                           nsDesc.getName(), rbac::P_NS_LIST);
            }
        }
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc>, std::shared_ptr<Query> query)
    {
        Attributes attributes;
        attributes.push_back(AttributeDesc("name",TID_STRING,0,CompressorType::NONE));

        string const what = getMainParameter();
        size_t       size = 0;
        bool         showSys = getShowSysParameter();

        if (what == "aggregates") {
            ListAggregatesArrayBuilder builder(
                AggregateLibrary::getInstance()->getNumAggregates(), showSys);
            return checkPS(builder.getSchema(query));
        } else if (what == "arrays") {
            ListArraysArrayBuilder builder;
            return checkPS(builder.getSchema(query));
        } else if (what == "operators") {
            vector<string> names;
            OperatorLibrary::getInstance()->getLogicalNames(names, showSys);
            size = names.size();
            attributes.push_back(AttributeDesc("library",TID_STRING,0,CompressorType::NONE));
            if (showSys) {
                attributes.push_back(AttributeDesc("internal",TID_BOOL,0,CompressorType::NONE));
                attributes.push_back(AttributeDesc("ddl",TID_BOOL,0,CompressorType::NONE));
                attributes.push_back(AttributeDesc("updater",TID_BOOL,0,CompressorType::NONE));
            }
        } else if (what == "types") {
            size =  TypeLibrary::typesCount();
            attributes.push_back(AttributeDesc("library",TID_STRING,0,CompressorType::NONE));
        } else if (what == "functions") {
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin(); i != funcs.end(); ++i)
            {
                size += i->second.size();
            }
            size += 3; // for hardcoded iif, missing_reason and sizeof
            attributes.push_back(AttributeDesc("profile",      TID_STRING, 0,CompressorType::NONE));
            attributes.push_back(AttributeDesc("deterministic",TID_BOOL,   0,CompressorType::NONE));
            attributes.push_back(AttributeDesc("library",      TID_STRING, 0,CompressorType::NONE));
        } else if (what == "macros") {
            return checkPS(logicalListMacros(query, showSys));
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder;
            return checkPS(builder.getSchema(query));
        } else if (what == "instances") {
            std::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
            size = queryLiveness->getNumInstances();
            attributes.push_back(AttributeDesc("port",         TID_UINT16,0,CompressorType::NONE));
            attributes.push_back(AttributeDesc("instance_id",  TID_UINT64,0,CompressorType::NONE));
            attributes.push_back(AttributeDesc("online_since", TID_STRING,0,CompressorType::NONE));
            attributes.push_back(AttributeDesc("instance_path",TID_STRING,0,CompressorType::NONE));
        } else if (what == "chunk map") {
            return checkPS(ListChunkMapArrayBuilder().getSchema(query));
        } else if (what == "disk indexes") {
            return checkPS(ListDiskIndexArrayBuilder().getSchema(query));
        } else if (what == "buffer stats") {
            return checkPS(ListBufferStatsArrayBuilder().getSchema(query));
        } else if (what == "libraries") {
            return checkPS(ListLibrariesArrayBuilder().getSchema(query));
        } else if (what == "datastores") {
            return checkPS(ListDataStoresArrayBuilder().getSchema(query));
        } else if (what == "counters") {
            return checkPS(ListCounterArrayBuilder().getSchema(query));
        } else if (what == "users") {
            // There is already a name field.
            std::vector<UserDesc> users;
            rbac::listUsers(users);
            size=users.size();
            attributes.push_back(AttributeDesc("id",  TID_UINT64,0,CompressorType::NONE));
        } else if (what == "roles") {
            // There is already a name field.
            std::vector<RoleDesc> roles;
            scidb::namespaces::Communicator::getRoles(roles);
            size=roles.size();
            attributes.push_back(AttributeDesc("id",  TID_UINT64,0,CompressorType::NONE));
        } else if (what == "namespaces") {
            // There is already a name field.
            std::vector<NamespaceDesc> namespaces;
            SystemCatalog::getInstance()->getNamespaces(namespaces);

            // Get the total # of namespaces.
            size = namespaces.size();

            // Now subtract one for each namespace which we do not have permission to list.
            SCIDB_ASSERT(query->getSession());
            for (auto const& nsDesc : namespaces) {
                rbac::RightsMap rights;
                rights.upsert(rbac::ET_NAMESPACE, nsDesc.getName(), rbac::P_NS_LIST);
                try {
                    NsComm::checkAccess(query->getSession().get(), &rights);
                }
                catch (Exception&) {
                    --size;
                }
            }
        } else {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_LIST_ERROR1,
                                       _parameters[0]->getParsingContext());
        }

        size_t const chunkInterval = size>0 ? size : 1;

        SCIDB_ASSERT(not isUninitialized(getSynthesizedDistType()));

        return ArrayDesc(what, attributes,
                         vector<DimensionDesc>(1, DimensionDesc("No", 0, 0, chunkInterval-1,
                                                                chunkInterval-1, chunkInterval, 0)),
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }
private:
    ArrayDesc checkPS(const ArrayDesc& arrayDesc) const
    {
        SCIDB_ASSERT(not isUninitialized(arrayDesc.getDistribution()->getDistType()));
        return arrayDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalList, "list")

/****************************************************************************/
}
/****************************************************************************/
