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
 * @file LogicalSave.cpp
 * @author roman.simakov@gmail.com
 * @brief Save operator for saving data from external files into array
 */

#include "Save.h"

#include <query/LogicalOperator.h>

#include <query/Expression.h>
#include <query/UserQueryException.h>
#include <system/SystemCatalog.h>
#include <util/IoFormats.h>
#include <util/PathUtils.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: save().
 *
 * @par Synopsis:
 *   save( srcArray, file, instanceId = -2, format = 'store' )
 *
 * @par Summary:
 *   Saves the data in an array to a file.
 *
 * @par Input:
 *   - srcArray: the source array to save from.
 *   - file: the file to save to.
 *   - instanceId: positive number means an instance ID on which file will be saved.
 *                 -1 means to save file on every instance. -2 - on coordinator.
 *   - format: I/O format in which file will be stored
 *
 * @see iofmt::isOutputFormat
 *
 * @par Output array:
 *   the srcArray is returned
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
/**
 * Must be called as SAVE('existing_array_name', '/path/to/file/on/instance')
 */
class LogicalSave: public LogicalOperator
{
    string _expandedPath;
    string _srcInstAsString;

public:
    LogicalSave(const std::string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    { }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)),
                 RE(RE::QMARK, {
                    RE(RE::OR, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // physical instance_id
                       RE(RE::GROUP, {
                          RE(PP(PLACEHOLDER_CONSTANT, TID_INT32)), // server_id
                          RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))  // server_instance_id
                       })
                    }),
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
                    })
                 })
              })
            },

            // keywords
            { KW_INSTANCE,
                RE(RE::OR, {
                   RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)), // physical instance_id
                   RE(RE::GROUP, {
                      RE(PP(PLACEHOLDER_CONSTANT, TID_INT32)), // server_id
                      RE(PP(PLACEHOLDER_CONSTANT, TID_INT32))  // server_instance_id
                   })
                })
            },
            { KW_FORMAT, RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) }
        };
        return &argSpec;
    }

    string getInspectable() const override { return _srcInstAsString; }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        assert(_parameters.size() >= 1);

        if (_expandedPath.empty()) {
            // Expand once and cache the result (since we're called many times).  Even though we
            // don't actually use _expandedPath here in the logical op, compute it anyway to detect
            // errors early.
            string path =
                evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                         TID_STRING).getString();
            _expandedPath = path::expandForSave(path, *query);
        }

        // Computing physical instance parameter may require a catalog
        // lookup, so do it just once here and stash the result in the
        // inspectable for use by PhysicalSave.
        InstanceID physIid = COORDINATOR_INSTANCE_MASK;
        Parameter instParam;
        if (_parameters.size() >= 2) {
            instParam = _parameters[1];
        }
        if (instParam) {
            if (findKeyword(KW_INSTANCE)) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                           instParam->getParsingContext())
                    << getLogicalName() << KW_INSTANCE << 2;
            }
        } else {
            instParam = findKeyword(KW_INSTANCE);
        }
        if (instParam) {
            if (instParam->getParamType() == PARAM_LOGICAL_EXPRESSION) {
                physIid = paramToInt64(instParam);
            }
            else {
                ASSERT_EXCEPTION(instParam->getParamType() == PARAM_NESTED,
                                 "Problem with LogicalSave::makePlistSpec()");
                auto nest = safe_dynamic_cast<OperatorParamNested*>(instParam.get());
                ASSERT_EXCEPTION(nest->size() == 2, "Trouble with LogicalSave::makePlistSpec()");
                auto lexp = (std::shared_ptr<OperatorParamLogicalExpression>&)nest->getParameters()[0];
                auto server_id = evaluate(lexp->getExpression(), TID_INT32).getInt32();
                lexp = (std::shared_ptr<OperatorParamLogicalExpression>&)nest->getParameters()[1];
                auto server_inst_id = evaluate(lexp->getExpression(), TID_INT32).getInt32();

                SystemCatalog& sysCat = *SystemCatalog::getInstance();
                physIid = sysCat.getPhysicalInstanceId(server_id, server_inst_id);
                if (physIid == INVALID_INSTANCE) {
                    // Didn't find a match in the current cluster membership.
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INSTANCE_OFFLINE)
                        << Iid(server_id, server_inst_id);
                }
            }
        }
        ostringstream oss;
        oss << physIid;
        _srcInstAsString = oss.str();

        Parameter fmtParam = findKeyword(KW_FORMAT);
        if (_parameters.size() >= 3) {
            if (fmtParam) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                           fmtParam->getParsingContext())
                    << getLogicalName() << KW_FORMAT << 3;
            }
            fmtParam = _parameters[2];
        }
        if (fmtParam) {
            Value v = evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)fmtParam)->getExpression(),
                TID_STRING);
            string const& format = v.getString();

            if (!format.empty()
                && compareStringsIgnoreCase(format, "auto") != 0
                && !iofmt::isOutputFormat(format))
            {
                throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                            SCIDB_LE_UNSUPPORTED_FORMAT,
                                            _parameters[2]->getParsingContext())
                    << format;
            }
        }

        return inputSchemas[0];
    }

};


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSave, "save")


} //namespace
