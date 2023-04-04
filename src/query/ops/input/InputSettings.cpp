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
 * @file InputSettings.cpp
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Encapsulated parameter parsing for the input() operator.
 */

#include "InputSettings.h"
#include "InputArray.h"

#include <query/Expression.h>
#include <query/UserQueryException.h>
#include <system/SystemCatalog.h>
#include <util/PathUtils.h>

#include <lib_json/json.h>

using namespace std;

namespace scidb {

const char* const InputSettings::KW_PATH = "path";
const char* const InputSettings::KW_INSTANCE = "instance";
const char* const InputSettings::KW_FORMAT = "format";
const char* const InputSettings::KW_MAX_ERRORS = "max_errors";
const char* const InputSettings::KW_STRICT = "strict";
const char* const InputSettings::KW_CHUNK_HINT = "_chunk_hint";

namespace {
    // JSON dict key(s) for stuff with no explicit KW_FOO keyword.
    const char* const J_DF_MODE = "dataframe";
}

void InputSettings::parse(Parameters const& _parameters,
                          KeywordParameters const& _kwParameters,
                          Query const& query)
{
    if (_valid) {
        // The inferSchema() method may be called several times, but we'll
        // always come up with the same settings, so just do it once.
        return;
    }

    SCIDB_ASSERT(_parameters.size() >= 2);

    //
    // Check schema parameter: are we expected to produce a dataframe?
    //
    ArrayDesc const& desc =
        ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
    _isDfMode = desc.isDataframe();

    //
    // File path, expanded per SDB-5753.
    //
    string originalPath = paramToString(_parameters[1]);
    _path = path::expandForRead(originalPath, query);

    //
    // Instance to do the work.
    //
    SCIDB_ASSERT(_instanceId == COORDINATOR_INSTANCE_MASK);
    Parameter instParam;
    if (_parameters.size() >= 3) {
        instParam = _parameters[2];
    }
    auto kw = _kwParameters.find(KW_INSTANCE);
    if (kw != _kwParameters.end()) {
        if (instParam) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                       instParam->getParsingContext())
                << "input" << KW_INSTANCE << 3;
        }
        instParam = kw->second;
    }
    if (instParam) {
        if (instParam->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            _instanceId = paramToInt64(instParam);
            if (_instanceId != COORDINATOR_INSTANCE_MASK && _instanceId != ALL_INSTANCE_MASK &&
                !isValidPhysicalInstance(_instanceId))
            {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_INSTANCE_ID,
                                           instParam->getParsingContext()) << _instanceId;
            }
        }
        else {
            ASSERT_EXCEPTION(instParam->getParamType() == PARAM_NESTED,
                             "Problem with LogicalInput::makePlistSpec()");
            auto nest = safe_dynamic_cast<OperatorParamNested*>(instParam.get());
            ASSERT_EXCEPTION(nest->size() == 2, "Trouble with LogicalInput::makePlistSpec()");
            auto lexp = safe_dynamic_cast<OperatorParamLogicalExpression*>(nest->getParameters()[0].get());
            auto server_id = evaluate(lexp->getExpression(), TID_INT32).getInt32();
            lexp = safe_dynamic_cast<OperatorParamLogicalExpression*>(nest->getParameters()[1].get());
            auto server_inst_id = evaluate(lexp->getExpression(), TID_INT32).getInt32();

            // The catalog knows the physical instance for (sid, siid)...
            SystemCatalog& sysCat = *SystemCatalog::getInstance();
            _instanceId = sysCat.getPhysicalInstanceId(server_id, server_inst_id);
            if (_instanceId == INVALID_INSTANCE) {
                // Didn't find a match in the current cluster membership.
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INSTANCE_OFFLINE)
                    << Iid(server_id, server_inst_id);
            }
        }
    }

    //
    // Format of input file.
    //
    Parameter fmtParam;
    if (_parameters.size() >= 4) {
        fmtParam = _parameters[3];
    }
    kw = _kwParameters.find(KW_FORMAT);
    if (kw != _kwParameters.end()) {
        if (fmtParam) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                       fmtParam->getParsingContext())
                << "input" << KW_FORMAT << 4;
        }
        fmtParam = kw->second;
    }
    if (fmtParam) {
        _format = paramToString(fmtParam);
        if (!InputArray::isSupportedFormat(_format))
        {
            throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNSUPPORTED_FORMAT,
                                        fmtParam->getParsingContext()) << _format;
        }
    }

    //
    // Maximum allowable error count.
    //
    Parameter maxErrParam;
    if (_parameters.size() >= 5) {
        maxErrParam = _parameters[4];
    }
    kw = _kwParameters.find(KW_MAX_ERRORS);
    if (kw != _kwParameters.end()) {
        if (maxErrParam) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                       maxErrParam->getParsingContext())
                << "input" << KW_MAX_ERRORS << 5;
        }
        maxErrParam = kw->second;
    }
    if (maxErrParam) {
        _maxErrors = paramToInt64(maxErrParam);
        if (_maxErrors < 0) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_INPUT_ERROR17,
                                       maxErrParam->getParsingContext());
        }
    }

    //
    // Data integrity enforcement, aka "strict".
    //
    Parameter strictParam;
    if (_parameters.size() >= 6) {
        if (_parameters[5]->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            strictParam = _parameters[5];
        } else {
            ASSERT_EXCEPTION(false, "InputSettings::parse: expect parameter 5 to be strict-ness");
        }
    }

    kw = _kwParameters.find(KW_STRICT);
    if (kw != _kwParameters.end()) {
        if (strictParam) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_KEYWORD_CONFLICTS_WITH_OPTIONAL,
                                       strictParam->getParsingContext())
                << "input" << KW_STRICT << 6;
        }
        strictParam = kw->second;
    }
    if (strictParam) {
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(strictParam.get());
        SCIDB_ASSERT(lExp->isConstant());
        assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
        _isStrict = evaluate(lExp->getExpression(), TID_BOOL).getBool();
    }

    //
    // Chunk size hints vector, used during some restores from backup.
    //
    kw = _kwParameters.find(KW_CHUNK_HINT);
    if (kw != _kwParameters.end()) {
        auto nest = dynamic_cast<OperatorParamNested*>(kw->second.get());
        SCIDB_ASSERT(nest);
        Parameters group = nest->getParameters();
        for (auto const& p : group) {
            _chunkHint.push_back(paramToInt64(p));
        }
    }

    //
    // Way too many positional arguments?
    //
    if (_parameters.size() >= 7) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,
                                   _parameters[6]->getParsingContext());
    }

    _valid = true;
}


std::string InputSettings::toString() const
{
    SCIDB_ASSERT(_valid);

    // Unfortunately, binary format strings can contain spaces (for example, "(double null)"), so we
    // can't simply emit a bunch of space-separated tokens.  We're already linking with a JSON
    // parser, so use that.  Actually the code is much shorter this way... nice.

    Json::Value root;
    root[KW_PATH] = _path;
    root[KW_INSTANCE] = Json::Value::UInt(_instanceId);
    root[KW_FORMAT] = _format;
    root[KW_STRICT] = _isStrict;
    root[KW_MAX_ERRORS] = Json::Value::Int(_maxErrors);
    root[J_DF_MODE] = _isDfMode;

    Json::Value hint(Json::arrayValue);
    for (auto const& h : _chunkHint) {
        hint.append(Json::Value::Int(h));
    }
    root[KW_CHUNK_HINT] = hint;

    Json::FastWriter writer;
    return writer.write(root);
}


void InputSettings::fromString(std::string const& s)
{
    Json::Value root;
    Json::Reader reader;
    bool ok = reader.parse(s, root);
    ASSERT_EXCEPTION(ok, s);

    _path = root[KW_PATH].asString();
    _instanceId = root[KW_INSTANCE].asUInt();
    _format = root[KW_FORMAT].asString();
    _isStrict = root[KW_STRICT].asBool();
    _maxErrors = root[KW_MAX_ERRORS].asInt();
    _isDfMode = root[J_DF_MODE].asBool();
    _chunkHint.resize(root[KW_CHUNK_HINT].size());
    for (Json::ArrayIndex i = 0; i < _chunkHint.size(); ++i) {
        _chunkHint[i] = root[KW_CHUNK_HINT][i].asInt();
    }
    _valid = true;
}


std::ostream& operator<< (std::ostream& os, InputSettings const& settings)
{
    os << "[path:" << settings.getPath()
       << ",instId:" << settings.getInstanceId()
       << ",fmt:" << settings.getFormat()
       << ",maxErr:" << settings.getMaxErrors()
       << ",dataframes:" << settings.dataframeMode()
       << ",chunkHint:" << CoordsToStr(settings.getChunkHint())
       << ']';
    return os;
}


} // namespace
