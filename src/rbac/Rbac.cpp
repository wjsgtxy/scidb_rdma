/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2017-2019 SciDB, Inc.
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
 * @file Rbac.cpp
 * @brief Role-based access control functions.
 */

#include <rbac/Rbac.h>

#include <query/FunctionLibrary.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <rbac/UserDesc.h>

using namespace std;

namespace scidb { namespace rbac {

void listUsers(vector<UserDesc>& usersDescs)
{
    std::vector<FunctionPointer> convs;
    FunctionDescription func;

    FunctionLibrary::getInstance()->findFunction(
        "_getUsers",            // const std::string& name
        {                       // const vector<TypeId>& inputArgTypes
            TID_BINARY},        //   in - const UserDesc * pUserDesc
        func,                   // FunctionDescription& funcDescription
        convs,                  // std::vector<FunctionPointer>& converters
        false);                 // bool tile );

    if(!func.getFuncPtr())
    {
        return;
    }

    Value inputParams[1] = {
        Value(TypeLibrary::getType(TID_BINARY))}; // pUserDescs

    std::vector<scidb::UserDesc> *pUserDescs = &usersDescs;
    inputParams[0].setData(&pUserDescs, sizeof(pUserDescs));

    const Value* vInputParams[1] = {
        &inputParams[0]};

    Value dontCare(TypeLibrary::getType(TID_INT32));
    func.getFuncPtr()(vInputParams, &dontCare, NULL);
}

bool findUser(UserDesc& desc)
{
    vector<FunctionPointer> converters;
    FunctionDescription func;

    FunctionLibrary::getInstance()->findFunction(
        "_findUser",
        {TID_UINT64}, // [in,out] UserDesc *pUserDesc
        func,
        converters,
        /*tileMode:*/ false);

    if (!func.getFuncPtr()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
            << "namespaces (_findUser)";
    }

    Value inputParams[] = {
        Value(TypeLibrary::getType(TID_UINT64)),  // pUserDesc
    };
    inputParams[0].setUint64(reinterpret_cast<uint64_t>(&desc));
    const Value* vInputParams[] = {
        &inputParams[0],
        nullptr
    };

    Value result(TypeLibrary::getType(TID_BOOL));
    func.getFuncPtr()(vInputParams, &result, nullptr);
    return result.getBool();
}


// MUST agree with Lexer.ll "Identifier" regex!
bool isAflIdentifier(string const& name)
{
    if (name.empty() || name.size() > LOGIN_NAME_MAX || !::isalpha(name[0])) {
        return false;
    }
    for (unsigned char const& ch : name) {
        if (ch > 0x7F || (!::isalnum(ch) && ch != '_' && ch != '$')) {
            return false;
        }
    }
    return true;
}

bool isBase64Alphabet(string const& s)
{
    static char const * const B64_CHARS = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789"
        "+/=";      // Just the main variant for now.

    auto pos = s.find_first_not_of(B64_CHARS);
    return pos == string::npos;
}

} } // namespaces
