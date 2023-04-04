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
 *  Created on: May 19, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <rbac/NamespacesCommunicator.h>

#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <rbac/Authenticator.h>
#include <rbac/NamespaceDesc.h>
#include <util/PluginManager.h>
#include <util/Pqxx.h>
#include <rbac/Rights.h>
#include <rbac/Session.h>

#include <memory>
#include <sstream>
#include <string>

using namespace std;

namespace scidb
{
    namespace namespaces
    {
        Authenticator* Communicator::makeAuthenticator(string const& user,
                                                       AuthMethod method)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_makeAuthenticator",
                {                         // const vector<TypeId>& inputArgTypes
                    TID_UINT64,           //   in - const char* username
                    TID_UINT32},          //   in - AuthMethod enum value
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_makeAuthenticator)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),
                Value(TypeLibrary::getType(TID_UINT32)),
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(user.c_str()));
            inputParams[1].setUint32(static_cast<uint32_t>(method));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                nullptr
            };

            Value retVal;
            func.getFuncPtr()(vInputParams, &retVal, nullptr);
            return reinterpret_cast<Authenticator*>(retVal.getUint64());
        }

        bool Communicator::findArray(
            SystemCatalog::FindArrayArgs const& args,
            SystemCatalog::FindArrayResult& result,
            pqxx::basic_transaction* tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_findArrayWithTransaction",
                {                            // const vector<TypeId>& inputArgTypes
                    TID_UINT64,              //   in - FindArrayArgs const *
                    TID_UINT64,              //   out - FindArrayResults *
                    TID_UINT64},             //   in,out - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_findArrayWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // FindArrayArgs*
                Value(TypeLibrary::getType(TID_UINT64)),  // FindArrayResult*
                Value(TypeLibrary::getType(TID_UINT64)),  // tr*
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&args));
            inputParams[1].setUint64(reinterpret_cast<uint64_t>(&result));
            inputParams[2].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                nullptr
            };

            Value retVal;
            func.getFuncPtr()(vInputParams, &retVal, nullptr);
            return retVal.getBool();
        }

        void Communicator::findNamespaceForArray(
            ArrayID arrayId,
            NamespaceDesc& nsDesc,
            pqxx::basic_transaction* tr)
        {
            SCIDB_ASSERT(arrayId);
            SCIDB_ASSERT(tr);

            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_findNsForArrayWithTransaction",
                {                // const vector<TypeId>& inputArgTypes
                    TID_UINT64,  //   in - arrayId
                    TID_UINT64,  //   out - NamespaceDesc * nsDesc
                    TID_UINT64}, //   in,out - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_findNsForArrayWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // arrayid
                Value(TypeLibrary::getType(TID_UINT64)),  // NamespaceDesc*
                Value(TypeLibrary::getType(TID_UINT64)),  // tr*
            };

            inputParams[0].setUint64(arrayId);
            inputParams[1].setUint64(reinterpret_cast<uint64_t>(&nsDesc));
            inputParams[2].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                nullptr
            };

            func.getFuncPtr()(vInputParams, nullptr, nullptr);
        }

        bool Communicator::findNamespaceWithTransaction(
            NamespaceDesc& nsDesc,
            bool byName,
            pqxx::basic_transaction* tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_findNamespaceWithTransaction",
                {                 // const vector<TypeId>& inputArgTypes
                    TID_UINT64,   //   in - const NamespaceDesc * pNamespaceDesc
                    TID_BOOL,     //   in - bool byName
                    TID_UINT64},  //   in - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_findNamespaceWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // pNsDesc
                Value(TypeLibrary::getType(TID_BOOL)),    // byName
                Value(TypeLibrary::getType(TID_UINT64)),  // tr
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&nsDesc));
            inputParams[1].setBool(byName);
            inputParams[2].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                nullptr
            };

            Value result;
            func.getFuncPtr()(vInputParams, &result, nullptr);
            return result.getBool();
        }

        void Communicator::checkAccess(Session const* pSess,
                                       rbac::RightsMap const* pRights)
        {
            SCIDB_ASSERT(pSess);
            SCIDB_ASSERT(pRights);

            if (Authenticator::getRequiredAuthMethod() == AUTH_TRUST) {
                // Everything is beautiful (in its own way).
                return;
            }

            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_checkAccess",         // const string& name
                {                       // const vector<TypeId>& inputArgTypes
                    TID_UINT64,         //   pSess
                    TID_UINT64},        //   pRights
                func,                   // FunctionDescription& funcDescription
                convs,                  // vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_checkAccess)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)), // pSession
                Value(TypeLibrary::getType(TID_UINT64))  // pRights
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(pSess));
            inputParams[1].setUint64(reinterpret_cast<uint64_t>(pRights));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                nullptr
            };

            Value dummy(TypeLibrary::getType(TID_BOOL));
            func.getFuncPtr()(vInputParams, &dummy, nullptr);
        }

        void Communicator::listArrayIds(
            NamespaceDesc& nsDesc,
            vector<ArrayID>& arrayIds,
            bool ignoreVersions,
            bool orderByName,
            pqxx::basic_transaction* tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_listArraysWithTransaction",
                {                 // const vector<TypeId>& inputArgTypes
                    TID_UINT64,   //   in - NamespaceDesc * pNamespaceDesc
                    TID_UINT64,   //   in - vector<ArrayID> * pVec
                    TID_BOOL,     //   in - bool ignoreVersions
                    TID_BOOL,     //   in - bool orderByName
                    TID_UINT64},  //   in - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_listArraysWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // pNsDesc
                Value(TypeLibrary::getType(TID_UINT64)),  // pVec
                Value(TypeLibrary::getType(TID_BOOL)),    // ignoreVersions
                Value(TypeLibrary::getType(TID_BOOL)),    // orderByName
                Value(TypeLibrary::getType(TID_UINT64)),  // tr
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&nsDesc));
            inputParams[1].setUint64(reinterpret_cast<uint64_t>(&arrayIds));
            inputParams[2].setBool(ignoreVersions);
            inputParams[3].setBool(orderByName);
            inputParams[4].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3],
                &inputParams[4],
                nullptr
            };

            func.getFuncPtr()(vInputParams, nullptr, nullptr);
        }

        void Communicator::listAllArrayIds(
            vector<ArrayID>& arrayIds,
            bool ignoreVersions,
            bool orderByName,
            pqxx::basic_transaction* tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_listAllArraysWithTransaction",
                {                 // const vector<TypeId>& inputArgTypes
                    TID_UINT64,   //   in - vector<ArrayID> * pVec
                    TID_BOOL,     //   in - bool ignoreVersions
                    TID_BOOL,     //   in - bool orderByName
                    TID_UINT64},  //   in - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if (!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_listAllArraysWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // pVec
                Value(TypeLibrary::getType(TID_BOOL)),    // ignoreVersions
                Value(TypeLibrary::getType(TID_BOOL)),    // orderByName
                Value(TypeLibrary::getType(TID_UINT64)),  // tr
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&arrayIds));
            inputParams[1].setBool(ignoreVersions);
            inputParams[2].setBool(orderByName);
            inputParams[3].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                &inputParams[3],
                nullptr
            };

            func.getFuncPtr()(vInputParams, nullptr, nullptr);
        }

        void Communicator::getNamespacesWithTransaction(
            vector<NamespaceDesc> &    nsVec,
            pqxx::basic_transaction *       tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getNamespacesWithTransaction", // const string& name
                {                       // const vector<TypeId>& inputArgTypes
                    TID_UINT64,         //   out - vector<NamespaceDesc> * pNsVec
                    TID_UINT64},        //   in,out - transaction pointer
                func,                   // FunctionDescription& funcDescription
                convs,                  // vector<FunctionPointer>& converters
                /*tileMode:*/false);

            if(!func.getFuncPtr())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_getNamespacesWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)), // pNsVec
                Value(TypeLibrary::getType(TID_UINT64))  // tr
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&nsVec));
            inputParams[1].setUint64(reinterpret_cast<uint64_t>(tr));
            
            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                nullptr
            };

            func.getFuncPtr()(vInputParams, nullptr, nullptr);
        }

        bool Communicator::getRoles(
            vector<scidb::RoleDesc> &  roleDescs)
        {
            scidb::RoleDesc roleDesc;
            vector<FunctionPointer> convs;
            FunctionDescription func;

            if (Authenticator::getRequiredAuthMethod() == AUTH_TRUST) {
                return true;
            }

            FunctionLibrary::getInstance()->findFunction(
                "_getRoles",            // const string& name
                {                       // const vector<TypeId>& inputArgTypes
                    TID_BINARY},        //   out - const vector<scidb::RoleDesc> * pRoleDescs
                func,                   // FunctionDescription& funcDescription
                convs,                  // vector<FunctionPointer>& converters
                false);                 // bool tile );

            if(!func.getFuncPtr())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_getRoles)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY))}; // pRoleDescs

            vector<scidb::RoleDesc> *pRoleDescs = &roleDescs;
            inputParams[0].setData(&pRoleDescs, sizeof(pRoleDescs));

            const Value* vInputParams[] = {
                &inputParams[0]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            int retval = returnParams.getInt32();
            return 0 == retval;
        }

        void Communicator::addArrayToNamespaceWithTransaction(
            const NamespaceDesc &       nsDesc,
            const ArrayID               UAId,
            pqxx::basic_transaction *   tr)
        {
            vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_addArrayToNamespaceWithTransaction",
                {                // const vector<TypeId>& inputArgTypes
                    TID_UINT64,  //   in - const NamespaceDesc * pNamespaceDesc
                    TID_UINT64,  //   in - const ArrayID         UAId
                    TID_UINT64}, //   in - pqxx::basic_transaction * tr
                func,
                convs,
                /*tileMode:*/ false);

            if(!func.getFuncPtr())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces (_addArrayToNamespaceWithTransaction)";
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_UINT64)),  // pNsDesc
                Value(TypeLibrary::getType(TID_UINT64)),  // UAId
                Value(TypeLibrary::getType(TID_UINT64)),  // tr
            };

            inputParams[0].setUint64(reinterpret_cast<uint64_t>(&nsDesc));
            inputParams[1].setUint64(UAId);
            inputParams[2].setUint64(reinterpret_cast<uint64_t>(tr));

            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1],
                &inputParams[2],
                nullptr
            };

            func.getFuncPtr()(vInputParams, nullptr, nullptr);
        }

    } // namespace namespaces
} // namespace scidb
