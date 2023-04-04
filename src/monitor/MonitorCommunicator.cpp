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
 * MonitorCommunicator.cpp
 *
 *  Created on: May 19, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <monitor/MonitorCommunicator.h>


#include <log4cxx/logger.h>
#include <memory>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Query.h>
#include <string>
#include <util/PluginManager.h>

namespace scidb
{
    namespace monitor
    {
        namespace Communicator
        {
            void addQueryInfoToHistory(Query* query)
            {

                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "_addQueryInfoToHistory",   // const std::string& name
                    {                           // const vector<TypeId>& inputArgTypes
                        TID_BINARY},            //   pQuery
                    func,                       // FunctionDescription& funcDescription
                    convs,                      // std::vector<FunctionPointer>& converters
                    false);                     // bool tile );

                if(!func.getFuncPtr())
                {
                    return;  // throw?
                }

                Value inputParams[] = {
                    Value(TypeLibrary::getType(TID_BINARY))};   // pQuery

                Query** const pQuery = &query;
                inputParams[0].setData(&pQuery, sizeof(pQuery));

                const Value* vInputParams[] = {
                    &inputParams[0]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                try
                {
                    func.getFuncPtr()(vInputParams, &returnParams, NULL);
                } catch (const Exception& e) {
                    LOG4CXX_ERROR(logger, "Communicator::_addHistory exception=" << e.what());
                    e.raise();
                }
            }
        } // namespace Communicator
    } // namespace monitor
} // namespace scidb
