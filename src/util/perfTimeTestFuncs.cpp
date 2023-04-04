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
 * @file PerfTimeTestFuncs.cpp
 *
 * @brief function plugins used only for testing PerfTime code
 */

#include <unistd.h>

#include <chrono>
#include <vector>

#include <util/PerfTime.h>

#include <query/FunctionLibrary.h>
#include <query/FunctionDescription.h>
#include <query/TypeSystem.h>
#include <system/ErrorsLibrary.h>

#include <SciDBAPI.h> // for #define EXPORTED_FUNCTION

using namespace std;
using namespace scidb;

/// invoke posix sleep() function for the number of seconds
/// in the first arg
void perfTimeTestSleep(const Value** args, Value* res, void*)
{
    ScopedWaitTimer timer(PTW_SWT_TEST_SLEEP);
    res->setInt32(::sleep(args[0]->getUint32()));
}

/// like scidb_sleep, but busy-waits
/// for the specified number of seconds
void perfTimeTestSpin(const Value** args, Value* res, void*)
{
    using namespace std::chrono;    // since c++11

    seconds durationSec(args[0]->getUint32());
    time_point<system_clock> stopPoint = system_clock::now() + durationSec;

    ScopedWaitTimer timer(PTW_SWT_TEST_SPIN);
    while (system_clock::now() < stopPoint) {
        ;
    }
    res->setInt32(0);
}

REGISTER_FUNCTION(perf_time_test_sleep, UILIST("uint32"), "uint32", perfTimeTestSleep);
REGISTER_FUNCTION(perf_time_test_spin,  UILIST("uint32"), "uint32", perfTimeTestSpin);

// the following is found by the plugin manager
// using symbol-table lookup on the loaded shared library
vector<FunctionDescription> _functionDescs;
EXPORTED_FUNCTION const vector<FunctionDescription>& GetFunctions()
{
    return _functionDescs;
}
