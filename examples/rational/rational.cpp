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
 * @file rational.cpp
 *
 * @author paul@scidb.org
 *
 * @brief Shared library that loads into SciDB a Rational data type.
 */

#include <vector>

#include <query/LogicalOperator.h>
#include <query/PhysicalOperator.h>
#include <query/FunctionLibrary.h>
#include <query/FunctionDescription.h>
#include <query/TypeSystem.h>
#include <system/ErrorsLibrary.h>
#include <query/TileFunctions.h>
#include <query/Aggregate.h>

#include <SciDBAPI.h> // for #define EXPORTED_FUNCTION

#include "rational_functions.h"

using namespace std;
using namespace scidb;

/**
 * EXPORT FUNCTIONS
 * Functions from this section will be used by LOAD LIBRARY operator.
 */
vector<BaseLogicalOperatorFactory*> _logicalOperatorFactories;
EXPORTED_FUNCTION const vector<BaseLogicalOperatorFactory*>& GetLogicalOperatorFactories()
{
    return _logicalOperatorFactories;
}

vector<BasePhysicalOperatorFactory*> _physicalOperatorFactories;
EXPORTED_FUNCTION const vector<BasePhysicalOperatorFactory*>& GetPhysicalOperatorFactories()
{
    return _physicalOperatorFactories;
}

vector<Type> _types;
EXPORTED_FUNCTION const vector<Type>& GetTypes()
{
    return _types;
}

vector<FunctionDescription> _functionDescs;
EXPORTED_FUNCTION const vector<FunctionDescription>& GetFunctions()
{
    return _functionDescs;
}

vector<AggregatePtr> _aggregates;
EXPORTED_FUNCTION const vector<AggregatePtr>& GetAggregates()
{
    return _aggregates;
}

const char TID_RATIONAL[] = "rational";
/**
 * Class for registering/unregistering user defined objects
 */
static class RationalLibrary
{
public:
    // Registering objects
    RationalLibrary()
    {
		//
		// The Type constructor takes:
		//   a) A Name for the type - a string.
		//   b) An Integer identifier that is scoped to within this
		//      shared library. That is, two different types, in two
		//      shared libraries, can share an identifier. SciDB assigns
		// 	    each module an identifier as it loads the module and the
		//      SciDB internal identifiers for things like types and
		//      functions combine the ID of the Module, with the IDs of
		//      objects defined within the module: here, for example.
		//   c) The third argument is the size, in bits, of the data
		//      in the type.
        Type rationalType("rational", sizeof(SciDB_Rational) * 8);
        _types.push_back(rationalType);

		//
		// The FunctionDescription constructor takes:
		//  a) A name for the function - a string.
		//  b) A vector of input types. These can be constructed using the
		//     argTypes() function, which takes a variable list of
		//     parameters. The first paramater to the argTypes() function is
		//     an integer corresponding to the number of parameters passed
		//     into the function. The rest of the arguments to argTypes() is
		//     a list of type identifiers. For the built in types (String,
		//     various numerical types) you can use the enum{} provided.
		//     For functions that take extended types, use the type identifiers
		//     supplied above.
		//
		//  c) The third argument to FunctionDescription() is an identifier
		//     for the function's return type.
		//  d) The fourth and final argument is a function pointer to the
		//     code that implements the function.
        _functionDescs.push_back(FunctionDescription("rational", {}, TID_RATIONAL, &construct_rational));
        _functionDescs.push_back(FunctionDescription("rational", {TID_STRING}, TID_RATIONAL, &str2Rational));
        _functionDescs.push_back(FunctionDescription("rational", {TID_INT64}, TID_RATIONAL, &int2Rational));
        _functionDescs.push_back(FunctionDescription("rational", {TID_INT64, TID_INT64}, TID_RATIONAL, &ints2Rational));
        _functionDescs.push_back(FunctionDescription("str", {TID_RATIONAL}, TID_STRING, &rational2Str));
        _functionDescs.push_back(FunctionDescription("getnumer", {TID_RATIONAL}, TID_INT64, &rationalGetNumerator));
        _functionDescs.push_back(FunctionDescription("getdenom", {TID_RATIONAL}, TID_INT64, &rationalGetDenominator));
        _functionDescs.push_back(FunctionDescription("+", {TID_RATIONAL, TID_RATIONAL}, TID_RATIONAL, &rationalPlus));
        _functionDescs.push_back(FunctionDescription("-", {TID_RATIONAL, TID_RATIONAL}, TID_RATIONAL, &rationalMinus));
        _functionDescs.push_back(FunctionDescription("*", {TID_RATIONAL, TID_RATIONAL}, TID_RATIONAL, &rationalTimes));
        _functionDescs.push_back(FunctionDescription("/", {TID_RATIONAL, TID_RATIONAL}, TID_RATIONAL, &rationalDivide));
        _functionDescs.push_back(FunctionDescription("/", {TID_RATIONAL, TID_INT64}, TID_RATIONAL, &rationalIntDivide));
        _functionDescs.push_back(FunctionDescription("<", {TID_RATIONAL, TID_RATIONAL}, TID_BOOL, &rationalLT));
        _functionDescs.push_back(FunctionDescription("<=", {TID_RATIONAL, TID_RATIONAL}, TID_BOOL, &rationalLTEQ));
        _functionDescs.push_back(FunctionDescription("=", {TID_RATIONAL, TID_RATIONAL}, TID_BOOL, &rationalEQ));
        _functionDescs.push_back(FunctionDescription(">=", {TID_RATIONAL, TID_RATIONAL}, TID_BOOL, &rationalGTEQ));
        _functionDescs.push_back(FunctionDescription(">", {TID_RATIONAL, TID_RATIONAL}, TID_BOOL, &rationalGT));

        // Aggregates
//        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggSum, SciDB_Rational, SciDB_Rational>("sum", rationalType, rationalType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggAvg, SciDB_Rational, SciDB_Rational>("avg", rationalType, rationalType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregateInitByFirst<AggMin, SciDB_Rational, SciDB_Rational>("min", rationalType, rationalType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregateInitByFirst<AggMax, SciDB_Rational, SciDB_Rational>("max", rationalType, rationalType)));
        _aggregates.push_back(AggregatePtr(new BaseAggregate<AggVar, SciDB_Rational, SciDB_Rational>("var", rationalType, rationalType)));

        _errors[RATIONAL_E_CANT_CONVERT_TO_RATIONAL] = "Can't convert '%1%' to rational, expected '( int / int )'";
        scidb::ErrorsLibrary::getInstance()->registerErrors("librational", &_errors);
    }

    ~RationalLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("librational");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;


REGISTER_CONVERTER(rational, string, EXPLICIT_CONVERSION_COST, rational2Str);
REGISTER_CONVERTER(string, rational, EXPLICIT_CONVERSION_COST, str2Rational);
