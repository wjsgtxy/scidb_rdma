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

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>

/****************************************************************************/
namespace scidb { namespace {
/****************************************************************************/
/**
 * @brief The operator: load_module().
 *
 * @par Synopsis:
 *   load_module( module )
 *
 * @par Summary:
 *   Loads a SciDB module.
 *
 * @par Input:
 *   - module: the path name of the module file to load.
 *
 * @par Output array:
 *   - NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 */
/****************************************************************************/
using namespace std;                                     // For all of std
/****************************************************************************/

struct LogicalLoadModule : LogicalOperator
{
    LogicalLoadModule(const string& n,const string& a)
     : LogicalOperator(n,a)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc>,std::shared_ptr<Query> query)
    {
        Attributes outputAttributes(1);
        outputAttributes.push_back(AttributeDesc("module", TID_STRING,
                                                 0,CompressorType::NONE));
        return ArrayDesc("load_module",
                         outputAttributes,
                         Dimensions(1,DimensionDesc("i",0,0,0,0,1,0)),
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }
};

/****************************************************************************/
} DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalLoadModule,"load_module")}
/****************************************************************************/
