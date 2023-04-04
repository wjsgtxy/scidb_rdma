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
 * LogicalDiskInfo.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <query/LogicalOperator.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: diskinfo().
 *
 * @par Synopsis:
 *   diskinfo()
 *
 * @par Summary:
 *   Checks disk usage.
 *
 * @par Input:
 *   n/a
 *
 * @par Output array:
 *        <
 *   <br>   used: uint64
 *   <br>   available: uint64
 *   <br>   clusterSize: uint64
 *   <br>   nFreeClusters: uint64
 *   <br>   nSegments: uint64
 *   <br> >
 *   <br> [
 *   <br>   Instance: start=0, end=#instances less 1, chunk interval=1.
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   - For internal usage.
 *
 */
class LogicalDiskInfo: public LogicalOperator
{
public:
    LogicalDiskInfo(const string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
    {
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 0);

        Attributes attributes(5);
        attributes[0] = AttributeDesc(0, "used",  TID_UINT64, 0, CompressorType::NONE);
        attributes[1] = AttributeDesc(1, "available",  TID_UINT64, 0, CompressorType::NONE);
        attributes[2] = AttributeDesc(2, "clusterSize",  TID_UINT64, 0, CompressorType::NONE);
        attributes[3] = AttributeDesc(3, "nFreeClusters",  TID_UINT64, 0, CompressorType::NONE);
        attributes[4] = AttributeDesc(4, "nSegments",  TID_UINT64, 0, CompressorType::NONE);
        vector<DimensionDesc> dimensions(1);
        const size_t nInstances = query->getInstancesCount();
        size_t end        = nInstances>0 ? nInstances-1 : 0;
        dimensions[0]     = DimensionDesc("Instance", 0, 0, end, end, 1, 0);
        return ArrayDesc("DiskInfo", attributes, dimensions,
                         createDistribution(getSynthesizedDistType()),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDiskInfo, "_diskinfo")

}  // namespace scidb
