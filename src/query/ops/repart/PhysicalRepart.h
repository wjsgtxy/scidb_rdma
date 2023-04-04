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
 * @file PhysicalRepart.h
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author apoliakov@paradigm4.com
 */

#include "../redimension/RedimensionCommon.h"

namespace scidb {

class PhysicalRepart : public RedimensionCommon
{
public:
    PhysicalRepart(std::string const& logicalName,
                   std::string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const & schema) :
       RedimensionCommon(logicalName, physicalName, parameters, schema)
    {}

    // inheritance
    std::vector<DistType> inferChildInheritances(DistType inherited, size_t numChildren)
        const override;

    PhysicalBoundaries getOutputBoundaries(std::vector<PhysicalBoundaries>const& sourceBoundaries,
                                                   std::vector<ArrayDesc>const& sourceSchema)
        const override;

    /// @see OperatorDist
    /// @note This is an operator that needs the inputSchemas argument because it uses it to
    ///       determine what the output DistType should be.  Would need further investigation
    ///       to determine whether that is important to proper functioning.
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/,
                                      std::vector<ArrayDesc> const& inSchema, size_t depth)
        const override;

    /// @see PhysicalOperator
    RedistributeContext getOutputDistribution(std::vector<RedistributeContext>const& inputDistributions,
                                              std::vector<ArrayDesc>const& inputSchemas)
        const override;

    bool outputFullChunks(std::vector<ArrayDesc>const& inputSchemas)
        const override;

    std::shared_ptr<Array>
    execute(std::vector< std::shared_ptr<Array> >& sourceArray, std::shared_ptr<Query> query)
        override;

private:
    //True if this is a no-op (just a metadata change, doesn't change chunk sizes or overlap)
    bool _isNoop(ArrayDesc const& inputSchema) const;
};

}  // namespace scidb
