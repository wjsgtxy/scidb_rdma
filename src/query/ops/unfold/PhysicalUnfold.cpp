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
 * PhysicalUnfold.cpp
 *
 *  Created on: 13 May 2014
 *      Author: Dave Gosselin
 */

#include <query/PhysicalOperator.h>
#include <query/AutochunkFixer.h>
#include "UnfoldArray.h"

using namespace std;

namespace scidb
{

  class PhysicalUnfold : public PhysicalOperator
  {
  public:
    PhysicalUnfold(string const& logicalName,
		   string const& physicalName,
		   Parameters const& parameters,
		   ArrayDesc const& schema)
      : PhysicalOperator(logicalName, physicalName, parameters, schema) {}

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
      // Distribution is undefined.
      SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
      return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext> const& inputDistributions,
                                                      vector<ArrayDesc> const& inputSchemas) const override
    {
      assertConsistency(inputSchemas[0], inputDistributions[0]);

      // Distribution is undefined.
      SCIDB_ASSERT(isUndefined(_schema.getDistribution()->getDistType()));
      _schema.setResidency(inputDistributions[0].getArrayResidency());

      return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    /**
     * Unfold transforms the input array into a 2-d matrix whose columns
     * correspond to the input array attributes. The output matrix row dimension
     * will have a chunk size equal to the input array, and column chunk size
     * equal to the number of columns.
     */
    std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& inputArrays,
	    std::shared_ptr<Query> query) {
      // This operator never takes more than one input array.
      assert(inputArrays.size() == 1);

      AutochunkFixer af(getControlCookie());
      af.fix(_schema, inputArrays);

      // Return an UnfoldArray which defers the work to the "pull" phase.
      return std::make_shared<UnfoldArray> (_schema, inputArrays[0],
					      query);
    }
  };

  // In this registration, the second argument must match the AFL operator
  // name and the name provided in the Logical##name file. The third
  // argument is arbitrary and used for debugging purposes.
  DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnfold,
				    "unfold",
				    "PhysicalUnfold")

}  // namespace scidb
