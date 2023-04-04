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

#include "PhysicalAddAttributes.h"

#include <array/ArrayDesc.h>
#include <array/TransientCache.h>
#include <array/MemArray.h>
#include <query/Query.h>

#include <string>

namespace scidb {

PhysicalAddAttributes::PhysicalAddAttributes(
    std::string const& logicalName,
    std::string const& physicalName,
    Parameters const& parameters,
    ArrayDesc const& schema)
    : PhysicalUpdate(logicalName,
                     physicalName,
                     parameters,
                     schema,
                     schema.getQualifiedArrayName(false))
{
    SCIDB_ASSERT(!schema.getName().empty());
}

PhysicalAddAttributes::~PhysicalAddAttributes()
{
}

std::shared_ptr<Array> PhysicalAddAttributes::execute(
    std::vector< std::shared_ptr<Array> >& inputArrays,
    std::shared_ptr<Query> query)
{
    SCIDB_ASSERT(inputArrays.empty());
    std::shared_ptr<Array> nullArray;
    executionPreamble(nullArray, query);

    // PhysicalUpdate::postSingleExecute updates the catalog for persistent arrays
    // but does not make equivalent updates for transient arrays.  Rather
    // than push catalog updates for transient arrays up into PhysicalUpdate,
    // push the finalizer here.
    if (_schema.isTransient()) {
        // Finalizer updates the catalog to place new attributes
        query->pushVotingFinalizer(std::bind(&PhysicalUpdate::updateTransient,
                                             this,
                                             std::placeholders::_1));
        // transient::lookup updates the array descriptor attached to the transient
        // array in the transient array cache
        return transient::lookup(_schema, query);
    }

    return {};
}

}  // namespace scidb
