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

#ifndef PHYSICAL_ADD_ATTRIBUTES_H_
#define PHYSICAL_ADD_ATTRIBUTES_H_

#include <query/PhysicalUpdate.h>

namespace scidb {

struct PhysicalAddAttributes : PhysicalUpdate
{
    PhysicalAddAttributes(std::string const& logicalName,
                          std::string const& physicalName,
                          Parameters const& parameters,
                          ArrayDesc const& schema);
    virtual ~PhysicalAddAttributes();

    virtual std::shared_ptr<Array> execute(
            std::vector<std::shared_ptr<Array>>& inputArrays,
            std::shared_ptr<Query> query) override;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalAddAttributes, "add_attributes", "physical_add_attributes")

}  // namespcae scidb

#endif  // PHYSICAL_ADD_ATTRIBUTES_H_
