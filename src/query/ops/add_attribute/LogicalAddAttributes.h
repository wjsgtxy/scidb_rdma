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

#ifndef LOGICAL_ADD_ATTRIBUTES_H_
#define LOGICAL_ADD_ATTRIBUTES_H_

#include <array/ArrayDesc.h>
#include <query/LogicalOperator.h>

namespace scidb {

class LogicalAddAttributes : public LogicalOperator
{
  public:
    LogicalAddAttributes(const std::string& logicalName, const std::string& aliasName = "");
    virtual ~LogicalAddAttributes() = default;

    static PlistSpec const* makePlistSpec();
    virtual void inferAccess(const std::shared_ptr<Query>& query) override;
    virtual ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                                  std::shared_ptr<Query> query) override;
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAddAttributes, "add_attributes")

}  // namespace scidb

#endif  // LOGICAL_ADD_ATTRIBUTES_H_
