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
 * @file LogicalInput.h
 * @author roman.simakov@gmail.com
 * @brief Input operator for reading data from external files.
 */

#include "InputSettings.h"
#include <query/LogicalOperator.h>

namespace scidb
{

/**
 * @brief The operator: input().
 *
 * @par Synopsis:
 *   input( schemaArray | schema, filename, instance=-2, format="", maxErrors=0, isStrict=false )
 *
 * @par Summary:
 *   Produces a result array and loads data from a given file.
 *
 * @par Input:
 *   - schemaArray | schema: the array schema.
 *   - filename: where to load data from.
 *   - instance: which instance; default is -2. ??
 *   - format: ??
 *   - maxErrors: ??
 *   - isStrict if true, enables the data integrity checks such as for data
 *              collisions and out-of-order input chunks, defualt=false.
 *
 * @par Output array:
 *   n/a
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 */
class LogicalInput: public LogicalOperator
{
public:
    LogicalInput(const std::string& logicalName, const std::string& alias);
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query) override;
    void inferAccess(const std::shared_ptr<Query>& query) override;
    static const char* OP_INPUT_NAME;
    std::string getInspectable() const override { return _settings.toString(); }
    static PlistSpec const* makePlistSpec();
private:
    InputSettings _settings;
};

} //namespace
