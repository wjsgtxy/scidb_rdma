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
 * LogicalBadReadWrite.cpp
 *
 */

#include <query/LogicalOperator.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: test_badreadwrite().
 *
 * @par Synopsis:
 *   test_badreadwrite( array [, numAttrsToScanAtOnce] )
 *
 * @par Summary:
 *       A modified and misbehaving version of the consume() operator.
 *       This test operator differs from consume in the following ways:
 *       1. It calls BufferMgr::clearCache at the beginning of execution
 *       2. It opens each chunk first as READ-only (via getConstChunk)
 *       3. It opens each chunk as WRITE (which dirties the buffer)
 *       4. It calls BufferMgr::clearCache a second time to force the dirtied (yet
 *          unchanged) buffers to be re-written to disk.
 *
 * @par Input:
 *   - array: the array to badreadwrite
 *   - numAttrsToScanAtOnce: optional "stride" of the scan, default is 1
 *
 * @par Output array (an empty array):
 *        <
 *   <br> >
 *   <br> [
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalBadReadWrite : public LogicalOperator
{
private:
    int _numVaryParam;

public:
    LogicalBadReadWrite(const string& logicalName, const string& alias)
        : LogicalOperator(logicalName, alias)
        , _numVaryParam(0)
    {
        _properties.tile = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64))
                 })
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc> schemas, shared_ptr<Query> query)
    {
        return schemas[0];
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalBadReadWrite, "test_badreadwrite");

}  // namespace scidb
