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
#include <query/Query.h>

namespace scidb
{

/**
 * @brief The operator: _dskeys().
 *
 * @par Synopsis:
 *   _dskeys( A )
 *
 * @par Summary:
 *   Discover all rocksdb entries on every instance for each unversioned
 *   array ID mentioned in the input array A.
 *   The input array A may have any shape but must have a numeric type
 *   as its first attribute.
 *
 * @par Input:
 *   - A - an input array
 *
 * @par Output:
 *   A two-dimensional array with six attributes, each cell of which
 *   describing chunks known to rocksdb for the given unversioned array ID.
 *   For each of those known chunks, the attributes are:
 *     - nsid - The datastore namespace ID
 *     - dsid - The datastore ID
 *     - ndims - The number of dimensions
 *     - coords - The upper-left corner of the chunk
 *     - arrverid - The versioned array ID that produced the chunk
 */
class LogicalDsKeys : public LogicalOperator
{
public:
    LogicalDsKeys(const std::string& logicalName,
                  const std::string& alias)
        : LogicalOperator(logicalName,
                          alias)
    { }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "",
              RE(RE::LIST, {
                      // Unversioned Array ID.
                      RE(PP(PLACEHOLDER_INPUT)),
                  })
            }
        };
        return &argSpec;
    }

private:
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override
    {
        SCIDB_ASSERT(schemas.size() == 1);
        const auto& fda = schemas[0].getAttributes().firstDataAttribute();
        const auto& attrType = fda.getType();
        if (!IS_NUMERIC(attrType) || !IS_INTEGRAL(attrType)) {
            // Support integer types only.
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "First attribute in the input array must be an integral numeric type";
        }

        Attributes attributes;
        attributes.push_back(AttributeDesc("$nsid", TID_UINT32, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("$dsid", TID_UINT64, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("$ndims", TID_UINT64, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("$attid", TID_STRING, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("$coords", TID_STRING, 0, CompressorType::NONE));
        attributes.push_back(AttributeDesc("$arrverid", TID_UINT64, 0, CompressorType::NONE));

        Dimensions dimensions(2);
        dimensions[0] = DimensionDesc("$instance", 0, 0,
                                      CoordinateBounds::getMax(),
                                      CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("$n", 0, 0,
                                      CoordinateBounds::getMax(),
                                      CoordinateBounds::getMax(), 1000, 0);

        return ArrayDesc("$dskeys",
                         attributes.addEmptyTagAttribute(),
                         dimensions,
                         createDistribution(dtUndefined),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalDsKeys, "_dskeys")

}
