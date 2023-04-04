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
 * LogicalTranspose.cpp
 *
 *  Created on: Mar 9, 2010
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDistributionInterface.h>
#include <query/AutochunkFixer.h>
#include <system/Exceptions.h>

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("LogicalTranspose"));

/**
 * @brief The operator: transpose().
 *
 * @par Synopsis:
 *   transpose( srcArray )
 *
 * @par Summary:
 *   Produces an array with the same data in srcArray but with the list of dimensions reversd.
 *
 * @par Input:
 *   - srcArray: a source array with srcAttrs and srcDims.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs.
 *   <br> >
 *   <br> [
 *   <br>   reverse order of srcDims
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
class LogicalTranspose : public LogicalOperator
{
    AutochunkFixer  _fixer;

public:
    /**
     * Create the transpose operator.
     * @param logicalName the operator name
     * @param alias the alias
     */
    LogicalTranspose(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.dataframe = false; // Dataframe inputs make no sense.
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_INPUT))
            }
        };
        return &argSpec;
    }

    std::string getInspectable() const override
    {
        return _fixer.str();
    }

    /**
     * Determine the schema of the output.
     * @param schemas the shapes of all the input arrays, only one expected
     * @param query the query context
     * @return the 0th element of schemas with the dimensions in reverse order
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 0);

        ArrayDesc const& schema = schemas[0];

        Dimensions const& dims(schema.getDimensions());
        Dimensions transDims(dims.size());

        _fixer.clear();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            _fixer.takeDimension(n-i-1).fromArray(0).fromDimension(i);
            transDims[n-i-1] = dims[i];
        }

        ArrayDistPtr dist;
        if (isByRow(schema.getDistribution()->getDistType())) {
            dist = createDistribution(dtByCol);
        } else if (isByCol(schema.getDistribution()->getDistType())) {
            dist = createDistribution(dtByRow);
        } else if (schema.getDistribution()->getDistType() == dtRowCyclic) {
            dist = createDistribution(dtColCyclic);
        } else if (schema.getDistribution()->getDistType() == dtColCyclic) {
            dist = createDistribution(dtRowCyclic);
        } else {
            dist = createDistribution(dtUndefined);
        }

        LOG4CXX_TRACE(logger, "LogicalTranspose::inferSchema(): "
                              << "dist DistType: " << dist->getDistType());

        return ArrayDesc(schema.getName(), schema.getAttributes(), transDims,
                         dist,
                         schema.getResidency());
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalTranspose, "transpose")

} //namespace scidb
