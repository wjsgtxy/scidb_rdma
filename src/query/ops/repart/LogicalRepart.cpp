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
 * LogicalRepart.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "../redimension/RedimSettings.h"

#include <array/ArrayDistributionInterface.h>
#include <query/LogicalOperator.h>
#include <system/Exceptions.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: repart().
 *
 * @par Synopsis:
 *   repart( srcArray, schema [ , offset: offset_vector ] )
 *
 * @par Summary:
 *   Produces a result array similar to the source array, but with different
 *   chunk sizes, different chunk overlaps, or both.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDims.
 *   - schema: the desired schema.
 *   - offset: vector of one value per schema dimension, added to each
 *             cell position
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   dimensions from the desired schema
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
class LogicalRepart: public LogicalOperator
{
public:
    LogicalRepart(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        // Disallow dataframe inputs for now.
        _properties.dataframe = false;

        // When schema parameter is a schema and not a reference, that schema is
        // allowed to refer to reserved attribute or dimension names.
        _properties.schemaReservedNames = true;
    }

     static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_SCHEMA))
              })
            },
            { RedimSettings::KW_CELLS_PER_CHUNK, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { RedimSettings::KW_PHYS_CHUNK_SIZE, RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) },
            { RedimSettings::KW_OFFSET,
              RE(RE::GROUP, { RE(RE::PLUS, { RE(PP(PLACEHOLDER_CONSTANT, TID_INT64)) }) })
            },
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        // RedimSettings constructor validates the keyword parameters.
        RedimSettings settings(getLogicalName(), _kwParameters, /*logical:*/true);

        ArrayDesc schemaParam = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();

        ArrayDesc const& srcArrayDesc = schemas[0];
        Attributes const& srcAttributes = srcArrayDesc.getAttributes();
        Dimensions const& srcDimensions = srcArrayDesc.getDimensions();
        Dimensions dstDimensions = schemaParam.getDimensions();

        // The user can refer to reserved names but not introduce new dimensions or attributes
        // with reserved names.
        if (schemaParam.hasReservedNames() &&
            !srcArrayDesc.hasReservedNames()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_RESERVED_NAMES_IN_SCHEMA)
                << getLogicalName();
        }

        if (!settings.getOffset().empty()
                && settings.getOffset().size() != dstDimensions.size()) {
            stringstream ss;
            ss << RedimSettings::KW_OFFSET << ':' << CoordsToStr(settings.getOffset());
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                << dstDimensions << ss.str();
        }

        if (schemaParam.getName().size() == 0)
        {
            schemaParam.setName(srcArrayDesc.getName());
        }

        if (srcDimensions.size() != dstDimensions.size())
        {
            ostringstream left, right;
            printDimNames(left, srcDimensions);
            printDimNames(right, dstDimensions);
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "repart" << left.str() << right.str();
        }

        for (size_t i = 0, n = srcDimensions.size(); i < n; i++)
        {
            if (srcDimensions[i].getStartMin() != dstDimensions[i].getStartMin())
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR3);

            // The source chunk interval may be unspecified (autochunked) if we are downstream from
            // a redimension().  If that's true, then the source array *must* have an empty bitmap:
            // we can't rely on its length being a multiple of its chunk interval.

            if (!(srcDimensions[i].getEndMax() == dstDimensions[i].getEndMax()
                               || (srcDimensions[i].getEndMax() < dstDimensions[i].getEndMax()
                                   && ((!srcDimensions[i].isAutochunked() &&
                                        (srcDimensions[i].getLength() % srcDimensions[i].getChunkInterval()) == 0)
                                       || srcArrayDesc.getEmptyBitmapAttribute() != NULL))))
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR4);
            }
            if (srcDimensions[i].getStartMin() == CoordinateBounds::getMin())
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REPART_ERROR5);
            }
        }

        return ArrayDesc(schemaParam.getName(), srcAttributes, dstDimensions,
                         createDistribution(dtUndefined),
                         srcArrayDesc.getResidency(),
                         schemaParam.getFlags());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRepart, "repart")


} //namespace
