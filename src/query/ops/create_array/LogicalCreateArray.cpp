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
 * @file LogicalCreateArray.cpp
 * @brief Logical DDL operator that creates a new persistent array.
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <query/LogicalExpression.h>
#include <query/LogicalOperator.h>

#include <array/ArrayName.h>
#include <query/Query.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <log4cxx/logger.h>


#define fail(e) throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,e)

namespace scidb {
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logicalCreate"));

using namespace std;

/**
 * @brief Implements the create_array() operator.
 *
 * @par Synopsis:
 *
 *  @code
 *      create_array ( array_name, array_schema , temp [, cells ] )
 *  @endcode
 *  or
 *  @code
 *      CREATE [ TEMP ] ARRAY array_name  array_schema [ '[' cells ']' ]
 *  @endcode
 *
 * @par Summary:
 *      Creates an array with the given name and schema and adds it to the database.
 *
 * @par Input:
 *      - array_name:      an identifier that names the new array.
 *      - array_schema:    a multidimensional array schema that describes the rank
 *                          and shape of the array to be created, as well as the types
 *                          of each its attributes.
 *      - temp:            a boolean flag, true for a temporary array, false for a db array.
 *      - cells            the desired number of logical cells per chunk (default is 1M)
 *
 *  An array schema has the following form:
 *
 *  @code
 *    array_schema := '<' attributes '>' '[' dimensions ']'
 *
 *    attributes   := attribute {',' attribute}*
 *
 *    dimensions   := dimension {',' dimension}*
 *
 *    attribute    := attribute_name ':' type [[NOT] NULL] [DEFAULT default_value] [COMPRESSION compression_algorithm] [RESERVE integer]
 *
 *    dimension    := dimension_name [= {dim_low|'?'} ':' {dim_high|'?'|'*'} ',' {chunk_interval|'?'} ',' {chunk_overlap|'?'}]
 *  @endcode
 *
 *  where a '?' in place of a dimension detail indicates the a sensible default value should be supplied by the system.
 *
 *  Note:
 *    - For a list of attribute types, use list('types'). Note that loading a plugin may introduce more types.
 *
 *    - The optional constraints of an attribute have the following meaning and default values:
 *      <ul>
 *
 *      <li>[[NOT] NULL]:            indicates whether the attribute may contain null values.
 *                                   Note that SciDB supports multiple null values, with different 'missing' reason codes.
 *                                   You may specify a null value with the function missing(i), where 0<=i<=127.
 *                                   Default is NOT NULL, meaning null values are not allowed.
 *
 *      <li>[DEFAULT default_value]: the value to be automatically substituted when a non-NULL attribute lacks a value.
 *                                   If the attribute is declared as NULL, this clause is ignored, and 'null' (with missing reason 0)
 *                                   will be used as the default value.
 *                                   Otherwise, the default value is 0 for numeric types and "" for the string type.
 *
 *      <li>[COMPRESSION string]:    the compression algorithm that is used to compress chunk data before storing on disk.
 *                                   Default is 'no compression'.
 *
 *                                   Paul describes COMPRESSION thus:
 *
 *                                   " a place holder that will allow users or administrators to tell SciDB which general
 *                                   compression method—for example, gzip—they want to apply to chunks for this attribute.
 *                                   The range of legal values is open: the idea was to make the list of compression methods
 *                                   extensible. The default is no compression. "
 *
 *                                   The syntax is currently recognized and is stored in the meta data, but does nothing
 *                                   at the moment.
 *
 *                                   This should not be described in the user manual.
 *
 *      <li>[RESERVE integer]:       the value of the CONFIG_CHUNK_RESERVE config file setting
 *
 *                                   Paul describes RESERVE it thus:
 *
 *                                   " RESERVE is used to reserve space at the end of a chunk for delta compression. If we
 *                                    anticipate that the update rate for this particular attribute is likely to be high,
 *                                    we’ll reserve more space at the end of the chunk. Because the deltas are turned off,
 *                                    this currently is inoperative."
 *
 *                                   This should not be described in the user manual.
 *      </ul>
 *
 *    - array_name, attribute_name, dimension_name are all identifiers
 *
 *    - dim_low, dim_high, chunk_interval, and chunk_overlap are expressions that should evaluate to a 64-bit integer.
 *
 * @par Output array:
 *        <
 *   <br>   attributes
 *   <br> >
 *   <br> [
 *   <br>   dimensions
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
struct LogicalCreateArray : LogicalOperator
{
    LogicalCreateArray(const string& logicalName,const string& alias)
     : LogicalOperator(logicalName,alias)
    {
        _properties.ddl       = true;
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_ARRAY_NAME).setMustExist(false)),
                 RE(PP(PLACEHOLDER_SCHEMA)),
                 RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)),
                 RE(RE::QMARK, {
                    RE(PP(PLACEHOLDER_DISTRIBUTION)),  // array distribution
                    RE(RE::QMARK, {
                       RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))   // empty tag compression
                    })
                 })
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector<ArrayDesc>, std::shared_ptr<Query> query) override
    {
        // param 0 (array reference/name)
        SCIDB_ASSERT(param<OperatorParam>(0)->getParamType() == PARAM_ARRAY_REF);

        // param 1 (dimension portion of schema)
        SCIDB_ASSERT(param<OperatorParam>(1)->getParamType() == PARAM_SCHEMA);
        auto opParamSchema = std::dynamic_pointer_cast<const OperatorParamSchema>(_parameters[1]);
        const Dimensions& dims = opParamSchema->getSchema().getDimensions();

        // param 2 (isTempArray, optional)
        if (_parameters.size() > 2) {
            std::dynamic_pointer_cast<const OperatorParamLogicalExpression>(_parameters[2]);
        }

        // param 3 (distribution, optional)
        auto distrib = createDistribution(defaultDistType());
        if (_parameters.size() > 3) {
            // If it exists, parse the distribution param.
            auto opd = std::dynamic_pointer_cast<const OperatorParamDistribution>(_parameters[3]);
            if (opd) {
                distrib = opd->getDistribution();
                if (!isStorable(distrib->getDistType())) {
                    throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                        << getLogicalName() << "specified distribution is not storable";
                }
            }
        }

        // param 4 (etcomp, optional)
        CompressorType emptyTagCompression = CompressorType::NONE;
        if (_parameters.size() > 4) {
            // If it exists, parse the empty tag compression param.
            auto targetCompression = paramToString(_parameters[4]);
            emptyTagCompression = stringToCompressorType(targetCompression);
        }

        // validate the given schema against the given dims (from the input schema)
        if (!distrib->valid(dims)) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                << getLogicalName() << "specified schema is not compatible with distribution";
        }

        ArrayDesc result;
        result.setDistribution(createDistribution(getSynthesizedDistType()));
        result.setResidency(query->getDefaultArrayResidency());
        result.setEmptyTagCompression(emptyTagCompression);
        return result;
    }

    void inferAccess(const std::shared_ptr<Query>& query) override
    {
        LogicalOperator::inferAccess(query);

        SCIDB_ASSERT(param<OperatorParam>(0)->getParamType() == PARAM_ARRAY_REF);

        string const& objName = param<OperatorParamArrayReference>(0)->getObjectName();
        SCIDB_ASSERT(!objName.empty());
        SCIDB_ASSERT(!isNameVersioned(objName)); // no version number

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(objName, namespaceName, arrayName);

        if (SystemCatalog::getInstance()->containsArray(namespaceName, arrayName)) {
            fail(SCIDB_LE_ARRAY_ALREADY_EXISTS)
                << makeQualifiedArrayName(namespaceName, arrayName);
        }

        std::shared_ptr<LockDesc> lock(
            make_shared<LockDesc>(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                LockDesc::COORD,
                LockDesc::XCL));
        std::shared_ptr<LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= LockDesc::XCL);

        query->getRights()->upsert(rbac::ET_NAMESPACE, namespaceName,
                                   rbac::P_NS_CREATE);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCreateArray,     "create_array")

} //namespace
