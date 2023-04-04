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
 * LogicalSpgemm.cpp
 *
 *  Created on: November 4, 2013
 */

#include <query/LogicalOperator.h>

#include <array/ArrayDesc.h>
#include <array/ArrayDistributionInterface.h>
#include <query/Expression.h>
#include <query/Query.h>

using namespace std;

namespace scidb
{

inline bool hasSingleAttribute(ArrayDesc const& desc)
{
    return desc.getAttributes(true).size() == 1;
}

/**
 * @brief The operator: spgemm().
 *
 * @par Synopsis:
 *   spgemm( leftArray, rightArray [,semiring] )
 *
 * @par Summary:
 *   Produces a result array via matrix multiplication.
 *   Both matrices must have a single numeric attribute.
 *   The two matrices must have the same size of 'inner' dimension and same chunk size along that dimension.
 *
 * @par Input:
 *   - leftArray: the left matrix with two dimensions: leftDim1, leftDim2
 *   - rightArray: the right matrix with two dimensions: rightDim1, rightDim2
 *   - [semiring]: optional name of a semiring to be used instead of ordinary arithmetic (+,*)
 *                 when performing the matrix multiplication. Supported values are:
 *                 "min.+" -- the Tropical Semiring, i.e. a+b -> min(a,b) ; a*b -> a+b ;
 *                            the implicit sparse value is +inf.
 *                 "max.+" -- the Arctic Semiring,   i.e. a+b -> max(a,b) ; a*b -> a+b ;
 *                            the implicit sparse vlaue is -inf.
 *                 This option is useful for writing graph theoretic operations expressed
 *                 and computed as linear algebra.  An introduction to the subject suitable
 *                 for a computer scientist is: Stephan Dolan, "Fun with Semirings,
 *                 A functional perl on the abuse of linear algebra"
 *                 [http://www.cl.cam.ac.uk/~sd601/papers/semirings.pdf]
 *
 * @par Output array:
 *        <
 *   <br>   'multiply': the result attribute name
 *   <br> >
 *   <br> [
 *   <br>   leftDim1
 *   <br>   rightDim2
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
class LogicalSpgemm : public  LogicalOperator
{
public:
    LogicalSpgemm(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(PP(PLACEHOLDER_INPUT)),
                 RE(RE::STAR, {
                    RE(PP(PLACEHOLDER_CONSTANT, TID_STRING)) // a string that contains the named semiring option
                 })
              })
            },
            {"right_replicate", RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 2);

        if (!hasSingleAttribute(schemas[0]) || !hasSingleAttribute(schemas[1]))
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR2);
        if (schemas[0].getDimensions().size() != 2 || schemas[1].getDimensions().size() != 2)
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR3);

        if (schemas[0].getDimensions()[0].isMaxStar()
                || schemas[0].getDimensions()[1].isMaxStar()
                || schemas[1].getDimensions()[0].isMaxStar()
                || schemas[1].getDimensions()[1].isMaxStar())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR4);

        if (schemas[0].getDimensions()[1].getLength() != schemas[1].getDimensions()[0].getLength()
                || schemas[0].getDimensions()[1].getStartMin() != schemas[1].getDimensions()[0].getStartMin())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR5);

#if 0
        // Chunk intervals are checked in PhysicalSpgemm::requiresRedimensionOrRepartition().
#else
        // Enabling "local auto-repart" (as implied above) would require changes to the
        // checkin.scalapack.14_gemm_neg test.  I'm reluctant to do that while ticket #5049 is open,
        // since a bogus result would have to be recorded in the .expected file.  So for now, keep
        // the old behavior so as not to break the test.
        //
        if (schemas[0].getDimensions()[1].getRawChunkInterval() != schemas[1].getDimensions()[0].getRawChunkInterval())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR6);
        // Two autochunked inputs don't count as having "same intervals".
        if (schemas[0].getDimensions()[1].isAutochunked())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED) << "spgemm";
#endif

        if (schemas[0].getAttributes().firstDataAttribute().getType() != schemas[1].getAttributes().firstDataAttribute().getType())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR7);
        if (schemas[0].getAttributes().firstDataAttribute().isNullable() || schemas[1].getAttributes().firstDataAttribute().isNullable())
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR8);

        // only support built-in numeric types
        TypeId type = schemas[0].getAttributes().firstDataAttribute().getType();
        if (type!=TID_FLOAT &&
            type!=TID_DOUBLE
            ) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_MULTIPLY_ERROR9);
        }

        //
        // get the optional 3rd argument: the semiring string: "min.+" or "max.+". They only apply to float and double
        //  or the optional 3rd/4th argument: the rightReplicate flag
        //
        string namedOptionStr;
        switch (_parameters.size()) { // number of options remaining to be processed?
        case 0:
            break; // done
        case 1:
        case 2:
            typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;
            namedOptionStr = evaluate(
                reinterpret_cast<ParamType_t&>(_parameters[0])->getExpression(), TID_STRING).getString();
            if (namedOptionStr != "min.+" &&
                namedOptionStr != "max.+" &&
                namedOptionStr != "count-mults") {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
                    << "spgemm" << namedOptionStr;
            }
            if (type != TID_FLOAT && type != TID_DOUBLE) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OPERATOR_FAILED)
                    << "spgemm" << "The 'min.+' and 'max.+' options support only float and double attribute types";
            }
            break;
        default:
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT3)
                << "spgemm";
            break;
        }


        Attributes atts;
        atts.push_back(AttributeDesc("multiply", type, 0, CompressorType::NONE));

        Dimensions dims(2);
        DimensionDesc const& d1 = schemas[0].getDimensions()[0];
        dims[0] = DimensionDesc(d1.getBaseName(),
                                d1.getNamesAndAliases(),
                                d1.getStartMin(),
                                d1.getCurrStart(),
                                d1.getCurrEnd(),
                                d1.getEndMax(),
                                d1.getRawChunkInterval(),
                                0);

        DimensionDesc const& d2 = schemas[1].getDimensions()[1];
        dims[1] = DimensionDesc(d1.getBaseName() == d2.getBaseName() ? d1.getBaseName() + "2" : d2.getBaseName(),
                                d2.getNamesAndAliases(),
                                d2.getStartMin(),
                                d2.getCurrStart(),
                                d2.getCurrEnd(),
                                d2.getEndMax(),
                                d2.getRawChunkInterval(),
                                0);

        // ArrayDesc consumes the new copy, source is discarded.
        return ArrayDesc("Multiply", atts.addEmptyTagAttribute(), dims, createDistribution(dtRowCyclic), query->getDefaultArrayResidency());
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSpgemm, "spgemm");

} // end namespace scidb
