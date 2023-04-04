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
 * @file LogicalAppendHelper.cpp
 * @date 2018-10-23
 * @author Mike Leibensperger
 */

#include <array/ArrayName.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <rbac/Rights.h>
#include <system/SystemCatalog.h>

#include <sys/param.h>          // for roundup

using namespace std;

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.append"));
}

namespace scidb {

/**
 * @brief The operator: _append_helper().
 *
 * @par Synopsis:
 *   @code
 *   _append_helper( SRC, DST [, dimName] )
 *   @endcode
 *
 * @par Summary:
 *   Called by the append() macro to reposition input cells prior to
 *   insert().  The goal is to insert the source into the destination
 *   at a position beyond the last cell of a particular dimension.
 *
 *   For dataframes, the "append dimension" is implied, and appending
 *   at chunk boundaries is permitted.  In other words, we can have
 *   empty gaps, so we don't need to know the exact bounding box of
 *   the input.
 *
 *   For arrays, the dimName "append dimension" is either given
 *   explicitly or is deduced for 1-D arrays.  We'll need to know the
 *   "left edge" of the input, so we'll have to materialize it and
 *   compute the bounding box.
 */
class LogicalAppendHelper : public LogicalOperator
{
    constexpr static char const * const cls = "LogicalAppendHelper::";
    using super = LogicalOperator;

public:
    LogicalAppendHelper(const string& logicalName, const string& alias);
    void inferAccess(const std::shared_ptr<Query>& query) override;
    ArrayDesc inferSchema(vector<ArrayDesc> schemas,
                          std::shared_ptr<Query> query) override;
    string getInspectable() const override;
    static PlistSpec const* makePlistSpec();

private:
    size_t _appendDim { ~0ULL };
    Coordinate _appendAt { CoordinateBounds::getMin() };
    Coordinate _offset { 0 };
};


LogicalAppendHelper::LogicalAppendHelper(string const& logicalName,
                                         string const& alias)
    : LogicalOperator(logicalName, alias)
{ }


PlistSpec const* LogicalAppendHelper::makePlistSpec()
{
    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_INPUT)),
             RE(PP(PLACEHOLDER_ARRAY_NAME)),
             RE(RE::QMARK, {
                RE(PP(PLACEHOLDER_DIMENSION_NAME))
             })
          })
        },
    };
    return &argSpec;
}


void LogicalAppendHelper::inferAccess(const std::shared_ptr<Query>& query)
{
    super::inferAccess(query);

    // We need explicit read rights for the destination array's
    // namespace, since we're not using it merely as a schema
    // parameter.  See comment in LogicalOperator::_requestLeastAccess().

    auto arrRef = dynamic_pointer_cast<OperatorParamReference>(_parameters[0]);
    SCIDB_ASSERT(arrRef);
    string nsName, arrayName;
    query->getNamespaceArrayNames(arrRef->getObjectName(), nsName, arrayName);

    query->getRights()->upsert(rbac::ET_NAMESPACE, nsName, rbac::P_NS_READ);
}


ArrayDesc LogicalAppendHelper::inferSchema(vector<ArrayDesc> inDescs,
                                      std::shared_ptr<Query> query)
{
    // Paranoid: verify PlistSpec is producing what we expect.
    SCIDB_ASSERT(inDescs.size() == 1);
    SCIDB_ASSERT(_parameters.size() == 1 || _parameters.size() == 2);

    auto arrRef =
        dynamic_pointer_cast<OperatorParamArrayReference>(_parameters[0]);
    SCIDB_ASSERT(arrRef);
    string const& fullName = arrRef->getObjectName();
    string dstArrayName, dstNsName;
    query->getNamespaceArrayNames(fullName, dstNsName, dstArrayName);

    ArrayDesc dstDesc;
    SystemCatalog::GetArrayDescArgs args;
    args.result = &dstDesc;
    args.nsName = dstNsName;
    args.arrayName = dstArrayName;
    args.versionId = LAST_VERSION; // Ensure we learn latest dimension bounds
    args.throwIfNotFound = true;
    args.catalogVersion = query->getCatalogVersion(dstNsName, dstArrayName);
    SystemCatalog::getInstance()->getArrayDesc(args);

    ArrayDesc const& srcDesc = inDescs[0];
    if (dstDesc.isDataframe() != srcDesc.isDataframe()) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_ARRAYS_NOT_CONFORMANT,
                                   _parameters[0]->getParsingContext())
            << "Both arguments must be dataframes, or else neither can be";
    }
    bool const isDataframe = dstDesc.isDataframe();

    // Find (or infer) the dimension to append along.
    Dimensions const& dstDims = dstDesc.getDimensions();
    if (isDataframe) {
        if (_parameters.size() == 2) {
            // Users are generally not allowed to name the dataframe dimensions.
            auto dimRef = dynamic_pointer_cast<OperatorParamDimensionReference>(_parameters[1]);
            SCIDB_ASSERT(dimRef);
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_DIMENSION_NOT_EXIST,
                                       dimRef->getParsingContext())
                << dimRef->getObjectName() << dstDesc.getQualifiedArrayName() << "dataframe";
        }
        _appendDim = DF_SEQ_DIM; // Append along the $seq dimension.
    }
    else if (_parameters.size() == 1) {
        // Destination has to be 1-D, since no dimension name was given.
        if (dstDims.size() != 1) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                 SCIDB_LE_MULTIDIMENSIONAL_ARRAY_NOT_ALLOWED);
        }
        _appendDim = 0;          // Append along the lone dimension.
    }
    else {
        auto dimRef = dynamic_pointer_cast<OperatorParamDimensionReference>(_parameters[1]);
        SCIDB_ASSERT(dimRef);
        ssize_t d = dstDesc.findDimension(dimRef->getObjectName(), dimRef->getArrayName());
        if (d < 0) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_DIMENSION_NOT_EXIST,
                                       dimRef->getParsingContext())
                << dimRef->getObjectName() << dstDesc.getQualifiedArrayName() << dstDims;
        }
        _appendDim = static_cast<size_t>(d);
    }
    SCIDB_ASSERT(_appendDim != ~0ULL);

    // What is first position on _appendDim to place input cells?
    // ("Right" edge of the destination.)  Since the destination's
    // boundaries are recorded in the SystemCatalog, this is easy to
    // compute from dstDims.
    _appendAt = max(dstDims[_appendDim].getCurrEnd() + 1,
                    dstDims[_appendDim].getStartMin());
    LOG4CXX_DEBUG(logger, cls << __func__ << ": Derived _appendAt=" << _appendAt
                  << " from " << dstDims[_appendDim]);

    // What _offset should be applied to input coordinates to obtain
    // output coordinates?  For that, we need the "left" edge of the
    // source.
    //
    // For dataframes, this calculation is easy: the "left" boundary
    // of the source dataframe is going to be $seq == 0, so all we
    // must do is round _appendAt to the next chunk boundary, and then
    // we'll be able to use reshape/ShiftArray.
    //
    if (isDataframe) {
        if (_appendAt == 0) {
            _offset = 0;
        } else {
            // Round to nearest chunk boundary.  Destination dataframe
            // is non-empty, so getChunkInterval() won't throw.
            _offset = roundup(_appendAt + 1,
                              dstDims[_appendDim].getChunkInterval());
        }
        LOG4CXX_DEBUG(logger, cls << __func__ << ": Append at " << _appendAt
                      << " on dimension " << _appendDim
                      << ", offset=" << _offset);
    }
    else {
        // For arrays, similar calculations must wait until
        // PhyscialAppendHelper() execute, when we've computed the
        // bounding box of the input.

        LOG4CXX_DEBUG(logger, cls << __func__ <<
                      ": Defer edge calculation for arrays");
    }

    if (isDataframe) {
        // Because dataframe dimensions are pretty uniform, we can use
        // the same canned conformance checking as for store/insert.
        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_INTERVAL |
                                   ArrayDesc::SHORT_OK_IF_EBM);
    }
    else {
        // For arrays, if it's conceivable that src can be appended to
        // dst, we should allow it, and let it fail at execute() time
        // if it won't actually fit.  In any case, there's little we
        // can deduce from the srcDims, because (a) cells will get
        // moved around by an as-yet-unknown offset, and (b) we've no
        // idea about the bounding box of the src yet, it may just be
        // one cell.  And we know we're going to invoke a repart()
        // suboperator, so chunk interval and overlap problems should
        // get solved automatically.

        // Dimension count has to match, though!
        Dimensions const& srcDims = srcDesc.getDimensions();
        if (srcDims.size() != dstDims.size()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
                << "append" << srcDims.size() << dstDims.size();
        }

        // And attribute count and types must match.
        ArrayDesc::checkAttributeConformity(srcDesc, dstDesc);
    }

    return dstDesc;
}


string LogicalAppendHelper::getInspectable() const
{
    // Physical op wants to know:
    // - which dimension to append along,
    // - first position to append a cell,
    // - offset along appendDim from source coordinate to destination coordinate
    // See PhysicalAppendHelper::setControlCookie().

    // Assert that inferSchema() has figured out stuff.
    SCIDB_ASSERT(_appendDim != ~0ULL);

    ostringstream oss;
    oss << _appendDim
        << ' ' << _appendAt
        << ' ' << _offset;

    return oss.str();
}


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalAppendHelper, "_append_helper")

}  // namespace scidb
