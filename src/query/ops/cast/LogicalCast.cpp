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
 * @file LogicalCast.cpp
 * @author Knizhnik
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include <array/ArrayName.h>
#include <query/FunctionLibrary.h>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/UserQueryException.h>
#include <system/Warnings.h>
#include <util/OnScopeExit.h>

#include <boost/regex.hpp>
#include <log4cxx/logger.h>

using namespace std;

namespace {
  log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.cast"));
}

namespace scidb
{

/**
 * @brief The operator: cast().
 *
 * @par Synopsis:
 *   cast( srcArray, schemaArray | schema )
 *
 * @par WARNING:
 *   This doc is now obsolete and will eventually be removed.  You
 *   should refer to the official release documentation.  Visit
 *   https://forum.paramdigm4.com to find the official link.
 *
 * @par Summary:
 *   Produces a result array with data from srcArray but with the provided schema.
 *   There are three primary purposes:
 *   - To change names of attributes or dimensions.
 *   - To change types of attributes
 *   - To change a non-integer dimension to an integer dimension (XXX NIDs are gone now)
 *   - To change a nulls-disallowed attribute to a nulls-allowed attribute.
 *
 * @par Input:
 *   - srcArray: a source array.
 *   - schemaArray | schema: an array or a schema, from which attrs and dims will be used by the output array.
 *
 * @par Output array:
 *        <
 *   <br>   attrs
 *   <br> >
 *   <br> [
 *   <br>   dims
 *   <br> ]
 *
 * @par Examples:
 *   - Given array A <quantity: uint64, sales:double> [year, item] =
 *     <br> year, item, quantity, sales
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *   - cast(A, <q:uint64, s:double>[y=2011:2012,2,0, i=1:3,3,0]) <q:uint64, s:double> [y, i] =
 *     <br>  y,    i,      q,       s
 *     <br> 2011,  2,      7,     31.64
 *     <br> 2011,  3,      6,     19.98
 *     <br> 2012,  1,      5,     41.65
 *     <br> 2012,  2,      9,     40.68
 *     <br> 2012,  3,      8,     26.64
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */

class LogicalCast : public LogicalOperator
{
    constexpr static char const * const cls = "LogicalCast::";

public:
    LogicalCast(const string& logicalName, const std::string& alias);

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas,
                          std::shared_ptr< Query> query) override;

    static PlistSpec const* makePlistSpec();

private:

    // Types

    using CtxPtr = std::shared_ptr<ParsingContext>;
    using Contexts = std::vector<CtxPtr>;

    enum LocErr { OK, NOT_FOUND, DUPLICATE };

    struct Location {
        bool isAttr { false };  // ...else dimension
        bool byPos { false };   // found name was $n or $_n
        size_t index { 0ULL };  // vector offset
        LocErr error { OK };

        Location() = default;
        Location(bool a, bool p, size_t i) : isAttr(a), byPos(p), index(i) {}
        explicit Location(LocErr e) : error(e) {}
    };

    // Data

    size_t _namedAttrCount { 0 }; // excludes empty bitmap
    Attributes _workingAttrs;
    Dimensions _workingDims;
    Contexts _workingContexts;  // (null entry ? from input : from parameter)
    string _inNs;               // input schema's namespace
    std::unique_ptr<ArrayDesc> _inferred;
    bool _castDataframe { false };

    // Methods

    Location _lookup(string const& name, string const& alias, bool byDollar);
    CtxPtr& _locToCtx(Location const&);
    void _overwrite(OperatorParamSchema const&);
    void _overwrite(ArrayDesc const&, CtxPtr ctx = CtxPtr());
    void _absorb(OperatorParamSchema const&);
    void _absorbAttribute(Location const&, AttributeDesc const&);
    void _absorbDimension(Location const&, DimensionDesc const&);
    void _rename(OperatorParamNested const&);
    void _renameAttribute(Location const&, string const&);
    void _renameDimension(Location const&, string const&);
    void _throwUnlessCompatibleWith(ArrayDesc const&);
    bool _isCongruent(ArrayDesc const&) const;

    /**
     * Parse the qualified name into its constituents, the array name and
     * identifier.  If the qualified name has only the identifier (that is,
     * it is not qualified by an array name), then the array name will be blank.
     * @param qn The qualified name to parse.
     * @param arrayName The array name portion of the qualified name.
     * @param objectName The object name portion of the qualified name.
     */
    void _getIdentifier(const scidb::OperatorParamObjectName::QualifiedName& qn,
                        std::string& arrayName,
                        std::string& objectName) const;
};


LogicalCast::LogicalCast(const string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
{
    _properties.dataframe = true;

    // Operator cast() allows you to cast dimensions and attributes
    // by some index... denoted by $n for n-th attr and $_n for
    // the n-th dimension.  So, need to allow these names.
    _properties.schemaReservedNames = true;
}


PlistSpec const* LogicalCast::makePlistSpec()
{
    // In the following example,
    //
    //    cast(A, (a, tmp), (b, a), (tmp, b))
    //
    // 'tmp' does not yet exist in the input array's schema
    // at query compile time.  PLACEHOLDER_OBJECT_NAME allows
    // us to pass 'tmp' through the translator to this class in
    // the form of an OperatorParam instance, leaving the
    // interpretation of that name up to us here in the operator.

    static PlistSpec argSpec {
        { "", // positionals
          RE(RE::LIST, {
             RE(PP(PLACEHOLDER_INPUT)),
             RE(RE::PLUS, {
                RE(RE::OR, {
                   RE(PP(PLACEHOLDER_SCHEMA)),
                   RE(RE::GROUP, {
                      RE(PP(PLACEHOLDER_OBJECT_NAME)),
                      RE(PP(PLACEHOLDER_OBJECT_NAME))
                   })
                })
             })
          })
        }
    };

    return &argSpec;
}


ArrayDesc LogicalCast::inferSchema(std::vector< ArrayDesc> schemas,
                                   std::shared_ptr< Query> query)
{
    assert(schemas.size() == 1);
    assert(!getParameters().empty());

    if (_inferred) {
        // We've been here before, return cached result.
        return *_inferred;
    }

    // We've *not* been here before.
    assert(_workingAttrs.empty());
    assert(_workingDims.empty());
    assert(_workingContexts.empty());

    ArrayDesc const& inSchema = schemas[0];
    string workingName = inSchema.getName();
    _inNs = inSchema.getNamespaceName();
    _castDataframe = inSchema.isDataframe();

    // Initial working attributes and dimensions come from the input schema.
    _overwrite(inSchema);

    for (auto& p : getParameters()) {

        auto schemaArg = dynamic_cast<OperatorParamSchema *>(p.get());
        if (schemaArg) {
            // If congruent, it's an old-style cast and we can
            // overwrite the working schema... but never overwrite
            // from a dataframe, that looks like user-specified
            // $inst/$seq names, which isn't allowed.
            if (_isCongruent(schemaArg->getSchema()) && !_castDataframe) {
                _overwrite(*schemaArg);
                workingName = schemaArg->getSchema().getName().empty()
                    ? inSchema.getName()
                    : schemaArg->getSchema().getName();
            } else {
                _absorb(*schemaArg);
            }
            continue;
        }

        auto renameArg = dynamic_cast<OperatorParamNested *>(p.get());
        if (renameArg) {
            _rename(*renameArg);
            continue;
        }

        // Still here?  Bummer.
        ASSERT_EXCEPTION_FALSE("Unsupported parameter type "
                               << p->getParamType()
                               << " snuck into "
                               << cls << __func__);
    }

    // After step-wise modifying the working schema, we're ready to
    // validate and build the result.
    _throwUnlessCompatibleWith(inSchema);
    _inferred.reset(new ArrayDesc(workingName,
                                  _workingAttrs,
                                  _workingDims,
                                  inSchema.getDistribution(),
                                  inSchema.getResidency(),
                                  inSchema.getFlags()));
    return *_inferred;
}


// True iff the schemas are the "same shape".
bool LogicalCast::_isCongruent(ArrayDesc const& schema) const
{
    // Pretend everyone has an empty bitmap, even if they don't.
    size_t workingSize = _workingAttrs.size();
    if (!_workingAttrs.hasEmptyIndicator()) {
        workingSize += 1;
    }
    Attributes const& schemaAttrs = schema.getAttributes(/*exclude:*/false);
    size_t schemaSize = schemaAttrs.size();
    if (!schemaAttrs.hasEmptyIndicator()) {  // DJG
        schemaSize += 1;
    }

    return schemaSize == workingSize
        && schema.getDimensions().size() == _workingDims.size();
}


// Normalized attribute/dimension lookup.  Returns a Location, even if
// the name indicates a position ($n for n-th attribute, $_n for n-th
// dimension).
LogicalCast::Location
LogicalCast::_lookup(string const& name, string const& alias, bool byDollar)
{
    LOG4CXX_TRACE(logger, cls << __func__ << ": name='" << name
                  << "', alias='" << alias << "'");

    if (byDollar && alias.empty()) {

        // No alias, so maybe it's a position name $n or $_n ...

        boost::regex positional("^\\$(_)?([0-9]+)$");
        boost::smatch result;
        if (boost::regex_match(name, result, positional)) {

            // Matched.  $n means n-th attribute, $_n means n-th dimension.
            assert(result.size() == 3);
            bool isAttr = result[1] == "";
            static_assert(sizeof(unsigned long) == sizeof(size_t),
                          "Uh oh, strtoul(3) is not what you want here.");
            size_t index = ::strtoul(result[2].str().c_str(), nullptr, 10);

            // Is this a valid position?
            if ((isAttr && index >= _workingAttrs.size()) ||
                (!isAttr && index >= _workingDims.size()))
            {
                LOG4CXX_TRACE(logger, "Bad positional name " << name);
                return Location(NOT_FOUND);
            }

            LOG4CXX_TRACE(logger, "Found positional "
                          << (isAttr ? "attribute" : "dimension")
                          << " name " << name << " at index " << index);
            return Location(isAttr, /*byPos:*/true, index);
        }
    }

    // Sadly, we'll have to trudge through *every* attribute *and*
    // dimension, -a la- Translator::resolveParam*() methods.

    Location result;
    bool found = false;
    string qualName = makeQualifiedArrayName(_inNs, alias);

    for (size_t i = 0; i < _namedAttrCount; ++i) {
        AttributeDesc const& attr = _workingAttrs.findattr(i);
        if (attr.getName() != name) {
            LOG4CXX_TRACE(logger, "No match vs. " << attr.getName());
            continue;
        }
        if (isDebug()) {
            stringstream ss;
            attr.toString(ss, 0);
            LOG4CXX_TRACE(logger, "Check vs. " << ss.str());
        }
        if (attr.hasAlias(alias) || attr.hasAlias(qualName)) {
            if (found) {
                LOG4CXX_TRACE(logger, "Dup attr at index " << i);
                return Location(DUPLICATE); // D'oh!
            }
            LOG4CXX_TRACE(logger, "Found attr at index " << i);
            found = true;
            result.isAttr = true;
            result.byPos = false;
            result.index = i;
        }
    }
    for (size_t i = 0; i < _workingDims.size(); ++i) {
        DimensionDesc const& dim = _workingDims[i];
        if (isDebug()) {
            stringstream ss;
            dim.toString(ss, 0);
            LOG4CXX_TRACE(logger, "Check vs. " << ss.str());
        }
        if (dim.hasNameAndAlias(name, alias)) {
            if (found) {
                LOG4CXX_TRACE(logger, "Dup dim at index " << i);
                return Location(DUPLICATE); // D'oh!
            }
            LOG4CXX_TRACE(logger, "Found dim at index " << i);
            found = true;
            result.isAttr = false;
            result.byPos = false;
            result.index = i;
        }
    }

    return found ? result : Location(NOT_FOUND);
}


LogicalCast::CtxPtr& LogicalCast::_locToCtx(Location const& loc)
{
    assert(loc.error == OK);
    size_t idx = loc.index;
    if (!loc.isAttr) {
        idx += _namedAttrCount;
    }
    assert(idx < _workingContexts.size());
    return _workingContexts[idx];
}


// Given schema overwrites the current working schema.
//
// @note The ctx ParsingContext pointer may be null, for example when
//       initially overwriting with the schema of the input array.  We
//       make use of this fact when checking for reserved $foo names:
//       if the context was null, the name came from the input schema
//       and wasn't introduced by this cast.
//
void LogicalCast::_overwrite(ArrayDesc const& schema, CtxPtr ctx)
{
    bool firstTime = _workingAttrs.empty();
    assert(_workingAttrs.empty() == _workingDims.empty());
    assert(_isCongruent(schema) || firstTime);

    // Overwrite attributes entirely... but be aware that (a) we can't
    // generate a missing empty bitmap, and (b) we allow schema
    // parameters that don't have them.
    //
    // DJG: Maybe once your EBM changes are in this can go back to
    // being a simple assignment?  Will grand aggregates (and
    // everything else that produces dense "non-emptyable" arrays) get
    // EBMs now?  The else-clause loop relies on named attributes
    // having the same index regardless of where the EBM sits.

    Attributes const& schemaAttrs = schema.getAttributes(/*exclude:*/false);
    if (firstTime
        || schemaAttrs.hasEmptyIndicator() == _workingAttrs.hasEmptyIndicator())
    {
        _workingAttrs = schemaAttrs;
    }
    else {
        // Either (a) or (b) means "clobber all except the EBM".
        for (const auto& attr : schemaAttrs) {
            if (attr.isEmptyIndicator()) {
                continue;
            }
            // Ugh.  Why can't I write "_workingAttrs[i] = schemaAttrs[i];" ??
            Location loc(/*isAttr:*/true, /*byPos:*/false, attr.getId());
            _absorbAttribute(loc, attr);
        }
    }

    size_t workingEbm = _workingAttrs.hasEmptyIndicator() ? 1 : 0; // DJG
    if (_namedAttrCount == 0) {
        _namedAttrCount = _workingAttrs.size() - workingEbm;
    } else {
        assert(_namedAttrCount == _workingAttrs.size() - workingEbm);
    }

    if (firstTime) {
        // Take dimensions wholesale, this is the initialization of
        // the working schema from the input array schema.
        _workingDims = schema.getDimensions();
    }
    else if (_castDataframe) {
        if (!schema.isDataframe()) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_BAD_DATAFRAME_USAGE)
                << getLogicalName() << "Cannot overwrite dataframe dimension";
        } // ...else DF-to-DF overwrite, ignore dimensions silently.
    }
    else {
        // Updating existing working schema.  This is the backward
        // compatibility case.
        //
        // Some dimension updates are legal, others are ignored.  So we
        // can't just overwrite the dimensions, instead we must start with
        // the input array dimensions and then "absorb" those from the
        // schema parameter.

        Dimensions const& srcDims = schema.getDimensions();
        assert(_workingDims.size() == srcDims.size());

        for (size_t i = 0; i < srcDims.size(); ++i) {
            Location loc(/*isAttr:*/false, /*byPos:*/false, i);
            _absorbDimension(loc, srcDims[i]);
        }
    }

    // Finally, if you are overwriting then everybody's context comes
    // from this schema parameter.  (Note we don't store a context for
    // the empty bitmap attribute.)
    _workingContexts.clear();
    _workingContexts.resize(_namedAttrCount + _workingDims.size(), ctx);
}


void LogicalCast::_overwrite(OperatorParamSchema const& schemaParam)
{
    _overwrite(schemaParam.getSchema(), schemaParam.getParsingContext());
}


void LogicalCast::_absorb(OperatorParamSchema const& schemaParam)
{
    int absorbed = 0;

    ArrayDesc const& schema = schemaParam.getSchema();
    for (size_t i = 0; i < schema.getAttributes(/*exclude:*/true).size(); ++i) {
        const auto& attr = schema.getAttributes(/*exclude:*/true).findattr(i);
        Location loc = _lookup(attr.getName(), "", /*$:*/true);
        if (loc.error != OK || !loc.isAttr) {
            continue;
        }

        // Don't bother with convertibility checking here, because
        // ultimately PhysicalCast will convert from the *original*
        // type, not from the working schema type.  We'll check
        // convertibility later, for now just slam it in there.

        _locToCtx(loc) = schemaParam.getParsingContext();
        _absorbAttribute(loc, attr);
        ++absorbed;
    }

    if (_castDataframe) {
        if (!schema.isDataframe()) {
            LOG4CXX_DEBUG(logger, getLogicalName() << "Dataframe dimension changes ignored");
        } // ...else DF-to-DF cast, ignore dimensions silently.
    }
    else if (schema.isDataframe()) {
        // Dataframe schema provided as an argument just as a
        // convenience to avoid having to invent an artificial
        // dimension name.  No need to absorb its dimensions: do
        // nothing.
        LOG4CXX_TRACE(logger, "Dataframe schema used only for its attributes: "
                      << schema);
    }
    else for (auto const& srcDim : schema.getDimensions()) {

        Location loc = _lookup(srcDim.getBaseName(), "", /*$:*/true);
        if (loc.error != OK || loc.isAttr) {
            continue;
        }

        _locToCtx(loc) = schemaParam.getParsingContext();
        _absorbDimension(loc, srcDim);
        ++absorbed;
    }

    if (!absorbed) {
        // If we didn't make use of any part of the schema, the user
        // probably made a mistake in composing the query.
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_OP_CAST_SCHEMA_NOT_USED,
                                   schemaParam.getParsingContext())
            << schema;
    }
}


void LogicalCast::_absorbAttribute(Location const& loc,
                                   AttributeDesc const& attr)
{
    // Incoming attribute likely has the wrong AttributeID.  Place it
    // into the indicated position in _workingAttrs, and make sure it
    // takes on the correct id.

    _workingAttrs.replace(static_cast<AttributeID>(loc.index),
        AttributeDesc((loc.byPos  // ...then don't rename to $42
                       ? _workingAttrs.findattr(loc.index).getName()
                       : attr.getName()),
                      attr.getType(),
                      attr.getFlags(),
                      attr.getDefaultCompressionMethod(),
                      attr.getAliases(),
                      &attr.getDefaultValue(),
                      attr.getDefaultValueExpr(),
                      attr.getVarSize()));
}


void LogicalCast::_absorbDimension(Location const& loc,
                                   DimensionDesc const& newDim)
{
    DimensionDesc& oldDim = _workingDims[loc.index];

    // Alex says: "If input is bounded and cast is unbounded -- don't
    // change the bounds of input.  Changing bounds is not cheap,
    // especially the lower bound.  If you want to change the bounds,
    // properly, use subarray."

    _workingDims[loc.index] =
        DimensionDesc((loc.byPos
                       ? oldDim.getBaseName()
                       : newDim.getBaseName()),
                      (loc.byPos
                       ? oldDim.getNamesAndAliases()
                       : newDim.getNamesAndAliases()),
                      oldDim.getStartMin(),
                      oldDim.getCurrStart(),
                      oldDim.getCurrEnd(),
                      newDim.getEndMax() == CoordinateBounds::getMax() &&
                          oldDim.getCurrEnd() != CoordinateBounds::getMin()
                              ? oldDim.getEndMax() : newDim.getEndMax(),
                      oldDim.getRawChunkInterval(),
                      oldDim.getChunkOverlap());
}

void LogicalCast::_getIdentifier(const scidb::OperatorParamObjectName::QualifiedName& qn,
                                 std::string& arrayName,
                                 std::string& objectName) const
{
    switch (qn.size()) {
    case 1:
        arrayName = "";
        objectName = qn[0];
        break;
    case 2:
        arrayName = qn[0];
        objectName = qn[1];
        break;
    default:
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_OPERATOR_ARGUMENT)
            << getLogicalName() << "Qualified names must take the form of X.Y";
    };
}

void LogicalCast::_rename(OperatorParamNested const& nest)
{
    assert(nest.size() == 2);

    // Pick out the two names.
    Parameters const& nested = nest.getParameters();
    auto p0 = dynamic_cast<OperatorParamObjectName *>(nested[0].get());
    auto p1 = dynamic_cast<OperatorParamObjectName *>(nested[1].get());
    assert(p0 && p1);

    // The "from" name p0 must exist.  By using PARAM_OBJECT_NAME,
    // we intentionally prevented the Translator from checking
    // that, because it would have checked against the input
    // schema, whereas we need to check it here, against the
    // _working_ schema (see comment above).

    std::string p0_arrayName;
    std::string p0_objectName;  // Could be an attribute or a dimension.
    _getIdentifier(p0->getName(),
                   p0_arrayName,
                   p0_objectName);

    Location fromLoc =
        _lookup(p0_objectName, p0_arrayName, /*$:*/true);
    if (fromLoc.error != OK) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION,
                                   p0->getParsingContext())
            << p0->toString();
    }

    // No renaming of dataframe dimensions, sorry.
    if (_castDataframe && !fromLoc.isAttr) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_BAD_DATAFRAME_USAGE,
                                   p0->getParsingContext())
            << getLogicalName() << "Cannot rename dataframe dimension";
    }

    // The "to" target name p1 can't be an alias (that is, can't be
    // contained in an array scope).

    std::string p1_arrayName;
    std::string p1_objectName;
    _getIdentifier(p1->getName(),
                   p1_arrayName,
                   p1_objectName);

    if (!p1_arrayName.empty()) {
        throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                   SCIDB_LE_NAME_CANT_BE_QUALIFIED,
                                   p1->getParsingContext())
            << p1_arrayName << p1_objectName;
    }

    // And of course the target name must *not* exist in the current
    // working schema.

    const std::string& toName = p1_objectName;
    Location toLoc = _lookup(toName, "", /*$:*/false);
    if (toLoc.error != NOT_FOUND) {
        // 'fromLoc' is what we're trying to rename, so it dictates
        // the message text.
        bool ok = toLoc.error == OK;
        if (ok && fromLoc.isAttr) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME,
                                       p1->getParsingContext())
                << toName;
        } else if (ok && !fromLoc.isAttr) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_DUPLICATE_DIMENSION_NAME,
                                       p1->getParsingContext())
                << toName;
        } else if (fromLoc.isAttr) {
            assert(toLoc.error == DUPLICATE);
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_AMBIGUOUS_ATTRIBUTE,
                                       p1->getParsingContext())
                << toName;
        } else {
            assert(toLoc.error == DUPLICATE);
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_AMBIGUOUS_DIMENSION,
                                       p1->getParsingContext())
                << toName;
        }
    }

    // Whew.  Finally we're ready to do the renaming.
    _locToCtx(fromLoc) = p1->getParsingContext();
    if (fromLoc.isAttr) {
        _renameAttribute(fromLoc, toName);
    } else {
        _renameDimension(fromLoc, toName);
    }
}


void LogicalCast::_renameAttribute(Location const& loc, string const& to)
{
    assert(loc.error == OK);
    assert(loc.isAttr);
    size_t attrId = loc.index;

    // Rename in the _workingAttrs vector.
    AttributeDesc const& oldDesc = _workingAttrs.findattr(attrId);
    _workingAttrs.replace(static_cast<AttributeID>(attrId),
        AttributeDesc(to,
                      oldDesc.getType(),
                      oldDesc.getFlags(),
                      oldDesc.getDefaultCompressionMethod(),
                      oldDesc.getAliases(),
                      &oldDesc.getDefaultValue(),
                      oldDesc.getDefaultValueExpr(),
                      oldDesc.getVarSize()));
}


void LogicalCast::_renameDimension(Location const& loc, string const& to)
{
    assert(loc.error == OK);
    assert(loc.isAttr == false);
    size_t dimNo = loc.index;

    // Rename in the _workingDims vector.
    DimensionDesc const& oldDesc = _workingDims[dimNo];
    _workingDims[dimNo] =
        DimensionDesc(to,
                      oldDesc.getStartMin(),
                      oldDesc.getCurrStart(),
                      oldDesc.getCurrEnd(),
                      oldDesc.getEndMax(),
                      oldDesc.getRawChunkInterval(),
                      oldDesc.getChunkOverlap());
}


void LogicalCast::_throwUnlessCompatibleWith(ArrayDesc const& inputSchema)
{
    // Check attributes.
    Attributes const& inputAttrs = inputSchema.getAttributes(/*exclude:*/true);
    for (size_t i = 0; i < _namedAttrCount; ++i) {

        CtxPtr& ctx = _locToCtx(Location(/*isAttr:*/true, /*dontCare:*/false, i));

        // Check for user-supplied (ctx != null) reserved name.
        // Reserved names from the input schema (ctx == null) are OK.
        string const& attrName = _workingAttrs.findattr(i).getName();
        if (isUserInserted() && ctx && attrName[0] == '$') {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_RESERVED_NAME,
                                       ctx)
                << attrName;
        }

        // Check type conversion.
        if (_workingAttrs.findattr(i).getType() != inputAttrs.findattr(i).getType()) {
            try {
                FunctionLibrary::getInstance()->findConverter(
                    inputAttrs.findattr(i).getType(), _workingAttrs.findattr(i).getType());
            }
            catch (Exception const& ex) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                           SCIDB_LE_OP_CAST_BAD_TYPE_CONVERSION,
                                           ctx)
                    << inputAttrs.findattr(i).getName()
                    << inputAttrs.findattr(i).getType()
                    << _workingAttrs.findattr(i).getType();
            }
        }

        // Check nullability conversion.  You can cast *to* nullable,
        // but you cannot cast *away* nullability (for now).
        auto inputFlags = inputAttrs.findattr(i).getFlags();
        if (_workingAttrs.findattr(i).getFlags() != inputFlags &&
            _workingAttrs.findattr(i).getFlags() != (inputFlags | AttributeDesc::IS_NULLABLE ))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_OP_CAST_FLAG_MISMATCH,
                                       ctx)
                << _workingAttrs.findattr(i).getName();
        }
    }

    // If we started with a dataframe, we should still have a
    // dataframe.  If not, not.
    ASSERT_EXCEPTION(isDataframe(_workingDims) == _castDataframe,
                     "cast() cannot convert to/from dataframe");

    // Check dimensions.
    Dimensions const& inputDims = inputSchema.getDimensions();
    assert(_workingDims.size() == inputDims.size());
    for (size_t i = 0; i < inputDims.size(); ++i) {
        DimensionDesc const& srcDim = inputDims[i];
        DimensionDesc const& dstDim = _workingDims[i];

        CtxPtr& ctx = _locToCtx(Location(/*isAttr:*/false, /*dontCare:*/false, i));

        // Check for user-supplied (ctx != null) reserved name.
        // Reserved names from the input schema (ctx == null) are OK.
        if (isUserInserted() && ctx && dstDim.getBaseName()[0] == '$') {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_RESERVED_NAME,
                                       ctx)
                << dstDim.getBaseName();
        }

        // The source chunk interval may be unspecified (autochunked)
        // if we are downstream from a redimension().  If that's true,
        // then the source array *must* have an empty bitmap: we can't
        // rely on its length being a multiple of its chunk interval.

        if (!(srcDim.getEndMax() == dstDim.getEndMax()
              || (srcDim.getEndMax() < dstDim.getEndMax()
                  && ((!srcDim.isAutochunked() && (srcDim.getLength() % srcDim.getChunkInterval()) == 0)
                      || inputSchema.getEmptyBitmapAttribute() != NULL)))) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_OP_CAST_BAD_DIMENSION_LENGTH,
                                       ctx)
                << dstDim.getBaseName();
        }
    }
}


DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalCast, "cast")

} //namespace
